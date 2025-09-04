import argparse
import csv
import datetime
import json
import logging
import os
import re
from datastation.common.batch_processing import BatchProcessor
from datastation.common.config import init
from datastation.dv_api import publish_dataset, get_dataset_metadata, is_draft_dataset, change_access_request, replace_dataset_metadata, \
    change_file_restrict, update_file_metas


def open_access_archeodepot(datasets_file, licenses_file, must_be_restricted_files, dataverse_config, dry_run, delay):
    doi_to_license_uri = read_doi_to_license(datasets_file, read_rights_holder_to_license(licenses_file))
    doi_to_keep_restricted = read_doi_to_keep_restricted(must_be_restricted_files)
    server_url = dataverse_config['server_url']
    api_token = dataverse_config['api_token']
    logging.info("is dry run: {}".format(dry_run))
    now = datetime.datetime.now().strftime("%Y-%m-%d_%H.%M.%S")
    processor = BatchProcessor(wait=delay)
    dataset_writer = create_csv(
        "datasets", now,
        ["DOI", "Modified", "OldLicense", "NewLicense", "OldRequestEnabled", "NewRequestEnabled", "OldTermsOfAccess", "NewTermsOfAccess"])
    datafiles_writer = create_csv(
        "datafiles", now,
        ["DOI", "FileID", "path", "Modified", "OldRestricted", "NewRestricted"])
    processor.process_entries(
        doi_to_license_uri.items(),
        lambda doi_to_license: update_license(
            "doi:" + doi_to_license[0],
            doi_to_license[1].strip().strip('"'),
            doi_to_keep_restricted.get(to_doi_key(doi_to_license[0]), []),
            server_url,
            api_token,
            dry_run,
            dataset_writer,
            datafiles_writer
        )
    )


def create_csv(object_type, time_stamp, fieldnames):
    file_name = "archeodepot-{}-{}.csv".format(object_type, time_stamp)
    fd = os.open(file_name, os.O_WRONLY | os.O_CREAT)
    file = os.fdopen(fd, 'w', newline='')
    csv_writer = csv.DictWriter(file, fieldnames=fieldnames)
    csv_writer.writeheader()
    return csv_writer


def read_doi_to_license(datasets_file, rights_holder_to_license_uri):
    doi_to_license_uri = {}
    with open(datasets_file, "r") as input_file_handler:
        csv_reader = csv.DictReader(input_file_handler, delimiter=',', fieldnames=["DOI"], restkey="rest")
        logging.info(next(csv_reader))
        for row in csv_reader:
            key = to_doi_key(row["rest"][-1].strip())
            uri = rights_holder_to_license_uri.get(key, "")
            if uri:
                doi_to_license_uri[row["DOI"]] = uri
            else:
                logging.warning("no license for line {}: {}".format(csv_reader.line_num, row))
    return doi_to_license_uri


def read_doi_to_keep_restricted(keep_restricted_files):
    doi_to_keep_restricted = {}
    with open(keep_restricted_files, "r") as input_file_handler:
        csv_reader = csv.DictReader(input_file_handler, delimiter=',',
                                    fieldnames=["title", "dataset_id", "DOI"], restkey="files")
        next(csv_reader)
        for row in csv_reader:
            doi_to_keep_restricted[to_doi_key(row["DOI"])] = list(filter(lambda item: item != "", row["files"]))
    return doi_to_keep_restricted


def read_rights_holder_to_license(licenses_file):
    rights_holder_to_license_uri = {}
    with open(licenses_file, "r") as input_file_handler:
        csv_reader = csv.DictReader(input_file_handler, delimiter=',')
        for row in csv_reader:
            rights_holder_to_license_uri[row["RIGHTS_HOLDER"]] = row["URI"]
    return rights_holder_to_license_uri


def to_doi_key(name):
    return re.sub("https://doi.org/", "", re.sub("doi:", "", name))


def update_license(doi, new_license_uri, must_be_restricted, server_url, api_token, dry_run, datasets_writer,
                   datafiles_writer):
    def change_dataset_metadata(data):
        logging.debug("json {}".format(data))
        if not dry_run:
            replace_dataset_metadata(server_url, api_token, doi, data)
            logging.debug("metadata changed")
        return True

    def log_files_unchanged(files):
        for file in files:
            file_id = file['dataFile']['id']
            value_ = {"DOI": doi,
                      "FileID": file_id,
                      "path": file_path(file),
                      "Modified": '',
                      "OldRestricted": file.get('restricted'),
                      "NewRestricted": file.get('restricted')}
            datafiles_writer.writerow(value_)
            logging.debug("no change {}".format(value_))

    def all_files_found_in_list(files_to_check, all_files):
        files_not_found = []
        for file in files_to_check:
            if file not in all_files:
                files_not_found.append(file)
        if len(files_not_found) > 0:
            logging.warning("files not found in list: {}".format(files_not_found))
            return False
        else:
            return True

    def get_filepaths(files):
        return list(map(lambda file: file_path(file), files))

    def file_to_file_meta_update(file, restricted_value):
        update = {
            "dataFileId": file['dataFile']['id'],
            "label": file['label'],
            "directoryLabel": file.get('directoryLabel', ""),
            "description": file.get('description', ""),
            "categories": file.get('categories', []),
            "provFreeForm": ""
        }
        # Setting restrict to the same value causes an error in the API
        if file.get('restricted') != restricted_value:
            update["restrict"] = restricted_value
        return update

    def change_files(restricted_value: bool, pid, files):
        if len(files) == 0:
            return False
        else:
            file_meta_updates = list(map(lambda file: file_to_file_meta_update(file, restricted_value), files))
            logging.info("changing {} files to restricted={}".format(len(files), restricted_value))
            if not dry_run:
                update_file_metas(server_url, api_token, pid, file_meta_updates)
            for file in files:
                file_id = file['dataFile']['id']
                value_ = {"DOI": doi,
                          "FileID": file_id,
                          "path": file_path(file),
                          "Modified": modified(),
                          "OldRestricted": file.get('restricted'),
                          "NewRestricted": restricted_value}
                logging.debug("updating dry_run={} {}".format(dry_run, value_))
                datafiles_writer.writerow(value_)
            return True

    try:
        resp_data = get_dataset_metadata(server_url, api_token, doi)
    except Exception as e:
        logging.warning("cannot get metadata for {}, skipping: {}".format(doi, str(e)))
        return

    if is_draft_dataset(server_url, api_token, doi):
        logging.warning("dataset is draft, skipping: {}".format(doi))
        return

    if not all_files_found_in_list(must_be_restricted, get_filepaths(resp_data['files'])):
        logging.warning("not all files found in list, skipping: {}".format(doi))
        return

    change_to_restricted = list(filter(
        lambda file: not file.get('restricted') and file_path(file) in must_be_restricted,
        resp_data['files']))
    change_to_accessible = list(filter(
        lambda file: file.get('restricted') and file_path(file) not in must_be_restricted,
        resp_data['files']))
    leave_unchanged = list(filter(
        lambda file: (file.get('restricted') and file_path(file) in must_be_restricted) or (not file.get('restricted') and file_path(file) not in must_be_restricted),
        resp_data['files']))
    log_files_unchanged(leave_unchanged)

    logging.info(
        "{} number of: files={}, must_be_restricted={}, change_to_restricted={}, change_to_accessible={}; fileAccessRequest={} termsOfAccess={}".format(
            doi, len(resp_data['files']), len(must_be_restricted), len(change_to_restricted), len(change_to_accessible),
            resp_data.get("fileAccessRequest"), resp_data.get("termsOfAccess")))
    #has_change_to_restricted = len(change_to_restricted) > 0
    has_change_to_accessible = len(change_to_accessible) > 0
    has_must_be_restricted = len(must_be_restricted) > 0
    dirty = False

    old_license_uri = resp_data['license']['uri']
    if old_license_uri != 'https://doi.org/10.17026/fp39-0x58':
        logging.warning(doi + ' does not have the DANS license but: ' + old_license_uri)
        return

    dirty = change_files(False, doi, change_to_accessible) or dirty
    new_terms_of_access = resp_data.get('termsOfAccess', "")
    if has_must_be_restricted:
        new_terms_of_access = "Not Available"
        data = access_json("termsOfAccess", new_terms_of_access)
        dirty = change_dataset_metadata(data) or dirty
    new_access_request = bool(resp_data['fileAccessRequest'])
    if bool(resp_data['fileAccessRequest']) and has_must_be_restricted:
        new_access_request = False
        data = access_json("fileRequestAccess", new_access_request)
        dirty = change_dataset_metadata(data) or dirty
    dirty = change_files(True, doi, change_to_restricted) or dirty
    if has_change_to_accessible and not has_must_be_restricted:
        new_terms_of_access = ""
        data = access_json("termsOfAccess", new_terms_of_access)
        dirty = change_dataset_metadata(data) or dirty

    if old_license_uri != new_license_uri:
        data = json.dumps({"http://schema.org/license": new_license_uri})
        dirty = change_dataset_metadata(data) or dirty
    row_to_write = {"DOI": doi, "Modified": modified(),
                    "OldLicense": old_license_uri,
                    "NewLicense": new_license_uri,
                    "OldRequestEnabled": resp_data['fileAccessRequest'],
                    "NewRequestEnabled": new_access_request,
                    "OldTermsOfAccess": resp_data.get('termsOfAccess', ""),
                    "NewTermsOfAccess": new_terms_of_access}
    logging.info('dirty={} {}'.format(dirty, row_to_write))
    if dirty:
        datasets_writer.writerow(row_to_write)
    if dirty and not dry_run:
        logging.info(doi + ' publish_dataset')
        publish_dataset(server_url, api_token, doi, 'updatecurrent')


def access_json(fieldName, value_):
    return json.dumps({
        "https://dataverse.org/schema/core#fileTermsOfAccess":
            {("https://dataverse.org/schema/core#" + fieldName): value_}
    })


def file_path(file_item):
    return re.sub("^/", "", file_item.get('directoryLabel', "") + "/" + file_item['label'])


def modified():
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")


def mdb_field_value(resp_data, metadata_block, field_name):
    return next(filter(lambda m: m['typeName'] == field_name,
                       resp_data['metadataBlocks'][metadata_block]['fields']
                       ))['value']


def main():
    config = init()
    parser = argparse.ArgumentParser(description='Change archeodepot dataset to open access')
    parser.add_argument('-d', '--datasets', dest='datasets',
                        help='CSV file (solr query result) header: DOI, ..., RIGHTS_HOLDER')
    parser.add_argument('-r', '--dag-rapporten', dest='dag_rapporten',
                        help='CSV file with header: dataset_id, DOI, File1, File2... N.B. The DOI is just the id, not a uri')
    parser.add_argument('-l', '--licenses', dest='licenses',
                        help='CSV file with: uri, name. N.B. no trailing slash for the uri')
    parser.add_argument('--delay', default=5.0,
                        help="Delay in seconds (publish does a lot after the asynchronous request is returning)")
    parser.add_argument('--dry-run', dest='dry_run', action='store_true',
                        help="only logs the actions, nothing is executed")

    args = parser.parse_args()
    open_access_archeodepot(args.datasets, args.licenses, args.dag_rapporten, config['dataverse'], args.dry_run,
                            float(args.delay))


if __name__ == '__main__':
    main()

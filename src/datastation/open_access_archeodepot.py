import argparse
import csv
import datetime
import json
import logging
import re
import os
from quopri import quote

from datastation.common.batch_processing import BatchProcessor
from datastation.common.config import init
from datastation.dv_api import publish_dataset, get_dataset_metadata, change_access_request, replace_dataset_metadata, \
    change_file_restrict


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
        ["DOI", "Modified", "OldLicense", "NewLicense", "OldRequestEnabled", "NewRequestEnabled"])
    datafiles_writer = create_csv(
        "datafiles", now,
        ["DOI", "FileID", "Modified", "OldRestricted", "NewRestricted"])
    processor.process_entries(
        doi_to_license_uri.items(),
        lambda doi_to_license: update_license(
            "doi:" + doi_to_license[0],
            doi_to_license[1].strip().strip('"'),
            doi_to_keep_restricted.get(to_key(doi_to_license[0]), []),
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
            key = to_key(row["rest"][-1].strip())
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
                                    fieldnames=["dataset_id", "DOI"], restkey="files")
        next(csv_reader)
        for row in csv_reader:
            doi_to_keep_restricted[to_key(row["DOI"])] = list(filter(lambda item: item != "", row["files"]))
    return doi_to_keep_restricted


def read_rights_holder_to_license(licenses_file):
    rights_holder_to_license_uri = {}
    with open(licenses_file, "r") as input_file_handler:
        csv_reader = csv.DictReader(input_file_handler, delimiter=',')
        for row in csv_reader:
            rights_holder_to_license_uri[to_key(row["RIGHTS_HOLDER"])] = row["URI"]
    return rights_holder_to_license_uri


def to_key(name):
    return re.sub("[^a-zA-Z0-1]", "_", name)


def update_license(doi, new_license_uri, must_be_restricted, server_url, api_token, dry_run, datasets_writer,
                   datafiles_writer):
    resp_data = get_dataset_metadata(server_url, api_token, doi)
    change_to_restricted = list(filter(
        lambda file: not file['restricted'] and file_path(file) in must_be_restricted,
        resp_data['files']))
    change_to_accessible = list(filter(
        lambda file: file['restricted'] and file_path(file) not in must_be_restricted,
        resp_data['files']))
    logging.info("number of: must_be_restricted={}, change_to_restricted={}, change_to_accessible={}; {}".format(
        len(must_be_restricted), len(change_to_restricted), len(change_to_accessible), must_be_restricted))
    has_change_to_restricted = len(change_to_restricted) > 0
    has_must_be_restricted = len(must_be_restricted) > 0
    if has_change_to_restricted and not resp_data.get("termsOfAccess", None):
        logging.warning("no terms of access, can't change files to restricted of {}".format(doi))
        return
    dirty = False
    if bool(resp_data['fileAccessRequest']) != has_must_be_restricted:
        dirty = True
        if not dry_run:
            change_access_request(server_url, api_token, doi, has_must_be_restricted)
    old_license_uri = resp_data['license']['uri']
    if old_license_uri != new_license_uri:
        dirty = True
        if not dry_run:
            data = json.dumps({"http://schema.org/license": new_license_uri})
            logging.debug("json {}".format(data))
            replace_dataset_metadata(server_url, api_token, doi, data)
    dirty = change_file(doi, True, change_to_restricted, server_url, api_token, datafiles_writer, dry_run) or dirty
    dirty = change_file(doi, False, change_to_accessible, server_url, api_token, datafiles_writer, dry_run) or dirty
    logging.info('dirty = {} fileAccessRequest = {}, license = {}, rightsHolder = {}, title = {}'
                 .format(dirty,
                         resp_data['fileAccessRequest'],
                         resp_data['license']['name'],
                         mdb_field_value(resp_data, 'dansRights', 'dansRightsHolder'),
                         mdb_field_value(resp_data, 'citation', 'title')))
    if dirty:
        datasets_writer.writerow({"DOI": doi, "Modified": modified(),
                                  "OldLicense": old_license_uri,
                                  "NewLicense": new_license_uri,
                                  "OldRequestEnabled": resp_data['fileAccessRequest'],
                                  "NewRequestEnabled": has_must_be_restricted})
    if dirty and not dry_run:
        logging.info(doi + ' publish_dataset')
        publish_dataset(server_url, api_token, doi, 'updatecurrent')


def file_path(file_item):
    return re.sub("^/", "", file_item.get('directoryLabel', "") + "/" + file_item['label'])


def change_file(doi, restricted_value: bool, files, server_url, api_token, datafiles_writer, dry_run):
    if len(files) == 0:
        return False
    else:
        for file_id in list(map(lambda file: file['dataFile']['id'], files)):
            value_ = {"DOI": doi, "FileID": file_id, "Modified": modified(),
                      "OldRestricted": not restricted_value,
                      "NewRestricted": restricted_value}
            datafiles_writer.writerow(value_)
            logging.debug("updating dry_run={} {}".format(dry_run, value_))
            if not dry_run:
                change_file_restrict(server_url, api_token, file_id, restricted_value)
                logging.debug("file changed")
        return True


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

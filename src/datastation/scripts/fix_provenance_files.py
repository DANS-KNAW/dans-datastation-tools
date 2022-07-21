import argparse
import csv
import hashlib
import logging
from lxml import etree
import os.path
import psycopg2
import re
import requests
import shutil
import sys
from datastation.config import init

provenance_element = 'provenance ' \
                     'xmlns:prov="http://easy.dans.knaw.nl/schemas/bag/metadata/prov/" ' \
                     'xsi:schemaLocation="http://easy.dans.knaw.nl/schemas/bag/metadata/prov/ https://easy.dans.knaw.nl/schemas/bag/metadata/prov/provenance.xsd" ' \
                     'xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" ' \
                     'xmlns:id-type="http://easy.dans.knaw.nl/schemas/vocab/identifier-type/" ' \
                     'xmlns:dcx-gml="http://easy.dans.knaw.nl/schemas/dcx/gml/" ' \
                     'xmlns:dcx-dai="http://easy.dans.knaw.nl/schemas/dcx/dai/" ' \
                     'xmlns:dcterms="http://purl.org/dc/terms/" xmlns:dc="http://purl.org/dc/elements/1.1/" ' \
                     'xmlns:abr="http://www.den.nl/standaard/166/Archeologisch-Basisregister/" ' \
                     'xmlns:ddm="http://easy.dans.knaw.nl/schemas/md/ddm/"'

provenance_schema_doc = etree.parse('http://easy.dans.knaw.nl/schemas/bag/metadata/prov/provenance.xsd')
provenance_xmlschema = etree.XMLSchema(provenance_schema_doc)


def validate(xml_path: str) -> bool:
    try:
        xml_doc = etree.parse(xml_path)
        provenance_xmlschema.assertValid(xml_doc)
        return True
    except etree.XMLSyntaxError as e:
        logging.error(e)
        return False
    except etree.DocumentInvalid as e:
        # info instead of error because we expect the old-provenance to be invalid
        logging.info(e)
        return False


def replace_provenance_tag(infile):
    old_provenance_tag = "<([a-zA-Z0-9_:]*)provenance .*>"
    new_str = '<\\1' + provenance_element + '>'
    replaced = False
    element_tag = ""
    line_list = []
    with open(infile) as f:
        for item in f:
            if not replaced and ("<prov:provenance" in item or len(element_tag) > 1):
                element_tag += item.strip('\n')
                if ">" in element_tag:
                    new_item = re.sub(old_provenance_tag, new_str, element_tag)
                    line_list.append(new_item)
                    replaced = True
                    element_tag = ""
            else:
                element_tag = ""
                line_list.append(item)

    with open(infile, "w") as f:
        f.truncate()
        for line in line_list:
            f.writelines(line)


def add_result(output_file, doi: str, storage_identifier: str, old_checksum: str, dvobject_id: str, status: str,
               new_checksum: str = None):
    if not new_checksum:
        new_checksum = old_checksum
    csvwriter = csv.writer(output_file)
    csvwriter.writerow([doi, storage_identifier, old_checksum, new_checksum, old_checksum is not new_checksum,
                        dvobject_id, status])
    output_file.flush()


def dv_file_validation(dv_api_token, dv_server_url, dvobject_id, dry_run_temp_file):
    if dry_run_temp_file:
        logging.info("dry run: no Dataverse file validation for {}".format(dvobject_id))
        return True

    headers = {'X-Dataverse-key': dv_api_token}
    dv_resp = requests.post(dv_server_url + '/api/admin/validateDataFileHashValue/' + dvobject_id,
                            headers=headers)
    dv_resp.raise_for_status()

    if dv_resp.json()['status'] == "OK":
        return True
    else:
        logging.error(dv_resp.json()["message"])
        return False


def calculate_checksum(file, dry_run_temp_file):
    if dry_run_temp_file:
        file_to_check = dry_run_temp_file
    else:
        file_to_check = file

    with open(file_to_check, 'rb') as f:
        new_checksum = hashlib.sha1()
        while True:
            chunk = f.read(16 * 1024)
            if not chunk:
                break
            new_checksum.update(chunk)
    return new_checksum.hexdigest()


def restore_old_provenance_file(provenance_path, dry_run_temp_file):
    if not dry_run_temp_file:
        shutil.copyfile(provenance_path + ".old-provenance", provenance_path)
        delete_old_provenance_file(provenance_path, dry_run_temp_file)


def make_new_provenance_file(provenance_path, dry_run_temp_file):
    if dry_run_temp_file:
        new_provenance_file = dry_run_temp_file
    else:
        new_provenance_file = provenance_path + ".new-provenance"
    logging.info("fixing {}".format(provenance_path))
    shutil.copyfile(provenance_path, new_provenance_file)
    replace_provenance_tag(new_provenance_file)
    return new_provenance_file


def replace_old_with_new_provenance_file(provenance_path, new_provenance_file, dry_run_temp_file):
    if not dry_run_temp_file:
        old_provenance_file = provenance_path + ".old-provenance"
        shutil.copyfile(provenance_path, old_provenance_file)
        shutil.move(new_provenance_file, provenance_path)


def update_dvndb_record(dry_run_temp_file, provenance_path, dvndb, output_file, doi, storage_identifier, old_checksum,
                        new_checksum, dvobject_id):
    update_statement = "update datafile set checksumvalue = '{}' where id = {} and checksumvalue='{}'"\
        .format(new_checksum, dvobject_id, old_checksum)
    logging.info(update_statement)
    if not dry_run_temp_file:
        try:
            dvndb_cursor = dvndb.cursor()
            dvndb_cursor.execute(update_statement)
        except psycopg2.DatabaseError as error:
            logging.error(error)
            add_result(output_file, doi=doi, storage_identifier=storage_identifier, old_checksum=old_checksum,
                       new_checksum=new_checksum, dvobject_id=dvobject_id, status="FAILED")
            restore_old_provenance_file(provenance_path, dry_run_temp_file)
            sys.exit("FATAL ERROR: problem updating dvndb record for {} and file {}".format(doi, dvobject_id))


def delete_old_provenance_file(provenance_path, dry_run_temp_file):
    if not dry_run_temp_file:
        os.remove(provenance_path + ".old-provenance")


def process_dataset(file_storage_root, doi, storage_identifier, current_checksum, dvobject_id: str,
                    dvndb, dv_server_url, dv_api_token, output_file, dry_run_file):
    logging.info("processing {} with file {}".format(doi, storage_identifier))
    provenance_path = os.path.join(file_storage_root, doi, storage_identifier)
    if not os.path.exists(provenance_path):
        sys.exit("FATAL ERROR: {} does not exist for doi {}".format(provenance_path, doi))

    with open(provenance_path) as provenance_file:
        if not is_provenance_xml_file(provenance_file):
            sys.exit("FATAL ERROR: {} for doi {} is not a provenance xml file".format(provenance_file, doi))

        if validate(provenance_path):
            logging.info("{} is already valid for doi {}".format(storage_identifier, doi))
            add_result(output_file, doi=doi, storage_identifier=storage_identifier,
                       old_checksum=current_checksum, dvobject_id=dvobject_id, status="OK")
            return

        new_provenance_file = make_new_provenance_file(provenance_path, dry_run_file)

        if not validate(new_provenance_file):
            add_result(output_file, doi=doi, storage_identifier=storage_identifier,
                       old_checksum=current_checksum, dvobject_id=dvobject_id, status="FAILED")
            sys.exit("FATAL ERROR: new provenance file not valid for {} at {}".format(doi, new_provenance_file))

        new_checksum = calculate_checksum(new_provenance_file, dry_run_file)
        replace_old_with_new_provenance_file(provenance_path, new_provenance_file, dry_run_file)
        update_dvndb_record(dry_run_file, provenance_path, dvndb, output_file, doi=doi,
                            storage_identifier=storage_identifier, old_checksum=current_checksum,
                            new_checksum=new_checksum, dvobject_id=dvobject_id)

        if dv_file_validation(dv_api_token, dv_server_url, dvobject_id, dry_run_file):
            logging.info("Successfully fixed {}".format(doi))
            delete_old_provenance_file(provenance_path, dry_run_file)
            add_result(output_file, doi=doi, storage_identifier=storage_identifier,
                       old_checksum=current_checksum,
                       new_checksum=new_checksum, dvobject_id=dvobject_id, status="OK")
        else:
            add_result(output_file, doi=doi, storage_identifier=storage_identifier,
                       old_checksum=current_checksum,
                       new_checksum=new_checksum, dvobject_id=dvobject_id, status="FAILED")
            # rollback the database update, resetting the original checksum
            update_dvndb_record(dry_run_file, provenance_path, dvndb, output_file, doi=doi,
                                storage_identifier=storage_identifier, old_checksum=new_checksum,
                                new_checksum=current_checksum, dvobject_id=dvobject_id)
            restore_old_provenance_file(provenance_path, dry_run_file)
            sys.exit("FATAL ERROR: Dataverse file validation failed for new provenance file with id {} of {}"
                     .format(dvobject_id, doi))


def is_provenance_xml_file(provenance_file):
    # don't parse the file as xml, just check the first line and tag
    xml_prolog_found = provenance_file.readline().rstrip() == '<?xml version="1.0" encoding="UTF-8"?>'
    if xml_prolog_found:
        line_count = 0
        for line in provenance_file:
            if "<prov:provenance" in line:
                return True
            if line_count > 2:
                return False
            line_count += 1
    return False


def connect_to_database(user: str, password: str):
    return psycopg2.connect(
        host="localhost",
        database="dvndb",
        user=user,
        password=password)


def main():
    config = init()

    parser = argparse.ArgumentParser(
        description='Fixes one or more invalid provenance.xml files. With the optional parameters, it is possible to process one dataset/provenance.xml.'
                    + ' If none of the optional parameters is provided the input-file is expected to contain a CSV file with the columns: doi, storage_identifier, current_sha1_checksum and dvobject_id')
    parser.add_argument('-d', '--doi', dest='doi', help='the dataset DOI')
    parser.add_argument('-s', '--storage-identifier', dest='storage_identifier',
                        help='the storage identifier of the provenance.xml file')
    parser.add_argument('-c', '--current-sha1-checksum', dest='current_sha1_checksum',
                        help='the expected current checksum of the provenance.xml file')
    parser.add_argument('-o', '--dvobject-id', dest='dvobject_id',
                        help='the dvobject.id for the provenance.xml in dvndb')
    parser.add_argument('-u', '--user', dest='dvndb_user', help="dvn user with update privileges on 'datafile'")
    parser.add_argument('-p', '--password', dest='dvndb_password', help="password for dvn user")
    parser.add_argument('-l', '--log', dest='output_csv', help='the csv log file with the result per doi',
                        default='fix-provenance-output.csv')
    parser.add_argument('-r', '--dryrun', dest='dryrun', help="only logs the actions, nothing is executed",
                        action='store_true')
    parser.add_argument('-i', '--input-file', dest='input_file',
                        help="csv file with columns: doi, storage_identifier, current_sha1_checksum and dvobject_id")
    args = parser.parse_args()

    dvndb_conn = None
    try:
        dry_run_provenance_file_path = None
        if args.dryrun:
            dry_run_provenance_file_path = "dry-run-provenance-file.xml"
            logging.info("--- DRY RUN, using {} for the temporary provenance file ---"
                         .format(dry_run_provenance_file_path))
        else:
            if not (args.dvndb_password and args.dvndb_user):
                sys.exit("please provide dvndb user and password when not in dry-run mode")
            dvndb_conn = connect_to_database(args.dvndb_user, args.dvndb_passwd)

        with open(args.output_csv, "w") as output_csv:
            logging.info("writing to " + args.output_csv)
            headers = ["doi", "storage_identifier", "old_checksum", "new_checksum", "updated", "dvobject_id", "status"]
            csv_writer = csv.DictWriter(output_csv, headers)
            csv_writer.writeheader()
            if args.input_file:
                with open(args.input_file, "r") as input_file_handler:
                    csv_reader = csv.DictReader(input_file_handler, delimiter=',')
                    line_count = 0
                    for row in csv_reader:
                        if line_count > 0:
                            process_dataset(file_storage_root=config['dataverse']['files_root'], doi=row["doi"],
                                            storage_identifier=row["storage_identifier"],
                                            current_checksum=row["current_sha1_checksum"],
                                            dvobject_id=row["dvobject_id"],
                                            dvndb=dvndb_conn, dv_server_url=config['dataverse']['server_url'],
                                            dv_api_token=config['dataverse']['api_token'],
                                            output_file=output_csv, dry_run_file=dry_run_provenance_file_path)
                        line_count += 1
            else:
                process_dataset(file_storage_root=config['dataverse']['files_root'], doi=args.doi,
                                storage_identifier=args.storage_identifier,
                                current_checksum=args.current_sha1_checksum, dvobject_id=args.dvobject_id,
                                dvndb=dvndb_conn,
                                dv_server_url=config['dataverse']['server_url'],
                                dv_api_token=config['dataverse']['api_token'], output_file=output_csv,
                                dry_run_file=dry_run_provenance_file_path)
        output_csv.close()
        if not args.dryrun:
            dvndb_conn.close()
    except psycopg2.DatabaseError as error:
        logging.error(error)
    finally:
        if dvndb_conn is not None:
            dvndb_conn.close()


if __name__ == '__main__':
    main()

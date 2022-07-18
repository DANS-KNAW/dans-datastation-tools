import argparse
import logging
import os.path
from datastation.config import init
from lxml import etree
import shutil
import re
import hashlib
import csv
import fileinput
import psycopg2
import requests

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
output_list = []


def validate(xml_path: str, xsd_path: str) -> bool:
    xmlschema_doc = etree.parse(xsd_path)
    xmlschema = etree.XMLSchema(xmlschema_doc)
    try:
        xml_doc = etree.parse(xml_path)
        result = xmlschema.validate(xml_doc)
        return result
    except etree.XMLSyntaxError:
        return False


def replace_provenance_tag(infile):
    old_str = "<([a-zA-Z0-9_:]*)provenance.*>"
    new_str = '<\\1' + provenance_element + '>'
    replaced = False
    element_tag = ""
    line_list = []
    with open(infile) as f:
        for item in f:
            if not replaced and ("<prov:provenance" in item or len(element_tag) > 1):
                element_tag += item.strip('\n')
                if ">" in element_tag:
                    new_item = re.sub(old_str, new_str, element_tag)
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


def add_result(doi: str, storage_identifier: str, old_checksum: str, dvobject_id: str, status: str,
               new_checksum: str = None):
    if not new_checksum:
        new_checksum = old_checksum
    output_list.append({
        "doi": doi,
        "storage_identifier": storage_identifier,
        "old_checksum": old_checksum,
        "new_checksum": new_checksum,
        "updated": old_checksum is not new_checksum,
        "dvobject_id": dvobject_id,
        "status": status
    })


def dv_file_validation(dv_api_token, dv_server_url, dvobject_id):
    # curl -H X-Dataverse-key:$API_TOKEN -X POST $SERVER_URL/api/admin/validateDataFileHashValue/{fileId}
    headers = {'X-Dataverse-key': dv_api_token}
    dv_resp = requests.post(dv_server_url + '/api/admin/validateDataFileHashValue/' + dvobject_id,
                            headers=headers)
    dv_resp.raise_for_status()
    # {"status":"OK","data":{"message":"Datafile validation complete for 10. The hash value is: 8d3ba34a32eac79232156da4974536f0405689ea"}}
    # {"status":"ERROR","message":"Datafile validation failed for 10. The saved hash value is: 7143025623d4807a1f61a4a8ce1faf1bdc87a66c while the recalculated hash value for the stored file is: 8d3ba34a32eac79232156da4974536f0405689ea"}

    if dv_resp.json()['status'] is "OK":
        return True
    else:
        print(dv_resp.json()["data"]["message"])
        return False


def process_dataset(file_storage_root, doi, storage_identifier, current_checksum, dvobject_id: str,
                    dvndb, dv_server_url, dv_api_token, output_file):
    logging.debug("({}, {}, {}, {})".format(doi, storage_identifier, current_checksum, dvobject_id))
    provenance_path = os.path.join(file_storage_root, doi, storage_identifier)
    if os.path.exists(provenance_path):
        provenance_file = open(provenance_path)
        if is_xml_file(provenance_file):
            if validate(provenance_path, file_storage_root + '/provenance.xsd'):
                add_result(doi=doi, storage_identifier=storage_identifier, old_checksum=current_checksum,
                           dvobject_id=dvobject_id, status="OK")
                return
            new_provenance_file = provenance_path + ".new-provenance"
            shutil.copyfile(provenance_path, new_provenance_file)
            replace_provenance_tag(new_provenance_file)
            if not validate(new_provenance_file, file_storage_root + '/provenance.xsd'):
                add_result(doi=doi, storage_identifier=storage_identifier, old_checksum=current_checksum,
                           dvobject_id=dvobject_id, status="FAILED")
                write_output(output_file)
                exit(-1)
            with open(new_provenance_file, 'rb') as f:
                new_checksum = hashlib.sha1()
                while True:
                    chunk = f.read(16 * 1024)
                    if not chunk:
                        break
                    new_checksum.update(chunk)

            old_provenance_file = provenance_path + ".old-provenance"
            shutil.copyfile(provenance_path, old_provenance_file)
            shutil.move(new_provenance_file, provenance_path)
            # TODO: update dvndb
            try:
                dvndb_cursor = dvndb.cursor()
                update_statement = "update datafile set checksumvalue = \'{}\' where id = {} and checksumvalue=\'{}\'"\
                    .format(new_checksum.hexdigest(), dvobject_id, current_checksum)
                logging.debug(update_statement)
                dvndb_cursor.execute(update_statement)
            except(Exception, psycopg2.DatabaseError) as error:
                print(error)
                add_result(doi=doi, storage_identifier=storage_identifier, old_checksum=current_checksum,
                           new_checksum=new_checksum.hexdigest(), dvobject_id=dvobject_id, status="FAILED")
                return

            status = "FAILED"
            # TODO: physical file validation of dataset, call to dataverse api
            if dv_file_validation(dv_api_token, dv_server_url, dvobject_id):
                status = "OK"
                # TODO: delete .old-provenance file

            add_result(doi=doi, storage_identifier=storage_identifier, old_checksum=current_checksum,
                       new_checksum=new_checksum.hexdigest(), dvobject_id=dvobject_id, status=status)


def is_xml_file(provenance_file):
    # don't parse the file as xml, just check the first line
    return provenance_file.readline().rstrip() == '<?xml version="1.0" encoding="UTF-8"?>'


def write_output(file: str):
    logging.debug("writing to " + file)
    with open(file, "w") as output_csv:
        csv_writer = csv.DictWriter(output_csv, output_list[0].keys())
        csv_writer.writeheader()
        csv_writer.writerows(output_list)


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
                    + ' If none of the optional parameters is provided the standard input is expected to contain a CSV file with the columns: DOI, STORAGE_IDENTIFIER, CURRENT_SHA1_CHECKSUM and DVOBJECT_ID')
    parser.add_argument('-d', '--doi', dest='doi', help='the dataset DOI')
    parser.add_argument('-s', '--storage-identifier', dest='storage_identifier',
                        help='the storage identifier of the provenance.xml file')
    parser.add_argument('-c', '--current-sha1-checksum', dest='current_sha1_checksum',
                        help='the expected current checksum of the provenance.xml file')
    parser.add_argument('-o', '--dvobject-id', dest='dvobject_id',
                        help='the dvobject.id for the provenance.xml in dvndb')
    parser.add_argument('-u', '--user', dest='dvndb_user', help="dvn user with update privileges on 'datafile'",
                        default="dvn_prov_user")
    parser.add_argument('-l', '--log', dest='output_csv', help='the csv log file with the result per doi',
                        default='fix-provenance-output.csv')
    args = parser.parse_args()
    dvndb_passwd = input("Enter password for user {}:".format(args.dvndb_user))

    try:
        conn = connect_to_database(args.dvndb_user, dvndb_passwd)

        if not (args.doi and args.storage_identifier):
            line_count = 0
            for line in fileinput.input():
                row = line.rstrip().split(",")
                if line_count > 0:
                    process_dataset(config['dataverse']['files_root'], row[0], row[1], row[2], row[3],
                                    conn, config['dataverse']['server_url'], config['dataverse']['api_token'],
                                    args.output_csv)
                line_count += 1
        else:
            process_dataset(config['dataverse']['files_root'], args.doi, args.storage_identifier,
                            args.current_sha1_checksum, args.dvobject_id, conn, config['dataverse']['server_url'],
                            config['dataverse']['api_token'],args.output_csv)
            write_output(args.output_csv)
            conn.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()



if __name__ == '__main__':
    main()

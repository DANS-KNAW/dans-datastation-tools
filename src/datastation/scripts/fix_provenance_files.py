import argparse
import sys
import logging
import os.path
from datastation.config import init
from lxml import etree
import shutil
import re
import hashlib
import csv
import fileinput


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


def write_output(doi: str, storage_identifier: str, old_checksum: str, new_checksum: str, updated: bool,
                 dvobject_id: str, status: str):
    print("{}, {}, {}, {}, {}, {}, {}", doi, storage_identifier, old_checksum, new_checksum, updated,
          dvobject_id, status)


def process_dataset(file_storage_root, doi, storage_identifier, current_checksum, dvobject_id):
    logging.debug("({}, {}, {}, {})".format(doi, storage_identifier, current_checksum, dvobject_id))
    provenance_path = os.path.join(file_storage_root, doi, storage_identifier)
    if os.path.exists(provenance_path):
        provenance_file = open(provenance_path)
        if is_xml_file(provenance_file):
            if validate(provenance_path, file_storage_root+'/provenance.xsd'):
                write_output(doi=doi, storage_identifier=storage_identifier, old_checksum=current_checksum,
                             new_checksum=current_checksum, updated=False, dvobject_id=dvobject_id, status="OK")
                return
            # copy provenance_path to <storageIdentifier>.new-provenance
            new_provenance_file = provenance_path+".new-provenance"
            shutil.copyfile(provenance_path, new_provenance_file)
            replace_provenance_tag(new_provenance_file)
            if not validate(new_provenance_file, file_storage_root + '/provenance.xsd'):
                write_output(doi=doi, storage_identifier=storage_identifier, old_checksum=current_checksum,
                             new_checksum=current_checksum, updated=False, dvobject_id=dvobject_id, status="FAILED")
                exit(-1)
            with open(new_provenance_file, 'rb') as f:
                new_checksum = hashlib.sha1()
                while True:
                    chunk = f.read(16 * 1024)
                    if not chunk:
                        break
                    new_checksum.update(chunk)

                print("SHA1: %s" % new_checksum.hexdigest())
            old_provenance_file = provenance_path + ".old-provenance"
            shutil.copyfile(provenance_path, old_provenance_file)
            shutil.move(new_provenance_file, provenance_path)
            # TODO: update dvndb
            logging.info("update datafile set checksumvalue = '{}' where id = {} and checksumvalue='{}'", new_checksum,
                         dvobject_id, current_checksum)
            # TODO: physical file validation of dataset
            write_output(doi=doi, storage_identifier=storage_identifier, old_checksum=current_checksum,
                         new_checksum=new_checksum.hexdigest(), updated=True, dvobject_id=dvobject_id, status="OK")


def is_xml_file(provenance_file):
    # don't parse the file as xml, just check the first line
    return provenance_file.readline().rstrip() == '<?xml version="1.0" encoding="UTF-8"?>'


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
    args = parser.parse_args()

    if not (args.doi and args.storage_identifier):
        linecount = 0
        for line in fileinput.input():
            print(line.rstrip())
            row = line.split(",")
            if linecount > 0:
                process_dataset(config['dataverse']['files_root'], row[0], row[1], row[2], row[3])
            linecount += 1
    else:
        process_dataset(config['dataverse']['files_root'], args.doi, args.storage_identifier,
                        args.current_sha1_checksum, args.dvobject_id)


if __name__ == '__main__':
    main()

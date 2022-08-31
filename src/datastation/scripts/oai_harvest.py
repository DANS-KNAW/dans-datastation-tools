import argparse
import logging
import os

from datetime import datetime
from lxml import etree

from datastation.config import init
from datastation.dv_api import get_oai_records, get_oai_records_resume


def save_oai_records(xml_doc, counter, records_output_dir):
    # Improve the next
    # count number of records and print it
    # print(len(xml_doc.findall('.//{http://www.openarchives.org/OAI/2.0/}record')))

    xml_filename = 'recordset_' + str(counter) + '.xml'
    f = open(os.path.join(records_output_dir, xml_filename), "wb")
    # Note that we don't have the XML declaration
    f.write(etree.tostring(xml_doc, pretty_print=True))
    f.close()
    # Note: maybe extract the dataset specific parts and save as individual xml files?


def oai_harvest_command(server_url, output_dir, format, set=None):
    # create directory for this export
    timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    harvest_dirname = 'oai_harvest_' + format + '_' + timestamp_str
    records_output_dir = os.path.join(output_dir, harvest_dirname)
    os.makedirs(records_output_dir)

    logging.info("Harvesting {} from {} in format {}".format(set, server_url, format))
    logging.info("Storing results in {}".format(os.path.abspath(records_output_dir)))
    xml_doc = get_oai_records(server_url, format=format, set=set)

    counter = 0
    save_oai_records(xml_doc, counter, records_output_dir)

    # get the resumptionToken, sax would be more efficient than DOM here!
    # OAI-PMH/ListRecords/resumptionToken
    token = xml_doc.find('.//{http://www.openarchives.org/OAI/2.0/}resumptionToken')

    # The resumptionToken is empty (no text) when we have all the records
    while token is not None and token.text is not None:
        logging.info("In page {} of the recordset, resumption token found: {}".format(counter, token.text))
        xml_doc = get_oai_records_resume(server_url, token.text)
        counter += 1
        save_oai_records(xml_doc, counter, records_output_dir)
        token = xml_doc.find('.//{http://www.openarchives.org/OAI/2.0/}resumptionToken')


def main():
    config = init()
    parser = argparse.ArgumentParser(description='Harvest dataset metadata records via the OAI-PMH protocol')
    parser.add_argument('-f', '--format', default='oai_dc',
                        help='The PMH metadataPrefix in which the metadata should be harvested.')
    parser.add_argument('-s', '--set', default='', help='The PMH recordset to be harvested.')
    parser.add_argument('-o', '--output-dir', dest='output_dir', required=True,
                        help="The folder to store the exported metadata records")
    args = parser.parse_args()

    oai_format = args.format  # Note that an important one we have in dataverse is 'oai_datacite'
    oai_set = None
    if args.set:
        oai_set = args.set

    server_url = config['dataverse']['server_url']

    oai_harvest_command(server_url, args.output_dir, format=oai_format, set=oai_set)


if __name__ == '__main__':
    main()

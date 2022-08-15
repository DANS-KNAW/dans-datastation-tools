import argparse
import json
import logging

from datastation.config import init
from datastation.dv_api import get_dataset_metadata


def retrieve_dataset_metadata_field(server_url, api_token, pid, mdb_name, field_name):
    resp_data = get_dataset_metadata(server_url, api_token, pid)
    mdb_fields = resp_data['metadataBlocks'][mdb_name]['fields']
    found = False
    for field in mdb_fields:
        if field['typeName'] == field_name:
            print(json.dumps(field))
            found = True
    if not found:
        logging.info("No field {} found in {} for dataset {}".format(field_name, mdb_name, pid))


def main():
    config = init()
    parser = argparse.ArgumentParser(description='Retrieves the json for the given field for a dataset')
    parser.add_argument('-p', '--pid', help='The pid of the dataset', required=True)
    parser.add_argument('-m', '--metadata_block',
                        help='The metadata_block that holds the target field')
    parser.add_argument('-f', '--field', required=True, help='The target field')
    args = parser.parse_args()

    server_url = config['dataverse']['server_url']
    api_token = config['dataverse']['api_token']
    retrieve_dataset_metadata_field(server_url, api_token, args.pid, args.metadata_block, args.field)


if __name__ == '__main__':
    main()
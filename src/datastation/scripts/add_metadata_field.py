import argparse
import logging
import json
import sys

from datastation.batch_processing import batch_process
from datastation.config import init
from datastation.ds_pidsfile import load_pids

from datastation.dv_api import edit_dataset_metadatafield, get_dataset_metadata


def add_metadata_field_value(server_url, api_token, pid, field_to_add):
    resp_data = get_dataset_metadata(server_url, api_token, pid)

    for metadata_block in resp_data['metadataBlocks']:
        mdb_fields = resp_data['metadataBlocks'][metadata_block]['fields']

        for field in mdb_fields:
            if field['typeName'] == field_to_add['typeName']:
                logging.info("Field {} already exists, adding new value to existing value {}"
                         .format(field_to_add['typeName'], field['value']))
    logging.debug("{}: Try adding field {} with value {}".format(pid, field_to_add['typeName'], field_to_add['value']))
    adding_fields = {'fields': [field_to_add]}
    logging.debug(json.dumps(adding_fields))
    edit_dataset_metadatafield(server_url, api_token, pid, adding_fields, False)
    return True


def add_metadata_field_value_command(server_url, api_token, delay, pids_file, field_to_add):
    pids = load_pids(pids_file)

    batch_process(pids,
                  lambda pid: add_metadata_field_value(server_url, api_token, pid, field_to_add), delay)


# Note that the datasets that got changed get into a DRAFT status
# and at some point need to be published with a minor version increment.
# This is not done here, because you might want several (other) changes
# on the same datasets before publishing.
def main():
    config = init()
    parser = argparse.ArgumentParser(
        description='Add metadata field in datasets with the dois in the given input file. See the json metadata '
                    'export (dataverse_json) to see what names are possible for the fields and metadata blocks. The '
                    'field must have typeClass=`primitive` or `controlledVocabulary`. If the field is already present, '
                    'the value is added to the existing values.')

    parser.add_argument("-n", "--field-name", help="Name of the primitive field (json typeName)", dest="field_name")
    parser.add_argument("-v", "--field-value", help="The replacement value (the new value)", dest="field_value")
    parser.add_argument('-m', '--multiple', help="'true' if the field can have multiple values, 'false' otherwise",
                        dest='field_multiple')
    parser.add_argument('-d', '--datasets', dest='pids_file', help='The input file with the dataset dois')
    parser.add_argument('-p', '--pid', help="Doi of the dataset for which to replace the metadata.")
    parser.add_argument('--delay', default=5.0, help="Delay in seconds")
    args = parser.parse_args()

    server_url = config['dataverse']['server_url']
    api_token = config['dataverse']['api_token']

    value = args.field_value if args.field_multiple == "false" else [ args.field_value ]
    field_to_add = {
        "typeName": args.field_name,
        "value": value
    }

    if args.pid is not None:
        add_metadata_field_value(server_url, api_token, args.pid, field_to_add)
    else:
        add_metadata_field_value_command(server_url, api_token, args.delay, args.pids_file, field_to_add)


if __name__ == '__main__':
    main()

import argparse
import logging
import json
import sys

from datastation.batch_processing import batch_process
from datastation.config import init
from datastation.ds_pidsfile import load_pids

from datastation.dv_api import replace_dataset_metadatafield, get_dataset_metadata


def replace_metadatafield(server_url, api_token, pid, updated_field):
    logging.debug("{}: Try updating it with: {}".format(pid, updated_field['value']))
    updated_fields = {'fields': [updated_field]}
    logging.debug(json.dumps(updated_fields))
    replace_dataset_metadatafield(server_url, api_token, pid, updated_fields)
    return True


def replace_metadata_field_value(server_url, api_token, pid, field_name, field_from_value, field_to_value):
    # Getting the metadata is not always needed when doing a replacement,
    # but when you need to determine if replace is needed by inspecting the current content
    # you need to 'get' it first.
    # Another approach would be to do that selection up front (via search)
    # and have that generate a list with pids to process 'blindly'.
    resp_data = get_dataset_metadata(server_url, api_token, pid)
    # print(resp_data['datasetPersistentId'])

    replace_field = field_name
    replace_from = field_from_value
    replace_to = field_to_value
    found_replace_from = False
    replaced = False

    for metadata_block in resp_data['metadataBlocks']:
        mdb_fields = resp_data['metadataBlocks'][metadata_block]['fields']

        for field in mdb_fields:
            # expecting (assuming) one and only one instance,
            # but the code will try to change all it can find
            if field['typeName'] == replace_field:
                logging.debug("{}: Found {} with value {} ".format(pid, field['typeName'],  field['value']))
                if not field['typeClass'] == 'primitive':
                    sys.exit("field {} does not have typeClass=`primitive` but {}".format(replace_field, field['typeClass']))
                # be safe and mutate a copy
                updated_field = field.copy()
                if field['multiple'] == 'true':
                    try:
                        index_replace_from = field['value'].index(replace_from)
                        found_replace_from = True
                        updated_field['value'][index_replace_from] = replace_to
                        replaced = replace_metadatafield(server_url, api_token, pid, updated_field)
                        logging.debug("{}: Updated {} from {} to {}".format(pid, replace_field, replace_from, replace_to))
                    except ValueError:
                        logging.debug("{} not found in {}".format(replace_from, field['value']))
                else:
                    if field['value'] == replace_from:
                        found_replace_from = True
                        updated_field['value'] = replace_to
                        replaced = replace_metadatafield(server_url, api_token, pid, updated_field)
                        logging.debug("{}: Updated {} from {} to {}".format(pid, replace_field, replace_from, replace_to))
                    else:
                        logging.debug("Found {} instead of {}, Leave as-is".format(field['value'], replace_from))
    if not found_replace_from:
        logging.debug("{}: {} not found, nothing to replace".format(pid, replace_field))
    return replaced


def replace_metadata_field_value_command(server_url, api_token, delay, pids_file, field_name,
                                         field_from_value, field_to_value):
    pids = load_pids(pids_file)

    batch_process(pids,
                  lambda pid: replace_metadata_field_value(server_url, api_token, pid, field_name,
                                                                  field_from_value,  field_to_value), delay)


# Note that the datasets that got changed get into a DRAFT status
# and at some point need to be published with a minor version increment.
# This is not done here, because you might want several (other) changes
# on the same datasets before publishing.
def main():
    config = init()
    parser = argparse.ArgumentParser(
        description='Replace metadata field in datasets with the dois in the given input file. See the json metadata '
                    'export (dataverse_json) to see what names are possible for the fields and metadata blocks. The '
                    'field must have typeClass=`primitive`. The field must already be present.')

    parser.add_argument("-n", "--field-name", help="Name of the primitive field (json typeName)", dest="field_name")
    parser.add_argument("-f", "--from-value", help="Value to be replaced", dest="field_from_value")
    parser.add_argument("-t", "--to-value", help="The replacement value (the new value)", dest="field_to_value")
    parser.add_argument('-d', '--datasets', dest='pids_file', help='The input file with the dataset dois')
    parser.add_argument('-p', '--pid', help="Doi of the dataset for which to replace the metadata.")
    parser.add_argument('--delay', default=5.0, help="Delay in seconds")
    args = parser.parse_args()

    server_url = config['dataverse']['server_url']
    api_token = config['dataverse']['api_token']

    if args.pid is not None:
        replace_metadata_field_value(server_url, api_token, args.pid, args.field_name,
                                      args.field_from_value, args.field_to_value)
    else:
        replace_metadata_field_value_command(server_url, api_token, args.delay, args.pids_file,
                                             args.field_name, args.field_from_value, args.field_to_value)


if __name__ == '__main__':
    main()

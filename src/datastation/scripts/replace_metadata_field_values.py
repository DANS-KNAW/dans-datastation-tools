import argparse
import logging
import json

from datastation.batch_processing import batch_process
from datastation.config import init
from datastation.ds_pidsfile import load_pids

from datastation.dv_api import replace_dataset_metadatafield, get_dataset_metadata


def replace_metadata_field_value_action(server_url, api_token, pid, mdb_name, field_name, field_from_value,
                                        field_to_value):
    """

    :param server_url: configured in yml
    :param api_token: configured in yml
    :param pid:
    :param mdb_name: name of the metadata block
    :param field_name: name of the field to be changed
    :param field_from_value: old value
    :param field_to_value: new value
    :return:
    """
    # Getting the metadata is not always needed when doing a replacement,
    # but when you need to determine if replace is needed by inspecting the current content
    # you need to 'get' it first.
    # Another approach would be to do that selection up front (via search)
    # and have that generate a list with pids to process 'blindly'.
    resp_data = get_dataset_metadata(server_url, api_token, pid)
    # print(resp_data['datasetPersistentId'])
    mdb_fields = resp_data['metadataBlocks'][mdb_name]['fields']
    # print(json.dumps(mdb_fields, indent=2))

    # metadata field replacement (an idempotent action I think)
    # replace replace_from with replace_to for field with typeName replace_field
    replace_field = field_name
    replace_from = field_from_value
    replace_to = field_to_value

    replaced = False
    found_field_name = False
    for field in mdb_fields:
        # expecting (assuming) one and only one instance,
        # but the code will try to change all it can find
        if field['typeName'] == replace_field:
            logging.debug("{}: Found {} with value {} ".format(pid, field['typeName'],  field['value']))
            updated_field = field.copy()
            if field['value'] == replace_from:
                found_field_name = True
                # be safe and mutate a copy
                updated_field['value'] = replace_to
                logging.debug("{}: Try updating it with: {}" .format(pid, updated_field['value']))
                updated_fields = {'fields': [updated_field]}
                logging.debug(json.dumps(updated_fields))
                replace_dataset_metadatafield(server_url, api_token, pid, updated_fields)
                logging.debug("{}: Updated {} from {} to {}".format(pid, replace_field, replace_from, replace_to))
                replaced = True
            else:
                logging.debug("Found {} instead of {}, Leave as-is".format(field['value'], replace_from))
    if not found_field_name:
        logging.debug("{}: {} not found, nothing to replace".format(pid, replace_field))
    return replaced


def replace_metadata_field_value_command(server_url, api_token, pids_file, mdb_name, field_name,
                                         field_from_value, field_to_value):
    pids = load_pids(pids_file)

    batch_process(pids,
                  lambda pid: replace_metadata_field_value_action(server_url, api_token, pid, mdb_name, field_name,
                                                                  field_from_value,  field_to_value),
                  output_file=None, delay=5.0)


# Note that the datasets that got changed get into a DRAFT status
# and at some point need to be published with a minor version increment.
# This is not done here, because you might want several (other) changes
# on the same datasets before publishing.
def main():
    config = init()
    parser = argparse.ArgumentParser(
        description='Replace metadata field in datasets with the dois in the given input file. See the json metadata '
                    'export (dataverse_json) to see what names are possible for the fields and metadata blocks. The '
                    'field must have `multiple=false` and typeClass=`primitive`. The field must already be present.')
    parser.add_argument("-m", "--metadata-block", help="Name of the metadata block", dest="mdb_name")
    parser.add_argument("-n", "--field-name", help="Name of the field (json typeName)", dest="field_name")
    parser.add_argument("-f", "--from-value", help="Value to be replaced", dest="field_from_value")
    parser.add_argument("-t", "--to-value", help="The replacement value (the new value)", dest="field_to_value")
    parser.add_argument('-i', '--input-file', dest='pids_file', help='The input file with the dataset dois')
    args = parser.parse_args()

    server_url = config['dataverse']['server_url']
    api_token = config['dataverse']['api_token']

    replace_metadata_field_value_command(server_url, api_token, args.pids_file, args.mdb_name, args.field_name,
                                         args.field_from_value, args.field_to_value)


if __name__ == '__main__':
    main()

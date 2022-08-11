import argparse
import os
import requests
import datastation.dv_api as dv_api
import logging
import json

from datetime import datetime
from datastation.batch_processing import batch_process
from datastation.config import init
from datastation.ds_metadatafile import construct_filename_base_from_pid
from datastation.ds_pidsfile import load_pids


def convert_dataset_metadata_action(server_url, pid, input_dir, output_dir):
    # find input metadata file
    input_filename_ext = 'json'  # assume json, but maybe detect this?
    input_full_name = os.path.join(input_dir, construct_filename_base_from_pid(pid) + '.' + input_filename_ext)
    if not os.path.isfile(input_full_name):
        logging.info("{} not found, retrieving from dataverse".format(input_full_name))
        json_data = dv_api.get_dataset_metadata_export(server_url, pid, exporter='dataverse_json', response_is_json=True)
        print(json_data)
        json_object = json.dumps(json_data, indent=4)
        with open(input_full_name, "w") as f:
            f.write(json_object)
            f.flush()
    #
    # TODO implement conversion with XSLT file (maybe with Saxon-HE ?) or with jq and write to output dir


def retrieve_json(dv_pid, dv_server_url):
    dv_resp = requests.get(dv_server_url + '/api/datasets/:persistentId/?persistentId=' + dv_pid)
    dv_resp.raise_for_status()


def convert_dataset_metadata_command(config, pids_file, input_dir, output_dir):
    print('Args: ' + input_dir + ',  ' + output_dir)
    print("Example using server URL: " + config['dataverse']['server_url'])

    # create input dir if not exists!
    if os.path.isdir(input_dir):
        logging.info("Found metadata_dir, assuming json files are here: {}".format(input_dir))
    else:
        logging.info("Creating metadata_dir for original json files: {}".format(input_dir))
        os.makedirs(input_dir)

    # create output dir if not exists!
    if os.path.isdir(output_dir):
        logging.info("Skipping dir creation, because it already exists: " + output_dir)
    else:
        logging.info("Creating output dir: " + output_dir)
        os.makedirs(output_dir)

    pids = load_pids(pids_file)
    timestamp_str = '_' + datetime.now().strftime("%Y%m%d_%H%M%S")
    output_pid_file = "out{}.txt".format(timestamp_str)

    batch_process(pids, lambda pid: convert_dataset_metadata_action(config['dataverse']['server_url'], pid, input_dir,
                                                                    output_dir), output_pid_file, delay=0.0)


def main():
    config = init()
    parser = argparse.ArgumentParser(
        description='Retrieves the metadata for all published datasets with the pids in the given input file and stores'
                    ' it in the metadata_dir. The transformed metadata is then stored in the converted_dir.')
    parser.add_argument('-i', '--input-file', help='The input file with the dataset dois with pattern '
                                                   'doi:prefix/shoulder/postfix')
    parser.add_argument('-m', '--metadata-dir',
                        help='The input dir with the dataset metadata files. If the folder is empty or does not exist'
                             'the json files are retrieved from dataverse and stored with file name '
                             'doi_prefix_shoulder_suffix.json')
    parser.add_argument('-c', '--converted-dir', required=True,
                        help='The output dir, for storing the converted metadata files')
    args = parser.parse_args()

    convert_dataset_metadata_command(config, args.input_file, args.metadata_dir, args.converted_dir)


if __name__ == '__main__':
    main()

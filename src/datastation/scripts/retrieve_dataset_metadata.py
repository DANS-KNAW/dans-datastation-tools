import argparse
import logging
import os

from datastation.batch_processing import batch_process
from datastation.config import init
from datastation.ds_metadatafile import store_dataset_result
from datastation.ds_pidsfile import load_pids
from datastation.dv_api import get_dataset_metadata_export


def retrieve_dataset_metadata_action(server_url, pid, output_dir):
    dataset_metadata = get_dataset_metadata_export(server_url, pid)
    # note that the dataset metadata might be large if there are a lot of files in the dataset!
    store_dataset_result(pid, dataset_metadata, output_dir)


def retrieve_dataset_metadata_command(server_url, input_filename, output_dir):
    logging.info('Args: {}, {}'.format(input_filename, output_dir))
    logging("Example using server URL: {}".format(server_url))

    if os.path.isdir(output_dir):
        logging.info("Skipping dir creation, because it already exists: " + output_dir)
    else:
        logging.warning("Creating output dir: " + output_dir)
        os.makedirs(output_dir)

    batch_process(load_pids(input_filename), lambda pid: retrieve_dataset_metadata_action(server_url, pid, output_dir),
                  output_file=None, delay=0.2)


def main():
    config = init()
    parser = argparse.ArgumentParser(description='Retrieves the metadata for all published datasets with the pids in '
                                                 'the given inputfile')
    parser.add_argument('-i', '--input-file', dest='pids_file', help='The input file with the dataset pids')
    parser.add_argument('-o', '--output', dest='output_dir',
                        help='The output dir, for storing the metadata files retrieved')
    args = parser.parse_args()

    retrieve_dataset_metadata_command(config['dataverse']['server_url'], args.pids_file, args.output_dir)


if __name__ == '__main__':
    main()

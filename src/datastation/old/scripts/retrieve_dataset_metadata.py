import argparse
import logging
import os

from src.datastation.old.batch_processing import batch_process
from src.datastation.old.config import init
from src.datastation.old.ds_metadatafile import store_dataset_result
from src.datastation.old.ds_pidsfile import load_pids
from src.datastation.old.dv_api import get_dataset_metadata_export


def retrieve_dataset_metadata_action(server_url, pid, output_dir):
    dataset_metadata = get_dataset_metadata_export(server_url, pid)
    # note that the dataset metadata might be large if there are a lot of files in the dataset!
    store_dataset_result(pid, dataset_metadata, output_dir)


def retrieve_dataset_metadata_command(server_url, delay, input_filename, output_dir):
    logging.info('Args: {}, {}'.format(input_filename, output_dir))
    logging.info("Example using server URL: {}".format(server_url))

    if os.path.isdir(output_dir):
        logging.info("Skipping dir creation, because it already exists: " + output_dir)
    else:
        logging.warning("Creating output dir: " + output_dir)
        os.makedirs(output_dir)

    batch_process(load_pids(input_filename), lambda pid: retrieve_dataset_metadata_action(server_url, pid, output_dir),
                  delay)


def main():
    config = init()
    parser = argparse.ArgumentParser(description='Retrieves the metadata for all published datasets with the pids in '
                                                 'the given inputfile')
    parser.add_argument('-d', '--datasets', dest='pids_file', help='The input file with the dataset pids')
    parser.add_argument('-p', '--pid', help='Pid of single dataset to retrieve the metadata for')
    parser.add_argument('-o', '--output', dest='output_dir',
                        help='The output dir, for storing the metadata files retrieved')
    parser.add_argument('--delay', default=0.2, help="Delay in seconds")
    args = parser.parse_args()

    server_url = config['dataverse']['server_url']
    if args.pid is not None:
        retrieve_dataset_metadata_action(server_url, args.pid, args.output_dir)
    else:
        retrieve_dataset_metadata_command(server_url, args.delay, args.pids_file, args.output_dir)


if __name__ == '__main__':
    main()
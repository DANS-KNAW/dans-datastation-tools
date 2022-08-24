import argparse
import logging

from datastation.config import init
from datastation.ds_pidsfile import store_pids
from datastation.dv_search import get_dataset_pids_from_search


def retrieve_dataset_pids_command(server_url, dataverse_alias, output_dir, output_filename):
    logging.info("Retrieving dataset PIDs from URL {}, dataverse {}".format(server_url, dataverse_alias))
    pids = get_dataset_pids_from_search(server_url, dataverse_alias)
    store_pids(pids, output_dir, output_filename)


def main():
    config = init()

    parser = argparse.ArgumentParser(
        description='Retrieves the pids for all published datasets in the given collection')
    parser.add_argument('-o', '--output-dir', dest='output_dir', required=True,
                        help='The output file, for storing the pids retrieved')
    parser.add_argument('-f', '--output-file', dest='output_file',
                        help='name of the output file that will contain all the pids of the published datasets. '
                             'Defaults to pids-<timestamp>.txt')
    parser.add_argument('dataverse_alias', help='The short name (or alias) of the dataverse (collection)')
    args = parser.parse_args()

    retrieve_dataset_pids_command(config['dataverse']['server_url'], args.dataverse_alias, args.output_dir,
                                  args.output_file)


if __name__ == '__main__':
    main()

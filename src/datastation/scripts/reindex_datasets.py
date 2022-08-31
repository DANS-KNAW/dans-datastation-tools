import argparse

from datastation.batch_processing import batch_process
from datastation.config import init
from datastation.ds_pidsfile import load_pids
from datastation.dv_api import reindex_dataset


def reindex_dataset_command(server_url, delay, pids_file):
    pids = load_pids(pids_file)

    # could be fast, but depends on number of files inside the dataset
    batch_process(pids, lambda pid: reindex_dataset(server_url, pid), delay)


def main():
    config = init()
    parser = argparse.ArgumentParser(description='Reindex datasets with the pids in the given input file')
    parser.add_argument('-d', '--datasets', dest='pids_file', help='The input file with the dataset dois with '
                                                                     'pattern doi:prefix/shoulder/postfix')
    parser.add_argument('--delay', default=2.0, help="Delay in seconds")
    args = parser.parse_args()

    server_url = config['dataverse']['server_url']

    reindex_dataset_command(server_url, args.delay, args.pids_file)


if __name__ == '__main__':
    main()

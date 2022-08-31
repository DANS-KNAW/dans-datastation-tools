import sys
import argparse
import logging

from datastation.batch_processing import batch_process
from datastation.config import init
from datastation.ds_pidsfile import load_pids
from datastation.dv_api import delete_dataset_draft


def delete_dataset_command(server_url, api_token, pids_file, delay):
    pids = load_pids(pids_file)

    batch_process(pids, lambda pid: delete_dataset_draft(server_url, api_token, pid), delay)


def main():
    config = init()
    parser = argparse.ArgumentParser(description='Delete datasets with the pids in the given input file. '
                                                 'Only the draft version is deleted '
                                                 'and it will fail if it is not a draft!')
    parser.add_argument('-d', '--datasets', dest='dataset_pids_file', required=True,
                        help='The input file with the dataset pids with pattern doi:prefix/shoulder/postfix')
    parser.add_argument('--delay', default=2.0, help="Delay in seconds")
    args = parser.parse_args()

    server_url = config['dataverse']['server_url']
    api_token = config['dataverse']['api_token']

    logging.info("Deleting draft datasets on: {}".format(server_url))
    logging.warning("Be aware that this is irreversible and you might lose information!")
    # Only proceed if user is sure
    if not input("Are you sure? (y/n): ").lower().strip()[:1] == "y":
        logging.info("Cancelling deletion")
        sys.exit(1)
    logging.info("Starting deletion")

    delete_dataset_command(server_url, api_token, pids_file=args.dataset_pids_file, delay=args.delay)


if __name__ == '__main__':
    main()

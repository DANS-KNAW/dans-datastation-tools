import argparse
import logging
import json

from datastation.batch_processing import batch_process
from datastation.config import init
from datastation.ds_pidsfile import load_pids

from datastation.dv_api import get_dataset_locks, delete_dataset_locks


def unlock_dataset_action(server_url, api_token, pid):
    deleted_locks = False
    resp_data = get_dataset_locks(server_url, pid)
    if len(resp_data) == 0:
        logging.debug("{} - No locks found, leave as-is".format(pid))
    else:
        logging.info("{} - Found locks".format(pid))
        logging.debug(json.dumps(resp_data, indent=2))
        logging.debug("{} - Try deleting the locks".format(pid))
        delete_dataset_locks(server_url, api_token, pid)
        deleted_locks = True

    return deleted_locks


def unlock_dataset_command(server_url, api_token, delay, pids_file):
    # could be fast, but depends on number of files inside the dataset
    batch_process(load_pids(pids_file),
                  lambda pid: unlock_dataset_action(server_url, api_token, pid), delay)


def main():
    config = init()
    parser = argparse.ArgumentParser(description='Unlock datasets (if locked) with the pids in the given input file')
    parser.add_argument('-d', '--datasets', dest='pids_file', help='The input file with the dataset pids')
    parser.add_argument('--delay', default=1.5, help="Delay in seconds")
    args = parser.parse_args()

    server_url = config['dataverse']['server_url']
    api_token = config['dataverse']['api_token']

    unlock_dataset_command(server_url, api_token, args.delay, args.pids_file)


if __name__ == '__main__':
    main()

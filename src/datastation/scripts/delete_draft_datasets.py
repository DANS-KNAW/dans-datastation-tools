import sys
import argparse

from datastation.batch_processing import batch_process
from datastation.config import init
from datastation.ds_pidsfile import load_pids
from datastation.dv_api import delete_dataset_draft


def delete_dataset_command(server_url, api_token, pids_file):
    pids = load_pids(pids_file)

    batch_process(pids, lambda pid: delete_dataset_draft(server_url, api_token, pid), output_file=None, delay=2.0)


def main():
    config = init()
    parser = argparse.ArgumentParser(description='Delete datasets with the pids in the given input file. '
                                                 'Only the draft version is deleted '
                                                 'and it will fail if it is not a draft!')
    parser.add_argument('-i', '--input_file', dest='dataset_pids_file', help='The input file with the dataset pids with'
                                                                             ' pattern doi:prefix/shoulder/postfix')
    args = parser.parse_args()

    server_url = config['dataverse']['server_url']
    api_token = config['dataverse']['api_token']

    print("Deleting draft datasets on: {}".format(server_url))
    print("Be aware that this is irreversible and you might lose information!")
    # Only proceed if user is sure
    if not input("Are you sure? (y/n): ").lower().strip()[:1] == "y": print("Cancelling deletion"), sys.exit(1)
    print("Starting deletion")

    delete_dataset_command(server_url, api_token, pids_file=args.dataset_pids_file)


if __name__ == '__main__':
    main()

import argparse

from datastation.batch_processing import batch_process
from datastation.config import init
from datastation.ds_pidsfile import load_pids
from datastation.dv_api import publish_dataset


def publish_dataset_command(server_url, api_token, pids_file, version_upgrade_type):
    pids = load_pids(pids_file)

    # Long delay because publish is doing a lot after the async. request is returning
    # and sometimes datasets get locked
    batch_process(pids, lambda pid: publish_dataset(server_url, api_token, pid, version_upgrade_type),
                  output_file=None, delay=5.0)


def main():
    config = init()
    parser = argparse.ArgumentParser(description='Publishes datasets with the pids in the given input file')
    parser.add_argument('-i', '--input-file', dest='pids_file', help='The input file with the dataset pids')
    parser.add_argument('-t', '--type', dest='version_upgrade_type', default='major',
                        help='The type of version upgrade, "minor" or "updatecurrent" (only for superusers) for '
                             'metadata changes, default is "major".')
    args = parser.parse_args()

    server_url = config['dataverse']['server_url']
    api_token = config['dataverse']['api_token']

    publish_dataset_command(server_url, api_token, args.pids_file, args.version_upgrade_type)


if __name__ == '__main__':
    main()

import argparse

from datastation.batch_processing import batch_process
from datastation.config import init
from datastation.ds_pidsfile import load_pids
from datastation.dv_api import publish_dataset


def publish_dataset_command(server_url, api_token, delay, pids_file, version_upgrade_type):
    pids = load_pids(pids_file)

    # Long delay because publish is doing a lot after the async. request is returning
    # and sometimes datasets get locked
    batch_process(pids, lambda pid: publish_dataset(server_url, api_token, pid, version_upgrade_type), delay)


def main():
    config = init()
    parser = argparse.ArgumentParser(description='Publishes datasets with the pids in the given input file')
    parser.add_argument('-d', '--datasets', dest='pids_file', help='The input file with the dataset pids')
    parser.add_argument('-p', '--pid', help="Pid of the dataset to be published.")
    parser.add_argument('-t', '--type', dest='version_upgrade_type', default='major',
                        help='The type of version upgrade, "minor" or "updatecurrent" (only for superusers) for '
                             'metadata changes, default is "major", which is also needed for the initial version.')
    parser.add_argument('--delay', default=5.0, help="Delay in seconds")
    args = parser.parse_args()

    server_url = config['dataverse']['server_url']
    api_token = config['dataverse']['api_token']

    if args.pid is not None:
        publish_dataset(server_url, api_token, args.delay, args.pid, args.version_upgrade_type)
    else:
        publish_dataset_command(server_url, api_token, args.pids_file, args.version_upgrade_type)


if __name__ == '__main__':
    main()

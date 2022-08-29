import argparse
import logging
import datetime

from datastation.batch_processing import batch_process
from datastation.config import init
from datastation.ds_pidsfile import load_pids
from datastation.dv_api import add_dataset_role_assignment, get_dataset_roleassigments


def add_roleassignment(server_url, api_token, pid, role_assignee, role_alias, dry_run):
    current_assignments = get_dataset_roleassigments(server_url, api_token, pid)
    found = False
    for current_assignment in current_assignments:
        if current_assignment.get('assignee') == role_assignee and current_assignment.get('_roleAlias') == role_alias:
            found = True
            break

    action = "None" if found else "Added"
    print("{},{},{},{},{}".format(pid, datetime.datetime.now(), role_assignee, role_alias, action))

    if dry_run:
        if found:
            logging.info("Dry run: {} is already {} for dataset {}".format(role_assignee, role_alias, pid))
        else:
            logging.info("Dry run: not adding {} as {} for dataset {}".format(role_assignee, role_alias, pid))
    else:
        if found:
            logging.warning("{} is already {} for dataset {}".format(role_assignee, role_alias, pid))
        else:
            role_assignment = {"assignee": role_assignee, "role": role_alias}
            add_dataset_role_assignment(server_url, api_token, pid, role_assignment)


def add_roleassignment_command(server_url, api_token, pids_file, role_assignmee, role_alias, dry_run):
    pids = load_pids(pids_file)

    print("DOI,Modified,Assignee,Role,Change")
    batch_process(pids, lambda pid: add_roleassignment(server_url, api_token, pid, role_assignmee, role_alias, dry_run))


def main():
    config = init()
    parser = argparse.ArgumentParser(description='Add role assignment to specified datasets')
    parser.add_argument('-d', '--datasets', dest='pid_file', help='The input file with the dataset pids')
    parser.add_argument('--dry-run', dest='dry_run', help="only logs the actions, nothing is executed", action='store_true')
    parser.add_argument('-r', "--role_assignment", help="Role assignee and alias (example: @dataverseAdmin=contributor)")
    args = parser.parse_args()

    role_assignee = args.role_assignment.split('=')[0]
    role_alias = args.role_assignment.split('=')[1]

    server_url = config['dataverse']['server_url']
    api_token = config['dataverse']['api_token']
    add_roleassignment_command(server_url, api_token, args.pid_file, role_assignee, role_alias, args.dry_run)


if __name__ == '__main__':
    main()
import argparse
import datetime
import logging

from datastation.batch_processing import batch_process
from datastation.config import init
from datastation.ds_pidsfile import load_pids
from datastation.dv_api import delete_dataset_role_assignment, get_dataset_roleassigments


def delete_roleassignment_action(server_url, api_token, pid, role_assignee, role_alias, dry_run):
    current_assignments = get_dataset_roleassigments(server_url, api_token, pid)
    found = False
    assignment_id = None
    for current_assignment in current_assignments:
        if current_assignment.get('assignee') == role_assignee and current_assignment.get('_roleAlias') == role_alias:
            assignment_id = current_assignment.get('id')
            found = True
            break

    action = "Removed" if found else "None"
    print("{},{},{},{},{}".format(pid, datetime.datetime.now(), role_assignee, role_alias, action))

    if dry_run:
        if found:
            logging.info("Dry run: not removing {} as {} for dataset {}".format(role_assignee, role_alias, pid))
        else:
            logging.info("Dry run: {} is not a {} for dataset {}".format(role_assignee, role_alias, pid))
    else:
        if found:
            delete_dataset_role_assignment(server_url, api_token, pid, assignment_id)
        else:
            logging.warning("{} is not a {} for dataset {}".format(role_assignee, role_alias, pid))


def delete_roleassignments_command(server_url, api_token, pids_file, role_assignee, role_alias, dry_run):
    pids = load_pids(pids_file)

    print("DOI,Modified,Assignee,Role,Change")
    batch_process(pids, lambda pid: delete_roleassignment_action(server_url,api_token, pid, role_assignee,
                                                                 role_alias, dry_run), None, delay=1.5)


def main():
    config = init()
    parser = argparse.ArgumentParser(
        description='Delete role assigment for datasets with the pids in the given inputfile')
    parser.add_argument('--dry-run', dest='dry_run', help="only logs the actions, nothing is executed", action='store_true')
    parser.add_argument('-r', "--role_assignment", help="Role assignee and alias (example: @dataverseAdmin=contributor)")
    parser.add_argument('-d', '--datasets', dest="pids_file",
                        default='dataset_pids.txt', help='The input file with the dataset pids')
    args = parser.parse_args()

    role_assignee = args.role_assignment.split('=')[0]
    role_alias = args.role_assignment.split('=')[1]

    server_url = config['dataverse']['server_url']
    api_token = config['dataverse']['api_token']

    delete_roleassignments_command(server_url, api_token, args.pids_file, role_assignee, role_alias, args.dry_run)


if __name__ == '__main__':
    main()

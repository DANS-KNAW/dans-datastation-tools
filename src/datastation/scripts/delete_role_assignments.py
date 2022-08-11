import argparse
import logging

from datastation.batch_processing import batch_process
from datastation.config import init
from datastation.ds_pidsfile import load_pids
from datastation.dv_api import delete_dataset_role_assignment, get_dataset_roleassigments


def delete_role_assignment_action(server_url, api_token, pid, role_assignee, role_alias):
    deleted_role = False
    resp_data = get_dataset_roleassigments(server_url, api_token, pid)
    for role_assignment in resp_data:
        assignee = role_assignment['assignee']
        alias = role_assignment['_roleAlias']
        logging.debug("Role assignee: " + assignee + ', role alias: ' + alias)
        if assignee == role_assignee and alias == role_alias:
            # delete this one
            assignment_id = role_assignment['id']
            logging.info("Try deleting the role assignment")
            delete_dataset_role_assignment(server_url, api_token, pid, assignment_id)
            logging.info("Deleted role {} for user {} in dataset {}".format(role_alias, role_assignee, pid))
            deleted_role = True
        else:
            logging.info("Leave as-is")
    if not deleted_role:
        logging.info("role {} not found for user {} in dataset {}".format(role_alias, role_assignee, pid))
    return deleted_role


def delete_role_assignments_command(config, pids_file, role_assignee, role_alias):
    pids = load_pids(pids_file)

    # could be fast, but depends on number of files inside the dataset
    batch_process(pids, lambda pid: delete_role_assignment_action(config['dataverse']['server_url'],
                                                                  config['dataverse']['api_token'], pid, role_assignee,
                                                                  role_alias), output_file=None, delay=1.5)


def main():
    config = init()
    parser = argparse.ArgumentParser(
        description='Delete role assignment for user in datasets with the pids in the given input file')
    parser.add_argument("role-assignee", help="Role assignee (example: @dataverseAdmin)", required=True)
    parser.add_argument("role-alias", help="Role alias (example: contributor)", required=True)
    parser.add_argument('-i', '--input-file', dest='dataset_pids_file',
                        help='The input file with the dataset dois with pattern doi:prefix/shoulder/postfix')
    args = parser.parse_args()

    role_assignee = args.role_assignee
    role_alias = args.role_alias

    delete_role_assignments_command(config, args.dataset_pids_file, role_assignee, role_alias)


if __name__ == '__main__':
    main()

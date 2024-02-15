import argparse


from datastation.common.config import init
from datastation.common.utils import add_batch_processor_args, add_dry_run_arg
from datastation.dataverse.dataverse_client import DataverseClient
from datastation.dataverse.roles import DataverseRole


def main():
    config = init()
    dataverse_client = DataverseClient(config['dataverse'])
    role_assignment = DataverseRole(dataverse_client)

    # Create main parser and subparsers
    parser = argparse.ArgumentParser(description='Manage role assignments on one or more datasets.')
    subparsers = parser.add_subparsers(help='subcommands', dest='subcommand')

    # Add role assignment
    parser_add = subparsers.add_parser('add', help='add role assignment to specified dataset(s)')
    parser_add.add_argument('role_assignment',
                            help='role assignee and alias (example: @dataverseAdmin=contributor) to add')
    parser_add.add_argument('alias_or_alias_file',
                            help='The dataverse alias or the input file with the dataverse aliases')
    add_batch_processor_args(parser_add)
    add_dry_run_arg(parser_add)

    parser_add.set_defaults(func=lambda _: role_assignment.add_role_assignments(_))

    # Remove role assignment
    parser_remove = subparsers.add_parser('remove', help='remove role assignment from specified dataset(s)')
    parser_remove.add_argument('role_assignment',
                               help='role assignee and alias (example: @dataverseAdmin=contributor)')
    parser_remove.add_argument('alias_or_alias_file',
                               help='The dataverse alias or the input file with the dataverse aliases')
    add_batch_processor_args(parser_remove)
    add_dry_run_arg(parser_remove)
    parser_remove.set_defaults(func=lambda _: role_assignment.remove_role_assignments(_))

    # List role assignments
    parser_list = subparsers.add_parser('list',
                                        help='list role assignments for specified dataverse (only one alias allowed)')
    parser_list.add_argument('alias', help='the dataverse alias')
    add_dry_run_arg(parser_list)
    parser_list.set_defaults(func=lambda _: role_assignment.list_role_assignments(_))

    args = parser.parse_args()
    args.func(args)


if __name__ == '__main__':
    main()

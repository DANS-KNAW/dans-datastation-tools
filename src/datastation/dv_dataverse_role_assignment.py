import argparse

from datastation.common.common_batch_processing import get_aliases, DataverseBatchProcessorWithReport
from datastation.common.config import init
from datastation.common.utils import add_batch_processor_args, add_dry_run_arg
from datastation.dataverse.dataverse_client import DataverseClient
from datastation.dataverse.roles import DataverseRole


def list_role_assignments(args, dataverse_client: DataverseClient):
    role_assignment = DataverseRole(dataverse_client)
    role_assignment.list_role_assignments(args.alias)

def add_role_assignments(args, dataverse_client: DataverseClient):
    role_assignment = DataverseRole(dataverse_client, args.dry_run)
    aliases = get_aliases(args.alias_or_alias_file)
    batch_processor = DataverseBatchProcessorWithReport(wait=args.wait, fail_on_first_error=args.fail_fast,
                                                        report_file=args.report_file,
                                                        headers=['alias', 'Modified', 'Assignee', 'Role', 'Change'])
    batch_processor.process_aliases(aliases,
                                    lambda alias,
                                           csv_report: role_assignment.add_role_assignment(args.role_assignment,
                                                                                dataverse_api=
                                                                                dataverse_client.dataverse(
                                                                                    alias),
                                                                                csv_report=csv_report))


def remove_role_assignments(args, dataverse_client: DataverseClient):
    role_assignment = DataverseRole(dataverse_client, args.dry_run)
    aliases = get_aliases(args.alias_or_alias_file)
    batch_processor = DataverseBatchProcessorWithReport(wait=args.wait, report_file=args.report_file,
                                                        headers=['alias', 'Modified', 'Assignee', 'Role', 'Change'])
    batch_processor.process_aliases(aliases,
                                    lambda alias,
                                           csv_report: role_assignment.remove_role_assignment(args.role_assignment,
                                                                                   dataverse_api=
                                                                                   dataverse_client.dataverse(
                                                                                       alias),
                                                                                   csv_report=csv_report))


def main():
    config = init()
    dataverse_client = DataverseClient(config['dataverse'])

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

    parser_add.set_defaults(func=lambda _: add_role_assignments(_, dataverse_client))

    # Remove role assignment
    parser_remove = subparsers.add_parser('remove', help='remove role assignment from specified dataset(s)')
    parser_remove.add_argument('role_assignment',
                               help='role assignee and alias (example: @dataverseAdmin=contributor)')
    parser_remove.add_argument('alias_or_alias_file',
                               help='The dataverse alias or the input file with the dataverse aliases')
    add_batch_processor_args(parser_remove)
    add_dry_run_arg(parser_remove)
    parser_remove.set_defaults(func=lambda _: remove_role_assignments(_, dataverse_client))

    # List role assignments
    parser_list = subparsers.add_parser('list',
                                        help='list role assignments for specified dataverse (only one alias allowed)')
    parser_list.add_argument('alias', help='the dataverse alias')
    add_dry_run_arg(parser_list)
    parser_list.set_defaults(func=lambda _: list_role_assignments(_, dataverse_client))

    args = parser.parse_args()
    args.func(args)


if __name__ == '__main__':
    main()

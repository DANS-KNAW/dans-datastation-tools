import argparse
from argparse_formatter import FlexiFormatter

from datastation.common.config import init
from datastation.common.utils import add_dry_run_arg
from datastation.dataverse.dataverse_client import DataverseClient
from datastation.dataverse.permissions_collect import PermissionsCollect


def main():
    config = init()

    output_explanation = '''
    The output has the following information:
    
      * depth: The depth in the tree of the . The top-level ('root') has depth 0.
      * parentalias: The alias of the parent dataverse
      * alias: The alias of the dataverse
      * name: The name of the dataverse
      * id: The id of the dataverse, sometimes used in places where the alias is not used
      * vpath: The virtual path of the collection, i.e. the path from the root to the dataverse
      * groups: A comma-separated list of the explicit groups under the dataverse. 
        For each group there is the 'identifier' with the number of 'containedRoleAssignees' in braces appended. 
      * roles:  A comma-separated list of the roles defined in the dataverse. 
        For each role there is the 'alias' with the number of 'permissions' in braces appended.
      * assignments: A comma-separated list of the assignments of roles on the dataverse. 
        For each assignment there is the 'assignee' with the '_roleAlias' in braces appended.
    '''
    parser = argparse.ArgumentParser(description='Collect the permissions overview for the dataverses (collections) in a Dataverse installation.',
                                     epilog=output_explanation, formatter_class=FlexiFormatter)
    parser.add_argument('-o', '--output-file', dest='output_file', default='-',
                        help='The file to write the output to or - for stdout')
    parser.add_argument('-f', '--format', dest='format',
                        help='Output format, one of: csv, json (default: json)')
    parser.add_argument('-s', '--selected-dataverse', dest='selected_dataverse', default=None,
                        help='The dataverse (top-level) sub-tree to collect the permissions for, by default all dataverses are collected')
    add_dry_run_arg(parser)
    args = parser.parse_args()

    selected_dataverse = args.selected_dataverse
    dataverse_client = DataverseClient(config['dataverse'])
    collector = PermissionsCollect(dataverse_client, args.output_file, args.format, args.dry_run)
    collector.collect_permissions_info_overview(selected_dataverse)

if __name__ == '__main__':
    main()

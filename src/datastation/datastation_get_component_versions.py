import argparse

import rich
from rich.console import Console
from rich.table import Table

from datastation.common.config import init
from datastation.common.version_info import get_rpm_versions, get_dataverse_version, get_dataverse_build_number, \
    get_payara_version


def main():
    config = init()

    parser = argparse.ArgumentParser(
        description='Gets the version of all Data Station components in this installation.')
    parser.add_argument('--json', dest='json', action='store_true', help='Output as JSON')
    args = parser.parse_args()

    components = get_rpm_versions(config['version_info']['dans_rpm_module_prefix'])
    dataverse_version = get_dataverse_version(config['version_info']['dataverse_application_path'])
    dataverse_build_number = get_dataverse_build_number(config['version_info']['dataverse_application_path'])
    components['dataverse'] = f'{dataverse_version} build {dataverse_build_number}'
    payara_version = get_payara_version(config['version_info']['payara_install_path'])
    components['payara'] = payara_version

    if args.json:
        rich.print(components)
        return
    else:
        table = Table(title="Data Station Component Versions")
        table.add_column("Component")
        table.add_column("Version")
        for component in components:
            table.add_row(component, components[component])
        console = Console()
        console.print(table)


if __name__ == '__main__':
    main()

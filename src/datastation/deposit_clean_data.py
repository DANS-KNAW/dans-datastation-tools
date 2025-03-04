import argparse
import csv
from datetime import date, timedelta
import io
import os
import  shutil

from datastation.managedeposit.manage_deposit import ManageDeposit
from datastation.common.config import init


class CleanHandler:
    def __init__(self, server_url, cmd_args):
        self.__deposit_folder_index_in_csv = 1
        self.__deposit_location_index_in_csv = 7
        self.__server_url = server_url
        self.__command_line_args = cmd_args

    def handle_request(self):
        report = ManageDeposit(self.__command_line_args).create_report(self.__server_url)

        if report is not None and len(report.split('\n')) > 1:
            self.remove_folders(self.collect_paths(report))
        else:
            print("deposit_clean_data: report is empty.")

    def collect_paths(self, csv_data):
        paths = []
        csv_mem = io.StringIO(csv_data)
        csv_reader = csv.reader(csv_mem)
        for row in csv_reader:
            if len(row) > max(self.__deposit_folder_index_in_csv, self.__deposit_location_index_in_csv):
                paths.append(row[self.__deposit_location_index_in_csv] + "/" + row[self.__deposit_folder_index_in_csv])

        return paths

    def remove_folders(self, paths):
        for path in paths:
            if os.path.exists(path) and os.path.isdir(path):
                shutil.rmtree(path)
                print(f"Folder '{path}' has been removed successfully.")
            else:
                print(f"Folder '{path}' does not exist.")


def main():
    config = init()
    parser = argparse.ArgumentParser(prog='deposit_data_cleaner', description='Clean up dd-manage-deposit database')
    parser.add_argument('-f', '--format', dest='file_format', default='text/csv', help='Output data format')
    parser.add_argument('-s', '--startdate', dest='startdate', help='Filter from the record creation of this date')

    group = parser.add_mutually_exclusive_group(required=False)
    group.add_argument('-a', '--age', dest='age', type=int, help='Filter records older than a number of days before today')
    group.add_argument('-e', '--enddate', dest='enddate', help='Filter until the record creation of this date')

    parser.add_argument('-t', '--state', dest='state', help='The state of the deposit (repeatable)', action='append')
    parser.add_argument('-u', '--user', dest='user', help='The depositor name (repeatable)', action='append')
    args = parser.parse_args()

    if args.age is not None: # Note: args is a Namespace object
        vars(args)['enddate'] = (date.today() + timedelta(days=-args.age)).strftime('%Y-%m-%d')

    server_url = config['manage_deposit']['service_baseurl'] + '/report'

    CleanHandler(server_url, args).handle_request()


if __name__ == '__main__':
    main()

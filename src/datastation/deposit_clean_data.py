import argparse
from datastation.managedeposit.manage_deposit import ManageDeposit
from datastation.common.config import init


def clean_manage_deposit_data(server_url, args):
    result = ManageDeposit(args).clean_data(server_url)
    if result is not None:
        print(result)


def main():
    config = init()
    parser = argparse.ArgumentParser(prog='deposit_data_cleaner', description='Clean up dd-manage-deposit database')
    parser.add_argument('-e', '--enddate', dest='enddate', help='Filter until the record creation of this date')
    parser.add_argument('-s', '--startdate', dest='startdate', help='Filter from the record creation of this date')
    parser.add_argument('-t', '--state', dest='state', help='The state of the deposit')
    parser.add_argument('-u', '--user', dest='user', help='The depositor name')
    args = parser.parse_args()

    server_url = config['manage_deposit']['service_baseurl'] + '/delete-deposit'

    clean_manage_deposit_data(server_url, args)


if __name__ == '__main__':
    main()

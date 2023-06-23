import argparse
import requests
from managedeposit.compose_curl_request import ComposeCurlRequest

from datastation.common.config import init


def clean_manage_deposit_data(server_url, args):
    curl_request = ComposeCurlRequest(args)
    response = requests.post(server_url + "?" + str(curl_request.compose_filter_params()))
    print(response.text)


def main():
    config = init()
    parser = argparse.ArgumentParser(prog='deposit_data_cleaner', description='Clean up dd-manage-deposit database')
    parser.add_argument('-e', '--enddate', dest='end_date', help='Filter until the record creation of this date')
    parser.add_argument('-s', '--startdate', dest='start_date', help='Filter from the record creation of this date')
    parser.add_argument('-t', '--state', help='The state of the deposit')
    parser.add_argument('-u', '--user', dest='user', help='The depositor name')
    args = parser.parse_args()

    print("input arguments: " + str(args))

    server_url = config['manage_deposit']['delete_request']

    clean_manage_deposit_data(server_url, args)


if __name__ == '__main__':
    main()

import argparse
import requests

from datastation.config import init


class CommandLineParser:
    def __init__(self, args):
        self.__input_args__ = args
        self.filter_params = ''
        self.headers = {}

        self.compose_filter_params()
        self.compose_headers()

    def compose_filter_params(self):
        add_amp_if_need = lambda x: x + "&" if len(x) > 0 else x

        if self.__input_args__.start_date is not None:
            self.filter_params = add_amp_if_need(self.filter_params)
            self.filter_params += "startdate=" + self.__input_args__.start_date

        if self.__input_args__.end_date is not None:
            self.filter_params = add_amp_if_need(self.filter_params)
            self.filter_params += "enddate=" + self.__input_args__.end_date

        if self.__input_args__.state is not None:
            self.filter_params = add_amp_if_need(self.filter_params)
            self.filter_params += "state=" + self.__input_args__.state

        if self.__input_args__.user is not None:
            self.filter_params = add_amp_if_need(self.filter_params)
            self.filter_params += "user=" + self.__input_args__.user

        print("self.filter_params: ", self.filter_params)

    def compose_headers(self):
        # Dictionary of HTTP Headers to send with the :class:`Request`
        if self.__input_args__.file_format is not None:
            self.headers["Accept"] = str(self.__input_args__.file_format)

        print(self.headers)


def clean_manage_deposit_data(server_url, command_line_parser):
    response = requests.post(server_url + "?" + str(command_line_parser.filter_params), headers=command_line_parser.headers)
    print(response.text)


def main():
    config = init()
    parser = argparse.ArgumentParser(description='Create and send report based on dd-manage-deposit database')
    parser.add_argument('-e', '--enddate', dest='end_date', help='The record creation date that ends on')
    parser.add_argument('-s', '--startdate', dest='start_date', help='The record creation date that starts on')
    parser.add_argument('-t', '--state', help='The state of the deposit')
    parser.add_argument('-u', '--user', dest='user', help='The depositor name')
    parser.add_argument('-f', '--format', dest='file_format', help='Output data format')
    parser.add_argument('--email-from', dest='email_from', help='')
    parser.add_argument('--email-to', dest='email_to', help='')
    parser.add_argument('--email-bcc', dest='email_bcc', help='')
    parser.add_argument('-d', '--datamanager', dest='datamanager', help='')
    args = parser.parse_args()

    print(args)

    server_url = config['manage_deposit']['report_request']
    # server_url = 'http://localhost:20347/report'

    clean_manage_deposit_data(server_url, CommandLineParser(args))


if __name__ == '__main__':
    main()

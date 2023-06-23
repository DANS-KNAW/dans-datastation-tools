import argparse
import requests
import os
from managedeposit.compose_curl_request import ComposeCurlRequest

from datastation.common.config import init


def collect_deposits_data(server_url, args):
    curl_request = ComposeCurlRequest(args)
    # print(server_url + "?" + str(curl_request.compose_filter_params()) + ", headers=" + str(curl_request.compose_headers()))
    response = requests.get(server_url + "?" + str(curl_request.compose_filter_params()),
                            headers=curl_request.compose_headers())

    # print(response.text)
    report = response.text
    if args.file_format == 'text/csv':
        index_end_title_line = report.find('\n')
        if index_end_title_line > 0:
            report = report.replace(report[0:index_end_title_line], report[0:index_end_title_line].upper(), 1)

    if args.output_file is not None:
        with open(args.output_file, 'w') as output:
            output.write(report)


def send_report(to_email, subject, message_body, attachment):
    email_template = " -s '{0}' {1}<<EOF {2} \nEOF"
    if attachment is not None:
        os.system("mail" + " -a " + attachment + email_template.format(subject, to_email, message_body))
    else:
        os.system("mail" + email_template.format(subject, to_email, message_body))


def main():
    config = init()
    parser = argparse.ArgumentParser(prog='deposit_create_report',
                                     description='Create and send reports based on dd-manage-deposit database')
    parser.add_argument('-o', '--output-file', dest='output_file', default='-',
                        help='the file to write the output to or - for stdout')
    parser.add_argument('-e', '--enddate', dest='end_date', help='Filter until the record creation of this date')
    parser.add_argument('-s', '--startdate', dest='start_date', help='Filter from the record creation of this date')
    parser.add_argument('-t', '--state', help='The state of the deposit')
    parser.add_argument('-u', '--user', dest='user', help='The depositor name')
    parser.add_argument('-f', '--format', dest='file_format', default='text/csv', help='Output data format')
    parser.add_argument('--email-from', dest='email_from', help='')
    parser.add_argument('--email-to', dest='email_to', help='')
    parser.add_argument('--email-bcc', dest='email_bcc', help='')
    parser.add_argument('-d', '--datamanager', dest='datamanager', help='')
    args = parser.parse_args()

    print("input arguments: " + str(args))

    server_url = config['manage_deposit']['report_request']

    collect_deposits_data(server_url, args)
    send_report(args.email_to, "Deposits report", " ", args.output_file)


if __name__ == '__main__':
    main()

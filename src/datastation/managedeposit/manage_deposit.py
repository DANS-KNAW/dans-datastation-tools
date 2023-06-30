import requests


class ManageDeposit:
    """ Get python script input arguments and
            convert them to HTTP request parameters and headers and
            submit the request"""

    def __init__(self, cmd_args):
        self.__payload = {'user': cmd_args.user, 'state': cmd_args.state,
                          'startdate': cmd_args.start_date, 'enddate': cmd_args.end_date}
        self.__headers = dict()
        self.compose_headers(cmd_args)

    def compose_headers(self, cmd_args):
        """ Build an HTTP __headers dictionary to send with the class:`Requests` """
        if cmd_args.file_format is not None:
            self.__headers['Accept'] = str(cmd_args.file_format)

    def create_report(self, server_url):
        response = requests.get(server_url, self.__payload, headers=self.__headers)

        if response.status_code == requests.codes.ok:
            return response.text

        print("Error: ManageDeposit:create_report() - response.status_code" + str(response.status_code))
        return None

    def clean_data(self, server_url):
        response = requests.post(server_url, self.__payload)
        if response.status_code == requests.codes.ok:
            return response.text

        print("Error: ManageDeposit:clean_datat() - response.status_code" + str(response.status_code))
        return None

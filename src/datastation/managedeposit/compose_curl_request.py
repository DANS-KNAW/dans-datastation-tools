class ComposeCurlRequest:
    """ Get python script input arguments and
        convert them to HTTP request parameters and heraders """

    def __init__(self, args):
        self.__input_args = args
        self.__filter_params = ''
        self.__headers = {}

    def compose_filter_params(self):
        """ Build filter parameters to make a selection inquiry via class:`Request` """
        if self.__input_args.start_date is not None:
            self.__filter_params = self.add_ampersand_conjunction_if_need(self.__filter_params)
            self.__filter_params += "startdate=" + self.__input_args.start_date

        if self.__input_args.end_date is not None:
            self.__filter_params = self.add_ampersand_conjunction_if_need(self.__filter_params)
            self.__filter_params += "enddate=" + self.__input_args.end_date

        if self.__input_args.state is not None:
            self.__filter_params = self.add_ampersand_conjunction_if_need(self.__filter_params)
            self.__filter_params += "state=" + self.__input_args.state

        if self.__input_args.user is not None:
            self.__filter_params = self.add_ampersand_conjunction_if_need(self.__filter_params)
            self.__filter_params += "user=" + self.__input_args.user

        return self.__filter_params

    def compose_headers(self):
        """ Build an HTTP __headers dictionary to send with the class:`Request` """
        if self.__input_args.file_format is not None:
            self.__headers["Accept"] = str(self.__input_args.file_format)

        return self.__headers

    @staticmethod
    def add_ampersand_conjunction_if_need(text):
        if len(text) > 0:
            return text + "&"
        return text

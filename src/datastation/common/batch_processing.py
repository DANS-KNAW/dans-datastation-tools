import logging
import os
import sys
import time

from datastation.common.csv import CsvReport


def get_pids(pid_or_pids_file, search_api=None, query="*", subtree="root", object_type="dataset", dry_run=False):
    """

    Args:
        pid_or_pids_file: The dataset pid, or a file with a list of pids.
        search_api:       must be provided if pid_or_pids_file is None
        query:            passed on to search_api().search
        object_type:      passed on to search_api().search
        subtree (object): passed on to search_api().search
        dry_run:          Do not perform the action, but show what would be done.
                          Only applicable if pid_or_pids_file is None.

    Returns: an iterator with pids,
             if pid_or_pids_file is not provided, it searches for all datasets
             and extracts their pids, fetching the result pages lazy.
    """
    if pid_or_pids_file is None:
        result = search_api.search(query=query, subtree=subtree, object_type=object_type, dry_run=dry_run)
        return map(lambda rec: rec['global_id'], result)
    elif os.path.isfile(os.path.expanduser(pid_or_pids_file)):
        pids = []
        with open(os.path.expanduser(pid_or_pids_file)) as f:
            for line in f:
                pids.append(line.strip())
        return pids
    else:
        return [pid_or_pids_file]


class BatchProcessor:
    def __init__(self, wait=0.1, fail_on_first_error=True):
        self.wait = wait
        self.fail_on_first_error = fail_on_first_error

    def process_pids(self, pids_or_objects, callback):
        """ The callback is called for each entry in pids_or_objects.

        Args:
            pids_or_objects: a list of pids or objects, or a single pid or object
            callback:        a function that takes a pid or object as argument
        Returns:
            None

        If an entry of pids_or_objects is a string or a dictionary with key 'PID',
        the value is used for progress logging.
        """
        if type(pids_or_objects) is list:
            num_pids = len(pids_or_objects)
            logging.info(f"Start batch processing on {num_pids} pids")
        else:
            logging.info(f"Start batch processing on unknown number of pids")
            num_pids = -1
        i = 0
        for obj in pids_or_objects:
            i += 1
            try:
                if self.wait > 0 and i > 1:
                    logging.debug(f"Waiting {self.wait} seconds before processing next pid")
                    time.sleep(self.wait)
                if type(obj) is dict and 'PID' in obj.keys():
                    logging.info(f"Processing {i} of {num_pids}: {obj['PID']}")
                elif type(obj) is str:
                    logging.info(f"Processing {i} of {num_pids}: {obj}")
                else:
                    logging.info(f"Processing {i} of {num_pids}")
                callback(obj)
            except Exception as e:
                logging.exception("Exception occurred", exc_info=True)
                if self.fail_on_first_error:
                    logging.error(f"Stop processing because of an exception: {e}")
                    break
                logging.debug("fail_on_first_error is False, continuing...")


class BatchProcessorWithReport(BatchProcessor):

    def __init__(self, report_file=None, headers=None, wait=0.1, fail_on_first_error=True):
        super().__init__(wait, fail_on_first_error)
        if headers is None:
            headers = ["DOI", "Modified", "Change"]
        self.report_file = report_file
        self.headers = headers

    def process_pids(self, pids_or_objects, callback):
        with CsvReport(os.path.expanduser(self.report_file), self.headers) as csv_report:
            super().process_pids(pids_or_objects, lambda pid: callback(pid, csv_report))

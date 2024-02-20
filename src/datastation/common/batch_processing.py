import logging
import os
import time

from datastation.common.csv import CsvReport


def get_pids(pid_or_pids_file):
    """ kept for backward compatibility"""
    return get_entries(pid_or_pids_file)


def get_entries(entries):
    """

    Args:
        entries: A string (e.g. a dataset PID or dataverse alias),
                 or a plain text file with a string per line

    Returns: an iterator with strings
    """
    if entries is None:
        return []
    elif os.path.isfile(os.path.expanduser(entries)):
        objects = []
        with open(os.path.expanduser(entries)) as f:
            for line in f:
                objects.append(line.strip())
        return objects
    else:
        return [entries]


class BatchProcessor:
    def __init__(self, wait=0.1, fail_on_first_error=True):
        self.wait = wait
        self.fail_on_first_error = fail_on_first_error

    def process_pids(self, entries, callback):
        """ kept for backward compatibility"""
        return self.process_entries(entries, callback)

    def process_entries(self, entries, callback):
        """ The callback is called for each entry in entries.

        Args:
            entries:  a single string (e.g. PID) or dict, or a list of string or dicts
            callback: a function that takes a single entry as argument
        Returns:
            None

        If an entry is a string or a dictionary with key 'PID',
        the value is used for progress logging.
        """
        if type(entries) is list:
            num_entries = len(entries)
            logging.info(f"Start batch processing on {num_entries} entries")
        else:
            logging.info(f"Start batch processing on unknown number of entries")
            num_entries = -1
        i = 0
        for obj in entries:
            i += 1
            try:
                if self.wait > 0 and i > 1:
                    logging.debug(f"Waiting {self.wait} seconds before processing next entry")
                    time.sleep(self.wait)
                if type(obj) is dict and 'PID' in obj.keys():
                    logging.info(f"Processing {i} of {num_entries}: {obj['PID']}")
                elif type(obj) is str:
                    logging.info(f"Processing {i} of {num_entries}: {obj}")
                else:
                    logging.info(f"Processing {i} of {num_entries}")
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

    def process_pids(self, entries, callback):
        """ kept for backward compatibility"""
        return self.process_entries(entries, callback)

    def process_entries(self, entries, callback):
        with CsvReport(os.path.expanduser(self.report_file), self.headers) as csv_report:
            super().process_entries(entries, lambda entry: callback(entry, csv_report))

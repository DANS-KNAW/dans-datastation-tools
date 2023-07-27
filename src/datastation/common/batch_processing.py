import logging
import os
import time

from datastation.common.csv import CsvReport


def get_pids(pid_or_file):
    if os.path.isfile(os.path.expanduser(pid_or_file)):
        pids = []
        with open(os.path.expanduser(pid_or_file)) as f:
            for line in f:
                pids.append(line.strip())
        return pids
    else:
        return [pid_or_file]


class BatchProcessor:
    def __init__(self, wait=0.1, fail_on_first_error=True):
        self.wait = wait
        self.fail_on_first_error = fail_on_first_error

    def process_pids(self, pids, callback):
        if type(pids) is list:
            num_pids = len(pids)
            logging.info(f"Start batch processing on {num_pids} pids")
        else:
            logging.info(f"Start batch processing on unknown numer of pids")
            num_pids = -1
        i = 0
        for pid in pids:
            i += 1
            try:
                if self.wait > 0 and i > 1:
                    logging.debug(f"Waiting {self.wait} seconds before processing next pid")
                    time.sleep(self.wait)
                logging.info(f"Processing {i} of {num_pids}: {pid}")
                callback(pid)
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

    def process_pids(self, pids, callback):
        with CsvReport(os.path.expanduser(self.report_file), self.headers) as csv_report:
            super().process_pids(pids, lambda pid: callback(pid, csv_report))

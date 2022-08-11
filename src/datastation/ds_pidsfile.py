import os
import logging
from datetime import datetime


# store in text file, that can be loaded with load_pids
def store_pids(pids, save_path, filename=None):
    if filename is None:
        # by default use a timestamp in the filename
        timestamp_str = '_' + datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = 'pids' + timestamp_str + '.txt'
    full_name = os.path.join(save_path, filename)
    logging.info('Storing: ' + full_name)
    with open(full_name, "w") as outfile:
        outfile.write("\n".join(pids))


# pids producing function (extracting from a file)
def load_pids(file_path):
    # should read from a text file, with each line contains a pid
    pids = []
    with open(file_path) as f:
        pids = f.read().splitlines()
        # Note that f.readlines() would miss the last 'line' if it has no newline character
    # trim whitespace and remove empty lines...
    return list(filter(lambda item: item.strip(), pids))

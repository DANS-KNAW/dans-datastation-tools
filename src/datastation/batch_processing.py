from datetime import datetime
import time
import logging


def batch_process(pids, process_action_func, output_file=None, delay=0.1, fail_on_first_error=True):
    """

    """
    if output_file is None:
        timestamp_str = '_' + datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = "out-{}.txt".format(timestamp_str)
    with open(output_file, 'w') as mutated_dataset_pids_file:
        num_pids = len(pids)
        logging.info("Start batch processing on {} datasets".format(num_pids))
        num = 0
        for pid in pids:
            num += 1
            logging.info("[{} of {}] Processing dataset with pid: {}".format(num, num_pids, pid))
            try:
                mutated = process_action_func(pid)
                if mutated:
                    mutated_dataset_pids_file.write(pid + '\n')
                    mutated_dataset_pids_file.flush()
            except Exception as e:
                logging.exception("Exception occurred", exc_info=True)
                if fail_on_first_error:
                    logging.error("Stop processing because of an exception:  {}".format(e))
                    break
                logging.debug("fail_on_first_error is False, continuing...")
            if delay > 0 and num < num_pids:
                logging.debug("Sleeping for {} seconds...".format(delay))
                time.sleep(delay)
        logging.info("Done processing {} datasets".format(num_pids))

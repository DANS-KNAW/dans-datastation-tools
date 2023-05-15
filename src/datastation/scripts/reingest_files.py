import argparse
import logging
from datastation.dv_api import DataverseAPI
from datastation.common.config import init
from typing import Optional, List
import time
import requests


def main():
    config = init()
    parser = argparse.ArgumentParser(
        description="Reingests tabular files from specified datasets."
    )

    # mutually exclusive group of doi or pids_file
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("doi", nargs="?", metavar="<doi>", help="A dataset pid")
    group.add_argument(
        "-p",
        "--pids-file",
        dest="pids_file",
        help="a file with a list of dataset pid's to scan, separated by newlines",
    )

    args = parser.parse_args()

    server_url = config["dataverse"]["server_url"]
    api_token = config["dataverse"]["api_token"]
    poll_interval_seconds = int(config["reingest_files"]["poll_interval_seconds"])

    dataverse_api = DataverseAPI(server_url, api_token)

    pids = get_pids_to_scan(args.doi, args.pids_file)
    logging.info("Found %s pids to scan", len(pids))

    for pid in pids:
        try:
            reingest_dataset_files(dataverse_api, pid, poll_interval_seconds)
        except Exception as e:
            logging.exception("[%s] Error reingesting dataset files: %s", pid, e)
            continue


def reingest_dataset_files(
    dataverse_api: DataverseAPI, pid: str, poll_interval_seconds: int
):
    try:
        logging.info("[%s] Reingesting dataset files", pid)
        # the dataset might be locked, so we just wait for the locks to clear
        wait_for_dataset_locks_to_clear(dataverse_api, pid, poll_interval_seconds)

        file_list = dataverse_api.get_dataset_files(pid)
        logging.debug("[%s] Found %s files", pid, len(file_list))

        for dataset_file in file_list:
            file_id = dataset_file["dataFile"]["id"]
            reingest_file(dataverse_api, pid, str(file_id), poll_interval_seconds)

    except requests.exceptions.RequestException as re:
        if re.response.status_code == 404:
            logging.error("[%s] Dataset not found", pid)
            return

        raise


def reingest_file(
    dataverse_api: DataverseAPI, pid: str, file_id: str, poll_interval_seconds: int
):
    logging.info("[%s] Checking file: %s", pid, file_id)

    try:
        reingest_response = dataverse_api.reingest_file(file_id)
        message = reingest_response.get("message", "")

        logging.info(
            '[%s] Reingest started: "%s"; waiting for dataset locks to clear',
            pid,
            message,
        )

        wait_for_dataset_locks_to_clear(dataverse_api, pid, poll_interval_seconds)

        logging.info("[%s] Reingest complete for file id %s", pid, file_id)

    except requests.exceptions.RequestException as re:
        # if the requests throws an exception, it might be to just tell us that the file cannot be ingested as tabular,
        # or some other reason that is a valid response. In that case, we just log it and move on
        try:
            message = re.response.json()["message"]
            logging.info(
                '[%s] Reingest not completed for file id %s, reason is "%s"',
                pid,
                file_id,
                message,
            )
            return
        except:
            pass

        logging.debug("[%s] RequestException: %s", pid, re.response.json())
        return


def get_pids_to_scan(pid: Optional[str], pid_file: Optional[str]) -> List[str]:
    pids = []

    if pid is not None:
        pids = [pid]

    elif pid_file is not None:
        try:
            with open(pid_file, "r") as f:
                pids = [line.strip() for line in f.readlines()]
        except FileNotFoundError as e:
            logging.error(
                "File not found: %s (system message: %s)", pid_file, e.strerror
            )
            raise

    # clear empty pids
    return list(filter(lambda s: len(s.strip()) > 0, pids))


def wait_for_dataset_locks_to_clear(
    dataverse_api: DataverseAPI, pid: str, poll_interval_seconds: int
):
    while True:
        locks = dataverse_api.get_dataset_locks(pid)
        logging.debug("[%s] Dataset is locked: %s locks present", pid, len(locks))

        if len(locks) == 0:
            logging.debug("[%s] Dataset is unlocked", pid)
            break

        time.sleep(poll_interval_seconds)


if __name__ == "__main__":
    main()

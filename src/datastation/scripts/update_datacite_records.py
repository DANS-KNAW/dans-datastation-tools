import argparse
import requests
import logging

from datastation.batch_processing import batch_process
from datastation.config import init
from datastation.ds_pidsfile import load_pids


def modify_registration_metadata(dataverse_url, dataverse_api_token, pid):
    logging.debug("Calling modifyRegistrationMetadata for {}".format(pid))
    url = '%s/api/datasets/:persistentId/modifyRegistrationMetadata?persistentId=%s' % (
        dataverse_url, pid)
    response = requests.post(
        url=url,
        headers={
            'X-Dataverse-key': dataverse_api_token
        }
    )
    logging.debug("Dataverse response: {}".format(response.text))


def update_datacite_record(dataverse_url, dataverse_api_token):
    def update_datacite_record_for_pid(pid):
        modify_registration_metadata(dataverse_url, dataverse_api_token, pid)
        return True

    return update_datacite_record_for_pid


def update_datacite_records(dataverse_url, dataverse_api_token, pid_file, delay):
    pids = load_pids(pid_file)
    batch_process(pids, update_datacite_record(dataverse_url, dataverse_api_token), delay)


def main():
    config = init()

    parser = argparse.ArgumentParser(
        description='Calls the Dataverse modifyRegistrationMetadata for the PIDs in the input file, with a delay'
                    + 'between calls')
    parser.add_argument('-d', '--datasets', dest='dataset_pids', help='Newline separated file with dataset PIDs')
    parser.add_argument('-o', '--output-file', dest='processed_pids', help="Datasets that were successfully processed")
    parser.add_argument('-s', '--seconds-delay', help='Delay between calls', default=0.25)
    args = parser.parse_args()

    dataverse_url = config['dataverse']['server_url']
    dataverse_api_token = config['dataverse']['api_token']

    update_datacite_records(dataverse_url, dataverse_api_token, args.dataset_pids, args.processed_pids, args.delay)


if __name__ == '__main__':
    main()

import argparse
import json

from requests import HTTPError

from datastation.common.batch_processing import get_pids, BatchProcessorWithReport
from datastation.common.config import init
from datastation.common.csv import CsvReport
from datastation.common.utils import add_batch_processor_args, add_dry_run_arg
from datastation.dataverse.dataverse_client import DataverseClient


def update_datacite_records(args, dataverse_client: DataverseClient):
    pids = get_pids(args.pid_or_pids_file)
    batch_processor = BatchProcessorWithReport(wait=args.wait, fail_on_first_error=args.fail_fast,
                                               report_file=args.report_file, headers=["PID", "Status", "Message"])
    batch_processor.process_pids(pids,
                                 lambda pid, csv_report: update_datacite_record(pid, dataverse_client,
                                                                                csv_report=csv_report,
                                                                                dry_run=args.dry_run))


def update_datacite_record(pid, dataverse_client: DataverseClient, csv_report: CsvReport, dry_run=False):
    try:
        r = dataverse_client.dataset(pid).modify_registration_metadata(dry_run=dry_run)
        csv_report.write({"PID": pid, "Status": "200", "Message": json.dumps(r)})
    except HTTPError as e:
        csv_report.write({"PID": pid, "Status": e.response.status_code, "Message": e.response.text})


def main():
    config = init()
    dataverse = DataverseClient(config['dataverse'])

    parser = argparse.ArgumentParser(description='Updates the DataCite records for the PIDs in the input')
    parser.add_argument('pid_or_pids_file', help='PID or newline separated file with PIDs')
    add_batch_processor_args(parser)
    add_dry_run_arg(parser)

    args = parser.parse_args()
    update_datacite_records(args, dataverse_client=dataverse)


if __name__ == '__main__':
    main()

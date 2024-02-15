import argparse
import json

from requests import HTTPError

from datastation.common.batch_processing import get_pids, BatchProcessorWithReport
from datastation.common.config import init
from datastation.common.csv import CsvReport
from datastation.common.utils import add_batch_processor_args, add_dry_run_arg
from datastation.dataverse.dataverse_client import DataverseClient


def reindex_datasets(args, dataverse_client: DataverseClient):
    pids = get_pids(args.pid_or_pid_file)
    batch_processor = BatchProcessorWithReport(wait=args.wait, fail_on_first_error=args.fail_fast,
                                               report_file=args.report_file, headers=["PID", "Status", "Message"])
    batch_processor.process_pids(pids,
                                 lambda pid, csv_report: reindex_dataset(pid, dataverse_client, csv_report=csv_report,
                                                                         dry_run=args.dry_run))


def reindex_dataset(pid, dataverse_client: DataverseClient, csv_report: CsvReport, dry_run=False):
    try:
        r = dataverse_client.dataset(pid).reindex(dry_run=dry_run)
        csv_report.write({"PID": pid, "Status": "200", "Message": json.dumps(r)})
    except HTTPError as e:
        csv_report.write({"PID": pid, "Status": e.response.status_code, "Message": e.response.text})


def main():
    config = init()
    dataverse = DataverseClient(config['dataverse'])

    parser = argparse.ArgumentParser(description='Reindex one or more datasets.')
    parser.add_argument('pid_or_pid_file', help='The pid or file with pids of the datasets to reindex')
    add_batch_processor_args(parser)
    add_dry_run_arg(parser)

    args = parser.parse_args()
    reindex_datasets(args, dataverse_client=dataverse)


if __name__ == '__main__':
    main()

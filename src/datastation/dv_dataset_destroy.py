import argparse
from datetime import datetime

from datastation.common.batch_processing import BatchProcessor, get_pids, BatchProcessorWithReport
from datastation.common.config import init
from datastation.common.utils import add_batch_processor_args, add_dry_run_arg
from datastation.dataverse.dataverse_client import DataverseClient


def destroy_datasets(args, dataverse_client: DataverseClient, batch_processor: BatchProcessor, dry_run: bool):
    pids = get_pids(args.pid_or_pid_file)
    batch_processor.process_pids(pids, lambda pid, csv_report: destroy_dataset(pid,
                                                                               dataset_api=dataverse_client.dataset(
                                                                                   pid),
                                                                               csv_report=csv_report,
                                                                               dry_run=dry_run))


def destroy_dataset(pid, dataset_api, csv_report, dry_run: bool):
    print("Destroying dataset {}".format(pid))
    dataset_api.destroy(dry_run=dry_run)
    action = "Destroyed"
    csv_report.write({'DOI': pid, 'Modified': datetime.now(), 'Change': action})


def main():
    config = init()

    parser = argparse.ArgumentParser(
        description='Deletes one or more, potentially published, datasets. Requires an API token with superuser '
                    'privileges. Furthermore, the dataverse.safety_latch must be set to OFF.')
    parser.add_argument('pid_or_pid_file', help='The pid or file with pids of the datasets to destroy')
    add_batch_processor_args(parser)
    add_dry_run_arg(parser)
    args = parser.parse_args()

    dataverse_client = DataverseClient(config['dataverse'])
    batch_processor = BatchProcessorWithReport(wait=args.wait, report_file=args.report_file)
    destroy_datasets(args, dataverse_client, batch_processor, dry_run=args.dry_run)


if __name__ == '__main__':
    main()

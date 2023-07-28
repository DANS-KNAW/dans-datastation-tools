import argparse

from datastation.common.batch_processing import get_pids, BatchProcessor
from datastation.common.config import init
from datastation.common.utils import add_batch_proccessor_args, add_dry_run_arg
from datastation.dataverse.datasets import Datasets
from datastation.dataverse.dataverse_client import DataverseClient


def main():
    config = init()
    parser = argparse.ArgumentParser(description="Retrieves attributes of a dataset")

    Datasets.add_attribute_args(parser)

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('pid_or_pids_file', help="The dataset pid, or a file with a list of pids", nargs="?")
    group.add_argument(
        "--all",
        dest="all_datasets",
        action="store_true",
        help="All datasets in the dataverse",
        required=False,
    )

    add_batch_proccessor_args(parser, report=False)
    add_dry_run_arg(parser)
    args = parser.parse_args()

    dataverse_client = DataverseClient(config["dataverse"])

    datasets = Datasets(dataverse_client, dry_run=args.dry_run)
    batch_processor = BatchProcessor(wait=args.wait, fail_on_first_error=args.fail_fast)
    batch_processor.process_pids(get_pids(args.pid_or_pids_file, dataverse_client.search_api(), dry_run=args.dry_run),
                                 lambda pid: datasets.print_dataset_attributes(args, pid))


if __name__ == "__main__":
    main()

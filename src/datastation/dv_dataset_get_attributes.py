import argparse
import logging
from typing import Optional

from datastation.common.batch_processing import BatchProcessor
from datastation.common.config import init
from datastation.common.utils import add_batch_proccessor_args, add_dry_run_arg
from datastation.dataverse.datasets import Datasets
from datastation.dataverse.dataverse_client import DataverseClient


class AttributeOptions:
    user_role: Optional[str] = None
    storage: bool = False


def main():
    config = init()
    parser = argparse.ArgumentParser(description="Retrieves attributes of a dataset")

    parser.add_argument(
        "--user-with-role",
        dest="user_with_role",
        help="List users with a specific role on the dataset",
    )
    parser.add_argument(
        "--storage",
        dest="storage",
        action="store_true",
        help="The storage in bytes",
    )

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("pid", help="The dataset pid", nargs="?")
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

    if args.pid is None:
        result = dataverse_client.search_api().search(dry_run=args.dry_run)
        pids = map(lambda rec: rec['global_id'], result)
    else:
        pids = [args.pid]

    datasets = Datasets(dataverse_client, dry_run=args.dry_run)
    batch_processor = BatchProcessor(wait=args.wait, fail_on_first_error=args.fail_fast)
    batch_processor.process_pids(
        pids, lambda pid: datasets.print_dataset_attributes(args.storage, args.user_with_role, pid))


if __name__ == "__main__":
    main()

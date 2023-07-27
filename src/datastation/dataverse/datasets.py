import json
import logging
from typing import Optional

from datastation.dataverse.dataverse_client import DataverseClient


class Datasets:

    @staticmethod
    def add_attribute_args(parser):
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

    def __init__(self, dataverse_client: DataverseClient, dry_run: bool = False):
        self.dataverse_client = dataverse_client
        self.dry_run = dry_run

    def print_dataset_attributes(self, args, pid: str):
        logging.debug(f"pid={pid}")
        attributes = {"pid": pid}

        dataset_api = self.dataverse_client.dataset(pid)
        if args.storage:
            dataset = dataset_api.get(dry_run=self.dry_run)
            attributes["storage"] = sum(
                f["dataFile"]["filesize"] for f in dataset["files"]
            )

        if args.user_with_role is not None:
            role_assignments = dataset_api.get_role_assignments(dry_run=self.dry_run)
            attributes["users"] = [
                user["assignee"].replace("@", "")
                for user in role_assignments
                if user["_roleAlias"] == args.user_with_role
            ]

        print(json.dumps(attributes, skipkeys=True))
        return

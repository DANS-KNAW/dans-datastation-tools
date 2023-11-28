import json
import logging

from datastation.dataverse.dataverse_client import DataverseClient


class Datasets:

    def __init__(self, dataverse_client: DataverseClient, dry_run: bool = False):
        self.dataverse_client = dataverse_client
        self.dry_run = dry_run

    def update_metadata(self, data: dict):
        dataset_api = self.dataverse_client.dataset(data['PID'])
        fields = []
        for key in data.keys():
            if key != 'PID':
                if '@' in key:
                    raise Exception('Compound fields not yet supported')
                fields.append({'typeName': key, 'value': data[key]})
        logging.debug(fields)
        result = dataset_api.edit_metadata(data=(json.dumps({'fields': fields})), dry_run=self.dry_run)
        logging.debug(result)
        return result

    def get_dataset_attributes(self, pid: str,  storage: bool = False, user_with_role: str = None):
        logging.debug(f"pid={pid}")
        attributes = {"pid": pid}

        dataset_api = self.dataverse_client.dataset(pid)
        if storage:
            dataset = dataset_api.get(dry_run=self.dry_run)
            attributes["storage"] = sum(
                f["dataFile"]["filesize"] for f in dataset["files"]
            )

        if user_with_role is not None:
            role_assignments = dataset_api.get_role_assignments(dry_run=self.dry_run)
            attributes["users"] = [
                user["assignee"].replace("@", "")
                for user in role_assignments
                if user["_roleAlias"] == user_with_role
            ]

        return attributes

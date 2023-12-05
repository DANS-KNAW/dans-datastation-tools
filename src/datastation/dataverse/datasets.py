import json
import logging

from datastation.dataverse.dataverse_client import DataverseClient


class Datasets:

    def __init__(self, dataverse_client: DataverseClient, dry_run: bool = False):
        self.dataverse_client = dataverse_client
        self.dry_run = dry_run

    def update_metadata(self, data: dict, replace: bool = False):
        if 'rest.column' in data.keys():
            logging.error(data)
            raise Exception("Quoting problem or too many values.")
        logging.debug(data)

        type_names = [key for key in data.keys() if key != 'PID' and data[key] is not None]

        compound_fields = {}
        for type_name in [key for key in type_names if '@' in key]:
            parent = type_name.split('@')[0]
            child = type_name.split('@')[1]
            if parent not in compound_fields.keys():
                compound_fields[parent] = {}
            compound_fields[parent][child] = json.loads(data[type_name])
        logging.debug(compound_fields)  # TODO array at parent level, check for  equal lengths

        if len(compound_fields.keys()) > 0:
            raise Exception(f"Compound fields not yet supported: {compound_fields}")

        all_fields = []
        simple_fields = [key for key in type_names if '@' not in key]
        for key in simple_fields:
            if data[key].startswith('['):
                all_fields.append({'typeName': key, 'value': (json.loads(data[key]))})
            else:
                all_fields.append({'typeName': key, 'value': data[key]})
        logging.debug(all_fields)
        dataset_api = self.dataverse_client.dataset(data['PID'])
        result = dataset_api.edit_metadata(data=(json.dumps({'fields': all_fields})), replace=replace, dry_run=self.dry_run)
        logging.info(result)
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

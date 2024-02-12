import requests


from datastation.common.utils import print_dry_run_message


class DataverseApi:
    def __init__(self, server_url, api_token):
        self.server_url = server_url
        self.api_token = api_token

    # get json data for a specific dataverses API endpoint using an API token
    def get_resource_data(self, resource, alias="root", dry_run=False):
        headers = {"X-Dataverse-key": self.api_token}
        url = f"{self.server_url}/api/dataverses/{alias}/{resource}"

        if dry_run:
            print_dry_run_message(method="GET", url=url, headers=headers)
            return None

        dv_resp = requests.get(url, headers=headers)
        raise_for_status(dv_resp)

        resp_data = dv_resp.json()["data"]
        return resp_data

    def get_contents(self, alias="root", dry_run=False):
        return self.get_resource_data("contents", alias, dry_run)

    def get_roles(self, alias="root", dry_run=False):
        return self.get_resource_data("roles", alias, dry_run)

    def get_assignments(self, alias="root", dry_run=False):
        return self.get_resource_data("assignments", alias, dry_run)

    def get_groups(self, alias="root", dry_run=False):
        return self.get_resource_data("groups", alias, dry_run)

    def get_storage_size(self, alias="root", dry_run=False):
        """ Get dataverse storage size (bytes). """
        url = f'{self.server_url}/api/dataverses/{alias}/storagesize'
        headers = {'X-Dataverse-key': self.api_token}
        if dry_run:
            print_dry_run_message(method='GET', url=url, headers=headers)
            return None
        else:
            r = requests.get(url, headers=headers)
        raise_for_status(r)()
        return r.json()['data']['message']

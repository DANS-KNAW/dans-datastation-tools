import requests


from datastation.common.utils import print_dry_run_message


class DataverseApi:
    def __init__(self, server_url, api_token):
        self.server_url = server_url
        self.api_token = api_token

    def get_contents(self, alias="root", dry_run=False):
        headers = {"X-Dataverse-key": self.api_token}
        url = f"{self.server_url}/api/dataverses/{alias}/contents"

        if dry_run:
            print_dry_run_message(method="GET", url=url, headers=headers)
            return None

        dv_resp = requests.get(url, headers=headers)
        dv_resp.raise_for_status()

        resp_data = dv_resp.json()["data"]
        return resp_data

    def get_storage_size(self, alias="root", dry_run=False):
        """ Get dataverse storage size (bytes). """
        url = f'{self.server_url}/api/dataverses/{alias}/storagesize'
        headers = {'X-Dataverse-key': self.api_token}
        if dry_run:
            print_dry_run_message(method='GET', url=url, headers=headers)
            return None
        else:
            r = requests.get(url, headers=headers)
        r.raise_for_status()
        return r.json()['data']['message']

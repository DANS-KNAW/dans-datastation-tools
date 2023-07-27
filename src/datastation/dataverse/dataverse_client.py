from src.datastation.common.database import Database
from src.datastation.dataverse.banner_api import BannerApi
from src.datastation.dataverse.builtin_users import BuiltInUsersApi
from src.datastation.dataverse.dataset_api import DatasetApi
from src.datastation.dataverse.dataverse_api import DataverseApi
from src.datastation.dataverse.file_api import FileApi
from src.datastation.dataverse.metrics_api import MetricsApi
from src.datastation.dataverse.search_api import SearchApi


class DataverseClient:
    """ A client for the Dataverse API. """

    def __init__(self, config: dict):
        self.server_url = config['server_url']
        self.api_token = config['api_token']
        self.unblock_key = config['unblock_key'] if 'unblock_key' in config else None
        self.safety_latch = config['safety_latch']
        self.db_config = config['db']

    def banner(self):
        return BannerApi(self.server_url, self.api_token, self.unblock_key)

    def search_api(self):
        return SearchApi(self.server_url, self.api_token)

    def dataset(self, pid):
        return DatasetApi(pid, self.server_url, self.api_token, self.unblock_key, self.safety_latch)

    def dataverse(self):
        return DataverseApi(self.server_url, self.api_token)

    def file(self, file_id):
        return FileApi(file_id, self.server_url, self.api_token, self.unblock_key, self.safety_latch)

    def built_in_users(self, builtin_users_key):
        return BuiltInUsersApi(self.server_url, self.api_token, builtin_users_key, self.unblock_key)

    def database(self):
        return Database(self.db_config)

    def metrics(self):
        return MetricsApi(self.server_url)

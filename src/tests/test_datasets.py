import pytest

from datastation.dataverse.datasets import Datasets
from datastation.dataverse.dataverse_client import DataverseClient


class TestDatasets:
    url = 'https://demo.archaeology.datastations.nl'
    cfg = {'server_url': url, 'api_token': 'xxx', 'safety_latch': False, 'db': {}}

    def test_update_metadata(self, caplog, capsys):
        caplog.set_level('DEBUG')
        client = DataverseClient(config=self.cfg)
        datasets = Datasets(client, dry_run=True)
        data = {'PID': 'doi:10.5072/FK2/8KQW3Y', 'title': 'New title'}
        datasets.update_metadata(data)
        assert capsys.readouterr().out == ('DRY-RUN: only printing command, not sending it...\n'
                                           f'PUT {self.url}/api/datasets/:persistentId/editMetadata\n'
                                           "headers: {'X-Dataverse-key': 'xxx'}\n"
                                           "params: {'persistentId': 'doi:10.5072/FK2/8KQW3Y', 'replace': 'true'}\n"
                                           'data: {"fields": [{"typeName": "title", "value": "New title"}]}\n'
                                           '\n')
        assert len(caplog.records) == 3

        assert caplog.records[0].message == "{'PID': 'doi:10.5072/FK2/8KQW3Y', 'title': 'New title'}"
        assert caplog.records[0].funcName == 'update_metadata'
        assert caplog.records[0].levelname == 'DEBUG'

        assert caplog.records[1].message == "[{'typeName': 'title', 'value': 'New title'}]"
        assert caplog.records[1].funcName == 'update_metadata'
        assert caplog.records[1].levelname == 'DEBUG'

        assert caplog.records[2].message == 'None'  # without dryrun: <Response [200]>
        assert caplog.records[2].funcName == 'update_metadata'
        assert caplog.records[2].levelname == 'INFO'

    def test_update_metadata_with_repetitive_field(self, caplog, capsys):
        caplog.set_level('DEBUG')
        client = DataverseClient(config=self.cfg)
        datasets = Datasets(client, dry_run=True)
        data = {'PID': 'doi:10.5072/FK2/8KQW3Y', 'author': '["me","O\'Neill"]'}

        datasets.update_metadata(data)
        assert capsys.readouterr().out == ('DRY-RUN: only printing command, not sending it...\n'
                                           f'PUT {self.url}/api/datasets/:persistentId/editMetadata\n'
                                           "headers: {'X-Dataverse-key': 'xxx'}\n"
                                           "params: {'persistentId': 'doi:10.5072/FK2/8KQW3Y', 'replace': 'true'}\n"
                                           'data: {"fields": [{"typeName": "author", "value": ["me", "O\'Neill"]}]}\n'
                                           '\n')
        assert len(caplog.records) == 3
        assert caplog.records[0].message == "{'PID': 'doi:10.5072/FK2/8KQW3Y', 'author': '[\"me\",\"O\\'Neill\"]'}"
        assert caplog.records[1].message == '[{\'typeName\': \'author\', \'value\': [\'me\', "O\'Neill"]}]'

    def test_update_metadata_with_subfield(self, caplog, capsys):
        caplog.set_level('DEBUG')
        client = DataverseClient(config=self.cfg)
        datasets = Datasets(client, dry_run=True)
        data = {'PID': 'doi:10.5072/FK2/8KQW3Y',
                'author@authorName': 'me',
                'author@authorAffiliation': 'my organization'}

        with pytest.raises(Exception) as e:
            datasets.update_metadata(data)
        assert str(e.value) == 'Subfields not yet supported.'

        # test driven expectations below

        # datasets.update_metadata(data)
        # assert capsys.readouterr().out == ('DRY-RUN: only printing command, not sending it...\n'
        #                                    f'PUT {self.url}/api/datasets/:persistentId/editMetadata\n'
        #                                    "headers: {'X-Dataverse-key': 'xxx'}\n"
        #                                    "params: {'persistentId': 'doi:10.5072/FK2/8KQW3Y', 'replace': 'true'}\n"
        #                                    'data: {"fields": [{"typeName": "author", "value": [{"authorName": "me", '
        #                                    '"authorAffiliation": "my organization"}]}]}\n'
        #                                    '\n')

        assert len(caplog.records) == 1
        assert caplog.records[0].message == "{'PID': 'doi:10.5072/FK2/8KQW3Y', 'author@authorName': 'me', " \
                                            "'author@authorAffiliation': 'my organization'}"
        # assert caplog.records[1].message == "[{'typeName': 'author', 'value': [{'authorName': 'me', " \
        #                                     "'authorAffiliation': 'my organization'}]}]"

    def test_update_metadata_with_invalid_quotes_for_repetitive_fields(self, caplog, capsys):
        caplog.set_level('DEBUG')
        client = DataverseClient(config=self.cfg)
        datasets = Datasets(client, dry_run=True)
        data = {'PID': 'doi:10.5072/FK2/8KQW3Y', 'title': 'New title', 'author': "['me','you']"}

        with pytest.raises(Exception) as e:
            datasets.update_metadata(data)
        assert str(e.value) == 'Expecting value: line 1 column 2 (char 1)'  # wants double quotes gets single quotes
        assert str(e.type) == "<class 'json.decoder.JSONDecodeError'>"

        assert capsys.readouterr().out == ''
        assert len(caplog.records) == 1
        assert caplog.records[0].levelname == 'DEBUG'
        assert (caplog.records[0].message ==
                "{'PID': 'doi:10.5072/FK2/8KQW3Y', 'title': 'New title', 'author': " '"[\'me\',\'you\']"}')

    def test_update_metadata_with_too_many_values(self, caplog, capsys):
        caplog.set_level('DEBUG')
        client = DataverseClient(config=self.cfg)
        datasets = Datasets(client, dry_run=True)
        data = {'PID': 'doi:10.5072/FK2/8KQW3Y', 'title': 'New title', 'author': 'me', 'rest.column': 'you'}
        with pytest.raises(Exception) as e:
            datasets.update_metadata(data)
        assert str(e.value) == 'Quoting problem or too many values.'
        assert capsys.readouterr().out == ''
        assert len(caplog.records) == 1
        assert caplog.records[0].levelname == 'ERROR'
        assert (caplog.records[0].message ==
                "{'PID': 'doi:10.5072/FK2/8KQW3Y', 'title': 'New title', 'author': 'me', 'rest.column': 'you'}")



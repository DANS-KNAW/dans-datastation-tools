import pytest

from datastation.dataverse.datasets import Datasets
from datastation.dataverse.dataverse_client import DataverseClient


class TestDatasets:
    url = 'https://demo.archaeology.datastations.nl'
    cfg = {'server_url': url, 'api_token': 'xxx', 'safety_latch': 'ON', 'db': {}}

    def test_update_metadata(self, caplog, capsys):
        caplog.set_level('INFO')
        client = DataverseClient(config=self.cfg)
        datasets = Datasets(client, dry_run=True)
        data = {'PID': 'doi:10.5072/FK2/8KQW3Y', 'title': 'New title'}
        datasets.update_metadata(data, replace=True)
        assert capsys.readouterr().out == ('DRY-RUN: only printing command, not sending it...\n'
                                           f'PUT {self.url}/api/datasets/:persistentId/editMetadata\n'
                                           "headers: {'X-Dataverse-key': 'xxx'}\n"
                                           "params: {'persistentId': 'doi:10.5072/FK2/8KQW3Y', 'replace': 'true'}\n"
                                           'data: {"fields": [{"typeName": "title", "value": "New title"}]}\n'
                                           '\n')
        assert len(caplog.records) == 1

        assert caplog.records[0].message == 'None'  # without dryrun: <Response [200]>
        assert caplog.records[0].funcName == 'update_metadata'
        assert caplog.records[0].levelname == 'INFO'

    def test_update_metadata_with_repetitive_field_without_replacing(self, caplog, capsys):
        caplog.set_level('INFO')
        client = DataverseClient(config=self.cfg)
        datasets = Datasets(client, dry_run=True)
        data = {'PID': 'doi:10.5072/FK2/8KQW3Y', 'dansRightsHolder': '["me","O\'Neill"]'}

        datasets.update_metadata(data, replace=False)
        assert (capsys.readouterr().out ==
                ('DRY-RUN: only printing command, not sending it...\n'
                 f'PUT {self.url}/api/datasets/:persistentId/editMetadata\n'
                 "headers: {'X-Dataverse-key': 'xxx'}\n"
                 "params: {'persistentId': 'doi:10.5072/FK2/8KQW3Y'}\n"
                 'data: {"fields": [{"typeName": "dansRightsHolder", "value": ["me", "O\'Neill"]}]}\n'
                 '\n'))
        assert len(caplog.records) == 1
        assert (caplog.records[0].message == 'None')

    def test_update_metadata_with_repetitive_compound_field(self, caplog, capsys):
        caplog.set_level('INFO')
        client = DataverseClient(config=self.cfg)
        datasets = Datasets(client, dry_run=True)
        data = {'PID': 'doi:10.5072/FK2/8KQW3Y',
                'author@authorName': '["me","you"]',
                'author@authorAffiliation': '["mine","yours"]'}

        datasets.update_metadata(data)

        assert (capsys.readouterr().out ==
                ('DRY-RUN: only printing command, not sending it...\n'
                 f'PUT {self.url}/api/datasets/:persistentId/editMetadata\n'
                 "headers: {'X-Dataverse-key': 'xxx'}\n"
                 "params: {'persistentId': 'doi:10.5072/FK2/8KQW3Y'}\n"
                 'data: {"fields": [{"typeName": "author", "value": ['
                 '{"authorName": {"typeName": "authorName", "value": "me"}, '
                 '"authorAffiliation": {"typeName": "authorAffiliation", "value": "mine"}}, '
                 '{"authorName": {"typeName": "authorName", "value": "you"}, '
                 '"authorAffiliation": {"typeName": "authorAffiliation", "value": "yours"}}'
                 ']}]}\n\n'))

        assert len(caplog.records) == 1
        assert caplog.records[0].message == 'None'

    def test_update_metadata_with_single_compound_field(self, caplog, capsys):
        caplog.set_level('INFO')
        client = DataverseClient(config=self.cfg)
        datasets = Datasets(client, dry_run=True)
        data = {'PID': 'doi:10.5072/FK2/8KQW3Y',
                'socialScienceNotes@socialScienceNotesType': 'p',
                'socialScienceNotes@socialScienceNotesSubject': 'q',
                'socialScienceNotes@socialScienceNotesText': 'r'}

        datasets.update_metadata(data)
        assert (capsys.readouterr().out ==
                ('DRY-RUN: only printing command, not sending it...\n'
                 f'PUT {self.url}/api/datasets/:persistentId/editMetadata\n'
                 "headers: {'X-Dataverse-key': 'xxx'}\n"
                 "params: {'persistentId': 'doi:10.5072/FK2/8KQW3Y'}\n"
                 'data: {"fields": [{"typeName": "socialScienceNotes", "value": '
                 '{"socialScienceNotesType": {"typeName": "socialScienceNotesType", "value": "p"}, '
                 '"socialScienceNotesSubject": {"typeName": "socialScienceNotesSubject", "value": "q"}, '
                 '"socialScienceNotesText": {"typeName": "socialScienceNotesText", "value": "r"}}'
                 '}]}\n\n'))
        assert len(caplog.records) == 1
        assert caplog.records[0].message == 'None'

    def test_update_metadata_with_invalid_quotes_for_repetitive_fields(self, caplog, capsys):
        caplog.set_level('DEBUG')
        client = DataverseClient(config=self.cfg)
        datasets = Datasets(client, dry_run=True)
        data = {'PID': 'doi:10.5072/FK2/8KQW3Y', 'title': 'New title', 'author': "['me','you']"}

        with pytest.raises(Exception) as e:
            datasets.update_metadata(data, replace=True)
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

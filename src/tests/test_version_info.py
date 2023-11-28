from unittest.mock import patch

from datastation.common.version_info import get_rpm_versions, get_dataverse_version, get_payara_version


def test_some_modules_with_matching_prefix_found():
    with patch('datastation.common.version_info.rpm_qa') as mock_qa:
        mock_qa.return_value = ['dans.knaw.nl-dd-vault-metadata-2.2.0-1.noarch',
                                'dans.knaw.nl-dans-schema-0.10.0-1.noarch',
                                'python3-rpm-generators-5-8.el8.noarch',
                                'dans.knaw.nl-dd-verify-dataset-0.10.0-1.noarch',
                                ]
        versions = get_rpm_versions('dans.knaw.nl-')
        assert versions == {
            'dans.knaw.nl-dd-vault-metadata': '2.2.0',
            'dans.knaw.nl-dans-schema': '0.10.0',
            'dans.knaw.nl-dd-verify-dataset': '0.10.0'
        }


def test_no_matching_modules_found():
    with patch('datastation.common.version_info.rpm_qa') as mock_qa:
        mock_qa.return_value = ['python3-rpm-generators-5-8.el8.noarch']
        versions = get_rpm_versions('dans.knaw.nl-')
        assert versions == {}


def test_get_dataverse_version():
    with patch('builtins.open') as mock_open:
        mock_open.return_value.__enter__.return_value = ['dataverse.version=5.0.1\n']
        version = get_dataverse_version('/opt/dv/application')
        assert version == '5.0.1'


def test_get_payara_version():
    with patch('builtins.open') as mock_open:
        mock_open.return_value.__enter__.return_value = ['Thank you for downloading Payara Server 5.2021.1!\n']
        version = get_payara_version('/opt/payara')
        assert version == '5.2021.1'

import logging
import os
from re import match


def rpm_qa():
    return os.popen('rpm -qa')


evr_pattern = r'(?P<name>.*?)-(?P<version>\d+\.\d+\.\d+)-(?P<release>[^\.]+)\.(?P<arch>.+)'
payara_version_pattern = r'Thank you for downloading Payara Server (.*).'


def get_rpm_versions(prefix):
    """Get the versions of the RPMs installed on the system."""
    rpm_versions = {}
    for line in rpm_qa():
        if line.startswith(prefix):
            logging.debug(f'Found RPM matching prefix {prefix}: {line}')
            try:
                evr = match(evr_pattern, line)
                version = evr.group('version')
                module = evr.group('name')
                release = evr.group('release')
                # Adding the release part of the EVR is not very informative for DANS modules,
                # because it is always 1 for releases, except for SNAPSHOTs, in which case it is
                # important to know the exact SNAPSHOT version.
                rpm_versions[module] = f'{version}-{release}' if release != '1' else f'{version}'
            except:
                rpm_versions[module] = 'ERROR'

    return rpm_versions


def get_dataverse_version(dataverse_application_path):
    with open(os.path.join(dataverse_application_path, 'WEB-INF', 'classes', 'META-INF',
                           'microprofile-config.properties'), 'r') as f:
        try:
            for line in f:
                if 'dataverse.version' in line:
                    return (line.split('=')[1]).strip()
        except:
            return 'ERROR'


def get_dataverse_build_number(dataverse_application_path):
    with open(os.path.join(dataverse_application_path, 'WEB-INF', 'classes', 'BuildNumber.properties'), 'r') as f:
        try:
            for line in f:
                if 'build.number' in line:
                    return (line.split('=')[1]).strip()
        except:
            return 'ERROR'


def get_payara_version(payara_application_path):
    try:
        with open(os.path.join(payara_application_path, 'README.txt'), 'r') as f:
            line = next((line for line in f if match(payara_version_pattern, line)), None)
            payara_version = match(payara_version_pattern, line).group(1)
        return payara_version
    except:
        return 'ERROR'

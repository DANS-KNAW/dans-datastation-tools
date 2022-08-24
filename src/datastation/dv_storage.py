import json
import logging
import os.path
from uuid import uuid4
from datetime import datetime
from os import walk, path
from shutil import copy
import hashlib


# Taken from edu.harvard.iq.dataverse.DataFileServiceBean#generateStorageIdentifier()
def generate_storage_identifier():
    uuid = str(uuid4())
    hexRandom = uuid[-12:]
    hexTimestamp = hex(int(datetime.now().timestamp() * 1000))[2:]  # remove 0x from beginning
    return "{}-{}".format(hexTimestamp, hexRandom)


def calc_sha1(file):
    sha1sum = hashlib.sha1()
    with open(file, 'rb') as source:
        block = source.read(2 ** 16)
        while len(block) != 0:
            sha1sum.update(block)
            block = source.read(2 ** 16)
    return sha1sum.hexdigest()


def new_file_json_data_creator(inputroot=None):
    logging.info("Creating directory names relative to: {}".format(inputroot))
    def create_json_data_for_file(inputfile, storage_identifier):
        if inputroot is not None:
            dir_label = os.path.dirname(os.path.relpath(inputfile, inputroot))
        else:
            dir_label = ""

        d = {
            "directoryLabel": dir_label,
            "fileName": os.path.basename(inputfile),
            "description": "",
            "categories": [],
            "restrict": "false",
            "storageIdentifier": "file://{}".format(storage_identifier),
            "mimeType": "application/octet-stream",
            "checksum": {
                "@type": "SHA-1",
                "@value": calc_sha1(inputfile)
            }
        }
        print(json.dumps(d))

    return create_json_data_for_file


def prestage_file(fileroot, doi, f, create_json_data):
    storage_id = generate_storage_identifier()
    dest = path.join(fileroot, doi, storage_id)
    logging.debug("Copying {} to {}".format(f, dest))
    copy(f, dest)
    create_json_data(f, storage_id)


def prestage_files(fileroot, doi, dir, create_json_data):
    print("[")
    first = True
    for root, _, files in walk(dir):
        for f in files:
            if (not first):
                print(", ")
            prestage_file(fileroot, doi, os.path.join(root, f), create_json_data)
            first = False
    print("]")

def ensure_doi_directory_exists(fileroot, doi):
    doi_dir = path.join(fileroot, doi)
    if os.path.isdir(doi_dir):
        logging.debug("DOI directory already exists: {}".format(doi_dir))
    else:
        logging.info("Creating new DOI directory: {}".format(doi_dir))
        os.makedirs(doi_dir)

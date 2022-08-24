import argparse
import logging
import os.path
import sys

from datastation.config import init
from datastation.dv_storage import prestage_file, prestage_files, ensure_doi_directory_exists, new_file_json_data_creator


def main():
    config = init()

    parser = argparse.ArgumentParser(
        description='Prestages files in the Dataverse storage directory')
    parser.add_argument('-d', '--doi', dest='doi', help='the dataset DOI', required=True)
    parser.add_argument('file',
                        help='file to prestage; if it is a directory, all files in it will be prestaged recursively')
    args = parser.parse_args()

    file_or_dir = os.path.expanduser(args.file)
    if not os.path.exists(file_or_dir):
        print('File or directory not found: {}'.format(file_or_dir), file=sys.stderr)
        exit(1)

    if args.doi.startswith('doi:'):
        logging.info("Removing 'doi:' prefix from DOI")
        doi = args.doi[4:]
    else:
        doi = args.doi

    files_root = config['dataverse']['files_root']
    ensure_doi_directory_exists(files_root, doi)

    if os.path.isfile(file_or_dir):
        prestage_file(files_root, doi, file_or_dir, new_file_json_data_creator())
    elif os.path.isdir(file_or_dir):
        prestage_files(files_root, doi, file_or_dir, new_file_json_data_creator(file_or_dir))


if __name__ == '__main__':
    main()

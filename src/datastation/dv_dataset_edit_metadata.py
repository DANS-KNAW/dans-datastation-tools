import csv
import re
from argparse import ArgumentParser
from csv import DictReader
from os.path import expanduser, isfile

from datastation.common.batch_processing import BatchProcessor
from datastation.common.config import init
from datastation.common.utils import add_batch_processor_args, add_dry_run_arg
from datastation.dataverse.datasets import Datasets
from datastation.dataverse.dataverse_client import DataverseClient


def main():
    config = init()
    parser = ArgumentParser(description='Edits one or more, potentially published, datasets. Requires an API token.')
    parser.add_argument('-r', '--replace', dest="replace", action='store_true',
                        help="Replace existing metadata fields with the new metadata. "
                             "Required for non-repetitive fields. "
                             "Note that without 'replace' an existing value of a repetitive field is not duplicated. "
                        )
    parser.add_argument('pid_or_file',
                        help="Either a CSV file or the PID of the dataset to edit. "
                             "One column of the file MUST have title 'PID'. "
                             "The other columns MUST have a typeName, as for the --value argument.")
    parser.add_argument('-v', '--value', action='append',
                        help="At least once in combination with a PID, none in combination with a CSV file. "
                             "The new values for fields must be formatted as <typeName>=<value>. "
                             "For example: title='New title'. "
                             "A subfield in a compound field must be prefixed with "
                             "the typeName of the compound field and an @ sign, for example: "
                             "--value author@authorName='the name' "
                             "--value author@authorAffiliation='the organization'. "
                             "Only repetitive compound fields are supported")
    parser.add_argument('-q', '--quote_char', dest="quote_char", default='"',
                        help="The quote character for a CSV file. The default is '\"'. "
                             "The quoting style on the command line for repetitive fields "
                             "is <typeName>='[\"<value>\"]', "
                             'for example: -v dansRightsHolder=''["me","O\'Neill"]''. '
                             'The default quoting style in a CSV matches plugins for Intellij and PyCharm: '
                             '"[""me"",""O''Neill""]". With quote_char "\'" it becomes:' "'[\"me\",\"O''Neill\"]'")
    add_batch_processor_args(parser, report=False)
    add_dry_run_arg(parser)
    args = parser.parse_args()

    def run(obj_list):
        client = DataverseClient(config['dataverse'])
        datasets = Datasets(client, dry_run=args.dry_run)
        batch_processor = BatchProcessor(wait=args.wait, fail_on_first_error=args.fail_fast)
        batch_processor.process_pids(obj_list, lambda obj: datasets.update_metadata(data=obj, replace=args.replace))

    def validate_fieldnames(fieldnames, suffix=' in ' + args.pid_or_file):
        if 'PID' not in fieldnames:
            parser.error(f"No column 'PID' found" + suffix)

        if len(fieldnames) < 2:
            parser.error(f"No values specified" + suffix)

        pat = re.compile('([-a-zA-Z]+)?[-a-zA-Z]+')
        for name in fieldnames:
            if not pat.match(name):
                parser.error(f"Invalid typeName {name}" + suffix)

    def parse_value_args():
        obj = {'PID': args.pid_or_file}
        for kv in args.value:
            key_value = kv.split('=')
            obj[key_value[0]] = key_value[1]
        validate_fieldnames(obj.keys(), suffix='')
        return [obj]

    if isfile(expanduser(args.pid_or_file)):
        if args.value is not None:
            parser.error("-v/--value arguments not allowed in combination with CSV file: " + args.pid_or_file)
        with open(args.pid_or_file, newline='') as csvfile:
            reader = DictReader(csvfile, quotechar=args.quote_char, delimiter=',', quoting=csv.QUOTE_MINIMAL,
                                skipinitialspace=True, restkey='rest.column', escapechar=None)
            validate_fieldnames(reader.fieldnames)
            run(reader)
    else:
        run(parse_value_args())


if __name__ == '__main__':
    main()

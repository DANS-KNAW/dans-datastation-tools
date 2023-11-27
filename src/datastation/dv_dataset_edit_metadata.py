import argparse
from csv import DictReader
from os.path import expanduser, isfile

from datastation.common.batch_processing import BatchProcessor
from datastation.common.config import init
from datastation.common.utils import add_batch_processor_args, add_dry_run_arg
from datastation.dataverse.dataverse_client import DataverseClient


def edit_dataset(row, dataverse_client, dry_run):
    type_names = filter(lambda n: n != 'PID', row.keys())
    fields = []
    for key in type_names:
        if key != 'PID':
            fields.append({'typeName': key, 'value': row[key]})
    print(row['PID'], fields)


def main():
    config = init()
    parser = argparse.ArgumentParser(
        description='Edits  one or more, potentially published, datasets. Requires an API token.')
    parser.add_argument('pid_or_file',
                        help="Either a CSV file or the PID of the dataset to edit. "
                             "The first column of the file has title 'PID'."
                             "The other columns have a typeName as for the --value argument.")
    parser.add_argument('-v', '--values', dest='values', nargs='+',
                        help="The new values for fields "
                             "formatted as <typeName>=<value> for example title='New title'. "
                             "A subfield in a compound field must be prefixed with "
                             "the typeName of the compound field and an @ sign, e.g. "
                             "--value author@authorName='the name' "
                             "--value author@authorAffiliation='the organization'")
    add_batch_processor_args(parser, report=False)
    add_dry_run_arg(parser)
    args = parser.parse_args()

    dataverse_client = DataverseClient(config['dataverse'])

    def run(obj):
        edit_dataset(obj, dataverse_client, args.dry_run)

    def get_arg_values():
        obj = {'PID': args.pid_or_file}
        for kv in args.values:
            key_value = kv.split('=')
            obj[key_value[0]] = key_value[1]
        return obj

    batch_processor = BatchProcessor(wait=1, fail_on_first_error=args.fail_fast)
    if isfile(expanduser(args.pid_or_file)):
        with open(args.pid_or_file, newline='') as csvfile:
            batch_processor.process_pids(DictReader(csvfile), run)
    else:
        batch_processor.process_pids([get_arg_values()], run)


if __name__ == '__main__':
    main()

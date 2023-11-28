import re
from argparse import ArgumentParser, ArgumentTypeError
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
    parser = ArgumentParser(
        description='Edits one or more, potentially published, datasets. Requires an API token.')
    parser.add_argument('pid_or_file',
                        help="Either a CSV file or the PID of the dataset to edit. "
                             "One column column of the file has title 'PID'. "
                             "The other columns have a typeName, as for the --value argument.")
    parser.add_argument('-v', '--value', action='append',
                        help="At least once in combination with a PID, not allowed in combination with CSV file. "
                             "The new values for fields formatted as "
                             "<typeName>=<value> for example title='New title'. "
                             "A subfield in a compound field must be prefixed with "
                             "the typeName of the compound field and an @ sign, e.g. "
                             "--value author@authorName='the name' "
                             "--value author@authorAffiliation='the organization'")
    add_batch_processor_args(parser, report=False)
    add_dry_run_arg(parser)
    args = parser.parse_args()

    def run(obj_list):
        dataverse_client = DataverseClient(config['dataverse'])
        batch_processor = BatchProcessor(wait=args.wait, fail_on_first_error=args.fail_fast)
        batch_processor.process_pids(obj_list, lambda obj: edit_dataset(obj, dataverse_client, args.dry_run))

    def validate_fieldnames(fieldnames):
        # redundant check when called from parse_value_args but should not hurt
        if 'PID' not in fieldnames:
            parser.error(f"No column 'PID' found in {args.pid_or_file}")

        pat = re.compile('([-a-fA-F]@)?[-a-fA-F]+')
        for name in reader.fieldnames:
            if not pat.match(pat):
                parser.error(f"Invalid typeName {name}")

    def parse_value_args():
        obj = {'PID': args.pid_or_file}
        for kv in args.value:
            key_value = kv.split('=')
            obj[key_value[0]] = key_value[1]
        validate_fieldnames(obj.keys())
        return [obj]

    if isfile(expanduser(args.pid_or_file)):
        if args.value is not None:
            parser.error("-v/--value arguments not allowed in combination with CSV file: " + args.pid_or_file)
        with open(args.pid_or_file, newline='') as csvfile:
            reader = DictReader(csvfile)
            validate_fieldnames(reader.fieldnames)
            run(reader)
    else:
        if args.value is None:
            parser.error("At least one -v/--value arguments is required in combination with a PID: " + args.pid_or_file)
        run(parse_value_args())


if __name__ == '__main__':
    main()

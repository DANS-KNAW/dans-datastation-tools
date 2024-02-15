import argparse
import os

from datastation.common.batch_processing import BatchProcessor, get_pids
from datastation.common.config import init
from datastation.common.utils import add_batch_processor_args, add_dry_run_arg
from datastation.dataverse.dataverse_client import DataverseClient

exporter_to_extension = {
    'dcterms': 'xml',
    'ddi': 'xml',
    'Datacite': 'xml',
    'html': 'html',
    'dataverse_json': 'json',
    'oai_dc': 'xml',
    'OAI_ORE': 'xml',
    'oai_datacite': 'xml',
    'schema.org': 'json'
}


def get_metadata_export(args, pid, dataverse):
    result = dataverse.dataset(pid).get_metadata_export(dry_run=args.dry_run, exporter=args.exporter)
    if args.output_dir:
        if not os.path.exists(args.output_dir):
            os.makedirs(args.output_dir)
        pid = pid.replace('/', '_')
        with open(f'{args.output_dir}/{pid}.{exporter_to_extension[args.exporter]}', 'w') as f:
            f.write(result)
    else:
        print(result)


def main():
    config = init()
    dataverse = DataverseClient(config['dataverse'])

    parser = argparse.ArgumentParser(description='Get metadata export for a dataset. Note that Dataverse currently '
                                                 'only supports getting metadata exports for the latest published '
                                                 'version of a dataset.')

    parser.add_argument('pid_or_pids_file',
                        help='the pid of the dataset to get the metadata export for, or a file with a list of pids')
    parser.add_argument('-e', '--exporter', default='dataverse_json',
                        help=f"the exporter to use (one of: {', '.join(exporter_to_extension.keys())}; "
                             f"default: dataverse_json)",
                        dest='exporter')
    parser.add_argument('-o', '--output-dir', help="the output directory where the exported metadata files will be "
                                                   "stored. If not provided, the files will be dumped to stdout. If "
                                                   "the directory does not exist, it will be created.",
                        dest='output_dir')
    add_batch_processor_args(parser)
    add_dry_run_arg(parser)

    args = parser.parse_args()
    batch_processor = BatchProcessor(wait=args.wait, fail_on_first_error=args.fail_fast)
    pids = get_pids(args.pid_or_pids_file)
    batch_processor.process_pids(pids,
                                 callback=lambda pid: get_metadata_export(args, pid, dataverse))


if __name__ == '__main__':
    main()

import csv
import sys


def open_csv_file(headers, output_file):
    csv_file = None
    if output_file == '-':
        csv_writer = csv.DictWriter(sys.stdout, headers)
    else:
        csv_file = open(output_file, 'w')
        csv_writer = csv.DictWriter(csv_file, headers)

    csv_writer.writeheader()
    return csv_file, csv_writer

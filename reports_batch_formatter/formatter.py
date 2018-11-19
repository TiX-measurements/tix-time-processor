import argparse
import tarfile
import tempfile
import sys

from os import path, listdir, makedirs, unlink
from os.path import join

import logging
from shutil import copy

sys.path.insert(0, '../')
from processor import reports

logger = logging.getLogger(__name__)
temp_dir = None


def create_batch_dir(working_directory, reports_handler):
    batch_dir_name = str(reports_handler.processable_reports[0].observations[0].day_timestamp)
    batch_dir_path = join(working_directory, batch_dir_name)
    makedirs(batch_dir_path)
    for report in reports_handler.processable_reports:
        src = report.file_path
        dst = batch_dir_path
        copy(src, dst)


def reshape_results(working_directory):
    reports_handler = reports.ReportHandler(working_directory)
    reports_handler.update_processable_reports()
    while len(reports_handler.processable_reports) > 0 and \
            reports_handler.MINIMUM_OBSERVATIONS_QTY <= reports_handler.calculate_observations_quantity(reports_handler.processable_reports):
        create_batch_dir(working_directory, reports_handler)
        reports_handler.delete_unneeded_reports()
        reports_handler.update_processable_reports()
    if len(reports_handler.processable_reports) > 0 and \
        reports_handler.calculate_observations_quantity(reports_handler.processable_reports) < reports_handler.MINIMUM_OBSERVATIONS_QTY:
        create_batch_dir(working_directory, reports_handler)
        for report in reports_handler.processable_reports:
            unlink(report.file_path)

def parse_args(raw_args=None):
    parser = argparse.ArgumentParser(description='Script to shape the report files from the tix-time-condenser into '
                                                 'the batches. This is to imitate the way the tix-time-processor takes '
                                                 'the files and computes them by separating them into different '
                                                 'directories. The idea behind this is to use the files for '
                                                 'exploratory analysis.')
    parser.add_argument('--source_directory',
                        help='The path to the directory where the reports are.')
    parser.add_argument('--output', '-o', action='store', default='batch-test-report.tar.gz', type=str,
                        help='The name of the output file. By default "batch-test-report.tar.gz".')
    args = parser.parse_args(raw_args)
    return args

if __name__ == "__main__":
    args = parse_args()
    logger.debug(args)
    # abs_file_path = path.abspath(args.file_path)
    # abs_output_path = path.abspath(args.output)
    abs_source_path = path.abspath(args.source_directory)
    abs_output_path = path.abspath(args.output)
    reshape_results(abs_source_path)
    logger.info("Creating output TAR.")
    tar = tarfile.open(abs_output_path, mode='w:gz')
    tar.add(abs_source_path, arcname='')
    tar.close()
    logger.info("Output TAR successfully created.")
    if temp_dir is not None:
        temp_dir.cleanup()
        logger.info("Temporary directory destroyed.")

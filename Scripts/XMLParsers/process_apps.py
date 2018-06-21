from datetime import datetime
import logging
import logging.config
import os

import settings
from application import parser

INPUT_DIR = settings.APP_XMLDIR
OUTPUT_DIR = settings.APP_CSVDIR


def create_logger():
    log_file = 'application_' + str(datetime.now().strftime('%Y-%m-%d')) + '.log'
    logging.config.fileConfig('log.ini', defaults={'logfilename': log_file})
    return logging.getLogger(__name__)


logger = create_logger()

if __name__ == '__main__':
    parser.prepare_output_dir(OUTPUT_DIR)
    parser.parse_patents(INPUT_DIR, OUTPUT_DIR)

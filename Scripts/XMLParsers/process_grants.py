from datetime import datetime
import logging
import logging.config
import os

import settings
from grant import generic_parser_2005

INPUT_DIR = settings.GRANT_XMLDIR
OUTPUT_DIR = settings.GRANT_CSVDIR


def create_logger():
    log_file = 'grant_' + str(datetime.now().strftime('%Y-%m-%d')) + '.log'
    logging.config.fileConfig('log.ini', defaults={'logfilename': log_file})
    return logging.getLogger(__name__)


logger = create_logger()

if __name__ == '__main__':
    # generic_parser_2005.prepare_output_dir(OUTPUT_DIR)
    generic_parser_2005.parse_patents(INPUT_DIR, OUTPUT_DIR)

import os

from dotenv import load_dotenv
load_dotenv()

MAIN_DIR = os.path.abspath(os.path.join(os.path.abspath(__file__), os.pardir))
APP_XMLDIR = os.path.join(MAIN_DIR, 'data_dirs/xmls/a')
APP_CSVDIR = os.path.join(MAIN_DIR, 'data_dirs/csvs/a')
GRANT_XMLDIR = os.path.join(MAIN_DIR, 'data_dirs/xmls/g')
GRANT_CSVDIR = os.path.join(MAIN_DIR, 'data_dirs/csvs/g')

import os
from dotenv import load_dotenv

dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
load_dotenv(dotenv_path)

MAIN_DIR = os.path.abspath(os.path.join(os.path.abspath(__file__), os.pardir))
APP_XMLDIR = os.path.join(MAIN_DIR, 'data_dirs', 'xmls', 'a')
APP_CSVDIR = os.path.join(MAIN_DIR, 'data_dirs', 'csvs', 'a')
GRANT_XMLDIR = os.path.join(MAIN_DIR, 'data_dirs', 'xmls', 'g')
GRANT_CSVDIR = os.path.join(MAIN_DIR, 'data_dirs', 'csvs', 'g')

DB_USER = os.getenv("DB_USER") or 'postgres'
DB_PASSWORD = os.getenv("DB_PASSWORD") or None
DB_HOST = os.getenv("DB_HOST") or '127.0.0.1'
DB_NAME = os.getenv("DB_NAME") or 'patent'


config = {
    'user': DB_USER,
    'password': DB_PASSWORD,
    'host': DB_HOST,
    'dbname': DB_NAME,
}

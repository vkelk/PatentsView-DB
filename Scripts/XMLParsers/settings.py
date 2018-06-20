import os

from dotenv import load_dotenv
load_dotenv()

XML_DIR = os.getenv('XML_DIR', './xml_dir')

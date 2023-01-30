# absolute path for reading config
import os
from pathlib import Path

# read ini file
import configparser
config = configparser.ConfigParser()
config_path = os.path.join(Path(__file__).parent, 'config.ini')
config.read(config_path)

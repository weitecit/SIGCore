import yaml
from pathlib import Path
import os

ROOT_FOLDER = Path(os.path.dirname(os.path.abspath(__file__))).parent
CONFIG_PATH = ROOT_FOLDER / "config.yaml"

LOGS_FOLDER = ROOT_FOLDER / "logs"
ASSETS_FOLDER = ROOT_FOLDER / 'Assets'

with open(CONFIG_PATH, "r") as f:
    config_file = yaml.safe_load(f)

ENVIRONMENT = config_file["ENVIRONMENT"]
MONGO_STRING = config_file["MONGO_STRING_PRO"] if ENVIRONMENT == "pro" else config_file["MONGO_STRING_DEV"]


REQUIRED_COLUMNS = config_file.get('REQUIRED_XLS_COLUMNS', [])
SIGPAC_YEAR = config_file.get('SIGPAC_YEAR', 2023)
COG_PROFILE = config_file.get('COG_PROFILE', 'webp')

os.makedirs(LOGS_FOLDER, exist_ok=True)
os.makedirs(ASSETS_FOLDER, exist_ok=True)

print('ENVIRONMENT set to ->', ENVIRONMENT)

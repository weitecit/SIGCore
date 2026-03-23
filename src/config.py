import yaml
from pathlib import Path
import os
import dotenv

ROOT_FOLDER = Path(os.path.dirname(os.path.abspath(__file__))).parent

LOGS_FOLDER = ROOT_FOLDER / "logs"
ASSETS_FOLDER = ROOT_FOLDER / 'Assets'
CONFIG_FOLDER = ROOT_FOLDER / 'config'
CONFIG_YAML_PATH = CONFIG_FOLDER / "config.yaml"

dotenv.load_dotenv(dotenv_path=CONFIG_FOLDER / ".env")
with open(CONFIG_YAML_PATH, "r") as f: config_yaml_file = yaml.safe_load(f)

ENVIRONMENT = os.getenv("ENVIRONMENT")
MONGO_STRING = os.getenv("MONGO_STRING")
REQUIRED_COLUMNS = config_yaml_file.get('REQUIRED_XLS_COLUMNS', [])

os.makedirs(LOGS_FOLDER, exist_ok=True)
os.makedirs(ASSETS_FOLDER, exist_ok=True)

print('ENVIRONMENT set to ->', ENVIRONMENT)

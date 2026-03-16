import importlib


def test_import_config_and_basic_attributes():
    Config = importlib.import_module('config')
    # Basic attributes expected from Config
    assert hasattr(Config, 'ROOT_FOLDER')
    assert hasattr(Config, 'ASSETS_FOLDER')
    assert hasattr(Config, 'ENVIRONMENT')




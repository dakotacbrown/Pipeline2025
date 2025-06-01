from logging import Logger
from pathlib import Path

import pytest

from utils.config_reader import ConfigReader


def test_initialize_configurations(log: Logger):
    configs_path = Path("tests/config/unit_test.yml")
    configs = ConfigReader(log, configs_path)
    assert configs is not None


def test_load_configurations(log: Logger):
    configs_path = Path("tests/config/unit_test.yml")
    configs = ConfigReader(log, configs_path).load_configurations()
    assert configs.configs_data["log_path"] != " "


def test_invalid_configurations_wrong_path(log: Logger):
    configs_path = Path("tests/config/test_wrong_file.yml")
    with pytest.raises(SystemExit) as failed:
        ConfigReader(log, configs_path).load_configurations()
    assert failed.type == SystemExit, "The method should exit due to the error"


def test_invalid_configurations_error_file(log: Logger):
    configs_path = Path("tests/config/")
    with pytest.raises(SystemExit) as failed:
        ConfigReader(log, configs_path).load_configurations()
    assert failed.type == SystemExit, "The method should exit due to the error"

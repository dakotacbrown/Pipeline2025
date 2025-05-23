from logging import Logger
from pathlib import Path

import utils.helper_functions as hf


def test_read_configs_success(log: Logger):
    BASE_DIR = Path(__file__).resolve()
    BASE_DIR = BASE_DIR.parent.parent
    config = hf.read_yml_configs(log, BASE_DIR, "unit_test")
    assert config is not None

from logging import Logger

import pytest

from logger.basic_logger import setup_logger


@pytest.fixture(scope="session", autouse=True)
def log() -> Logger:
    log = setup_logger()
    return log

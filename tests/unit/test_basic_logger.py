import logging

from logger.basic_logger import setup_logger


def test_setup_logger():
    logger = setup_logger()
    assert logger.level == logging.INFO
    stream_handler_exists = any(
        isinstance(handler, logging.StreamHandler)
        for handler in logger.handlers
    )
    assert stream_handler_exists is True

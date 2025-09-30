import logging


def setup_logger():
    logger = logging.getLogger()
    logger.propagate = False

    # logger is singleton so clear handlers and set level to prevent duplicate logs
    logger.handlers.clear()
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    return logger

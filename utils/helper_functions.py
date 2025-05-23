from logging import Logger
from pathlib import Path

from utils.config_reader import ConfigReader


def read_yml_configs(log: Logger, base_dir: str, file_name: str) -> dict:
    # read yaml and convert to dictionary
    config = (
        ConfigReader(log, Path(f"{base_dir}/config/{file_name}.yml"))
        .load_configurations()
        .configs_data
    )
    log.info("Configuration loaded successfully.")
    return config

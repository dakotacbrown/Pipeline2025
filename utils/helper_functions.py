import sys
import traceback
from logging import Logger
from pathlib import Path

import requests

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


def retrieve_oauth_token(
    log: Logger,
    oauth_link: str,
    headers: dict,
    data: dict,
) -> str:
    log.info("Attempting to retrieve access token.")

    try:
        response = requests.post(
            oauth_link,
            headers=headers,
            data=data,
            verify=False,
            timeout=15,
        )

        response.raise_for_status()

        json_data = response.json()

        access_token = json_data["access_token"]

        log.info("Successfully retrieved access token.")

        return access_token

    except Exception as e:
        log.error(
            f"An error occurred while retrieving access token: {e}\nStack Trace: {traceback.format_exc()}"
        )
        sys.exit(1)

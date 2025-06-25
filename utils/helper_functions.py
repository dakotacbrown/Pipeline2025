import sys
import traceback
from io import StringIO
from logging import Logger
from pathlib import Path
from typing import List

import numpy as np
import pandas as pd
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


def retrieve_report_data(
    log: Logger,
    data_endpoint: str,
    query: str,
    headers: dict,
) -> pd.DataFrame:
    log.info("Attempting to retrieve data.")

    try:
        full_url = f"{data_endpoint}{query}"
        response = requests.get(
            full_url,
            headers=headers,
            verify=False,
        )
        response.raise_for_status()

        csv_string = response.content.decode("utf-8")
        dataframe = pd.read_csv(StringIO(csv_string))

        log.info("Successfully retrieved data.")
        return dataframe

    except Exception as e:
        log.error(
            f"An error occurred while retrieving data: {e}\nStack Trace: {traceback.format_exc()}"
        )
        sys.exit(1)


def normalize_data(
    dataframe: pd.DataFrame, new_column_names: List[str]
) -> pd.DataFrame:
    # 1. Rename columns
    if len(new_column_names) != len(dataframe.columns):
        raise ValueError(
            "Number of new column names must match number of DataFrame columns."
        )
    dataframe.columns = new_column_names

    # 2. Replace "-" and "null" strings with np.nan
    dataframe.replace(["-", "null"], np.nan, inplace=True)

    # 3. Replace nulls in 'parent_account_id' with default fallback
    if "parent_account_id" in dataframe.columns:
        dataframe["parent_account_id"] = dataframe["parent_account_id"].fillna(
            "000000000000000"
        )

    return dataframe

from logging import Logger
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

import utils.helper_functions as hf


def test_read_configs_success(log: Logger):
    BASE_DIR = Path(__file__).resolve()
    BASE_DIR = BASE_DIR.parent.parent
    config = hf.read_yml_configs(log, BASE_DIR, "unit_test")
    assert config is not None


@patch("requests.post")
def test_retrieve_oauth_success(mock_post, log: Logger):
    oauth_link = "https://testing.com/api/v1"
    mock_response = MagicMock()
    mock_response.json.return_value = {"access_token": "mocked_token_value"}
    mock_response.raise_for_status = MagicMock()
    mock_post.return_value = mock_response

    headers = {
        "Accept": "application/json; v=1",
        "Content-Type": "application/x-www-form-urlencoded",
    }

    data = {
        "client_id": "client_id",
        "client_secret": "client_secret",
        "grant_type": "client_credentials",
    }

    token = hf.retrieve_oauth_token(log, oauth_link, headers, data)
    assert token == "mocked_token_value"


def test_retrieve_oauth_failure(log: Logger):
    oauth_link = "https://testing.com/api/v1"

    headers = {
        "Accept": "application/json; v=1",
        "Content-Type": "application/x-www-form-urlencoded",
    }

    data = {
        "client_id": "client_id",
        "client_secret": "client_secret",
        "grant_type": "client_credentials",
    }

    with pytest.raises(SystemExit) as failed:
        hf.retrieve_oauth_token(log, oauth_link, headers, data)
    assert failed.type == SystemExit, "The method should exit due to the error"


@patch("requests.get")
def test_retrieve_report_data_success(mock_get, log: Logger):
    # Fake CSV response
    fake_csv = "account_id,account_name,parent_account_id\n001,A Corp,003\n002,B Corp,-"

    mock_response = MagicMock()
    mock_response.content = fake_csv.encode("utf-8")
    mock_response.raise_for_status = MagicMock()
    mock_get.return_value = mock_response

    df = hf.retrieve_report_data(
        log=log,
        data_endpoint="https://instance.salesforce.com",
        query="/services/data/v61.0/analytics/reports/00OXXXX?export=1&xf=csv",
        headers={"Authorization": "Bearer token"},
    )

    assert isinstance(df, pd.DataFrame)
    assert df.shape == (2, 3)
    assert list(df.columns) == [
        "account_id",
        "account_name",
        "parent_account_id",
    ]


@patch("requests.get")
def test_retrieve_report_data_failure(mock_get, log: Logger):
    mock_response = MagicMock()
    mock_response.raise_for_status.side_effect = Exception("API failed")
    mock_get.return_value = mock_response

    with pytest.raises(SystemExit) as e:
        hf.retrieve_report_data(
            log=log,
            data_endpoint="https://bad.url",
            query="/bad_query",
            headers={},
        )

    assert e.type == SystemExit


def test_normalize_data_success():
    df = pd.DataFrame(
        [
            ["001", "Org A", "003"],
            ["002", "Org B", "-"],
            ["003", "Org C", "null"],
        ]
    )

    new_columns = ["account_id", "account_name", "parent_account_id"]
    normalized = hf.normalize_data(df, new_columns)

    assert normalized.shape == (3, 3)
    assert normalized["parent_account_id"].iloc[1] == "000000000000000"
    assert normalized["parent_account_id"].iloc[2] == "000000000000000"
    assert pd.isna(normalized["account_name"].iloc[1]) is False


def test_normalize_data_column_mismatch():
    df = pd.DataFrame([[1, 2], [3, 4]])

    new_columns = ["col1", "col2", "col3"]  # too many

    with pytest.raises(ValueError) as e:
        hf.normalize_data(df, new_columns)

    assert "Number of new column names must match" in str(e.value)

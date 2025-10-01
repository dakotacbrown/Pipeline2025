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

from __future__ import annotations

import json
import os
from datetime import datetime
from typing import Any, Dict, Optional

import requests

from api_ingestor.api_ingestor import ApiIngestor
from utils.basic_logger import setup_logger

log = setup_logger()


def set_env_vars_from_dict(env_vars: Dict[str, str]) -> None:
    """Set environment variables from a dictionary."""
    for key, value in env_vars.items():
        if not key or not value:
            log.warning("Skipping empty env var %s=%s", key, value)
            continue

        os.environ[key.upper()] = value
        log.info("Set env %s=%s", key, value)


def retrieve_oauth_token(
    oauth_link: str,
    headers: dict,
    data: dict,
) -> str:
    """Call the OAuth endpoint and return the access token."""
    response = requests.post(
        oauth_link,
        headers=headers,
        data=data,
        verify=False,
    )
    response.raise_for_status()

    json_data = response.json()
    access_token = json_data["access_token"]
    return access_token


# ---------------------------------------------------------------------------
# Public entrypoint used by the runner
# ---------------------------------------------------------------------------


def run_ingester(
    table: str,
    env: str,
    event: Dict[str, Any],
    config: Dict[str, Any],
    run_mode: str = "once",
    start: Optional[str] = None,
    end: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Execute the ApiIngester and return the metadata dict.

    Args:
        table: Table key under 'apis' in the YAML (e.g., 'events_api').
        env: Environment key under 'envs' (e.g., 'dev', 'prod').
        event: Glue event dict containing headers/auth config.
        config: Loaded YAML configuration dictionary.
        run_mode: 'once' or 'backfill'.
        start: (backfill) 'YYYY-MM-DD'.
        end: (backfill) 'YYYY-MM-DD'.
    """
    # env-specific vars from the YAML
    env_vars_all = config.get("env_vars", {})
    env_vars = env_vars_all.get(env, {})
    set_env_vars_from_dict(env_vars)

    if not table:
        raise ValueError("Parameter 'table' is required.")
    if not env:
        raise ValueError("Parameter 'env' is required.")

    # Exchange OAuth
    exchange_headers = event.get("exchange_headers", {})
    exchange_data = event.get("exchange_data", {})

    c1_oauth_token = retrieve_oauth_token(
        event.get("c1_oauth_url", ""),
        exchange_headers,
        exchange_data,
    )

    # Optional data OAuth
    data_headers = event.get("data_headers", {})
    data_auth = event.get("data_auth", {})

    if "data_headers" in event and "data_auth" in event:
        data_oauth_token = retrieve_oauth_token(
            event.get("data_auth_url", ""),
            data_headers,
            data_auth,
        )
        os.environ["DATA_OAUTH_TOKEN"] = data_oauth_token

    # Fixed proxy + job-wide env vars
    os.environ["C1_OAUTH_TOKEN"] = c1_oauth_token
    os.environ["HTTPS_PROXY"] = "http://aws-proxy-qa.cloud.capitalone.com:8099"
    os.environ["HTTP_PROXY"] = "http://aws-proxy-qa.cloud.capitalone.com:8099"
    os.environ["NO_PROXY"] = (
        "169.254.169.254,170.2.0.1,localhost,s3.amazonaws.com,"
        "s3.amazonaws.com,kdc.capitalone.com,cloud.capitalone.com,"
        "cloudgdt.capitalone.com"
    )  # noqa

    os.environ["ENV"] = env
    os.environ["TABLE"] = table
    if start:
        os.environ["START_DATE"] = start
    if end:
        os.environ["END_DATE"] = end

    api = ApiIngestor(config=config, log=log)

    if run_mode.lower() == "backfill":
        if not start or not end:
            raise ValueError(
                "Backfill requires both 'start' and 'end' (YYYY-MM-DD)."
            )

        try:
            d1 = datetime.strptime(start, "%Y-%m-%d").date()
            d2 = datetime.strptime(end, "%Y-%m-%d").date()
        except Exception as e:  # noqa: BLE001
            raise ValueError(
                f"Invalid start/end; expected 'YYYY-MM-DD'. "
                f"Got start={start!r}, end={end!r}"
            ) from e

        meta = api.run_backfill(
            table_name=table, env_name=env, start=d1, end=d2
        )
    else:
        meta = api.run_once(table_name=table, env_name=env)

    log.info("Ingester metadata: %s", json.dumps(meta))
    return meta

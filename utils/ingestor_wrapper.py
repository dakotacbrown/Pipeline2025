"""
ingestor_wrapper.py

Thin wrapper that:
  - reads the YAML config (with ${ENV_VAR} placeholders),
  - constructs ApiIngestor,
  - runs either run_once or run_backfill,
  - returns the metadata dict.

You can call run_ingestor(...) directly from code, or rely on
environment variables and call run_ingestor_from_environ().
"""

import logging
import os
from datetime import date, datetime
from typing import Optional

import yaml

# Adjust this import path to match where ApiIngestor lives in your project
from utils.api_ingestor import ApiIngestor  # noqa: E402


def _coerce_date(s: Optional[str]) -> Optional[date]:
    if not s:
        return None
    return datetime.strptime(s, "%Y-%m-%d").date()


def run_ingestor(
    *,
    yaml_path: str = "ingestor.yml",
    mode: str,
    table: str,
    env_name: str,
    start: Optional[str] = None,
    end: Optional[str] = None,
    logger: Optional[logging.Logger] = None,
):
    """
    Run the ingestor according to args and return the metadata dict.
    """
    log = logger or logging.getLogger("ingestor_wrapper")
    if not log.handlers:
        logging.basicConfig(level=logging.INFO)

    # 1) Load YAML (keep env var placeholders; ApiIngestor expands them)
    with open(yaml_path, "r", encoding="utf-8") as fh:
        config = yaml.safe_load(fh) or {}

    # 2) Construct ingestor
    ingestor = ApiIngestor(config=config, log=log)

    # 3) Dispatch
    if mode == "run_once":
        meta = ingestor.run_once(table, env_name)
        return meta

    if mode == "backfill":
        d0 = _coerce_date(start)
        d1 = _coerce_date(end)
        if not d0 or not d1:
            raise ValueError("backfill requires both --start and --end (YYYY-MM-DD).")
        meta = ingestor.run_backfill(table, env_name, d0, d1)
        return meta

    raise ValueError(f"Unknown mode: {mode!r}")


def run_ingestor_from_environ() -> dict:
    """
    Optional helper: read core settings from environment variables and run.
    Useful if your platform injects env vars instead of CLI args.

    Uses:
      RUN_MODE, TABLE_NAME, ENV_NAME, BACKFILL_START, BACKFILL_END, INGEST_YAML_PATH
    """
    mode = os.getenv("RUN_MODE")
    table = os.getenv("TABLE_NAME")
    env_name = os.getenv("ENV_NAME")
    yaml_path = os.getenv("INGEST_YAML_PATH", "ingestor.yml")
    start = os.getenv("BACKFILL_START")
    end = os.getenv("BACKFILL_END")

    if not (mode and table and env_name):
        raise RuntimeError(
            "RUN_MODE, TABLE_NAME, and ENV_NAME environment variables are required."
        )

    return run_ingestor(
        yaml_path=yaml_path,
        mode=mode,
        table=table,
        env_name=env_name,
        start=start,
        end=end,
    )

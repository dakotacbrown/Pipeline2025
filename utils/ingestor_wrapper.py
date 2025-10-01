from __future__ import annotations

import logging
from datetime import date
from typing import Any, Dict, Optional, Union

import yaml

# Adjust the import below if your ApiIngestor lives elsewhere
from utils.api_ingestor import ApiIngestor


def _make_logger(name: str = "api_ingestor") -> logging.Logger:
    """Create a simple logger if the caller does not provide one."""
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        fmt = logging.Formatter(
            fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
            datefmt="%Y-%m-%dT%H:%M:%SZ",
        )
        handler.setFormatter(fmt)
        logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    return logger


def load_config_from_yaml(yaml_path: str) -> Dict[str, Any]:
    """
    Load and parse the YAML configuration file into a Python dict.
    """
    with open(yaml_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def _coerce_date(d: Union[str, date]) -> date:
    """Accept either a date or an ISO string (YYYY-MM-DD) and return a date."""
    if isinstance(d, date):
        return d
    return date.fromisoformat(d)


def run_ingest_from_yaml(
    yaml_path: str,
    table_name: str,
    env_name: str,
    *,
    mode: str = "once",  # "once" | "backfill"
    start: Optional[Union[str, date]] = None,
    end: Optional[Union[str, date]] = None,
    logger: Optional[logging.Logger] = None,
) -> Dict[str, Any]:
    """
    Read config YAML, create an ApiIngestor, run it, and return the metadata dict.

    Args:
        yaml_path: Path to the YAML config included alongside the code.
        table_name: The table key under `apis:` in the YAML.
        env_name: The environment key under `envs:` in the YAML.
        mode: "once" for a single pull, "backfill" for a windowed backfill.
        start/end: Required for backfill. Accept date objects or "YYYY-MM-DD" strings.
        logger: Optional logger; if not provided, a basic one is created.

    Returns:
        The metadata dictionary returned by ApiIngestor.run_once/run_backfill.
    """
    log = logger or _make_logger()
    config = load_config_from_yaml(yaml_path)

    ingestor = ApiIngestor(config=config, log=log)

    if mode == "once":
        return ingestor.run_once(table_name, env_name)

    if mode == "backfill":
        if start is None or end is None:
            raise ValueError("Backfill mode requires both 'start' and 'end'.")
        start_d = _coerce_date(start)
        end_d = _coerce_date(end)
        return ingestor.run_backfill(table_name, env_name, start_d, end_d)

    raise ValueError(f"Unsupported mode: {mode!r}. Use 'once' or 'backfill'.")


__all__ = [
    "load_config_from_yaml",
    "run_ingest_from_yaml",
]

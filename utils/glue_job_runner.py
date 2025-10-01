"""
Glue-friendly runner script (works locally too).

Reads configuration from environment variables, optionally extends sys.path
(e.g., if you want to hard-code a local ZIP path during testing), then calls
the ingestor wrapper and prints the resulting metadata dict as JSON.

Environment variables consumed:
  - TABLE_NAME        (required)
  - ENV_NAME          (required)
  - YAML_PATH         (optional; local path, s3://..., or package resource like "ingestor_wrapper:ingestor.yml")
  - BACKFILL          (optional: true/false; default false)
  - START_DATE        (required if BACKFILL=true; format YYYY-MM-DD)
  - END_DATE          (required if BACKFILL=true; format YYYY-MM-DD)
  - LOG_LEVEL         (optional; default INFO)
  - ZIP_PY_PATHS      (optional; colon-separated paths to append to sys.path, e.g. "/tmp/deps.zip:/tmp/other.zip")
"""

import json
import os
import sys
from typing import Any, Dict


def _extend_sys_path_from_env() -> None:
    """
    If ZIP_PY_PATHS is provided, extend sys.path with those entries.
    Useful when testing locally to simulate Glue's --extra-py-files behavior.
    """
    zip_paths = os.environ.get("ZIP_PY_PATHS")
    if not zip_paths:
        return
    for p in zip_paths.split(":"):
        p = p.strip()
        if p and p not in sys.path:
            sys.path.insert(0, p)


def _read_env_bool(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name, "").strip().lower()
    if not raw:
        return default
    return raw in ("1", "true", "yes", "y")


def main() -> int:
    _extend_sys_path_from_env()

    # Import the wrapper from the ZIP (should succeed when ZIP is on sys.path or passed via Glue extra py files).
    from ingestor_wrapper import run_ingestor  # noqa: WPS433

    # Required
    table = os.environ.get("TABLE_NAME")
    env = os.environ.get("ENV_NAME")

    if not table or not env:
        print(
            "ERROR: TABLE_NAME and ENV_NAME must be set as environment variables.",
            file=sys.stderr,
        )
        return 2

    # Optional / conditional
    yaml_path = os.environ.get("YAML_PATH")  # can be omitted to auto-discover
    backfill = _read_env_bool("BACKFILL", default=False)
    start = os.environ.get("START_DATE")
    end = os.environ.get("END_DATE")
    log_level = os.environ.get("LOG_LEVEL", "INFO")

    # If doing a backfill, sanity-check dates
    if backfill and (not start or not end):
        print(
            "ERROR: BACKFILL=true but START_DATE or END_DATE is missing (YYYY-MM-DD).",
            file=sys.stderr,
        )
        return 3

    # Execute
    meta: Dict[str, Any] = run_ingestor(
        yaml_path=yaml_path,
        table=table,
        env=env,
        backfill=backfill,
        start=start,
        end=end,
        log_level=log_level,
    )

    # Print metadata as a single JSON line (easy to parse in logs)
    print(json.dumps(meta, separators=(",", ":"), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

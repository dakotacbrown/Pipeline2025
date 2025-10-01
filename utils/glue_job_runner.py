"""
Glue-friendly runner script (works locally too).

Reads configuration from environment variables, optionally extends sys.path
(using literal paths and/or glob patterns), then calls the ingestor wrapper and
prints the resulting metadata dict as JSON.

Environment variables consumed:
  - TABLE_NAME        (required)
  - ENV_NAME          (required)
  - YAML_PATH         (optional; local path, s3://..., or package resource like "ingestor_wrapper:ingestor.yml")
  - BACKFILL          (optional: true/false; default false)
  - START_DATE        (required if BACKFILL=true; format YYYY-MM-DD)
  - END_DATE          (required if BACKFILL=true; format YYYY-MM-DD)
  - LOG_LEVEL         (optional; default INFO)

  - ZIP_PY_PATHS      (optional; colon-separated entries that may be literal paths
                       OR glob patterns, e.g. "/tmp/libs/*.zip:/tmp/site/*.whl")
  - ZIP_PY_GLOB       (optional; colon-separated glob patterns ONLY, e.g. "/tmp/*/site-packages/*.zip")
"""

import json
import os
import sys
import glob
from typing import Any, Dict, List


def _extend_sys_path_from_env() -> None:
    """
    Extend sys.path using:
      1) ZIP_PY_PATHS: colon-separated entries that can be literal paths or globs
      2) ZIP_PY_GLOB : colon-separated glob-only patterns
      3) Fallback: scan a few common /tmp patterns if nothing added
    """
    added: List[str] = []

    def _add(p: str):
        if p and p not in sys.path:
            sys.path.insert(0, p)
            added.append(p)

    # 1) ZIP_PY_PATHS: expand each entry with glob; if no match, treat as literal
    raw_paths = os.environ.get("ZIP_PY_PATHS", "")
    for entry in [e.strip() for e in raw_paths.split(":") if e.strip()]:
        matches = glob.glob(entry)
        if matches:
            for m in matches:
                _add(m)
        else:
            # treat as literal path
            _add(entry)

    # 2) ZIP_PY_GLOB: glob-only
    raw_globs = os.environ.get("ZIP_PY_GLOB", "")
    for pattern in [g.strip() for g in raw_globs.split(":") if g.strip()]:
        for m in glob.glob(pattern):
            _add(m)

    # 3) Fallback if nothing was added: scan common Glue temp patterns
    if not added:
        for pattern in ("/tmp/*.zip", "/tmp/*.whl", "/tmp/*/*.zip", "/tmp/*/*.whl"):
            for m in glob.glob(pattern):
                _add(m)

    # Optional visibility
    if added:
        print(f"[runner] Added to sys.path: {added}", file=sys.stderr)


def _read_env_bool(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name, "").strip().lower()
    if not raw:
        return default
    return raw in ("1", "true", "yes", "y")


def main() -> int:
    _extend_sys_path_from_env()

    # Import the wrapper from the ZIP (should succeed when the bundle/zip is on sys.path)
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
    yaml_path = os.environ.get("YAML_PATH")  # can be omitted to auto-discover inside the package
    backfill = _read_env_bool("BACKFILL", default=False)
    start = os.environ.get("START_DATE")
    end = os.environ.get("END_DATE")
    log_level = os.environ.get("LOG_LEVEL", "INFO")

    if backfill and (not start or not end):
        print(
            "ERROR: BACKFILL=true but START_DATE or END_DATE is missing (YYYY-MM-DD).",
            file=sys.stderr,
        )
        return 3

    meta: Dict[str, Any] = run_ingestor(
        yaml_path=yaml_path,
        table=table,
        env=env,
        backfill=backfill,
        start=start,
        end=end,
        log_level=log_level,
    )

    print(json.dumps(meta, separators=(",", ":"), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

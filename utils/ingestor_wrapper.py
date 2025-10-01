"""
Ingestor wrapper:
- Reads a YAML config (local file, S3, or package resource inside the ZIP)
- Creates ApiIngestor
- Runs once or backfill and returns the metadata dict
"""

import io
import json
import logging
import os
from datetime import datetime, date
from typing import Optional, Dict, Any

# Third party
try:
    import yaml
except Exception as e:
    raise RuntimeError("PyYAML is required (yaml). Install pyyaml.") from e

# Local import from your ZIP
from utils.api_ingestor import ApiIngestor  # noqa: E402

# For package-resource loading (ZIP-internal files)
try:
    from importlib.resources import files as ir_files  # Python 3.9+
except Exception:  # pragma: no cover
    ir_files = None

# ---------------------------------------------------------------------------

_DEFAULT_RESOURCE_PACKAGE = "ingestor_wrapper"
_DEFAULT_CANDIDATE_FILENAMES = (
    "ingestor.yml",
    "ingestor.yaml",
    "config.yml",
    "config.yaml",
)

# ---------------------------------------------------------------------------


def _read_yaml_from_package(resource: Optional[str] = None) -> str:
    """
    Read YAML text from inside the ZIP via importlib.resources.

    resource forms supported:
      - None -> try default filenames in _DEFAULT_RESOURCE_PACKAGE
      - "ingestor_wrapper:ingestor.yml"  (package:resource)
      - "res://ingestor_wrapper/ingestor.yml" or "pkg://ingestor_wrapper/ingestor.yml"
      - "ingestor_wrapper/ingestor.yml" (best-effort)
    """
    if ir_files is None:
        raise FileNotFoundError(
            "importlib.resources not available to load package resources."
        )

    pkg = _DEFAULT_RESOURCE_PACKAGE
    name = None

    if resource:
        # Normalize different forms
        if resource.startswith(("res://", "pkg://")):
            resource = resource.split("://", 1)[1]
        if ":" in resource:
            pkg, name = resource.split(":", 1)
        elif "/" in resource:
            # "package/path.yml" -> split once
            pkg, name = resource.split("/", 1)
        else:
            # Only a name -> use default package
            name = resource

        handle = ir_files(pkg).joinpath(name)
        if not handle.is_file():
            raise FileNotFoundError(
                f"Package resource not found: {pkg}:{name}"
            )
        return handle.read_text(encoding="utf-8")

    # Auto-discover: look for default names under default package
    base = ir_files(pkg)
    for cand in _DEFAULT_CANDIDATE_FILENAMES:
        handle = base.joinpath(cand)
        if handle.is_file():
            return handle.read_text(encoding="utf-8")

    raise FileNotFoundError(
        f"Could not auto-discover a YAML in package '{pkg}'. "
        f"Tried: {', '.join(_DEFAULT_CANDIDATE_FILENAMES)}. "
        f"Pass YAML_PATH env var (e.g. 'ingestor_wrapper:ingestor.yml' or a real file path)."
    )


def _read_yaml_from_s3(s3_uri: str) -> str:
    """
    Read YAML text from s3://bucket/key using boto3.
    """
    try:
        import boto3  # imported lazily so local runs don't require it unless needed
    except Exception as e:
        raise RuntimeError("boto3 is required to read S3 URIs.") from e

    if not s3_uri.startswith("s3://"):
        raise ValueError("S3 URI must start with s3://")

    _, rest = s3_uri.split("s3://", 1)
    bucket, key = rest.split("/", 1)
    s3 = boto3.client("s3")
    obj = s3.get_object(Bucket=bucket, Key=key)
    return obj["Body"].read().decode("utf-8")


def _load_yaml_text(yaml_path: Optional[str]) -> str:
    """
    Load YAML text from:
      1) explicit path/URI if provided
         - local file path
         - s3://bucket/key
         - package resource forms
      2) otherwise: auto-discover inside 'ingestor_wrapper' package
    """
    if yaml_path:
        # S3?
        if yaml_path.startswith("s3://"):
            return _read_yaml_from_s3(yaml_path)

        # Package resource forms?
        if any(
            yaml_path.startswith(prefix)
            for prefix in ("res://", "pkg://")
        ) or ":" in yaml_path or (
            "/" in yaml_path and not os.path.exists(yaml_path)
        ):
            return _read_yaml_from_package(yaml_path)

        # Local file
        if os.path.isfile(yaml_path):
            with open(yaml_path, "r", encoding="utf-8") as f:
                return f.read()

        # As a last attempt, try package form
        return _read_yaml_from_package(yaml_path)

    # No path given -> auto-discover in package
    return _read_yaml_from_package(resource=None)


def _build_logger(level: str = "INFO") -> logging.Logger:
    log = logging.getLogger("ApiIngestor")
    if not log.handlers:
        handler = logging.StreamHandler()
        fmt = logging.Formatter(
            "%(asctime)s %(levelname)s %(name)s: %(message)s"
        )
        handler.setFormatter(fmt)
        log.addHandler(handler)
    log.setLevel(getattr(logging, level.upper(), logging.INFO))
    return log


def _parse_iso_date(d: str) -> date:
    return datetime.strptime(d, "%Y-%m-%d").date()


def run_ingestor(
    yaml_path: Optional[str],
    table: str,
    env: str,
    backfill: bool = False,
    start: Optional[str] = None,
    end: Optional[str] = None,
    log_level: str = "INFO",
) -> Dict[str, Any]:
    """
    Main entry for programmatic use (called by the runner).

    Returns: metadata dict from ApiIngestor.run_once / run_backfill
    """
    logger = _build_logger(log_level)

    yaml_text = _load_yaml_text(yaml_path)
    config = yaml.safe_load(io.StringIO(yaml_text))

    ing = ApiIngestor(config=config, log=logger)

    if backfill:
        if not start or not end:
            raise ValueError(
                "BACKFILL is true but START_DATE / END_DATE not provided."
            )
        start_d = _parse_iso_date(start)
        end_d = _parse_iso_date(end)
        meta = ing.run_backfill(table, env, start_d, end_d)
    else:
        meta = ing.run_once(table, env)

    # Log and return
    logger.info("Ingest complete: %s", json.dumps(meta, indent=2))
    return meta


# Optional CLI for local debugging
def main() -> int:
    # Use environment variables to keep Glue/UI and local consistent.
    table = os.environ.get("TABLE_NAME")
    env = os.environ.get("ENV_NAME")
    yaml_path = os.environ.get("YAML_PATH")  # optional
    backfill = os.environ.get("BACKFILL", "false").strip().lower() in (
        "1",
        "true",
        "yes",
    )
    start = os.environ.get("START_DATE")
    end = os.environ.get("END_DATE")
    log_level = os.environ.get("LOG_LEVEL", "INFO")

    if not table or not env:
        raise SystemExit(
            "Missing TABLE_NAME or ENV_NAME. Set env vars and re-run."
        )

    run_ingestor(
        yaml_path=yaml_path,
        table=table,
        env=env,
        backfill=backfill,
        start=start,
        end=end,
        log_level=log_level,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

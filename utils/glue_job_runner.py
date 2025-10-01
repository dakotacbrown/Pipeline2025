"""
Glue runner for ApiIngestor.

- Does NOT read YAML. The ingestor wrapper handles YAML loading (from the ZIP).
- Provides a place to hard-code environment variables (tokens, etc).
- Calls the wrapper's `run_ingestor(...)` and prints the returned metadata.

USAGE OPTION A (recommended): let Glue add your ZIP to sys.path
----------------------------------------------------------------
Create your Glue job and add:
  --extra-py-files s3://your-bucket/path/ingestor_bundle.zip

Then set USE_RUNTIME_ZIP_IMPORT = False (default) and set WRAPPER_IMPORT to
match the module path inside the ZIP (e.g., "ingestor_wrapper.run_ingestor"
or "utils.ingestor_wrapper.run_ingestor").

USAGE OPTION B: add the ZIP to sys.path at runtime
--------------------------------------------------
Set USE_RUNTIME_ZIP_IMPORT = True and S3_ZIP_URI below to your ZIP.
The runner will download the ZIP to /tmp and import from it.
"""

import json
import logging
import os
import sys
from datetime import date
from typing import Dict, Optional

# ---------- Configurable block (edit to taste) ----------

# 0) Hard-coded environment variables (safe defaults). Override here as needed.
#    These are injected into os.environ before running the ingestor.
HARD_CODED_ENV: Dict[str, str] = {
    # Example tokens/keys; replace with your own and/or leave blank for now.
    # "VENDOR_A_TOKEN": "REPLACE_ME",
    # "ANOTHER_API_KEY": "REPLACE_ME",
}

# 1) How to import the wrapper that lives in your ZIP:
#    - If your ZIP contains "ingestor_wrapper.py" at its root:
#        WRAPPER_IMPORT = "ingestor_wrapper.run_ingestor"
#    - If it's "utils/ingestor_wrapper.py":
#        WRAPPER_IMPORT = "utils.ingestor_wrapper.run_ingestor"
WRAPPER_IMPORT = "ingestor_wrapper.run_ingestor"

# 2) If you cannot pass --extra-py-files yet, enable this and point to your ZIP on S3.
USE_RUNTIME_ZIP_IMPORT = False
S3_ZIP_URI = "s3://your-bucket/libs/ingestor_bundle.zip"  # used only if USE_RUNTIME_ZIP_IMPORT=True

# 3) What to run (hard-coded; you can wire job params later if you like).
RUN_MODE = "once"  # "once" or "backfill"
TABLE_NAME = "marketing_reports"
ENV_NAME = "prod"

# The wrapper will load YAML from inside the ZIP when yaml_path=None.
YAML_PATH: Optional[str] = None

# Backfill window (only used when RUN_MODE == "backfill")
BACKFILL_START: Optional[date] = date(2025, 1, 1)
BACKFILL_END: Optional[date] = date(2025, 1, 7)

# ---------- End configurable block ----------


def _add_zip_to_path(s3_uri: str) -> str:
    """
    Downloads a ZIP from S3 to /tmp and adds it to sys.path so we can import from it.
    Used only when USE_RUNTIME_ZIP_IMPORT = True.
    """
    if not s3_uri or not s3_uri.startswith("s3://"):
        raise ValueError(
            "S3_ZIP_URI must be an s3:// URI when USE_RUNTIME_ZIP_IMPORT=True"
        )

    import boto3  # Glue runtime includes boto3

    bucket, key = s3_uri[5:].split("/", 1)
    local_zip = os.path.join("/tmp", os.path.basename(key))
    if not os.path.exists(local_zip):
        boto3.client("s3").download_file(bucket, key, local_zip)
    if local_zip not in sys.path:
        sys.path.insert(0, local_zip)
    return local_zip


def _load_entrypoint(dotted: str):
    """Import a dotted path like 'pkg.module.func' and return the function object."""
    if "." not in dotted:
        raise ValueError(
            f"WRAPPER_IMPORT must be a dotted path like 'module.func', got: {dotted}"
        )
    module_name, func_name = dotted.rsplit(".", 1)
    mod = __import__(module_name, fromlist=[func_name])
    fn = getattr(mod, func_name, None)
    if fn is None:
        raise ImportError(
            f"Function '{func_name}' not found in module '{module_name}'"
        )
    return fn


def _configure_logging() -> logging.Logger:
    logger = logging.getLogger("glue_runner")
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter(
            fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
            datefmt="%Y-%m-%dT%H:%M:%SZ",
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    return logger


def _apply_env(env_map: Dict[str, str]) -> None:
    """
    Apply a map of environment variables to os.environ.
    Does not overwrite existing values unless the value is not empty.
    """
    for k, v in (env_map or {}).items():
        if v is None:
            continue
        # Only set if missing; flip to os.environ[k] = v if you want to force overrides.
        os.environ.setdefault(k, v)


def main() -> int:
    logger = _configure_logging()
    logger.info("Starting Glue runner.")

    # Optionally make the ZIP importable at runtime.
    if USE_RUNTIME_ZIP_IMPORT:
        try:
            local_zip = _add_zip_to_path(S3_ZIP_URI)
            logger.info(f"Added ZIP to sys.path: {local_zip}")
        except Exception as e:
            logger.exception(
                f"Failed to add ZIP to sys.path from {S3_ZIP_URI}: {e}"
            )
            return 2

    # Load the wrapper entrypoint.
    try:
        run_ingestor = _load_entrypoint(WRAPPER_IMPORT)
    except Exception as e:
        logger.exception(
            f"Failed to import wrapper entrypoint '{WRAPPER_IMPORT}': {e}"
        )
        return 3

    # Apply hard-coded env vars (tokens, etc.) before running.
    _apply_env(HARD_CODED_ENV)

    # Call the wrapper. The wrapper is responsible for:
    #  - reading the YAML (from inside the ZIP if yaml_path=None)
    #  - creating ApiIngestor
    #  - executing once or backfill
    #  - returning a metadata dict
    try:
        if RUN_MODE == "once":
            meta = run_ingestor(
                table_name=TABLE_NAME,
                env_name=ENV_NAME,
                yaml_path=YAML_PATH,  # None => wrapper loads from ZIP
                backfill=None,  # not a backfill
                extra_env=None,  # wrapper can also accept extra_env; hard-coded vars already applied
                log_level="INFO",
            )
        elif RUN_MODE == "backfill":
            if not (BACKFILL_START and BACKFILL_END):
                raise ValueError(
                    "BACKFILL_START and BACKFILL_END must be set for RUN_MODE='backfill'"
                )
            meta = run_ingestor(
                table_name=TABLE_NAME,
                env_name=ENV_NAME,
                yaml_path=YAML_PATH,
                backfill=(BACKFILL_START, BACKFILL_END),  # tuple[date, date]
                extra_env=None,
                log_level="INFO",
            )
        else:
            raise ValueError(f"Unsupported RUN_MODE: {RUN_MODE}")

        # Print JSON for convenient capture in Glue logs
        logger.info("Ingestion complete. Metadata:")
        print(
            json.dumps(meta, default=str)
        )  # default=str to serialize dates/timestamps
        return 0

    except Exception as e:
        logger.exception(f"Ingestion failed: {e}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

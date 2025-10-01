# ingestor_wrapper.py
from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, date
from pathlib import Path
from typing import Dict, Any, Optional
from zipfile import ZipFile

import yaml  # Make sure pyyaml is available on the job

# Import your ingestor
from utils.api_ingestor import ApiIngestor


LOG = logging.getLogger("ingestor_wrapper")
if not LOG.handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s :: %(message)s",
    )


# ---------------------------------------------------------------------
# Helpers: read YAML from filesystem or from any .zip present on sys.path
# ---------------------------------------------------------------------
def _zip_candidates_from_sys_path() -> list[Path]:
    """
    Locate any .zip files referenced by sys.path entries. We normalize
    entries like 'something.zip/', 'something.zip/Python', etc. back to
    the real ZIP file path (ending with .zip) and keep uniques.
    """
    cands: list[Path] = []
    seen = set()
    for p in sys.path:
        if ".zip" not in p.lower():
            continue
        # Normalize to the actual .zip path
        idx = p.lower().find(".zip")
        zip_path = Path(p[: idx + 4]).resolve()
        if zip_path.exists() and zip_path.suffix.lower() == ".zip":
            if zip_path not in seen:
                cands.append(zip_path)
                seen.add(zip_path)
    return cands


def _read_text_from_zip(zip_path: Path, inner_path: str) -> Optional[str]:
    """
    Try to read 'inner_path' from the given zip. We also try common
    subpath variants used by some bundles:
      - as-is
      - Python/<inner_path>
      - <zip_stem>/<inner_path>
      - <zip_stem>/Python/<inner_path>
    Returns text or None if not found.
    """
    inner = inner_path.lstrip("/")

    variants = [inner, f"Python/{inner}"]
    stem = zip_path.stem
    variants += [f"{stem}/{inner}", f"{stem}/Python/{inner}"]

    try:
        with ZipFile(zip_path, "r") as zf:
            for v in variants:
                try:
                    with zf.open(v) as fh:
                        data = fh.read()
                        return data.decode("utf-8")
                except KeyError:
                    continue
    except Exception:
        # Don’t let a single broken zip abort the search
        return None
    return None


def _load_yaml_from_anywhere(yaml_path: str) -> Dict[str, Any]:
    """
    Load YAML either from the filesystem (if the path exists) or from
    any .zip referenced by sys.path (typical in AWS Glue jobs).
    """
    p = Path(yaml_path)
    if p.exists():
        LOG.info("Loading YAML from filesystem: %s", p)
        return yaml.safe_load(p.read_text())

    # Try every zip on sys.path
    for z in _zip_candidates_from_sys_path():
        text = _read_text_from_zip(z, yaml_path)
        if text is not None:
            LOG.info("Loaded YAML from zip: %s :: %s", z, yaml_path)
            return yaml.safe_load(text)

    raise FileNotFoundError(
        f"Could not locate YAML '{yaml_path}' on disk or in any sys.path zip."
    )


# ---------------------------------------------------------------------
# Public entrypoint used by the runner
# ---------------------------------------------------------------------
def run_ingestor(
    table: str,
    env_name: str,
    yaml_path: str,
    run_mode: str = "once",
    start: Optional[str] = None,
    end: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Run the ApiIngestor using config from YAML and return the metadata dict.

    Args:
        table:     Table key under 'apis' in the YAML (e.g., 'events_api').
        env_name:  Environment key under 'envs' (e.g., 'dev', 'prod').
        yaml_path: Path to YAML on disk or inside the uploaded zip, e.g.
                   'config/ingestor.yml'.
        run_mode:  'once' or 'backfill'.
        start:     Backfill start date 'YYYY-MM-DD' (required for backfill).
        end:       Backfill end date 'YYYY-MM-DD' (required for backfill).

    Returns:
        Metadata dict returned by ApiIngestor.run_once / run_backfill.
    """
    if not table:
        raise ValueError("Parameter 'table' is required.")
    if not env_name:
        raise ValueError("Parameter 'env_name' is required.")

    config = _load_yaml_from_anywhere(yaml_path)

    # Minimal logger that ApiIngestor expects (std logging works fine)
    log = LOG

    ingestor = ApiIngestor(config=config, log=log)

    if (run_mode or "once").lower() == "backfill":
        if not start or not end:
            raise ValueError(
                "Backfill requires both 'start' and 'end' in YYYY-MM-DD format."
            )
        try:
            d0: date = datetime.strptime(start, "%Y-%m-%d").date()
            d1: date = datetime.strptime(end, "%Y-%m-%d").date()
        except Exception as e:
            raise ValueError(
                f"Invalid start/end; expected YYYY-MM-DD. Got start={start!r}, end={end!r}"
            ) from e

        meta = ingestor.run_backfill(table_name=table, env_name=env_name, start=d0, end=d1)
    else:
        meta = ingestor.run_once(table_name=table, env_name=env_name)

    # Helpful for Glue log searchers
    LOG.info("Ingestor metadata: %s", json.dumps(meta))
    return meta

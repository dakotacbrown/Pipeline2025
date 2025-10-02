# ingestor_wrapper.py
from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, date
from pathlib import Path
from typing import Dict, Any, Optional
from zipfile import ZipFile

import yaml  # pyyaml must be available in the job/bundle

# Your ingestor implementation
from utils.api_ingestor import ApiIngestor


LOG = logging.getLogger("ingestor_wrapper")
if not LOG.handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s :: %(message)s",
    )


# ---------------------------------------------------------------------
# Helpers: load YAML from disk or from any .zip found on sys.path
# ---------------------------------------------------------------------
def _zip_candidates_from_sys_path() -> list[Path]:
    """
    Find any .zip files referenced by sys.path entries. Handles entries like:
      '<zip>.zip', '<zip>.zip/', '<zip>.zip/Python', etc.
    Returns unique, existing Paths to the .zip files.
    """
    cands: list[Path] = []
    seen: set[Path] = set()
    for p in sys.path:
        low = p.lower()
        if ".zip" not in low:
            continue
        try:
            # normalize to the actual .zip
            idx = low.find(".zip")
            zp = Path(p[: idx + 4]).resolve()
            if zp.suffix.lower() == ".zip" and zp.exists() and zp not in seen:
                cands.append(zp)
                seen.add(zp)
        except Exception:
            # ignore malformed path entries
            continue
    return cands


def _read_text_from_zip(zip_path: Path, inner_path: str) -> Optional[str]:
    """
    Try to read a file from a zip, considering common subpath layouts:
      inner_path
      Python/inner_path
      <zip_stem>/inner_path
      <zip_stem>/Python/inner_path
    Returns the text or None if not found.
    """
    inner = inner_path.lstrip("/")
    variants = [inner, f"Python/{inner}"]
    stem = zip_path.stem
    variants += [f"{stem}/{inner}", f"{stem}/Python/{inner}"]

    try:
        with ZipFile(zip_path, "r") as zf:
            for candidate in variants:
                try:
                    with zf.open(candidate) as fh:
                        return fh.read().decode("utf-8")
                except KeyError:
                    continue
    except Exception:
        # don't let a single bad zip abort the search
        return None
    return None


def _load_yaml_from_anywhere(yaml_path: str) -> Dict[str, Any]:
    """
    Load YAML from disk (if present) or from any zip on sys.path (Glue job style).
    """
    p = Path(yaml_path)
    if p.exists():
        LOG.info("Loading YAML from filesystem: %s", p)
        return yaml.safe_load(p.read_text())

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
    Execute the ApiIngestor and return the metadata dict.

    Args:
        table:     Table key under 'apis' in the YAML (e.g. 'events_api').
        env_name:  Environment key under 'envs' (e.g. 'dev', 'prod').
        yaml_path: Path to the YAML within the bundle or local FS (e.g. 'config/ingestor.yml').
        run_mode:  'once' or 'backfill'.
        start:     (backfill) 'YYYY-MM-DD'.
        end:       (backfill) 'YYYY-MM-DD'.
    """
    if not table:
        raise ValueError("Parameter 'table' is required.")
    if not env_name:
        raise ValueError("Parameter 'env_name' is required.")

    config = _load_yaml_from_anywhere(yaml_path)
    ing = ApiIngestor(config=config, log=LOG)

    if (run_mode or "once").lower() == "backfill":
        if not start or not end:
            raise ValueError("Backfill requires both 'start' and 'end' (YYYY-MM-DD).")
        try:
            d0: date = datetime.strptime(start, "%Y-%m-%d").date()
            d1: date = datetime.strptime(end, "%Y-%m-%d").date()
        except Exception as e:
            raise ValueError(
                f"Invalid start/end; expected YYYY-MM-DD. Got start={start!r}, end={end!r}"
            ) from e

        meta = ing.run_backfill(table_name=table, env_name=env_name, start=d0, end=d1)
    else:
        meta = ing.run_once(table_name=table, env_name=env_name)

    LOG.info("Ingestor metadata: %s", json.dumps(meta))
    return meta

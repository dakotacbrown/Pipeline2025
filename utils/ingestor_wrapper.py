# ingestor_wrapper.py
from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, date
from pathlib import Path
from typing import Dict, Any, Optional
from zipfile import ZipFile

import yaml

from api_ingestor import ApiIngestor

LOG = logging.getLogger("ingestor_wrapper")
if not LOG.handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s :: %(message)s",
    )

# ---------- Minimal helpers (no Python/ subpaths) ----------

def _zip_candidates_from_sys_path() -> list[Path]:
    """Return unique .zip paths mentioned anywhere on sys.path."""
    cands: list[Path] = []
    seen: set[Path] = set()
    for p in sys.path:
        low = p.lower()
        if ".zip" not in low:
            continue
        z = Path(p[: low.find(".zip") + 4]).resolve()
        if z.suffix.lower() == ".zip" and z.exists() and z not in seen:
            cands.append(z)
            seen.add(z)
    return cands


def _read_text_from_zip(zip_path: Path, inner_path: str) -> Optional[str]:
    """
    Read exactly `inner_path` (or '<zip_stem>/<inner_path>') from the zip.
    """
    inner = inner_path.lstrip("/")
    variants = [inner, f"{zip_path.stem}/{inner}"]

    try:
        with ZipFile(zip_path, "r") as zf:
            for v in variants:
                try:
                    with zf.open(v) as fh:
                        return fh.read().decode("utf-8")
                except KeyError:
                    continue
    except Exception:
        return None
    return None


def _load_yaml_from_anywhere(yaml_path: str) -> Dict[str, Any]:
    """Load YAML from filesystem or any zip on sys.path."""
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

# ---------- Public entrypoint ----------

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
    """
    if not table:
        raise ValueError("Parameter 'table' is required.")
    if not env_name:
        raise ValueError("Parameter 'env_name' is required.")

    config = _load_yaml_from_anywhere(yaml_path)
    ingestor = ApiIngestor(config=config, log=LOG)

    if (run_mode or "once").lower() == "backfill":
        if not start or not end:
            raise ValueError("Backfill requires 'start' and 'end' (YYYY-MM-DD).")
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

    LOG.info("Ingestor metadata: %s", json.dumps(meta))
    return meta

# run_step.py
import argparse
import importlib
import json
import logging
import os
import sys
from pathlib import Path


# -------- zip path setup (Python path removed) --------
def setup_path(bundle_glob: str) -> None:
    """
    Look in the current working directory for a zip matching bundle_glob
    (e.g., 'ingestor_bundle*.zip'), and add only:
        <zip>/
    """
    files = list(Path.cwd().rglob(bundle_glob))

    if len(files) > 1:
        raise ValueError(f"More than one {bundle_glob} zip found: {files}")
    elif len(files) == 1:
        zip_file_with_zip = files[0].name
        base_path = f"{zip_file_with_zip}/"

        sys.path.insert(0, base_path)
    else:
        # zero files: no-op
        pass


def _parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument("-y", "--yaml_path", required=True, help="Path to YAML inside the zip or local FS, e.g. 'config/ingestor.yml'")
    parser.add_argument("--table", required=True, help="Table key under 'apis' in YAML")
    parser.add_argument("--env", dest="env_name", required=True, help="Env key under 'envs' in YAML (e.g., dev, qa, prod)")
    parser.add_argument("--run_mode", choices=["once", "backfill"], default="once")
    parser.add_argument("--backfill_start", help="YYYY-MM-DD (when run_mode=backfill)")
    parser.add_argument("--backfill_end", help="YYYY-MM-DD (when run_mode=backfill)")
    parser.add_argument("--log_level", default="INFO")
    parser.add_argument("--bundle_glob", default="ingestor_bundle*.zip", help="Glob for the job zip in CWD")
    parser.add_argument("--extra_env", action="append", default=[], help="Repeatable KEY=VALUE entries to export to os.environ")

    # tolerate Glue's extra flags
    args, unknown = parser.parse_known_args()
    if unknown:
        print(f"[runner] Ignoring {len(unknown)} unknown args from Glue: {unknown[:8]}{' ...' if len(unknown)>8 else ''}")
    return args


def main() -> int:
    args = _parse_args()

    logging.basicConfig(
        level=getattr(logging, str(args.log_level).upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s :: %(message)s",
    )
    log = logging.getLogger("glue_runner")

    # Make the zip content importable (ZIP root only)
    setup_path(args.bundle_glob)

    # Apply any extra env vars (KEY=VALUE)
    for kv in args.extra_env:
        if "=" in kv:
            k, v = kv.split("=", 1)
            os.environ[k] = v
            log.info("Set env %s", k)
        else:
            log.warning("Skipping malformed --extra_env %r (expected KEY=VALUE)", kv)

    # Import the wrapper after sys.path is patched
    try:
        wrapper = importlib.import_module("ingestor_wrapper")
    except Exception as e:
        log.error("Could not import 'ingestor_wrapper' after sys.path setup: %s", e)
        return 2

    log.info(
        "Starting wrapper: table=%s env=%s yaml_path=%s mode=%s",
        args.table, args.env_name, args.yaml_path, args.run_mode
    )

    try:
        meta = wrapper.run_ingestor(
            table=args.table,
            env_name=args.env_name,
            yaml_path=args.yaml_path,
            run_mode=args.run_mode,
            start=args.backfill_start,
            end=args.backfill_end,
        )
        print(json.dumps({"status": "ok", "meta": meta}))
        return 0
    except Exception:
        log.error("Ingestor wrapper run failed.", exc_info=True)
        print(json.dumps({"status": "error", "stage": "run_ingestor"}))
        return 3


if __name__ == "__main__":
    raise SystemExit(main())

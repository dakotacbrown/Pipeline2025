import sys
import os
import json
import argparse
import logging
from pathlib import Path

# -------------------------------
# Prepare: add job ZIP to sys.path
# -------------------------------
# NOTE: This intentionally mirrors the old implementation (your screenshot):
#  - rglob a fixed pattern
#  - error if >1 match
#  - add "<zip>/<zip_name>/" and "<zip>/<zip_name>/Python" to sys.path
#  - also add "<zip>/" and "<zip>/Python"
# Keep the pattern the same as your bundle name.
files = list(Path.cwd().rglob("ingestor_bundle*.zip"))

def setup_path():
    """Set up sys.path like the old Glue 4 runner."""
    files = list(Path.cwd().rglob("ingestor_bundle*.zip"))
    if len(files) > 1:
        raise ValueError(f"More than one ingestor_bundle zip found: {files}")
    elif len(files) == 1:
        zip_file_with_zip = files[0].name
        zip_file_without_zip = zip_file_with_zip.split(".zip")[0]

        # 1) <zip>/<zip_name>/
        base_path = f"{zip_file_with_zip}/{zip_file_without_zip}/"
        sys.path.insert(0, base_path)

        # 2) <zip>/ and <zip>/Python
        base_path = f"{zip_file_with_zip}/"
        sys.path.insert(0, base_path)
    else:
        # 0 files: do nothing (kept identical to screenshot behavior)
        pass

# Call the prepare step
setup_path()

# --------------------------------
# Parse job parameters (Glue args)
# --------------------------------
def _parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-y", "--yaml_path", required=True, help="Path inside the zip (e.g. config/ingestor.yml)")
    parser.add_argument("--table", required=True, help="Table key under 'apis'")
    parser.add_argument("--env", dest="env_name", required=True, help="Env key under 'envs'")
    parser.add_argument("--run_mode", choices=["once", "backfill"], default="once")
    parser.add_argument("--backfill_start")
    parser.add_argument("--backfill_end")
    parser.add_argument("--log_level", default="INFO")
    parser.add_argument("--extra_env", action="append", default=[], help="KEY=VALUE; repeatable")

    # Glue often passes extra arguments; ignore them
    args, unknown = parser.parse_known_args()
    if unknown:
        print(f"[runner] Ignoring unknown args: {unknown[:8]}{' ...' if len(unknown)>8 else ''}")
    return args

# --------------------------------
# Main: import wrapper and run it
# --------------------------------
def main() -> None:
    args = _parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s glue_runner :: %(message)s",
    )
    log = logging.getLogger("glue_runner")

    # Optional: export any extra envs
    for kv in args.extra_env:
        if "=" in kv:
            k, v = kv.split("=", 1)
            os.environ[k] = v
            log.info("Set env %s", k)

    # Import the wrapper from the job zip we just put on sys.path
    from ingestor_wrapper import run_ingestor

    log.info(
        "Starting run: table=%s env=%s yaml=%s mode=%s",
        args.table, args.env_name, args.yaml_path, args.run_mode
    )

    meta = run_ingestor(
        table=args.table,
        env_name=args.env_name,
        yaml_path=args.yaml_path,
        run_mode=args.run_mode,
        start=args.backfill_start,
        end=args.backfill_end,
    )
    print(json.dumps({"status": "ok", "meta": meta}))

if __name__ == "__main__":
    main()

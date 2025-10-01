import sys
import os
import json
import argparse
import logging
from pathlib import Path


def setup_path(bundle_glob: str = "ingestor_bundle*.zip") -> None:
    """
    Find the bundle zip that Glue staged locally and add it to sys.path.

    Matches the 'photo-style' logic:
      1) rglob the working dir for <bundle_glob>
      2) If multiple, prefer the one under '/extra-py-files/'
      3) Add BOTH:
         - <zip>/
         - <zip>/<zip_name_without_.zip>/Python
    """
    files = list(Path.cwd().rglob(bundle_glob))

    if len(files) == 0:
        print(json.dumps({"bundle_glob": bundle_glob, "matches": 0}))
        return

    if len(files) == 1:
        chosen = files[0]
    else:
        preferred = [p for p in files if "extra-py-files" in str(p)]
        chosen = preferred[0] if preferred else files[0]

    zip_file_with_zip = chosen.name
    zip_file_without_zip = zip_file_with_zip.split(".zip")[0]

    base_path = f"{chosen}/"
    python_path = f"{chosen}/{zip_file_without_zip}/Python"

    # Add both to sys.path so either layout works
    sys.path.insert(0, base_path)
    sys.path.insert(0, python_path)

    print(
        json.dumps(
            {
                "bundle_glob": bundle_glob,
                "chosen_bundle": str(chosen),
                "added_sys_path": [base_path, python_path],
                "all_matches": [str(p) for p in files],
            }
        )
    )


def parse_args():
    p = argparse.ArgumentParser(description="Glue runner for ApiIngestor")
    # wrapper inputs
    p.add_argument("--yaml_path", required=True, help="Path to YAML (e.g., res:config/ingestor.yml)")
    p.add_argument("--table", required=True, help="Table name under apis.* in YAML")
    p.add_argument("--env", dest="env_name", required=True, help="Env key under envs.* in YAML")

    # >>> NEW: explicit run mode (optional). If omitted, wrapper will infer.
    p.add_argument(
        "--run_mode",
        choices=["once", "backfill"],
        default=None,
        help="Execution mode. If omitted, wrapper infers from backfill args.",
    )

    p.add_argument("--backfill_start", default=None, help="YYYY-MM-DD (optional)")
    p.add_argument("--backfill_end", default=None, help="YYYY-MM-DD (optional)")
    p.add_argument("--log_level", default=os.getenv("LOG_LEVEL", "INFO"))

    # allow passing extra env as JSON if needed
    p.add_argument("--extra_env", default=None, help='JSON dict of env overrides, e.g. {"VENDOR_A_TOKEN":"xyz"}')

    # job parameter (or env) for the bundle glob
    p.add_argument(
        "--bundle_glob",
        "--BUNDLE_GLOB",
        dest="bundle_glob",
        default=None,
        help='Glob for the job zip (default "ingestor_bundle*.zip")',
    )
    return p.parse_args()


def main() -> int:
    args = parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s :: %(message)s",
    )
    log = logging.getLogger("glue_runner")
    log.info("Starting Glue runner.")

    # Optional env overrides
    if args.extra_env:
        try:
            mapping = json.loads(args.extra_env)
            for k, v in mapping.items():
                os.environ[str(k)] = str(v)
            log.info("Applied extra_env overrides: %s", list(mapping.keys()))
        except Exception:
            log.exception("Failed to parse --extra_env JSON; continuing without overrides.")

    # Ensure the bundle is on sys.path before importing wrapper
    bundle_glob = args.bundle_glob or os.getenv("BUNDLE_GLOB") or "ingestor_bundle*.zip"
    setup_path(bundle_glob=bundle_glob)

    # import after sys.path is primed
    try:
        from ingestor_wrapper import run_ingestor
    except Exception:
        log.exception("Failed to import 'ingestor_wrapper'. sys.path may not include the bundle.")
        return 2

    try:
        meta = run_ingestor(
            yaml_path=args.yaml_path,
            table=args.table,
            env_name=args.env_name,
            run_mode=args.run_mode,                 # <<< pass run_mode
            backfill_start=args.backfill_start,
            backfill_end=args.backfill_end,
            logger=log,
            log_level=args.log_level,
        )
        print(json.dumps({"status": "ok", "stage": "run_ingestor", "meta": meta}, default=str))
        return 0
    except Exception as e:
        log.exception("Ingestor wrapper run failed.")
        print(json.dumps({"status": "error", "stage": "run_ingestor", "error": str(e)}))
        return 3


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())

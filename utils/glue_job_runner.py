# run_step.py
import json
import logging
import sys
from pathlib import Path

LOG = logging.getLogger("glue_runner")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s :: %(message)s",
)

# ----------------------------------------------------------------------
# Zip discovery & sys.path injection (mirrors the screenshot’s approach)
# ----------------------------------------------------------------------
# NOTE: Override the pattern with --zip_glob if you like (see argparse below).
DEFAULT_ZIP_GLOB = "ingestor_bundle*.zip"

def setup_path(zip_glob: str = DEFAULT_ZIP_GLOB) -> None:
    """
    Find the single job zip in CWD and add the two typical paths to sys.path.
    """
    files = list(Path.cwd().rglob(zip_glob))
    if len(files) > 1:
        raise ValueError(f"More than one job zip found via '{zip_glob}': {files}")
    elif len(files) == 1:
        zip_with_ext = files[0].name
        zip_without_ext = zip_with_ext.split(".zip")[0]

        # First pair
        base_path = f"{zip_with_ext}/{zip_without_ext}/"
        sys.path.insert(0, base_path)

        # Second pair
        base_with_path = f"{zip_with_ext}/"
        sys.path.insert(0, base_with_path)

        LOG.info("Added to sys.path: %s", sys.path[:4])
    else:
        # No-op; your wrapper may still be co-located with this script
        LOG.info("No zip found with pattern '%s' in CWD; continuing.", zip_glob)

# Call the setup immediately so imports below can resolve
# (We also parse args first to allow --zip_glob to customize the pattern.)
# ----------------------------------------------------------------------
def _build_arg_parser():
    import argparse

    p = argparse.ArgumentParser(
        description="Glue runner for ApiIngestor wrapper (parses job parameters)."
    )
    # Accept BOTH lowercase and uppercase flags (Glue UI often uses --UPPER)
    p.add_argument("--table", "--TABLE", dest="table", required=True)
    p.add_argument("--env", "--ENV", dest="env", required=True)
    p.add_argument(
        "--yaml_path", "--YAML_PATH",
        dest="yaml_path",
        default="config/ingestor.yml",
        help="Path to the ingestor YAML *inside* the bundle (or a package resource path)."
    )
    p.add_argument(
        "--run_mode", "--RUN_MODE",
        dest="run_mode",
        choices=["once", "backfill"],
        default="once",
        help="Run a one-off fetch or a backfill over a date range."
    )
    p.add_argument("--start", "--START", dest="start", default=None,
                   help="Backfill start date (YYYY-MM-DD). Required for run_mode=backfill.")
    p.add_argument("--end", "--END", dest="end", default=None,
                   help="Backfill end date (YYYY-MM-DD). Required for run_mode=backfill.")
    p.add_argument("--zip_glob", "--ZIP_GLOB", dest="zip_glob",
                   default=DEFAULT_ZIP_GLOB,
                   help=f"Zip filename glob to add to sys.path (default '{DEFAULT_ZIP_GLOB}').")
    p.add_argument("--log_level", "--LOG_LEVEL", dest="log_level",
                   default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    return p

def _parse_args():
    # Try Glue's resolver first (safe if library exists), else argparse.
    try:
        from awsglue.utils import getResolvedOptions  # type: ignore
        # The keys here must be UPPERCASE for Glue
        opts = getResolvedOptions(
            sys.argv,
            ["TABLE", "ENV", "YAML_PATH", "RUN_MODE", "START", "END", "ZIP_GLOB", "LOG_LEVEL"],
        )
        return {
            "table": opts.get("TABLE"),
            "env": opts.get("ENV"),
            "yaml_path": opts.get("YAML_PATH", "config/ingestor.yml"),
            "run_mode": (opts.get("RUN_MODE") or "once").lower(),
            "start": opts.get("START"),
            "end": opts.get("END"),
            "zip_glob": opts.get("ZIP_GLOB", DEFAULT_ZIP_GLOB),
            "log_level": opts.get("LOG_LEVEL", "INFO").upper(),
        }
    except Exception:
        # Fallback to argparse (local runs, unit tests, etc.)
        parser = _build_arg_parser()
        ns, _ = parser.parse_known_args()
        return {
            "table": ns.table,
            "env": ns.env,
            "yaml_path": ns.yaml_path,
            "run_mode": ns.run_mode.lower(),
            "start": ns.start,
            "end": ns.end,
            "zip_glob": ns.zip_glob,
            "log_level": ns.log_level.upper(),
        }

def main() -> int:
    args = _parse_args()

    # Now that we have --zip_glob, add the bundle to sys.path
    setup_path(args["zip_glob"])

    # Adjust log level after parsing
    LOG.setLevel(getattr(logging, args["log_level"], logging.INFO))
    LOG.info("Resolved args: %s", json.dumps({k: v for k, v in args.items() if k != 'zip_glob'}))

    # Import AFTER sys.path is set so we can find the wrapper in the zip
    try:
        import ingestor_wrapper  # your wrapper module at the root of the zip
    except Exception as e:
        LOG.exception("Failed to import 'ingestor_wrapper' from zip/system path.")
        print(json.dumps({"status": "error", "stage": "import_wrapper", "error": str(e)}))
        return 2

    # Run the ingestor via the wrapper
    try:
        meta = ingestor_wrapper.run_ingestor(
            table=args["table"],
            env_name=args["env"],
            yaml_path=args["yaml_path"],
            run_mode=args["run_mode"],
            start=args["start"],
            end=args["end"],
        )
        LOG.info("Ingestor finished: %s", json.dumps(meta))
        print(json.dumps(meta))  # convenient for scraping
        return 0
    except SystemExit as se:
        # keep Glue happy with a clean numeric exit
        code = int(getattr(se, "code", 1) or 0)
        LOG.error("Wrapper raised SystemExit(%s)", code)
        return code
    except Exception as e:
        LOG.exception("Ingestor wrapper run failed.")
        print(json.dumps({"status": "error", "stage": "run_ingestor", "error": str(e)}))
        return 3

if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())

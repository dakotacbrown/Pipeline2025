import argparse
import glob
import json
import logging
import os
import sys
from typing import Optional

LOG = logging.getLogger("glue_runner")
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)

# ---------- Optional: add local ZIP to sys.path if --extra-py-files wasn't set ----------

def _add_zip_to_syspath(zip_prefix: str = "ingestor_bundle", explicit_name: Optional[str] = None) -> None:
    """
    If the ingestor ZIP isn't already on sys.path, try to find it locally
    (CWD or /tmp) and add it. Useful when the job wasn't configured with
    --extra-py-files.

    We add the ZIP root; if a 'Python' dir exists inside, we add that too.
    """
    candidates = []
    if explicit_name:
        for base in (os.getcwd(), "/tmp"):
            p = os.path.join(base, explicit_name)
            if os.path.exists(p):
                candidates.append(p)
    else:
        for base in (os.getcwd(), "/tmp"):
            candidates.extend(glob.glob(os.path.join(base, f"{zip_prefix}*.zip")))

    if not candidates:
        LOG.info("No local ingestor bundle found (prefix=%r, explicit=%r).", zip_prefix, explicit_name)
        return

    zip_path = candidates[0]
    if zip_path not in sys.path:
        sys.path.insert(0, zip_path)
        LOG.info("Added ZIP to sys.path: %s", zip_path)

    # Some bundles place code under ZIP/Python — add if present.
    py_subdir = f"{zip_path}/Python"
    if os.path.exists(py_subdir) and py_subdir not in sys.path:
        sys.path.insert(0, py_subdir)
        LOG.info("Added ZIP/Python to sys.path: %s", py_subdir)

# ---------- Hard-coded env overrides (edit these as needed) ----------

def _apply_manual_env_overrides():
    """
    A single place to hard-code environment variables that ApiIngestor will read
    via ${ENV_VAR} substitution in your YAML.
    """
    overrides = {
        # Example secrets/tokens / config:
        # "VENDOR_A_TOKEN": "REPLACE_ME",
        # "SFDC_TOKEN": "REPLACE_ME",
        # "AWS_DEFAULT_REGION": "us-east-1",

        # S3 output defaults (only if your YAML references them):
        # "INGEST_S3_BUCKET": "your-bucket",
        # "INGEST_S3_PREFIX": "ingestor/outputs/",
    }

    for k, v in overrides.items():
        # setdefault -> won't clobber values already provided by the job config
        os.environ.setdefault(k, str(v))


# ---------- Main ----------

def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Glue runner for ApiIngestor")
    parser.add_argument("--table", required=True, help="Table name in YAML (apis.<table>)")
    parser.add_argument("--env", dest="env_name", required=True, help="Environment key in YAML (envs.<env>)")
    # Optional hints if you’re NOT using --extra-py-files:
    parser.add_argument("--bundle-prefix", default=os.getenv("INGESTOR_BUNDLE_PREFIX", "ingestor_bundle"),
                        help="Local ZIP filename prefix to search when not using --extra-py-files")
    parser.add_argument("--bundle-name", default=os.getenv("INGESTOR_BUNDLE_NAME"),
                        help="Exact local ZIP filename to load (overrides prefix search)")
    parser.add_argument("--dry-run", action="store_true", help="Parse args / set env / import wrapper, then exit")

    args = parser.parse_args(argv)

    # Make sure our local bundle is importable if --extra-py-files wasn't set
    try:
        from ingestor_wrapper import run_ingestor_from_env  # type: ignore
        LOG.info("Imported wrapper from existing sys.path.")
    except ModuleNotFoundError:
        LOG.info("ingestor_wrapper not found on sys.path; attempting to locate local ZIP...")
        _add_zip_to_syspath(args.bundle_prefix, args.bundle_name)
        # Try again after modifying sys.path
        from ingestor_wrapper import run_ingestor_from_env  # type: ignore

    # Apply manual env overrides (won't overwrite pre-set env vars)
    _apply_manual_env_overrides()

    LOG.info("Starting ingestion: table=%s env=%s", args.table, args.env_name)

    if args.dry_run:
        LOG.info("Dry run complete. Exiting 0.")
        return 0

    # Delegate to the wrapper (which reads the YAML packaged in the ZIP)
    meta = run_ingestor_from_env(table_name=args.table, env_name=args.env_name)

    # Log JSON to make CloudWatch copy/paste friendly
    LOG.info("Ingestion complete. Metadata:\n%s", json.dumps(meta, indent=2, default=str))
    # Also print on stdout in case you capture output
    print(json.dumps(meta, default=str))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

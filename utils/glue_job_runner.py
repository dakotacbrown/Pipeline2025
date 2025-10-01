#!/usr/bin/env python3
"""
Glue entrypoint that:
  1) parses Glue Job parameters (e.g., --mode, --table, --env, --start, --end, --yaml_path, --set),
  2) exports them into OS environment variables (so ${...} placeholders in YAML work),
  3) calls the ingestor wrapper to run once or backfill,
  4) prints the returned metadata as JSON.

Usage examples (Glue UI → Job parameters):
  --mode        run_once
  --table       orders_with_details
  --env         prod
  --yaml_path   ingestor.yml

Backfill example:
  --mode        backfill
  --table       events_api
  --env         qa
  --start       2025-01-01
  --end         2025-01-15

Env injection (repeat --set to add many):
  --set         OUT_BUCKET=my-bucket
  --set         OUT_PREFIX=ingestor/exports
  --set         VENDOR_A_TOKEN=eyJhbGciOi...<redacted>...
"""
import argparse
import json
import logging
import os
import sys
from typing import Dict

# The wrapper is packaged in the same Python library ZIP on the Glue job
from ingestor_wrapper import run_ingestor  # noqa: E402


def _parse_kv(s: str) -> Dict[str, str]:
    """
    Parse KEY=VALUE strings (for --set). Whitespace around '=' is not allowed.
    """
    if "=" not in s:
        raise argparse.ArgumentTypeError(
            f"--set expects KEY=VALUE but got: {s!r}"
        )
    k, v = s.split("=", 1)
    k = k.strip()
    if not k:
        raise argparse.ArgumentTypeError("--set key cannot be empty")
    return {k: v}


def _export_env(core: argparse.Namespace, extra_sets: Dict[str, str]):
    """
    Export both core args and user-provided --set pairs to os.environ
    so YAML can reference them via ${VARNAME}.
    """
    # Core arguments -> env (helpful for YAML placeholders or logging)
    mappings = {
        "RUN_MODE": core.mode or "",
        "TABLE_NAME": core.table or "",
        "ENV_NAME": core.env or "",
        "BACKFILL_START": core.start or "",
        "BACKFILL_END": core.end or "",
        "INGEST_YAML_PATH": core.yaml_path or "ingestor.yml",
    }
    for k, v in mappings.items():
        if v:
            os.environ[k] = v

    # User-supplied overrides
    for k, v in extra_sets.items():
        os.environ[k] = v


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Glue runner for ApiIngestor (env-backed config)."
    )
    parser.add_argument(
        "--mode",
        choices=["run_once", "backfill"],
        required=True,
        help="One-off pull or backfill windowing.",
    )
    parser.add_argument(
        "--table", required=True, help="Table key under 'apis:' in YAML."
    )
    parser.add_argument(
        "--env", dest="env", required=True, help="Env key under 'envs:' in YAML."
    )
    parser.add_argument(
        "--start",
        required=False,
        help="YYYY-MM-DD (required for --mode backfill).",
    )
    parser.add_argument(
        "--end",
        required=False,
        help="YYYY-MM-DD (required for --mode backfill).",
    )
    parser.add_argument(
        "--yaml_path",
        default="ingestor.yml",
        help="Path to the YAML file INSIDE the library ZIP (default: ingestor.yml).",
    )
    parser.add_argument(
        "--set",
        dest="set_pairs",
        action="append",
        type=_parse_kv,
        default=[],
        help="Inject KEY=VALUE into process env (repeatable).",
    )
    args = parser.parse_args()

    # Flatten list[dict] from repeated --set into one dict
    extra_sets: Dict[str, str] = {}
    for d in args.set_pairs:
        extra_sets.update(d)

    # Basic logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    log = logging.getLogger("glue_runner")

    # Export env so YAML ${...} can see them
    _export_env(args, extra_sets)

    # Log a minimal view (avoid logging secrets!)
    log.info(
        "Starting run: mode=%s table=%s env=%s yaml=%s",
        args.mode,
        args.table,
        args.env,
        args.yaml_path,
    )
    if args.mode == "backfill":
        log.info("Backfill window: start=%s end=%s", args.start, args.end)

    # Call the wrapper (it loads YAML and runs the ApiIngestor)
    meta = run_ingestor(
        yaml_path=args.yaml_path,
        mode=args.mode,
        table=args.table,
        env_name=args.env,
        start=args.start,
        end=args.end,
        logger=log,
    )

    # Print metadata as JSON to stdout for downstream capture
    print(json.dumps(meta, indent=2, default=str))
    log.info("Done. Rows=%s, Bytes=%s, Sink=%s", meta.get("rows"), meta.get("bytes"), meta.get("s3_uri"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

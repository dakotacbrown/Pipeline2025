# src/run_step.py
import argparse
import json
import os
import sys
from pathlib import Path
from fnmatch import fnmatch


def setup_path(
    pattern: str = "debi-etl-framework-glue*.zip",
    search_dirs: list[Path] | None = None,
    sys_path: list[str] | None = None,
) -> None:
    """
    Add the job zip (and common 'src/' locations inside it) to sys.path.

    Why:
      - In AWS Glue, the zip is often not in Path.cwd(), so we:
          1) first look for the zip already present on sys.path
          2) otherwise search common directories (/tmp, cwd) using rglob

      - For Python imports to work, sys.path must include:
          - the zip file itself (best), and/or
          - zip + "/src" if your modules live under src/ inside the zip

    This function is written to be unit-testable by injecting sys_path/search_dirs.
    """
    sp = sys_path if sys_path is not None else sys.path
    dirs = (
        search_dirs if search_dirs is not None else [Path("/tmp"), Path.cwd()]
    )

    # 1) Prefer: the zip already on sys.path (Glue commonly adds --extra-py-files)
    zip_entry = next(
        (
            p
            for p in sp
            if p.endswith(".zip") and fnmatch(Path(p).name, pattern)
        ),
        None,
    )

    # 2) Otherwise search filesystem
    if not zip_entry:
        matches: list[Path] = []
        for d in dirs:
            try:
                matches.extend(list(d.rglob(pattern)))
            except Exception:
                # ignore unreadable dirs in Glue
                continue

        if len(matches) > 1:
            raise ValueError(f"More than one {pattern} zip found: {matches}")
        if len(matches) == 0:
            # Nothing to do (tests may run without the zip present)
            return

        zip_entry = str(matches[0])

    zip_name = Path(zip_entry).name
    zip_without = zip_name[:-4] if zip_name.endswith(".zip") else zip_name

    # These cover common layouts:
    #   <zip>/<zip_name_without_zip>/src
    #   <zip>/src
    # And we also add the zip itself so top-level packages resolve.
    candidates = [
        zip_entry,  # best: add the zip itself
        f"{zip_entry}/src",
        f"{zip_name}/{zip_without}/src",
        f"{zip_name}/{zip_without}/",
        f"{zip_name}/src",
        f"{zip_name}/",
    ]

    # Insert in reverse so the first candidate ends up highest priority.
    for p in reversed(candidates):
        if p and p not in sp:
            sp.insert(0, p)


# Call the prepare step BEFORE importing packages that live in the zip
setup_path()

from asvc1scoredataservices_common.github.common import (
    GithubConnection,
)  # noqa: E402
from asvc1scoredataservices_common.logger.basic_logger import (
    setup_logger,
)  # noqa: E402

log = setup_logger()


# --------------------------------
# Parse job parameters (Glue args)
# --------------------------------
def _parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-y",
        "--yaml_path",
        required=True,
        help="Path inside the zip (e.g. config/ingestor.yml)",
    )
    parser.add_argument("--table", required=True, help="Table key under 'apis'")
    parser.add_argument(
        "--env", dest="env_name", required=True, help="Env key under 'envs'"
    )
    parser.add_argument(
        "--run_mode", choices=["once", "backfill"], default="once"
    )
    parser.add_argument("--backfill_start")
    parser.add_argument("--backfill_end")
    parser.add_argument("--log_level", default="INFO")
    parser.add_argument(
        "--extra_env", action="append", default=[], help="KEY=VALUE; repeatable"
    )

    # Glue often passes extra arguments; ignore them
    args, unknown = parser.parse_known_args()
    if unknown:
        print(
            f"[runner] Ignoring unknown args: {unknown[:8]}{' ...' if len(unknown) > 8 else ''}"
        )
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
        args.table,
        args.env_name,
        args.yaml_path,
        args.run_mode,
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

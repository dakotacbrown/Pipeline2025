import argparse
import json
import os
import sys
from fnmatch import fnmatch
from pathlib import Path
from typing import List, Optional


def setup_path(
    pattern: str = "debi-etl-framework-glue*.zip",
    search_dirs: Optional[List[Path]] = None,
    sys_path: Optional[List[str]] = None,
) -> None:
    """
    Add the job zip (and common 'src/' locations inside it) to sys.path.

    Important: if 'asvclscoredataservices_common/' is at the TOP of the zip,
    the zip file ITSELF must be on sys.path.
    """
    sp = sys_path if sys_path is not None else sys.path
    dirs = (
        search_dirs if search_dirs is not None else [Path("/tmp"), Path.cwd()]
    )

    # 1) Prefer zip already on sys.path (Glue commonly adds --extra-py-files)
    zip_entry = next(
        (
            p
            for p in sp
            if isinstance(p, str)
            and p.endswith(".zip")
            and fnmatch(Path(p).name, pattern)
        ),
        None,
    )

    # 2) Otherwise search filesystem
    if not zip_entry:
        matches: List[Path] = []
        for d in dirs:
            try:
                matches.extend(list(d.rglob(pattern)))
            except Exception:
                # ignore unreadable dirs in Glue
                continue

        if len(matches) > 1:
            raise ValueError(f"More than one ({pattern}) zip found: {matches}")
        if len(matches) == 0:
            # Nothing to do (tests may run without the zip present)
            return

        zip_entry = str(matches[0])

    zip_name = Path(zip_entry).name
    zip_without = zip_name[:-4] if zip_name.endswith(".zip") else zip_name

    # Common layouts we want to support
    candidates = [
        zip_entry,  # best: add zip itself
        f"{zip_entry}/src",
        f"{zip_name}/{zip_without}/src",
        f"{zip_name}/src",
        f"{zip_name}/",
    ]

    # Promote candidates so the first ends up highest priority
    for p in reversed(candidates):
        if not p:
            continue
        if p in sp:
            sp.remove(p)
        sp.insert(0, p)


def _parse_args(argv: Optional[List[str]] = None):
    parser = argparse.ArgumentParser()

    parser.add_argument("--env", required=True, help="Env key under 'envs'")
    parser.add_argument(
        "--run_mode",
        choices=["once", "backfill"],
        default="once",
    )
    parser.add_argument(
        "-v",
        "--vendor",
        required=True,
        help="Vendor being ingested",
    )
    parser.add_argument(
        "-t",
        "--table",
        required=True,
        help="Table key under 'apis'",
    )

    # IMPORTANT: parse JSON so api_wrapper receives a dict
    parser.add_argument(
        "--event",
        required=True,
        type=json.loads,
        help="Secrets and other data passed to the ingester (JSON)",
    )

    parser.add_argument(
        "-f", "--file_path", required=True, help="Path to YAML in repo"
    )
    parser.add_argument(
        "-r", "--repo_name", required=True, help="GitHub repo name"
    )
    parser.add_argument(
        "-g", "--github_token", required=True, help="GitHub token"
    )

    parser.add_argument("--start_date")
    parser.add_argument("--end_date")
    parser.add_argument("--log_level", default="INFO")

    parser.add_argument(
        "--extra_env",
        action="append",
        default=[],
        help="KEY=VALUE; repeatable",
    )

    args, unknown = parser.parse_known_args(argv)

    if unknown:
        print(
            f"[runner] Ignoring unknown args: {unknown[:8]}"
            + ("..." if len(unknown) > 8 else "")
        )

    return args


def main(argv: Optional[List[str]] = None) -> None:
    # Call BEFORE importing packages that live in the zip
    setup_path()

    # Delay imports (keeps unit tests easy, and still works in Glue)
    from asvc1scoredataservices_common.github.common import (  # noqa: E402
        GithubConnection,
    )
    from asvc1scoredataservices_common.logger.basic_logger import (  # noqa: E402
        setup_logger,
    )

    log = setup_logger()
    args = _parse_args(argv)

    github_conn = GithubConnection(log, args.github_token, args.repo_name)
    full_yaml = github_conn.get_github_file_contents(args.file_path)

    # Optional: export extra envs
    for kv in args.extra_env:
        if "=" in kv:
            k, v = kv.split("=", 1)
            os.environ[k] = v
            log.info("Set env %s", k)

    from src.api_wrapper import run_ingester  # noqa: E402

    log.info(
        "Starting run: table=%s env=%s mode=%s",
        args.table,
        args.env,
        args.run_mode,
    )

    meta = run_ingester(
        table=args.table,
        env=args.env,
        event=args.event,
        config=full_yaml,
        run_mode=args.run_mode,
        start=args.start_date,
        end=args.end_date,
    )

    print(json.dumps({"status": "ok", "meta": meta}))


if __name__ == "__main__":
    main()

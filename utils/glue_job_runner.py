import argparse
import fnmatch
import json
import os
import sys
from pathlib import Path
from typing import Iterable


def setup_path(
    pattern: str = "debi-etl-framework-glue*.zip",
    search_dirs: list[Path] | None = None,
    sys_path: list[str] | None = None,
) -> None:
    """
    Add the job zip (and common 'src/' locations inside it) to sys.path.

    In AWS Glue, the zip may already be on sys.path (from --extra-py-files).
    If not, we search common directories (default: /tmp and cwd) to find it.

    Designed to be unit-testable via injection of sys_path/search_dirs.
    """
    sp = sys_path if sys_path is not None else sys.path
    dirs = (
        search_dirs if search_dirs is not None else [Path("/tmp"), Path.cwd()]
    )

    # 1) Prefer: already present on sys.path (Glue commonly adds --extra-py-files)
    zip_entry = next(
        (
            p
            for p in sp
            if isinstance(p, str)
            and p.endswith(".zip")
            and fnmatch.fnmatch(Path(p).name, pattern)
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

    zip_path = Path(zip_entry)
    zip_name = zip_path.name
    zip_without = zip_name[:-4] if zip_name.endswith(".zip") else zip_name
    zip_parent = str(zip_path.parent)

    # Candidates cover common layouts:
    # - zip itself (best for zipimport)
    # - zip/subdir inside archive (zipimport supports "archive.zip/subdir")
    # - extracted folder next to the zip (defensive; some runtimes unzip)
    candidates = [
        zip_entry,  # best: add the zip itself
        f"{zip_entry}/src",
        f"{zip_entry}/{zip_without}/src",
        f"{zip_entry}/{zip_without}/",
        f"{zip_parent}/{zip_without}/src",
        f"{zip_parent}/{zip_without}/",
        f"{zip_name}/{zip_without}/src",
        f"{zip_name}/{zip_without}/",
        f"{zip_name}/src",
        f"{zip_name}/",
    ]

    # Insert in reverse so the first candidate ends up highest priority.
    for p in reversed(candidates):
        if p and p not in sp:
            sp.insert(0, p)


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
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
        help="Vendor corresponding to the table being ingested",
    )
    parser.add_argument(
        "-t",
        "--table",
        required=True,
        help="Table key under 'apis'",
    )
    parser.add_argument(
        "--event",
        required=True,
        help="Secrets and other data passed to the ingester",
    )
    parser.add_argument(
        "-f",
        "--file_path",
        required=True,
        help="Path to the config file in the GitHub repo",
    )
    parser.add_argument(
        "-r",
        "--repo_name",
        required=True,
        help="GitHub repo name for accessing the config file",
    )
    parser.add_argument(
        "-g",
        "--github_token",
        required=True,
        help="GitHub token for accessing the repo",
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

    # Glue often passes extra args; ignore them
    args, unknown = parser.parse_known_args(argv)
    if unknown:
        print(
            f"[runner] Ignoring unknown args: {unknown[:8]}"
            + (" ..." if len(unknown) > 8 else "")
        )
    return args


def main(argv: list[str] | None = None) -> dict:
    # IMPORTANT: ensure the job zip is on sys.path BEFORE importing common libs
    setup_path()

    # Import after setup_path so Glue can resolve it from the zip
    from asvc1scoredataservices_common.github.common import (  # noqa: E402
        GithubConnection,
    )
    from asvc1scoredataservices_common.logger.basic_logger import (  # noqa: E402
        setup_logger,
    )
    from src.api_wrapper import run_ingester  # noqa: E402

    log = setup_logger()
    args = _parse_args(argv)

    github_conn = GithubConnection(
        log,
        args.github_token,
        args.repo_name,
    )

    full_yaml = github_conn.get_github_file_contents(args.file_path)

    # Optional: export any extra envs
    for kv in args.extra_env:
        if "=" in kv:
            k, v = kv.split("=", 1)
            os.environ[k] = v
            log.info("Set env %s", k)

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
    return meta


if __name__ == "__main__":
    main()

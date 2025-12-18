import os
import sys
import types
from argparse import Namespace
from pathlib import Path

import pytest
from src import run_step


def test_setup_path_single_match_inserts_candidates(tmp_path: Path):
    z = tmp_path / "debi-etl-framework-glue-abc.zip"
    z.write_text("x")

    sp: list[str] = []
    run_step.setup_path(search_dirs=[tmp_path], sys_path=sp)

    assert sp[0] == str(z)
    assert sp[1] == f"{z}/src"
    assert sp[2] == f"{z.name}/{z.stem}/src"
    assert sp[3] == f"{z.name}/{z.stem}/"
    assert sp[4] == f"{z.name}/src"
    assert sp[5] == f"{z.name}/"


def test_setup_path_promotes_existing_zip_to_front(tmp_path: Path):
    z = tmp_path / "debi-etl-framework-glue-abc.zip"
    z.write_text("x")

    sp = ["/something/else", str(z)]
    run_step.setup_path(search_dirs=[], sys_path=sp)

    assert sp[0] == str(z)


def test_setup_path_zero_matches_is_noop(tmp_path: Path):
    sp = ["keepme"]
    run_step.setup_path(search_dirs=[tmp_path], sys_path=sp)
    assert sp == ["keepme"]


def test_setup_path_multiple_matches_raises(tmp_path: Path):
    (tmp_path / "debi-etl-framework-glue-a.zip").write_text("x")
    (tmp_path / "debi-etl-framework-glue-b.zip").write_text("x")

    with pytest.raises(ValueError, match="More than one"):
        run_step.setup_path(search_dirs=[tmp_path], sys_path=[])


def test_main_happy_path_runs(monkeypatch, capsys):
    # Don't mutate real sys.path in this unit test
    monkeypatch.setattr(run_step, "setup_path", lambda *a, **k: None)

    # Fake asvc1... modules that main() imports
    root = types.ModuleType("asvc1scoredataservices_common")
    github_pkg = types.ModuleType("asvc1scoredataservices_common.github")
    github_mod = types.ModuleType("asvc1scoredataservices_common.github.common")
    logger_pkg = types.ModuleType("asvc1scoredataservices_common.logger")
    logger_mod = types.ModuleType(
        "asvc1scoredataservices_common.logger.basic_logger"
    )

    class DummyLog:
        def info(self, *a, **k): ...
        def warning(self, *a, **k): ...

    class DummyGithubConnection:
        def __init__(self, log, token, repo):
            self.log = log
            self.token = token
            self.repo = repo

        def get_github_file_contents(self, file_path):
            return {"some": "yaml"}

    github_mod.GithubConnection = DummyGithubConnection
    logger_mod.setup_logger = lambda: DummyLog()

    monkeypatch.setitem(sys.modules, "asvc1scoredataservices_common", root)
    monkeypatch.setitem(
        sys.modules, "asvc1scoredataservices_common.github", github_pkg
    )
    monkeypatch.setitem(
        sys.modules, "asvc1scoredataservices_common.github.common", github_mod
    )
    monkeypatch.setitem(
        sys.modules, "asvc1scoredataservices_common.logger", logger_pkg
    )
    monkeypatch.setitem(
        sys.modules,
        "asvc1scoredataservices_common.logger.basic_logger",
        logger_mod,
    )

    # Patch args returned by _parse_args
    monkeypatch.setattr(
        run_step,
        "_parse_args",
        lambda argv=None: Namespace(
            env="dev",
            run_mode="once",
            vendor="salesforce",
            table="accounts",
            event={"env_vars": {"dev": {}}, "c1_oauth_url": "https://x"},
            file_path="cfg.yml",
            repo_name="repo",
            github_token="tok",
            start_date=None,
            end_date=None,
            extra_env=["FOO=BAR", "BADVALUE"],
            log_level="INFO",
        ),
    )

    # Patch api_wrapper.run_ingester
    import src.api_wrapper as api_wrapper

    called = {}

    def fake_run_ingester(**kwargs):
        called.update(kwargs)
        return {"meta": "ok"}

    monkeypatch.setattr(api_wrapper, "run_ingester", fake_run_ingester)

    run_step.main([])

    out = capsys.readouterr().out
    assert '"status": "ok"' in out
    assert called["table"] == "accounts"
    assert called["env"] == "dev"
    assert os.environ["FOO"] == "BAR"

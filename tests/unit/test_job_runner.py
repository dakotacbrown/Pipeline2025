import importlib
import json
import os
import sys
import types
from pathlib import Path

import pytest
import requests  # <-- add this


@pytest.fixture()
def run_step_module():
    """
    Import src.run_step. It delays heavy imports until main(), so importing is safe.
    """
    mod = importlib.import_module("src.run_step")
    importlib.reload(mod)
    return mod


def test_setup_path_prefers_existing_zip_in_sys_path(run_step_module):
    mod = run_step_module

    fake_zip = "/tmp/debi-etl-framework-glue-1.0.zip"
    sp = ["/something/else", fake_zip, "/another"]

    mod.setup_path(
        pattern="debi-etl-framework-glue*.zip",
        sys_path=sp,
        search_dirs=[Path("/tmp")],
    )

    assert sp[0] == fake_zip
    assert f"{fake_zip}/src" in sp


def test_setup_path_returns_when_no_zip_found(run_step_module):
    mod = run_step_module
    sp = ["/a", "/b"]

    mod.setup_path(
        pattern="does-not-exist-*.zip", sys_path=sp, search_dirs=[Path("/tmp")]
    )
    assert sp == ["/a", "/b"]


def test_setup_path_raises_if_multiple_matches(run_step_module, tmp_path):
    mod = run_step_module
    (tmp_path / "debi-etl-framework-glue-a.zip").write_text("x")
    (tmp_path / "debi-etl-framework-glue-b.zip").write_text("y")

    with pytest.raises(ValueError):
        mod.setup_path(
            pattern="debi-etl-framework-glue*.zip",
            sys_path=[],
            search_dirs=[tmp_path],
        )


def test_parse_args_parses_event_json_and_extra_env(run_step_module, capsys):
    mod = run_step_module

    argv = [
        "--env",
        "dev",
        "--run_mode",
        "once",
        "-v",
        "vendor1",
        "-t",
        "events_api",
        "--event",
        '{"k":"v"}',
        "-f",
        "path/to/config.yml",
        "-r",
        "some-repo",
        "-g",
        "ghp_token",
        "--extra_env",
        "K1=V1",
        "--extra_env",
        "K2=V2",
        "--unknown",
        "zzz",
    ]

    args = mod._parse_args(argv)

    assert args.env == "dev"
    assert args.run_mode == "once"
    assert args.vendor == "vendor1"
    assert args.table == "events_api"
    assert args.event == {"k": "v"}
    assert args.extra_env == ["K1=V1", "K2=V2"]

    out = capsys.readouterr().out
    assert "Ignoring unknown args" in out


def test_main_happy_path_no_network(run_step_module, monkeypatch, capsys):
    mod = run_step_module

    # ---- block ALL requests network calls (post/get/etc)
    def _no_http(*args, **kwargs):
        raise AssertionError(
            "Network call attempted via requests during unit test"
        )

    monkeypatch.setattr(
        requests.sessions.Session, "request", _no_http, raising=True
    )

    # prevent filesystem/path scanning affecting test
    monkeypatch.setattr(mod, "setup_path", lambda *a, **k: None)

    # ---- stub logger setup
    class DummyLog:
        def info(self, *a, **k):
            return None

    basic_logger_mod = types.ModuleType(
        "asvclscoredataservices_common.logger.basic_logger"
    )
    basic_logger_mod.setup_logger = lambda: DummyLog()

    # ---- stub GithubConnection so it never calls requests
    class DummyGithubConnection:
        def __init__(self, log, token, repo_name):
            self.log = log
            self.token = token
            self.repo_name = repo_name

        def get_github_file_contents(self, file_path):
            assert file_path == "cfg.yml"
            return {"yaml": "dict"}

    github_common_mod = types.ModuleType(
        "asvclscoredataservices_common.github.common"
    )
    github_common_mod.GithubConnection = DummyGithubConnection

    # install stubs BEFORE main() imports them
    monkeypatch.setitem(
        sys.modules,
        "asvclscoredataservices_common",
        types.ModuleType("asvclscoredataservices_common"),
    )
    monkeypatch.setitem(
        sys.modules,
        "asvclscoredataservices_common.logger",
        types.ModuleType("asvclscoredataservices_common.logger"),
    )
    monkeypatch.setitem(
        sys.modules,
        "asvclscoredataservices_common.logger.basic_logger",
        basic_logger_mod,
    )
    monkeypatch.setitem(
        sys.modules,
        "asvclscoredataservices_common.github",
        types.ModuleType("asvclscoredataservices_common.github"),
    )
    monkeypatch.setitem(
        sys.modules,
        "asvclscoredataservices_common.github.common",
        github_common_mod,
    )

    # stub src.api_wrapper.run_ingester imported inside main
    api_wrapper_stub = types.ModuleType("src.api_wrapper")

    def fake_run_ingester(*, table, env, event, config, run_mode, start, end):
        assert table == "events_api"
        assert env == "dev"
        assert event == {"k": "v"}
        assert config == {"yaml": "dict"}
        assert run_mode == "once"
        assert start is None
        assert end is None
        return {"ok": True}

    api_wrapper_stub.run_ingester = fake_run_ingester
    monkeypatch.setitem(sys.modules, "src.api_wrapper", api_wrapper_stub)

    # clean env
    monkeypatch.delenv("X", raising=False)

    argv = [
        "--env",
        "dev",
        "--run_mode",
        "once",
        "-v",
        "vendor1",
        "-t",
        "events_api",
        "--event",
        json.dumps({"k": "v"}),
        "-f",
        "cfg.yml",
        "-r",
        "repo",
        "-g",
        "token",
        "--extra_env",
        "X=1",
    ]

    mod.main(argv)

    assert os.environ["X"] == "1"

    out = capsys.readouterr().out.strip()
    payload = json.loads(out)
    assert payload["status"] == "ok"
    assert payload["meta"] == {"ok": True}

import json
import os
import sys
import types
from pathlib import Path

import pytest
import src.run_step as run_step


def _ensure_module(monkeypatch, dotted_name: str) -> types.ModuleType:
    """
    Ensure a module path exists in sys.modules:
      "a.b.c" -> creates "a", "a.b", "a.b.c"
    Returns the leaf module.
    """
    parts = dotted_name.split(".")
    cur = ""
    parent = None
    for p in parts:
        cur = f"{cur}.{p}" if cur else p
        if cur not in sys.modules:
            mod = types.ModuleType(cur)
            monkeypatch.setitem(sys.modules, cur, mod)
            # attach as attribute on parent package
            if parent is not None:
                setattr(parent, p, mod)
        parent = sys.modules[cur]
    return sys.modules[dotted_name]


def test_setup_path_no_matches_no_change(tmp_path):
    sp = ["existing"]
    run_step.setup_path(
        pattern="debi-etl-framework-glue*.zip",
        search_dirs=[tmp_path],
        sys_path=sp,
    )
    assert sp == ["existing"]


def test_setup_path_multiple_matches_raises(tmp_path):
    (tmp_path / "debi-etl-framework-glue-a.zip").write_text("x")
    (tmp_path / "debi-etl-framework-glue-b.zip").write_text("x")

    sp = []
    with pytest.raises(ValueError, match=r"More than one .* zip found"):
        run_step.setup_path(
            pattern="debi-etl-framework-glue*.zip",
            search_dirs=[tmp_path],
            sys_path=sp,
        )


def test_setup_path_prefers_zip_already_on_sys_path():
    zip_entry = "/tmp/debi-etl-framework-glue-thing.zip"
    sp = ["one", zip_entry, "two"]

    run_step.setup_path(
        pattern="debi-etl-framework-glue*.zip",
        search_dirs=[Path("/nope")],
        sys_path=sp,
    )

    # zip entry should be highest priority after insertion
    assert sp[0] == zip_entry
    assert f"{zip_entry}/src" in sp


def test_parse_args_ignores_unknown_and_collects_extra_env(capsys):
    argv = [
        "--env",
        "dev",
        "--run_mode",
        "once",
        "--vendor",
        "salesforce",
        "--table",
        "Account",
        "--event",
        '{"k":"v"}',
        "--file_path",
        "path/to/config.yml",
        "--repo_name",
        "config_management",
        "--github_token",
        "TOKEN",
        "--extra_env",
        "FOO=bar",
        "--extra_env",
        "BAZ=qux",
        "--some_glue_arg",
        "whatever",
    ]
    args = run_step._parse_args(argv)
    out = capsys.readouterr().out

    assert args.env == "dev"
    assert args.table == "Account"
    assert args.extra_env == ["FOO=bar", "BAZ=qux"]
    assert "Ignoring unknown args" in out


def test_main_happy_path_sets_env_and_calls_ingester(monkeypatch, capsys):
    # --- Fake asvc1scoredataservices_common modules (so import works) ---
    gh_mod = _ensure_module(
        monkeypatch, "asvc1scoredataservices_common.github.common"
    )
    log_mod = _ensure_module(
        monkeypatch, "asvc1scoredataservices_common.logger.basic_logger"
    )

    calls = {
        "github_init": None,
        "github_file": None,
        "run_ingester": None,
        "log_info": [],
    }

    class FakeLogger:
        def info(self, msg, *args):
            calls["log_info"].append((msg, args))

    def fake_setup_logger():
        return FakeLogger()

    class FakeGithubConnection:
        def __init__(self, log, github_token, repo_name):
            calls["github_init"] = (log, github_token, repo_name)

        def get_github_file_contents(self, file_path):
            calls["github_file"] = file_path
            return "YAML_CONTENT"

    gh_mod.GithubConnection = FakeGithubConnection
    log_mod.setup_logger = fake_setup_logger

    # --- Fake src.api_wrapper.run_ingester ---
    api_mod = _ensure_module(monkeypatch, "src.api_wrapper")

    def fake_run_ingester(**kwargs):
        calls["run_ingester"] = kwargs
        return {"meta": "ok"}

    api_mod.run_ingester = fake_run_ingester

    # Avoid touching real sys.path in this unit test
    monkeypatch.setattr(run_step, "setup_path", lambda *a, **k: None)

    argv = [
        "--env",
        "dev",
        "--run_mode",
        "once",
        "--vendor",
        "salesforce",
        "--table",
        "Account",
        "--event",
        '{"hello":"world"}',
        "--file_path",
        "cfg.yml",
        "--repo_name",
        "config_management",
        "--github_token",
        "TOKEN",
        "--start_date",
        "2025-01-01",
        "--end_date",
        "2025-01-31",
        "--extra_env",
        "FOO=bar",
    ]

    # ensure clean env for assertion
    monkeypatch.delenv("FOO", raising=False)

    meta = run_step.main(argv)

    assert os.environ["FOO"] == "bar"
    assert calls["github_file"] == "cfg.yml"
    assert calls["run_ingester"]["table"] == "Account"
    assert calls["run_ingester"]["env"] == "dev"
    assert calls["run_ingester"]["event"] == '{"hello":"world"}'
    assert calls["run_ingester"]["config"] == "YAML_CONTENT"
    assert calls["run_ingester"]["start"] == "2025-01-01"
    assert calls["run_ingester"]["end"] == "2025-01-31"
    assert meta == {"meta": "ok"}

    out = capsys.readouterr().out.strip()
    payload = json.loads(out)
    assert payload["status"] == "ok"
    assert payload["meta"] == {"meta": "ok"}

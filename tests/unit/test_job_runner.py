import json
import sys
import types
from pathlib import Path
from unittest.mock import MagicMock

import pytest

# Adjust this import if your module path differs
# (Based on your screenshots, run_step.py lives under src/)
import src.run_step as run_step


def _install_fake_module(monkeypatch, name: str) -> types.ModuleType:
    """
    Ensure `name` exists in sys.modules as a ModuleType and return it.
    Also creates parent packages so `from a.b.c import X` works.
    """
    parts = name.split(".")
    for i in range(1, len(parts) + 1):
        mod_name = ".".join(parts[:i])
        if mod_name not in sys.modules:
            mod = types.ModuleType(mod_name)
            monkeypatch.setitem(sys.modules, mod_name, mod)
        # Link child as attribute on parent for package-like behavior
        if i > 1:
            parent_name = ".".join(parts[: i - 1])
            child_name = parts[i - 1]
            setattr(sys.modules[parent_name], child_name, sys.modules[mod_name])
    return sys.modules[name]


# -------------------------
# setup_path tests
# -------------------------


def test_setup_path_prefers_zip_already_on_sys_path():
    sp = [
        "/something/else",
        "/opt/job/debi-etl-framework-glue-1.2.3.zip",
    ]

    run_step.setup_path(
        pattern="debi-etl-framework-glue*.zip",
        search_dirs=[Path("/tmp")],  # shouldn't matter; it should pick sys.path
        sys_path=sp,
    )

    # zip itself should be promoted to front (important for top-level packages)
    assert sp[0] == "/opt/job/debi-etl-framework-glue-1.2.3.zip"

    # and candidates should be present (in a stable, high-priority order)
    assert sp[1] == "/opt/job/debi-etl-framework-glue-1.2.3.zip/src"


def test_setup_path_finds_zip_on_filesystem(tmp_path: Path):
    z = tmp_path / "debi-etl-framework-glue-9.9.9.zip"
    z.write_bytes(b"fake zip content")

    sp = []

    run_step.setup_path(
        pattern="debi-etl-framework-glue*.zip",
        search_dirs=[tmp_path],
        sys_path=sp,
    )

    assert sp[0] == str(z)
    assert sp[1] == f"{z}/src"


def test_setup_path_multiple_matches_raises(tmp_path: Path):
    (tmp_path / "debi-etl-framework-glue-a.zip").write_bytes(b"x")
    (tmp_path / "debi-etl-framework-glue-b.zip").write_bytes(b"y")

    with pytest.raises(ValueError):
        run_step.setup_path(
            pattern="debi-etl-framework-glue*.zip",
            search_dirs=[tmp_path],
            sys_path=[],
        )


def test_setup_path_no_match_no_change(tmp_path: Path):
    sp = ["keep-me"]

    run_step.setup_path(
        pattern="debi-etl-framework-glue*.zip",
        search_dirs=[tmp_path],
        sys_path=sp,
    )

    assert sp == ["keep-me"]


# -------------------------
# _parse_args tests
# -------------------------


def test_parse_args_parses_event_json_and_defaults(capsys):
    event = {"hello": "world", "env_vars": {"dev": {"FOO": "bar"}}}

    args = run_step._parse_args(
        [
            "--env",
            "dev",
            "--vendor",
            "x",
            "--table",
            "accounts",
            "--event",
            json.dumps(event),
            "--file_path",
            "path/to/config.yml",
            "--repo_name",
            "my-repo",
            "--github_token",
            "token123",
        ]
    )

    assert args.env == "dev"
    assert args.run_mode == "once"
    assert args.event == event
    assert args.extra_env == []
    # no unknown-args message expected
    out = capsys.readouterr().out
    assert out == ""


def test_parse_args_prints_unknown_args(capsys):
    args = run_step._parse_args(
        [
            "--env",
            "dev",
            "--vendor",
            "x",
            "--table",
            "accounts",
            "--event",
            json.dumps({"k": "v"}),
            "--file_path",
            "path/to/config.yml",
            "--repo_name",
            "my-repo",
            "--github_token",
            "token123",
            "--unknown",
            "1",
            "--unknown2",
            "2",
        ]
    )
    assert args.env == "dev"

    out = capsys.readouterr().out
    assert "[runner] Ignoring unknown args:" in out


# -------------------------
# main tests (delayed imports + runner wiring)
# -------------------------


def test_main_happy_path_calls_github_and_run_ingester_and_prints_json(
    monkeypatch, capsys
):
    # Avoid filesystem scanning logic here (we test setup_path separately)
    monkeypatch.setattr(run_step, "setup_path", MagicMock())

    # Fake logger
    fake_log = MagicMock()
    fake_log.info = MagicMock()

    # Install fake asvc1... modules for delayed imports
    basic_logger_mod = _install_fake_module(
        monkeypatch, "asvc1scoredataservices_common.logger.basic_logger"
    )
    basic_logger_mod.setup_logger = MagicMock(return_value=fake_log)

    # Fake GithubConnection
    github_common_mod = _install_fake_module(
        monkeypatch, "asvc1scoredataservices_common.github.common"
    )

    class FakeGithubConnection:
        def __init__(self, log, token, repo_name):
            self.log = log
            self.token = token
            self.repo_name = repo_name

        def get_github_file_contents(self, file_path):
            return {"yaml": "content", "file_path": file_path}

    github_common_mod.GithubConnection = FakeGithubConnection

    # Fake src.api_wrapper.run_ingester
    api_wrapper_mod = _install_fake_module(monkeypatch, "src.api_wrapper")
    api_wrapper_mod.run_ingester = MagicMock(return_value={"rows": 7})

    # Ensure env is clean for this test
    monkeypatch.delenv("XTRA", raising=False)

    event_dict = {"some": "event"}
    argv = [
        "--env",
        "dev",
        "--vendor",
        "ignored-by-wrapper",  # parsed but not used by main
        "--table",
        "accounts",
        "--event",
        json.dumps(event_dict),
        "--file_path",
        "configs/my.yml",
        "--repo_name",
        "my-repo",
        "--github_token",
        "gh-token",
        "--run_mode",
        "once",
        "--extra_env",
        "XTRA=1",
        "--unknown",
        "ok",
    ]

    run_step.main(argv)

    # run_ingester called with config from GitHub and parsed event dict
    api_wrapper_mod.run_ingester.assert_called_once()
    _, kwargs = api_wrapper_mod.run_ingester.call_args
    assert kwargs["table"] == "accounts"
    assert kwargs["env"] == "dev"
    assert kwargs["event"] == event_dict
    assert kwargs["config"] == {
        "yaml": "content",
        "file_path": "configs/my.yml",
    }
    assert kwargs["run_mode"] == "once"
    assert kwargs["start"] is None
    assert kwargs["end"] is None

    # extra env applied
    assert "XTRA" in sys.modules["os"].environ
    assert sys.modules["os"].environ["XTRA"] == "1"

    # prints JSON status
    out = capsys.readouterr().out.strip()
    payload = json.loads(out)
    assert payload["status"] == "ok"
    assert payload["meta"] == {"rows": 7}

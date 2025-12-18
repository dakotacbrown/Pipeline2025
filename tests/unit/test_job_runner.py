import importlib
import json
import sys
import types
from pathlib import Path
from unittest.mock import MagicMock

import pytest

RUN_STEP_MODULE = "src.run_step"


def _stub_asvcl_modules(monkeypatch):
    """
    run_step.py imports these at import-time:

      from asvclscoreddataservices_common.github.common import GithubConnection
      from asvclscoreddataservices_common.logger.basic_logger import setup_logger
    """
    pkg = types.ModuleType("asvclscoreddataservices_common")
    github_pkg = types.ModuleType("asvclscoreddataservices_common.github")
    github_common = types.ModuleType(
        "asvclscoreddataservices_common.github.common"
    )
    logger_pkg = types.ModuleType("asvclscoreddataservices_common.logger")
    basic_logger = types.ModuleType(
        "asvclscoreddataservices_common.logger.basic_logger"
    )

    # default stubs (individual tests can override run_step.GithubConnection/run_step.log)
    class _GithubConnection:
        def __init__(self, *args, **kwargs):
            pass

        def get_github_file_contents(self, file_path: str):
            return ""

    def _setup_logger():
        # must support .info(...)
        return MagicMock()

    github_common.GithubConnection = _GithubConnection
    basic_logger.setup_logger = _setup_logger

    monkeypatch.setitem(sys.modules, "asvclscoreddataservices_common", pkg)
    monkeypatch.setitem(
        sys.modules, "asvclscoreddataservices_common.github", github_pkg
    )
    monkeypatch.setitem(
        sys.modules,
        "asvclscoreddataservices_common.github.common",
        github_common,
    )
    monkeypatch.setitem(
        sys.modules, "asvclscoreddataservices_common.logger", logger_pkg
    )
    monkeypatch.setitem(
        sys.modules,
        "asvclscoreddataservices_common.logger.basic_logger",
        basic_logger,
    )


def _import_fresh(module_name: str):
    sys.modules.pop(module_name, None)
    return importlib.import_module(module_name)


def test_setup_path_inserts_expected_sys_path_entries(tmp_path, monkeypatch):
    _stub_asvcl_modules(monkeypatch)

    # match the glob in your code: debi-etl-framework-glue*.zip
    zip_name = "debi-etl-framework-glue-jobs.zip"
    (tmp_path / zip_name).write_text("dummy")

    # run_step.py does Path.cwd().rglob(...), so chdir into tmp_path
    monkeypatch.chdir(tmp_path)

    # isolate sys.path effects
    original_sys_path = list(sys.path)
    try:
        mod = _import_fresh(RUN_STEP_MODULE)

        # setup_path inserts 4 entries at the front (insert(0, ...))
        zip_without = zip_name.split(".zip")[0]
        expected_front = [
            f"{zip_name}/src",
            f"{zip_name}/",
            f"{zip_name}/{zip_without}/src",
            f"{zip_name}/{zip_without}/",
        ]
        assert sys.path[:4] == expected_front
        assert hasattr(mod, "setup_path")
    finally:
        sys.path[:] = original_sys_path
        sys.modules.pop(RUN_STEP_MODULE, None)


def test_setup_path_multiple_zip_files_raises_value_error(
    tmp_path, monkeypatch
):
    _stub_asvcl_modules(monkeypatch)

    (tmp_path / "debi-etl-framework-glue-a.zip").write_text("a")
    (tmp_path / "debi-etl-framework-glue-b.zip").write_text("b")
    monkeypatch.chdir(tmp_path)

    # importing the module triggers setup_path() at import time
    sys.modules.pop(RUN_STEP_MODULE, None)
    with pytest.raises(
        ValueError, match=r"More than one debi-etl-framework-glue zip found"
    ):
        importlib.import_module(RUN_STEP_MODULE)

    sys.modules.pop(RUN_STEP_MODULE, None)


def test_parse_args_ignores_unknown_args(tmp_path, monkeypatch, capsys):
    _stub_asvcl_modules(monkeypatch)

    (tmp_path / "debi-etl-framework-glue-jobs.zip").write_text("dummy")
    monkeypatch.chdir(tmp_path)

    # fresh import
    mod = _import_fresh(RUN_STEP_MODULE)

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_step.py",
            "--env",
            "qa",
            "--vendor",
            "salesforce",
            "--table",
            "Account",
            "--event",
            '{"k":"v"}',
            "--file_path",
            "configs/salesforce.yml",
            "--repo_name",
            "config_management",
            "--github_token",
            "ghp_123",
            "--extra_env",
            "FOO=bar",
            "--unknown_flag",
            "wat",
        ],
        raising=False,
    )

    args = mod._parse_args()
    out = capsys.readouterr().out

    assert args.env == "qa"
    assert args.vendor == "salesforce"
    assert args.table == "Account"
    assert args.event == '{"k":"v"}'
    assert args.file_path == "configs/salesforce.yml"
    assert args.repo_name == "config_management"
    assert args.github_token == "ghp_123"
    assert args.extra_env == ["FOO=bar"]
    assert "Ignoring unknown args" in out


def test_main_calls_github_and_run_ingester(tmp_path, monkeypatch, capsys):
    _stub_asvcl_modules(monkeypatch)

    (tmp_path / "debi-etl-framework-glue-jobs.zip").write_text("dummy")
    monkeypatch.chdir(tmp_path)

    mod = _import_fresh(RUN_STEP_MODULE)

    # match your code's usage: GithubConnection(log, github_token, args.github_token, args.repo_name)
    monkeypatch.setattr(mod, "github_token", "GLOBAL_TOKEN", raising=False)

    github_conn_instance = MagicMock()
    github_conn_instance.get_github_file_contents.return_value = "yaml: true"

    GithubConnectionMock = MagicMock(return_value=github_conn_instance)
    monkeypatch.setattr(
        mod, "GithubConnection", GithubConnectionMock, raising=True
    )

    # main does: from src.api_wrapper import run_ingester
    api_wrapper_stub = types.ModuleType("src.api_wrapper")
    run_ingester_mock = MagicMock(return_value={"result": "ok"})
    api_wrapper_stub.run_ingester = run_ingester_mock
    monkeypatch.setitem(sys.modules, "src.api_wrapper", api_wrapper_stub)

    # use a controllable logger
    mod.log = MagicMock()

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_step.py",
            "--env",
            "qa",
            "--run_mode",
            "once",
            "--vendor",
            "salesforce",
            "--table",
            "Account",
            "--event",
            '{"hello":"world"}',
            "--file_path",
            "configs/salesforce.yml",
            "--repo_name",
            "config_management",
            "--github_token",
            "ghp_123",
            "--extra_env",
            "FOO=bar",
        ],
        raising=False,
    )

    mod.main()
    printed = capsys.readouterr().out.strip()
    payload = json.loads(printed)

    assert payload["status"] == "ok"
    assert payload["meta"] == {"result": "ok"}

    # extra env exported
    assert "FOO" in __import__("os").environ
    assert __import__("os").environ["FOO"] == "bar"

    GithubConnectionMock.assert_called_once()
    github_conn_instance.get_github_file_contents.assert_called_once_with(
        "configs/salesforce.yml"
    )

    run_ingester_mock.assert_called_once_with(
        table="Account",
        env="qa",
        event='{"hello":"world"}',
        config="yaml: true",
        run_mode="once",
        start=None,
        end=None,
    )

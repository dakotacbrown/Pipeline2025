import json
import os
import sys
import types

import pytest

from src import run_step


# ------------------------
# setup_path tests
# ------------------------
def test_setup_path_zero_zips_no_change(tmp_path, monkeypatch):
    original_cwd = os.getcwd()
    original_sys_path = list(sys.path)

    try:
        monkeypatch.chdir(tmp_path)
        sys.path = list(original_sys_path)

        run_step.setup_path()

        assert sys.path == original_sys_path
    finally:
        os.chdir(original_cwd)
        sys.path = original_sys_path


def test_setup_path_single_zip_adds_paths(tmp_path, monkeypatch):
    original_cwd = os.getcwd()
    original_sys_path = list(sys.path)

    try:
        monkeypatch.chdir(tmp_path)
        zip_file = tmp_path / "ingester_bundle_test.zip"
        zip_file.write_text("dummy")

        sys.path = list(original_sys_path)

        run_step.setup_path()

        zip_name = zip_file.name
        zip_stem = zip_file.stem
        expected_1 = f"{zip_name}/{zip_stem}/"
        expected_2 = f"{zip_name}/"

        assert sys.path[0] == expected_2
        assert sys.path[1] == expected_1
    finally:
        os.chdir(original_cwd)
        sys.path = original_sys_path


def test_setup_path_multiple_zips_raises(tmp_path, monkeypatch):
    original_cwd = os.getcwd()
    original_sys_path = list(sys.path)

    try:
        monkeypatch.chdir(tmp_path)
        (tmp_path / "ingester_bundle_a.zip").write_text("a")
        (tmp_path / "ingester_bundle_b.zip").write_text("b")

        sys.path = list(original_sys_path)

        with pytest.raises(ValueError):
            run_step.setup_path()

        assert sys.path == original_sys_path
    finally:
        os.chdir(original_cwd)
        sys.path = original_sys_path


# ------------------------
# _parse_args tests
# ------------------------
def test_parse_args_minimal(monkeypatch):
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "prog",
            "-y",
            "config/ingester.yml",
            "--table",
            "accounts",
            "--env",
            "dev",
        ],
    )

    args = run_step._parse_args()

    assert args.yaml_path == "config/ingester.yml"
    assert args.table == "accounts"
    assert args.env_name == "dev"
    assert args.run_mode == "once"
    assert args.start_date is None
    assert args.end_date is None
    assert args.log_level == "INFO"
    assert args.extra_env == []


def test_parse_args_with_extra_env_and_unknown(monkeypatch, capsys):
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "prog",
            "-y",
            "config.yml",
            "--table",
            "t1",
            "--env",
            "prod",
            "--run_mode",
            "backfill",
            "--start_date",
            "2024-01-01",
            "--end_date",
            "2024-01-31",
            "--log_level",
            "DEBUG",
            "--extra_env",
            "FOO=bar",
            "--extra_env",
            "BAZ=qux",
            "--job-language",
            "python",
        ],
    )

    args = run_step._parse_args()
    out = capsys.readouterr().out

    assert args.run_mode == "backfill"
    assert args.start_date == "2024-01-01"
    assert args.end_date == "2024-01-31"
    assert args.log_level == "DEBUG"
    assert args.extra_env == ["FOO=bar", "BAZ=qux"]
    assert "Ignoring unknown args" in out


# ------------------------
# main() tests
# ------------------------
def test_main_calls_run_ingester_and_prints_json(monkeypatch, capsys):
    from types import SimpleNamespace

    fake_args = SimpleNamespace(
        yaml_path="config/ingester.yml",
        table="accounts",
        env_name="dev",
        run_mode="once",
        start_date="2024-01-01",
        end_date="2024-01-31",
        log_level="INFO",
        extra_env=["FOO=bar", "NO_EQUALS"],
        event={"foo": "bar"},
    )
    monkeypatch.setattr(run_step, "_parse_args", lambda: fake_args)

    # Don't let tests reconfigure global logging
    monkeypatch.setattr(run_step.logging, "basicConfig", lambda *a, **k: None)

    # Return a dummy logger whenever getLogger is called inside main()
    class DummyLogger:
        def debug(self, *a, **k): ...
        def info(self, *a, **k): ...
        def warning(self, *a, **k): ...
        def error(self, *a, **k): ...
        def exception(self, *a, **k): ...

    monkeypatch.setattr(
        run_step.logging,
        "getLogger",
        lambda name=None: DummyLogger(),
    )

    # Fake src.api_wrapper.run_ingester
    called = {}

    def fake_run_ingester(**kwargs):
        called["kwargs"] = kwargs
        return {"rows": 42}

    dummy_module = types.SimpleNamespace(run_ingester=fake_run_ingester)
    monkeypatch.setitem(sys.modules, "src.api_wrapper", dummy_module)

    old_environ = os.environ.copy()
    try:
        run_step.main()
        assert os.environ["FOO"] == "bar"
        assert "NO_EQUALS" not in os.environ
    finally:
        os.environ.clear()
        os.environ.update(old_environ)

    assert called["kwargs"] == {
        "table": "accounts",
        "env_name": "dev",
        "yaml_path": "config/ingester.yml",
        "event": {"foo": "bar"},
        "run_mode": "once",
        "start": "2024-01-01",
        "end": "2024-01-31",
    }

    out = capsys.readouterr().out.strip()
    assert json.loads(out) == {"status": "ok", "meta": {"rows": 42}}

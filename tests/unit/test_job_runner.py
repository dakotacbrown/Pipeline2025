# tests/unit/test_setup_path.py
from pathlib import Path
import pytest

from src.run_step import setup_path


def test_setup_path_inserts_expected_entries_when_one_zip_found(monkeypatch):
    # Pretend the zip exists on disk (found via rglob)
    fake_zip = Path("/tmp/debi-etl-framework-glue-1.2.3.zip")

    def fake_rglob(self, pattern):
        if str(self) == "/tmp":
            return [fake_zip]
        return []

    monkeypatch.setattr(Path, "rglob", fake_rglob)

    sp = ["existing"]

    setup_path(
        search_dirs=[Path("/tmp"), Path("/does-not-matter")], sys_path=sp
    )

    # Must include the zip itself (most important)
    assert "/tmp/debi-etl-framework-glue-1.2.3.zip" in sp

    # Also includes src variants (depending on layout)
    assert "/tmp/debi-etl-framework-glue-1.2.3.zip/src" in sp
    assert "debi-etl-framework-glue-1.2.3.zip/src" in sp
    assert "debi-etl-framework-glue-1.2.3.zip/" in sp

    # Existing entry should still be present
    assert "existing" in sp


def test_setup_path_raises_when_multiple_zips_found(monkeypatch):
    fake_zip1 = Path("/tmp/debi-etl-framework-glue-1.0.0.zip")
    fake_zip2 = Path("/tmp/debi-etl-framework-glue-2.0.0.zip")

    def fake_rglob(self, pattern):
        return [fake_zip1, fake_zip2]

    monkeypatch.setattr(Path, "rglob", fake_rglob)

    sp = []

    with pytest.raises(ValueError, match="More than one"):
        setup_path(search_dirs=[Path("/tmp")], sys_path=sp)


def test_setup_path_noop_when_no_zip_found(monkeypatch):
    def fake_rglob(self, pattern):
        return []

    monkeypatch.setattr(Path, "rglob", fake_rglob)

    sp = ["keepme"]
    setup_path(search_dirs=[Path("/tmp")], sys_path=sp)

    assert sp == ["keepme"]


def test_setup_path_uses_zip_already_on_syspath_and_does_not_search(
    monkeypatch,
):
    # If Glue already added the zip (common), setup_path should use that and not rglob.
    def explode_rglob(self, pattern):
        raise AssertionError(
            "rglob should not be called when zip already on sys.path"
        )

    monkeypatch.setattr(Path, "rglob", explode_rglob)

    sp = ["/somewhere/debi-etl-framework-glue-9.9.9.zip"]

    setup_path(search_dirs=[Path("/tmp")], sys_path=sp)

    # Should keep zip and add related candidates
    assert "/somewhere/debi-etl-framework-glue-9.9.9.zip" in sp
    assert "/somewhere/debi-etl-framework-glue-9.9.9.zip/src" in sp

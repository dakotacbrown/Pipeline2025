import pandas as pd
import pytest

from api_ingestor.api_ingestor import ApiIngestor

# ----------------------------
# Common fixtures
# ----------------------------


class Log:
    def __init__(self):
        self.msgs = []

    def info(self, *a, **k):
        self.msgs.append(("info", a, k))

    def error(self, *a, **k):
        self.msgs.append(("error", a, k))


@pytest.fixture
def logger():
    return Log()


@pytest.fixture
def base_config():
    return {"envs": {"prod": {"base_url": "https://api/"}}, "apis": {}}


# ----------------------------
# run_once: single pull path
# ----------------------------


def test_run_once_single_pull_writes_and_resolves_session_id(
    monkeypatch, logger, base_config
):
    # table config: no multi_pulls => single paginate path
    base_config["apis"]["tbl"] = {
        "path": "items",
        "parse": {"type": "json"},
        "pagination": {"mode": "none"},
        "link_expansion": {
            "enabled": True,
            # simulate captured session ids in expand_links
            "session_id": {"policy": "first"},
        },
        "output": {"format": "jsonl", "s3": {"bucket": "b", "prefix": "p"}},
    }
    ing = ApiIngestor(base_config, logger)

    # capture paginate -> returns a df
    df_single = pd.DataFrame([{"a": 1}])
    monkeypatch.setattr(
        "api_ingestor.api_ingestor.paginate", lambda *a, **k: df_single
    )

    # expand_links adds nothing but marks session id via ctx side-effect
    def fake_expand(ctx, *a, **k):
        ctx["expansion_session_ids"].update({"SID_123"})
        return df_single

    monkeypatch.setattr("api_ingestor.api_ingestor.expand_links", fake_expand)

    def fake_write(ctx, df, out_cfg):
        assert df.equals(df_single)
        # session id should be resolved and pushed into ctx["current_output_ctx"]
        assert ctx["current_output_ctx"].get("session_id") == "SID_123"
        # return normal meta
        return {
            "format": "jsonl",
            "s3_bucket": "b",
            "s3_key": "k",
            "s3_uri": "s3://b/k",
            "bytes": 10,
        }

    monkeypatch.setattr(
        "api_ingestor.api_ingestor._write_output",
        lambda ctx, df, out: fake_write(ctx, df, out),
    )

    # no-op session builder/defaults (avoid real HTTP)
    monkeypatch.setattr(
        "api_ingestor.api_ingestor.build_session", lambda *a, **k: object()
    )
    monkeypatch.setattr(
        "api_ingestor.api_ingestor.apply_session_defaults", lambda *a, **k: None
    )

    out = ing.run_once("tbl", "prod")
    assert out["rows"] == 1
    assert out["format"] == "jsonl"
    assert out["pagination_mode"] == "none"
    # session id should be cleared post-write
    # (ensures we don't leak context into next run)
    # using internal ctx via closure not exposed; rely on no exception + assertions above


# ----------------------------
# run_once: multi_pulls + join path
# ----------------------------


def test_run_once_multi_pulls_join(monkeypatch, logger, base_config):
    base_config["apis"]["tbl"] = {
        "multi_pulls": [
            {"name": "A", "path": "a", "parse": {"type": "json"}},
            {"name": "B", "path": "b", "parse": {"type": "json"}},
        ],
        "join": {
            "how": "left",
            "on": ["id"],
            "case": "ignore",
            "select_from": {"B": {"keep": ["v"]}},
        },
        "output": {"format": "jsonl", "s3": {"bucket": "b", "prefix": "p"}},
    }
    ing = ApiIngestor(base_config, logger)

    df_joined = pd.DataFrame([{"id": "1", "v": 9}])
    monkeypatch.setattr(
        "api_ingestor.api_ingestor.run_multi_pulls_with_join",
        lambda *a, **k: df_joined,
    )
    monkeypatch.setattr(
        "api_ingestor.api_ingestor.expand_links", lambda ctx, *a, **k: df_joined
    )
    monkeypatch.setattr(
        "api_ingestor.api_ingestor.build_session", lambda *a, **k: object()
    )
    monkeypatch.setattr(
        "api_ingestor.api_ingestor.apply_session_defaults", lambda *a, **k: None
    )

    monkeypatch.setattr(
        "api_ingestor.api_ingestor._write_output",
        lambda ctx, df, out: {
            "format": "jsonl",
            "s3_bucket": "b",
            "s3_key": "k",
            "s3_uri": "s3://b/k",
            "bytes": 1,
        },
    )

    out = ing.run_once("tbl", "prod")
    assert out["rows"] == 1
    assert out["format"] == "jsonl"


# ----------------------------
# run_once: flush-only (no aggregate write)
# ----------------------------


def test_run_once_flush_only_skips_write(monkeypatch, logger, base_config):
    base_config["apis"]["tbl"] = {
        "path": "items",
        "parse": {"type": "json"},
        "pagination": {"mode": "none"},
        "link_expansion": {"enabled": True, "flush": {"only": True}},
        "output": {"format": "jsonl", "s3": {"bucket": "b", "prefix": "p"}},
    }
    ing = ApiIngestor(base_config, logger)

    df = pd.DataFrame([{"x": 1}])
    monkeypatch.setattr(
        "api_ingestor.api_ingestor.paginate", lambda *a, **k: df
    )
    monkeypatch.setattr(
        "api_ingestor.api_ingestor.expand_links", lambda ctx, *a, **k: df
    )
    monkeypatch.setattr(
        "api_ingestor.api_ingestor.build_session", lambda *a, **k: object()
    )
    monkeypatch.setattr(
        "api_ingestor.api_ingestor.apply_session_defaults", lambda *a, **k: None
    )

    # If this is called, test should fail
    monkeypatch.setattr(
        "api_ingestor.api_ingestor._write_output",
        lambda *a, **k: (_ for _ in ()).throw(
            AssertionError("write_output should not be called")
        ),
    )

    out = ing.run_once("tbl", "prod")
    assert out["rows"] == 0
    assert out["s3_key"] == "" and out["s3_uri"] == ""


# ----------------------------
# run_backfill: cursor strategy
# ----------------------------


def test_run_backfill_cursor_strategy(monkeypatch, logger, base_config):
    base_config["apis"]["tbl"] = {
        "path": "items",
        "parse": {"type": "json"},
        "pagination": {
            "mode": "cursor",
            "cursor_param": "cursor",
            "next_cursor_path": "meta.next",
        },
        "backfill": {
            "enabled": True,
            "strategy": "cursor",
            "cursor": {"start_value": "s0"},
        },
        "link_expansion": {"enabled": False},
        "output": {"format": "jsonl", "s3": {"bucket": "b", "prefix": "p"}},
    }
    ing = ApiIngestor(base_config, logger)

    df = pd.DataFrame([{"i": 1}, {"i": 2}])
    monkeypatch.setattr(
        "api_ingestor.api_ingestor.cursor_backfill", lambda *a, **k: df
    )
    monkeypatch.setattr(
        "api_ingestor.api_ingestor.build_session", lambda *a, **k: object()
    )
    monkeypatch.setattr(
        "api_ingestor.api_ingestor.apply_session_defaults", lambda *a, **k: None
    )
    monkeypatch.setattr(
        "api_ingestor.api_ingestor._write_output",
        lambda ctx, df, out: {
            "format": "jsonl",
            "s3_bucket": "b",
            "s3_key": "k",
            "s3_uri": "s3://b/k",
            "bytes": 2,
        },
    )

    out = ing.run_backfill(
        "tbl",
        "prod",
        start=pd.Timestamp("2024-01-01").date(),
        end=pd.Timestamp("2024-01-02").date(),
    )
    assert out["strategy"] == "cursor"
    assert out["rows"] == 2


# ----------------------------
# run_backfill: soql_window strategy
# ----------------------------


def test_run_backfill_soql_window(monkeypatch, logger, base_config):
    base_config["apis"]["tbl"] = {
        "path": "services/data/v61.0/query",
        "parse": {"type": "json"},
        "pagination": {
            "mode": "salesforce",
            "done_path": "done",
            "next_url_path": "nextRecordsUrl",
        },
        "backfill": {
            "enabled": True,
            "strategy": "soql_window",
            "window_days": 1,
            "date_field": "LastModifiedDate",
            "date_format": "%Y-%m-%dT%H:%M:%SZ",
            "soql_template": "SELECT Id FROM Obj WHERE {date_field} >= {start} AND {date_field} < {end}",
        },
        "output": {"format": "jsonl", "s3": {"bucket": "b", "prefix": "p"}},
    }
    ing = ApiIngestor(base_config, logger)

    df = pd.DataFrame([{"Id": "1"}, {"Id": "2"}])
    monkeypatch.setattr(
        "api_ingestor.api_ingestor.soql_window_backfill", lambda *a, **k: df
    )
    monkeypatch.setattr(
        "api_ingestor.api_ingestor.build_session", lambda *a, **k: object()
    )
    monkeypatch.setattr(
        "api_ingestor.api_ingestor.apply_session_defaults", lambda *a, **k: None
    )
    monkeypatch.setattr(
        "api_ingestor.api_ingestor._write_output",
        lambda ctx, df, out: {
            "format": "jsonl",
            "s3_bucket": "b",
            "s3_key": "k",
            "s3_uri": "s3://b/k",
            "bytes": 2,
        },
    )

    out = ing.run_backfill(
        "tbl",
        "prod",
        start=pd.Timestamp("2024-02-01").date(),
        end=pd.Timestamp("2024-02-02").date(),
    )
    assert out["strategy"] == "soql_window"
    assert out["rows"] == 2


def test_run_backfill_soql_window_wrong_mode_raises(
    monkeypatch, logger, base_config
):
    base_config["apis"]["tbl"] = {
        "path": "services/data/v61.0/query",
        "parse": {"type": "json"},
        "pagination": {"mode": "none"},
        "backfill": {
            "enabled": True,
            "strategy": "soql_window",
            "soql_template": "SELECT Id FROM Obj WHERE {date_field} >= {start} AND {date_field} < {end}",
        },
        "output": {"format": "jsonl", "s3": {"bucket": "b", "prefix": "p"}},
    }
    ing = ApiIngestor(base_config, logger)
    monkeypatch.setattr(
        "api_ingestor.api_ingestor.build_session", lambda *a, **k: object()
    )
    monkeypatch.setattr(
        "api_ingestor.api_ingestor.apply_session_defaults", lambda *a, **k: None
    )

    with pytest.raises(ValueError):
        ing.run_backfill(
            "tbl",
            "prod",
            start=pd.Timestamp("2024-02-01").date(),
            end=pd.Timestamp("2024-02-02").date(),
        )


# ----------------------------
# run_backfill: date-window (with and without multi_pulls)
# ----------------------------


def test_run_backfill_date_window_single_pull(monkeypatch, logger, base_config):
    base_config["apis"]["tbl"] = {
        "path": "items",
        "parse": {"type": "json"},
        "pagination": {"mode": "none"},
        # two 1-day windows across 2 days
        "backfill": {"enabled": True, "strategy": "date", "window_days": 1},
        "output": {"format": "jsonl", "s3": {"bucket": "b", "prefix": "p"}},
    }
    ing = ApiIngestor(base_config, logger)

    # two windows -> return 1 row each -> total 2
    pages = [pd.DataFrame([{"x": 1}]), pd.DataFrame([{"x": 2}])]

    def fake_paginate(sess, url, safe, parse_cfg, pag_cfg):
        return pages.pop(0)

    monkeypatch.setattr("api_ingestor.api_ingestor.paginate", fake_paginate)
    monkeypatch.setattr(
        "api_ingestor.api_ingestor.expand_links", lambda ctx, *a, **k: a[1]
    )
    monkeypatch.setattr(
        "api_ingestor.api_ingestor.build_session", lambda *a, **k: object()
    )
    monkeypatch.setattr(
        "api_ingestor.api_ingestor.apply_session_defaults", lambda *a, **k: None
    )
    monkeypatch.setattr(
        "api_ingestor.api_ingestor._write_output",
        lambda ctx, df, out: {
            "format": "jsonl",
            "s3_bucket": "b",
            "s3_key": "k",
            "s3_uri": "s3://b/k",
            "bytes": len(df),
        },
    )

    out = ing.run_backfill(
        "tbl",
        "prod",
        start=pd.Timestamp("2024-01-01").date(),
        end=pd.Timestamp("2024-01-02").date(),
    )
    assert out["strategy"] == "date"
    assert out["rows"] == 2
    assert out["windows"] == 2


def test_run_backfill_date_window_multi_pulls(monkeypatch, logger, base_config):
    base_config["apis"]["tbl"] = {
        "multi_pulls": [
            {"name": "A", "path": "a", "parse": {"type": "json"}},
            {"name": "B", "path": "b", "parse": {"type": "json"}},
        ],
        "join": {"how": "left", "on": ["id"], "case": "ignore"},
        "backfill": {"enabled": True, "strategy": "date", "window_days": 1},
        "output": {"format": "jsonl", "s3": {"bucket": "b", "prefix": "p"}},
    }
    ing = ApiIngestor(base_config, logger)

    # one window -> multi_pulls join returns 3 rows
    monkeypatch.setattr(
        "api_ingestor.api_ingestor.run_multi_pulls_with_join",
        lambda *a, **k: pd.DataFrame([{"id": 1}, {"id": 2}, {"id": 3}]),
    )
    monkeypatch.setattr(
        "api_ingestor.api_ingestor.expand_links", lambda ctx, *a, **k: a[1]
    )
    monkeypatch.setattr(
        "api_ingestor.api_ingestor.build_session", lambda *a, **k: object()
    )
    monkeypatch.setattr(
        "api_ingestor.api_ingestor.apply_session_defaults", lambda *a, **k: None
    )
    monkeypatch.setattr(
        "api_ingestor.api_ingestor._write_output",
        lambda ctx, df, out: {
            "format": "jsonl",
            "s3_bucket": "b",
            "s3_key": "k",
            "s3_uri": "s3://b/k",
            "bytes": 3,
        },
    )

    out = ing.run_backfill(
        "tbl",
        "prod",
        start=pd.Timestamp("2024-01-01").date(),
        end=pd.Timestamp("2024-01-01").date(),
    )
    assert out["strategy"] == "date"
    assert out["rows"] == 3
    assert out["windows"] == 1


# ----------------------------
# Errors + disabled backfill
# ----------------------------


def test_run_backfill_disabled_raises(monkeypatch, logger, base_config):
    base_config["apis"]["tbl"] = {
        "path": "items",
        "parse": {"type": "json"},
        "output": {"format": "jsonl", "s3": {"bucket": "b", "prefix": "p"}},
        "backfill": {"enabled": False},
    }
    ing = ApiIngestor(base_config, logger)
    with pytest.raises(ValueError):
        ing.run_backfill(
            "tbl",
            "prod",
            start=pd.Timestamp("2024-01-01").date(),
            end=pd.Timestamp("2024-01-02").date(),
        )


def test_run_once_logs_exception_and_bubbles(monkeypatch, logger, base_config):
    base_config["apis"]["tbl"] = {
        "path": "items",
        "parse": {"type": "json"},
        "pagination": {"mode": "none"},
        "output": {"format": "jsonl", "s3": {"bucket": "b", "prefix": "p"}},
    }
    ing = ApiIngestor(base_config, logger)

    monkeypatch.setattr(
        "api_ingestor.api_ingestor.paginate",
        lambda *a, **k: (_ for _ in ()).throw(RuntimeError("boom")),
    )
    monkeypatch.setattr(
        "api_ingestor.api_ingestor.build_session", lambda *a, **k: object()
    )
    monkeypatch.setattr(
        "api_ingestor.api_ingestor.apply_session_defaults", lambda *a, **k: None
    )

    with pytest.raises(RuntimeError):
        ing.run_once("tbl", "prod")
    # Ensure error got logged
    assert any(level == "error" for (level, *_rest) in logger.msgs)

# tests/unit/test_backfill_extended.py
import types
from datetime import date, timedelta

import pandas as pd
import pytest

from api_ingestor import backfill

# ---------- Common fakes ----------


class FakeResponse:
    def __init__(self, json_data=None, status_code=200, links=None):
        self._json = json_data or {}
        self.status_code = status_code
        self.links = links or {}

    def json(self):
        return self._json

    def raise_for_status(self):
        if not (200 <= self.status_code < 300):
            raise RuntimeError(f"HTTP {self.status_code}")


class FakeSession:
    """
    Map: url -> list[FakeResponse]; each GET pops the next response.
    Also records call kwargs so we can assert the cursor param chaining.
    """

    def __init__(self, mapping):
        self._m = {k: list(v) for k, v in mapping.items()}
        self.calls = []  # list of {"url": url, "kw": kw}

    def get(self, url, **kw):
        self.calls.append(
            {
                "url": url,
                "kw": {
                    k: (v.copy() if isinstance(v, dict) else v)
                    for k, v in kw.items()
                },
            }
        )
        arr = self._m[url]
        if not arr:
            raise KeyError(url)
        return arr.pop(0)


def _ctx():
    # minimal ctx with a no-op logger
    return {
        "log": type("L", (object,), {"info": lambda *a, **k: None})(),
        "current_output_ctx": {},
        "expansion_session_ids": set(),
    }


# ---------- cursor_backfill coverage ----------


def test_cursor_backfill_next_token_and_link_expansion(monkeypatch):
    # two pages via next_cursor_path, link_expansion enabled
    sess = FakeSession(
        {
            "https://api/cur": [
                FakeResponse(
                    json_data={"data": [{"i": 1}], "meta": {"next": "s1"}}
                ),
                FakeResponse(
                    json_data={"data": [{"i": 2}], "meta": {"next": None}}
                ),
            ]
        }
    )

    # to_dataframe -> read "data"
    monkeypatch.setattr(
        backfill,
        "to_dataframe",
        lambda resp, parse: pd.DataFrame(resp.json()["data"]),
    )

    # capture expand_links calls and return df unchanged
    called = {"n": 0}

    def _expand(ctx, s, df, link_cfg, parse, **kw):
        called["n"] += 1
        return df

    monkeypatch.setattr(backfill, "expand_links", _expand)

    df = backfill.cursor_backfill(
        ctx=_ctx(),
        sess=sess,
        url="https://api/cur",
        base_opts={"params": {}},
        parse_cfg={"type": "json", "json_record_path": "data"},
        pag_cfg={
            "mode": "cursor",
            "cursor_param": "cursor",
            "next_cursor_path": "meta.next",
        },
        cur_cfg={"start_value": "s0"},
        link_cfg={"enabled": True},
    )

    # concat both pages
    assert list(df["i"]) == [1, 2]
    # link expansion ran for each non-empty page
    assert called["n"] == 2
    # verify that second call included cursor param "s1"
    assert sess.calls[1]["kw"]["params"]["cursor"] == "s1"


def test_cursor_backfill_stop_item_inclusive_false(monkeypatch):
    # stop when id == "stop" (exclusive)
    sess = FakeSession(
        {
            "https://api/cur": [
                FakeResponse(
                    json_data={
                        "data": [{"id": "a"}, {"id": "stop"}, {"id": "b"}],
                        "meta": {"next": "s1"},
                    }
                ),
            ]
        }
    )
    monkeypatch.setattr(
        backfill,
        "to_dataframe",
        lambda resp, parse: pd.DataFrame(resp.json()["data"]),
    )
    # expand_links should be invoked on trimmed page as well
    called = {"n": 0}
    monkeypatch.setattr(
        backfill, "expand_links", lambda *a, **k: a[2]
    )  # pass-through DF

    df = backfill.cursor_backfill(
        ctx=_ctx(),
        sess=sess,
        url="https://api/cur",
        base_opts={"params": {}},
        parse_cfg={"type": "json"},
        pag_cfg={
            "mode": "cursor",
            "cursor_param": "cursor",
            "next_cursor_path": "meta.next",
        },
        cur_cfg={
            "stop_at_item": {"field": "id", "value": "stop", "inclusive": False}
        },
        link_cfg={"enabled": True},
    )
    assert list(df["id"]) == ["a"]  # 'stop' row excluded and loop ends


def test_cursor_backfill_stop_item_inclusive_true(monkeypatch):
    sess = FakeSession(
        {
            "https://api/cur": [
                FakeResponse(
                    json_data={
                        "data": [{"id": "a"}, {"id": "stop"}, {"id": "b"}],
                        "meta": {"next": "s1"},
                    }
                ),
            ]
        }
    )
    monkeypatch.setattr(
        backfill,
        "to_dataframe",
        lambda resp, parse: pd.DataFrame(resp.json()["data"]),
    )
    df = backfill.cursor_backfill(
        ctx=_ctx(),
        sess=sess,
        url="https://api/cur",
        base_opts={"params": {}},
        parse_cfg={"type": "json"},
        pag_cfg={
            "mode": "cursor",
            "cursor_param": "cursor",
            "next_cursor_path": "meta.next",
        },
        cur_cfg={
            "stop_at_item": {"field": "id", "value": "stop", "inclusive": True}
        },
        link_cfg={},
    )
    assert list(df["id"]) == ["a", "stop"]


def test_cursor_backfill_stop_when_older_than_with_link_expansion(monkeypatch):
    # mix of ts values; threshold filters out older
    sess = FakeSession(
        {
            "https://api/cur": [
                FakeResponse(
                    json_data={
                        "data": [
                            {"ts": "2024-01-02T00:00:00Z"},
                            {"ts": "2023-12-31T23:59:59Z"},
                        ],
                        "meta": {"next": "s1"},
                    }
                ),
            ]
        }
    )
    monkeypatch.setattr(
        backfill,
        "to_dataframe",
        lambda resp, parse: pd.DataFrame(resp.json()["data"]),
    )
    x = {"called": 0}

    def _expand(ctx, s, df, link_cfg, parse, **kw):
        x["called"] += 1
        return df

    monkeypatch.setattr(backfill, "expand_links", _expand)

    df = backfill.cursor_backfill(
        ctx=_ctx(),
        sess=sess,
        url="https://api/cur",
        base_opts={"params": {}},
        parse_cfg={"type": "json"},
        pag_cfg={
            "mode": "cursor",
            "cursor_param": "cursor",
            "next_cursor_path": "meta.next",
        },
        cur_cfg={
            "stop_when_older_than": {
                "field": "ts",
                "value": "2024-01-01T00:00:00Z",
            }
        },
        link_cfg={"enabled": True},
    )
    # Should keep only >= threshold, invoke expansion once, and stop loop
    assert list(df["ts"]) == ["2024-01-02T00:00:00Z"]
    assert x["called"] == 1


def test_cursor_backfill_chain_field(monkeypatch):
    # No next_cursor_path; use chain_field with last id from page
    sess = FakeSession(
        {
            "https://api/cur": [
                FakeResponse(json_data={"items": [{"id": 1}, {"id": 2}]}),
                FakeResponse(json_data={"items": []}),  # empty -> loop ends
            ]
        }
    )
    monkeypatch.setattr(
        backfill,
        "to_dataframe",
        lambda resp, parse: pd.DataFrame(resp.json()["items"]),
    )

    df = backfill.cursor_backfill(
        ctx=_ctx(),
        sess=sess,
        url="https://api/cur",
        base_opts={"params": {}},
        parse_cfg={"type": "json", "json_record_path": "items"},
        pag_cfg={
            "mode": "cursor",
            "cursor_param": "cursor",
            "chain_field": "id",
        },
        cur_cfg={},
        link_cfg={},
    )

    # First page appended, second empty -> stops
    assert list(df["id"]) == [1, 2]
    # Verify chained param equals last id ("2")
    assert sess.calls[1]["kw"]["params"]["cursor"] == 2


def test_cursor_backfill_max_pages(monkeypatch):
    # Ensure we respect max_pages=1 (should only take first)
    sess = FakeSession(
        {
            "https://api/cur": [
                FakeResponse(
                    json_data={"data": [{"x": 1}], "meta": {"next": "s1"}}
                ),
                FakeResponse(
                    json_data={"data": [{"x": 2}], "meta": {"next": None}}
                ),
            ]
        }
    )
    monkeypatch.setattr(
        backfill,
        "to_dataframe",
        lambda resp, parse: pd.DataFrame(resp.json()["data"]),
    )
    df = backfill.cursor_backfill(
        ctx=_ctx(),
        sess=sess,
        url="https://api/cur",
        base_opts={"params": {}},
        parse_cfg={"type": "json"},
        pag_cfg={
            "mode": "cursor",
            "cursor_param": "cursor",
            "next_cursor_path": "meta.next",
            "max_pages": 1,
        },
        cur_cfg={},
        link_cfg={},
    )
    assert list(df["x"]) == [1]


# ---------- soql_window_backfill coverage ----------


def test_soql_window_backfill_happy_path_and_delay(monkeypatch):
    # Simulate paginate being called once per 1-day window, across 2 days => 2 calls
    calls = []

    def _paginate(sess, url, safe, parse_cfg, pag_cfg):
        calls.append({"url": url, "params": safe.get("params", {}).copy()})
        # Return a single-row frame per window
        return pd.DataFrame([{"Id": len(calls)}])

    monkeypatch.setattr(backfill, "paginate", _paginate)

    slept = {"n": 0}
    monkeypatch.setattr(
        backfill.time, "sleep", lambda s: slept.__setitem__("n", slept["n"] + 1)
    )

    sess = FakeSession({"https://sf/query": [FakeResponse()] * 2})

    df = backfill.soql_window_backfill(
        ctx=_ctx(),
        sess=sess,
        url="https://sf/query",
        base_opts={"params": {}},
        parse_cfg={"type": "json", "json_record_path": "records"},
        pag_cfg={
            "mode": "salesforce",
            "done_path": "done",
            "next_url_path": "nextRecordsUrl",
        },
        bf_cfg={
            "window_days": 1,
            "per_request_delay": 0.01,
            "date_field": "LastModifiedDate",
            "date_format": "%Y-%m-%dT%H:%M:%SZ",
            "soql_template": "SELECT Id FROM Obj WHERE {date_field} >= {start} AND {date_field} < {end}",
        },
        start=date(2024, 1, 1),
        end=date(2024, 1, 2),
        link_cfg={},  # link expansion off here
    )

    # Two windows -> two rows
    assert list(df["Id"]) == [1, 2]
    # paginate called twice, and we injected a small sleep between
    assert len(calls) == 2
    assert slept["n"] == 2

    # Assert SOQL was placed into safe params["q"]
    assert (
        "q" in calls[0]["params"]
        and "SELECT Id FROM Obj" in calls[0]["params"]["q"]
    )


def test_soql_window_backfill_with_link_expansion(monkeypatch):
    # paginate returns a frame; expand_links should be called and can modify it
    monkeypatch.setattr(
        backfill, "paginate", lambda *a, **k: pd.DataFrame([{"k": 1}])
    )

    def _expand(ctx, sess, df, link_cfg, parse, **kw):
        df = df.copy()
        df["z"] = 9
        return df

    monkeypatch.setattr(backfill, "expand_links", _expand)

    sess = FakeSession({"https://sf/query": [FakeResponse()]})
    df = backfill.soql_window_backfill(
        ctx=_ctx(),
        sess=sess,
        url="https://sf/query",
        base_opts={"params": {}},
        parse_cfg={"type": "json"},
        pag_cfg={"mode": "salesforce"},
        bf_cfg={
            "window_days": 1,
            "soql_template": "SELECT Id FROM Obj WHERE {date_field} >= {start} AND {date_field} < {end}",
        },
        start=date(2024, 1, 1),
        end=date(2024, 1, 1),
        link_cfg={"enabled": True},
    )
    assert list(df["z"]) == [9]


def test_soql_window_backfill_missing_template_raises():
    sess = FakeSession({"https://sf/query": [FakeResponse()]})
    with pytest.raises(ValueError):
        backfill.soql_window_backfill(
            ctx=_ctx(),
            sess=sess,
            url="https://sf/query",
            base_opts={"params": {}},
            parse_cfg={"type": "json"},
            pag_cfg={"mode": "salesforce"},
            bf_cfg={},  # no soql_template
            start=date(2024, 1, 1),
            end=date(2024, 1, 1),
            link_cfg={},
        )

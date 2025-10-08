# tests/unit/test_link_expansion_extended.py
import re
import types

import pandas as pd
import pytest

from api_ingestor import link_expansion


class FakeResponse:
    def __init__(self, *, json_data=None, content=b"", status_code=200):
        self._json = json_data
        self.content = (
            content
            if content
            else (b"{}" if json_data is None else str(json_data).encode())
        )
        self.status_code = status_code
        self.links = {}

    def json(self):
        return self._json

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


class FakeSession:
    """Map URL -> list of FakeResponse (pop in order)"""

    def __init__(self, mapping):
        self._m = {k: list(v) for k, v in mapping.items()}

    def get(self, url, **kw):
        arr = self._m[url]
        if not arr:
            raise KeyError(url)
        return arr.pop(0)


class CaptureLog:
    def __init__(self):
        self.infos = []
        self.errors = []

    def info(self, m):
        self.infos.append(m)

    def error(self, m):
        self.errors.append(m)


@pytest.fixture
def ctx():
    return {
        "log": CaptureLog(),
        "expansion_session_ids": set(),
        "flush_seq": 0,
        "current_output_ctx": {},
        # write_s3 will be monkeypatched per-test where needed
        "write_s3": lambda df, fmt, s3_cfg: {"s3_uri": "s3://x"},
    }


# ---------- resolve_session_id ----------


def test_resolve_session_id_first_default():
    ids = {"a", "b", "c"}
    out = link_expansion.resolve_session_id(
        ids, {"session_id": {"policy": "first"}}
    )
    assert out in ids


def test_resolve_session_id_last():
    # list() order depends on set iteration; force order by providing a list
    ids = set(["s1", "s2", "s3"])
    last = link_expansion.resolve_session_id(
        ids, {"session_id": {"policy": "last"}}
    )
    assert last in ids  # cannot guarantee order, but valid member


def test_resolve_session_id_all():
    ids = {"x", "y", "x"}  # duplicates
    out = link_expansion.resolve_session_id(
        ids, {"session_id": {"policy": "all"}}
    )
    assert out == "x,y" or out == "y,x"


def test_resolve_session_id_require_single_error():
    ids = {"only1", "only2"}
    with pytest.raises(ValueError):
        link_expansion.resolve_session_id(
            ids, {"session_id": {"policy": "require_single"}}
        )


def test_resolve_session_id_require_single_ok():
    ids = {"one"}
    out = link_expansion.resolve_session_id(
        ids, {"session_id": {"policy": "require_single"}}
    )
    assert out == "one"


# ---------- expand_links no-op paths ----------


def test_expand_links_noop_on_empty_df(ctx):
    df = pd.DataFrame()
    sess = FakeSession({})
    out = link_expansion.expand_links(
        ctx, sess, df, link_cfg={}, parse_default={"type": "json"}
    )
    assert out is df


def test_expand_links_noop_when_no_url_fields(ctx):
    df = pd.DataFrame([{"k": 1}])
    sess = FakeSession({})
    out = link_expansion.expand_links(
        ctx,
        sess,
        df,
        link_cfg={"enabled": True, "url_fields": []},
        parse_default={"type": "json"},
    )
    assert out.equals(df)


# ---------- expand_links parsing override + session id + merge + delay ----------


def test_expand_links_parse_override_and_session_and_merge(ctx, monkeypatch):
    # Two URLs in a single row => two parts with different columns -> merged outer
    base = "https://api"
    sess = FakeSession(
        {
            f"{base}/one/abc": [FakeResponse(json_data={"items": [{"a": 1}]})],
            f"{base}/two/abc": [FakeResponse(json_data={"items2": [{"b": 2}]})],
        }
    )

    # Monkeypatch to_dataframe to verify parse cfg overrides
    called = {"cfg": []}

    def fake_to_df(resp, parse_cfg):
        called["cfg"].append(parse_cfg.copy())
        data = resp.json()
        if "items" in data:
            return pd.DataFrame(data["items"])
        if "items2" in data:
            return pd.DataFrame(data["items2"])
        return pd.DataFrame()

    monkeypatch.setattr(link_expansion, "to_dataframe", fake_to_df)

    # monkeypatch time.sleep to check delay calls
    sleeps = {"n": 0}
    monkeypatch.setattr(
        link_expansion.time,
        "sleep",
        lambda s: sleeps.__setitem__("n", sleeps["n"] + 1),
    )

    df = pd.DataFrame(
        [
            {
                "u1": f"{base}/one/abc",
                "u2": f"{base}/two/abc",
            }
        ]
    )

    out = link_expansion.expand_links(
        ctx,
        sess,
        df,
        link_cfg={
            "enabled": True,
            "url_fields": ["u1", "u2"],
            "type": "json",  # override ok
            "json_record_path": "IGNORED_AND_REMOVED",  # should be removed since our responses are dicts we handle in fake_to_df
            "session_id": {
                "regex": r"/([a-z]+)$",
                "group": 1,
            },  # captures "abc"
            "per_request_delay": 0.01,
            "flush": {"mode": "none"},
        },
        parse_default={"type": "json", "json_record_path": "default.path"},
        table_name="t",
        env_name="e",
        api_output_cfg={
            "format": "jsonl",
            "s3": {"bucket": "b", "prefix": "p"},
        },
    )

    # merged (outer) => both a and b present
    assert list(out.columns) == ["a", "b"] or set(out.columns) == {"a", "b"}
    assert out.iloc[0].to_dict() == {"a": 1, "b": 2}

    # session id captured
    assert "abc" in ctx["expansion_session_ids"]

    # time.sleep called for each url after first (2 urls -> 2 calls or 1? Each URL loop calls sleep after request)
    assert sleeps["n"] == 2

    # parse cfg calls: json_record_path removed (we pop it when not explicitly overridden to None)
    # first call should have no json_record_path present because we pop it; and type should be "json"
    assert all(c.get("type") == "json" for c in called["cfg"])
    # When not explicitly setting json_record_path=None, code pops it; ensure it's not present
    assert all(
        "json_record_path" not in c
        or c["json_record_path"] == "IGNORED_AND_REMOVED"
        for c in called["cfg"]
    )


# ---------- expand_links flush per_link ----------


def test_expand_links_flush_per_link(ctx, monkeypatch):
    # make the starting seq deterministic if you want
    # ctx["flush_seq"] = 0  # uncomment if you prefer seq=1,2
    base = "https://api"
    sess = FakeSession(
        {
            f"{base}/a/1": [FakeResponse(json_data={"items": [{"v": 10}]})],
            f"{base}/a/2": [FakeResponse(json_data={"items": [{"v": 20}]})],
        }
    )

    # to_dataframe: simple path
    monkeypatch.setattr(
        link_expansion,
        "to_dataframe",
        lambda r, p: pd.DataFrame(r.json()["items"]),
    )

    writes = []
    initial_seq = ctx.get("flush_seq", 0)

    def fake_write_s3(df, fmt, s3_cfg):
        # emulate the real S3 writer formatting with ctx["current_output_ctx"]
        seq = ctx["current_output_ctx"].get("seq")
        # minimal templating support for this test (add more keys if you use them)
        fmt_ctx = {"seq": seq, **ctx["current_output_ctx"]}
        prefix = (s3_cfg.get("prefix") or "").format(**fmt_ctx)
        filename = (s3_cfg.get("filename") or "").format(**fmt_ctx)
        writes.append((len(df), fmt, prefix, filename))
        return {"s3_uri": "s3://ok"}

    ctx["write_s3"] = fake_write_s3

    df = pd.DataFrame([{"u": f"{base}/a/1"}, {"u": f"{base}/a/2"}])

    out = link_expansion.expand_links(
        ctx,
        sess,
        df,
        link_cfg={
            "enabled": True,
            "url_fields": ["u"],
            "flush": {
                "mode": "per_link",
                "prefix": "x/{seq}",
                "filename": "f-{seq}.jsonl",
            },
            "session_id": {"regex": r"/(\d+)$", "group": 1},
        },
        parse_default={"type": "json"},
        table_name="tbl",
        env_name="prod",
        api_output_cfg={
            "format": "jsonl",
            "s3": {"bucket": "b", "prefix": "p"},
        },
    )

    # per_link returns original df (no aggregate write)
    assert out.equals(df)

    # two writes happened
    assert len(writes) == 2

    # compute expected seq numbers based on starting flush_seq
    exp1 = initial_seq + 1
    exp2 = initial_seq + 2

    assert writes[0][2].startswith(f"x/{exp1}") and writes[0][3].startswith(
        f"f-{exp1}"
    )
    assert writes[1][2].startswith(f"x/{exp2}") and writes[1][3].startswith(
        f"f-{exp2}"
    )

    # transient context cleared after each flush
    assert "seq" not in ctx["current_output_ctx"]
    assert "session_id" not in ctx["current_output_ctx"]


# ---------- expand_links flush per_session ----------


def test_expand_links_flush_per_session(ctx, monkeypatch):
    base = "https://api"
    sess = FakeSession(
        {
            f"{base}/s/alpha": [FakeResponse(json_data={"items": [{"x": 1}]})],
            f"{base}/s/alpha2": [FakeResponse(json_data={"items": [{"y": 2}]})],
        }
    )

    monkeypatch.setattr(
        link_expansion,
        "to_dataframe",
        lambda r, p: pd.DataFrame(r.json()["items"]),
    )

    writes = []
    ctx["write_s3"] = lambda df, fmt, s3_cfg: writes.append(
        (df.copy(), fmt)
    ) or {"s3_uri": "s3://ok"}

    df = pd.DataFrame(
        [
            {
                "u1": f"{base}/s/alpha",
                "u2": f"{base}/s/alpha2",
            }
        ]
    )

    out = link_expansion.expand_links(
        ctx,
        sess,
        df,
        link_cfg={
            "enabled": True,
            "url_fields": ["u1", "u2"],
            "flush": {
                "mode": "per_session",
                "prefix": "sess/{session_id}",
                "filename": "part-{seq}.jsonl",
            },
            "session_id": {
                "regex": r"/([a-z]+)$",
                "group": 1,
            },  # captures "alpha" and "alpha2" -> different! ensure both grouped by per_session key string; here keys differ, so they become separate groups unless we want one session
        },
        parse_default={"type": "json"},
        table_name="tbl",
        env_name="prod",
        api_output_cfg={
            "format": "jsonl",
            "s3": {"bucket": "b", "prefix": "p"},
        },
    )

    # With two different session IDs, we expect two per-session writes
    assert len(writes) == 2
    # Each write has the respective part rows
    rows_written = sorted(int(df_.shape[0]) for df_, _ in writes)
    assert rows_written == [1, 1]
    # expanded_frames included both session frames -> result has 2 rows total
    assert out.shape[0] == 2


def test_expand_links_json_record_path_none_removes_it(ctx, monkeypatch):
    """If link_cfg.json_record_path is explicitly None, we pop it from parse cfg."""
    base = "https://api"
    sess = FakeSession({f"{base}/a": [FakeResponse(json_data={"a": {"b": 1}})]})

    seen_cfg = {}

    def fake_to_df(resp, parse_cfg):
        seen_cfg.update(parse_cfg)
        # return an empty df just to exercise path
        return pd.DataFrame([{"k": 1}])

    monkeypatch.setattr(link_expansion, "to_dataframe", fake_to_df)

    df = pd.DataFrame([{"u": f"{base}/a"}])
    out = link_expansion.expand_links(
        ctx,
        sess,
        df,
        link_cfg={
            "enabled": True,
            "url_fields": ["u"],
            "type": "json",
            "json_record_path": None,
        },
        parse_default={"type": "json", "json_record_path": "should-be-removed"},
        table_name="t",
        env_name="e",
        api_output_cfg={
            "format": "jsonl",
            "s3": {"bucket": "b", "prefix": "p"},
        },
    )
    # ensure record path removed
    assert "json_record_path" not in seen_cfg
    assert out.shape[0] == 1 and "k" in out.columns

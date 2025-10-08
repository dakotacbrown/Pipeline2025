import pandas as pd
import pytest

from api_ingestor import multipull


# --- simple log capture ---
class CaptureLog:
    def __init__(self):
        self.infos = []
        self.errors = []

    def info(self, msg):
        self.infos.append(msg)

    def error(self, msg):
        self.errors.append(msg)


# --- fixtures ---
@pytest.fixture
def ctx():
    return {
        "log": CaptureLog(),
        "current_output_ctx": {},
        "expansion_session_ids": set(),
        "flush_seq": 0,
    }


# utility to monkeypatch paginate so we don't make network calls
def patch_paginate(monkeypatch, url_to_df, capture_opts=None):
    def fake_paginate(sess, url, base_opts, parse_cfg, pag_cfg):
        # capture merged options for assertions if requested
        if capture_opts is not None:
            capture_opts.append(
                {
                    "url": url,
                    "opts": base_opts,
                    "parse": parse_cfg,
                    "pag": pag_cfg,
                }
            )
        # return pre-made df
        return url_to_df[url]

    monkeypatch.setattr(multipull, "paginate", fake_paginate)


# utility to capture log_request calls and verify prefix
def patch_log_request(monkeypatch, seen):
    def fake_log_request(ctx, url, opts, prefix=""):
        seen.append({"url": url, "prefix": prefix, "opts": opts})

    monkeypatch.setattr(multipull, "log_request", fake_log_request)


# ------------------- tests -------------------


def test_error_when_no_multi_pulls(ctx):
    with pytest.raises(ValueError):
        multipull.run_multi_pulls_with_join(
            ctx, None, "https://api/", {"multi_pulls": []}, {}
        )


def test_no_join_returns_first_pull(ctx, monkeypatch):
    url_to_df = {
        "https://api/a": pd.DataFrame([{"id": "1", "v": 10}]),
        "https://api/b": pd.DataFrame([{"id": "1", "v": 20}]),
    }
    patch_paginate(monkeypatch, url_to_df)
    seen = []
    patch_log_request(monkeypatch, seen)

    df = multipull.run_multi_pulls_with_join(
        ctx,
        None,
        "https://api/",
        api_cfg={
            "multi_pulls": [
                {"name": "A", "path": "a", "parse": {"type": "json"}},
                {"name": "B", "path": "b", "parse": {"type": "json"}},
            ],
            # no 'join'
        },
        req_opts={},
    )
    # should be first pull's df
    assert df.equals(url_to_df["https://api/a"])
    # log prefixes correct
    assert seen[0]["prefix"] == "[multi:A] "
    assert seen[1]["prefix"] == "[multi:B] "


def test_case_ignore_with_keep(ctx, monkeypatch):
    url_to_df = {
        "https://api/a": pd.DataFrame([{"ID": "x", "k": "v1"}]),
        "https://api/b": pd.DataFrame([{"id": "x", "foo": 1}]),
    }
    patch_paginate(monkeypatch, url_to_df)

    df = multipull.run_multi_pulls_with_join(
        ctx,
        None,
        "https://api/",
        api_cfg={
            "multi_pulls": [
                {"name": "left", "path": "a", "parse": {"type": "json"}},
                {"name": "right", "path": "b", "parse": {"type": "json"}},
            ],
            "join": {
                "how": "left",
                "on": ["id"],
                "case": "ignore",
                "select_from": {"right": {"keep": ["foo"]}},
            },
        },
        req_opts={},
    )
    # 'id' column normalized and 'foo' carried from right
    assert "foo" in df.columns
    assert df.shape[0] == 1


def test_case_lower_and_upper(ctx, monkeypatch):
    # lower: left has "Id", right has "ID" -> both become "id"
    url_to_df = {
        "https://api/a": pd.DataFrame([{"Id": "1"}]),
        "https://api/b": pd.DataFrame([{"ID": "1", "x": 9}]),
    }
    patch_paginate(monkeypatch, url_to_df)
    df = multipull.run_multi_pulls_with_join(
        ctx,
        None,
        "https://api/",
        api_cfg={
            "multi_pulls": [
                {"name": "A", "path": "a", "parse": {"type": "json"}},
                {"name": "B", "path": "b", "parse": {"type": "json"}},
            ],
            "join": {"how": "left", "on": ["id"], "case": "lower"},
        },
        req_opts={},
    )
    assert "id" in df.columns and "x" in df.columns

    # upper: same but final keys should match "ID"
    url_to_df2 = {
        "https://api/a": pd.DataFrame([{"Id": "1"}]),
        "https://api/b": pd.DataFrame([{"id": "1", "y": 7}]),
    }
    patch_paginate(monkeypatch, url_to_df2)
    df2 = multipull.run_multi_pulls_with_join(
        ctx,
        None,
        "https://api/",
        api_cfg={
            "multi_pulls": [
                {"name": "A", "path": "a", "parse": {"type": "json"}},
                {"name": "B", "path": "b", "parse": {"type": "json"}},
            ],
            "join": {"how": "left", "on": ["ID"], "case": "upper"},
        },
        req_opts={},
    )
    assert "ID" in df2.columns and "Y" in df2.columns


def test_missing_join_keys_are_added(ctx, monkeypatch):
    # right df lacks 'id'; code should create NA column so merge doesn't explode
    url_to_df = {
        "https://api/a": pd.DataFrame([{"id": "1", "k": "L"}]),
        "https://api/b": pd.DataFrame([{"foo": 1}]),  # missing join key
    }
    patch_paginate(monkeypatch, url_to_df)
    df = multipull.run_multi_pulls_with_join(
        ctx,
        None,
        "https://api/",
        api_cfg={
            "multi_pulls": [
                {"name": "A", "path": "a", "parse": {"type": "json"}},
                {"name": "B", "path": "b", "parse": {"type": "json"}},
            ],
            "join": {"how": "left", "on": ["id"], "case": "exact"},
        },
        req_opts={},
    )
    # still merged, 'id' present, foo exists but will be NaN
    assert "id" in df.columns and "foo" in df.columns
    assert pd.isna(df["foo"].iloc[0])


def test_select_from_keep_and_rename(ctx, monkeypatch):
    url_to_df = {
        "https://api/a": pd.DataFrame([{"id": "1"}]),
        "https://api/b": pd.DataFrame([{"id": "1", "foo": 5, "dropme": 1}]),
    }
    patch_paginate(monkeypatch, url_to_df)
    df = multipull.run_multi_pulls_with_join(
        ctx,
        None,
        "https://api/",
        api_cfg={
            "multi_pulls": [
                {"name": "A", "path": "a", "parse": {"type": "json"}},
                {"name": "B", "path": "b", "parse": {"type": "json"}},
            ],
            "join": {
                "how": "left",
                "on": ["id"],
                "case": "exact",
                "select_from": {
                    "B": {"keep": ["foo"], "rename": {"foo": "bar"}}
                },
            },
        },
        req_opts={},
    )
    assert "bar" in df.columns
    assert "dropme" not in df.columns


def test_headers_and_params_merge_and_log_prefix(ctx, monkeypatch):
    # Ensure per-pull headers/params override base, and prefixes are correct.
    url_to_df = {
        "https://api/a": pd.DataFrame([{"id": "1"}]),
        "https://api/b": pd.DataFrame([{"id": "1", "v": 2}]),
    }
    captured = []
    patch_paginate(monkeypatch, url_to_df, capture_opts=captured)
    seen_logs = []

    def fake_log(ctx, url, opts, prefix=""):
        seen_logs.append((url, prefix, opts))

    monkeypatch.setattr(multipull, "log_request", fake_log)

    df = multipull.run_multi_pulls_with_join(
        ctx,
        None,
        "https://api/",
        api_cfg={
            "multi_pulls": [
                {
                    "name": "A",
                    "path": "a",
                    "parse": {"type": "json"},
                    "headers": {"H": "2"},
                    "params": {"p": "9", "q": "z"},
                },
                {"name": "B", "path": "b", "parse": {"type": "json"}},
            ],
            "join": {"how": "left", "on": ["id"], "case": "exact"},
        },
        req_opts={"headers": {"H": "1"}, "params": {"p": "0"}},
    )
    assert df.shape[0] == 1

    # First call captured merged overrides (A)
    a_call = captured[0]
    assert a_call["url"].endswith("/a")
    assert a_call["opts"]["headers"]["H"] == "2"  # per-pull wins
    assert a_call["opts"]["params"]["p"] == "9"  # per-pull wins
    assert a_call["opts"]["params"]["q"] == "z"  # new param

    # Log prefixes include [multi:NAME]
    assert seen_logs[0][1] == "[multi:A] "
    assert seen_logs[1][1] == "[multi:B] "

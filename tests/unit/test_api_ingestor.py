import builtins
import os
import time
from datetime import date

import pandas as pd
import pytest

# ---- Fakes / test utilities --------------------------------------------------


class FakeResponse:
    def __init__(
        self, json_data=None, content=None, status_code=200, links=None
    ):
        self._json_data = json_data
        self.content = content if content is not None else b""
        self.status_code = status_code
        self.links = links or {}

    def json(self):
        return self._json_data

    def raise_for_status(self):
        if self.status_code >= 400:
            import requests

            raise requests.HTTPError(f"HTTP {self.status_code}")


class FakeSession:
    """
    Minimal stand-in for requests.Session:
      - records headers/proxies/verify/auth
      - returns pre-seeded responses in FIFO order
      - records every GET call (url, kwargs)
    """

    def __init__(self, responses=None):
        self.headers = {}
        self.proxies = {}
        self.verify = True
        self.auth = None
        self._responses = list(responses or [])
        self.calls = []  # list of (url, kwargs)

    def mount(self, *args, **kwargs):
        # noop for retry adapter mounting
        pass

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        if not self._responses:
            raise AssertionError("FakeSession ran out of queued responses")
        return self._responses.pop(0)


# Simple logger that captures output for assertions
class CapturingLogger:
    def __init__(self):
        self.infos = []
        self.errors = []

    def info(self, msg):
        self.infos.append(str(msg))

    def error(self, msg):
        self.errors.append(str(msg))


# ---- Fixtures ----------------------------------------------------------------


@pytest.fixture
def logger():
    return CapturingLogger()


@pytest.fixture
def make_ingestor(logger, monkeypatch):
    """
    Factory that returns a function to construct an ApiIngestor with a patched
    _build_session returning our FakeSession filled with specific responses.
    Also stubs _write_output to capture the produced DataFrame and avoid S3/boto3.
    """
    from utils.api_ingestor import (  # <-- replace with your real module path if needed
        ApiIngestor,
    )

    def _build(config, responses):
        fake = FakeSession(responses=responses)
        monkeypatch.setenv("TOKEN", "XYZ")
        monkeypatch.setattr(ApiIngestor, "_build_session", lambda self, r: fake)

        # Capture DF and bypass any real I/O in tests
        def _fake_write_output(self, df, table_name, env_name, out_cfg):
            # capture the DF so tests can assert on it
            self._last_df = df.copy()
            # return stable, dummy metadata
            fmt = (
                out_cfg.get("format") if isinstance(out_cfg, dict) else None
            ) or "csv"
            return {
                "format": fmt,
                "s3_bucket": "test-bucket",
                "s3_key": "test/key",
                "s3_uri": "s3://test-bucket/test/key",
                "bytes": len(df.to_json().encode("utf-8")),
            }

        monkeypatch.setattr(ApiIngestor, "_write_output", _fake_write_output)

        ing = ApiIngestor(config=config, log=logger)
        return ing, fake

    return _build


# ---- Tests: env var substitution ---------------------------------------------


def test_env_substitution_applies_to_headers(make_ingestor):
    # apis.request_defaults.headers.Authorization contains ${TOKEN}
    config = {
        "envs": {"prod": {"base_url": "http://host/"}},
        "apis": {
            "request_defaults": {
                "headers": {"Authorization": "Bearer ${TOKEN}"},
                "timeout": 10,
            },
            "path": "items",
            "pagination": {"mode": "none"},
            "my_table": {"parse": {"type": "json", "json_record_path": "data"}},
        },
    }

    responses = [FakeResponse(json_data={"data": []})]
    ing, sess = make_ingestor(config, responses)

    meta = ing.run_once("my_table", "prod")
    df = ing._last_df
    assert df.empty
    # session defaults should be applied
    assert sess.headers.get("Authorization") == "Bearer XYZ"
    # sanity metadata check
    assert meta["rows"] == 0
    assert meta["pagination_mode"] == "none"


# ---- Tests: pagination modes -------------------------------------------------


def test_pagination_none_single_call(make_ingestor):
    config = {
        "envs": {"prod": {"base_url": "http://api/"}},
        "apis": {
            "request_defaults": {"headers": {"X": "1"}},
            "path": "things",
            "pagination": {"mode": "none"},
            "t": {"parse": {"type": "json", "json_record_path": "data"}},
        },
    }
    responses = [FakeResponse(json_data={"data": [{"id": 1}, {"id": 2}]})]
    ing, sess = make_ingestor(config, responses)

    meta = ing.run_once("t", "prod")
    df = ing._last_df
    assert list(df["id"]) == [1, 2]
    assert len(sess.calls) == 1
    assert sess.calls[0][0] == "http://api/things"
    assert meta["rows"] == 2
    assert meta["pagination_mode"] == "none"


def test_pagination_page_until_empty(make_ingestor):
    config = {
        "envs": {"prod": {"base_url": "http://api/"}},
        "apis": {
            "request_defaults": {},
            "path": "pg",
            "pagination": {
                "mode": "page",
                "page_param": "page",
                "start_page": 1,
            },
            "t": {"parse": {"type": "json", "json_record_path": "data"}},
        },
    }
    responses = [
        FakeResponse(json_data={"data": [{"id": 1}, {"id": 2}]}),  # page 1
        FakeResponse(json_data={"data": []}),  # page 2 -> stop
    ]
    ing, sess = make_ingestor(config, responses)

    meta = ing.run_once("t", "prod")
    df = ing._last_df
    assert list(df["id"]) == [1, 2]
    # Two calls: initial + empty page (used to detect stop)
    assert len(sess.calls) == 2
    # page=2 should have been set in the second request
    assert sess.calls[1][1]["params"]["page"] == 2
    assert meta["rows"] == 2
    assert meta["pagination_mode"] == "page"


def test_pagination_cursor_token(make_ingestor):
    config = {
        "envs": {"prod": {"base_url": "http://api/"}},
        "apis": {
            "request_defaults": {},
            "path": "cur",
            "pagination": {
                "mode": "cursor",
                "cursor_param": "cursor",
                "next_cursor_path": "meta.next",
                "page_size_param": "limit",
                "page_size_value": 2,
            },
            "t": {
                "parse": {"type": "json", "json_record_path": "data"},
            },
        },
    }
    responses = [
        FakeResponse(json_data={"data": [{"id": 1}], "meta": {"next": "abc"}}),
        FakeResponse(json_data={"data": [{"id": 2}], "meta": {"next": None}}),
    ]
    ing, sess = make_ingestor(config, responses)

    meta = ing.run_once("t", "prod")
    df = ing._last_df
    assert list(df["id"]) == [1, 2]
    # First call should include page size limit
    assert sess.calls[0][1]["params"]["limit"] == 2
    # Second call should include cursor=abc
    assert sess.calls[1][1]["params"]["cursor"] == "abc"
    assert meta["rows"] == 2
    assert meta["pagination_mode"] == "cursor"


def test_pagination_link_header(make_ingestor):
    config = {
        "envs": {"prod": {"base_url": "http://api/"}},
        "apis": {
            "request_defaults": {},
            "path": "alpha",
            "pagination": {"mode": "link-header"},
            "t": {"parse": {"type": "json", "json_record_path": "data"}},
        },
    }
    responses = [
        FakeResponse(
            json_data={"data": [{"id": 1}]},
            links={"next": {"url": "http://api/alpha?page=2"}},
        ),
        FakeResponse(json_data={"data": [{"id": 2}]}),
    ]
    ing, sess = make_ingestor(config, responses)

    meta = ing.run_once("t", "prod")
    df = ing._last_df
    assert list(df["id"]) == [1, 2]
    assert len(sess.calls) == 2
    assert sess.calls[1][0] == "http://api/alpha?page=2"
    assert meta["rows"] == 2
    assert meta["pagination_mode"] == "link-header"


def test_pagination_salesforce(make_ingestor):
    config = {
        "envs": {"prod": {"base_url": "https://sf.example/"}},
        "apis": {
            "request_defaults": {},
            "path": "services/data/v61.0/query",
            "pagination": {
                "mode": "salesforce",
                "done_path": "done",
                "next_url_path": "nextRecordsUrl",
                "clear_params_on_next": True,
            },
            "t": {
                "parse": {
                    "type": "json",
                    "json_record_path": "records",
                    "json_drop_keys_any_depth": ["attributes"],
                }
            },
        },
    }
    responses = [
        FakeResponse(
            json_data={
                "done": False,
                "records": [{"id": 1, "attributes": {"type": "X"}}],
                "nextRecordsUrl": "/services/data/v61.0/query/nextA",
            }
        ),
        FakeResponse(
            json_data={
                "done": True,
                "records": [{"id": 2, "attributes": {"type": "Y"}}],
            }
        ),
    ]
    ing, sess = make_ingestor(config, responses)

    meta = ing.run_once("t", "prod")
    df = ing._last_df
    assert list(df["id"]) == [1, 2]
    # second call must have used absolute join of relative nextRecordsUrl
    assert sess.calls[1][0].endswith("/services/data/v61.0/query/nextA")
    assert meta["rows"] == 2
    assert meta["pagination_mode"] == "salesforce"


# ---- Tests: backfill strategies ---------------------------------------------


def test_backfill_date_windows_two_slices(make_ingestor):
    # window_days=2 over a 3-day range -> 2 windows
    config = {
        "envs": {"prod": {"base_url": "http://api/"}},
        "apis": {
            "request_defaults": {},
            "path": "daily",
            "pagination": {"mode": "none"},
            "t": {
                "parse": {"type": "json", "json_record_path": "data"},
                "backfill": {
                    "enabled": True,
                    "strategy": "date",
                    "window_days": 2,
                    "start_param": "start_date",
                    "end_param": "end_date",
                    "date_format": "%Y-%m-%d",
                },
            },
        },
    }
    responses = [
        FakeResponse(json_data={"data": [{"d": "A1"}]}),  # first slice
        FakeResponse(json_data={"data": [{"d": "B1"}]}),  # second slice
    ]
    ing, sess = make_ingestor(config, responses)

    meta = ing.run_backfill(
        "t", "prod", start=date(2024, 1, 1), end=date(2024, 1, 3)
    )
    df = ing._last_df
    assert list(df["d"]) == ["A1", "B1"]
    # Inspect the params passed on each slice
    p0 = sess.calls[0][1]["params"]
    p1 = sess.calls[1][1]["params"]
    assert p0["start_date"] == "2024-01-01" and p0["end_date"] == "2024-01-02"
    assert p1["start_date"] == "2024-01-03" and p1["end_date"] == "2024-01-03"
    assert meta["rows"] == 2
    assert meta["strategy"] == "date"


def test_backfill_soql_window_with_salesforce_pager(make_ingestor):
    config = {
        "envs": {"prod": {"base_url": "https://sf.example/"}},
        "apis": {
            "request_defaults": {},
            "path": "services/data/v61.0/query",
            "pagination": {
                "mode": "salesforce",
                "done_path": "done",
                "next_url_path": "nextRecordsUrl",
            },
            "t": {
                "parse": {"type": "json", "json_record_path": "records"},
                "backfill": {
                    "enabled": True,
                    "strategy": "soql_window",
                    "date_field": "LastModifiedDate",
                    "date_format": "%Y-%m-%dT%H:%M:%SZ",
                    "window_days": 1,
                    "soql_template": "SELECT Id FROM Obj WHERE {date_field} >= {start} AND {date_field} < {end}",
                },
            },
        },
    }
    responses = [
        FakeResponse(json_data={"done": True, "records": [{"Id": "1"}]}),
        FakeResponse(json_data={"done": True, "records": [{"Id": "2"}]}),
    ]
    ing, sess = make_ingestor(config, responses)

    meta = ing.run_backfill(
        "t", "prod", start=date(2024, 1, 1), end=date(2024, 1, 2)
    )
    df = ing._last_df
    assert list(df["Id"]) == ["1", "2"]

    # Verify 'q' param was injected on each slice, with half-open end date
    q0 = sess.calls[0][1]["params"]["q"]
    q1 = sess.calls[1][1]["params"]["q"]
    assert "2024-01-01T00:00:00Z" in q0 and "2024-01-02T00:00:00Z" in q0
    assert "2024-01-02T00:00:00Z" in q1 and "2024-01-03T00:00:00Z" in q1
    assert meta["rows"] == 2
    assert meta["strategy"] == "soql_window"
    assert meta["pagination_mode"] == "salesforce"


def test_backfill_cursor_with_next_token(make_ingestor):
    config = {
        "envs": {"prod": {"base_url": "http://api/"}},
        "apis": {
            "request_defaults": {},
            "path": "cur",
            "pagination": {
                "mode": "cursor",
                "cursor_param": "cursor",
                "next_cursor_path": "meta.next",
            },
            "t": {
                "parse": {"type": "json", "json_record_path": "data"},
                "backfill": {
                    "enabled": True,
                    "strategy": "cursor",
                    "cursor": {"start_value": "s0"},
                },
            },
        },
    }
    responses = [
        FakeResponse(json_data={"data": [{"i": 1}], "meta": {"next": "s1"}}),
        FakeResponse(json_data={"data": [{"i": 2}], "meta": {"next": None}}),
    ]
    ing, sess = make_ingestor(config, responses)

    meta = ing.run_backfill(
        "t", "prod", start=date(2024, 1, 1), end=date(2024, 1, 2)
    )
    df = ing._last_df
    assert list(df["i"]) == [1, 2]
    assert sess.calls[0][1]["params"]["cursor"] == "s0"
    assert sess.calls[1][1]["params"]["cursor"] == "s1"
    assert meta["rows"] == 2
    assert meta["strategy"] == "cursor"


# ---- Tests: cursor stop conditions ------------------------------------------


def test_cursor_backfill_stop_by_item(make_ingestor):
    config = {
        "envs": {"prod": {"base_url": "http://api/"}},
        "apis": {
            "request_defaults": {},
            "path": "cur",
            "pagination": {
                "mode": "cursor",
                "cursor_param": "cursor",
                "next_cursor_path": "meta.next",
            },
            "t": {
                "parse": {"type": "json", "json_record_path": "data"},
                "backfill": {
                    "enabled": True,
                    "strategy": "cursor",
                    "cursor": {
                        "stop_at_item": {
                            "field": "id",
                            "value": "b",
                            "inclusive": False,
                        }
                    },
                },
            },
        },
    }
    responses = [
        FakeResponse(
            json_data={
                "data": [{"id": "a"}, {"id": "b"}, {"id": "c"}],
                "meta": {"next": "x"},
            }
        ),
    ]
    ing, sess = make_ingestor(config, responses)

    meta = ing.run_backfill(
        "t", "prod", start=date(2024, 1, 1), end=date(2024, 1, 2)
    )
    df = ing._last_df
    assert list(df["id"]) == ["a"]  # trimmed before 'b' because inclusive=False
    assert meta["rows"] == 1
    assert meta["strategy"] == "cursor"


def test_cursor_backfill_stop_when_older_than(make_ingestor):
    config = {
        "envs": {"prod": {"base_url": "http://api/"}},
        "apis": {
            "request_defaults": {},
            "path": "cur",
            "pagination": {
                "mode": "cursor",
                "cursor_param": "cursor",
                "next_cursor_path": "meta.next",
            },
            "t": {
                "parse": {"type": "json", "json_record_path": "data"},
                "backfill": {
                    "enabled": True,
                    "strategy": "cursor",
                    "cursor": {
                        "stop_when_older_than": {
                            "field": "ts",
                            "value": "2024-01-02T00:00:00Z",
                        }
                    },
                },
            },
        },
    }
    responses = [
        FakeResponse(
            json_data={
                "data": [
                    {"id": 1, "ts": "2024-01-03T01:00:00Z"},
                    {
                        "id": 2,
                        "ts": "2024-01-01T10:00:00Z",
                    },  # older -> cut here
                    {"id": 3, "ts": "2024-01-01T09:00:00Z"},
                ],
                "meta": {"next": None},
            }
        ),
    ]
    ing, _ = make_ingestor(config, responses)

    meta = ing.run_backfill(
        "t", "prod", start=date(2024, 1, 1), end=date(2024, 1, 2)
    )
    df = ing._last_df
    # only the >= cutoff rows are kept in the final (and loop stops)
    assert list(df["id"]) == [1]
    assert meta["rows"] == 1
    assert meta["strategy"] == "cursor"


# ---- Tests: link expansion ---------------------------------------------------


def test_link_expansion_inherits_session_defaults_and_timeout(make_ingestor):
    # base page has a 'detail_url'; expansion fetch returns a list under "items"
    config = {
        "envs": {"prod": {"base_url": "http://api/"}},
        "apis": {
            "request_defaults": {
                "headers": {"Authorization": "Bearer ${TOKEN}"},
                "verify": False,
            },
            "path": "base",
            "pagination": {"mode": "none"},
            # Tell link expansion how to parse the expanded payload
            "link_expansion": {
                "enabled": True,
                "url_fields": ["detail_url"],
                "timeout": 5,
                "json_record_path": "items",
            },
            # Base table parse still uses "data" for the first call
            "t": {"parse": {"type": "json", "json_record_path": "data"}},
        },
    }
    base = FakeResponse(
        json_data={"data": [{"detail_url": "http://exp/detail"}]}
    )
    expanded = FakeResponse(json_data={"items": [{"k": 1}]})
    responses = [base, expanded]
    ing, sess = make_ingestor(config, responses)

    meta = ing.run_once("t", "prod")
    df = ing._last_df

    # We now parse the expanded payload at "items" → column 'k' exists
    assert "k" in df.columns
    assert df["k"].tolist() == [1]

    # Two calls: 1) base url, 2) link expansion
    assert len(sess.calls) == 2

    # On expansion call, kwargs should only include 'timeout'
    exp_kwargs = sess.calls[1][1]
    assert "timeout" in exp_kwargs and len(exp_kwargs.keys()) == 1

    # Session defaults applied globally
    assert sess.headers.get("Authorization") == "Bearer XYZ"
    assert sess.verify is False
    assert meta["rows"] == 1


# ---- Tests: logging redaction -----------------------------------------------


def test_log_redaction_redacts_sensitive_headers_and_params(
    make_ingestor, logger
):
    config = {
        "envs": {"prod": {"base_url": "http://api/"}},
        "apis": {
            "request_defaults": {
                "headers": {"Authorization": "Bearer secret", "X-Ok": "ok"},
                "timeout": 10,
            },
            "path": "items",
            "pagination": {"mode": "none"},
            "t": {
                "parse": {"type": "json", "json_record_path": "data"},
                "params": {"token": "supersecret", "safe": "1"},
            },
        },
    }
    responses = [FakeResponse(json_data={"data": []})]
    ing, _ = make_ingestor(config, responses)

    _ = ing.run_once("t", "prod")

    # find the request log line (not the final "done" line)
    req_line = next((m for m in logger.infos if m.startswith("GET http")), "")
    assert req_line, f"Request log not found. Infos: {logger.infos}"

    # Redactions present
    assert "***REDACTED***" in req_line

    # Secret values absent
    assert "supersecret" not in req_line
    assert "Bearer secret" not in req_line

    # Non-sensitive values/keys may still be present
    assert "safe" in req_line or "SAFE" in req_line


# ---- Tests: whitelist --------------------------------------------------------


def test_whitelist_drops_unknown_kwargs(make_ingestor):
    # Access the private method through an instance
    config = {"envs": {"prod": {"base_url": "http://api/"}}, "apis": {"t": {}}}
    from utils.api_ingestor import ApiIngestor

    ing = ApiIngestor(config=config, log=CapturingLogger())

    opts = {
        "headers": {"A": "1"},
        "timeout": 5,
        "retries": {"total": 3},
        "bogus": True,
    }
    safe = ing._whitelist_request_opts(opts)
    assert "headers" in safe and "timeout" in safe
    assert "retries" not in safe and "bogus" not in safe


# ===================== Extra tests to lift coverage ===========================


def test_to_dataframe_jsonl_parsing(make_ingestor):
    config = {
        "envs": {"prod": {"base_url": "http://api/"}},
        "apis": {
            "path": "events",
            "pagination": {"mode": "none"},
            "t": {"parse": {"type": "jsonl"}},
        },
    }
    ndjson = b'{"a":1}\n{"a":2}\n'
    responses = [FakeResponse(content=ndjson)]
    ing, _ = make_ingestor(config, responses)

    _ = ing.run_once("t", "prod")
    df = ing._last_df
    assert list(df["a"]) == [1, 2]


def test_to_dataframe_csv_parsing(make_ingestor):
    config = {
        "envs": {"prod": {"base_url": "http://api/"}},
        "apis": {
            "path": "csv",
            "pagination": {"mode": "none"},
            "t": {"parse": {"type": "csv"}},
        },
    }
    csv_bytes = b"x,y\n1,2\n3,4\n"
    responses = [FakeResponse(content=csv_bytes)]
    ing, _ = make_ingestor(config, responses)

    _ = ing.run_once("t", "prod")
    df = ing._last_df
    assert df.shape == (2, 2)
    assert df["x"].tolist() == [1, 3]


def test_apply_session_defaults_and_build_url(logger):
    from utils.api_ingestor import ApiIngestor

    ing = ApiIngestor(
        config={"envs": {"e": {"base_url": "http://h/"}}, "apis": {"t": {}}},
        log=logger,
    )
    sess = FakeSession()
    ing._apply_session_defaults(
        sess,
        {
            "headers": {"A": "1"},
            "verify": False,
            "auth": ("user", "pass"),
            "proxies": {"http": "http://p"},
        },
    )
    assert sess.headers["A"] == "1"
    assert sess.verify is False
    assert sess.auth == ("user", "pass")
    assert sess.proxies["http"] == "http://p"
    # build_url handles slashes
    assert ing._build_url("http://h", "/x") == "http://h/x"
    assert ing._build_url("http://h/", "x") == "http://h/x"


def test_build_session_with_retries(logger):
    from utils.api_ingestor import ApiIngestor

    ing = ApiIngestor(
        config={"envs": {"e": {"base_url": "http://h"}}, "apis": {"t": {}}},
        log=logger,
    )
    s = ing._build_session(
        {
            "total": 2,
            "backoff_factor": 0.1,
            "status_forcelist": [429, 500],
            "allowed_methods": ["GET"],
        }
    )
    import requests

    assert isinstance(s, requests.Session)


def test_build_session_retry_fallbacks(monkeypatch, logger):
    """
    Force urllib3 Retry to raise TypeError on the first signature (allowed_methods)
    and succeed on the second signature (method_whitelist).
    """
    from utils import api_ingestor as api_mod

    calls = {"attempts": []}

    class FakeRetry:
        def __init__(self, *args, **kwargs):
            # record kwargs to ensure both code paths exercised
            calls["attempts"].append(set(kwargs.keys()))
            if "allowed_methods" in kwargs:
                # simulate v1.x that doesn't accept allowed_methods
                raise TypeError("no allowed_methods")
            # if 'raise_on_status' is present on the v1 path, also simulate another failure
            # to drive the third fallback path (older urllib3 without raise_on_status)
            if "raise_on_status" in kwargs and "method_whitelist" in kwargs:
                raise TypeError("no raise_on_status in very old urllib3")

        # emulate attribute used later
        respect_retry_after_header = True

    monkeypatch.setattr(api_mod, "Retry", FakeRetry)
    ing = api_mod.ApiIngestor(
        config={"envs": {"e": {"base_url": "http://h"}}, "apis": {"t": {}}},
        log=logger,
    )

    # Should not raise
    _ = ing._build_session(
        {
            "total": 1,
            "backoff_factor": 0.1,
            "status_forcelist": [429],
            "allowed_methods": ["GET"],
        }
    )

    # We should have tried first with 'allowed_methods' then with 'method_whitelist'
    assert any("allowed_methods" in a for a in calls["attempts"])
    assert any("method_whitelist" in a for a in calls["attempts"])


def test_drop_keys_and_record_path(logger):
    from utils.api_ingestor import ApiIngestor

    ing = ApiIngestor(
        config={"envs": {"e": {"base_url": "http://h"}}, "apis": {"t": {}}},
        log=logger,
    )
    data = {
        "meta": {"attributes": {"x": 1}},
        "outer": {"items": [{"i": 1, "attributes": {"y": 2}}]},
    }
    dropped = ing._drop_keys_any_depth(data, {"attributes"})
    assert "attributes" not in str(dropped)
    df = ing._json_obj_to_df(dropped, {"json_record_path": "outer.items"})
    assert df["i"].tolist() == [1]


def test_prepare_validation_errors(logger):
    from utils.api_ingestor import ApiIngestor

    # missing base_url
    with pytest.raises(ValueError):
        ApiIngestor(
            config={"envs": {"prod": {}}, "apis": {"t": {}}}, log=logger
        )._prepare("t", "prod")
    # missing table
    with pytest.raises(KeyError):
        ApiIngestor(
            config={"envs": {"prod": {"base_url": "http://h"}}, "apis": {}},
            log=logger,
        )._prepare("t", "prod")


def test_log_exception_records_stack(make_ingestor, logger):
    config = {
        "envs": {"prod": {"base_url": "http://api/"}},
        "apis": {
            "path": "x",
            "pagination": {"mode": "none"},
            "t": {"parse": {"type": "json"}},
        },
    }
    responses = [FakeResponse(status_code=500)]  # will raise_for_status
    ing, _ = make_ingestor(config, responses)
    with pytest.raises(Exception):
        ing.run_once("t", "prod")
    assert any("Stack Trace:" in e for e in logger.errors)


def test_cursor_backfill_chain_field(make_ingestor):
    # Advance using chain_field instead of next_cursor_path
    config = {
        "envs": {"prod": {"base_url": "http://api/"}},
        "apis": {
            "path": "cur",
            "pagination": {
                "mode": "cursor",
                "cursor_param": "cursor",
                "chain_field": "id",
            },
            "t": {
                "parse": {"type": "json", "json_record_path": "data"},
                "backfill": {"enabled": True, "strategy": "cursor"},
            },
        },
    }
    responses = [
        FakeResponse(json_data={"data": [{"id": "a"}, {"id": "b"}]}),  # page 1
        FakeResponse(json_data={"data": [{"id": "c"}]}),  # page 2
        FakeResponse(json_data={"data": []}),  # page 3 -> stop
    ]
    ing, sess = make_ingestor(config, responses)

    meta = ing.run_backfill(
        "t", "prod", start=date(2024, 1, 1), end=date(2024, 1, 2)
    )
    df = ing._last_df

    # rows from first two pages
    assert df["id"].tolist() == ["a", "b", "c"]
    assert meta["strategy"] == "cursor"

    # call 2 uses last id from page 1
    assert sess.calls[1][1]["params"]["cursor"] == "b"
    # call 3 uses last id from page 2
    assert sess.calls[2][1]["params"]["cursor"] == "c"
    # three total calls (third empty page triggers stop)
    assert len(sess.calls) == 3


def test_serialize_df_csv_gzip_and_jsonl(logger):
    from utils.api_ingestor import ApiIngestor

    ing = ApiIngestor(
        config={"envs": {"e": {"base_url": "http://h"}}, "apis": {"t": {}}},
        log=logger,
    )
    df = pd.DataFrame({"x": [1, 2]})
    raw, ctype, enc = ing._serialize_df(df, "csv", {"compression": "gzip"})
    assert (
        ctype == "text/csv"
        and enc == "gzip"
        and isinstance(raw, (bytes, bytearray))
    )
    raw2, ctype2, enc2 = ing._serialize_df(df, "jsonl", {})
    assert ctype2 == "application/x-ndjson" and enc2 is None and b"\n" in raw2


def test_serialize_df_parquet_no_skip(logger):
    """
    Always run this test:
      - If pyarrow is available, it should succeed.
      - If not, _serialize_df('parquet', ...) should raise (pandas will complain).
    """
    import importlib

    from utils.api_ingestor import ApiIngestor

    ing = ApiIngestor(
        config={"envs": {"e": {"base_url": "http://h"}}, "apis": {"t": {}}},
        log=logger,
    )
    df = pd.DataFrame({"x": [1, 2]})

    has_pyarrow = importlib.util.find_spec("pyarrow") is not None
    if has_pyarrow:
        raw, ctype, enc = ing._serialize_df(
            df, "parquet", {"compression": "snappy"}
        )
        assert ctype == "application/vnd.apache.parquet"
        assert enc is None
        assert isinstance(raw, (bytes, bytearray)) and len(raw) > 0
    else:
        with pytest.raises(Exception):
            ing._serialize_df(df, "parquet", {"compression": "snappy"})


def test_write_output_validation_errors(logger):
    from utils.api_ingestor import ApiIngestor

    ing = ApiIngestor(
        config={"envs": {"e": {"base_url": "http://h"}}, "apis": {"t": {}}},
        log=logger,
    )
    with pytest.raises(ValueError):
        ing._write_output(pd.DataFrame(), "t", "e", None)  # no out_cfg
    with pytest.raises(ValueError):
        ing._write_output(
            pd.DataFrame(), "t", "e", {"format": "xml"}
        )  # bad fmt
    with pytest.raises(ValueError):
        ing._write_output(
            pd.DataFrame(),
            "t",
            "e",
            {"format": "csv", "write_empty": False, "s3": {"bucket": "b"}},
        )
    with pytest.raises(ValueError):
        ing._write_output(
            pd.DataFrame({"x": [1]}), "t", "e", {"format": "csv"}
        )  # no s3 block


def test_write_s3_with_fake_boto3(monkeypatch, logger):
    from utils.api_ingestor import ApiIngestor

    class FakeS3Client:
        def __init__(self):
            self.puts = []

        def put_object(self, **kwargs):
            self.puts.append(kwargs)

    class FakeSessionObj:
        def __init__(self, region_name=None):
            self.region = region_name

        def client(self, name, endpoint_url=None):
            assert name == "s3"
            return FakeS3Client()

    class FakeBoto3:
        class session:
            Session = FakeSessionObj

    monkeypatch.setitem(os.sys.modules, "boto3", FakeBoto3)

    ing = ApiIngestor(
        config={"envs": {"e": {"base_url": "http://h"}}, "apis": {"t": {}}},
        log=logger,
    )
    df = pd.DataFrame({"x": [1]})
    meta = ing._write_s3(
        df,
        "t",
        "e",
        "csv",
        {
            "bucket": "bkt",
            "prefix": "p",
            "filename": "f.csv",
            "region_name": "us",
            "endpoint_url": "http://s3.local",
        },
    )
    assert meta["s3_uri"].startswith("s3://bkt/")


def test_real_write_output_via_run_once_with_fake_boto3(monkeypatch, logger):
    """
    End-to-end run_once that exercises the real _write_output/_write_s3/_serialize_df (jsonl),
    using a fake boto3 and a FakeSession for HTTP.
    """
    from utils.api_ingestor import ApiIngestor

    class FakeS3Client:
        def __init__(self):
            self.puts = []

        def put_object(self, **kwargs):
            self.puts.append(kwargs)

    class FakeSessionObj:
        def __init__(self, region_name=None):
            self.region = region_name

        def client(self, name, endpoint_url=None):
            assert name == "s3"
            return FakeS3Client()

    class FakeBoto3:
        class session:
            Session = FakeSessionObj

    monkeypatch.setitem(os.sys.modules, "boto3", FakeBoto3)

    config = {
        "envs": {"prod": {"base_url": "http://api/"}},
        "apis": {
            "path": "things",
            "pagination": {"mode": "none"},
            "t": {
                "parse": {"type": "json", "json_record_path": "data"},
                "output": {
                    "format": "jsonl",
                    "s3": {
                        "bucket": "b",
                        "prefix": "p",
                        "filename": "f.jsonl",
                        "region_name": "us",
                        "endpoint_url": "http://s3.local",
                    },
                },
            },
        },
    }
    ing = ApiIngestor(config=config, log=logger)

    # Use FakeSession for HTTP; IMPORTANT: pass responses positionally to avoid kw-arg mismatch
    fs = FakeSession([FakeResponse(json_data={"data": [{"x": 1}, {"x": 2}]})])

    import utils.api_ingestor as api_mod

    monkeypatch.setattr(
        api_mod.ApiIngestor, "_build_session", lambda self, r: fs
    )

    meta = ing.run_once("t", "prod")
    assert meta["rows"] == 2 and meta["format"] == "jsonl"


def test_to_dataframe_unsupported_parse_type(logger, make_ingestor):
    config = {
        "envs": {"prod": {"base_url": "http://api/"}},
        "apis": {
            "path": "bad",
            "pagination": {"mode": "none"},
            "t": {"parse": {"type": "xml"}},
        },
    }
    responses = [FakeResponse(content=b"<x/>")]
    ing, _ = make_ingestor(config, responses)
    with pytest.raises(ValueError):
        ing.run_once("t", "prod")


def test_expand_env_value_nested_and_missing(logger, monkeypatch):
    from utils.api_ingestor import ApiIngestor

    ing = ApiIngestor(
        config={"envs": {"e": {"base_url": "http://h"}}, "apis": {"t": {}}},
        log=logger,
    )
    monkeypatch.setenv("FOO", "bar")
    val = {"a": "${FOO}", "b": ["${FOO}", "${MISSING}"]}
    out = ing._expand_env_value(val)
    assert out["a"] == "bar"
    assert out["b"][0] == "bar"
    assert out["b"][1] == "${MISSING}"


def test_dig_handles_missing_and_nondict(logger):
    from utils.api_ingestor import ApiIngestor

    ing = ApiIngestor(
        config={"envs": {"e": {"base_url": "http://h"}}, "apis": {"t": {}}},
        log=logger,
    )
    obj = {"a": {"b": {"c": 1}}}
    assert ing._dig(obj, "a.b.c") == 1
    assert ing._dig(obj, "a.x.c") is None
    assert ing._dig({"a": 1}, "a.b") is None


def test_get_from_row_nested_and_missing(logger):
    from utils.api_ingestor import ApiIngestor

    ing = ApiIngestor(
        config={"envs": {"e": {"base_url": "http://h"}}, "apis": {"t": {}}},
        log=logger,
    )
    row = pd.Series({"flat": 1, "root": {"inner": {"leaf": 9}}})
    assert ing._get_from_row(row, "flat") == 1
    assert ing._get_from_row(row, "root.inner.leaf") == 9
    assert ing._get_from_row(row, "root.missing") is None


def test_salesforce_clear_params_on_next(make_ingestor):
    config = {
        "envs": {"prod": {"base_url": "https://sf.example/"}},
        "apis": {
            "path": "services/data/v61.0/query",
            "pagination": {
                "mode": "salesforce",
                "done_path": "done",
                "next_url_path": "nextRecordsUrl",
                "clear_params_on_next": True,
            },
            "t": {
                "parse": {"type": "json", "json_record_path": "records"},
                "params": {"q": "SELECT Id FROM Account"},
            },
        },
    }
    responses = [
        FakeResponse(
            json_data={
                "done": False,
                "records": [{"Id": 1}],
                "nextRecordsUrl": "/services/data/v61.0/query/next",
            }
        ),
        FakeResponse(json_data={"done": True, "records": [{"Id": 2}]}),
    ]
    ing, sess = make_ingestor(config, responses)
    ing.run_once("t", "prod")
    # first call has params
    assert "params" in sess.calls[0][1]
    # second call should NOT have 'params'
    assert "params" not in sess.calls[1][1]


def test_link_expansion_multiple_urls_merged(make_ingestor):
    config = {
        "envs": {"prod": {"base_url": "http://api/"}},
        "apis": {
            "path": "base",
            "pagination": {"mode": "none"},
            "link_expansion": {
                "enabled": True,
                "url_fields": ["u1", "u2"],
                "json_record_path": None,  # explicit drop is fine
            },
            "t": {"parse": {"type": "json", "json_record_path": "rows"}},
        },
    }
    base = FakeResponse(
        json_data={"rows": [{"u1": "http://api/a", "u2": "http://api/b"}]}
    )
    exp_a = FakeResponse(json_data={"A": 1})
    exp_b = FakeResponse(json_data={"B": 2})
    ing, _ = make_ingestor(config, [base, exp_a, exp_b])
    ing.run_once("t", "prod")
    df = ing._last_df
    assert set(df.columns) >= {"A", "B"}
    assert df.iloc[0]["A"] == 1 and df.iloc[0]["B"] == 2


def test_run_once_success_logs(make_ingestor, logger):
    config = {
        "envs": {"prod": {"base_url": "http://api/"}},
        "apis": {
            "path": "things",
            "pagination": {"mode": "none"},
            "t": {
                "parse": {"type": "json", "json_record_path": "data"},
                "output": {"format": "csv", "s3": {"bucket": "b"}},
            },
        },
    }
    responses = [FakeResponse(json_data={"data": [{"x": 1}]})]
    ing, _ = make_ingestor(config, responses)
    ing.run_once("t", "prod")
    assert any("[run_once] done" in m for m in logger.infos)


def test_run_backfill_success_logs(make_ingestor, logger):
    config = {
        "envs": {"prod": {"base_url": "http://api/"}},
        "apis": {
            "path": "daily",
            "pagination": {"mode": "none"},
            "t": {
                "parse": {"type": "json", "json_record_path": "data"},
                "backfill": {
                    "enabled": True,
                    "strategy": "date",
                    "window_days": 1,
                },
                "output": {"format": "csv", "s3": {"bucket": "b"}},
            },
        },
    }
    responses = [
        FakeResponse(json_data={"data": [{"d": "A"}]}),
        FakeResponse(json_data={"data": [{"d": "B"}]}),
    ]
    ing, _ = make_ingestor(config, responses)
    ing.run_backfill("t", "prod", start=date(2024, 1, 1), end=date(2024, 1, 2))
    assert any("[run_backfill] done" in m for m in logger.infos)


def test_log_request_case_insensitive_redaction(make_ingestor, logger):
    """
    Ensure header/param names are treated case-insensitively and values are redacted.
    """
    config = {
        "envs": {"prod": {"base_url": "http://h/"}},
        "apis": {
            "path": "x",
            "pagination": {"mode": "none"},
            "request_defaults": {
                "headers": {
                    "authorization": "Bearer SSS",
                    "X-Api-Key": "k123",
                    "Other": "ok",
                },
            },
            "t": {
                "parse": {"type": "json"},
                "params": {"TOKEN": "shhh", "SAFE": "ok"},
            },
        },
    }
    responses = [FakeResponse(json_data={})]
    ing, _ = make_ingestor(config, responses)
    _ = ing.run_once("t", "prod")

    # find the request log line (not the final "done" line)
    req_line = next((m for m in logger.infos if m.startswith("GET http")), "")
    assert req_line, f"Request log not found. Infos: {logger.infos}"

    # redactions present
    assert "***REDACTED***" in req_line

    # secret values absent
    assert "Bearer SSS" not in req_line
    assert "k123" not in req_line
    assert "shhh" not in req_line

    # sanity: other non-sensitive values can still appear
    assert "SAFE" in req_line or "safe" in req_line
    assert "Other" in req_line


def test_expand_links_no_url_fields_returns_df_unchanged(make_ingestor):
    """
    Cover the branch where link_expansion has no url_fields -> function returns original df.
    """
    config = {
        "envs": {"prod": {"base_url": "http://api/"}},
        "apis": {
            "path": "base",
            "pagination": {"mode": "none"},
            "link_expansion": {"enabled": True, "url_fields": []},
            "t": {"parse": {"type": "json", "json_record_path": "rows"}},
        },
    }
    base = FakeResponse(json_data={"rows": [{"x": 1}]})
    ing, _ = make_ingestor(config, [base])
    ing.run_once("t", "prod")
    df = ing._last_df
    # unchanged columns
    assert list(df.columns) == ["x"]


def test_run_backfill_disabled_raises(make_ingestor):
    config = {
        "envs": {"prod": {"base_url": "http://api/"}},
        "apis": {
            "path": "x",
            "pagination": {"mode": "none"},
            "t": {"parse": {"type": "json"}},
        },
    }
    ing, _ = make_ingestor(config, [FakeResponse(json_data={})])
    with pytest.raises(ValueError):
        ing.run_backfill(
            "t", "prod", start=date(2024, 1, 1), end=date(2024, 1, 2)
        )


def test_run_backfill_date_respects_per_request_delay(
    monkeypatch, make_ingestor
):
    config = {
        "envs": {"prod": {"base_url": "http://api/"}},
        "apis": {
            "path": "daily",
            "pagination": {"mode": "none"},
            "t": {
                "parse": {"type": "json", "json_record_path": "data"},
                "backfill": {
                    "enabled": True,
                    "strategy": "date",
                    "window_days": 1,
                    "per_request_delay": 0.25,
                },
                "output": {"format": "csv", "s3": {"bucket": "b"}},
            },
        },
    }
    calls = {"n": 0}
    monkeypatch.setattr(
        time, "sleep", lambda s: calls.__setitem__("n", calls["n"] + 1)
    )
    ing, _ = make_ingestor(
        config,
        [
            FakeResponse(json_data={"data": [1]}),
            FakeResponse(json_data={"data": [2]}),
        ],
    )
    ing.run_backfill("t", "prod", start=date(2024, 1, 1), end=date(2024, 1, 2))
    assert calls["n"] == 2  # two windows -> sleep twice


def test_paginate_cursor_no_next_token_first_page(make_ingestor):
    config = {
        "envs": {"prod": {"base_url": "http://api/"}},
        "apis": {
            "path": "cur",
            "pagination": {
                "mode": "cursor",
                "cursor_param": "cursor",
                "next_cursor_path": "meta.next",
            },
            "t": {"parse": {"type": "json", "json_record_path": "data"}},
        },
    }
    responses = [
        FakeResponse(json_data={"data": [{"id": 1}], "meta": {"next": None}})
    ]
    ing, sess = make_ingestor(config, responses)
    meta = ing.run_once("t", "prod")
    assert meta["rows"] == 1
    # only one call since there was no next token
    assert len(sess.calls) == 1


def test_paginate_link_header_single_page(make_ingestor):
    config = {
        "envs": {"prod": {"base_url": "http://api/"}},
        "apis": {
            "path": "alpha",
            "pagination": {"mode": "link-header"},
            "t": {"parse": {"type": "json", "json_record_path": "data"}},
        },
    }
    responses = [FakeResponse(json_data={"data": [{"id": 1}]}, links={})]
    ing, sess = make_ingestor(config, responses)
    _ = ing.run_once("t", "prod")
    assert len(sess.calls) == 1  # no next link → no second call


def test_paginate_salesforce_done_immediate(make_ingestor):
    config = {
        "envs": {"prod": {"base_url": "https://sf.example/"}},
        "apis": {
            "path": "services/data/v61.0/query",
            "pagination": {
                "mode": "salesforce",
                "done_path": "done",
                "next_url_path": "nextRecordsUrl",
            },
            "t": {"parse": {"type": "json", "json_record_path": "records"}},
        },
    }
    responses = [FakeResponse(json_data={"done": True, "records": [{"Id": 1}]})]
    ing, sess = make_ingestor(config, responses)
    _ = ing.run_once("t", "prod")
    assert len(sess.calls) == 1


def test_paginate_unsupported_mode_raises(make_ingestor):
    config = {
        "envs": {"prod": {"base_url": "http://api/"}},
        "apis": {
            "path": "x",
            "pagination": {"mode": "weird"},
            "t": {"parse": {"type": "json"}},
        },
    }
    ing, _ = make_ingestor(config, [FakeResponse(json_data={})])
    with pytest.raises(ValueError):
        ing.run_once("t", "prod")


def test_build_session_no_retries_returns_session(logger):
    from utils.api_ingestor import ApiIngestor

    ing = ApiIngestor(
        config={"envs": {"e": {"base_url": "http://h"}}, "apis": {"t": {}}},
        log=logger,
    )
    s = ing._build_session(None)
    import requests

    assert isinstance(s, requests.Session)


def test_prepare_merging_and_overrides(logger):
    from utils.api_ingestor import ApiIngestor

    cfg = {
        "envs": {"prod": {"base_url": "http://h"}},
        "apis": {
            "request_defaults": {
                "headers": {"A": "1"},
                "params": {"p": "g"},
                "verify": True,
                "timeout": 10,
            },
            "path": "x",
            "t": {"headers": {"B": "2"}, "params": {"q": "t"}, "verify": False},
        },
    }
    ing = ApiIngestor(config=cfg, log=logger)
    env_cfg, api_cfg, req_opts, parse_cfg = ing._prepare("t", "prod")
    assert req_opts["headers"] == {"A": "1", "B": "2"}
    assert req_opts["params"] == {"p": "g", "q": "t"}
    assert req_opts["verify"] is False  # overridden
    assert api_cfg["path"] == "x"
    assert parse_cfg["type"] == "json"  # default


def test_expand_links_type_override_jsonl_and_delay(monkeypatch, make_ingestor):
    calls = {"sleep": 0}
    monkeypatch.setattr(
        time, "sleep", lambda s: calls.__setitem__("sleep", calls["sleep"] + 1)
    )
    ndjson = b'{"k":1}\n{"k":2}\n'
    config = {
        "envs": {"prod": {"base_url": "http://api/"}},
        "apis": {
            "path": "base",
            "pagination": {"mode": "none"},
            "link_expansion": {
                "enabled": True,
                "url_fields": ["u"],
                "type": "jsonl",
                "per_request_delay": 0.1,
            },
            "t": {"parse": {"type": "json", "json_record_path": "rows"}},
        },
    }
    base = FakeResponse(json_data={"rows": [{"u": "http://api/d"}]})
    expanded = FakeResponse(content=ndjson)
    ing, _ = make_ingestor(config, [base, expanded])
    ing.run_once("t", "prod")
    df = ing._last_df
    assert df["k"].tolist() == [1, 2]
    assert calls["sleep"] == 1


def test_expand_links_http_error_logs_and_raises(make_ingestor, logger):
    config = {
        "envs": {"prod": {"base_url": "http://api/"}},
        "apis": {
            "path": "base",
            "pagination": {"mode": "none"},
            "link_expansion": {"enabled": True, "url_fields": ["u"]},
            "t": {"parse": {"type": "json", "json_record_path": "rows"}},
        },
    }
    base = FakeResponse(json_data={"rows": [{"u": "http://api/fail"}]})
    error_resp = FakeResponse(status_code=500)  # will raise
    ing, _ = make_ingestor(config, [base, error_resp])
    with pytest.raises(Exception):
        ing.run_once("t", "prod")
    # run_once should have logged an exception with stack
    assert any("Stack Trace:" in e for e in logger.errors)


def test_write_s3_extra_args_and_headers(monkeypatch, logger):
    from utils.api_ingestor import ApiIngestor

    captured = {}

    class FakeS3Client:
        def put_object(self, **kwargs):
            captured.update(kwargs)

    class FakeSessionObj:
        def __init__(self, region_name=None):
            pass

        def client(self, name, endpoint_url=None):
            return FakeS3Client()

    class FakeBoto3:
        class session:
            Session = FakeSessionObj

    monkeypatch.setitem(os.sys.modules, "boto3", FakeBoto3)

    ing = ApiIngestor(
        config={"envs": {"e": {"base_url": "http://h"}}, "apis": {"t": {}}},
        log=logger,
    )
    df = pd.DataFrame({"x": [1]})
    _ = ing._write_s3(
        df,
        "t",
        "e",
        "csv",
        {
            "bucket": "bkt",
            "prefix": "p",
            "filename": "f.csv",
            "acl": "bucket-owner-full-control",
            "sse": "aws:kms",
            "sse_kms_key_id": "key123",
            "region_name": "us",
            "endpoint_url": "http://s3.local",
            "compression": "gzip",  # drive gzip ContentEncoding via _serialize_df
        },
    )
    # Extra args present
    assert captured["Bucket"] == "bkt" and captured["Key"].endswith("f.csv")
    assert captured["ACL"] == "bucket-owner-full-control"
    assert captured["ServerSideEncryption"] == "aws:kms"
    assert captured["SSEKMSKeyId"] == "key123"
    assert captured["ContentType"] == "text/csv"
    assert captured["ContentEncoding"] == "gzip"


def test_write_s3_import_failure_raises_runtimeerror(monkeypatch, logger):
    from utils.api_ingestor import ApiIngestor

    real_import = builtins.__import__

    def fake_import(name, *args, **kwargs):
        if name == "boto3":
            raise ImportError("no boto3")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fake_import)

    ing = ApiIngestor(
        config={"envs": {"e": {"base_url": "http://h"}}, "apis": {"t": {}}},
        log=logger,
    )
    with pytest.raises(RuntimeError):
        ing._write_s3(
            pd.DataFrame({"x": [1]}), "t", "e", "csv", {"bucket": "b"}
        )


def test_run_once_logs_link_expansion_line(make_ingestor, logger):
    config = {
        "envs": {"prod": {"base_url": "http://api/"}},
        "apis": {
            "path": "base",
            "pagination": {"mode": "none"},
            "link_expansion": {"enabled": True, "url_fields": ["u"]},
            "t": {"parse": {"type": "json", "json_record_path": "rows"}},
        },
    }
    base = FakeResponse(json_data={"rows": [{"u": "http://api/d"}]})
    exp = FakeResponse(json_data={"k": 1})
    ing, _ = make_ingestor(config, [base, exp])
    ing.run_once("t", "prod")
    assert any(
        m.startswith("[link_expansion] GET http://api/d") for m in logger.infos
    )


def test_prepare_includes_multi_pulls_and_join(logger):
    # Ensure _prepare passes through multi_pulls + join
    from utils.api_ingestor import ApiIngestor

    cfg = {
        "envs": {"prod": {"base_url": "http://h"}},
        "apis": {
            "t": {
                "multi_pulls": [{"name": "a"}, {"name": "b"}],
                "join": {"how": "left", "on": ["k"], "select_from": {}},
            }
        },
    }
    ing = ApiIngestor(config=cfg, log=logger)
    _, api_cfg, _, _ = ing._prepare("t", "prod")
    assert isinstance(api_cfg.get("multi_pulls"), list)
    assert isinstance(api_cfg.get("join"), dict)


def test_multi_pulls_left_join_keep_and_rename(make_ingestor):
    """
    Covers: _run_multi_pulls_and_join
      - two single GET pulls (no pagination)
      - left join on keys
      - select_from.keep and select_from.rename behaviors
      - join keys auto-preserved even if not in 'keep'
    """
    config = {
        "envs": {"prod": {"base_url": "http://api/"}},
        "apis": {
            "path": "default",  # unused because each pull overrides
            "t": {
                "multi_pulls": [
                    {
                        "name": "cost_core",
                        "path": "cost/v1/report",
                        "parse": {
                            "type": "json",
                            "json_record_path": "awsCostReport.awsCostList",
                        },
                        "params": {
                            "select": "asvName,accountName,costStartTimestamp,costEndTimestamp"
                        },
                    },
                    {
                        "name": "cost_dept",
                        "path": "cost/v1/report",
                        "parse": {
                            "type": "json",
                            "json_record_path": "awsCostReport.awsCostList",
                        },
                        "params": {
                            "select": "asvName,accountName,departmentId,costStartTimestamp,costEndTimestamp"
                        },
                    },
                ],
                "join": {
                    "how": "left",
                    "on": [
                        "asvName",
                        "accountName",
                        "costStartTimestamp",
                        "costEndTimestamp",
                    ],
                    "select_from": {
                        "cost_dept": {
                            "keep": [
                                "departmentId"
                            ],  # join keys must still appear
                            "rename": {
                                "departmentId": "departmentId"
                            },  # no-op rename path
                        }
                    },
                },
                # output present so run_once uses normal write path
                "output": {"format": "csv", "s3": {"bucket": "b"}},
            },
        },
    }

    # Pull #1 rows
    r1 = FakeResponse(
        json_data={
            "awsCostReport": {
                "awsCostList": [
                    {
                        "asvName": "A",
                        "accountName": "ACC",
                        "costStartTimestamp": "2024-01-01T00:00:00Z",
                        "costEndTimestamp": "2024-01-01T01:00:00Z",
                    }
                ]
            }
        }
    )
    # Pull #2 rows
    r2 = FakeResponse(
        json_data={
            "awsCostReport": {
                "awsCostList": [
                    {
                        "asvName": "A",
                        "accountName": "ACC",
                        "departmentId": "D1",
                        "costStartTimestamp": "2024-01-01T00:00:00Z",
                        "costEndTimestamp": "2024-01-01T01:00:00Z",
                    }
                ]
            }
        }
    )

    ing, sess = make_ingestor(config, [r1, r2])
    meta = ing.run_once("t", "prod")
    df = ing._last_df

    # Exactly two single GETs, one per pull (no pagination)
    assert len(sess.calls) == 2
    assert set(df.columns) >= {
        "asvName",
        "accountName",
        "costStartTimestamp",
        "costEndTimestamp",
        "departmentId",
    }
    assert df.loc[0, "departmentId"] == "D1"
    assert meta["rows"] == 1


def test_multi_pulls_respects_per_pull_overrides_and_parse(make_ingestor):
    """
    - First pull uses explicit per-pull parse (json_record_path=data) so 'id' is a column
    - Second pull overrides parse to CSV
    - Per-pull params override base params
    """
    config = {
        "envs": {"prod": {"base_url": "http://api/"}},
        "apis": {
            "request_defaults": {"params": {"base": "1"}},
            "t": {
                # Table-level parse remains as-is, but we don't rely on it being inherited
                "parse": {"type": "json", "json_record_path": "data"},
                "multi_pulls": [
                    {
                        "name": "a",
                        "path": "x",
                        "params": {"p": "A"},
                        # ✅ Explicit parse so the first pull flattens on 'data'
                        "parse": {"type": "json", "json_record_path": "data"},
                    },  # JSON at data
                    {
                        "name": "b",
                        "path": "y",
                        "parse": {"type": "csv"},  # CSV override
                    },
                ],
                "join": {
                    "how": "left",
                    "on": ["id"],
                    "select_from": {"b": {"keep": ["y"]}},
                },
                "output": {"format": "csv", "s3": {"bucket": "b"}},
            },
        },
    }
    # Pull a (JSON under data)
    r1 = FakeResponse(json_data={"data": [{"id": 1}]})
    # Pull b (CSV with id,y)
    r2 = FakeResponse(content=b"id,y\n1,9\n")
    ing, _ = make_ingestor(config, [r1, r2])

    _ = ing.run_once("t", "prod")
    df = ing._last_df

    # We should have joined on 'id', bringing in 'y' from pull 'b'
    assert "id" in df.columns and "y" in df.columns
    assert df["id"].tolist() == [1]
    assert df["y"].tolist() == [9]


def test_link_expansion_only_flush_skips_aggregate_write(monkeypatch, logger):
    """
    Ensures the 'only_flush' branch returns before the aggregate write.
    We detect this by checking that _write_output is never called.
    """
    from utils.api_ingestor import ApiIngestor

    # Fake boto3 to satisfy any internal imports if reached (it shouldn't)
    class FakeS3:
        class session:
            class Session:
                def __init__(self, region_name=None):
                    pass

                def client(self, name, endpoint_url=None):
                    class C:
                        def put_object(self, **kwargs):
                            pass

                    return C()

    monkeypatch.setitem(os.sys.modules, "boto3", FakeS3)

    calls = {"writes": 0}

    def _fake_write_output(self, df, table_name, env_name, out_cfg):
        calls["writes"] += 1
        return {
            "format": "csv",
            "s3_bucket": "b",
            "s3_key": "k",
            "s3_uri": "u",
            "bytes": 1,
        }

    config = {
        "envs": {"prod": {"base_url": "http://api/"}},
        "apis": {
            "path": "base",
            "pagination": {"mode": "none"},
            "link_expansion": {
                "enabled": True,
                "url_fields": ["u"],
                "flush": {
                    "mode": "per_link",
                    "only": True,
                    "prefix": "p",
                    "filename": "f-{seq}.csv",
                },
            },
            "t": {
                "parse": {"type": "json", "json_record_path": "rows"},
                "output": {"format": "csv", "s3": {"bucket": "b"}},
            },
        },
    }
    base = FakeResponse(json_data={"rows": [{"u": "http://api/d"}]})
    exp = FakeResponse(json_data={"k": 1})
    ing = ApiIngestor(config=config, log=logger)

    fs = FakeSession([base, exp])
    import utils.api_ingestor as api_mod

    monkeypatch.setattr(
        api_mod.ApiIngestor, "_build_session", lambda self, r: fs
    )
    monkeypatch.setattr(
        api_mod.ApiIngestor, "_write_output", _fake_write_output
    )

    meta = ing.run_once("t", "prod")
    assert meta["rows"] == 0
    assert calls["writes"] == 0  # aggregate write skipped


def test_stringify_non_scalars_for_parquet_and_resolve_session_id(logger):
    from utils.api_ingestor import ApiIngestor

    ing = ApiIngestor(
        config={"envs": {"e": {"base_url": "http://h"}}, "apis": {"t": {}}},
        log=logger,
    )
    df = pd.DataFrame(
        {
            "a": [1, {"x": 2}],
            "b": [[1, 2], "z"],
            "c": [None, 3],
        }
    )
    out = ing._stringify_non_scalars_for_parquet(df)
    # dict/list become JSON strings; scalars untouched
    assert isinstance(out.loc[1, "a"], str) and out.loc[0, "a"] == 1
    assert isinstance(out.loc[0, "b"], str) and out.loc[1, "b"] == "z"

    # session id policy coverage
    ing._expansion_session_ids = {"s1", "s2"}
    assert ing._resolve_session_id({"session_id": {"policy": "first"}}) in {
        "s1",
        "s2",
    }
    assert ing._resolve_session_id({"session_id": {"policy": "last"}}) in {
        "s1",
        "s2",
    }
    with pytest.raises(ValueError):
        ing._resolve_session_id({"session_id": {"policy": "require_single"}})
    # 'all' returns a single comma-joined string
    all_ids = ing._resolve_session_id({"session_id": {"policy": "all"}})
    assert set(all_ids.split(",")) == {"s1", "s2"}

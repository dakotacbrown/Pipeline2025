# test_api_ingestor.py
import json
import os
from datetime import date
from types import SimpleNamespace

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
    """
    from utils.api_ingestor import (  # <-- replace with your real module path
        ApiIngestor,
    )

    def _build(config, responses):
        fake = FakeSession(responses=responses)
        monkeypatch.setenv("TOKEN", "XYZ")
        monkeypatch.setattr(ApiIngestor, "_build_session", lambda self, r: fake)
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

    df = ing.run_once("my_table", "prod")
    assert df.empty
    # session defaults should be applied
    assert sess.headers.get("Authorization") == "Bearer XYZ"


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

    df = ing.run_once("t", "prod")
    assert list(df["id"]) == [1, 2]
    assert len(sess.calls) == 1
    assert sess.calls[0][0] == "http://api/things"


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

    df = ing.run_once("t", "prod")
    assert list(df["id"]) == [1, 2]
    # Two calls: initial + empty page
    assert len(sess.calls) == 2
    # page=2 should have been set in the second request
    assert sess.calls[1][1]["params"]["page"] == 2


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

    df = ing.run_once("t", "prod")
    assert list(df["id"]) == [1, 2]
    # First call should include page size limit
    assert sess.calls[0][1]["params"]["limit"] == 2
    # Second call should include cursor=abc
    assert sess.calls[1][1]["params"]["cursor"] == "abc"


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

    df = ing.run_once("t", "prod")
    assert list(df["id"]) == [1, 2]
    assert len(sess.calls) == 2
    assert sess.calls[1][0] == "http://api/alpha?page=2"


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

    df = ing.run_once("t", "prod")
    assert list(df["id"]) == [1, 2]
    # second call must have used absolute join of relative nextRecordsUrl
    assert sess.calls[1][0].endswith("/services/data/v61.0/query/nextA")


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

    df = ing.run_backfill(
        "t", "prod", start=date(2024, 1, 1), end=date(2024, 1, 3)
    )
    assert list(df["d"]) == ["A1", "B1"]
    # Inspect the params passed on each slice
    p0 = sess.calls[0][1]["params"]
    p1 = sess.calls[1][1]["params"]
    assert p0["start_date"] == "2024-01-01" and p0["end_date"] == "2024-01-02"
    assert p1["start_date"] == "2024-01-03" and p1["end_date"] == "2024-01-03"


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

    df = ing.run_backfill(
        "t", "prod", start=date(2024, 1, 1), end=date(2024, 1, 2)
    )
    assert list(df["Id"]) == ["1", "2"]

    # Verify 'q' param was injected on each slice, with half-open end date
    q0 = sess.calls[0][1]["params"]["q"]
    q1 = sess.calls[1][1]["params"]["q"]
    assert "2024-01-01T00:00:00Z" in q0 and "2024-01-02T00:00:00Z" in q0
    assert "2024-01-02T00:00:00Z" in q1 and "2024-01-03T00:00:00Z" in q1


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

    df = ing.run_backfill(
        "t", "prod", start=date(2024, 1, 1), end=date(2024, 1, 2)
    )
    assert list(df["i"]) == [1, 2]
    assert sess.calls[0][1]["params"]["cursor"] == "s0"
    assert sess.calls[1][1]["params"]["cursor"] == "s1"


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

    df = ing.run_backfill(
        "t", "prod", start=date(2024, 1, 1), end=date(2024, 1, 2)
    )
    assert list(df["id"]) == ["a"]  # trimmed before 'b' because inclusive=False


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

    df = ing.run_backfill(
        "t", "prod", start=date(2024, 1, 1), end=date(2024, 1, 2)
    )
    # only the >= cutoff rows are kept in the final (and loop stops)
    assert list(df["id"]) == [1]


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

    df = ing.run_once("t", "prod")

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

    ing.run_once("t", "prod")
    # Last info log should contain redactions
    assert any("***REDACTED***" in m for m in logger.infos)
    assert not any("supersecret" in m for m in logger.infos)
    assert not any("Bearer secret" in m for m in logger.infos)


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

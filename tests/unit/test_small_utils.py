import pandas as pd
import pytest

from api_ingestor.small_utils import (
    ALLOWED_REQUEST_KW,
    dig,
    get_from_row,
    whitelist_request_opts,
)

# ---------------- whitelist_request_opts ----------------


def test_whitelist_request_opts_keeps_only_allowed_and_handles_none():
    # includes some disallowed keys that should be dropped
    opts = {
        "headers": {"X": 1},
        "params": {"a": 1},
        "timeout": 5,
        "verify": False,
        "auth": ("u", "p"),
        "proxies": {"https": "x"},
        "stream": True,
        "allow_redirects": False,
        "retries": {"total": 3},  # not allowed
        "something_else": 123,  # not allowed
    }
    out = whitelist_request_opts(opts)
    assert set(out.keys()) == ALLOWED_REQUEST_KW - set()  # exactly allowed keys
    # None/empty safety
    assert whitelist_request_opts(None) == {}
    assert whitelist_request_opts({}) == {}


# ---------------- dig ----------------


def test_dig_none_or_empty_path_returns_none():
    assert dig({"a": 1}, None) is None
    assert dig({"a": 1}, "") is None


def test_dig_nested_success_and_missing_key():
    obj = {"a": {"b": {"c": 7}}}
    assert dig(obj, "a.b.c") == 7
    assert dig(obj, "a.b.x") is None


def test_dig_non_dict_midway_returns_none():
    obj = {"a": {"b": 3}}
    # at "b" we have an int, so going deeper should return None
    assert dig(obj, "a.b.c") is None


# ---------------- get_from_row ----------------


def test_get_from_row_direct_column_hit():
    row = pd.Series({"x": 10, "y": 20})
    assert get_from_row(row, "x") == 10
    assert get_from_row(row, "y") == 20


def test_get_from_row_nested_dict_path_success():
    row = pd.Series({"meta": {"id": "abc", "info": {"score": 99}}, "other": 1})
    assert get_from_row(row, "meta.id") == "abc"
    assert get_from_row(row, "meta.info.score") == 99


def test_get_from_row_nested_missing_or_wrong_type():
    row = pd.Series({"meta": {"id": "abc"}, "z": 3})
    # missing deeper key
    assert get_from_row(row, "meta.info.score") is None
    # root exists but is not a dict -> should return None
    assert get_from_row(pd.Series({"meta": "not-a-dict"}), "meta.id") is None
    # completely missing root
    assert get_from_row(row, "unknown.path") is None

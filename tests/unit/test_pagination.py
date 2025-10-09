import pandas as pd
import pytest

from api_ingestor import pagination

# --- Test doubles -------------------------------------------------------------


class FakeResponse:
    def __init__(self, *, json_data=None, links=None):
        self._json = json_data if json_data is not None else {}
        self.links = links or {}
        self.content = b"{}"

    def json(self):
        return self._json

    def raise_for_status(self):
        return None


class FakeSession:
    def __init__(self, url_map):
        # url_map: { url: [FakeResponse, ...], ... }
        self._m = {k: list(v) for k, v in (url_map or {}).items()}
        self.calls = []  # tuples (url, kwargs)

    def get(self, url, **kw):
        self.calls.append((url, kw))
        arr = self._m[url]
        return arr.pop(0)


# --- Fixtures ----------------------------------------------------------------


@pytest.fixture
def patch_to_dataframe(monkeypatch):
    # make {"items":[...]} -> DataFrame([...])
    monkeypatch.setattr(
        pagination,
        "to_dataframe",
        lambda resp, parse_cfg: pd.DataFrame(resp.json().get("items", [])),
        raising=True,
    )


@pytest.fixture
def patch_salesforce_helpers(monkeypatch):
    # simplify SF helpers
    monkeypatch.setattr(
        pagination,
        "drop_keys_any_depth",
        lambda o, keys: (
            {k: v for k, v in o.items() if k not in keys}
            if isinstance(o, dict)
            else o
        ),
        raising=True,
    )
    monkeypatch.setattr(
        pagination,
        "json_obj_to_df",
        lambda data_obj, parse_cfg: pd.DataFrame(data_obj.get("records", [])),
        raising=True,
    )


# --- Tests: mode = none -------------------------------------------------------


def test_paginate_mode_none(patch_to_dataframe):
    sess = FakeSession(
        {"https://api/x": [FakeResponse(json_data={"items": [{"a": 1}]})]}
    )
    df = pagination.paginate(
        sess,
        "https://api/x",
        {"params": {}},
        {"type": "json", "json_record_path": "items"},
        {"mode": "none"},
    )
    assert df.to_dict("records") == [{"a": 1}]
    assert sess.calls[0][0] == "https://api/x"


# --- Tests: mode = link-header ------------------------------------------------


def test_paginate_link_header(patch_to_dataframe):
    sess = FakeSession(
        {
            "https://api/x": [
                FakeResponse(
                    json_data={"items": [{"a": 1}]},
                    links={"next": {"url": "https://api/x?p=2"}},
                )
            ],
            "https://api/x?p=2": [
                FakeResponse(json_data={"items": [{"a": 2}]}, links={})
            ],
        }
    )
    df = pagination.paginate(
        sess, "https://api/x", {}, {"type": "json"}, {"mode": "link-header"}
    )
    assert df.to_dict("records") == [{"a": 1}, {"a": 2}]
    assert sess.calls[1][0] == "https://api/x?p=2"


# --- Tests: mode = cursor -----------------------------------------------------


def test_paginate_cursor_next_token(patch_to_dataframe):
    sess = FakeSession(
        {
            "https://api/cur": [
                FakeResponse(
                    json_data={"items": [{"i": 1}], "meta": {"next": "t1"}}
                ),
                FakeResponse(
                    json_data={"items": [{"i": 2}], "meta": {"next": None}}
                ),
            ]
        }
    )
    df = pagination.paginate(
        sess,
        "https://api/cur",
        {"params": {}},
        {"type": "json"},
        {
            "mode": "cursor",
            "cursor_param": "cursor",
            "next_cursor_path": "meta.next",
        },
    )
    assert df.to_dict("records") == [{"i": 1}, {"i": 2}]
    assert [c[0] for c in sess.calls] == ["https://api/cur", "https://api/cur"]
    assert sess.calls[1][1]["params"]["cursor"] == "t1"


def test_paginate_cursor_max_pages_cutoff(patch_to_dataframe):
    sess = FakeSession(
        {
            "https://api/cur": [
                FakeResponse(
                    json_data={"items": [{"i": 1}], "meta": {"next": "t1"}}
                ),
                FakeResponse(
                    json_data={"items": [{"i": 2}], "meta": {"next": None}}
                ),
            ]
        }
    )
    df = pagination.paginate(
        sess,
        "https://api/cur",
        {"params": {}},
        {"type": "json"},
        {"mode": "cursor", "next_cursor_path": "meta.next", "max_pages": 1},
    )
    assert df.to_dict("records") == [{"i": 1}]
    assert len(sess.calls) == 1


# --- Tests: mode = page -------------------------------------------------------


def test_paginate_page_increments_and_stops_on_empty(patch_to_dataframe):
    sess = FakeSession(
        {
            "https://api/p": [
                FakeResponse(json_data={"items": [{"p": 1}]}),
                FakeResponse(json_data={"items": []}),  # empty => stop
            ]
        }
    )
    df = pagination.paginate(
        sess,
        "https://api/p",
        {"params": {}},
        {"type": "json"},
        {
            "mode": "page",
            "page_param": "page",
            "start_page": 1,
            "page_size_param": "ps",
            "page_size_value": 50,
        },
    )
    assert df.to_dict("records") == [{"p": 1}]
    assert len(sess.calls) == 2
    assert sess.calls[0][1]["params"]["page"] == 1
    assert sess.calls[0][1]["params"]["ps"] == 50
    assert sess.calls[1][1]["params"]["page"] == 2


# --- Tests: mode = salesforce -------------------------------------------------


def test_paginate_salesforce_multiple_pages_clear_params(
    patch_salesforce_helpers,
):
    base = "https://sf.example/services/data/v61.0/query"
    next_rel = "/services/data/v61.0/query/next123"

    # Expect RFC3986 encoding: spaces %20, commas stay ',', quotes %27
    encoded_first = f"{base}?q=SELECT%20Id,%20Name%20FROM%20Account%20WHERE%20Name%20%3D%20%27Acme%27"

    sess = FakeSession(
        {
            encoded_first: [
                FakeResponse(
                    json_data={
                        "done": False,
                        "records": [{"Id": "1", "Name": "Acme"}],
                        "nextRecordsUrl": next_rel,
                    }
                )
            ],
            "https://sf.example/services/data/v61.0/query/next123": [
                FakeResponse(
                    json_data={
                        "done": True,
                        "records": [{"Id": "2", "Name": "Acme"}],
                    }
                )
            ],
        }
    )

    parse_cfg = {"type": "json", "json_drop_keys_any_depth": ["attributes"]}

    df = pagination.paginate(
        sess,
        base,
        {
            "params": {
                "q": "SELECT Id, Name FROM Account WHERE Name = 'Acme'",
                "ignored": "x",
            }
        },
        parse_cfg,
        {
            "mode": "salesforce",
            "done_path": "done",
            "next_url_path": "nextRecordsUrl",
            "clear_params_on_next": True,
        },
    )

    assert df.to_dict("records") == [
        {"Id": "1", "Name": "Acme"},
        {"Id": "2", "Name": "Acme"},
    ]
    # first call URL matches encoded string and remaining params exclude q
    assert sess.calls[0][0] == encoded_first
    assert "q" not in (sess.calls[0][1].get("params") or {})
    # second call follows nextRecordsUrl and has no params when clear_params_on_next=True
    assert (
        sess.calls[1][0]
        == "https://sf.example/services/data/v61.0/query/next123"
    )
    assert "params" not in sess.calls[1][1]


# --- Tests: unsupported mode --------------------------------------------------


def test_paginate_unsupported_mode_raises(patch_to_dataframe):
    sess = FakeSession(
        {"https://api/x": [FakeResponse(json_data={"items": []})]}
    )
    with pytest.raises(ValueError):
        pagination.paginate(
            sess, "https://api/x", {}, {"type": "json"}, {"mode": "weird"}
        )

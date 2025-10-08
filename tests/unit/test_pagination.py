from urllib.parse import quote

import pandas as pd
import pytest

from api_ingestor import pagination

# --- Test doubles -------------------------------------------------------------


class FakeResponse:
    def __init__(self, *, json_data=None, links=None):
        self._json = json_data if json_data is not None else {}
        self.links = links or {}
        self.content = b"{}"  # not used by our patches but present

    def json(self):
        return self._json

    def raise_for_status(self):
        return None


class FakeSession:
    def __init__(self, url_map):
        """
        url_map:
          {
            "https://api/x": [FakeResponse(...), FakeResponse(...), ...],
            "https://api/x?p=2": [FakeResponse(...)]
          }
        """
        self._m = {k: list(v) for k, v in (url_map or {}).items()}
        self.calls = []  # tuples of (url, kwargs)

    def get(self, url, **kw):
        self.calls.append((url, kw))
        arr = self._m[url]
        return arr.pop(0)


# --- Fixtures ----------------------------------------------------------------


@pytest.fixture
def patch_to_dataframe(monkeypatch):
    """Patch *pagination.to_dataframe* (not parsing) so JSON like {"items":[...]}
    becomes DataFrame([...])."""
    monkeypatch.setattr(
        pagination,
        "to_dataframe",
        lambda resp, parse_cfg: pd.DataFrame(resp.json().get("items", [])),
        raising=True,
    )


@pytest.fixture
def patch_salesforce_helpers(monkeypatch):
    """Patch *pagination.drop_keys_any_depth* and *pagination.json_obj_to_df* to
    simplify SF pagination behavior."""

    def fake_drop_keys_any_depth(obj, keys):
        if isinstance(obj, dict):
            return {k: v for k, v in obj.items() if k not in keys}
        return obj

    def fake_json_obj_to_df(data_obj, parse_cfg):
        return pd.DataFrame(data_obj.get("records", []))

    monkeypatch.setattr(
        pagination,
        "drop_keys_any_depth",
        fake_drop_keys_any_depth,
        raising=True,
    )
    monkeypatch.setattr(
        pagination, "json_obj_to_df", fake_json_obj_to_df, raising=True
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
    # called the same base URL twice; 2nd call had cursor param set
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
    # 1st page has rows, 2nd is empty => stop before attempting a 3rd call
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
    # Confirm we made exactly two calls and advanced page param
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

    # Build encoded q EXACTLY as the code under test does
    q_raw = "SELECT Id, Name FROM Account WHERE Name = 'Acme'"
    encoded_q = quote(q_raw, safe="*(),.:=<>-_'\"$")
    encoded_first = f"{base}?q={encoded_q}"

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
        # q supplied in params; paginate moves it into the URL and removes it from params
        {"params": {"q": q_raw, "ignored": "x"}},
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

    # First call uses encoded URL; q removed from params but others remain
    assert sess.calls[0][0] == encoded_first
    assert "q" not in sess.calls[0][1]["params"]
    assert sess.calls[0][1]["params"].get("ignored") == "x"

    # Second call follows nextRecordsUrl; params cleared
    assert (
        sess.calls[1][0]
        == "https://sf.example/services/data/v61.0/query/next123"
    )
    assert "params" not in sess.calls[1][1] or not sess.calls[1][1]["params"]


# --- Tests: unsupported mode --------------------------------------------------


def test_paginate_unsupported_mode_raises(patch_to_dataframe):
    sess = FakeSession(
        {"https://api/x": [FakeResponse(json_data={"items": []})]}
    )
    with pytest.raises(ValueError):
        pagination.paginate(
            sess, "https://api/x", {}, {"type": "json"}, {"mode": "weird"}
        )

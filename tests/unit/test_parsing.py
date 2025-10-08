import pytest

from api_ingestor import parsing

# -------------------- Test doubles --------------------


class FakeResponse:
    def __init__(self, *, json_data=None, text_data=None, bytes_data=None):
        """Use exactly one of json_data / text_data / bytes_data.

        - For CSV/JSONL paths, we feed .content (bytes).
        - For JSON path, we return json_data from .json().
        """
        self._json = json_data
        if text_data is not None and bytes_data is None:
            bytes_data = text_data.encode("utf-8")
        self.content = bytes_data or b""
        self._raised = False

    def json(self):
        if self._json is None:
            return {}
        return self._json

    def raise_for_status(self):
        return None


# -------------------- drop_keys_any_depth --------------------


def test_drop_keys_any_depth_dict_and_list():
    obj = {
        "keep": 1,
        "drop": 2,
        "nested": {
            "drop": 3,
            "keep2": [{"drop": 4, "x": 5}, {"x": 6}],
        },
        "list": [{"a": 1, "drop": 9}, {"a": 2}],
    }
    out = parsing.drop_keys_any_depth(obj, {"drop"})
    assert "drop" not in out
    assert "drop" not in out["nested"]
    assert "drop" not in out["nested"]["keep2"][0]
    assert out["nested"]["keep2"][0]["x"] == 5
    assert "drop" not in out["list"][0]


def test_drop_keys_any_depth_noop_non_container():
    assert parsing.drop_keys_any_depth("s", {"a"}) == "s"
    assert parsing.drop_keys_any_depth(3, {"a"}) == 3


# -------------------- json_obj_to_df --------------------


def test_json_obj_to_df_record_path_list():
    data = {"root": {"items": [{"a": 1}, {"a": 2}]}}
    df = parsing.json_obj_to_df(data, {"json_record_path": "root.items"})
    assert df.to_dict("records") == [{"a": 1}, {"a": 2}]


def test_json_obj_to_df_record_path_missing_becomes_empty_list():
    data = {"root": {}}
    df = parsing.json_obj_to_df(data, {"json_record_path": "root.items"})
    assert df.empty


def test_json_obj_to_df_non_list_normalize():
    data = {"a": 1, "b": {"c": 2}}
    df = parsing.json_obj_to_df(data, {"type": "json"})
    # pandas.json_normalize nests keys with dots
    rec = df.to_dict("records")[0]
    assert rec["a"] == 1 and rec["b.c"] == 2


# -------------------- to_dataframe(JSON) --------------------


def test_to_dataframe_json_with_drop_keys_any_depth():
    resp = FakeResponse(
        json_data={"keep": 1, "dropme": 2, "nested": {"dropme": 3, "x": 9}}
    )
    df = parsing.to_dataframe(
        resp, {"type": "json", "json_drop_keys_any_depth": ["dropme"]}
    )
    # after drop, json_normalize produces one record with 'keep' and 'nested.x'
    rec = df.to_dict("records")[0]
    assert "dropme" not in rec
    assert rec["keep"] == 1 and rec["nested.x"] == 9


def test_to_dataframe_json_default_type_is_json():
    resp = FakeResponse(json_data={"x": 1})
    df = parsing.to_dataframe(resp, {})
    assert df.to_dict("records")[0]["x"] == 1


# -------------------- to_dataframe(CSV) --------------------


def test_to_dataframe_csv_parses_rows():
    csv_text = "a,b\n1,2\n3,4\n"
    resp = FakeResponse(text_data=csv_text)
    df = parsing.to_dataframe(resp, {"type": "csv"})
    assert df.to_dict("records") == [{"a": 1, "b": 2}, {"a": 3, "b": 4}]


# -------------------- to_dataframe(JSONL/NDJSON) --------------------


@pytest.mark.parametrize("ptype", ["jsonl", "ndjson"])
def test_to_dataframe_jsonl_and_ndjson(ptype):
    text = '{"a":1}\n{"a":2}\n'
    resp = FakeResponse(text_data=text)
    df = parsing.to_dataframe(resp, {"type": ptype})
    assert df.to_dict("records") == [{"a": 1}, {"a": 2}]


@pytest.mark.parametrize("ptype", ["jsonl", "ndjson"])
def test_to_dataframe_jsonl_empty_returns_empty_df(ptype):
    resp = FakeResponse(text_data="\n   \n")
    df = parsing.to_dataframe(resp, {"type": ptype})
    assert df.empty


# -------------------- to_dataframe(unsupported) --------------------


def test_to_dataframe_unsupported_type_raises():
    with pytest.raises(ValueError):
        parsing.to_dataframe(FakeResponse(json_data={}), {"type": "xlsx"})

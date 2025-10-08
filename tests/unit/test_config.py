# tests/unit/test_config_extended.py
import pytest

from api_ingestor import config as cfg

# ---------- expand_env_value ----------


def test_expand_env_value_str_replaces_and_keeps_unknown(monkeypatch):
    monkeypatch.setenv("KNOWN", "ok")
    s = "x ${KNOWN} y ${UNKNOWN}"
    out = cfg.expand_env_value(s)
    # ${KNOWN} -> ok, ${UNKNOWN} left as-is
    assert out == "x ok y ${UNKNOWN}"


def test_expand_env_value_dict_and_list_recursive(monkeypatch):
    monkeypatch.setenv("A", "1")
    obj = {
        "k": "${A}",
        "inner": {"v": "${A}", "raw": "${NOPE}"},
        "arr": ["${A}", {"x": "${A}"}],
    }
    out = cfg.expand_env_value(obj)
    assert out["k"] == "1"
    assert out["inner"]["v"] == "1" and out["inner"]["raw"] == "${NOPE}"
    assert out["arr"][0] == "1" and out["arr"][1]["x"] == "1"


# ---------- prepare (happy path + precedence) ----------


def _base_config():
    return {
        "envs": {
            "prod": {"base_url": "https://api/"},
        },
        "apis": {
            # global request defaults and defaults
            "request_defaults": {
                "headers": {"h1": "g"},
                "params": {"p1": "g"},
                "timeout": 5,
                "verify": True,
            },
            "retries": {"total": 7},
            "pagination": {"mode": "cursor", "cursor_param": "c_global"},
            "link_expansion": {"enabled": False, "timeout": 9},
            "output": {"format": "csv"},
            # table config
            "tbl": {
                "path": "items",
                "headers": {"h2": "t"},
                "params": {"p2": "t"},
                "timeout": 10,  # override global
                "verify": False,  # override global
                "retries": {"total": 3},  # table > global > root
                "pagination": {"page_size_param": "ps", "mode": "cursor"},
                "link_expansion": {"enabled": True, "per_request_delay": 0.1},
                "parse": {"type": "json", "json_record_path": "items"},
                "multi_pulls": [{"name": "A", "path": "a"}],
                "join": {"how": "left", "on": ["id"]},
                "backfill": {"enabled": False},
                "output": {"s3": {"bucket": "b", "prefix": "p"}},
            },
        },
    }


def test_prepare_happy_path_and_merges(monkeypatch):
    config = _base_config()
    env_cfg, api_eff, req_opts, parse_cfg = cfg.prepare(config, "tbl", "prod")

    # env config
    assert env_cfg["base_url"] == "https://api/"

    # request defaults merged: global + table overrides for headers/params
    assert req_opts["headers"] == {"h1": "g", "h2": "t"}
    assert req_opts["params"] == {"p1": "g", "p2": "t"}
    assert req_opts["timeout"] == 10
    assert req_opts["verify"] is False

    # retries precedence: table > request_defaults > apis.retries
    assert req_opts["retries"] == {"total": 3}

    # api effective: path comes from table, pagination/link_expansion merged
    assert api_eff["path"] == "items"
    # pagination: table overrides mode (keeps global additions where not overridden)
    assert api_eff["pagination"]["mode"] == "cursor"
    assert api_eff["pagination"]["cursor_param"] == "c_global"
    assert api_eff["pagination"]["page_size_param"] == "ps"
    # link_expansion merged and table toggles enabled
    assert api_eff["link_expansion"]["enabled"] is True
    assert api_eff["link_expansion"]["timeout"] == 9
    assert api_eff["link_expansion"]["per_request_delay"] == 0.1

    # output merged: table adds s3; format inherited from apis.output
    assert api_eff["output"]["format"] == "csv"
    assert api_eff["output"]["s3"]["bucket"] == "b"

    # passthroughs
    assert isinstance(api_eff["multi_pulls"], list)
    assert api_eff["join"]["how"] == "left"

    # parse_cfg default from table
    assert parse_cfg == {"type": "json", "json_record_path": "items"}


def test_prepare_parse_default_when_missing():
    config = _base_config()
    # remove table parse -> default {"type":"json"}
    del config["apis"]["tbl"]["parse"]
    env_cfg, api_eff, req_opts, parse_cfg = cfg.prepare(config, "tbl", "prod")
    assert parse_cfg == {"type": "json"}


def test_prepare_pagination_precedence_when_only_root():
    config = _base_config()
    # remove pagination from table; should inherit root
    del config["apis"]["tbl"]["pagination"]
    env_cfg, api_eff, req_opts, parse_cfg = cfg.prepare(config, "tbl", "prod")
    assert api_eff["pagination"]["mode"] == "cursor"
    assert api_eff["pagination"]["cursor_param"] == "c_global"
    assert "page_size_param" not in api_eff["pagination"]


def test_prepare_link_expansion_precedence_when_only_root():
    config = _base_config()
    # remove link_expansion from table; inherit disabled root config
    del config["apis"]["tbl"]["link_expansion"]
    env_cfg, api_eff, req_opts, parse_cfg = cfg.prepare(config, "tbl", "prod")
    assert api_eff["link_expansion"]["enabled"] is False
    assert api_eff["link_expansion"]["timeout"] == 9


def test_prepare_env_var_expansion_in_all_layers(monkeypatch):
    config = _base_config()
    # inject env vars into headers and prefix
    config["apis"]["request_defaults"]["headers"]["auth"] = "Bearer ${TOKEN}"
    config["apis"]["tbl"]["output"]["s3"]["prefix"] = "p/${ENV_NAME}"
    monkeypatch.setenv("TOKEN", "t0k")
    monkeypatch.setenv("ENV_NAME", "prodX")
    env_cfg, api_eff, req_opts, parse_cfg = cfg.prepare(config, "tbl", "prod")
    # headers expanded
    assert req_opts["headers"]["auth"] == "Bearer t0k"
    # output s3 prefix expanded
    assert api_eff["output"]["s3"]["prefix"] == "p/prodX"


# ---------- prepare (error paths) ----------


def test_prepare_missing_base_url_raises():
    config = _base_config()
    # nuke base_url
    del config["envs"]["prod"]["base_url"]
    with pytest.raises(ValueError):
        cfg.prepare(config, "tbl", "prod")


def test_prepare_missing_table_raises():
    config = _base_config()
    with pytest.raises(KeyError):
        cfg.prepare(config, "nope", "prod")


def test_prepare_retries_precedence_global_when_table_missing():
    config = _base_config()
    # Remove table retries; request_defaults has none; root has 7 -> still placed into req_opts["retries"]
    del config["apis"]["tbl"]["retries"]
    env_cfg, api_eff, req_opts, parse_cfg = cfg.prepare(config, "tbl", "prod")
    assert req_opts["retries"] == {"total": 7}

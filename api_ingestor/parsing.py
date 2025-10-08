from io import StringIO
from typing import Any, Dict

import pandas as pd
import requests


def drop_keys_any_depth(obj, keys: set):
    if isinstance(obj, dict):
        return {
            k: drop_keys_any_depth(v, keys)
            for k, v in obj.items()
            if k not in keys
        }
    if isinstance(obj, list):
        return [drop_keys_any_depth(v, keys) for v in obj]
    return obj


def json_obj_to_df(data_obj: Any, parse_cfg: Dict[str, Any]) -> pd.DataFrame:
    record_path = parse_cfg.get("json_record_path")
    data = data_obj
    if record_path:
        for key in record_path.split("."):
            data = (data or {}).get(key, [])
    if isinstance(data, list):
        return pd.DataFrame(data)
    return pd.json_normalize(data)


def to_dataframe(
    resp: requests.Response, parse_cfg: Dict[str, Any]
) -> pd.DataFrame:
    ptype = (parse_cfg.get("type") or "json").lower()
    if ptype == "csv":
        csv_string = resp.content.decode("utf-8", errors="replace")
        return pd.read_csv(StringIO(csv_string))
    if ptype in {"jsonl", "ndjson"}:
        text = resp.content.decode("utf-8", errors="replace")
        if not text.strip():
            return pd.DataFrame()
        return pd.read_json(StringIO(text), lines=True)
    if ptype == "json":
        data = resp.json()
        drop = set(parse_cfg.get("json_drop_keys_any_depth", []))
        if drop:
            data = drop_keys_any_depth(data, drop)
        return json_obj_to_df(data, parse_cfg)
    raise ValueError(f"Unsupported parse.type: {ptype}")

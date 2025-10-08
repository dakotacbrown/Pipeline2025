from typing import Any, Dict, Optional

import pandas as pd

ALLOWED_REQUEST_KW = {
    "headers",
    "params",
    "timeout",
    "verify",
    "auth",
    "proxies",
    "stream",
    "allow_redirects",
}


def whitelist_request_opts(opts: Dict[str, Any]) -> Dict[str, Any]:
    return {k: v for k, v in (opts or {}).items() if k in ALLOWED_REQUEST_KW}


def dig(obj: Any, path: Optional[str]):
    if not path:
        return None
    cur = obj
    for key in path.split("."):
        if not isinstance(cur, dict):
            return None
        cur = cur.get(key)
        if cur is None:
            return None
    return cur


def get_from_row(row: pd.Series, path: str):
    if path in row.index:
        return row[path]
    root = path.split(".")[0]
    if root in row.index and isinstance(row[root], dict):
        cur = row[root]
        for k in path.split(".")[1:]:
            if not isinstance(cur, dict):
                return None
            cur = cur.get(k)
        return cur
    return None

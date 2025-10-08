from typing import Any, Dict, List

import pandas as pd
from requests import Session

from api_ingestor.pagination import paginate
from api_ingestor.request_helpers import build_url, log_request
from api_ingestor.small_utils import whitelist_request_opts


def run_multi_pulls_with_join(
    ctx: Dict[str, Any],
    sess: Session,
    base_url: str,
    api_cfg: Dict[str, Any],
    req_opts: Dict[str, Any],
) -> pd.DataFrame:
    pulls = api_cfg.get("multi_pulls") or []
    if not pulls:
        raise ValueError(
            "multi_pulls is empty but run_multi_pulls_with_join was called"
        )

    dfs: Dict[str, pd.DataFrame] = {}
    for pull in pulls:
        name = pull.get("name") or "pull"
        path = pull.get("path", api_cfg.get("path", ""))
        url = build_url(base_url, path)

        pull_opts = dict(req_opts)
        if "headers" in pull:
            base_headers = dict(pull_opts.get("headers") or {})
            pull_opts["headers"] = {
                **base_headers,
                **(pull.get("headers") or {}),
            }

        base_params = dict(pull_opts.get("params") or {})
        pull_params = dict(pull.get("params") or {})
        if base_params or pull_params:
            pull_opts["params"] = {**base_params, **pull_params}

        safe = whitelist_request_opts(pull_opts)
        parse_cfg = pull.get("parse") or {"type": "json"}

        log_request(ctx, url, safe, prefix=f"[multi:{name}] ")
        df = paginate(sess, url, safe, parse_cfg, pag_cfg=None)
        dfs[name] = df

    join_cfg = api_cfg.get("join") or {}
    if not join_cfg:
        first = pulls[0].get("name") if pulls else None
        return (
            dfs.get(first)
            if first in dfs
            else next(iter(dfs.values()), pd.DataFrame())
        )

    how = (join_cfg.get("how") or "left").lower()
    on_cols_cfg = list(join_cfg.get("on") or [])
    case_mode = (
        join_cfg.get("case") or "exact"
    ).lower()  # exact|lower|upper|ignore
    select_from = join_cfg.get("select_from") or {}

    if len(pulls) < 2:
        first = pulls[0].get("name")
        return dfs.get(first, pd.DataFrame())

    left_name = pulls[0]["name"]
    left_df = dfs[left_name].copy()

    def _normalize(df: pd.DataFrame, mode: str) -> pd.DataFrame:
        if mode == "lower":
            return df.rename(columns={c: c.lower() for c in df.columns})
        if mode == "upper":
            return df.rename(columns={c: c.upper() for c in df.columns})
        return df  # exact/ignore

    def _resolve_keys(
        df: pd.DataFrame, expected_keys: List[str], mode: str
    ) -> Dict[str, str]:
        """Return mapping expected_key -> actual_column in df."""
        if mode != "ignore":
            return {k: k for k in expected_keys}
        cols = list(df.columns)
        lower_map = {c.lower(): c for c in cols}
        out = {}
        for k in expected_keys:
            out[k] = lower_map.get(k.lower(), k)  # may be missing
        return out

    # Prepare left keys + normalization
    if case_mode in ("lower", "upper"):
        left_df = _normalize(left_df, case_mode)
        on_cols = [
            c.lower() if case_mode == "lower" else c.upper()
            for c in on_cols_cfg
        ]
    elif case_mode == "ignore":
        on_cols = on_cols_cfg[:]
    else:
        on_cols = on_cols_cfg[:]

    if case_mode == "ignore":
        lmap = _resolve_keys(left_df, on_cols, "ignore")
        for exp, actual in lmap.items():
            if actual not in left_df.columns:
                left_df[exp] = pd.NA
        if any(lmap[k] != k for k in on_cols):
            left_df = left_df.rename(
                columns={
                    lmap[k]: k for k in on_cols if lmap[k] in left_df.columns
                }
            )
    else:
        for k in on_cols:
            if k not in left_df.columns:
                left_df[k] = pd.NA

    # Join subsequent pulls
    for pull in pulls[1:]:
        rname = pull["name"]
        right_df = dfs[rname].copy()

        if case_mode in ("lower", "upper"):
            right_df = _normalize(right_df, case_mode)

        if case_mode == "ignore":
            rmap = _resolve_keys(right_df, on_cols, "ignore")
            for exp, actual in rmap.items():
                if actual not in right_df.columns:
                    right_df[exp] = pd.NA
            rename_for_keys = {
                rmap[k]: k
                for k in on_cols
                if rmap[k] in right_df.columns and rmap[k] != k
            }
            if rename_for_keys:
                right_df = right_df.rename(columns=rename_for_keys)
        else:
            for k in on_cols:
                if k not in right_df.columns:
                    right_df[k] = pd.NA

        sel_cfg = select_from.get(rname, {}) or {}
        keep_cols = sel_cfg.get("keep")
        rename_map = sel_cfg.get("rename") or {}

        if keep_cols:
            needed = set(keep_cols) | set(on_cols)
            right_df = right_df[[c for c in right_df.columns if c in needed]]

        if rename_map:
            right_df = right_df.rename(columns=rename_map)

        left_df = left_df.merge(right_df, how=how, on=on_cols)

    return left_df

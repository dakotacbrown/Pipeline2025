import re
import time
from typing import Any, Dict, List, Optional

import pandas as pd
from requests import Session

from api_ingestor.parsing import to_dataframe
from api_ingestor.small_utils import get_from_row


def resolve_session_id(
    expansion_session_ids: set, link_cfg: Dict[str, Any]
) -> Optional[str]:
    if not expansion_session_ids:
        return None
    policy = ((link_cfg or {}).get("session_id") or {}).get("policy", "first")
    ids = list(expansion_session_ids)
    if policy == "last":
        return ids[-1]
    if policy == "all":
        return ",".join(sorted(set(ids)))
    if policy == "require_single":
        if len(set(ids)) != 1:
            raise ValueError(f"Multiple session ids found: {sorted(set(ids))}")
        return ids[0]
    return ids[0]


def _flush_part(
    ctx: Dict[str, Any],
    df: pd.DataFrame,
    out_cfg: Dict[str, Any],
    session_id: Optional[str],
    flush_cfg: Dict[str, Any],
) -> Dict[str, Any]:
    ctx["flush_seq"] = ctx.get("flush_seq", 0) + 1
    seq = ctx["flush_seq"]
    s3_cfg = dict((out_cfg.get("s3") or {}).copy())
    if (flush_cfg or {}).get("prefix"):
        s3_cfg["prefix"] = flush_cfg["prefix"]
    if (flush_cfg or {}).get("filename"):
        s3_cfg["filename"] = flush_cfg["filename"]
    if session_id:
        ctx["current_output_ctx"]["session_id"] = session_id
    ctx["current_output_ctx"]["seq"] = seq
    try:
        meta = ctx["write_s3"](
            df, (out_cfg.get("format") or "csv").lower(), s3_cfg
        )
        ctx["log"].info(
            f"[flush] wrote part seq={seq} rows={len(df)} uri={meta.get('s3_uri')}"
        )
        return meta
    finally:
        ctx["current_output_ctx"].pop("seq", None)
        ctx["current_output_ctx"].pop("session_id", None)


def expand_links(
    ctx: Dict[str, Any],
    sess: Session,
    df: pd.DataFrame,
    link_cfg: Dict[str, Any],
    parse_default: Dict[str, Any],
    *,
    table_name: Optional[str] = None,
    env_name: Optional[str] = None,
    api_output_cfg: Optional[Dict[str, Any]] = None,
) -> pd.DataFrame:
    if df.empty:
        return df
    url_fields = (link_cfg or {}).get("url_fields", [])
    if not url_fields:
        return df

    le_timeout = (link_cfg or {}).get(
        "timeout", parse_default.get("timeout", None)
    )
    flush_cfg = (link_cfg or {}).get("flush") or {}
    flush_mode = (flush_cfg.get("mode") or "none").lower()
    per_delay = float((link_cfg or {}).get("per_request_delay", 0.0))

    sid_cfg = (link_cfg or {}).get("session_id") or {}
    sid_regex = sid_cfg.get("regex")
    sid_group = int(sid_cfg.get("group", 0))

    expanded_frames: List[pd.DataFrame] = []
    per_session_parts: Dict[str, List[pd.DataFrame]] = {}

    for _, row in df.iterrows():
        urls: List[str] = []
        for fld in url_fields:
            val = get_from_row(row, fld)
            if isinstance(val, str) and val.startswith(("http://", "https://")):
                urls.append(val)

        row_parts: List[pd.DataFrame] = []
        for u in urls:
            resp = sess.get(u, timeout=le_timeout or 30)
            ctx["log"].info(
                f"[link_expansion] GET {u} -> bytes={len(resp.content)}"
            )
            resp.raise_for_status()

            session_id: Optional[str] = None
            if sid_regex:
                m = re.search(sid_regex, u)
                if m:
                    try:
                        session_id = str(m.group(sid_group) or "").strip()
                    except IndexError:
                        session_id = None
            if session_id:
                ctx["expansion_session_ids"].add(session_id)

            parse_override = dict(parse_default)
            if "type" in (link_cfg or {}):
                parse_override["type"] = link_cfg["type"]
            if parse_override.get("type", "json") == "json":
                if "json_record_path" in (link_cfg or {}):
                    if link_cfg.get("json_record_path") is None:
                        parse_override.pop("json_record_path", None)
                    else:
                        parse_override["json_record_path"] = link_cfg[
                            "json_record_path"
                        ]
                else:
                    parse_override.pop("json_record_path", None)

            df_part = to_dataframe(resp, parse_override)

            if (
                flush_mode == "per_link"
                and table_name
                and env_name
                and api_output_cfg
            ):
                _flush_part(ctx, df_part, api_output_cfg, session_id, flush_cfg)
            elif (
                flush_mode == "per_session"
                and table_name
                and env_name
                and api_output_cfg
            ):
                per_session_parts.setdefault(session_id or "none", []).append(
                    df_part
                )
            else:
                row_parts.append(df_part)

            if per_delay > 0:
                time.sleep(per_delay)

        if row_parts:
            merged = row_parts[0]
            for f in row_parts[1:]:
                merged = merged.merge(
                    f, left_index=True, right_index=True, how="outer"
                )
            expanded_frames.append(merged)

    if (
        flush_mode == "per_session"
        and table_name
        and env_name
        and api_output_cfg is not None
    ):
        for sid, parts in per_session_parts.items():
            if not parts:
                continue
            df_session = pd.concat(parts, ignore_index=True)
            _flush_part(
                ctx,
                df_session,
                api_output_cfg,
                (None if sid == "none" else sid),
                flush_cfg,
            )
            expanded_frames.append(df_session)

    return (
        pd.concat(expanded_frames, ignore_index=True) if expanded_frames else df
    )

import time
from datetime import date
from typing import Any, Dict, List

import pandas as pd
from requests import Session

from api_ingestor.link_expansion import expand_links
from api_ingestor.pagination import paginate
from api_ingestor.parsing import to_dataframe
from api_ingestor.small_utils import dig, whitelist_request_opts


def cursor_backfill(
    ctx: Dict[str, Any],
    sess: Session,
    url: str,
    base_opts: Dict[str, Any],
    parse_cfg: Dict[str, Any],
    pag_cfg: Dict[str, Any],
    cur_cfg: Dict[str, Any],
    link_cfg: Dict[str, Any],
) -> pd.DataFrame:
    """
    Cursor backfill that:
      - Seeds the initial request with cur_cfg.start_value (if provided)
        into params[cursor_param].
      - Follows pag_cfg.next_cursor_path to advance.
      - Respects pag_cfg.max_pages.
      - Applies optional stop guards (stop_at_item, stop_when_older_than).
      - Performs link expansion ONCE after the final DataFrame is built
        (to match run_once parity and avoid N*page expansion).
    """
    safe = whitelist_request_opts(dict(base_opts))
    frames: List[pd.DataFrame] = []

    cursor_param = (pag_cfg or {}).get("cursor_param", "cursor")

    next_cursor_path = (pag_cfg or {}).get("next_cursor_path")
    max_pages = int((pag_cfg or {}).get("max_pages", 10000))

    # stop guards
    stop_item = (cur_cfg or {}).get("stop_at_item") or {}
    stop_field = stop_item.get("field")
    stop_value = stop_item.get("value")
    stop_inclusive = bool(stop_item.get("inclusive", False))

    stop_time_cfg = (cur_cfg or {}).get("stop_when_older_than") or {}
    stop_time_field = stop_time_cfg.get("field")
    stop_time_value = stop_time_cfg.get("value")
    stop_dt = (
        pd.to_datetime(stop_time_value, utc=True)
        if (stop_time_field and stop_time_value)
        else None
    )

    pages = 0
    next_url = url

    while pages < max_pages and next_url:
        resp = sess.get(next_url, **safe)
        resp.raise_for_status()
        df_page = to_dataframe(resp, parse_cfg)

        # stop-by-item
        if (
            stop_field
            and stop_value is not None
            and not df_page.empty
            and stop_field in df_page.columns
        ):
            mask = df_page[stop_field] == stop_value
            if mask.any():
                idx = mask.idxmax()
                df_page = (
                    df_page.loc[:idx]
                    if stop_inclusive
                    else df_page.loc[:idx].iloc[:-1]
                )
                if not df_page.empty:
                    frames.append(df_page)
                break

        # stop-by-time
        if (
            stop_dt is not None
            and not df_page.empty
            and stop_time_field in df_page.columns
        ):
            ts = pd.to_datetime(
                df_page[stop_time_field], errors="coerce", utc=True
            )
            keep_mask = ts >= stop_dt
            if not keep_mask.all():
                trimmed = df_page[keep_mask]
                if not trimmed.empty:
                    frames.append(trimmed)
                break

        if not df_page.empty:
            frames.append(df_page)

        # advance cursor strictly via next_cursor_path
        if next_cursor_path:
            token = dig(resp.json(), next_cursor_path)
            if token:
                params = dict(safe.get("params") or {})
                params[cursor_param] = token
                safe["params"] = params
                next_url = url
            else:
                next_url = None
        else:
            # No way to continue
            next_url = None

        pages += 1

    result = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

    # single-shot expansion (matches run_once)
    if link_cfg.get("enabled", False) and not result.empty:
        result = expand_links(
            ctx,
            sess,
            result,
            link_cfg,
            parse_cfg,
            table_name=None,
            env_name=None,
            api_output_cfg=None,
        )
    return result


def soql_window_backfill(
    ctx: Dict[str, Any],
    sess: Session,
    url: str,
    base_opts: Dict[str, Any],
    parse_cfg: Dict[str, Any],
    pag_cfg: Dict[str, Any],
    bf_cfg: Dict[str, Any],
    start: date,
    end: date,
    link_cfg: Dict[str, Any],
) -> pd.DataFrame:
    window_days = int(bf_cfg.get("window_days", 7))
    per_request_delay = float(bf_cfg.get("per_request_delay", 0.0))
    date_field = bf_cfg.get("date_field", "LastModifiedDate")
    date_format = bf_cfg.get("date_format", "%Y-%m-%dT%H:%M:%SZ")
    soql_template = bf_cfg.get("soql_template")
    if not soql_template:
        raise ValueError(
            "backfill.strategy=soql_window requires 'soql_template' in config."
        )

    frames: List[pd.DataFrame] = []
    current = start

    while current <= end:
        window_end = min(
            current + pd.Timedelta(days=window_days), end + pd.Timedelta(days=1)
        )
        start_str = pd.Timestamp(current).strftime(date_format)
        end_str = pd.Timestamp(window_end).strftime(date_format)

        soql = soql_template.format(
            date_field=date_field, start=start_str, end=end_str
        )

        req_opts = dict(base_opts)
        params = dict(req_opts.get("params") or {})
        params["q"] = soql
        req_opts["params"] = params

        safe = whitelist_request_opts(req_opts)
        ctx["log"].info(f"[{start_str} → {end_str}) GET {url}")

        df = paginate(sess, url, safe, parse_cfg, pag_cfg)

        if link_cfg.get("enabled", False) and not df.empty:
            df = expand_links(
                ctx,
                sess,
                df,
                link_cfg,
                parse_cfg,
                table_name=None,
                env_name=None,
                api_output_cfg=None,
            )

        frames.append(df)

        if per_request_delay > 0:
            time.sleep(per_request_delay)

        current = (pd.Timestamp(window_end)).to_pydatetime().date()

    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

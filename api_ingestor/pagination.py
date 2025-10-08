from typing import Any, Dict, List, Optional
from urllib.parse import urljoin

import pandas as pd
from requests import Session

from api_ingestor.parsing import (
    drop_keys_any_depth,
    json_obj_to_df,
    to_dataframe,
)
from api_ingestor.small_utils import dig


def paginate(
    sess: Session,
    url: str,
    base_opts: Dict[str, Any],
    parse_cfg: Dict[str, Any],
    pag_cfg: Optional[Dict[str, Any]],
) -> pd.DataFrame:
    mode = (pag_cfg or {}).get("mode", "none")
    frames: List[pd.DataFrame] = []

    if mode == "none":
        resp = sess.get(url, **base_opts)
        resp.raise_for_status()
        return to_dataframe(resp, parse_cfg)

    if mode == "salesforce":
        safe = dict(base_opts)
        host_base = urljoin(url, "/")
        done_path = (pag_cfg or {}).get("done_path", "done")
        next_url_path = (pag_cfg or {}).get("next_url_path", "nextRecordsUrl")
        clear_params_on_next = bool(
            (pag_cfg or {}).get("clear_params_on_next", True)
        )
        max_pages = int((pag_cfg or {}).get("max_pages", 10000))
        pages = 0
        next_url = url
        while pages < max_pages and next_url:
            resp = sess.get(next_url, **safe)
            resp.raise_for_status()
            data = resp.json()
            drop = set(parse_cfg.get("json_drop_keys_any_depth", []))
            if drop:
                data = drop_keys_any_depth(data, drop)
            dfp = json_obj_to_df(data, parse_cfg)
            if not dfp.empty:
                frames.append(dfp)
            if (data.get(done_path, True)) is True:
                break
            nxt = data.get(next_url_path)
            if not nxt:
                break
            next_url = urljoin(host_base, nxt)
            if clear_params_on_next and "params" in safe:
                safe = dict(safe)
                safe.pop("params", None)
            pages += 1
        return (
            pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
        )

    # cursor/page/link-header
    safe = dict(base_opts)
    if mode in {"cursor", "page"}:
        params = dict(safe.get("params") or {})
        ps_param = pag_cfg.get("page_size_param") if pag_cfg else None
        ps_value = pag_cfg.get("page_size_value") if pag_cfg else None
        if ps_param and ps_value:
            params[ps_param] = ps_value
        if mode == "page":
            page_param = (pag_cfg or {}).get("page_param", "page")
            start_page = int((pag_cfg or {}).get("start_page", 1))
            params.setdefault(page_param, start_page)
        if params:
            safe["params"] = params

    max_pages = int((pag_cfg or {}).get("max_pages", 10000))
    pages = 0
    next_url = url

    while pages < max_pages and next_url:
        resp = sess.get(next_url, **safe)
        resp.raise_for_status()
        page_df = to_dataframe(resp, parse_cfg)
        if mode == "page" and page_df.empty:
            break
        frames.append(page_df)
        pages += 1

        if mode == "cursor":
            next_cursor = dig(
                resp.json(), (pag_cfg or {}).get("next_cursor_path")
            )
            if next_cursor:
                params = dict(safe.get("params") or {})
                params[(pag_cfg or {}).get("cursor_param", "cursor")] = (
                    next_cursor
                )
                safe["params"] = params
                next_url = url
            else:
                next_url = None
        elif mode == "page":
            page_param = (pag_cfg or {}).get("page_param", "page")
            start_page = int((pag_cfg or {}).get("start_page", 1))
            current_page = int(
                (safe.get("params") or {}).get(page_param, start_page)
            )
            current_page += 1
            params = dict(safe.get("params") or {})
            params[page_param] = current_page
            safe["params"] = params
        elif mode == "link-header":
            next_url = resp.links.get("next", {}).get("url")
        else:
            raise ValueError(f"Unsupported pagination mode: {mode}")

    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

from typing import Any, Dict, List, Optional
from urllib.parse import quote, urljoin

import pandas as pd
from requests import Session

from api_ingestor.parsing import (
    drop_keys_any_depth,
    json_obj_to_df,
    to_dataframe,
)
from api_ingestor.small_utils import dig, whitelist_request_opts


def _encode_soql_for_q(soql: str) -> str:
    """
    Encode SOQL for the query component exactly like Insomnia's URL preview:
    - spaces -> %20 (NOT '+')
    - commas remain ','
    - RFC3986 query-component encoding for everything else
    """
    soql = soql.replace("z", "Z")
    return quote(soql, safe="()*._-,")


def paginate(
    sess: Session,
    url: str,
    base_opts: Dict[str, Any],
    parse_cfg: Dict[str, Any],
    pag_cfg: Optional[Dict[str, Any]],
) -> pd.DataFrame:
    """
    Unified paginator for modes: none, salesforce, cursor, page, link-header.
    """
    mode = (pag_cfg or {}).get("mode", "none")
    frames: List[pd.DataFrame] = []

    # ---------- No pagination ----------
    if mode == "none":
        resp = sess.get(url, **whitelist_request_opts(base_opts))
        resp.raise_for_status()
        return to_dataframe(resp, parse_cfg)

    # ---------- Salesforce (SOQL query) ----------
    if mode == "salesforce":
        safe = whitelist_request_opts(dict(base_opts))
        host_base = urljoin(url, "/")
        done_path = (pag_cfg or {}).get("done_path", "done")
        next_url_path = (pag_cfg or {}).get("next_url_path", "nextRecordsUrl")
        clear_params_on_next = bool(
            (pag_cfg or {}).get("clear_params_on_next", True)
        )
        max_pages = int((pag_cfg or {}).get("max_pages", 10000))

        # Place q in URL (encoded) to avoid form-style '+' for spaces
        q = (safe.get("params") or {}).get("q")
        if q:
            encoded_q = _encode_soql_for_q(q)
            base_only = url.split("?", 1)[0]
            first_url = f"{base_only}?q={encoded_q}"
            # remove q from params so requests doesn't touch it
            safe = dict(safe)
            ps = dict(safe.get("params") or {})
            ps.pop("q", None)
            if ps:
                safe["params"] = ps
            else:
                safe.pop("params", None)
        else:
            first_url = url

        pages = 0
        next_url = first_url
        while pages < max_pages and next_url:
            resp = sess.get(next_url, **safe)
            resp.raise_for_status()
            data = resp.json()

            drop_keys = set(parse_cfg.get("json_drop_keys_any_depth", []))
            if drop_keys:
                data = drop_keys_any_depth(data, drop_keys)

            df_page = json_obj_to_df(data, parse_cfg)
            if not df_page.empty:
                frames.append(df_page)

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

    # ---------- Cursor / Page / Link-header ----------
    safe = whitelist_request_opts(dict(base_opts))

    if mode in {"cursor", "page"}:
        params = dict(safe.get("params") or {})
        ps_param = (pag_cfg or {}).get("page_size_param")
        ps_value = (pag_cfg or {}).get("page_size_value")
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

import os
import time
import traceback
from datetime import date, timedelta
from io import StringIO
from logging import Logger
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin

import pandas as pd
import requests
from requests import Session
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


class ApiIngestor:
    """
    Config-driven API ingestor that:
      - Loads env + API settings from YAML (with ${ENV_VAR} substitution)
      - Builds a requests.Session with retry/backoff (HTTPAdapter + urllib3.Retry)
      - Supports one-off pulls and windowed backfills
      - Handles pagination: 'none', 'cursor', 'page', 'link-header'
      - Optionally expands per-row URLs ("link_expansion")
      - Parses JSON or CSV into pandas DataFrames
      - Redacts secrets in logs

    Merge precedence for request options:
      API request_defaults <- env overrides <- per-call overrides
    """

    # For safety, only allow these kwargs to pass into requests.get(...)
    _ALLOWED_REQUEST_KW = {
        "headers",
        "params",
        "timeout",
        "verify",
        "auth",
        "proxies",
        "stream",
        "allow_redirects",
    }

    # ---------- Construction / Config ----------

    def __init__(self, config: Dict[str, Any], log: Logger):
        self.config = config
        self.log = log

    # ---------- Public entry points ----------

    def run_once(
        self,
        table_name: str,
        env_name: str,
    ) -> pd.DataFrame:
        """
        Single pull:
          - Merges config layers to form final request options
          - Applies pagination strategy (if any)
          - Optionally expands links found in result rows
          - Returns a DataFrame
        """
        env_cfg, api_cfg, req_opts, parse_cfg = self._prepare(
            table_name, env_name
        )
        # Build or reuse a Session; pop "retries" so it doesn't leak into requests.get(...)
        sess = self._build_session(req_opts.pop("retries", None))
        url = self._build_url(env_cfg["base_url"], api_cfg.get("path", ""))

        safe_opts = self._whitelist_request_opts(req_opts)
        self._log_request(url, safe_opts)

        try:
            # Use the unified paginator (handles none/cursor/page/link-header)
            df = self._paginate(
                sess, url, safe_opts, parse_cfg, api_cfg.get("pagination")
            )

            # Optional link expansion step (follow URLs embedded in each row)
            link_cfg = api_cfg.get("link_expansion")
            if link_cfg and link_cfg.get("enabled", False):
                df = self._expand_links(sess, df, link_cfg, parse_cfg)

            return df

        except Exception as e:
            self._log_exception(url, e)
            raise

    def run_backfill(
        self,
        table_name: str,
        env_name: str,
        start: date,
        end: date,
    ) -> pd.DataFrame:
        """
        Unified windowed backfill entry point.

        Supports three strategies (choose via YAML under apis.<name>.backfill.strategy):
        - "date" (default): injects two query params per window (e.g., start_date/end_date)
        - "soql_window": Salesforce-specific; builds a windowed SOQL and sends via params['q']
        - "cursor": cursor-bounded backfill (seed cursor and stop by id or timestamp)

        Returns a single concatenated DataFrame across all slices/pages.
        """

        env_cfg, api_cfg, base_req_opts, parse_cfg = self._prepare(
            table_name, env_name
        )

        bf = api_cfg.get("backfill", {}) or {}
        if not bf.get("enabled", False):
            raise ValueError(
                f"Backfill is not enabled for '{table_name}' in config."
            )

        # Build/reuse a Session; strip adapter config out of request kwargs
        sess = self._build_session(base_req_opts.pop("retries", None))
        url = self._build_url(env_cfg["base_url"], api_cfg.get("path", ""))

        strategy = bf.get("strategy", "date").lower()
        pag_mode = (api_cfg.get("pagination") or {}).get("mode", "none").lower()
        link_cfg = api_cfg.get("link_expansion") or {}

        if strategy == "cursor":
            # Cursor-bounded (no date windows). Good for cursor-only APIs.
            return self._cursor_backfill(
                sess=sess,
                url=url,
                base_opts=base_req_opts,
                parse_cfg=parse_cfg,
                pag_cfg=api_cfg.get("pagination") or {},
                cur_cfg=bf.get("cursor") or {},
                link_cfg=link_cfg,
            )

        if strategy == "soql_window":
            # Salesforce: build windowed SOQL per slice, then use the Salesforce paginator.
            if pag_mode != "salesforce":
                raise ValueError(
                    "soql_window strategy requires pagination.mode == 'salesforce'."
                )
            return self._soql_window_backfill(
                sess=sess,
                url=url,
                base_opts=base_req_opts,
                parse_cfg=parse_cfg,
                pag_cfg=api_cfg.get("pagination") or {},
                bf_cfg=bf,
                start=start,
                end=end,
                link_cfg=link_cfg,
            )

        # Default: DATE PARAM windows (generic)
        window_days = int(bf.get("window_days", 7))
        start_param = bf.get("start_param", "start_date")
        end_param = bf.get("end_param", "end_date")
        date_format = bf.get("date_format", "%Y-%m-%d")
        per_request_delay = float(bf.get("per_request_delay", 0.0))

        all_frames: List[pd.DataFrame] = []
        current = start

        while current <= end:
            # Inclusive window: [current .. window_end]
            window_end = min(current + timedelta(days=window_days - 1), end)

            # Clone base options and inject window params
            req_opts = dict(base_req_opts)
            params = dict(req_opts.get("params") or {})
            params[start_param] = current.strftime(date_format)
            params[end_param] = window_end.strftime(date_format)
            req_opts["params"] = params

            safe_opts = self._whitelist_request_opts(req_opts)
            self._log_request(
                url,
                safe_opts,
                prefix=f"[{params[start_param]} → {params[end_param]}] ",
            )

            try:
                df = self._paginate(
                    sess, url, safe_opts, parse_cfg, api_cfg.get("pagination")
                )

                # Optional link expansion (non-metadata URLs only; metadata already dropped)
                if link_cfg.get("enabled", False):
                    df = self._expand_links(sess, df, link_cfg, parse_cfg)

                all_frames.append(df)

                if per_request_delay > 0:
                    time.sleep(per_request_delay)

            except Exception as e:
                self._log_exception(
                    url,
                    e,
                    prefix=f"[{params[start_param]} → {params[end_param]}] ",
                )
                raise

            # Advance to the next day after this window
            current = window_end + timedelta(days=1)

        if not all_frames:
            return pd.DataFrame()
        return pd.concat(all_frames, ignore_index=True)

    # ---------- Core helpers ----------

    def _prepare(
        self,
        table_name: str,
        env_name: str,
    ) -> Tuple[
        Dict[str, Any],
        Dict[str, Any],
        Dict[str, Any],
        Dict[str, Any],
    ]:
        """
        Merge the three layers of request config and return:
          - api_cfg (the API's block from YAML),
          - req_opts (final request options for requests.get),
          - parse_cfg (CSV vs JSON settings).
        """
        env_cfg = (self.config.get("envs") or {}).get(env_name) or {}
        api_cfg = self.config.get("apis") or {}
        table_cfg = api_cfg.get(table_name)
        if not table_cfg:
            raise KeyError(f"Table config '{table_name}' not found.")

        req_opts = api_cfg.get("request_defaults", {})

        # Gets the params for the table from the table config
        req_opts["params"] = table_cfg.get("params", None)

        # Gets the backfill for the table from the table config
        api_cfg["backfill"] = table_cfg.get("backfill", None)

        parse_cfg = table_cfg.get("parse", {}) or {"type": "csv"}
        return env_cfg, api_cfg, req_opts, parse_cfg

    def _build_session(self, retries_cfg: Optional[Dict[str, Any]]) -> Session:
        """
        Build a requests.Session and mount an HTTPAdapter with retry/backoff config.
        - Retries live at the session/adapter level, not as a requests.get kwarg.
        """
        s = requests.Session()
        if retries_cfg:
            r = Retry(
                total=int(retries_cfg.get("total", 3)),
                connect=int(
                    retries_cfg.get("connect", retries_cfg.get("total", 3))
                ),
                read=int(retries_cfg.get("read", retries_cfg.get("total", 3))),
                backoff_factor=float(retries_cfg.get("backoff_factor", 0.5)),
                status_forcelist=tuple(
                    retries_cfg.get(
                        "status_forcelist", [429, 500, 502, 503, 504]
                    )
                ),
                allowed_methods=frozenset(
                    retries_cfg.get("allowed_methods", ["GET"])
                ),
                respect_retry_after_header=True,
                raise_on_status=False,
            )
            adapter = HTTPAdapter(max_retries=r)
            # Mount the retry-enabled adapter for all HTTP/S requests
            s.mount("https://", adapter)
            s.mount("http://", adapter)
        return s

    def _build_url(self, base_url: str, path: str) -> str:
        """Safely join the base URL and path."""
        return urljoin(base_url.rstrip("/") + "/", path.lstrip("/"))

    def _paginate(
        self,
        sess: requests.Session,
        url: str,
        base_opts: Dict[str, Any],
        parse_cfg: Dict[str, Any],
        pag_cfg: Optional[Dict[str, Any]],
    ) -> pd.DataFrame:
        """
        Unified paginator:
          - mode='none': single request
          - mode='salesforce': increment page based on done or not
          - mode='cursor': use meta.next cursor pattern
          - mode='page': increment page param until empty
          - mode='link-header': follow HTTP Link headers (rel="next")
        Returns a concatenated DataFrame of all pages.
        """
        mode = (pag_cfg or {}).get("mode", "none")
        frames: List[pd.DataFrame] = []

        if mode == "none":
            resp = sess.get(url, **self._whitelist_request_opts(base_opts))
            resp.raise_for_status()
            return self._to_dataframe(resp, parse_cfg)

        if mode == "salesforce":
            safe = self._whitelist_request_opts(dict(base_opts))
            host_base = urljoin(url, "/")
            done_path = (pag_cfg or {}).get("done_path", "done")
            next_url_path = (pag_cfg or {}).get(
                "next_url_path", "nextRecordsUrl"
            )
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

                # >>> Option A: drop configured keys anywhere before building the DataFrame
                drop_keys = set(parse_cfg.get("json_drop_keys_any_depth", []))
                if drop_keys:
                    data = self._drop_keys_any_depth(data, drop_keys)

                # Convert this page -> DataFrame without re-decoding
                df_page = self._json_obj_to_df(data, parse_cfg)
                if not df_page.empty:
                    frames.append(df_page)

                # Stop if 'done' or no 'nextRecordsUrl'
                if (data.get(done_path, True)) is True:
                    break
                next_rel = data.get(next_url_path)
                if not next_rel:
                    break

                next_url = urljoin(host_base, next_rel)
                if clear_params_on_next and "params" in safe:
                    safe = dict(safe)
                    safe.pop("params", None)

                pages += 1

            if not frames:
                return pd.DataFrame()
            return pd.concat(frames, ignore_index=True)

        # copy so we can mutate params safely
        opts = dict(base_opts)
        safe = self._whitelist_request_opts(opts)

        if mode in {"cursor", "page"}:
            ps_param = pag_cfg.get("page_size_param") if pag_cfg else None
            ps_value = pag_cfg.get("page_size_value") if pag_cfg else None
            if ps_param and ps_value:
                params = dict(safe.get("params") or {})
                params[ps_param] = ps_value
                safe["params"] = params

        max_pages = int((pag_cfg or {}).get("max_pages", 10000))
        pages = 0
        next_url = url

        while pages < max_pages and next_url:
            resp = sess.get(next_url, **safe)
            resp.raise_for_status()
            frames.append(self._to_dataframe(resp, parse_cfg))
            pages += 1

            if mode == "cursor":
                next_cursor = self._dig(
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
                if frames[-1].empty:
                    break

            elif mode == "link-header":
                next_link = resp.links.get("next", {}).get("url")
                next_url = next_link

            else:
                raise ValueError(f"Unsupported pagination mode: {mode}")

        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, ignore_index=True)

    def _drop_keys_any_depth(self, obj, keys: set):
        """Recursively drop any dict keys whose name is in `keys`."""
        if isinstance(obj, dict):
            return {
                k: self._drop_keys_any_depth(v, keys)
                for k, v in obj.items()
                if k not in keys
            }
        if isinstance(obj, list):
            return [self._drop_keys_any_depth(v, keys) for v in obj]
        return obj

    def _json_obj_to_df(
        self, data_obj: Any, parse_cfg: Dict[str, Any]
    ) -> pd.DataFrame:
        record_path = parse_cfg.get("json_record_path")
        data = data_obj
        if record_path:
            for key in record_path.split("."):
                data = (data or {}).get(key, [])
        if isinstance(data, list):
            return pd.DataFrame(data)
        return pd.json_normalize(data)

    def _to_dataframe(
        self, resp: requests.Response, parse_cfg: Dict[str, Any]
    ) -> pd.DataFrame:
        parse_type = (parse_cfg.get("type") or "json").lower()
        if parse_type == "csv":
            csv_string = resp.content.decode("utf-8", errors="replace")
            return pd.read_csv(StringIO(csv_string))

        if parse_type == "json":
            data = resp.json()
            drop_keys = set(parse_cfg.get("json_drop_keys_any_depth", []))
            if drop_keys:
                data = self._drop_keys_any_depth(data, drop_keys)
            return self._json_obj_to_df(data, parse_cfg)

        raise ValueError(f"Unsupported parse.type: {parse_type}")

    def _soql_window_backfill(
        self,
        sess: requests.Session,
        url: str,
        base_opts: Dict[str, Any],
        parse_cfg: Dict[str, Any],
        pag_cfg: Dict[str, Any],
        bf_cfg: Dict[str, Any],
        start: date,
        end: date,
        link_cfg: Dict[str, Any],
    ) -> pd.DataFrame:
        """
        Build a windowed SOQL for each slice and let the Salesforce paginator
        (done + nextRecordsUrl) page within that slice.

        Uses HALF-OPEN windows: [start, end) to avoid duplicate boundaries.
        """
        window_days = int(bf_cfg.get("window_days", 7))
        per_request_delay = float(bf_cfg.get("per_request_delay", 0.0))
        date_field = bf_cfg.get(
            "date_field", "LastModifiedDate"
        )  # or "SystemModstamp"
        date_format = bf_cfg.get("date_format", "%Y-%m-%dT%H:%M:%SZ")  # UTC ISO
        soql_template = bf_cfg.get("soql_template")
        if not soql_template:
            raise ValueError(
                "backfill.strategy=soql_window requires 'soql_template' in config."
            )

        frames: List[pd.DataFrame] = []
        current = start

        while current <= end:
            # HALF-OPEN window: [current .. window_end) (end exclusive)
            window_end = min(
                current + timedelta(days=window_days), end + timedelta(days=1)
            )
            start_str = current.strftime(date_format)
            end_str = window_end.strftime(date_format)

            soql = soql_template.format(
                date_field=date_field, start=start_str, end=end_str
            )

            # Inject SOQL as the 'q' parameter (first Salesforce request in a slice)
            req_opts = dict(base_opts)
            params = dict(req_opts.get("params") or {})
            params["q"] = soql
            req_opts["params"] = params

            safe_opts = self._whitelist_request_opts(req_opts)
            self._log_request(
                url, safe_opts, prefix=f"[{start_str} → {end_str}) "
            )

            try:
                df = self._paginate(sess, url, safe_opts, parse_cfg, pag_cfg)

                if link_cfg.get("enabled", False):
                    df = self._expand_links(sess, df, link_cfg, parse_cfg)

                frames.append(df)

                if per_request_delay > 0:
                    time.sleep(per_request_delay)

            except Exception as e:
                self._log_exception(
                    url, e, prefix=f"[{start_str} → {end_str}) "
                )
                raise

            # Move to next half-open window
            current = window_end

        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, ignore_index=True)

    def _cursor_backfill(
        self,
        sess: requests.Session,
        url: str,
        base_opts: Dict[str, Any],
        parse_cfg: Dict[str, Any],
        pag_cfg: Dict[str, Any],
        cur_cfg: Dict[str, Any],
        link_cfg: Dict[str, Any],
    ) -> pd.DataFrame:
        """
        Seed a starting cursor and keep paginating until a stop condition is met.
        Supports both:
        - Opaque next-token style (next_cursor_path -> cursor_param)
        - ID-chaining style (chain_field -> cursor_param)
        Stop conditions (optional):
        - stop_at_item: { field, value, inclusive }
        - stop_when_older_than: { field, value (ISO 8601) }
        """
        safe = self._whitelist_request_opts(dict(base_opts))
        frames: List[pd.DataFrame] = []

        # Seed cursor if provided
        start_value = (cur_cfg or {}).get("start_value")
        cursor_param = (pag_cfg or {}).get("cursor_param", "cursor")
        if start_value:
            params = dict(safe.get("params") or {})
            params[cursor_param] = start_value
            safe["params"] = params

        next_cursor_path = (pag_cfg or {}).get(
            "next_cursor_path"
        )  # opaque-token style
        chain_field = (pag_cfg or {}).get("chain_field")  # id-chaining style
        max_pages = int((pag_cfg or {}).get("max_pages", 10000))

        # Stop-by-item
        stop_item = (cur_cfg or {}).get("stop_at_item") or {}
        stop_field = stop_item.get("field")
        stop_value = stop_item.get("value")
        stop_inclusive = bool(stop_item.get("inclusive", False))

        # Stop-by-time (ISO)
        stop_time_cfg = (cur_cfg or {}).get("stop_when_older_than") or {}
        stop_time_field = stop_time_cfg.get("field")
        stop_time_value = stop_time_cfg.get("value")
        stop_dt = (
            pd.to_datetime(stop_time_value, utc=True)
            if stop_time_field and stop_time_value
            else None
        )

        pages = 0
        next_url = url

        while pages < max_pages and next_url:
            resp = sess.get(next_url, **safe)
            resp.raise_for_status()
            df_page = self._to_dataframe(resp, parse_cfg)

            # stop-by-item: trim current page and finish
            if (
                stop_field
                and stop_value
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

                    # Optional link expansion per page
                    if link_cfg.get("enabled", False) and not df_page.empty:
                        df_page = self._expand_links(
                            sess, df_page, link_cfg, parse_cfg
                        )

                    frames.append(df_page)
                    break

            # stop-by-time: keep only rows newer/equal than cutoff; then finish
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
                        if link_cfg.get("enabled", False):
                            trimmed = self._expand_links(
                                sess, trimmed, link_cfg, parse_cfg
                            )
                        frames.append(trimmed)
                    break

            # Optional per-page link expansion
            if link_cfg.get("enabled", False) and not df_page.empty:
                df_page = self._expand_links(sess, df_page, link_cfg, parse_cfg)

            if not df_page.empty:
                frames.append(df_page)

            # Compute next cursor
            if next_cursor_path:
                token = self._dig(resp.json(), next_cursor_path)
                if token:
                    params = dict(safe.get("params") or {})
                    params[cursor_param] = token
                    safe["params"] = params
                    next_url = url
                else:
                    next_url = None
            elif chain_field:
                if df_page.empty or chain_field not in df_page.columns:
                    break
                last_val = df_page[chain_field].iloc[-1]
                params = dict(safe.get("params") or {})
                params[cursor_param] = last_val
                safe["params"] = params
                next_url = url
            else:
                # No way to advance
                next_url = None

            pages += 1

        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, ignore_index=True)

    def _expand_links(
        self,
        sess: requests.Session,
        df: pd.DataFrame,
        link_cfg: Dict[str, Any],
        parse_cfg: Dict[str, Any],
    ) -> pd.DataFrame:
        """
        Follow URLs found in each row and aggregate those payloads into a DataFrame.
        Notes:
          - For large datasets, consider batching/async.
          - If expanded responses need a different JSON record path, set link_cfg.json_record_path.
        """
        if df.empty:
            return df

        url_fields = (link_cfg or {}).get("url_fields", [])
        if not url_fields:
            return df

        per_delay = float((link_cfg or {}).get("per_request_delay", 0.0))
        expanded_frames: List[pd.DataFrame] = []

        # Iterate rows; for each, collect any configured URL fields
        for _, row in df.iterrows():
            urls: List[str] = []
            for fld in url_fields:
                val = self._get_from_row(row, fld)
                if isinstance(val, str) and val.startswith(
                    ("http://", "https://")
                ):
                    urls.append(val)

            # Fetch each URL and parse into a DataFrame
            row_frames: List[pd.DataFrame] = []
            for u in urls:
                resp = sess.get(u, timeout=30)  # inherits session retry policy
                resp.raise_for_status()

                # Optionally override parse path for expanded payloads
                parse_override = dict(parse_cfg)
                alt_path = (link_cfg or {}).get("json_record_path")
                if alt_path and parse_override.get("type", "json") == "json":
                    parse_override["json_record_path"] = alt_path

                row_frames.append(self._to_dataframe(resp, parse_override))

                if per_delay > 0:
                    time.sleep(per_delay)

            # Merge multiple expansions for the same row side-by-side (outer join on index)
            if row_frames:
                merged = row_frames[0]
                for f in row_frames[1:]:
                    merged = merged.merge(
                        f, left_index=True, right_index=True, how="outer"
                    )
                expanded_frames.append(merged)

        if not expanded_frames:
            return df

        # Simple concatenation of all expanded data; if you have a join key, you can join back to df
        expanded_all = pd.concat(expanded_frames, ignore_index=True)
        return expanded_all

    # ---------- Small utilities ----------

    def _whitelist_request_opts(self, opts: Dict[str, Any]) -> Dict[str, Any]:
        """Keep only request.get-supported kwargs; drop unknown keys like 'retries'."""
        return {
            k: v
            for k, v in (opts or {}).items()
            if k in self._ALLOWED_REQUEST_KW
        }

    def _dig(self, obj: Any, path: Optional[str]):
        """Walk a dot path through nested dicts (returns None if missing)."""
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

    def _get_from_row(self, row: pd.Series, path: str):
        """
        Read a dot-path from a DataFrame row.
        Supports:
          - flat columns (exact name match),
          - dict-like columns (e.g., 'links' holding {'self': 'https://...'})
        """
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

    def _log_request(self, url: str, opts: Dict[str, Any], prefix: str = ""):
        """Log request details with sensitive headers redacted."""
        safe_headers = dict(opts.get("headers") or {})
        for sensitive in ("authorization", "x-api-key"):
            for key in list(safe_headers.keys()):
                if key.lower() == sensitive:
                    safe_headers[key] = "***REDACTED***"
        self.log.info(
            f"{prefix}GET {url} params={(opts.get('params') or {})} headers={safe_headers}"
        )

    def _log_exception(self, url: str, e: Exception, prefix: str = ""):
        """
        Log error details when a request fails (including full stack trace).
        Called from run_once/run_backfill exception blocks.
        """
        self.log.error(
            f"{prefix}Error retrieving data from {url}: {e}\n"
            f"Stack Trace: {traceback.format_exc()}"
        )

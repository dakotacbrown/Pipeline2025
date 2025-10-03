import gzip
import os
import re
import time
import traceback
from datetime import date, timedelta
from io import BytesIO, StringIO
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
      - Builds a `requests.Session` with retry/backoff (HTTPAdapter + urllib3.Retry)
      - Supports one-off pulls and windowed backfills
      - Handles pagination: 'none', 'cursor', 'page', 'link-header', and Salesforce ('salesforce')
      - Optionally expands per-row URLs ("link_expansion")
      - Parses JSON or CSV into pandas DataFrames
      - Redacts secrets in logs

    IMPORTANT implementation detail (the “fix #2” you chose):
      We copy request-level defaults (headers/auth/proxies/verify) to the Session
      once via `_apply_session_defaults`. This ensures **all** requests—including
      the per-row link expansion calls—inherit the same auth/headers/verify/proxies,
      even if those calls don’t pass per-request kwargs.
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

    # Names we will redact in logs for headers and params
    _SENSITIVE_HEADERS = {
        "authorization",
        "x-api-key",
        "api-key",
        "proxy-authorization",
    }
    _SENSITIVE_PARAMS = {
        "access_token",
        "token",
        "apikey",
        "api_key",
        "authorization",
        "signature",
        "client_id",
        "client_secret",
        "refresh_token",
        "secret",
        "password",
        "private_key",
    }

    # ${ENV_VAR} placeholder pattern
    _ENV_RE = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")

    # ---------- Construction / Config ----------

    def __init__(self, config: Dict[str, Any], log: Logger):
        """
        Args:
            config: Parsed YAML (dict) with 'envs' and 'apis' roots.
            log:    Logger to emit info/errors (secrets are redacted).
        """
        self.config = config
        self.log = log

    # ---------- Public entry points ----------

    def run_once(self, table_name: str, env_name: str) -> Dict[str, Any]:
        """
        Execute a single request (with optional pagination and link expansion),
        write the result to the configured sink, and return lightweight metadata.

        Steps:
          1) Merge env + API config layers via _prepare
          2) Create a retry-enabled Session
          3) Apply request defaults to the Session (so link expansion inherits them)
          4) Build the URL from env.base_url + apis.path
          5) Page through results (if configured) and parse into a DataFrame
          6) Optionally expand per-row URLs

        Returns:
            {
            "table": str,
            "env": str,
            "rows": int,
            "format": "csv"|"jsonl"|"parquet",
            "s3_bucket": str,
            "s3_key": str,
            "s3_uri": str,
            "bytes": int,
            "started_at": str,      # ISO-8601 UTC
            "ended_at": str,        # ISO-8601 UTC
            "duration_s": float,
            "source_url": str,
            "pagination_mode": str
            }
        """
        started = pd.Timestamp.now(tz="UTC")
        self.log.info(f"[run_once] start table={table_name} env={env_name}")

        env_cfg, api_cfg, req_opts, parse_cfg = self._prepare(
            table_name, env_name
        )

        sess = self._build_session(req_opts.pop("retries", None))
        safe_opts = self._whitelist_request_opts(req_opts)
        self._apply_session_defaults(sess, safe_opts)

        url = self._build_url(env_cfg["base_url"], api_cfg.get("path", ""))
        self._log_request(url, safe_opts)

        try:
            df = self._paginate(
                sess, url, safe_opts, parse_cfg, api_cfg.get("pagination")
            )
            link_cfg = api_cfg.get("link_expansion")
            if link_cfg and link_cfg.get("enabled", False):
                df = self._expand_links(sess, df, link_cfg, parse_cfg)

            out_meta = self._write_output(
                df, table_name, env_name, api_cfg.get("output") or {}
            )
            ended = pd.Timestamp.now(tz="UTC")
            self.log.info(
                f"[run_once] done table={table_name} env={env_name} "
                f"rows={len(df)} duration={(ended - started).total_seconds():.3f}s "
                f"dest={out_meta.get('s3_uri')}"
            )
            return {
                "table": table_name,
                "env": env_name,
                "rows": int(len(df)),
                "format": out_meta["format"],
                "s3_bucket": out_meta["s3_bucket"],
                "s3_key": out_meta["s3_key"],
                "s3_uri": out_meta["s3_uri"],
                "bytes": out_meta["bytes"],
                "started_at": started.isoformat(),
                "ended_at": ended.isoformat(),
                "duration_s": float((ended - started).total_seconds()),
                "source_url": url,
                "pagination_mode": (api_cfg.get("pagination") or {}).get(
                    "mode", "none"
                ),
            }

        except Exception as e:
            self._log_exception(url, e)
            raise

    def run_backfill(
        self, table_name: str, env_name: str, start: date, end: date
    ) -> Dict[str, Any]:
        """
        Windowed backfill over a date range.

        Strategies (apis.<table>.backfill.strategy):
          - "date"        : Generic date windows via query params (default)
          - "soql_window" : Salesforce SOQL per window (uses 'q' param) and SF pagination
          - "cursor"      : Cursor-bounded backfill with stop conditions

        Returns:
            Metadata dictionary.
        """
        started = pd.Timestamp.now(tz="UTC")
        self.log.info(
            f"[run_backfill] start table={table_name} env={env_name} range={start}..{end}"
        )
        env_cfg, api_cfg, base_req_opts, parse_cfg = self._prepare(
            table_name, env_name
        )

        bf = api_cfg.get("backfill", {}) or {}
        if not bf.get("enabled", False):
            raise ValueError(
                f"Backfill is not enabled for '{table_name}' in config."
            )

        sess = self._build_session(base_req_opts.pop("retries", None))
        self._apply_session_defaults(
            sess, self._whitelist_request_opts(base_req_opts)
        )

        url = self._build_url(env_cfg["base_url"], api_cfg.get("path", ""))

        strategy = bf.get("strategy", "date").lower()
        pag_mode = (api_cfg.get("pagination") or {}).get("mode", "none").lower()
        link_cfg = api_cfg.get("link_expansion") or {}

        if strategy == "cursor":
            df = self._cursor_backfill(
                sess=sess,
                url=url,
                base_opts=base_req_opts,
                parse_cfg=parse_cfg,
                pag_cfg=api_cfg.get("pagination") or {},
                cur_cfg=bf.get("cursor") or {},
                link_cfg=link_cfg,
            )
            out_meta = self._write_output(
                df, table_name, env_name, api_cfg.get("output") or {}
            )
            ended = pd.Timestamp.now(tz="UTC")
            self.log.info(
                f"[run_backfill] done strategy=cursor table={table_name} env={env_name} "
                f"rows={len(df)} duration={(ended - started).total_seconds():.3f}s "
                f"dest={out_meta.get('s3_uri')}"
            )
            return {
                "table": table_name,
                "env": env_name,
                "rows": int(len(df)),
                "format": out_meta["format"],
                "s3_bucket": out_meta["s3_bucket"],
                "s3_key": out_meta["s3_key"],
                "s3_uri": out_meta["s3_uri"],
                "bytes": out_meta["bytes"],
                "started_at": started.isoformat(),
                "ended_at": ended.isoformat(),
                "duration_s": float((ended - started).total_seconds()),
                "source_url": url,
                "strategy": "cursor",
                "pagination_mode": pag_mode,
            }

        if strategy == "soql_window":
            if pag_mode != "salesforce":
                raise ValueError(
                    "soql_window strategy requires pagination.mode == 'salesforce'."
                )
            df = self._soql_window_backfill(
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
            out_meta = self._write_output(
                df, table_name, env_name, api_cfg.get("output") or {}
            )
            ended = pd.Timestamp.now(tz="UTC")
            self.log.info(
                f"[run_backfill] done strategy=soql_window table={table_name} env={env_name} "
                f"rows={len(df)} duration={(ended - started).total_seconds():.3f}s "
                f"dest={out_meta.get('s3_uri')}"
            )
            return {
                "table": table_name,
                "env": env_name,
                "rows": int(len(df)),
                "format": out_meta["format"],
                "s3_bucket": out_meta["s3_bucket"],
                "s3_key": out_meta["s3_key"],
                "s3_uri": out_meta["s3_uri"],
                "bytes": out_meta["bytes"],
                "started_at": started.isoformat(),
                "ended_at": ended.isoformat(),
                "duration_s": float((ended - started).total_seconds()),
                "source_url": url,
                "strategy": "soql_window",
                "pagination_mode": pag_mode,
            }

        # Default: date-window strategy
        window_days = int(bf.get("window_days", 7))
        start_param = bf.get("start_param", "start_date")
        end_param = bf.get("end_param", "end_date")
        date_format = bf.get("date_format", "%Y-%m-%d")
        per_request_delay = float(bf.get("per_request_delay", 0.0))

        all_frames: List[pd.DataFrame] = []
        num_windows = 0
        current = start

        while current <= end:
            window_end = min(current + timedelta(days=window_days - 1), end)
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
                if link_cfg.get("enabled", False):
                    df = self._expand_links(sess, df, link_cfg, parse_cfg)
                all_frames.append(df)
                num_windows += 1
                if per_request_delay > 0:
                    time.sleep(per_request_delay)
            except Exception as e:
                self._log_exception(
                    url,
                    e,
                    prefix=f"[{params[start_param]} → {params[end_param]}] ",
                )
                raise

            current = window_end + timedelta(days=1)

        result = (
            pd.concat(all_frames, ignore_index=True)
            if all_frames
            else pd.DataFrame()
        )
        out_meta = self._write_output(
            result, table_name, env_name, api_cfg.get("output") or {}
        )
        ended = pd.Timestamp.now(tz="UTC")
        self.log.info(
            f"[run_backfill] done strategy=date table={table_name} env={env_name} "
            f"windows={num_windows} rows={len(result)} "
            f"duration={(ended - started).total_seconds():.3f}s dest={out_meta.get('s3_uri')}"
        )
        return {
            "table": table_name,
            "env": env_name,
            "rows": int(len(result)),
            "format": out_meta["format"],
            "s3_bucket": out_meta["s3_bucket"],
            "s3_key": out_meta["s3_key"],
            "s3_uri": out_meta["s3_uri"],
            "bytes": out_meta["bytes"],
            "started_at": started.isoformat(),
            "ended_at": ended.isoformat(),
            "duration_s": float((ended - started).total_seconds()),
            "source_url": url,
            "strategy": "date",
            "pagination_mode": (api_cfg.get("pagination") or {}).get(
                "mode", "none"
            ),
            "windows": num_windows,
        }

    # ---------- Core helpers ----------

    def _expand_env_value(self, v: Any) -> Any:
        """
        Recursively expand ${ENV_VAR} placeholders in strings/dicts/lists.

        Missing env vars are left as-is (e.g., '${FOO}') so callers can detect
        them instead of silently replacing with empty strings.
        """
        if isinstance(v, str):

            def repl(m):
                return os.getenv(
                    m.group(1), m.group(0)
                )  # keep ${...} literal if missing

            return self._ENV_RE.sub(repl, v)
        if isinstance(v, dict):
            return {k: self._expand_env_value(vv) for k, vv in v.items()}
        if isinstance(v, list):
            return [self._expand_env_value(x) for x in v]
        return v

    def _prepare(
        self, table_name: str, env_name: str
    ) -> Tuple[Dict[str, Any], Dict[str, Any], Dict[str, Any], Dict[str, Any]]:
        """
        Merge env + API layers and return the effective configs.

        Returns:
            env_cfg:  env-level settings (must include base_url)
            api_cfg:  effective API settings for the run (path/pagination/link_expansion/backfill)
            req_opts: request kwargs defaults (headers/params/verify/auth/proxies/timeout/retries)
            parse_cfg: parsing options (type=json|csv, json_record_path, etc.)
        """
        # ----- env
        env_cfg = (self.config.get("envs") or {}).get(env_name) or {}
        if not env_cfg.get("base_url"):
            raise ValueError(
                f"env '{env_name}' must define a non-empty base_url"
            )

        # ----- apis
        apis_root = self.config.get("apis") or {}
        table_cfg = apis_root.get(table_name) or {}
        if not table_cfg:
            raise KeyError(
                f"Table config '{table_name}' not found under 'apis'."
            )

        # Global request defaults; table-specific params/headers merged over them
        global_opts = apis_root.get("request_defaults", {}) or {}
        req_opts = dict(global_opts)
        # Merge dict-like request bits
        for k in ("headers", "params"):
            tv = table_cfg.get(k)
            if isinstance(tv, dict):
                req_opts[k] = {**(req_opts.get(k) or {}), **tv}
        # Override scalars
        for k in ("timeout", "verify", "auth", "proxies"):
            if k in table_cfg:
                req_opts[k] = table_cfg[k]

        # Pick up retries with table-level override > request_defaults > apis root
        retries = (
            table_cfg.get("retries")
            or req_opts.get("retries")
            or apis_root.get("retries")
        )
        if retries:
            req_opts["retries"] = retries

        # Effective API-level controls for this run
        api_eff = {
            "path": table_cfg.get("path", apis_root.get("path", "")),
            "pagination": {
                **(apis_root.get("pagination", {}) or {}),
                **(table_cfg.get("pagination", {}) or {}),
            },
            "link_expansion": {
                **(apis_root.get("link_expansion", {}) or {}),
                **(table_cfg.get("link_expansion", {}) or {}),
            },
            "backfill": table_cfg.get("backfill", {}) or {},
            # NEW: table-level output overrides global output defaults
            "output": {
                **(apis_root.get("output", {}) or {}),
                **(table_cfg.get("output", {}) or {}),
            },
        }

        # Expand ${ENV_VAR} placeholders everywhere they might appear
        env_cfg = self._expand_env_value(env_cfg)
        req_opts = self._expand_env_value(req_opts)
        api_eff = self._expand_env_value(api_eff)

        parse_cfg = table_cfg.get("parse", {}) or {"type": "json"}
        return env_cfg, api_eff, req_opts, parse_cfg

    def _build_session(self, retries_cfg: Optional[Dict[str, Any]]) -> Session:
        """
        Build a requests.Session and mount an HTTPAdapter with retry/backoff config.

        Compatible with urllib3 v1.x (uses `method_whitelist`) and v2.x
        (uses `allowed_methods`). We try the v2 signature first and fall
        back to the v1 signature if needed.
        """
        s = requests.Session()

        if not retries_cfg:
            return s

        # Pull values from config (with sensible defaults)
        total = int(retries_cfg.get("total", 3))
        connect = int(retries_cfg.get("connect", total))
        read = int(retries_cfg.get("read", total))
        backoff_factor = float(retries_cfg.get("backoff_factor", 0.5))
        status_forcelist = tuple(
            retries_cfg.get("status_forcelist", [429, 500, 502, 503, 504])
        )
        allowed = retries_cfg.get("allowed_methods", ["GET"])
        # Normalize to an uppercase frozenset, as urllib3 expects
        allowed_set = frozenset(m.upper() for m in allowed)

        # Core kwargs that exist in both lines
        base_kwargs = dict(
            total=total,
            connect=connect,
            read=read,
            backoff_factor=backoff_factor,
            status_forcelist=status_forcelist,
        )

        # Try urllib3 v2.x signature first (allowed_methods, raise_on_status)
        try:
            r = Retry(
                **base_kwargs,
                allowed_methods=allowed_set,
                raise_on_status=False,
            )
        except TypeError:
            # Fall back to urllib3 v1.x (method_whitelist, possibly no raise_on_status)
            try:
                r = Retry(
                    **base_kwargs,
                    method_whitelist=allowed_set,  # deprecated name in v1.x
                    raise_on_status=False,
                )
            except TypeError:
                # Very old urllib3 without raise_on_status
                r = Retry(
                    **base_kwargs,
                    method_whitelist=allowed_set,
                )

        # Respect Retry-After header when the attr exists (v1.26+/v2)
        if hasattr(r, "respect_retry_after_header"):
            setattr(r, "respect_retry_after_header", True)

        adapter = HTTPAdapter(max_retries=r)
        s.mount("https://", adapter)
        s.mount("http://", adapter)
        return s

    def _apply_session_defaults(
        self, sess: requests.Session, opts: Dict[str, Any]
    ) -> None:
        """
        Copy request-level defaults onto the session so *all* requests (including
        link expansion) inherit them.

        Applies:
            - headers (merged into session.headers)
            - auth    (sess.auth)
            - proxies (merged into session.proxies)
            - verify  (sess.verify)

        Not applied (session has no global settings for these):
            - timeout
            - params
        """
        headers = opts.get("headers")
        if headers:
            sess.headers.update(headers)
        if "auth" in opts:
            sess.auth = opts["auth"]
        proxies = opts.get("proxies")
        if proxies:
            sess.proxies.update(proxies)
        if "verify" in opts:
            sess.verify = opts["verify"]

    def _build_url(self, base_url: str, path: str) -> str:
        """Safely join the base URL and path (handles trailing/leading slashes)."""
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

          - mode='none'       : single request
          - mode='salesforce' : Salesforce 'done' flag + 'nextRecordsUrl'
          - mode='cursor'     : opaque next token at json path; sent via 'cursor_param'
          - mode='page'       : increment page param until empty
          - mode='link-header': follow HTTP Link headers (rel="next")

        Returns:
            Concatenated DataFrame of all pages for the current slice.
        """
        mode = (pag_cfg or {}).get("mode", "none")
        frames: List[pd.DataFrame] = []

        if mode == "none":
            resp = sess.get(url, **self._whitelist_request_opts(base_opts))
            resp.raise_for_status()
            return self._to_dataframe(resp, parse_cfg)

        if mode == "salesforce":
            # Salesforce-style pagination:
            #   - Stop when 'done' is true OR there is no 'nextRecordsUrl'
            #   - The 'nextRecordsUrl' is RELATIVE, so we join against host_base
            safe = self._whitelist_request_opts(dict(base_opts))
            host_base = urljoin(url, "/")  # scheme+host root (keeps origin)
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

                # Drop configured keys anywhere in the JSON before parsing (e.g., SF 'attributes')
                drop_keys = set(parse_cfg.get("json_drop_keys_any_depth", []))
                if drop_keys:
                    data = self._drop_keys_any_depth(data, drop_keys)

                # Convert this page -> DataFrame
                df_page = self._json_obj_to_df(data, parse_cfg)
                if not df_page.empty:
                    frames.append(df_page)

                # Stop if 'done' or there is no 'nextRecordsUrl'
                if (data.get(done_path, True)) is True:
                    break
                next_rel = data.get(next_url_path)
                if not next_rel:
                    break

                # nextRecordsUrl is relative to host; rebuild a full URL
                next_url = urljoin(host_base, next_rel)

                # Salesforce expects subsequent calls without the initial query params (e.g., 'q')
                if clear_params_on_next and "params" in safe:
                    safe = dict(safe)
                    safe.pop("params", None)

                pages += 1

            if not frames:
                return pd.DataFrame()
            return pd.concat(frames, ignore_index=True)

        # Non-SF modes: copy so we can mutate params safely per page
        opts = dict(base_opts)
        safe = self._whitelist_request_opts(opts)

        if mode in {"cursor", "page"}:
            # Respect configured page size if present, and seed first page if needed
            params = dict(safe.get("params") or {})

            ps_param = pag_cfg.get("page_size_param") if pag_cfg else None
            ps_value = pag_cfg.get("page_size_value") if pag_cfg else None
            if ps_param and ps_value:
                params[ps_param] = ps_value

            if mode == "page":
                page_param = (pag_cfg or {}).get("page_param", "page")
                start_page = int((pag_cfg or {}).get("start_page", 1))
                # Only set if not already provided upstream
                params.setdefault(page_param, start_page)

            if params:
                safe["params"] = params

        max_pages = int((pag_cfg or {}).get("max_pages", 10000))
        pages = 0
        next_url = url

        while pages < max_pages and next_url:
            resp = sess.get(next_url, **safe)
            resp.raise_for_status()
            page_df = self._to_dataframe(resp, parse_cfg)
            # For 'page' pagination, stop before appending an empty page
            if mode == "page" and page_df.empty:
                break
            frames.append(page_df)
            pages += 1

            if mode == "cursor":
                # Look up the next token at the configured JSON path
                next_cursor = self._dig(
                    resp.json(), (pag_cfg or {}).get("next_cursor_path")
                )
                if next_cursor:
                    params = dict(safe.get("params") or {})
                    params[(pag_cfg or {}).get("cursor_param", "cursor")] = (
                        next_cursor
                    )
                    safe["params"] = params
                    next_url = url  # same endpoint with updated cursor param
                else:
                    next_url = None

            elif mode == "page":
                # Increment a numeric page parameter until the page is empty
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
                # Follow RFC5988 Link: <...>; rel="next"
                next_link = resp.links.get("next", {}).get("url")
                next_url = next_link

            else:
                raise ValueError(f"Unsupported pagination mode: {mode}")

        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, ignore_index=True)

    def _drop_keys_any_depth(self, obj, keys: set):
        """
        Recursively drop any dict keys whose name is in `keys`.
        Useful for removing metadata (e.g., Salesforce 'attributes') before normalization.
        """
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
        """
        Convert an already-decoded JSON object to a DataFrame, respecting
        json_record_path if provided.

        If the record path points to a list -> DataFrame(list).
        Else -> json_normalize(dict).
        """
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
        """
        Convert a single HTTP response into a DataFrame.

        CSV:
            Decode to text and use pandas.read_csv
        JSON:
            Optionally drop keys anywhere in the structure, then honor json_record_path
        """
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
        Salesforce-specific backfill:
          - Build a HALF-OPEN SOQL window per slice: [start, end)
          - Send the SOQL via params['q'] for the *first* request in each slice
          - Let the Salesforce paginator follow 'nextRecordsUrl' within the slice
        """
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

            # Next HALF-OPEN window
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
        Cursor-bounded backfill.

        Supports two advancement modes:
          - Opaque token: read next token at next_cursor_path and send via 'cursor_param'
          - ID chaining : use last row's 'chain_field' value as next cursor

        Optional stop conditions:
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
            if (stop_time_field and stop_time_value)
            else None
        )

        pages = 0
        next_url = url

        while pages < max_pages and next_url:
            resp = sess.get(next_url, **safe)
            resp.raise_for_status()
            df_page = self._to_dataframe(resp, parse_cfg)

            # Stop-by-item: trim current page and finish
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

                    if link_cfg.get("enabled", False) and not df_page.empty:
                        df_page = self._expand_links(
                            sess, df_page, link_cfg, parse_cfg
                        )

                    frames.append(df_page)
                    break

            # Stop-by-time: keep only rows newer/equal than cutoff; then finish
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
        - Inherits auth/headers/verify/proxies from the Session (via _apply_session_defaults)
        - For large datasets, consider batching/async
        - If expanded responses need a different JSON record path, set link_cfg.json_record_path
            (otherwise we *drop* any base json_record_path to correctly parse object payloads).
        """
        if df.empty:
            return df

        le_timeout = (link_cfg or {}).get(
            "timeout", parse_cfg.get("timeout", None)
        )
        url_fields = (link_cfg or {}).get("url_fields", [])
        if not url_fields:
            return df

        per_delay = float((link_cfg or {}).get("per_request_delay", 0.0))
        expanded_frames: List[pd.DataFrame] = []

        for _, row in df.iterrows():
            urls: List[str] = []
            for fld in url_fields:
                val = self._get_from_row(row, fld)
                if isinstance(val, str) and val.startswith(
                    ("http://", "https://")
                ):
                    urls.append(val)
                # If you later need to support relative links, resolve with urljoin(base_url, val).

            row_frames: List[pd.DataFrame] = []
            for u in urls:
                resp = sess.get(u, timeout=le_timeout or 30)
                resp.raise_for_status()

                # ------ KEY CHANGE: do not inherit base json_record_path by default ------
                parse_override = dict(parse_cfg)
                if parse_override.get("type", "json") == "json":
                    if "json_record_path" in (link_cfg or {}):
                        # If provided for expansion, use it (including explicit None)
                        if link_cfg.get("json_record_path") is None:
                            parse_override.pop("json_record_path", None)
                        else:
                            parse_override["json_record_path"] = link_cfg[
                                "json_record_path"
                            ]
                    else:
                        # Not provided for expansion -> drop the base record path
                        parse_override.pop("json_record_path", None)
                # ------------------------------------------------------------------------

                row_frames.append(self._to_dataframe(resp, parse_override))

                if per_delay > 0:
                    time.sleep(per_delay)

            if row_frames:
                merged = row_frames[0]
                for f in row_frames[1:]:
                    merged = merged.merge(
                        f, left_index=True, right_index=True, how="outer"
                    )
                expanded_frames.append(merged)

        if not expanded_frames:
            return df

        return pd.concat(expanded_frames, ignore_index=True)

    # ---------- Small utilities ----------

    def _whitelist_request_opts(self, opts: Dict[str, Any]) -> Dict[str, Any]:
        """Keep only requests.get-supported kwargs; drop unknown keys like 'retries'."""
        return {
            k: v
            for k, v in (opts or {}).items()
            if k in self._ALLOWED_REQUEST_KW
        }

    def _dig(self, obj: Any, path: Optional[str]):
        """Walk a dot path through nested dicts (returns None if any segment is missing)."""
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

    def _write_output(
        self,
        df: pd.DataFrame,
        table_name: str,
        env_name: str,
        out_cfg: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Mandatory sink: always attempt to write the DataFrame using the configured output.
        Returns a metadata dict describing the write (e.g., S3 key, bytes).
        """
        if not out_cfg:
            raise ValueError(
                "Output config is required but missing (apis.<table>.output or apis.output)."
            )

        fmt = (out_cfg.get("format") or "csv").lower()
        if fmt not in {"csv", "parquet", "jsonl"}:
            raise ValueError(f"Unsupported output.format: {fmt}")

        write_empty = out_cfg.get("write_empty", True)
        if df.empty and not write_empty:
            raise ValueError(
                "DataFrame is empty and output.write_empty=false; refusing to skip since writes are mandatory."
            )

        s3_cfg = out_cfg.get("s3") or {}
        if not s3_cfg:
            raise ValueError(
                "Output sink must be S3. Provide apis.<table>.output.s3."
            )

        return self._write_s3(df, table_name, env_name, fmt, s3_cfg)

    def _write_s3(
        self,
        df: pd.DataFrame,
        table_name: str,
        env_name: str,
        fmt: str,
        s3_cfg: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Write the DataFrame to S3 using boto3 and return a metadata dict.
        """
        try:
            import boto3
        except Exception as e:
            raise RuntimeError("boto3 is required for S3 output.") from e

        now = pd.Timestamp.now(tz="UTC").to_pydatetime()
        ctx = {
            "table": table_name,
            "env": env_name,
            "now": now,
            "today": now,
        }

        bucket = (s3_cfg.get("bucket") or "").strip()
        if not bucket:
            raise ValueError("output.s3.bucket is required.")

        prefix = (s3_cfg.get("prefix") or "").format(**ctx).strip("/")
        ext = {"csv": "csv", "parquet": "parquet", "jsonl": "jsonl"}[fmt]
        default_fname = "{table}-{now:%Y%m%dT%H%M%SZ}." + ext
        filename = (s3_cfg.get("filename") or default_fname).format(**ctx)
        key = "/".join([p for p in [prefix, filename] if p])

        region_name = s3_cfg.get("region_name")
        endpoint_url = s3_cfg.get("endpoint_url")
        session = (
            boto3.session.Session(region_name=region_name)
            if region_name
            else boto3.session.Session()
        )
        s3 = session.client("s3", endpoint_url=endpoint_url)

        extra_args = {}
        if s3_cfg.get("acl"):
            extra_args["ACL"] = s3_cfg["acl"]
        if s3_cfg.get("sse"):
            extra_args["ServerSideEncryption"] = s3_cfg["sse"]
        if s3_cfg.get("sse_kms_key_id"):
            extra_args["SSEKMSKeyId"] = s3_cfg["sse_kms_key_id"]

        body_bytes, content_type, content_encoding = self._serialize_df(
            df, fmt, s3_cfg
        )
        if content_type:
            extra_args["ContentType"] = content_type
        if content_encoding:
            extra_args["ContentEncoding"] = content_encoding

        s3.put_object(Bucket=bucket, Key=key, Body=body_bytes, **extra_args)

        meta = {
            "format": fmt,
            "s3_bucket": bucket,
            "s3_key": key,
            "s3_uri": f"s3://{bucket}/{key}",
            "bytes": int(len(body_bytes)),
        }
        self.log.info(
            f"[output] Wrote {len(df)} rows ({meta['bytes']} bytes) to {meta['s3_uri']}"
        )
        return meta

    def _serialize_df(
        self,
        df: pd.DataFrame,
        fmt: str,
        s3_cfg: Dict[str, Any],
    ) -> Tuple[bytes, Optional[str], Optional[str]]:
        """
        Serialize a DataFrame to bytes for upload.
        Returns: (bytes, content_type, content_encoding)
        """
        if fmt == "csv":
            index = bool(s3_cfg.get("index", False))
            sep = s3_cfg.get("sep", ",")
            compression = (
                s3_cfg.get("compression") or ""
            ).lower()  # e.g., 'gzip'
            csv_text = df.to_csv(index=index, sep=sep)
            raw = csv_text.encode("utf-8")
            if compression == "gzip":
                buf = BytesIO()
                with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
                    gz.write(raw)
                return buf.getvalue(), "text/csv", "gzip"
            return raw, "text/csv", None

        if fmt == "jsonl":
            text = df.to_json(orient="records", lines=True, date_format="iso")
            return text.encode("utf-8"), "application/x-ndjson", None

        if fmt == "parquet":
            compression = s3_cfg.get("compression", "snappy")
            if isinstance(compression, str) and compression.lower() == "none":
                compression = None
            buf = BytesIO()
            df.to_parquet(buf, index=False, compression=compression)
            return buf.getvalue(), "application/vnd.apache.parquet", None

        raise ValueError(f"Unsupported format: {fmt}")

    def _log_request(self, url: str, opts: Dict[str, Any], prefix: str = ""):
        """
        Log request details with sensitive headers/params redacted.
        Helps to debug parameter windows and SOQL slices without leaking secrets.
        """
        safe_headers = dict(opts.get("headers") or {})
        for k in list(safe_headers.keys()):
            if k.lower() in self._SENSITIVE_HEADERS:
                safe_headers[k] = "***REDACTED***"

        safe_params = dict(opts.get("params") or {})
        for k in list(safe_params.keys()):
            if k.lower() in self._SENSITIVE_PARAMS:
                safe_params[k] = "***REDACTED***"

        self.log.info(
            f"{prefix}GET {url} params={safe_params} headers={safe_headers}"
        )

    def _log_exception(self, url: str, e: Exception, prefix: str = ""):
        """Log error details when a request fails (including full stack trace)."""
        self.log.error(
            f"{prefix}Error retrieving data from {url}: {e}\n"
            f"Stack Trace: {traceback.format_exc()}"
        )

import gzip
import json
import os
import re
import time
import traceback
from datetime import date, timedelta
from io import BytesIO, StringIO
from logging import Logger
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin

import numpy as np
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
      - Parses JSON, CSV, or JSONL into pandas DataFrames
      - Can run multiple single pulls and JOIN them: `apis.<table>.multi_pulls` + `apis.<table>.join`
      - Redacts secrets in logs

    IMPORTANT:
      We copy request-level defaults (headers/auth/proxies/verify) to the Session
      once via `_apply_session_defaults`. This ensures **all** requests—including
      the per-row link expansion calls—inherit the same auth/headers/verify/proxies,
      even if those calls don't pass per-request kwargs.
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
        "x-authorization",
        "auth",
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

        # runtime helpers for link-expansion extras
        self._expansion_session_ids: set[str] = set()
        self._flush_seq: int = 0
        self._current_output_ctx: Dict[str, Any] = {}

    # ---------- Public entry points ----------

    def run_once(self, table_name: str, env_name: str) -> Dict[str, Any]:
        """
        Execute a single request (with optional pagination and link expansion),
        or multiple single pulls with a JOIN, then write to the sink.

        Returns lightweight metadata about the write.
        """
        started = pd.Timestamp.now(tz="UTC")
        self.log.info(f"[run_once] start table={table_name} env={env_name}")

        env_cfg, api_cfg, req_opts, parse_cfg = self._prepare(
            table_name, env_name
        )

        sess = self._build_session(req_opts.pop("retries", None))
        safe_opts = self._whitelist_request_opts(req_opts)
        self._apply_session_defaults(sess, safe_opts)

        # Multi-pull + join path. If present, we do NOT use pagination.
        if api_cfg.get("multi_pulls"):
            self.log.info(
                "[run_once] multi_pulls detected -> running pulls and join"
            )
            df = self._run_multi_pulls_and_join(
                sess=sess,
                env_cfg=env_cfg,
                api_cfg=api_cfg,  # new-style
                table_cfg=api_cfg,  # back-compat with older arg name
                base_req_opts=safe_opts,
            )
            # After join, we proceed exactly like the normal path (link expansion, writes).
            link_cfg = api_cfg.get("link_expansion")
            if link_cfg and link_cfg.get("enabled", False):
                df = self._expand_links(
                    sess,
                    df,
                    link_cfg,
                    parse_cfg,  # base parse isn't used for pulls; only for expansion default
                    table_name=table_name,
                    env_name=env_name,
                    api_output_cfg=(api_cfg.get("output") or {}),
                )

            resolved_session_id = self._resolve_session_id(link_cfg or {})
            if resolved_session_id:
                self._current_output_ctx["session_id"] = resolved_session_id

            flush_cfg = (api_cfg.get("link_expansion") or {}).get("flush") or {}
            only_flush = bool(flush_cfg.get("only"))

            if only_flush:
                self.log.info(
                    "[run_once] only_flush=true -> skipping aggregate write (multi_pulls)"
                )
                ended = pd.Timestamp.now(tz="UTC")
                return {
                    "table": table_name,
                    "env": env_name,
                    "rows": 0,
                    "format": (api_cfg.get("output") or {}).get(
                        "format", "csv"
                    ),
                    "s3_bucket": (api_cfg.get("output") or {})
                    .get("s3", {})
                    .get("bucket", ""),
                    "s3_key": "",
                    "s3_uri": "",
                    "bytes": 0,
                    "started_at": started.isoformat(),
                    "ended_at": ended.isoformat(),
                    "duration_s": float((ended - started).total_seconds()),
                }

            out_meta = self._write_output(
                df, table_name, env_name, api_cfg.get("output") or {}
            )
            ended = pd.Timestamp.now(tz="UTC")
            self._current_output_ctx.pop("session_id", None)

            self.log.info(
                f"[run_once] done (multi_pulls) table={table_name} env={env_name} "
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
            }

        # ---- Normal single-pull path (existing behavior) ----
        url = self._build_url(env_cfg["base_url"], api_cfg.get("path", ""))
        self._log_request(url, safe_opts)

        try:
            df = self._paginate(
                sess, url, safe_opts, parse_cfg, api_cfg.get("pagination")
            )

            link_cfg = api_cfg.get("link_expansion")
            if link_cfg and link_cfg.get("enabled", False):
                # pass table/env/output so per-link flush (optional) can write
                df = self._expand_links(
                    sess,
                    df,
                    link_cfg,
                    parse_cfg,
                    table_name=table_name,
                    env_name=env_name,
                    api_output_cfg=(api_cfg.get("output") or {}),
                )

            # expose resolved session_id to final write via ctx (optional)
            resolved_session_id = self._resolve_session_id(link_cfg or {})
            if resolved_session_id:
                self._current_output_ctx["session_id"] = resolved_session_id

            # inside run_once right before out_meta = self._write_output(...)
            flush_cfg = (api_cfg.get("link_expansion") or {}).get("flush") or {}
            only_flush = bool(flush_cfg.get("only"))

            if only_flush:
                # skip aggregate write entirely
                self.log.info(
                    "[run_once] only_flush=true -> skipping aggregate write"
                )
                ended = pd.Timestamp.now(tz="UTC")
                return {
                    "table": table_name,
                    "env": env_name,
                    "rows": 0,
                    "format": (api_cfg.get("output") or {}).get(
                        "format", "csv"
                    ),
                    "s3_bucket": (api_cfg.get("output") or {})
                    .get("s3", {})
                    .get("bucket", ""),
                    "s3_key": "",
                    "s3_uri": "",
                    "bytes": 0,
                    "started_at": started.isoformat(),
                    "ended_at": ended.isoformat(),
                    "duration_s": float((ended - started).total_seconds()),
                    "source_url": url,
                    "pagination_mode": (api_cfg.get("pagination") or {}).get(
                        "mode", "none"
                    ),
                }

            out_meta = self._write_output(
                df, table_name, env_name, api_cfg.get("output") or {}
            )
            ended = pd.Timestamp.now(tz="UTC")

            # clear ctx for safety between runs
            self._current_output_ctx.pop("session_id", None)

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

        Strategies:
          - "date"        : Generic date windows via query params (default)
          - "soql_window" : Salesforce SOQL per window (uses 'q' param) and SF pagination
          - "cursor"      : Cursor-bounded backfill with stop conditions
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

            # expose resolved session_id for final write (optional)
            resolved_session_id = self._resolve_session_id(link_cfg or {})
            if resolved_session_id:
                self._current_output_ctx["session_id"] = resolved_session_id

            # inside run_once right before out_meta = self._write_output(...)
            flush_cfg = (api_cfg.get("link_expansion") or {}).get("flush") or {}
            only_flush = bool(flush_cfg.get("only"))

            if only_flush:
                # skip aggregate write entirely
                self.log.info(
                    "[run_once] only_flush=true -> skipping aggregate write"
                )
                ended = pd.Timestamp.now(tz="UTC")
                return {
                    "table": table_name,
                    "env": env_name,
                    "rows": 0,
                    "format": (api_cfg.get("output") or {}).get(
                        "format", "csv"
                    ),
                    "s3_bucket": (api_cfg.get("output") or {})
                    .get("s3", {})
                    .get("bucket", ""),
                    "s3_key": "",
                    "s3_uri": "",
                    "bytes": 0,
                    "started_at": started.isoformat(),
                    "ended_at": ended.isoformat(),
                    "duration_s": float((ended - started).total_seconds()),
                    "source_url": url,
                    "pagination_mode": (api_cfg.get("pagination") or {}).get(
                        "mode", "none"
                    ),
                }

            out_meta = self._write_output(
                df, table_name, env_name, api_cfg.get("output") or {}
            )
            self._current_output_ctx.pop("session_id", None)

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

            resolved_session_id = self._resolve_session_id(link_cfg or {})
            if resolved_session_id:
                self._current_output_ctx["session_id"] = resolved_session_id

            out_meta = self._write_output(
                df, table_name, env_name, api_cfg.get("output") or {}
            )
            self._current_output_ctx.pop("session_id", None)

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
                link_cfg = api_cfg.get("link_expansion") or {}
                if link_cfg.get("enabled", False):
                    df = self._expand_links(
                        sess,
                        df,
                        link_cfg,
                        parse_cfg,
                        table_name=table_name,
                        env_name=env_name,
                        api_output_cfg=(api_cfg.get("output") or {}),
                    )
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

        resolved_session_id = self._resolve_session_id(
            api_cfg.get("link_expansion") or {}
        )
        if resolved_session_id:
            self._current_output_ctx["session_id"] = resolved_session_id

        out_meta = self._write_output(
            result, table_name, env_name, api_cfg.get("output") or {}
        )
        self._current_output_ctx.pop("session_id", None)

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
        Missing env vars are left as-is.
        """
        if isinstance(v, str):

            def repl(m):
                return os.getenv(m.group(1), m.group(0))

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
            api_cfg:  effective API settings for the run (path/pagination/link_expansion/backfill/multi_pulls/join)
            req_opts: request kwargs defaults (headers/params/verify/auth/proxies/timeout/retries)
            parse_cfg: parsing options (type=json|csv, json_record_path, etc.) for the base table
        """
        env_cfg = (self.config.get("envs") or {}).get(env_name) or {}
        if not env_cfg.get("base_url"):
            raise ValueError(
                f"env '{env_name}' must define a non-empty base_url"
            )

        apis_root = self.config.get("apis") or {}
        table_cfg = apis_root.get(table_name) or {}
        if not table_cfg:
            raise KeyError(
                f"Table config '{table_name}' not found under 'apis'."
            )

        # Global request defaults; table-specific params/headers merged over them
        global_opts = apis_root.get("request_defaults", {}) or {}
        req_opts = dict(global_opts)
        for k in ("headers", "params"):
            tv = table_cfg.get(k)
            if isinstance(tv, dict):
                req_opts[k] = {**(req_opts.get(k) or {}), **tv}
        for k in ("timeout", "verify", "auth", "proxies"):
            if k in table_cfg:
                req_opts[k] = table_cfg[k]

        # retries selection precedence
        retries = (
            table_cfg.get("retries")
            or req_opts.get("retries")
            or apis_root.get("retries")
        )
        if retries:
            req_opts["retries"] = retries

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
            "output": {
                **(apis_root.get("output", {}) or {}),
                **(table_cfg.get("output", {}) or {}),
            },
            # Pass through multi-pull + join instructions (table-level only)
            "multi_pulls": table_cfg.get("multi_pulls"),
            "join": table_cfg.get("join"),
        }

        env_cfg = self._expand_env_value(env_cfg)
        req_opts = self._expand_env_value(req_opts)
        api_eff = self._expand_env_value(api_eff)

        parse_cfg = table_cfg.get("parse", {}) or {"type": "json"}
        return env_cfg, api_eff, req_opts, parse_cfg

    def _build_session(self, retries_cfg: Optional[Dict[str, Any]]) -> Session:
        """
        Build a requests.Session and mount an HTTPAdapter with retry/backoff config.
        Compatible with urllib3 v1/v2.
        """
        s = requests.Session()

        if not retries_cfg:
            return s

        total = int(retries_cfg.get("total", 3))
        connect = int(retries_cfg.get("connect", total))
        read = int(retries_cfg.get("read", total))
        backoff_factor = float(retries_cfg.get("backoff_factor", 0.5))
        status_forcelist = tuple(
            retries_cfg.get("status_forcelist", [429, 500, 502, 503, 504])
        )
        allowed = retries_cfg.get("allowed_methods", ["GET"])
        allowed_set = frozenset(m.upper() for m in allowed)

        base_kwargs = dict(
            total=total,
            connect=connect,
            read=read,
            backoff_factor=backoff_factor,
            status_forcelist=status_forcelist,
        )

        try:
            r = Retry(
                **base_kwargs,
                allowed_methods=allowed_set,
                raise_on_status=False,
            )
        except TypeError:
            try:
                r = Retry(
                    **base_kwargs,
                    method_whitelist=allowed_set,
                    raise_on_status=False,
                )
            except TypeError:
                r = Retry(**base_kwargs, method_whitelist=allowed_set)

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
        Copy request-level defaults onto the session so *all* requests inherit them.
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
        Unified paginator for modes: none, salesforce, cursor, page, link-header.
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

                drop_keys = set(parse_cfg.get("json_drop_keys_any_depth", []))
                if drop_keys:
                    data = self._drop_keys_any_depth(data, drop_keys)

                df_page = self._json_obj_to_df(data, parse_cfg)
                if not df_page.empty:
                    frames.append(df_page)

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

        # Non-SF modes
        opts = dict(base_opts)
        safe = self._whitelist_request_opts(opts)

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
            page_df = self._to_dataframe(resp, parse_cfg)
            if mode == "page" and page_df.empty:
                break
            frames.append(page_df)
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

            elif mode == "link-header":
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
        Convert an already-decoded JSON object to a DataFrame, respecting json_record_path if provided.
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
        Supports csv, json, jsonl/ndjson.
        """
        parse_type = (parse_cfg.get("type") or "json").lower()
        if parse_type == "csv":
            csv_string = resp.content.decode("utf-8", errors="replace")
            return pd.read_csv(StringIO(csv_string))

        if parse_type in {"jsonl", "ndjson"}:
            text = resp.content.decode("utf-8", errors="replace")
            if not text.strip():
                return pd.DataFrame()
            return pd.read_json(StringIO(text), lines=True)

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
        Salesforce-specific backfill with HALF-OPEN windows [start, end).
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
            window_end = min(
                current + timedelta(days=window_days), end + timedelta(days=1)
            )
            start_str = current.strftime(date_format)
            end_str = window_end.strftime(date_format)

            soql = soql_template.format(
                date_field=date_field, start=start_str, end=end_str
            )

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
                    df = self._expand_links(
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

            except Exception as e:
                self._log_exception(
                    url, e, prefix=f"[{start_str} → {end_str}) "
                )
                raise

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
        Cursor-bounded backfill with optional stop conditions and link expansion.
        """
        safe = self._whitelist_request_opts(dict(base_opts))
        frames: List[pd.DataFrame] = []

        # seed
        start_value = (cur_cfg or {}).get("start_value")
        cursor_param = (pag_cfg or {}).get("cursor_param", "cursor")
        if start_value:
            params = dict(safe.get("params") or {})
            params[cursor_param] = start_value
            safe["params"] = params

        next_cursor_path = (pag_cfg or {}).get("next_cursor_path")
        chain_field = (pag_cfg or {}).get("chain_field")
        max_pages = int((pag_cfg or {}).get("max_pages", 10000))

        # stop-by-item
        stop_item = (cur_cfg or {}).get("stop_at_item") or {}
        stop_field = stop_item.get("field")
        stop_value = stop_item.get("value")
        stop_inclusive = bool(stop_item.get("inclusive", False))

        # stop-by-time
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

            # stop-by-item
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
                            sess,
                            df_page,
                            link_cfg,
                            parse_cfg,
                            table_name=None,
                            env_name=None,
                            api_output_cfg=None,
                        )

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
                        if link_cfg.get("enabled", False):
                            trimmed = self._expand_links(
                                sess,
                                trimmed,
                                link_cfg,
                                parse_cfg,
                                table_name=None,
                                env_name=None,
                                api_output_cfg=None,
                            )
                        frames.append(trimmed)
                    break

            # per-page link expansion
            if link_cfg.get("enabled", False) and not df_page.empty:
                df_page = self._expand_links(
                    sess,
                    df_page,
                    link_cfg,
                    parse_cfg,
                    table_name=None,
                    env_name=None,
                    api_output_cfg=None,
                )

            if not df_page.empty:
                frames.append(df_page)

            # compute next cursor
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
        *,
        table_name: Optional[str] = None,
        env_name: Optional[str] = None,
        api_output_cfg: Optional[Dict[str, Any]] = None,
    ) -> pd.DataFrame:
        """
        Follow URLs found in each row and aggregate those payloads into a DataFrame.

        - Inherits auth/headers/verify/proxies from the Session
        - If expanded responses need a different parse type, set link_cfg.type (e.g., 'jsonl')
        - If needed, override record path with link_cfg.json_record_path
        - Optional: extract session id from URL (link_expansion.session_id)
        - Optional: write per-link or per-session files using the same S3 writer
        """
        if df.empty:
            return df

        le_timeout = (link_cfg or {}).get(
            "timeout", parse_cfg.get("timeout", None)
        )
        url_fields = (link_cfg or {}).get("url_fields", [])
        if not url_fields:
            return df

        # optional flush
        flush_cfg = (link_cfg or {}).get("flush") or {}
        flush_mode = (flush_cfg.get("mode") or "none").lower()
        per_delay = float((link_cfg or {}).get("per_request_delay", 0.0))

        # optional session-id extraction
        sid_cfg = (link_cfg or {}).get("session_id") or {}
        sid_regex = sid_cfg.get("regex")
        sid_group = int(sid_cfg.get("group", 0))

        expanded_frames: List[pd.DataFrame] = []
        per_session_parts: Dict[str, List[pd.DataFrame]] = {}

        for _, row in df.iterrows():
            urls: List[str] = []
            for fld in url_fields:
                val = self._get_from_row(row, fld)
                if isinstance(val, str) and val.startswith(
                    ("http://", "https://")
                ):
                    urls.append(val)

            row_frames: List[pd.DataFrame] = []
            for u in urls:
                resp = sess.get(u, timeout=le_timeout or 30)
                self.log.info(
                    f"[link_expansion] GET {u} -> "
                    f"{getattr(resp, 'headers', {}).get('Content-Type', 'unknown')}, "
                    f"bytes={len(resp.content)}"
                )
                resp.raise_for_status()

                # Extract session id from the URL (if configured)
                session_id: Optional[str] = None
                if sid_regex:
                    m = re.search(sid_regex, u)
                    if m:
                        try:
                            session_id = str(m.group(sid_group) or "").strip()
                        except IndexError:
                            session_id = None
                if session_id:
                    self._expansion_session_ids.add(session_id)

                # Build parse override (type / record path)
                parse_override = dict(parse_cfg)
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

                df_part = self._to_dataframe(resp, parse_override)

                # Optional flush handling
                if (
                    flush_mode == "per_link"
                    and table_name
                    and env_name
                    and api_output_cfg is not None
                ):
                    self._flush_part(
                        df=df_part,
                        table_name=table_name,
                        env_name=env_name,
                        out_cfg=api_output_cfg,
                        session_id=session_id,
                        flush_cfg=flush_cfg,
                    )
                elif flush_mode == "per_session":
                    key = session_id or "none"
                    per_session_parts.setdefault(key, []).append(df_part)
                else:
                    row_frames.append(df_part)

                if per_delay > 0:
                    time.sleep(per_delay)

            if row_frames:
                merged = row_frames[0]
                for f in row_frames[1:]:
                    merged = merged.merge(
                        f, left_index=True, right_index=True, how="outer"
                    )
                expanded_frames.append(merged)

        # If per_session mode, write one file per session id (and still return the combined DF)
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
                self._flush_part(
                    df=df_session,
                    table_name=table_name,
                    env_name=env_name,
                    out_cfg=api_output_cfg,
                    session_id=(None if sid == "none" else sid),
                    flush_cfg=flush_cfg,
                )
                expanded_frames.append(df_session)

        if not expanded_frames:
            return df

        return pd.concat(expanded_frames, ignore_index=True)

    # ----------  Multi-pull + join helper ----------

    def _run_multi_pulls_and_join(
        self,
        *,
        sess: requests.Session,
        env_cfg: Dict[str, Any],
        base_req_opts: Dict[str, Any],
        api_cfg: Optional[Dict[str, Any]] = None,
        table_cfg: Optional[Dict[str, Any]] = None,
    ) -> pd.DataFrame:
        """
        Run multiple single GET pulls (no pagination) and join them.

        Accepts both `api_cfg` (effective table config without parse) and
        `table_cfg` (back-compat / place to carry table-level parse). We merge:
        cfg = {**(table_cfg or {}), **(api_cfg or {})}
        so that table-level `parse` is available for pull inheritance.
        """
        # Merge so `parse` from table_cfg is visible, while api_cfg can override other keys
        cfg_table = table_cfg or {}
        cfg_api = api_cfg or {}
        cfg = {**cfg_table, **cfg_api}

        pulls = cfg.get("multi_pulls") or []
        if not pulls:
            return pd.DataFrame()

        table_parse = cfg.get("parse", {}) or {}
        base_url = env_cfg["base_url"]

        frames: Dict[str, pd.DataFrame] = {}

        for pull in pulls:
            name = pull.get("name")
            if not name:
                raise ValueError(
                    "Each item in multi_pulls must have a non-empty 'name'."
                )

            # parse inheritance (table-level parse → per-pull override)
            pull_parse = {**table_parse, **(pull.get("parse") or {})}

            # inherit base opts; allow per-pull headers/params to extend/override
            eff_opts = dict(base_req_opts)
            if isinstance(pull.get("headers"), dict):
                eff_opts["headers"] = {
                    **(eff_opts.get("headers") or {}),
                    **pull["headers"],
                }
            if isinstance(pull.get("params"), dict):
                eff_opts["params"] = {
                    **(eff_opts.get("params") or {}),
                    **pull["params"],
                }

            # build per-pull URL (pull.path overrides table path if present)
            path = pull.get("path", cfg.get("path", ""))
            url = self._build_url(base_url, path)
            self._log_request(
                url,
                self._whitelist_request_opts(eff_opts),
                prefix=f"[multi_pulls:{name}] ",
            )

            # one GET (no pagination)
            resp = sess.get(url, **self._whitelist_request_opts(eff_opts))
            resp.raise_for_status()
            df_pull = self._to_dataframe(resp, pull_parse)
            frames[name] = df_pull

        # perform the join in pull order
        join_cfg = cfg.get("join") or {}
        how = (join_cfg.get("how") or "left").lower()
        on_keys = list(join_cfg.get("on") or [])
        if not on_keys:
            raise ValueError("join.on must list one or more column names.")

        order = [p["name"] for p in pulls]
        result = frames[order[0]].copy()
        for right_name in order[1:]:
            result = result.merge(
                frames[right_name], how=how, on=on_keys, copy=False
            )

        # select_from (keep / rename) – ensure join keys are preserved
        select_from = join_cfg.get("select_from") or {}
        if select_from:
            keep_cols: set[str] = set(on_keys)
            rename_map: Dict[str, str] = {}

            for src_name, sel in select_from.items():
                if not isinstance(sel, dict):
                    continue
                for col in sel.get("keep") or []:
                    keep_cols.add(col)
                for old, new in (sel.get("rename") or {}).items():
                    rename_map[old] = new
                    keep_cols.add(old)

            if rename_map:
                result = result.rename(columns=rename_map)

            translated_keep = [rename_map.get(c, c) for c in keep_cols]
            for k in on_keys:
                if k not in translated_keep and k in result.columns:
                    translated_keep.append(k)

            result = result.loc[
                :, [c for c in translated_keep if c in result.columns]
            ]

        return result

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
        Supports flat columns and dict-like columns (e.g., 'links.self').
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

    # resolve a single session_id to expose in final filename/prefix
    def _resolve_session_id(self, link_cfg: Dict[str, Any]) -> Optional[str]:
        if not self._expansion_session_ids:
            return None
        policy = ((link_cfg or {}).get("session_id") or {}).get(
            "policy", "first"
        )
        ids = list(self._expansion_session_ids)
        if policy == "last":
            return ids[-1]
        if policy == "all":
            return ",".join(sorted(set(ids)))
        if policy == "require_single":
            if len(set(ids)) != 1:
                raise ValueError(
                    f"Multiple session ids found: {sorted(set(ids))}"
                )
            return ids[0]
        return ids[0]  # default: first

    # per-link/per-session flush helper (reuses normal writer)
    def _flush_part(
        self,
        df: pd.DataFrame,
        table_name: str,
        env_name: str,
        out_cfg: Dict[str, Any],
        session_id: Optional[str],
        flush_cfg: Dict[str, Any],
    ) -> Dict[str, Any]:
        self._flush_seq += 1
        seq = self._flush_seq

        # clone s3 cfg and override prefix/filename if provided
        out_format = (out_cfg.get("format") or "csv").lower()
        s3_cfg = dict((out_cfg.get("s3") or {}).copy())
        prefix_tpl = (flush_cfg or {}).get("prefix")
        fname_tpl = (flush_cfg or {}).get("filename")
        if prefix_tpl:
            s3_cfg["prefix"] = prefix_tpl
        if fname_tpl:
            s3_cfg["filename"] = fname_tpl

        # set transient output context (session_id + seq)
        if session_id:
            self._current_output_ctx["session_id"] = session_id
        self._current_output_ctx["seq"] = seq

        try:
            meta = self._write_s3(df, table_name, env_name, out_format, s3_cfg)
            self.log.info(
                f"[flush] wrote part seq={seq} rows={len(df)} uri={meta.get('s3_uri')}"
            )
            return meta
        finally:
            # clear transient keys
            self._current_output_ctx.pop("seq", None)
            self._current_output_ctx.pop("session_id", None)

    def _stringify_non_scalars_for_parquet(
        self, df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Make a DataFrame Parquet-safe (minimal): convert dict/list (and other non-scalars)
        to JSON strings; keep scalars unchanged. This preserves data but loses nested columns.
        """
        if df.empty:
            return df

        def _to_json_if_needed(v):
            # Treat NaN/NA as-is
            if v is None or (isinstance(v, float) and np.isnan(v)):
                return v
            # Scalars we keep as-is
            if isinstance(v, (str, int, float, bool, pd.Timestamp)):
                return v
            # Everything else (dict, list, set, tuple, custom) → JSON string
            try:
                return json.dumps(v, default=str, ensure_ascii=False)
            except Exception:
                return str(v)

        # Only touch object-dtype columns (fast)
        out = df.copy()
        obj_cols = [c for c in out.columns if out[c].dtype == "object"]
        for c in obj_cols:
            out[c] = out[c].map(_to_json_if_needed)
        return out

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
            # allow placeholders from link-expansion flush/final write:
            **(self._current_output_ctx or {}),
        }
        # default session_id when referenced but unknown
        ctx.setdefault("session_id", "none")

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
            compression = (s3_cfg.get("compression") or "").lower()
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
            # MINIMAL FIX: stringify dict/list cells so pyarrow can write
            df_safe = self._stringify_non_scalars_for_parquet(df)
            buf = BytesIO()
            df_safe.to_parquet(buf, index=False, compression=compression)
            return buf.getvalue(), "application/vnd.apache.parquet", None

        raise ValueError(f"Unsupported format: {fmt}")

    def _log_request(self, url: str, opts: Dict[str, Any], prefix: str = ""):
        """
        Log request details with sensitive headers/params redacted.
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

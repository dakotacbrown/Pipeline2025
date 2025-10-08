from datetime import date, timedelta
from typing import Any, Dict, List

import pandas as pd

from api_ingestor.backfill import cursor_backfill, soql_window_backfill
from api_ingestor.config import prepare
from api_ingestor.link_expansion import expand_links, resolve_session_id
from api_ingestor.multipull import run_multi_pulls_with_join
from api_ingestor.output import write_output, write_s3
from api_ingestor.pagination import paginate
from api_ingestor.parsing import (
    to_dataframe as _to_dataframe,  # for ctx injection
)
from api_ingestor.request_helpers import (
    apply_session_defaults,
    build_session,
    build_url,
    log_exception,
    log_request,
)
from api_ingestor.small_utils import whitelist_request_opts


class ApiIngestor:
    """Thin orchestrator that wires the split helpers together."""

    def __init__(self, config: Dict[str, Any], log):
        self.config = config
        self.log = log

    # ------------ run_once ------------
    def run_once(self, table_name: str, env_name: str) -> Dict[str, Any]:
        started = pd.Timestamp.now(tz="UTC")
        self.log.info(f"[run_once] start table={table_name} env={env_name}")

        env_cfg, api_cfg, req_opts, parse_cfg = prepare(
            self.config, table_name, env_name
        )
        sess = build_session(req_opts.pop("retries", None))
        safe_defaults = whitelist_request_opts(req_opts)
        apply_session_defaults(sess, safe_defaults)

        # build ctx for helpers
        ctx: Dict[str, Any] = {
            "log": self.log,
            "table": table_name,
            "env": env_name,
            "current_output_ctx": {},
            "expansion_session_ids": set(),
            "flush_seq": 0,
            "to_dataframe": _to_dataframe,
            "write_s3": lambda df, fmt, s3_cfg: write_s3(
                ctx, ctx["table"], ctx["env"], df, fmt, s3_cfg
            ),
            "log_request": log_request,
            "log_exception": log_exception,
        }

        source_url = build_url(env_cfg["base_url"], api_cfg.get("path", ""))

        try:
            if api_cfg.get("multi_pulls"):
                df = run_multi_pulls_with_join(
                    ctx=ctx,
                    sess=sess,
                    base_url=env_cfg["base_url"],
                    api_cfg=api_cfg,
                    req_opts=safe_defaults,
                )
            else:
                log_request(ctx, source_url, safe_defaults)
                df = paginate(
                    sess,
                    source_url,
                    safe_defaults,
                    parse_cfg,
                    api_cfg.get("pagination"),
                )

            link_cfg = api_cfg.get("link_expansion") or {}
            if link_cfg.get("enabled", False) and not df.empty:
                df = expand_links(
                    ctx,
                    sess,
                    df,
                    link_cfg,
                    parse_default=parse_cfg,
                    table_name=table_name,
                    env_name=env_name,
                    api_output_cfg=(api_cfg.get("output") or {}),
                )

            sid = resolve_session_id(ctx["expansion_session_ids"], link_cfg)
            if sid:
                ctx["current_output_ctx"]["session_id"] = sid

            flush_cfg = link_cfg.get("flush") or {}
            if bool(flush_cfg.get("only")):
                self.log.info(
                    "[run_once] only_flush=true -> skipping aggregate write"
                )
                ended = pd.Timestamp.now(tz="UTC")
                ctx["current_output_ctx"].pop("session_id", None)
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
                    "source_url": source_url,
                    "pagination_mode": (api_cfg.get("pagination") or {}).get(
                        "mode", "none"
                    ),
                }

            out_meta = write_output(ctx, df, api_cfg.get("output") or {})
            ended = pd.Timestamp.now(tz="UTC")
            ctx["current_output_ctx"].pop("session_id", None)

            self.log.info(
                f"[run_once] done table={table_name} env={env_name} rows={len(df)} "
                f"duration={(ended - started).total_seconds():.3f}s dest={out_meta.get('s3_uri')}"
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
                "source_url": source_url,
                "pagination_mode": (api_cfg.get("pagination") or {}).get(
                    "mode", "none"
                ),
            }
        except Exception as e:
            log_exception(ctx, source_url, e)
            raise

    # ------------ run_backfill ------------
    def run_backfill(
        self, table_name: str, env_name: str, start: date, end: date
    ) -> Dict[str, Any]:
        started = pd.Timestamp.now(tz="UTC")
        self.log.info(
            f"[run_backfill] start table={table_name} env={env_name} range={start}..{end}"
        )

        env_cfg, api_cfg, base_req_opts, parse_cfg = prepare(
            self.config, table_name, env_name
        )

        bf = api_cfg.get("backfill", {}) or {}
        if not bf.get("enabled", False):
            raise ValueError(
                f"Backfill is not enabled for '{table_name}' in config."
            )

        sess = build_session(base_req_opts.pop("retries", None))
        safe_defaults = whitelist_request_opts(base_req_opts)
        apply_session_defaults(sess, safe_defaults)

        ctx: Dict[str, Any] = {
            "log": self.log,
            "table": table_name,
            "env": env_name,
            "current_output_ctx": {},
            "expansion_session_ids": set(),
            "flush_seq": 0,
            "to_dataframe": _to_dataframe,
            "write_s3": lambda df, fmt, s3_cfg: write_s3(
                ctx, ctx["table"], ctx["env"], df, fmt, s3_cfg
            ),
            "log_request": log_request,
            "log_exception": log_exception,
        }

        url = build_url(env_cfg["base_url"], api_cfg.get("path", ""))
        strategy = (bf.get("strategy") or "date").lower()
        pag_mode = (api_cfg.get("pagination") or {}).get("mode", "none").lower()
        link_cfg = api_cfg.get("link_expansion") or {}

        # cursor
        if strategy == "cursor":
            df = cursor_backfill(
                ctx,
                sess,
                url,
                base_opts=base_req_opts,
                parse_cfg=parse_cfg,
                pag_cfg=api_cfg.get("pagination") or {},
                cur_cfg=bf.get("cursor") or {},
                link_cfg=link_cfg,
            )
            sid = resolve_session_id(ctx["expansion_session_ids"], link_cfg)
            if sid:
                ctx["current_output_ctx"]["session_id"] = sid
            flush_cfg = link_cfg.get("flush") or {}
            if bool(flush_cfg.get("only")):
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
                    "pagination_mode": pag_mode,
                }
            out_meta = write_output(ctx, df, api_cfg.get("output") or {})
            ended = pd.Timestamp.now(tz="UTC")
            self.log.info(
                f"[run_backfill] done strategy=cursor table={table_name} env={env_name} "
                f"rows={len(df)} duration={(ended - started).total_seconds():.3f}s dest={out_meta.get('s3_uri')}"
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

        # soql_window
        if strategy == "soql_window":
            if pag_mode != "salesforce":
                raise ValueError(
                    "soql_window strategy requires pagination.mode == 'salesforce'."
                )
            df = soql_window_backfill(
                ctx,
                sess,
                url,
                base_opts=base_req_opts,
                parse_cfg=parse_cfg,
                pag_cfg=api_cfg.get("pagination") or {},
                bf_cfg=bf,
                start=start,
                end=end,
                link_cfg=link_cfg,
            )
            sid = resolve_session_id(ctx["expansion_session_ids"], link_cfg)
            if sid:
                ctx["current_output_ctx"]["session_id"] = sid
            out_meta = write_output(ctx, df, api_cfg.get("output") or {})
            ended = pd.Timestamp.now(tz="UTC")
            self.log.info(
                f"[run_backfill] done strategy=soql_window table={table_name} env={env_name} "
                f"rows={len(df)} duration={(ended - started).total_seconds():.3f}s dest={out_meta.get('s3_uri')}"
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

        # default date-window (with optional multi-pulls on each window)
        window_days = int(bf.get("window_days", 7))
        start_param = bf.get("start_param", "start_date")
        end_param = bf.get("end_param", "end_date")
        date_format = bf.get("date_format", "%Y-%m-%d")
        per_request_delay = float(bf.get("per_request_delay", 0.0))

        has_multi = bool(api_cfg.get("multi_pulls"))
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
            safe = whitelist_request_opts(req_opts)

            ctx["log_request"](
                ctx,
                url,
                safe,
                prefix=f"[{params[start_param]} → {params[end_param]}] ",
            )

            try:
                if has_multi:
                    df_win = run_multi_pulls_with_join(
                        ctx, sess, env_cfg["base_url"], api_cfg, safe
                    )
                else:
                    df_win = paginate(
                        sess, url, safe, parse_cfg, api_cfg.get("pagination")
                    )
                if (link_cfg.get("enabled", False)) and not df_win.empty:
                    df_win = expand_links(
                        ctx,
                        sess,
                        df_win,
                        link_cfg,
                        parse_default=parse_cfg,
                        table_name=table_name,
                        env_name=env_name,
                        api_output_cfg=(api_cfg.get("output") or {}),
                    )
                all_frames.append(df_win)
                num_windows += 1
                if per_request_delay > 0:
                    import time as _t

                    _t.sleep(per_request_delay)
            except Exception as e:
                ctx["log_exception"](
                    ctx,
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
        sid = resolve_session_id(ctx["expansion_session_ids"], link_cfg)
        if sid:
            ctx["current_output_ctx"]["session_id"] = sid
        out_meta = write_output(ctx, result, api_cfg.get("output") or {})
        ended = pd.Timestamp.now(tz="UTC")

        self.log.info(
            f"[run_backfill] done strategy=date table={table_name} env={env_name} "
            f"windows={num_windows} rows={len(result)} duration={(ended - started).total_seconds():.3f}s "
            f"dest={out_meta.get('s3_uri')}"
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

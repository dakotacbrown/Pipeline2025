import gzip
import json
from io import BytesIO
from typing import Any, Dict, Optional, Tuple

import numpy as np
import pandas as pd


def stringify_non_scalars_for_parquet(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    def _to_json(v):
        if v is None or (isinstance(v, float) and np.isnan(v)):
            return v
        if isinstance(v, (str, int, float, bool, pd.Timestamp)):
            return v
        try:
            return json.dumps(v, default=str, ensure_ascii=False)
        except Exception:
            return str(v)

    out = df.copy()
    obj_cols = [c for c in out.columns if out[c].dtype == "object"]
    for c in obj_cols:
        out[c] = out[c].map(_to_json)
    return out


def serialize_df(
    df: pd.DataFrame, fmt: str, s3_cfg: Dict[str, Any]
) -> Tuple[bytes, Optional[str], Optional[str]]:
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
        df_safe = stringify_non_scalars_for_parquet(df)
        buf = BytesIO()
        df_safe.to_parquet(buf, index=False, compression=compression)
        return buf.getvalue(), "application/vnd.apache.parquet", None

    raise ValueError(f"Unsupported format: {fmt}")


def write_s3(
    ctx: Dict[str, Any],
    table: str,
    env: str,
    df: pd.DataFrame,
    fmt: str,
    s3_cfg: Dict[str, Any],
) -> Dict[str, Any]:
    try:
        import boto3
    except Exception as e:
        raise RuntimeError("boto3 is required for S3 output.") from e

    now = pd.Timestamp.now(tz="UTC").to_pydatetime()
    context_vars = {
        "table": table,
        "env": env,
        "now": now,
        "today": now,
        **(ctx.get("current_output_ctx") or {}),
    }
    context_vars.setdefault("session_id", "none")

    bucket = (s3_cfg.get("bucket") or "").strip()
    if not bucket:
        raise ValueError("output.s3.bucket is required.")

    prefix = (s3_cfg.get("prefix") or "").format(**context_vars).strip("/")
    ext = {"csv": "csv", "parquet": "parquet", "jsonl": "jsonl"}[fmt]
    default_fname = "{table}-{now:%Y%m%dT%H%M%SZ}." + ext
    filename = (s3_cfg.get("filename") or default_fname).format(**context_vars)
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

    body_bytes, content_type, content_encoding = serialize_df(df, fmt, s3_cfg)
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
    ctx["log"].info(
        f"[output] Wrote {len(df)} rows ({meta['bytes']} bytes) to {meta['s3_uri']}"
    )
    return meta


def write_output(
    ctx: Dict[str, Any], df: pd.DataFrame, out_cfg: Dict[str, Any]
) -> Dict[str, Any]:
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

    return write_s3(ctx, ctx["table"], ctx["env"], df, fmt, s3_cfg)

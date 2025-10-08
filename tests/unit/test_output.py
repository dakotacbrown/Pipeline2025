import gzip
import json
import types
from io import BytesIO

import pandas as pd
import pytest

from api_ingestor import output

# ---------- helpers ----------


class CaptureLog:
    def info(self, *a, **k):
        pass

    def error(self, *a, **k):
        pass


@pytest.fixture
def ctx():
    # include table/env so write_output() can resolve placeholders
    return {
        "log": CaptureLog(),
        "table": "tbl",
        "env": "prod",
        "current_output_ctx": {},  # used for {session_id}, {seq}, etc
    }


# We’ll stub boto3 for write_s3 tests
@pytest.fixture
def patch_boto3(monkeypatch):
    uploads = []

    class Client:
        def put_object(self, Bucket, Key, Body, **kw):
            uploads.append(
                {"Bucket": Bucket, "Key": Key, "Body": Body, "kw": kw}
            )

    class Session:
        def __init__(self, region_name=None):
            pass

        def client(self, name, endpoint_url=None):
            return Client()

    fake_boto3 = types.SimpleNamespace(
        session=types.SimpleNamespace(Session=Session)
    )
    monkeypatch.setitem(__import__("sys").modules, "boto3", fake_boto3)
    return uploads


# ---------- stringify_non_scalars_for_parquet ----------


def test_stringify_non_scalars_for_parquet_scalars_unchanged():
    df = pd.DataFrame([{"a": 1, "b": "x", "c": True}])
    out = output.stringify_non_scalars_for_parquet(df)
    assert out.equals(df)


def test_stringify_non_scalars_for_parquet_nested_to_json():
    df = pd.DataFrame([{"a": {"k": 1}, "b": [1, 2, 3], "c": "ok"}])
    out = output.stringify_non_scalars_for_parquet(df)
    # nested fields become JSON strings
    assert (
        isinstance(out.loc[0, "a"], str)
        and json.loads(out.loc[0, "a"])["k"] == 1
    )
    assert isinstance(out.loc[0, "b"], str) and json.loads(out.loc[0, "b"]) == [
        1,
        2,
        3,
    ]
    assert out.loc[0, "c"] == "ok"


# ---------- serialize_df: csv ----------


def test_serialize_df_csv_plain():
    df = pd.DataFrame([{"a": 1}])
    raw, ctype, enc = output.serialize_df(df, "csv", {"index": False})
    assert ctype == "text/csv" and enc is None
    text = raw.decode("utf-8").strip()
    assert "a" in text and "1" in text


def test_serialize_df_csv_gzip():
    df = pd.DataFrame([{"a": 1}])
    raw, ctype, enc = output.serialize_df(
        df, "csv", {"index": False, "compression": "gzip"}
    )
    assert ctype == "text/csv" and enc == "gzip"
    # gunzip and verify contents
    buf = BytesIO(raw)
    with gzip.GzipFile(fileobj=buf, mode="rb") as gz:
        txt = gz.read().decode("utf-8")
    assert "a" in txt and "1" in txt


# ---------- serialize_df: jsonl ----------


def test_serialize_df_jsonl():
    df = pd.DataFrame([{"a": 1}, {"a": 2}])
    raw, ctype, enc = output.serialize_df(df, "jsonl", {})
    assert ctype == "application/x-ndjson" and enc is None
    lines = raw.decode("utf-8").strip().splitlines()
    assert len(lines) == 2 and json.loads(lines[0])["a"] == 1


# ---------- serialize_df: parquet (patch DataFrame.to_parquet) ----------


def test_serialize_df_parquet_monkeypatch(monkeypatch):
    df = pd.DataFrame([{"a": {"k": 1}}, {"a": {"k": 2}}])

    def fake_to_parquet(self, buf, index=False, compression=None):
        # Write a tiny marker so we can assert bytes were produced
        buf.write(b"PARQUET-BYTES")

    monkeypatch.setattr(
        pd.DataFrame, "to_parquet", fake_to_parquet, raising=True
    )
    raw, ctype, enc = output.serialize_df(
        df, "parquet", {"compression": "snappy"}
    )
    assert ctype == "application/vnd.apache.parquet" and enc is None
    assert b"PARQUET-BYTES" in raw


# ---------- write_s3 ----------


def test_write_s3_invokes_boto_and_formats_key(ctx, patch_boto3):
    # Provide session_id/seq to test templating
    ctx["current_output_ctx"]["session_id"] = "abc"
    df = pd.DataFrame([{"a": 1}])
    meta = output.write_s3(
        ctx,
        table="tbl",
        env="prod",
        df=df,
        fmt="csv",
        s3_cfg={
            "bucket": "bkt",
            "prefix": "p/{table}/{env}/{session_id}",
            "filename": "x.csv",
        },
    )
    assert meta["s3_bucket"] == "bkt"
    assert meta["s3_key"].startswith("p/tbl/prod/abc/x.csv")
    assert meta["format"] == "csv"
    # logged via ctx["log"].info (no assertion needed, just exercised)


def test_write_s3_sets_headers_for_csv_gzip(ctx, patch_boto3):
    df = pd.DataFrame([{"a": 1}])
    # capture extra args via uploads fixture
    uploads = patch_boto3  # already set
    meta = output.write_s3(
        ctx,
        table="tbl",
        env="prod",
        df=df,
        fmt="csv",
        s3_cfg={
            "bucket": "bkt",
            "prefix": "p",
            "filename": "x.csv",
            "compression": "gzip",
        },
    )
    # Ensure last upload had correct headers
    last = uploads[-1]
    assert last["kw"]["ContentType"] == "text/csv"
    assert last["kw"]["ContentEncoding"] == "gzip"
    assert meta["bytes"] == len(last["Body"])


# ---------- write_output ----------


def test_write_output_success_path(ctx, monkeypatch):
    # stub write_s3 to avoid boto3 here
    called = {}

    def fake_write_s3(ctx_in, table, env, df, fmt, s3_cfg):
        called.update(
            {
                "table": table,
                "env": env,
                "fmt": fmt,
                "rows": len(df),
                "bucket": s3_cfg.get("bucket"),
            }
        )
        return {
            "format": fmt,
            "s3_bucket": s3_cfg["bucket"],
            "s3_key": "k",
            "s3_uri": "s3://b/k",
            "bytes": 10,
        }

    monkeypatch.setattr(output, "write_s3", fake_write_s3, raising=True)

    df = pd.DataFrame([{"a": 1}])
    meta = output.write_output(
        ctx, df, {"format": "jsonl", "s3": {"bucket": "b"}}
    )
    assert meta["format"] == "jsonl" and called["fmt"] == "jsonl"
    assert (
        called["table"] == "tbl"
        and called["env"] == "prod"
        and called["rows"] == 1
    )


def test_write_output_errors(ctx):
    df = pd.DataFrame([{"a": 1}])
    # missing out_cfg
    with pytest.raises(ValueError):
        output.write_output(ctx, df, {})
    # unsupported format
    with pytest.raises(ValueError):
        output.write_output(ctx, df, {"format": "xml", "s3": {"bucket": "b"}})
    # missing s3 sink
    with pytest.raises(ValueError):
        output.write_output(ctx, df, {"format": "csv"})
    # write_empty = False with empty df
    with pytest.raises(ValueError):
        output.write_output(
            ctx,
            pd.DataFrame(),
            {"format": "csv", "write_empty": False, "s3": {"bucket": "b"}},
        )

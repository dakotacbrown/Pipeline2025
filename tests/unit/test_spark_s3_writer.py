import types
from datetime import date
from pathlib import Path

import pytest

import writer.spark_s3_writer as s3w_mod
from writer.spark_s3_writer import SparkS3Writer


def _local_format_like_prod(root: Path):
    """
    Return a small module-like object that exposes format_clz_s3_prefix(cfg),
    mirroring your production format but using a local path instead of s3a://.
    """

    def format_clz_s3_prefix(cfg):
        today = date.today()
        base = root / cfg["vendor"] / cfg["dataset_id"]
        if cfg.get("table"):
            base = base / cfg["table"]
        # same y/m/d + "filename.ext" suffix
        dest = (
            base
            / f"year={today.year}"
            / f"month={today.month:02d}"
            / f"day={today.day:02d}"
            / f"{cfg['output_name']}.{cfg['file_type'].lower()}"
        )
        return str(dest)

    return types.SimpleNamespace(format_clz_s3_prefix=format_clz_s3_prefix)


def test_csv_write_uses_partitioned_path_shape(
    spark, log, tmp_path, monkeypatch
):
    monkeypatch.setattr(
        s3w_mod, "hf", _local_format_like_prod(tmp_path), raising=False
    )

    writer = SparkS3Writer(log=log, spark_session=spark)
    df = spark.createDataFrame([(1, "x"), (2, "y")], "a int, b string")

    cfg = {
        "s3_bucket": "ignored",
        "vendor": "ven",
        "dataset_id": "ds",
        "output_name": "out",
        "file_type": "csv",
        "mode": "overwrite",
        "options": {"header": "true"},
    }
    # path is computed inside the writer; we just write
    writer.write_dataframe(df, cfg)

    # reconstruct expected root and find the leaf path
    # (the helper returned "<tmp>/ven/ds/year=YYYY/month=MM/day=DD/out.csv")
    # Spark writes a DIRECTORY at that path containing part files
    leaf_dir = Path(s3w_mod.hf.format_clz_s3_prefix(cfg))
    assert leaf_dir.exists() and leaf_dir.is_dir()

    back = spark.read.option("header", True).csv(str(leaf_dir))
    assert set(back.columns) == {"a", "b"}
    assert back.count() == 2


def test_jsonl_write_with_table_segment(spark, log, tmp_path, monkeypatch):
    monkeypatch.setattr(
        s3w_mod, "hf", _local_format_like_prod(tmp_path), raising=False
    )

    writer = SparkS3Writer(log=log, spark_session=spark)
    df = spark.createDataFrame([(1, "x")], "a int, b string")

    cfg = {
        "s3_bucket": "ignored",
        "vendor": "ven",
        "dataset_id": "ds",
        "table": "events",
        "output_name": "payload",
        "file_type": "jsonl",
        "mode": "overwrite",
    }
    writer.write_dataframe(df, cfg)

    leaf_dir = Path(s3w_mod.hf.format_clz_s3_prefix(cfg))
    # Ensure the "table" segment is present
    assert "events" in str(leaf_dir)
    assert leaf_dir.exists() and leaf_dir.is_dir()

    back = spark.read.json(str(leaf_dir))
    assert set(back.columns) == {"a", "b"}
    assert back.count() == 1


def test_parquet_write_partition_and_append(spark, log, tmp_path, monkeypatch):
    monkeypatch.setattr(
        s3w_mod, "hf", _local_format_like_prod(tmp_path), raising=False
    )

    writer = SparkS3Writer(log=log, spark_session=spark)
    df1 = spark.createDataFrame([(1, "x", 1)], "a int, b string, p int")
    df2 = spark.createDataFrame([(2, "y", 2)], "a int, b string, p int")

    cfg = {
        "s3_bucket": "ignored",
        "vendor": "ven",
        "dataset_id": "ds",
        "output_name": "pq",
        "file_type": "parquet",
        "mode": "overwrite",
        "partition_by": ["p"],
    }
    writer.write_dataframe(df1, cfg)

    # append more rows
    cfg["mode"] = "append"
    writer.write_dataframe(df2, cfg)

    leaf_dir = Path(s3w_mod.hf.format_clz_s3_prefix(cfg))
    # partition folders like p=1/, p=2/ should exist under the leaf dir
    assert any(d.name.startswith("p=") for d in leaf_dir.iterdir())

    back = spark.read.parquet(str(leaf_dir))
    assert back.count() == 2
    assert set(back.columns) == {"a", "b", "p"}


def test_bad_type_raises(spark, log, tmp_path, monkeypatch):
    monkeypatch.setattr(
        s3w_mod, "hf", _local_format_like_prod(tmp_path), raising=False
    )
    writer = SparkS3Writer(log=log, spark_session=spark)
    with pytest.raises(ValueError):
        writer.write_dataframe(
            spark.createDataFrame([(1,)], "x int"),
            {
                "s3_bucket": "ignored",
                "vendor": "ven",
                "dataset_id": "ds",
                "output_name": "bad",
                "file_type": "xlsx",
            },
        )

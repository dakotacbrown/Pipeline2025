# tests/test_spark_s3_reader.py
import os
from typing import List, Tuple
from unittest.mock import Mock

import pytest
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from reader.spark_s3_reader import SparkS3Reader

# ---------- helpers ----------


def make_schema_ab() -> StructType:
    return StructType(
        [
            StructField("b", IntegerType(), False),
            StructField("a", StringType(), False),
        ]
    )


def write_sample_csv(spark, base: str) -> str:
    df = spark.createDataFrame([(1, "x"), (2, "y")], ["a", "b"])
    path = os.path.join(base, "csv_dir")
    (
        df.select("a", "b")
        .write.mode("overwrite")
        .option("header", True)
        .csv(path)
    )
    return path  # spark expects directory for CSV


def write_sample_jsonl(base: str) -> str:
    path = os.path.join(base, "data.jsonl")
    with open(path, "w", encoding="utf-8") as f:
        f.write('{"a":"x","b":1}\n')
        f.write('{"a":"y","b":2}\n')
    return path


def write_sample_parquet(spark, base: str) -> str:
    df = spark.createDataFrame([(1, "x"), (2, "y")], ["a", "b"])
    path = os.path.join(base, "pq_dir")
    df.write.mode("overwrite").parquet(path)
    return path


# ---------- tests ----------


def test_csv_with_schema_and_options_preserves_order(
    spark, log, tmp_path, monkeypatch
):
    csv_dir = write_sample_csv(spark, str(tmp_path))

    reader = SparkS3Reader(log=log, spark_session=spark)
    # bypass S3 by returning a local path
    monkeypatch.setattr(reader, "_find_file", lambda p, n, b: csv_dir)

    cfg = {
        "s3_bucket": "n/a",
        "file_path": "n/a",
        "input_file_name": "n/a",
        "file_type": "csv",
        "options": {"header": "true", "inferSchema": "false"},
        "schema": make_schema_ab(),
    }
    df = reader.read_to_dataframe(cfg)

    assert df.columns == ["b", "a"]
    assert [f.nullable for f in df.schema.fields] == [True, True]
    rows: List[Tuple[int, str]] = [tuple(r) for r in df.orderBy("b").collect()]
    assert rows == [(1, "x"), (2, "y")]


def test_jsonl_read_with_schema(spark, log, tmp_path, monkeypatch):
    jsonl_path = write_sample_jsonl(str(tmp_path))

    reader = SparkS3Reader(log=log, spark_session=spark)
    monkeypatch.setattr(reader, "_find_file", lambda p, n, b: jsonl_path)

    cfg = {
        "s3_bucket": "n/a",
        "file_path": "n/a",
        "input_file_name": "n/a",
        "file_type": "jsonl",
        "options": {},
        "schema": make_schema_ab(),
    }
    df = reader.read_to_dataframe(cfg)

    assert df.columns == ["b", "a"]
    assert set(map(tuple, df.collect())) == {(1, "x"), (2, "y")}


def test_parquet_read_no_schema(spark, log, tmp_path, monkeypatch):
    pq_dir = write_sample_parquet(spark, str(tmp_path))

    reader = SparkS3Reader(log=log, spark_session=spark)
    monkeypatch.setattr(reader, "_find_file", lambda p, n, b: pq_dir)

    cfg = {
        "s3_bucket": "n/a",
        "file_path": "n/a",
        "input_file_name": "n/a",
        "file_type": "parquet",
        "options": {},
    }
    df = reader.read_to_dataframe(cfg)

    assert set(df.columns) == {"a", "b"}
    assert df.count() == 2


def test_unsupported_type_raises(spark, log, monkeypatch):
    reader = SparkS3Reader(log=log, spark_session=spark)
    monkeypatch.setattr(
        reader, "_find_file", lambda p, n, b: "/tmp/does-not-matter"
    )

    with pytest.raises(ValueError):
        reader.read_to_dataframe(
            {
                "s3_bucket": "n/a",
                "file_path": "n/a",
                "input_file_name": "n/a",
                "file_type": "xlsx",
            }
        )


def test_find_file_uses_paginator_and_regex(log, spark, monkeypatch):
    # Fake paginator pages
    pages = [
        {
            "Contents": [
                {"Key": "landing/foo/data_2025-01-01.json"},
                {"Key": "landing/foo/data_2025-01-02.json"},
            ]
        }
    ]
    fake_paginator = Mock()
    fake_paginator.paginate.return_value = pages

    fake_client = Mock()
    fake_client.get_paginator.return_value = fake_paginator

    monkeypatch.setattr("boto3.client", lambda service: fake_client)

    reader = SparkS3Reader(log=log, spark_session=spark)
    out = reader._find_file(
        file_path="landing/foo/",
        file_name=r"2025-01-02\.json$",
        s3_bucket="my-bucket",
    )

    assert out == "s3a://my-bucket/landing/foo/data_2025-01-02.json"
    fake_client.get_paginator.assert_called_with("list_objects_v2")
    fake_paginator.paginate.assert_called_with(
        Bucket="my-bucket", Prefix="landing/foo/"
    )


def test_empty_csv_returns_typed_empty_df_when_schema_given(
    spark, log, tmp_path, monkeypatch
):
    # Header-only CSV
    path = os.path.join(str(tmp_path), "header_only")
    os.makedirs(path, exist_ok=True)
    with open(os.path.join(path, "part-0000.csv"), "w", encoding="utf-8") as f:
        f.write("a,b\n")

    reader = SparkS3Reader(log=log, spark_session=spark)
    monkeypatch.setattr(reader, "_find_file", lambda p, n, b: path)

    cfg = {
        "s3_bucket": "n/a",
        "file_path": "n/a",
        "input_file_name": "n/a",
        "file_type": "csv",
        "options": {"header": "true"},
        "schema": make_schema_ab(),
    }
    df = reader.read_to_dataframe(cfg)

    assert df.columns == ["b", "a"]
    assert df.count() == 0
    assert [
        (f.name, f.dataType.simpleString(), f.nullable)
        for f in df.schema.fields
    ] == [
        ("b", "int", True),
        ("a", "string", True),
    ]

# test_deduplicate.py
import logging

import pandas as pd
import pytest

# ---------- Spark test setup ----------
from pyspark.sql import SparkSession

from deduplicate.deduplication import (
    remove_duplicates_pandas,
    remove_duplicates_spark,
)


@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder.master("local[1]")
        .appName("deduplicate-tests")
        .getOrCreate()
    )
    # Keep deterministic shuffle for tests
    spark.conf.set("spark.sql.shuffle.partitions", "1")
    yield spark
    spark.stop()


# ---------- Pandas tests ----------


def test_remove_duplicates_pandas_all_columns(caplog):
    data = {
        "id": [1, 1, 2, 3, 3],
        "value": ["A", "A", "B", "C", "C"],
    }
    df = pd.DataFrame(data)

    logger = logging.getLogger("pandas-dedup-test")

    with caplog.at_level(logging.INFO):
        out = remove_duplicates_pandas(df, log=logger)

    # Expect to keep 1,2,3 (unique full rows)
    assert len(out) == 3
    assert set(map(tuple, out.values.tolist())) == {
        (1, "A"),
        (2, "B"),
        (3, "C"),
    }

    # Logging assertions
    assert any("initial rows: 5" in rec.message for rec in caplog.records)
    assert any("final rows: 3" in rec.message for rec in caplog.records)
    assert any("duplicates removed: 2" in rec.message for rec in caplog.records)
    assert any(
        "subset used: ALL COLUMNS" in rec.message for rec in caplog.records
    )


def test_remove_duplicates_pandas_subset_keep_last(caplog):
    data = {
        "id": [1, 1, 2, 3, 3],
        "val": ["x", "y", "b", "c", "d"],  # different secondary values
    }
    df = pd.DataFrame(data)
    logger = logging.getLogger("pandas-dedup-test-keep-last")

    with caplog.at_level(logging.INFO):
        out = remove_duplicates_pandas(
            df, log=logger, subset=["id"], keep="last"
        )

    # For id=1 keep last row ("y"), for id=3 keep last row ("d"), id=2 stays
    # Order may reset due to reset_index(drop=True), so assert by grouping
    expected = pd.DataFrame({"id": [1, 2, 3], "val": ["y", "b", "d"]})
    # Compare as sets of tuples to ignore order
    assert set(map(tuple, out[["id", "val"]].values.tolist())) == set(
        map(tuple, expected.values.tolist())
    )

    assert any("subset used: ['id']" in rec.message for rec in caplog.records)
    # initial 5, final 3, removed 2
    assert any("initial rows: 5" in rec.message for rec in caplog.records)
    assert any("final rows: 3" in rec.message for rec in caplog.records)
    assert any("duplicates removed: 2" in rec.message for rec in caplog.records)


# ---------- Spark tests ----------


def test_remove_duplicates_spark_all_columns(spark, caplog):
    rows = [(1, "A"), (1, "A"), (2, "B"), (3, "C"), (3, "C")]
    df = spark.createDataFrame(rows, ["id", "value"])
    logger = logging.getLogger("spark-dedup-test")

    with caplog.at_level(logging.INFO):
        out = remove_duplicates_spark(df, log=logger)

    # Expect distinct full rows: (1,A), (2,B), (3,C)
    assert out.count() == 3
    assert set(map(tuple, out.collect())) == {(1, "A"), (2, "B"), (3, "C")}

    # Logging assertions
    msgs = [rec.message for rec in caplog.records]
    assert any("initial rows: 5" in m for m in msgs)
    assert any("final rows: 3" in m for m in msgs)
    assert any("duplicates removed: 2" in m for m in msgs)
    assert any("subset used: ALL COLUMNS" in m for m in msgs)


def test_remove_duplicates_spark_subset(spark, caplog):
    # When deduping by subset, PySpark's dropDuplicates keeps an arbitrary row per subset key.
    # So we assert uniqueness/count on the subset and not the non-key columns' exact values.
    rows = [
        (1, "x"),
        (1, "y"),
        (2, "b"),
        (3, "c"),
        (3, "d"),
    ]
    df = spark.createDataFrame(rows, ["id", "val"])
    logger = logging.getLogger("spark-dedup-test-subset")

    with caplog.at_level(logging.INFO):
        out = remove_duplicates_spark(df, log=logger, subset=["id"])

    # Expect unique ids {1,2,3} -> 3 rows
    assert out.select("id").distinct().count() == 3
    assert out.count() == 3
    assert set(r.id for r in out.select("id").collect()) == {1, 2, 3}

    msgs = [rec.message for rec in caplog.records]
    assert any("subset used: ['id']" in m for m in msgs)
    assert any("initial rows: 5" in m for m in msgs)
    assert any("final rows: 3" in m for m in msgs)
    assert any("duplicates removed: 2" in m for m in msgs)

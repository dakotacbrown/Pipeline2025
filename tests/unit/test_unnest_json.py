import os

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from unnest.json import unnest_json_column

# --- Make Spark imports safe ---
pytest.importorskip("pyspark", reason="PySpark not installed")
from pyspark.sql import SparkSession
from pyspark.sql import types as T

from unnest.json import unnest_json_column_spark


def _order(df: pd.DataFrame) -> pd.DataFrame:
    return df.reset_index(drop=True).reindex(sorted(df.columns), axis=1)


def test_deep_flatten_with_underscores():
    df = pd.DataFrame(
        {
            "id": [1],
            "payload": [
                {
                    "a": 1,
                    "b": {"c": 2, "d": {"e": 3}},
                }
            ],
        }
    )
    out = unnest_json_column(df, "payload", sep="_", keep_original=False)
    expected = pd.DataFrame(
        {
            "id": [1],
            "a": [1],
            "b_c": [2],
            "b_d_e": [3],
        }
    )
    assert_frame_equal(_order(out), _order(expected))


def test_nested_arrays_of_structs_explode_outer():
    df = pd.DataFrame(
        {
            "id": [1],
            "payload": [
                {"arr": [{"x": 10}, {"x": 20, "y": [1, 2]}], "b": {"c": 2}}
            ],
        }
    )
    out = unnest_json_column(df, "payload", sep="_", keep_original=False)

    # Expect 3 rows:
    # - (x=10, y=None)
    # - (x=20, y=1)
    # - (x=20, y=2)
    assert len(out) == 3
    assert set(out["id"]) == {1}
    assert set(out["b_c"]) == {2}

    def _none_if_nan(x):
        return x if pd.notna(x) else None

    # Validate the (x, y) pairs; coerce NaN -> None for comparison
    pairs = {
        (x, _none_if_nan(y))
        for x, y in zip(
            out["arr_x"], out.get("arr_y", pd.Series([None] * len(out)))
        )
    }
    assert pairs == {(10, None), (20, 1), (20, 2)}


def test_empty_arrays_preserved_single_row():
    df = pd.DataFrame({"id": [1], "payload": [{"empty": []}]})
    out = unnest_json_column(df, "payload", sep="_", keep_original=False)
    # one row preserved, 'empty' becomes None after explode-outer style handling
    assert len(out) == 1
    assert (
        "empty" in out.columns
    )  # may not create a column if all values were [] -> handled as None dict; so tolerate both
    # If column exists, ensure it is NA/None
    if "empty" in out.columns:
        assert pd.isna(out.loc[0, "empty"])


def test_invalid_json_ignore_results_in_no_new_cols():
    df = pd.DataFrame({"id": [1], "payload": ['{"not": "closed"']})
    out = unnest_json_column(
        df, "payload", errors="ignore", keep_original=False
    )
    assert list(out.columns) == ["id"]
    assert out.iloc[0]["id"] == 1


def test_prefix_and_keep_original_true():
    df = pd.DataFrame({"id": [1], "payload": [{"a": 1, "b": {"c": 2}}]})
    out = unnest_json_column(
        df, "payload", prefix="j_", sep="_", keep_original=True
    )
    assert "payload" in out.columns
    assert "j_a" in out.columns and "j_b_c" in out.columns
    assert out.loc[0, "j_a"] == 1
    assert out.loc[0, "j_b_c"] == 2


def test_top_level_list_duplicates_base_rows():
    df = pd.DataFrame({"id": [1], "payload": [[{"k": 1}, {"k": 2}]]})
    out = unnest_json_column(df, "payload", sep="_", keep_original=False)
    expected = pd.DataFrame({"id": [1, 1], "k": [1, 2]})
    assert_frame_equal(_order(out), _order(expected))


# --- Spark fixture that skips cleanly when Java isn't available ---
@pytest.fixture(scope="session")
def spark():
    # If Java isn't available, skip Spark tests gracefully
    if not os.environ.get("JAVA_HOME"):
        pytest.skip(
            "Skipping Spark tests: JAVA_HOME not set / Java not available"
        )
    try:
        spark = (
            SparkSession.builder.master("local[1]")
            .appName("unnest_json_tests")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )
    except Exception as e:
        pytest.skip(f"Skipping Spark tests: failed to start JVM ({e})")
    yield spark
    spark.stop()


def _collect_set(df, cols):
    return {tuple(row[c] for c in cols) for row in df.select(*cols).collect()}


def test_structs_and_arrays_flatten(spark):
    data = [
        (
            1,
            '{"a":1,"b":{"c":2,"d":{"e":3}},"arr":[{"x":10},{"x":20,"y":[1,2]}],"empty":[]}',
        ),
    ]
    sdf = spark.createDataFrame(data, ["id", "payload"])

    schema = T.StructType(
        [
            T.StructField("a", T.LongType()),
            T.StructField(
                "b",
                T.StructType(
                    [
                        T.StructField("c", T.LongType()),
                        T.StructField(
                            "d",
                            T.StructType([T.StructField("e", T.LongType())]),
                        ),
                    ]
                ),
            ),
            T.StructField(
                "arr",
                T.ArrayType(
                    T.StructType(
                        [
                            T.StructField("x", T.LongType()),
                            T.StructField("y", T.ArrayType(T.LongType())),
                        ]
                    )
                ),
            ),
            T.StructField("empty", T.ArrayType(T.StringType())),
        ]
    )

    out = unnest_json_column_spark(sdf, "payload", schema=schema, sep="_")

    # Expect 3 rows: (10, None), (20, 1), (20, 2)
    assert out.count() == 3

    # Columns we expect (id + flattened)
    for name in ["a", "b_c", "b_d_e", "arr_x"]:
        assert name in out.columns

    pairs = _collect_set(out, ["arr_x", "arr_y"])
    assert pairs == {(10, None), (20, 1), (20, 2)}

    # constant fields are carried through
    ac = _collect_set(out, ["a", "b_c", "b_d_e"])
    assert ac == {(1, 2, 3)}


def test_empty_array_preserved(spark):
    data = [(1, '{"empty": []}')]
    sdf = spark.createDataFrame(data, ["id", "payload"])

    schema = T.StructType([T.StructField("empty", T.ArrayType(T.StringType()))])

    out = unnest_json_column_spark(sdf, "payload", schema=schema, sep="_")
    # Should keep a single row with empty=NULL
    assert out.count() == 1
    # Column may remain named 'empty' after flatten/explode
    assert "empty" in out.columns
    row = out.select("empty").collect()[0]
    assert row["empty"] is None


def test_prefix_and_keep_original(spark):
    data = [(1, '{"a":1,"b":{"c":2}}')]
    sdf = spark.createDataFrame(data, ["id", "payload"])

    schema = T.StructType(
        [
            T.StructField("a", T.LongType()),
            T.StructField(
                "b", T.StructType([T.StructField("c", T.LongType())])
            ),
        ]
    )

    out = unnest_json_column_spark(
        sdf, "payload", schema=schema, sep="_", prefix="j_", keep_original=True
    )

    assert "payload" in out.columns
    # prefixed columns present
    assert "j_a" in out.columns and "j_b_c" in out.columns
    vals = _collect_set(out, ["j_a", "j_b_c"])
    assert vals == {(1, 2)}


def test_map_entries_without_nested_schema(spark):
    # Example where a map is present; explode map entries and flatten to key/value
    data = [(1, '{"m":{"k1":"v1","k2":"v2"}}')]
    sdf = spark.createDataFrame(data, ["id", "payload"])

    schema = T.StructType(
        [T.StructField("m", T.MapType(T.StringType(), T.StringType(), True))]
    )

    out = unnest_json_column_spark(sdf, "payload", schema=schema, sep="_")
    # After map_entries + explode + flatten, expect columns: m_key, m_value
    assert "m_key" in out.columns and "m_value" in out.columns
    kv = _collect_set(out, ["m_key", "m_value"])
    assert kv == {("k1", "v1"), ("k2", "v2")}

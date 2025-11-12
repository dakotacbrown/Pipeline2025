import pytest
from pyspark.sql import DataFrame
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from reader.spark_reader import SparkReader


def test_abstract_cannot_instantiate(spark, log):
    with pytest.raises(TypeError):
        SparkReader(
            log=log, spark_session=spark
        )  # abstract method not implemented


class EchoReader(SparkReader):
    """Tiny concrete subclass for tests."""

    def read_to_dataframe(self, read_config):
        # read_config supports either rows or schema for this stub
        rows = read_config.get("rows", [])
        schema = read_config.get("schema")
        if schema is not None:
            # ensure schema is honored
            return self.spark_session.createDataFrame(rows, schema=schema)
        return self.spark_session.createDataFrame(rows)


def test_concrete_initializes_and_reads_dataframe(spark, log):
    r = EchoReader(log=log, spark_session=spark)
    df = r.read_to_dataframe({"rows": [("x", 1)], "schema": "a string, b int"})
    assert isinstance(df, DataFrame)
    assert df.columns == ["a", "b"]
    assert df.count() == 1
    # properties set by base __init__
    assert r.log is log
    assert r.spark_session is spark


def test_empty_df_helper_returns_schema(spark, log):
    r = EchoReader(log=log, spark_session=spark)
    schema = StructType(
        [
            StructField("a", StringType(), True),
            StructField("b", IntegerType(), True),
        ]
    )
    empty = r._empty_df(schema)
    assert empty.schema.simpleString() == "struct<a:string,b:int>"
    assert empty.count() == 0


def test_base_init_guards_none_args(spark):
    from logging import getLogger

    logger = getLogger("test")
    with pytest.raises(TypeError):
        SparkReader(log=None, spark_session=spark)  # type: ignore[arg-type]

    with pytest.raises(TypeError):
        SparkReader(log=logger, spark_session=None)  # type: ignore[arg-type]

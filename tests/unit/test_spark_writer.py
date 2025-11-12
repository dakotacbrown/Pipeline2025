import pytest
from pyspark.sql import DataFrame
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from writer.spark_writer import SparkWriter


def test_abstract_cannot_instantiate(spark, log):
    with pytest.raises(TypeError):
        SparkWriter(log=log, spark_session=spark)


class LocalWriter(SparkWriter):
    def write_dataframe(self, df, write_config):
        # trivial write to local parquet for testing the base class
        path = write_config["path"]
        df.write.mode(write_config.get("mode", "overwrite")).parquet(path)


def test_base_init_and_write_parquet_roundtrip(spark, log, tmp_path):
    writer = LocalWriter(log=log, spark_session=spark)
    df = spark.createDataFrame([(1, "x"), (2, "y")], schema="a int, b string")

    out = tmp_path / "roundtrip"
    writer.write_dataframe(df, {"path": str(out), "mode": "overwrite"})

    back = spark.read.parquet(str(out))
    assert back.count() == 2
    assert set(back.columns) == {"a", "b"}


def test_base_init_guards_none_args(spark):
    from logging import getLogger

    logger = getLogger("t")
    with pytest.raises(TypeError):
        SparkWriter(log=None, spark_session=spark)  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        SparkWriter(log=logger, spark_session=None)  # type: ignore[arg-type]

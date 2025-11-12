from __future__ import annotations

from abc import ABC, abstractmethod
from logging import Logger
from typing import Any, Dict, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType


class SparkReader(ABC):
    """
    Abstract base for concrete Spark readers (e.g., S3, local FS, Snowflake external).

    Subclasses must implement `read_to_dataframe`, which returns a Spark DataFrame
    given a config dictionary (connection/path/options/schema, etc.).
    """

    def __init__(self, log: Logger, spark_session: SparkSession) -> None:
        """
        :param spark_session: The SparkSession object.
        :param log: Logger instance for logging messages.
        """
        if log is None:
            raise ValueError("log must not be None")
        if spark_session is None:
            raise ValueError("spark_session must not be None")

        self.log = log
        self.spark_session = spark_session

    @abstractmethod
    def read_to_dataframe(self, read_config: Dict[str, Any]) -> DataFrame:
        """Read according to `read_config` and return a DataFrame."""
        raise NotImplementedError

    # Optional helper that many readers find handy.
    def _empty_df(self, schema: Optional[StructType] = None) -> DataFrame:
        """
        Return an empty DataFrame with the provided schema (or empty schema).
        """
        if schema is None:
            schema = StructType([])
        empty_rdd = self.spark_session.sparkContext.emptyRDD()
        return self.spark_session.createDataFrame(empty_rdd, schema)

from __future__ import annotations

from abc import ABC, abstractmethod
from logging import Logger
from typing import Any, Dict

from pyspark.sql import DataFrame, SparkSession


class SparkWriter(ABC):
    """
    Abstract base for concrete Spark writers (S3, local FS, etc.).
    Subclasses implement `write_dataframe`.
    """

    def __init__(self, log: Logger, spark_session: SparkSession) -> None:
        if log is None:
            raise ValueError("log must not be None")
        if spark_session is None:
            raise ValueError("spark_session must not be None")
        self.log = log
        self.spark_session = spark_session

    @abstractmethod
    def write_dataframe(
        self, df: DataFrame, write_config: Dict[str, Any]
    ) -> None:
        """Write the DataFrame based on `write_config`."""
        raise NotImplementedError

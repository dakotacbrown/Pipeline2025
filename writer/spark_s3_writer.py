from __future__ import annotations

from logging import Logger
from typing import Any, Dict

from pyspark.sql import DataFrame, SparkSession

import utils.helper_functions as hf
from writer.spark_writer import SparkWriter


class SparkS3Writer(SparkWriter):
    """
    Writes a DataFrame to an S3 path decided by a helper that formats the prefix.

    write_config keys:
      - file_type: "csv" | "json" | "jsonl" | "parquet"
      - mode: Spark save mode (default "errorifexists")
      - options: dict of DataFrameWriter options (e.g., {"header": "true"})
      - partition_by: list[str] of partition columns (optional)
      - coalesce: int (optional, reduces small files)
      - (plus whatever keys your helper needs to build the S3 path)
      - OR: path (string) if helper isn't available (mainly for tests)
    """

    def __init__(self, log: Logger, spark_session: SparkSession) -> None:
        super().__init__(log, spark_session)

    def _resolve_path(self, cfg: Dict[str, Any]) -> str:
        if hf is not None and hasattr(hf, "format_clz_s3_prefix"):
            return hf.format_clz_s3_prefix(cfg)  # type: ignore[attr-defined]
        path = cfg.get("path")
        if not path:
            raise ValueError(
                "No helper to build S3 path and no 'path' provided in write_config."
            )
        return path

    def write_dataframe(
        self, df: DataFrame, write_config: Dict[str, Any]
    ) -> None:
        file_type = (write_config.get("file_type") or "").lower()
        if not file_type:
            raise ValueError("write_config['file_type'] is required")

        # Optional small-files control
        coalesce_n = write_config.get("coalesce")
        if isinstance(coalesce_n, int) and coalesce_n > 0:
            df = df.coalesce(coalesce_n)

        full_path = self._resolve_path(write_config)
        writer = df.write.mode(write_config.get("mode", "errorifexists"))

        # Options
        for k, v in (write_config.get("options") or {}).items():
            if v is not None:
                writer = writer.option(k, v)

        # Partitioning
        partition_cols = write_config.get("partition_by") or []
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)

        self.log.info(f"Writing as {file_type} to {full_path}")

        if file_type in {"csv", "txt", "text"}:
            writer.csv(full_path)
        elif file_type in {"json", "jsonl"}:
            writer.json(full_path)
        elif file_type == "parquet":
            writer.parquet(full_path)
        else:
            raise ValueError(
                f"Unsupported file_type: {file_type!r}. Supported: csv, json, parquet."
            )

        self.log.info(f"Wrote DataFrame to {full_path}")

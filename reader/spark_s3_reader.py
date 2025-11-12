from __future__ import annotations

import re
from logging import Logger
from typing import Any, Dict, Optional

import boto3
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructField, StructType

from reader.spark_reader import SparkReader


class SparkS3Reader(SparkReader):
    """
    Read a single file (matched by regex) from S3 into a Spark DataFrame.

    Config keys (all strings unless noted):
      - s3_bucket: str
      - file_path: S3 prefix inside the bucket (e.g., "landing/foo/")
      - input_file_name: regex pattern applied to the object key
      - file_type: one of {"csv","txt","text","json","jsonl","parquet"}
      - options: dict[str, Any] of Spark reader options (optional)
      - schema: pyspark.sql.types.StructType (optional)
    """

    def __init__(self, log: Logger, spark_session: SparkSession) -> None:
        super().__init__(log, spark_session)

    def read_to_dataframe(self, cfg: Dict[str, Any]) -> DataFrame:
        bucket = cfg["s3_bucket"]
        prefix = cfg["file_path"]
        pattern = cfg["input_file_name"]
        file_type = (cfg.get("file_type") or "").lower()

        # find the object (returns a fully qualified s3a:// path)
        path = self._find_file(prefix, pattern, bucket)
        self.log.info(f"Reading {file_type} from {path}")

        # build the reader with options
        reader = self.spark_session.read
        for k, v in (cfg.get("options") or {}).items():
            if v is not None:
                reader = reader.option(k, v)

        # optional schema — rebuild as all-nullable to be permissive
        schema: Optional[StructType] = cfg.get("schema")
        if schema is not None:
            schema = StructType(
                [StructField(f.name, f.dataType, True) for f in schema.fields]
            )
            reader = reader.schema(schema)

        # dispatch by file type
        if file_type in {"csv", "txt", "text"}:
            df = reader.csv(path)
        elif file_type in {"json", "jsonl"}:
            # Spark JSON reader supports jsonl by default
            df = reader.json(path)
        elif file_type == "parquet":
            df = reader.parquet(path)
        else:
            raise ValueError(f"Unsupported file_type: {file_type!r}")

        # If source is empty and caller provided a schema, return a typed empty DF
        if df.rdd.isEmpty() and schema is not None:
            return self.spark_session.createDataFrame(
                self.spark_session.sparkContext.emptyRDD(), schema
            )

        # If schema was provided, preserve its column order (subset to existing cols)
        if schema is not None:
            desired = [f.name for f in schema.fields]
            existing = [c for c in desired if c in df.columns]
            if existing:
                df = df.select(*existing)

        return df

    def _find_file(self, file_path: str, file_name: str, s3_bucket: str) -> str:
        """
        Return the first S3 key under (bucket, file_path) that matches the regex file_name.
        """
        s3_client = boto3.client("s3")
        paginator = s3_client.get_paginator("list_objects_v2")
        pat = re.compile(file_name)

        for page in paginator.paginate(Bucket=s3_bucket, Prefix=file_path):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if pat.search(key):
                    self.log.info(
                        f"Found s3://{s3_bucket}/{key} for pattern {file_name!r}"
                    )
                    return f"s3a://{s3_bucket}/{key}"

        raise FileNotFoundError(
            f"No S3 object matched pattern {file_name!r} under "
            f"s3://{s3_bucket}/{file_path}"
        )

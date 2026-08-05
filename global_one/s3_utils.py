"""
S3 I/O utilities shared between salesforce_global_one.py (this repo's name
for gl_journal_builder_pandas_s3.py) and helpers/gl_source_join.py. Pulled
out into its own module specifically so neither of those two files needs to
import from the other — gl_source_join.py used to import
read_table_by_dataset_id from gl_journal_builder_pandas_s3.py at module load
time, while gl_journal_builder_pandas_s3.py imported build_source_dataframe
from gl_source_join.py (deferred, inside run()). That wasn't a true circular
import (the second one only happens at call-time, not at module load), but
it was fragile and easy to accidentally turn into a real cycle. Now the
dependency graph is a clean one-way DAG:

    helpers/s3_utils.py  <-- helpers/gl_source_join.py  <-- salesforce_global_one.py

Matches the real repo layout: salesforce_global_one.py sits directly under
src/salesforce/resources/scripts/, with gl_source_join.py and s3_utils.py
both under src/salesforce/resources/scripts/helpers/ — same convention as
the existing helpers.helper_functions / helpers.onelake_writer modules used
by salesforce_ofac.py.

All S3 access here uses an injected s3_client rather than a bare
boto3.client("s3") call — on Databricks this needs to come from
new_session(service_credential) (see helpers.helper_functions in
salesforce_ofac.py), which vends AWS credentials via
set_ingester_aws_credentials(). A bare boto3.client("s3") would use the
default credential chain, which won't have the right permissions on a job
cluster.
"""

from datetime import datetime
import io
import pandas as pd


# ---------------------------------------------------------------------------
# Read JSONL from S3 into pandas
# ---------------------------------------------------------------------------

def read_jsonl_from_s3(s3_client, bucket: str, key: str) -> pd.DataFrame:
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    body = obj["Body"].read().decode("utf-8")
    return pd.read_json(io.StringIO(body), lines=True)


def read_jsonl_prefix_from_s3(s3_client, bucket: str, prefix: str, expected_columns: list = None) -> pd.DataFrame:
    """
    Read + concat every JSONL object under a prefix (handles multi-part
    ingester output). If the prefix has no objects — which is expected for
    some tables (e.g. Refund, PaymentLineInvoiceLine currently have 0 rows) —
    returns an empty dataframe with expected_columns instead of raising, so
    downstream joins don't break on legitimately-empty source tables.
    """
    paginator = s3_client.get_paginator("list_objects_v2")
    frames = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".jsonl") or key.endswith(".json"):
                frames.append(read_jsonl_from_s3(s3_client, bucket, key))
    if not frames:
        return pd.DataFrame(columns=expected_columns or [])
    return pd.concat(frames, ignore_index=True)


def find_dataset_prefix(bucket: str, dataset_id: str, source_prefix: str = "salesforce/reports") -> str:
    """
    Builds the S3 prefix for a table by its dataset_id: {source_prefix}/{dataset_id}/

    source_prefix is now a single configurable value passed in from the DAB
    job YAML (see salesforce_global_one.yml's source_key_prefix parameter),
    rather than assembled here from separate vendor/reports_segment guesses.
    That guessing is exactly what caused a real bug: an earlier hardcoded
    pattern (salesforce/{dataset_id}/) omitted a "reports" segment
    (confirmed via S3 console browse: the real path is
    salesforce/reports/{dataset_id}/), producing a syntactically valid but
    empty prefix — every table read silently returned 0 rows instead of
    erroring. Making this a config value means a future path change is a
    YAML edit, not a code deploy + another guess.
    """
    return f"{source_prefix.strip('/')}/{dataset_id}/"


def read_table_by_dataset_id(s3_client, bucket: str, dataset_id: str, expected_columns: list = None,
                              source_prefix: str = "salesforce/reports") -> pd.DataFrame:
    """Convenience wrapper: locate + read a table's JSONL by its dataset_id."""
    prefix = find_dataset_prefix(bucket, dataset_id, source_prefix)
    return read_jsonl_prefix_from_s3(s3_client, bucket, prefix, expected_columns=expected_columns)


# ---------------------------------------------------------------------------
# Write to S3
# ---------------------------------------------------------------------------

def upload_to_s3(s3_client, content: str, bucket: str, key_prefix: str, filename: str):
    key = f"{key_prefix.rstrip('/')}/{filename}"
    s3_client.put_object(Bucket=bucket, Key=key, Body=content.encode("utf-8"))
    return f"s3://{bucket}/{key}"


def write_success_file(s3_client, bucket: str, key_prefix: str):
    key = f"{key_prefix.rstrip('/')}/_SUCCESS"
    s3_client.put_object(Bucket=bucket, Key=key, Body=b"")
    return f"s3://{bucket}/{key}"


def build_partitioned_prefix(base_prefix: str, creation_dt: datetime) -> str:
    """
    Builds a year=/month=/day=/hour=/ partitioned path segment under
    base_prefix. Shared by both the main GL output file and the validation
    file so their partition structure stays in sync — pass the SAME
    creation_dt to both calls in one run() so they land in matching folders.
    """
    return (
        f"{base_prefix.rstrip('/')}/"
        f"year={creation_dt.strftime('%Y')}/"
        f"month={creation_dt.strftime('%m')}/"
        f"day={creation_dt.strftime('%d')}/"
        f"hour={creation_dt.strftime('%H')}/"
    )


def save_validation_file(s3_client, df: pd.DataFrame, bucket: str, base_prefix: str,
                          file_type: str = "parquet", creation_dt: datetime = None) -> str:
    """
    Saves the combined/joined dataframe to S3 as either parquet or csv,
    before it gets formatted into the fixed-width text file — gives you
    something to validate the join/business logic against independently of
    the file layout itself.

    file_type: "parquet" (default) or "csv" — driven by the YAML job's
    validation_file_type parameter, not hardcoded here.

    Path pattern: {base_prefix}/year=/month=/day=/hour=/general_ledger_{epoch}.<ext>
    """
    creation_dt = creation_dt or datetime.now()
    file_type = file_type.lower()
    if file_type not in ("csv", "parquet"):
        raise ValueError(f"Unsupported validation file_type: {file_type!r}. Must be 'csv' or 'parquet'.")

    epoch_ts = int(creation_dt.timestamp())
    partitioned_prefix = build_partitioned_prefix(base_prefix, creation_dt)
    key = f"{partitioned_prefix}general_ledger_{epoch_ts}.{file_type}"

    buffer = io.BytesIO()
    if file_type == "parquet":
        df.to_parquet(buffer, engine="pyarrow", index=False)
    else:
        buffer.write(df.to_csv(index=False).encode("utf-8"))
    buffer.seek(0)

    s3_client.put_object(Bucket=bucket, Key=key, Body=buffer.getvalue())
    return f"s3://{bucket}/{key}"

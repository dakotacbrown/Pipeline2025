"""
GL Journal Entry Interface — pandas version.

Reads JSONL source files from S3 (as landed by api_ingester), joins them,
formats fixed-width rows, and uploads the assembled file back to S3.

S3 access uses an injected s3_client rather than a bare boto3.client("s3")
call — on Databricks this needs to come from new_session(service_credential)
(see helpers.helper_functions in salesforce_ofac.py), which vends AWS
credentials via set_ingester_aws_credentials(). A bare boto3.client("s3")
would use the default credential chain, which won't have the right
permissions on a job cluster.
"""

from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP
import io
import pandas as pd


# ---------------------------------------------------------------------------
# 1. Read JSONL from S3 into pandas
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


def find_dataset_prefix(bucket: str, dataset_id: str, base_prefix: str = "",
                         vendor: str = "salesforce") -> str:
    """
    Builds the S3 prefix for a table by its dataset_id rather than table
    name, since that's what the ingester keys output by (per the DAB job's
    for_each_task inputs: {"table": ..., "dataset_id": ...}).

    ASSUMPTION — the exact key pattern api_ingester.py writes to hasn't been
    confirmed here. This defaults to:
        {base_prefix}/{vendor}/{dataset_id}/
    Swap this to match api_ingester.py's actual output path convention
    (check the script or browse a known-populated dataset_id folder in S3,
    e.g. the UsageResource one with 3 rows, to confirm the real pattern).
    """
    parts = [p for p in [base_prefix.strip("/"), vendor, dataset_id] if p]
    return "/".join(parts) + "/"


def read_table_by_dataset_id(s3_client, bucket: str, dataset_id: str, expected_columns: list = None,
                              base_prefix: str = "", vendor: str = "salesforce") -> pd.DataFrame:
    """Convenience wrapper: locate + read a table's JSONL by its dataset_id."""
    prefix = find_dataset_prefix(bucket, dataset_id, base_prefix, vendor)
    return read_jsonl_prefix_from_s3(s3_client, bucket, prefix, expected_columns=expected_columns)


# ---------------------------------------------------------------------------
# 2. Fixed-width formatting helpers
# ---------------------------------------------------------------------------

def fmt(value, length, justify="left", fill=" "):
    s = "" if value is None or (isinstance(value, float) and pd.isna(value)) else str(value)
    s = s[:length]
    return s.ljust(length, fill) if justify == "left" else s.rjust(length, fill)


def build_line(fields):
    return "".join(fmt(v, l, j, f) for v, l, j, f in fields)


def file_header(creation_dt: datetime, transmit_id: str = ""):
    return build_line([
        ("#H", 2, "left", " "),
        (creation_dt.strftime("%Y%m%d"), 8, "left", " "),
        (creation_dt.strftime("%H%M%S"), 6, "left", " "),
        (transmit_id, 8, "left", " "),
        ("", 76, "left", " "),
    ])


def journal_header(business_unit, journal_date, source, description=""):
    return build_line([
        ("H", 1, "left", " "),
        (business_unit, 5, "left", " "),
        ("NEXT", 10, "left", " "),
        (journal_date, 8, "left", " "),
        ("", 4, "left", " "),
        ("", 8, "left", " "),
        ("RECORDING", 10, "left", " "),
        ("", 21, "left", " "),
        (source, 3, "left", " "),
        ("", 8, "left", " "),
        (description, 30, "left", " "),
        ("", 33, "left", " "),
        ("", 39, "left", " "),
    ])


def journal_line(business_unit, account, dept_id="", project_id="",
                  journal_line_ref="", journal_line_desc="",
                  txn_currency_code="", txn_monetary_amount=0):
    ledger = "CORP" if str(business_unit).upper().startswith("US") else "LOCAL"
    amt = Decimal(str(txn_monetary_amount)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    return build_line([
        ("L", 1, "left", " "),
        (business_unit, 5, "left", " "),
        ("0", 9, "right", " "),
        (ledger, 10, "left", " "),
        (account, 10, "left", " "),
        ("", 10, "left", " "),
        (dept_id, 10, "left", " "),
        ("", 37, "left", " "),
        ("", 5, "left", " "),
        ("", 30, "left", " "),
        ("", 10, "left", " "),
        ("", 10, "left", " "),
        (project_id, 15, "left", " "),
        ("", 25, "left", " "),
        ("0", 28, "right", " "),
        ("", 1, "left", " "),
        ("0", 17, "right", " "),
        (journal_line_ref, 10, "left", " "),
        (journal_line_desc, 30, "left", " "),
        (txn_currency_code, 3, "left", " "),
        ("USDLY", 5, "left", " "),
        (str(amt), 28, "right", " "),
        ("0", 17, "right", " "),
        ("", 92, "left", " "),
    ])


def file_trailer(row_count, total_debits, total_credits, total_stat=0):
    def money(v):
        return str(Decimal(str(v)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))
    return build_line([
        ("#T", 2, "left", " "),
        (str(row_count), 9, "right", "0"),
        (money(total_debits), 28, "right", " "),
        (money(total_credits), 25, "right", " "),
        (money(total_stat), 25, "right", " "),
        ("", 5, "left", " "),
    ])


# ---------------------------------------------------------------------------
# 3. Assemble full file from the joined dataframe
# ---------------------------------------------------------------------------

def build_gl_file(df: pd.DataFrame, business_unit: str = None, source: str = "",
                   creation_dt: datetime = None) -> str:
    """
    business_unit=None (default): file covers every business unit present in
    df — one journal_header + its journal_lines per distinct business_unit,
    all wrapped in a single file_header/file_trailer. Matches the "Multi-
    Record Fixed Width" structure from the spec.
    business_unit="US001" (etc.): filters to just that BU, single journal
    header block — original single-BU behavior, unchanged.
    """
    creation_dt = creation_dt or datetime.now()

    lines = [file_header(creation_dt)]
    total_debits = Decimal("0")
    total_credits = Decimal("0")

    if business_unit is not None:
        bu_groups = [(business_unit, df)]
    elif "business_unit" in df.columns and not df.empty:
        bu_groups = [
            (bu, df[df["business_unit"] == bu])
            for bu in sorted(df["business_unit"].dropna().unique().tolist())
        ]
    else:
        bu_groups = []

    for bu, group_df in bu_groups:
        # Journal Header Description: placeholder "RevCloud Batch" per Dakota —
        # follow-up needed on what this should actually be when a BU's batch
        # spans multiple TransactionJournal.Name values.
        header_description = "RevCloud Batch"

        lines.append(journal_header(bu, creation_dt.strftime("%m%d%Y"), source,
                                     description=header_description))

        for _, row in group_df.iterrows():
            # "amount" is what gl_source_join.build_source_dataframe produces;
            # "txn_monetary_amount" supported for direct/manual calls.
            raw_amt = row.get("amount", row.get("txn_monetary_amount", 0))
            amt = Decimal(str(raw_amt)) if pd.notna(raw_amt) else Decimal("0")
            if amt >= 0:
                total_debits += amt
            else:
                total_credits += amt

            # Field mapping confirmed by Dakota:
            #   account          <- Account.Name, leading "A" stripped (clean_account_name)
            #   dept_id          <- Product2.Department_ID_DID__c ("did")
            #   project_id       <- left blank for now
            #   journal_line_ref <- TransactionJournal.UsageType (marked "?" — tentative)
            #   journal_line_desc <- TransactionJournal.TransactionType (marked "?" — tentative)
            lines.append(journal_line(
                business_unit=bu,
                account=row.get("account", row.get("account_name", "")),
                dept_id=row.get("dept_id", row.get("did", "")),
                project_id=row.get("project_id", ""),
                journal_line_ref=row.get("journal_line_ref", row.get("usage_type", "")),
                journal_line_desc=row.get("journal_line_desc", row.get("transaction_type", "")),
                txn_currency_code=row.get("txn_currency_code", ""),
                txn_monetary_amount=amt,
            ))

    row_count = len(lines) - 1  # subtract file_header only; trailer not yet appended
    lines.append(file_trailer(row_count, total_debits, total_credits))
    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------------------
# 4. Filename + upload
# ---------------------------------------------------------------------------

def build_filename(prefix: str, creation_dt: datetime) -> str:
    if len(prefix) != 3:
        raise ValueError("prefix (XXX) must be exactly 3 letters, e.g. 'BX1'")
    return f"{prefix}_{creation_dt.strftime('%Y%m%d%H%M%S')}.txt"


def upload_to_s3(s3_client, content: str, bucket: str, key_prefix: str, filename: str):
    key = f"{key_prefix.rstrip('/')}/{filename}"
    s3_client.put_object(Bucket=bucket, Key=key, Body=content.encode("utf-8"))
    return f"s3://{bucket}/{key}"


def write_success_file(s3_client, bucket: str, key_prefix: str):
    key = f"{key_prefix.rstrip('/')}/_SUCCESS"
    s3_client.put_object(Bucket=bucket, Key=key, Body=b"")
    return f"s3://{bucket}/{key}"


def save_validation_parquet(s3_client, df: pd.DataFrame, bucket: str, base_prefix: str,
                             creation_dt: datetime = None) -> str:
    """
    Saves the combined/joined dataframe to S3 as parquet, before it gets
    formatted into the fixed-width text file — gives you something to
    validate the join/business logic against independently of the file
    layout itself.

    Path pattern matches the year=/month=/day=/ partition style, with an
    epoch-timestamp filename (e.g. general_ledger_1767243625.parquet).
    """
    creation_dt = creation_dt or datetime.now()
    epoch_ts = int(creation_dt.timestamp())

    key = (
        f"{base_prefix.rstrip('/')}/"
        f"year={creation_dt.strftime('%Y')}/"
        f"month={creation_dt.strftime('%m')}/"
        f"day={creation_dt.strftime('%d')}/"
        f"general_ledger_{epoch_ts}.parquet"
    )

    buffer = io.BytesIO()
    df.to_parquet(buffer, engine="pyarrow", index=False)
    buffer.seek(0)

    s3_client.put_object(Bucket=bucket, Key=key, Body=buffer.getvalue())
    return f"s3://{bucket}/{key}"


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def run(log, s3_client, bucket, output_key_prefix, filename_prefix,
        business_unit=None, source="CS1",
        validation_key_prefix=None, vendor="salesforce", source_base_prefix=""):
    """
    s3_client must come from an authenticated session (e.g.
    new_session(service_credential).client("s3") — see helpers.helper_functions,
    same pattern used in salesforce_ofac.py), not a bare boto3.client("s3").

    bucket is used for both reading source Salesforce datasets (by dataset_id,
    see gl_source_join.DATASET_IDS) and writing the output file + validation
    parquet.

    business_unit: if None (default), the file covers ALL business units —
    build_source_dataframe pulls TransactionJournal broadly and build_gl_file
    groups by business_unit internally (multi-record Journal Header/Line
    blocks per BU in one file). Pass a specific value to filter to just that
    BU instead. Still needs a decision — see note in Dakota's chat.
    """
    from gl_source_join import build_source_dataframe

    creation_dt = datetime.now()

    log.info("building source dataframe...")
    df = build_source_dataframe(s3_client, bucket, base_prefix=source_base_prefix, vendor=vendor, log=log)
    log.info(f"building source dataframe...complete ({len(df)} rows)")

    if business_unit is not None:
        log.info(f"filtering to business_unit={business_unit}...")
        df = df[df["business_unit"] == business_unit]
        log.info(f"filtering to business_unit={business_unit}...complete ({len(df)} rows)")

    if validation_key_prefix:
        log.info("saving validation parquet...")
        parquet_url = save_validation_parquet(s3_client, df, bucket, validation_key_prefix, creation_dt)
        log.info(f"saving validation parquet...complete ({parquet_url})")

    log.info("building GL journal file...")
    content = build_gl_file(df, business_unit=business_unit, source=source, creation_dt=creation_dt)
    filename = build_filename(filename_prefix, creation_dt)
    log.info(f"building GL journal file...complete ({filename})")

    log.info("uploading GL journal file to s3...")
    url = upload_to_s3(s3_client, content, bucket, output_key_prefix, filename)
    write_success_file(s3_client, bucket, output_key_prefix)
    log.info(f"uploading GL journal file to s3...complete ({url})")

    return url, len(df)


def main():
    import sys
    from datetime import timezone
    import traceback

    from asvc1scoredataservices_common.logger.basic_logger import setup_logger
    from asvc1scoredataservices_common.logger.logger import write_execution_log_to_s3
    from pyspark.sql import SparkSession
    from helpers.helper_functions import new_session  # same helper salesforce_ofac.py uses

    logger = setup_logger()

    if len(sys.argv) < 8:
        raise ValueError(
            "Usage: script.py <env> <chamber_role> <service_credential> <bucket> "
            "<output_key_prefix> <validation_key_prefix> <filename_prefix>"  # noqa
        )

    args = sys.argv[1:]
    env = args[0]
    chamber_role = args[1]
    service_credential = args[2]
    bucket = args[3]
    output_key_prefix = args[4]
    validation_key_prefix = args[5]
    filename_prefix = args[6]

    # NOTE: chamber_role isn't used yet — it's only needed if this script
    # ever calls read_secret_from_chamber() directly for its own secrets.
    # Right now the only credential this script needs is AWS, which
    # new_session() handles. Keeping it as an accepted arg in case that
    # changes (e.g. if Salesforce API calls get added here directly).

    job_name = "salesforce_global_one"
    run_start_timestamp = datetime.now(tz=timezone.utc)
    final_state = "FAILED"
    failure_message = None
    record_count = 0
    aws_session = None

    try:
        logger.info("running gl journal builder...")

        logger.info("retrieving aws credentials...")
        aws_session = new_session(service_credential)
        s3_client = aws_session.client("s3")
        logger.info("retrieving aws credentials...complete")

        url, record_count = run(
            logger, s3_client, bucket,
            output_key_prefix=output_key_prefix,
            validation_key_prefix=validation_key_prefix,
            filename_prefix=filename_prefix,
        )

        logger.info("running gl journal builder...complete")
        final_state = "SUCCESS"
        return {
            "status_code": 200,
            "s3_url": url,
            "num_records": record_count,
            "message": "SUCCESS",
        }

    except Exception as e:
        failure_message = str(e)
        logger.error(f"""
        Unhandled error during gl journal builder execution. Returning error response.
        Error Type: {type(e).__name__}
        Error Message: {str(e)}
        Stack Trace: {traceback.format_exc()}
        """)
        raise Exception(
            "Unhandled error during gl journal builder execution. See logs for details"
        )

    finally:
        run_end_timestamp = datetime.now(tz=timezone.utc)
        current_date_str = run_end_timestamp.strftime("%Y-%m-%d")
        log_s3_path = (
            f"s3a://c1scoredataservices-{env}-east/databricks_job_logs/"
            f"job_run_date={current_date_str}/"
            f"job_family=salesforce/"
            f"job_name={job_name}/"
        )
        spark = SparkSession.builder.getOrCreate()
        write_execution_log_to_s3(
            logger=logger,
            spark=spark,
            s3_path=log_s3_path,
            severity_text="info" if final_state == "SUCCESS" else "error",
            body=(
                f"Job execution completed successfully: {job_name}"
                if final_state == "SUCCESS"
                else f"Job execution failed: {job_name} - {failure_message}"
            ),
            job_family="salesforce",
            job_name=job_name,
            final_state=final_state,
            failure_message=failure_message,
            records_published=record_count,
            run_start_timestamp=run_start_timestamp,
            run_end_timestamp=run_end_timestamp,
            data_interval_start_timestamp=None,
            data_interval_end_timestamp=run_end_timestamp,
            environment=env,
        )


if __name__ == "__main__":
    main()

"""
GL Journal Entry Interface — pandas version.

Reads JSONL source files from S3 (as landed by api_ingester), joins them,
formats fixed-width rows, and writes + submits the assembled file to
OneLake. Uses write_and_submit_file (helper_functions.py) for the
outbound/validation-write + OneLake-submission step, the same shared
function salesforce_ofac.py uses — see that function's docstring in
helper_functions.py for what it does.
"""

from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP
import json
import pandas as pd

from helpers.s3_utils import (
    read_jsonl_from_s3,
    read_jsonl_prefix_from_s3,
    find_dataset_prefix,
    read_table_by_dataset_id,
)
from helpers.helper_functions import build_execution_log_s3_path, write_and_submit_file
import helpers.gl_source_join as gl_source_join


# Confirmed against the actual field layout in build_gl_file() below — see
# decode_metadata.json / the OneStream decodeMetadata discussion for how
# this was derived and verified (each record type's total width checked
# against the real fixed-width spec).
DECODE_METADATA = json.loads(r'''{"fieldDefinitions": [{"fieldName": "record_type_1", "position": 0, "width": 1, "isRecordTypeKey": true}], "multiRecordDefinitions": [{"recordName": "file_header", "recordType": "#", "fieldDefinitions": [{"fieldName": "header_indicator", "position": 1, "width": 1}, {"fieldName": "creation_date", "position": 2, "width": 8}, {"fieldName": "creation_time", "position": 10, "width": 6}, {"fieldName": "transmit_id", "position": 16, "width": 8}, {"fieldName": "filler", "position": 24, "width": 76}]}, {"recordName": "journal_header", "recordType": "H", "fieldDefinitions": [{"fieldName": "business_unit", "position": 1, "width": 5}, {"fieldName": "journal_id", "position": 6, "width": 10}, {"fieldName": "journal_date", "position": 16, "width": 8}, {"fieldName": "adjusting_entry_info", "position": 24, "width": 4}, {"fieldName": "avg_daily_balance_date", "position": 28, "width": 8}, {"fieldName": "ledger_group", "position": 36, "width": 10}, {"fieldName": "reversal_info", "position": 46, "width": 21}, {"fieldName": "source", "position": 67, "width": 3}, {"fieldName": "transaction_reference_number", "position": 70, "width": 8}, {"fieldName": "header_description", "position": 78, "width": 30}, {"fieldName": "default_currency_info", "position": 108, "width": 33}, {"fieldName": "filler", "position": 141, "width": 39}]}, {"recordName": "journal_line_detail", "recordType": "L", "fieldDefinitions": [{"fieldName": "business_unit", "position": 1, "width": 5}, {"fieldName": "journal_line_number", "position": 6, "width": 9}, {"fieldName": "ledger", "position": 15, "width": 10}, {"fieldName": "journal_account", "position": 25, "width": 10}, {"fieldName": "alternate_account", "position": 35, "width": 10}, {"fieldName": "department_id", "position": 45, "width": 10}, {"fieldName": "unused_chartfields_1", "position": 55, "width": 37}, {"fieldName": "affiliate", "position": 92, "width": 5}, {"fieldName": "unused_chartfields_2", "position": 97, "width": 30}, {"fieldName": "reg_code", "position": 127, "width": 10}, {"fieldName": "unused_chartfields_3", "position": 137, "width": 10}, {"fieldName": "project_id", "position": 147, "width": 15}, {"fieldName": "filler_1", "position": 162, "width": 25}, {"fieldName": "base_currency_amount", "position": 187, "width": 28}, {"fieldName": "movement_flag", "position": 215, "width": 1}, {"fieldName": "statistics_amount", "position": 216, "width": 17}, {"fieldName": "journal_line_reference", "position": 233, "width": 10}, {"fieldName": "journal_line_description", "position": 243, "width": 30}, {"fieldName": "transaction_currency_code", "position": 273, "width": 3}, {"fieldName": "currency_rate_type", "position": 276, "width": 5}, {"fieldName": "transaction_monetary_amount", "position": 281, "width": 28}, {"fieldName": "currency_exchange_rate", "position": 309, "width": 17}, {"fieldName": "filler_2", "position": 326, "width": 92}]}, {"recordName": "file_trailer", "recordType": "#", "fieldDefinitions": [{"fieldName": "trailer_indicator", "position": 1, "width": 1}, {"fieldName": "row_count", "position": 2, "width": 9}, {"fieldName": "total_debits", "position": 11, "width": 28}, {"fieldName": "total_credits", "position": 39, "width": 25}, {"fieldName": "total_statistical_amount", "position": 64, "width": 25}, {"fieldName": "filler", "position": 89, "width": 5}]}]}''')


def choose_gl_identity(env: str):
    """
    GL journal builder's own job identity — the OneStream schema_name.
    Local to this file rather than helper_functions.py, for the same
    reason salesforce_ofac.py has its own choose_ofac_identity(): it's
    job-specific, not shared infrastructure.

    No 'source' value here (unlike choose_ofac_identity()) — confirmed
    by Dakota: OFAC's source was only ever used for S3 file naming, not
    part of the actual Exchange submission payload (the payload is just
    businessApplication/schemaName/fileSubmissions — no source field at
    all). GL already has filename_prefix/output_key_prefix filling that
    same S3-naming role, so there's nothing for a GL 'source' to do.

    PLACEHOLDER VALUE BELOW — unlike choose_ofac_identity() (which
    preserves real, previously-confirmed values), this is NOT a confirmed
    real prod/qa value. Fill in the actual OneStream schema_name once the
    GL journal's OneStream schema is registered. Do not deploy with this
    placeholder still in place.
    """
    if env == "prod":
        return "PLACEHOLDER_GL_SCHEMA_PROD"
    elif env == "qa":
        return "PLACEHOLDER_GL_SCHEMA_QA"
    else:
        raise ValueError(f"Invalid environment: {env}. Must be one of ['prod', 'qa'].")


# ---------------------------------------------------------------------------
# 3. Fixed-width formatting helpers
# ---------------------------------------------------------------------------

def fmt(value, length, justify="left", fill=" "):
    """
    pd.isna() (not isinstance(value, float) and pd.isna(value)) — the
    narrower float-only check misses pd.NA, which is what a missing value
    actually looks like now that read_jsonl_from_s3() reads every column
    as pandas' nullable StringDtype (see s3_utils.py). Confirmed via an
    end-to-end run against realistic data with genuinely-null fields
    (e.g. UsageType on a Payment/CreditMemo transaction, which
    legitimately has none): the old check let pd.NA fall through to
    str(value), writing the literal text "<NA>" into the fixed-width
    output instead of blank spaces. pd.isna() alone correctly catches
    None, float NaN, pd.NA, and pd.NaT in one check.
    """
    s = "" if pd.isna(value) else str(value)
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
    # Real business units are numeric (e.g. "10901"), not "US"/"EU"-prefixed
    # strings — the original startswith("US") check never matched anything,
    # so every line silently fell to LOCAL. Defaulting to CORP for now per
    # Dakota, pending a real rule for distinguishing CORP vs LOCAL by
    # numeric BU.
    ledger = "CORP"
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
# 4. Assemble full file from the joined dataframe
# ---------------------------------------------------------------------------

def build_gl_file(df: pd.DataFrame, business_unit: str = None, source: str = "",
                   creation_dt: datetime = None) -> str:
    """
    One journal_header is emitted per (business_unit, activity_date) pair,
    not per business_unit alone. journal_header()'s Journal Date is a
    header-level field meaning "Transaction Date from Source" (per spec) —
    a single date, not a range — so a business unit whose transactions
    span multiple days needs one header per day, not one header covering
    all of them. Per Dakota: this applies across a run's whole date
    window (month-to-date by default, via gl_source_join.resolve_date_window()),
    not just a single day — a BU with three distinct activity dates in the
    window gets three separate journal headers, each with only that day's
    transactions under it.

    business_unit=None (default): covers every (business_unit,
    activity_date) pair present in df. Matches the "Multi-Record Fixed
    Width" structure from the spec.
    business_unit="US001" (etc.): every row gets labeled with this BU
    regardless of its own InvoiceLine.Business_Unit value (unchanged from
    before this date-grouping change — run() already filters df to one BU
    before calling this when it wants that), still split into one header
    per distinct activity_date within it.

    Journal Date resolution, per row: TransactionJournal.ActivityDate if
    present and non-null, else creation_dt's date — the same
    "TransactionJournal.X, else a manual-call fallback" pattern used
    elsewhere in this function (e.g. the amount field). This means a
    direct/manual call that doesn't supply ActivityDate at all still gets
    one consistent header (dated by creation_dt), same as before this
    change.

    Rows with a null business_unit are dropped from grouping entirely
    (pandas groupby's default dropna=True) — there's no valid header to
    put them under.

    NOTE: creation_date/creation_time in the FILE header (see
    file_header()) are unrelated to this — those stay tied to when the
    file was actually built (creation_dt), not any transaction's date.
    Also note the format difference: file_header's creation_date is
    YYYYMMDD, but journal_header's journal_date below is MMDDYYYY — easy
    to transpose by accident, so worth double-checking if either ever
    changes.
    """
    creation_dt = creation_dt or datetime.now()

    lines = [file_header(creation_dt)]
    total_debits = Decimal("0")
    total_credits = Decimal("0")

    working = df

    if business_unit is None and (working.empty or "InvoiceLine.Business_Unit" not in working.columns):
        bu_date_groups = []
    elif business_unit is not None and working.empty:
        # Explicit business_unit requested but zero matching rows — still
        # emit one empty header for that BU, dated by creation_dt (no
        # per-row ActivityDate to draw from). Matches the original
        # behavior: business_unit=X always produced at least one header
        # block, even with 0 rows under it.
        fallback_ts = pd.Timestamp(creation_dt)
        if fallback_ts.tzinfo is None:
            fallback_ts = fallback_ts.tz_localize("UTC")
        bu_date_groups = [(business_unit, fallback_ts.date(), working)]
    else:
        working = working.copy()

        if "TransactionJournal.ActivityDate" in working.columns:
            raw_dates = pd.to_datetime(working["TransactionJournal.ActivityDate"], utc=True, errors="coerce")
        else:
            raw_dates = pd.Series(
                pd.NaT, index=working.index, dtype="datetime64[ns, UTC]"
            )

        fallback_ts = pd.Timestamp(creation_dt)
        if fallback_ts.tzinfo is None:
            fallback_ts = fallback_ts.tz_localize("UTC")
        working["_journal_date_key"] = raw_dates.fillna(fallback_ts).dt.date
        working["_bu_key"] = business_unit if business_unit is not None else working["InvoiceLine.Business_Unit"]

        bu_date_groups = [
            (bu, journal_date, group_df.drop(columns=["_journal_date_key", "_bu_key"]))
            for (bu, journal_date), group_df in working.groupby(["_bu_key", "_journal_date_key"])
        ]
        bu_date_groups.sort(key=lambda g: (str(g[0]), g[1]))

    for bu, journal_date, group_df in bu_date_groups:
        # Journal Header Description: placeholder "RevCloud Batch" per Dakota —
        # follow-up needed on what this should actually be when a BU's batch
        # spans multiple TransactionJournal.Name values.
        header_description = "RevCloud Batch"

        lines.append(journal_header(bu, journal_date.strftime("%m%d%Y"), source,
                                     description=header_description))

        for _, row in group_df.iterrows():
            # "TransactionJournal.CreditDebit" is what
            # gl_source_join.build_source_dataframe produces;
            # "txn_monetary_amount" supported for direct/manual calls.
            raw_amt = row.get("TransactionJournal.CreditDebit", row.get("txn_monetary_amount", 0))
            amt = Decimal(str(raw_amt)) if pd.notna(raw_amt) else Decimal("0")
            if amt >= 0:
                total_debits += amt
            else:
                total_credits += amt

            # Field mapping confirmed by Dakota:
            #   account          <- GeneralLedgerAccount.GL_Accounting_Number__c
            #                       directly, no stripping/cleaning applied.
            #                       Replaces the old Account.AccountNumber
            #                       source (which stripped a leading "A" via
            #                       clean_account_number) — per Dakota,
            #                       Account is no longer used for the GL
            #                       journal; GL_Accounting_Number__c isn't a
            #                       Salesforce Account ID, so that "A"-prefix
            #                       quirk doesn't apply to it.
            #   dept_id          <- InvoiceLine.Department_Id (bu default /
            #                       did overrides already applied upstream
            #                       in gl_source_join.apply_did_overrides())
            #   project_id       <- left blank for now
            #   journal_line_ref <- TransactionJournal.UsageType (marked "?" — tentative)
            #   journal_line_desc <- TransactionJournal.TransactionType (marked "?" — tentative)
            lines.append(journal_line(
                business_unit=bu,
                account=row.get("account", row.get("GeneralLedgerAccount.GL_Accounting_Number__c", "")),
                dept_id=row.get("dept_id", row.get("InvoiceLine.Department_Id", "")),
                project_id=row.get("project_id", ""),
                journal_line_ref=row.get("journal_line_ref", row.get("TransactionJournal.UsageType", "")),
                journal_line_desc=row.get("journal_line_desc",
                                           row.get("TransactionJournal.TransactionType", "")),
                txn_currency_code=row.get("txn_currency_code", ""),
                txn_monetary_amount=amt,
            ))

    row_count = len(lines) - 1  # subtract file_header only; trailer not yet appended
    lines.append(file_trailer(row_count, total_debits, total_credits))
    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------------------
# 5. Filename + upload
# ---------------------------------------------------------------------------

def build_filename(prefix: str, creation_dt: datetime) -> str:
    if len(prefix) != 3:
        raise ValueError("prefix (XXX) must be exactly 3 letters, e.g. 'BX1'")
    return f"{prefix}_{creation_dt.strftime('%Y%m%d%H%M%S')}.txt"


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def run(log, s3_client, oauth_token, bucket, output_key_prefix, filename_prefix,
        writer_config,
        business_unit=None, source="CS1",
        validation_key_prefix=None, validation_file_type="parquet",
        source_prefix="salesforce/reports",
        start_date=None, end_date=None):
    """
    s3_client must come from an authenticated session (e.g.
    new_session(service_credential).client("s3") — see helpers.helper_functions,
    same pattern used in salesforce_ofac.py), not a bare boto3.client("s3").

    oauth_token: Exchange OAuth token used to authenticate the OneLake
    submission — see helpers.helper_functions.retrieve_oauth_token, same
    pattern salesforce_ofac.py uses.

    writer_config: dict with ba/schema_name/iam_role/base_url/env/region —
    passed straight through to write_and_submit_file / s3_to_onelake.
    bucket and file_name get set/overwritten inside write_and_submit_file,
    so don't rely on any values already present under those two keys.

    bucket is used for both reading source Salesforce datasets (by dataset_id,
    see gl_source_join.DATASET_IDS) and writing the output file + validation
    file.

    source_prefix: the S3 prefix each dataset_id folder sits under (e.g.
    "salesforce/reports"). Comes from the YAML job's source_key_prefix
    parameter — kept as a config value rather than assembled from separate
    vendor/segment guesses in code, since a wrong guess here fails silently
    (an empty-but-valid prefix just returns 0 rows) rather than erroring.

    validation_file_type: "parquet" (default) or "csv" — comes from the
    YAML job's validation_file_type parameter.

    start_date / end_date: optional YYYY-MM-DD strings bounding
    TransactionJournal.ActivityDate — passed straight through to
    gl_source_join.build_source_dataframe() (see
    gl_source_join.resolve_date_window()). Both omitted (the default)
    filters to month-to-date.

    Both the main GL output file and the validation file land under the
    same year=/month=/day=/hour=/ partition (computed once from this run's
    creation_dt), so a given run's outputs are easy to find together and to
    correlate.

    business_unit: if None (default), the file covers ALL business units —
    build_source_dataframe pulls TransactionJournal broadly and build_gl_file
    groups by business_unit internally (multi-record Journal Header/Line
    blocks per BU in one file). Pass a specific value to filter to just that
    BU instead. Still needs a decision — see note in Dakota's chat.
    """
    creation_dt = datetime.now()

    log.info("building source dataframe...")
    df = gl_source_join.build_source_dataframe(
        s3_client, bucket, source_prefix=source_prefix, log=log,
        start_date=start_date, end_date=end_date,
    )
    log.info(f"building source dataframe...complete ({len(df)} rows)")

    if business_unit is not None:
        log.info(f"filtering to business_unit={business_unit}...")
        df = df[df["InvoiceLine.Business_Unit"] == business_unit]
        log.info(f"filtering to business_unit={business_unit}...complete ({len(df)} rows)")

    log.info("building GL journal file...")
    content = build_gl_file(df, business_unit=business_unit, source=source, creation_dt=creation_dt)
    filename = build_filename(filename_prefix, creation_dt)
    log.info(f"building GL journal file...complete ({filename})")

    log.info("writing outbound + validation files and submitting to onelake...")
    outbound_url, validation_url = write_and_submit_file(
        log, s3_client, oauth_token, bucket,
        content=content,
        filename=filename,
        file_type="MULTI_RECORD_FIXED_WIDTH",
        output_key_prefix=output_key_prefix,
        writer_config=writer_config,
        creation_dt=creation_dt,
        validation_df=df,
        validation_key_prefix=validation_key_prefix,
        validation_file_type=validation_file_type,
        decode_metadata=DECODE_METADATA,
    )
    log.info(
        f"writing outbound + validation files and submitting to onelake...complete "
        f"(outbound={outbound_url}, validation={validation_url})"
    )

    return outbound_url, len(df)


def main():
    import sys
    from datetime import timezone
    import traceback

    from asvc1scoredataservices_common.logger.basic_logger import setup_logger
    from asvc1scoredataservices_common.logger.logger import write_execution_log_to_s3
    from asvc1scoredataservices_common.utils.helper_functions import read_secret_from_chamber
    from pyspark.sql import SparkSession
    from helpers.helper_functions import new_session, retrieve_oauth_token, choose_exchange_env

    logger = setup_logger()

    if len(sys.argv) < 9:
        raise ValueError(
            "Usage: script.py <env> <chamber_role> <service_credential> <bucket> "
            "<output_key_prefix> <validation_key_prefix> <filename_prefix> "
            "<source_key_prefix> <validation_file_type> "
            "[<start_date> <end_date>]"  # noqa
        )

    args = sys.argv[1:]
    env = args[0]
    chamber_role = args[1]
    service_credential = args[2]
    bucket = args[3]
    output_key_prefix = args[4]
    validation_key_prefix = args[5]
    filename_prefix = args[6]
    source_key_prefix = args[7]
    validation_file_type = args[8]
    # Optional trailing positional args — both omitted (the default) means
    # month-to-date (see gl_source_join.resolve_date_window()). Trailing
    # rather than inserted earlier in the list so existing job YAML
    # invocations that don't pass them keep working unchanged.
    start_date = args[9] if len(sys.argv) > 10 else None
    end_date = args[10] if len(sys.argv) > 11 else None

    # schema_name resolved by env, not passed in via YAML — see
    # choose_gl_identity()'s docstring (placeholder value pending real
    # OneStream schema registration — do not deploy until filled in).
    schema_name = choose_gl_identity(env)

    # BAC1SCOREDATASERVICES / us-west-2: same Exchange app + region OFAC
    # uses, hardcoded here rather than passed in — matches
    # salesforce_ofac.py's own WRITER_CONFIG literals, not job-specific.
    business_application = "BAC1SCOREDATASERVICES"

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
        # Explicit timeouts so a network/NCC connectivity problem fails fast
        # with a clear error instead of hanging indefinitely — bare
        # boto3 clients have no default timeout, so a stuck connection
        # (e.g. DNS/routing issue reaching S3 from this compute) just hangs
        # forever with no error, which is much harder to diagnose than a
        # clean ConnectTimeoutError.
        from botocore.config import Config
        s3_client = aws_session.client(
            "s3",
            config=Config(connect_timeout=10, read_timeout=30, retries={"max_attempts": 3}),
        )
        logger.info("retrieving aws credentials...complete")

        # Same Exchange app registration as salesforce_ofac.py (confirmed
        # by Dakota) — not a per-job YAML parameter, one shared source of
        # truth in choose_exchange_env() instead.
        exchange_oauth_url, iam_role, base_url = choose_exchange_env(env)

        logger.info("retrieving exchange secrets...")
        exchange_client_id = read_secret_from_chamber(
            env, chamber_role, "c1scoredataservices/exchange/id", "c1scoredataservices_exchange_id"
        )
        exchange_client_secret = read_secret_from_chamber(
            env, chamber_role, "c1scoredataservices/exchange/secret", "c1scoredataservices_exchange_secret"
        )
        logger.info("retrieving exchange secrets...complete")

        logger.info("generating oauth token...")
        oauth_token = retrieve_oauth_token(
            logger,
            exchange_oauth_url,
            {"Content-Type": "application/x-www-form-urlencoded"},
            {
                "client_id": exchange_client_id,
                "client_secret": exchange_client_secret,
                "grant_type": "client_credentials",
            },
        )
        logger.info("generating oauth token...complete")

        writer_config = {
            "ba": business_application,
            "schema_name": schema_name,
            "iam_role": iam_role,
            "base_url": base_url,
            "env": env,
            "region": "us-west-2",
        }

        url, record_count = run(
            logger, s3_client, oauth_token, bucket,
            output_key_prefix=output_key_prefix,
            writer_config=writer_config,
            validation_key_prefix=validation_key_prefix,
            validation_file_type=validation_file_type,
            filename_prefix=filename_prefix,
            source_prefix=source_key_prefix,
            start_date=start_date,
            end_date=end_date,
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
        log_s3_path = build_execution_log_s3_path(env, "salesforce", job_name, current_date_str)
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

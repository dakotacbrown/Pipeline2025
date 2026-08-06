import sys
import traceback
from io import StringIO
from logging import Logger
from typing import List

import numpy as np
import pandas as pd
import requests
from asvc1scoredataservices_common.utils.databricks_helper_functions import (
    set_ingester_aws_credentials,
)

from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession

from helpers.onelake_writer import s3_to_onelake
from helpers.s3_utils import (
    build_partitioned_prefix,
    save_validation_file,
    upload_to_s3,
    write_success_file,
)


def retrieve_oauth_token(
    log: Logger,
    oauth_link: str,
    headers: dict,
    data: dict,
) -> str:
    log.info("Attempting to retrieve access token.")

    try:
        response = requests.post(
            oauth_link,
            headers=headers,
            data=data,
            verify=False,
        )

        response.raise_for_status()

        json_data = response.json()

        access_token = json_data["access_token"]

        log.info("Successfully retrieved access token.")

        return access_token

    except Exception as e:
        log.error(
            f"An error has occurred while retrieving access token: {e}\nStack Trace: {traceback.format_exc()}"  # noqa
        )
        sys.exit(1)


MAX_RECORDS = 1_000_000
MAX_PAGES = 10_000


def retrieve_report_data(
    log: Logger,
    data_endpoint: str,
    query: str,
    headers: dict,
    data_columns: dict,
    columns_order: List[str],
) -> pd.DataFrame:
    log.info("Attempting to retrieve data.")

    try:
        full_records = []
        full_url = f"{data_endpoint}{query}"
        response = requests.get(
            full_url,
            headers=headers,
            verify=False,
        )
        response.raise_for_status()

        json_data = response.json()
        records = json_data.get("records", [])
        full_records.extend(records)

        pages_fetched = 1
        while not json_data.get("done"):
            if len(full_records) >= MAX_RECORDS:
                log.warning(
                    f"Record limit of {MAX_RECORDS} reached after {pages_fetched} pages; stopping pagination."  # noqa
                )
                break
            if pages_fetched >= MAX_PAGES:
                log.warning(
                    f"Page limit of {MAX_PAGES} reached; stopping pagination."
                )
                break
            next_url = _validate_next_records_url(json_data["nextRecordsUrl"])
            response = requests.get(
                f"{data_endpoint}{next_url}",
                headers=headers,
                verify=False,
            )

            response.raise_for_status()
            json_data = response.json()
            records = json_data.get("records", [])
            full_records.extend(records)
            pages_fetched += 1

        dataframe = pd.DataFrame(full_records)
        dataframe = dataframe.drop(columns=["attributes"], errors="ignore")
        dataframe.columns = dataframe.columns.str.lower()
        log.info(f"Columns before renaming: {dataframe.columns.tolist()}")
        dataframe = dataframe.rename(columns=data_columns)
        log.info(f"Columns after renaming: {dataframe.columns.tolist()}")
        log.info(f"Retrieved {len(dataframe)} records from Salesforce.")

        log.info("normalizing data...")
        log.info(f"Columns before normalization: {dataframe.columns.tolist()}")
        normalized_sf_data = normalize_data(dataframe, columns_order)
        log.info(
            f"Columns after normalization: {normalized_sf_data.columns.tolist()}"
        )
        log.info("normalizing data... complete")

        log.info("Successfully retrieved data.")
        return normalized_sf_data

    except Exception as e:
        log.error(
            f"An error occurred while retrieving data: {e}\nStack Trace: {traceback.format_exc()}"  # noqa
        )
        sys.exit(1)


def normalize_data(
    dataframe: pd.DataFrame, columns_order: List[str]
) -> pd.DataFrame:

    # 1. Add address line 2 columns with null string values
    dataframe["shipping_addr_ln_2"] = np.nan
    dataframe["billing_addr_ln_2"] = np.nan

    # 2. Replace "-" and "null" strings with np.nan
    dataframe.replace(["-", "null"], np.nan, inplace=True)

    # 3. Replace nulls in 'parent_account_id' with default fallback
    if "parent_account_id" in dataframe.columns:
        dataframe["parent_account_id"] = dataframe["parent_account_id"].fillna(
            "000000000000000"
        )

    # 4. Normalize columns types
    dataframe = dataframe.astype(str)
    dataframe = dataframe.reindex(columns=columns_order)

    return dataframe


def format_s3_prefix(source, execution_date, execution_time, current_run_time):  # noqa
    return f"salesforce/ofac/{source}/execution_date={execution_date}/execution_time={execution_time}/run_time={current_run_time}"  # noqa


def new_session(service_credential):
    spark = SparkSession.builder.getOrCreate()
    dbutils = DBUtils(spark)
    aws_session = set_ingester_aws_credentials(service_credential, dbutils)
    return aws_session


def choose_exchange_env(env: str):
    """
    The Exchange OAuth endpoint, IAM role, and base URL — shared by every
    script submitting through the same Capital One Exchange app
    registration. Confirmed by Dakota: salesforce_ofac.py and
    salesforce_global_one.py are under the same Exchange app, so this
    doesn't need to be a separate per-job YAML parameter for either of
    them — one place to update if it ever changes, instead of two YAML
    files that could drift apart.
    """
    if env == "prod":
        return (
            "https://partner-apis.cloud.capitalone.com/oauth2/token",
            "arn:aws:iam::065587737899:role/BAC1SCOREDATASERVICES/ASVSDP/OneStream-File-Pull-c1scoredataservices-ol",  # noqa
            "https://api-sdp.cloud.capitalone.com/internal-operations/developer-platform/stream-management",  # noqa
        )
    elif env == "qa":
        return (
            "https://partner-apis-it.cloud.capitalone.com/oauth2/token",
            "arn:aws:iam::204098028041:role/BAC1SCOREDATASERVICES/ASVSDP/OneStream-File-Pull-c1scoredataservices-ol",  # noqa
            "https://api-sdp-it.cloud.capitalone.com/internal-operations/developer-platform/stream-management",  # noqa
        )
    else:
        raise ValueError(
            f"Invalid environment: {env}. Must be one of ['prod', 'qa']."
        )


def choose_env(env, source, schema_name):
    """
    source and schema_name are job-specific, not environment-specific —
    salesforce_ofac.py and salesforce_global_one.py each need their own
    values, so they're passed in here rather than hardcoded per env
    branch. Everything else this returns (bucket, upstream_env,
    exchange_oauth, sf_oauth, sf_data_endpoint, iam_role, base_url) is
    genuinely environment-dependent infrastructure config, shared across
    every job that calls this.

    NOTE: the previous hardcoded values for OFAC specifically had
    schema_name differing by env in a way worth double-checking —
    "cos_ofac_sanctions_reporting_v2" (prod) vs
    "c1s_ofac_sanctions_reporting_v2" (qa), a "cos_" vs "c1s_" prefix
    difference that looks like it could be a typo rather than an
    intentional distinction. Preserved as-is here (now as the caller's
    responsibility to supply correctly per env), not silently corrected —
    worth confirming with whoever owns the OneStream schema registration
    before relying on it.
    """
    exchange_oauth, iam_role, base_url = choose_exchange_env(env)

    if env == "prod":
        return (
            "c1scoredataservices-prod-west",
            "capitalonesowtare-prod",
            exchange_oauth,
            "https://partner-apis.cloud.capitalone.com/third-party/salesforce/services/oauth2/token",  # noqa
            "https://partner-apis.cloud.capitalone.com/third-party/salesforce/",
            source,
            schema_name,
            iam_role,
            base_url,
        )
    elif env == "qa":
        return (
            "c1scoredataservices-qa-west",
            "capitalonesoftware-qa",
            exchange_oauth,
            "https://partner-apis-it.cloud.capitalone.com/third-party/salesforce/services/oauth2/token",  # noqa
            "https://partner-apis-it.cloud.capitalone.com/third-party/salesforce/",
            source,
            schema_name,
            iam_role,
            base_url,
        )
    else:
        raise ValueError(
            f"Invalid environment: {env}. Must be one of ['prod', 'qa']."
        )


def publish_to_s3(
    dataframe: pd.DataFrame,
    file_name: str,
    bucket: str,
    execution_date,
    execution_time,
    source,
    current_run_time,
    session,
):
    key = format_s3_prefix(
        source, execution_date, execution_time, current_run_time
    )

    s3 = session.client("s3")
    csv_buffer = StringIO()
    dataframe.to_csv(
        csv_buffer,
        index=False,
    )

    s3.put_object(
        Bucket=bucket,
        Key=f"{key}/{file_name}",
        Body=csv_buffer.getvalue(),
        ContentType="text/csv",
    )
    return key


def _validate_next_records_url(next_url: str) -> str:
    """
    Salesforce nextRecordsUrl should always be a relative path under
    /services/data/. Reject anything else to prevent SSRF via a
    malicious or spoofed response.
    """

    parsed_url = requests.utils.urlparse(next_url)

    # Absolute URLS (with scheme or netloc) are never legitimate here.
    if parsed_url.scheme or parsed_url.netloc:
        raise ValueError(
            f"Invalid nextRecordsUrl: {next_url}. Must be a relative path."
        )

    path = next_url.lstrip("/")

    if not path.startswith("services/data/"):
        raise ValueError(
            f"Invalid nextRecordsUrl: {next_url}. Must start with /services/data/."
        )

    return path


def build_execution_log_s3_path(env: str, job_family: str, job_name: str, run_date: str) -> str:
    """
    The job-execution-log S3 path pattern used identically by
    salesforce_ofac.py and salesforce_global_one.py's finally blocks —
    extracted here since both built the exact same f-string independently.
    """
    return (
        f"s3a://c1scoredataservices-{env}-east/databricks_job_logs/"
        f"job_run_date={run_date}/"
        f"job_family={job_family}/"
        f"job_name={job_name}/"
    )


def rename_and_clean_columns(log: Logger, dataframe: pd.DataFrame, data_columns: dict) -> pd.DataFrame:
    """
    Drops the "attributes" column (present on raw ingested Salesforce
    records), lowercases column names, then renames via data_columns.

    Split out from transform_account_data() so it can be applied
    independently to more than one source (e.g. Account and a separate
    BillingAccount dataset) before joining them, with normalize_data()
    applied once on the combined result afterward — most of a shared
    columns_order's fields won't exist on any single source alone until
    after such a join, so normalizing per-source too early would reindex
    away real data.
    """
    dataframe = dataframe.drop(columns=["attributes"], errors="ignore")
    dataframe.columns = dataframe.columns.str.lower()
    log.info(f"Columns before renaming: {dataframe.columns.tolist()}")
    dataframe = dataframe.rename(columns=data_columns)
    log.info(f"Columns after renaming: {dataframe.columns.tolist()}")
    log.info(f"Retrieved {len(dataframe)} records.")
    return dataframe


def transform_account_data(
    log: Logger, dataframe: pd.DataFrame, data_columns: dict, columns_order: List[str]
) -> pd.DataFrame:
    """
    The column-rename + normalize_data steps that used to live inside
    retrieve_report_data(), extracted so they can be reused independently
    of how the raw data was obtained. salesforce_ofac.py used to fetch
    live via SOQL and transform in the same function; it now reads
    already-ingested data from S3 (see helpers.s3_utils.read_table_by_dataset_id)
    and calls this separately — same transformation, different source.
    """
    dataframe = rename_and_clean_columns(log, dataframe, data_columns)

    log.info("normalizing data...")
    log.info(f"Columns before normalization: {dataframe.columns.tolist()}")
    normalized_data = normalize_data(dataframe, columns_order)
    log.info(f"Columns after normalization: {normalized_data.columns.tolist()}")
    log.info("normalizing data... complete")

    return normalized_data


def write_and_submit_file(
    log: Logger,
    s3_client,
    oauth_token: str,
    bucket: str,
    content: str,
    filename: str,
    file_type: str,
    output_key_prefix: str,
    writer_config: dict,
    creation_dt,
    validation_df: pd.DataFrame = None,
    validation_key_prefix: str = None,
    validation_file_type: str = "parquet",
    decode_metadata: dict = None,
):
    """
    Shared by salesforce_ofac.py and salesforce_global_one.py: writes the
    main output file to a partitioned outbound S3 location (year=/month=/
    day=/hour=/), optionally writes a validation copy, then submits the
    outbound file to OneLake via s3_to_onelake.

    content: the fully-serialized file content as a string — fixed-width
    text for MULTI_RECORD_FIXED_WIDTH, CSV text for CSV_WITH_HEADER. This
    function doesn't build the content itself; that's entirely
    format-specific and stays with each calling script.

    validation_df: if given together with validation_key_prefix, saved as
    a separate validation file. Pass validation_df=None (or omit
    validation_key_prefix) to skip writing a validation file entirely.

    decode_metadata: required for MULTI_RECORD_FIXED_WIDTH submissions,
    omit for CSV_WITH_HEADER and other self-describing formats.

    writer_config must contain: ba, schema_name, iam_role, base_url, env,
    region — bucket and file_name get set/overwritten here, so don't rely
    on any values already present under those two keys.

    Returns (outbound_url, validation_url) — validation_url is None if no
    validation file was written.
    """
    partitioned_output_prefix = build_partitioned_prefix(output_key_prefix, creation_dt)
    outbound_url = upload_to_s3(s3_client, content, bucket, partitioned_output_prefix, filename)
    write_success_file(s3_client, bucket, partitioned_output_prefix)

    validation_url = None
    if validation_df is not None and validation_key_prefix:
        validation_url = save_validation_file(
            s3_client, validation_df, bucket, validation_key_prefix,
            file_type=validation_file_type, creation_dt=creation_dt,
        )

    file_submission = {
        "fileName": filename,
        "fileSize": len(content.encode("utf-8")),
        "fileType": file_type,
        "overrideMultiPartSize": 0,
        "sourceFilePath": outbound_url,
    }
    if decode_metadata is not None:
        file_submission["decodeMetadata"] = decode_metadata

    config = dict(writer_config)
    config["bucket"] = bucket
    config["file_name"] = filename

    s3_to_onelake(log, oauth_token, config, [file_submission])

    return outbound_url, validation_url

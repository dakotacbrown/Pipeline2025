import sys
import traceback
from datetime import datetime, timezone

from asvc1scoredataservices_common.logger.basic_logger import setup_logger
from asvc1scoredataservices_common.logger.logger import \
    write_execution_log_to_s3
from asvc1scoredataservices_common.logger.security_logger import SecurityLogger
from asvc1scoredataservices_common.utils.helper_functions import (
    read_secret_from_chamber,
    set_proxy,
)
from pyspark.sql import SparkSession

from helpers.helper_functions import (
    build_execution_log_s3_path,
    choose_env,
    new_session,
    retrieve_oauth_token,
    transform_account_data,
    write_and_submit_file,
)
from helpers.gl_source_join import DATASET_IDS
from helpers.s3_utils import read_table_by_dataset_id

logger = setup_logger()

def choose_ofac_identity(env: str):
    """
    OFAC's own job identity — the Exchange 'source' label and OneStream
    schema_name. Local to this file rather than helper_functions.py, since
    (unlike choose_exchange_env's values) these are specific to this job,
    not shared with salesforce_global_one.py — that script needs its own
    different values, resolved by its own choose_gl_identity().

    Resolved here based on env rather than passed in via YAML, since
    Databricks job YAML doesn't have a clean built-in way to vary one
    parameter's default by environment without Asset Bundle
    target-specific overrides — this way env is the only thing that has
    to be configured correctly, same pattern as choose_exchange_env().

    CONFIRM before relying on this: schema_name preserves the original
    "cos_" vs "c1s_" prefix difference between prod and qa exactly as it
    was before this got extracted — flagged previously as possibly a
    typo, not silently corrected. Worth checking with whoever owns the
    OneStream schema registration.
    """
    if env == "prod":
        return "c1s_ofac_sanctions_reporting", "cos_ofac_sanctions_reporting_v2"
    elif env == "qa":
        return "c1s_ofac_sanctions_reporting", "c1s_ofac_sanctions_reporting_v2"
    else:
        raise ValueError(f"Invalid environment: {env}. Must be one of ['prod', 'qa'].")


DATA_COLUMNS = {
    "name": "account_name",
    "parentid": "parent_account_id",
    "shippingstreet": "shipping_addr_ln_1",
    "billingstreet": "billing_addr_ln_1",
    "billingcity": "billing_city",
    "billingstatecode": "billing_state_providence",
    "billingpostalcode": "zip_code",
    "billingcountrycode": "billing_country",
    "id": "account_id",
    "type": "type",
}

COLUMNS_ORDER = [
    "account_name",
    "parent_account_id",
    "shipping_addr_ln_1",
    "shipping_addr_ln_2",
    "billing_addr_ln_1",
    "billing_addr_ln_2",
    "billing_city",
    "billing_state_providence",
    "zip_code",
    "billing_country",
    "account_id",
    "type",
]


def main():
    if len(sys.argv) < 8:
        raise ValueError(
            "Usage: script.py <env> <chamber_role> <service_credential> "
            "<source_key_prefix> <output_key_prefix> "
            "<validation_key_prefix> <validation_file_type>"  # noqa
        )

    args = sys.argv[1:]
    env = args[0]
    chamber_role = args[1]
    service_credential = args[2]
    source_key_prefix = args[3]
    output_key_prefix = args[4]
    validation_key_prefix = args[5]
    validation_file_type = args[6]

    # source/schema_name resolved by env, not passed in via YAML — see
    # choose_ofac_identity()'s docstring for why.
    source, schema_name = choose_ofac_identity(env)

    # Account dataset_id is the SAME one gl_source_join.py's GL pipeline
    # uses (confirmed by Dakota) — reused directly from DATASET_IDS rather
    # than as a separate config value, so the two pipelines can't drift
    # apart on what should be one shared piece of truth.
    account_dataset_id = DATASET_IDS["account"]

    job_name = "salesforce_ofac"
    run_start_timestamp = datetime.now(tz=timezone.utc)
    final_state = "FAILED"
    failure_message = None
    record_count = 0
    aws_session = None

    try:
        logger.info("running ofac report...")

        logger.info("retrieving env specific values...")

        # NOTE: sf_oauth and sf_data_endpoint are no longer used — they
        # only ever fed the live Salesforce SOQL pull, which is gone now
        # that this reads already-ingested data from S3 instead. Left
        # unpacked (not renamed to _) so choose_env's tuple shape doesn't
        # need to change, in case other callers still rely on it.
        #
        # source/schema_name (resolved above via choose_ofac_identity) are
        # passed straight through by choose_env unchanged — using _ here
        # since re-binding them to their own already-known values would be
        # confusing to read, not because they're unused.
        (
            bucket,
            upstream_env,
            exchange_oauth,
            sf_oauth,
            sf_data_endpoint,
            _source,
            _schema_name,
            iam_role,
            base_url,
        ) = choose_env(env, source, schema_name)

        WRITER_CONFIG = {
            "schema_name": schema_name,
            "ba": "BAC1SCOREDATASERVICES",
            "env": env,
            "region": "us-west-2",
            "iam_role": iam_role,
            "base_url": base_url,
        }

        logger.info("retrieving secrets...")
        aws_session = new_session(service_credential)
        set_proxy(logger, env)
        sec = SecurityLogger(logger, aws_session, env, "salesforce", "salesforce_ofac")
        creds = {}
        # Only the Exchange credentials are needed now — the Salesforce
        # username/password/client secrets were only ever used for the
        # live SOQL pull's own OAuth step, which is gone.
        secrets = [
            {
                "path": "c1scoredataservices/exchange/id",
                "key": "c1scoredataservices_exchange_id",
            },
            {
                "path": "c1scoredataservices/exchange/secret",
                "key": "c1scoredataservices_exchange_secret",
            },
        ]

        for secret in secrets:
            secret_value = read_secret_from_chamber(
                env, chamber_role, secret["path"], secret["key"]
            )
            creds[secret["key"]] = secret_value

        logger.info("generating oauth token...")
        exchange_headers = {"Content-Type": "application/x-www-form-urlencoded"}
        exchange_data = {
            "client_id": creds["c1scoredataservices_exchange_id"],
            "client_secret": creds["c1scoredataservices_exchange_secret"],
            "grant_type": "client_credentials",
        }
        _ctx_exchange = sec.oauth_token(endpoint=exchange_oauth, provider="exchange")
        _ctx_exchange.__enter__()
        try:
            c1_oauth_token = retrieve_oauth_token(
                logger,
                exchange_oauth,
                exchange_headers,
                exchange_data,
            )
            _ctx_exchange.__exit__(None, None, None)
        except (Exception, SystemExit) as exc:
            _ctx_exchange.__exit__(type(exc), exc, exc.__traceback__)
            raise
        logger.info("generating oauth token...complete")

        logger.info("retrieving aws credentials...")
        from botocore.config import Config
        s3_client = aws_session.client(
            "s3",
            config=Config(connect_timeout=10, read_timeout=30, retries={"max_attempts": 3}),
        )
        logger.info("retrieving aws credentials...complete")

        logger.info("reading account data from s3...")
        raw_account_data = read_table_by_dataset_id(
            s3_client, bucket, account_dataset_id, source_prefix=source_key_prefix
        )
        logger.info(f"reading account data from s3...complete ({len(raw_account_data)} rows)")

        logger.info("transforming account data...")
        normalized_sf_data = transform_account_data(
            logger, raw_account_data, DATA_COLUMNS, COLUMNS_ORDER
        )
        logger.info("transforming account data...complete")

        creation_dt = datetime.now(timezone.utc)
        file_name = f"{source}.csv"
        csv_content = normalized_sf_data.to_csv(index=False)

        logger.info("writing outbound + validation files and submitting to onelake...")
        with sec.etl_lifecycle(job_name="salesforce_ofac", task_name=schema_name,
                                data_source="salesforce", data_destination="onelake") as ctx:
            outbound_url, validation_url = write_and_submit_file(
                logger, s3_client, c1_oauth_token, bucket,
                content=csv_content,
                filename=file_name,
                file_type="CSV_WITH_HEADER",
                output_key_prefix=f"salesforce/ofac/{source}/outbound",
                writer_config=WRITER_CONFIG,
                creation_dt=creation_dt,
                validation_df=normalized_sf_data,
                validation_key_prefix=f"salesforce/ofac/{source}/validation",
                validation_file_type=validation_file_type,
            )
            ctx.detail["rows_loaded"] = len(normalized_sf_data)
        logger.info(
            f"writing outbound + validation files and submitting to onelake...complete "
            f"(outbound={outbound_url}, validation={validation_url})"
        )

        record_count = len(normalized_sf_data)
        final_state = "SUCCESS"
        return {
            "status_code": 200,
            "s3_url": outbound_url,
            "num_records": record_count,
            "message": "SUCCESS",
        }

    except Exception as e:
        failure_message = str(e)
        logger.error(f"""
Unhandled error during lambda execution. Returning error response.
Error Type: {type(e).__name__}
Error Message: {str(e)}
Stack Trace:{traceback.format_exc()}
""")
        raise Exception(
            "Unhandled error during lambda execution. See logs for details"
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

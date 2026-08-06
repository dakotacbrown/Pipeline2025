"""
Preview of the Account + BillingAccount join for salesforce_ofac.py.

NOT wired into the shipped salesforce_ofac.py yet — per Dakota, this is an
"eventually" change. This file shows what the read/transform/join/normalize
logic looks like on its own, ready to drop into salesforce_ofac.py's main()
when the time comes.

To integrate later, in salesforce_ofac.py:
  1. Replace DATA_COLUMNS with ACCOUNT_DATA_COLUMNS below (same mapping,
     renamed for clarity now that there are two column-mapping dicts).
  2. Replace COLUMNS_ORDER with the one below (adds billing_account_name).
  3. Replace the "reading account data from s3..." / "transforming account
     data..." section of main() with a single call to
     build_normalized_account_and_billing_data().
  4. Nothing else changes — write_and_submit_file, the outbound/validation
     writes, and the OneLake submission all stay exactly as they are, since
     they only care about the final normalized dataframe, not where it
     came from.

ASSUMPTION worth confirming before wiring this in: the join assumes each
Account has at most one BillingAccount row (account_id unique on the
billing side). If that's not true, this merge will fan out — one Account
row becomes multiple rows, one per matching BillingAccount. Nothing here
currently checks for that; see TestBuildNormalizedAccountAndBillingData's
fan-out test in the companion test file for what that looks like when it
happens, so it's at least visible rather than silent if the assumption
turns out to be wrong.
"""

from logging import Logger

import pandas as pd

from helpers.gl_source_join import DATASET_IDS
from helpers.helper_functions import normalize_data, rename_and_clean_columns
from helpers.s3_utils import read_table_by_dataset_id

BILLING_ACCOUNT_DATASET_ID = "78073a60-0e7a-40a5-b59a-fced1d6a89aa"

ACCOUNT_DATA_COLUMNS = {
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

BILLING_DATA_COLUMNS = {
    "name": "billing_account_name",
    "accountid": "account_id",
}

COLUMNS_ORDER = [
    "billing_account_name",
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


def build_normalized_account_and_billing_data(
    log: Logger, s3_client, bucket: str, source_prefix: str
) -> pd.DataFrame:
    """
    Reads Account (DATASET_IDS["account"], the same dataset_id the GL
    pipeline uses — see gl_source_join.py) and BillingAccount
    (BILLING_ACCOUNT_DATASET_ID) from S3, renames/cleans each
    independently via rename_and_clean_columns(), joins them on
    account_id, then applies normalize_data() once on the combined
    result.

    normalize_data() is applied AFTER the join, not per-source, since most
    of COLUMNS_ORDER's fields (billing_account_name, account_name, etc.)
    only exist once both sides are combined — reindexing either source to
    the full column list before the join would just produce a frame full
    of NaNs for whichever columns come from the other source.
    """
    log.info("reading account data from s3...")
    raw_account = read_table_by_dataset_id(
        s3_client, bucket, DATASET_IDS["account"], source_prefix=source_prefix
    )
    log.info(f"reading account data from s3...complete ({len(raw_account)} rows)")

    log.info("reading billing account data from s3...")
    raw_billing = read_table_by_dataset_id(
        s3_client, bucket, BILLING_ACCOUNT_DATASET_ID, source_prefix=source_prefix
    )
    log.info(f"reading billing account data from s3...complete ({len(raw_billing)} rows)")

    account_df = rename_and_clean_columns(log, raw_account, ACCOUNT_DATA_COLUMNS)
    billing_df = rename_and_clean_columns(log, raw_billing, BILLING_DATA_COLUMNS)

    log.info("joining account and billing account data on account_id...")
    joined = account_df.merge(billing_df, on="account_id", how="left")
    log.info(f"joining account and billing account data on account_id...complete ({len(joined)} rows)")

    log.info("normalizing combined data...")
    log.info(f"Columns before normalization: {joined.columns.tolist()}")
    normalized_data = normalize_data(joined, COLUMNS_ORDER)
    log.info(f"Columns after normalization: {normalized_data.columns.tolist()}")
    log.info("normalizing combined data... complete")

    return normalized_data

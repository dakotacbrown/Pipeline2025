"""
Source-side join: builds the flat dataframe that feeds into the GL journal
file builder. Pulls TransactionJournal + related billing objects (as JSONL
landed in S3 by api_ingester) and resolves:

  1. bu / did via TransactionJournal's TransactionType-specific path:
       InvoiceLine     -> InvoiceLine's own fields directly
       InvoiceLineTax  -> InvoiceLine (one hop via InvoiceLineId)
       Payment         -> PaymentLineInvoiceLine (line-level, currently
                           0 rows) or PaymentLineInvoice -> Invoice ->
                           InvoiceLine (header-level fallback, what
                           actually resolves data today)
       CreditMemo      -> CreditMemoLine -> CreditMemoLineInvoiceLine
                           (line-level, currently 0 rows) or
                           CreditMemoInvApplication -> Invoice ->
                           InvoiceLine (header-level fallback, what
                           actually resolves data today)
     NOTE: an earlier design routed this through
     TransactionJournal -> UsageResource -> RateCardEntry -> Product2, but
     that was abandoned — UsageResourceId/Product2Id were confirmed to
     always point to the same product regardless of transaction, a dead
     end for differentiating bu/did. UsageResource/RateCardEntry/Product2
     are not used anywhere in the current resolution and don't need to be
     pulled from S3 for this job.
  2. Account Name via TransactionJournal -> (Invoice | CreditMemo | Payment
     | Refund | InvoiceLine | InvoiceLineTax) -> Account, resolved through
     the polymorphic ReferenceTransactionRecordId

NOTE: TransactionJournal.AccountId exists on the object but is NOT populated
in practice — confirmed. The polymorphic ReferenceTransactionRecordId path
below is the only reliable way to resolve the account for a transaction.

All field names below are confirmed against the actual Salesforce object
schemas (not placeholders).
"""

import re
import pandas as pd
from helpers.s3_utils import read_table_by_dataset_id  # dataset_id-based S3 reader


# ---------------------------------------------------------------------------
# 0. Dataset IDs, pulled from the salesforce_global_one.yml for_each_task inputs.
#    Update this if the job config changes. Only tables actually loaded by
#    build_source_dataframe() are listed here — usage_resource,
#    rate_card_entry, product2, and credit_memo_line_tax were removed since
#    none of them are used in the current bu/did or account-name resolution
#    (see module docstring above for why UsageResource/RateCardEntry/Product2
#    specifically were dropped).
# ---------------------------------------------------------------------------

DATASET_IDS = {
    "account": "96f8954f-ad71-4cb5-9dbb-4d9355fb3d38",
    "credit_memo": "c3d391d6-6e3d-4d18-83a5-7d41ce223b6e",
    "credit_memo_inv_application": "beac368e-b5e0-418f-9643-63c7ae746711",
    "credit_memo_line": "3ddfc4de-6efe-4994-b104-a2ccd929754c",
    "credit_memo_line_invoice_line": "0134e58d-be02-4f45-9a77-4f91ccad31fd",
    "invoice": "89ff4754-a708-4399-9be8-2e5acc1310c9",
    "invoice_line": "54ac4870-eeec-4fe5-b27a-1a1bde0a3ad2",
    "invoice_line_tax": "cd087f0b-57f4-4c15-9e35-fa228dbed0b9",
    "payment": "cfeab860-7d21-4dac-a6ee-a808aab35ba8",
    "payment_line_invoice": "684a8f4f-21cb-4261-a9d1-69ca6b6c66e9",
    "payment_line_invoice_line": "ee3ff156-133d-4165-9a00-f752b1fc32a4",
    "refund": "2e0bb7aa-a383-47a3-8d46-205739c680d0",
    "transaction_journal": "ffa0bf8e-1c98-49c4-9935-fb6e88efae8f",
}

# Expected columns per table — used so an empty/never-populated table (e.g.
# Refund, PaymentLineInvoiceLine currently have 0 rows) still returns a
# dataframe with the right shape instead of breaking downstream merges.
EXPECTED_COLUMNS = {
    "invoice": ["Id", "BillingAccountId"],
    "credit_memo": ["Id", "BillingAccountId"],
    "payment": ["Id", "AccountId"],
    "refund": ["Id", "AccountId"],
    "account": ["Id", "AccountNumber"],
    "invoice_line": ["Id", "InvoiceId", "Product2Id", "Business_Unit_BU__c", "Department_ID_DID__c"],
    "invoice_line_tax": ["Id", "InvoiceLineId"],
    "payment_line_invoice_line": ["PaymentId", "InvoiceLineId"],
    "payment_line_invoice": ["PaymentId", "InvoiceId"],
    "credit_memo_line": ["Id", "CreditMemoId"],
    "credit_memo_line_invoice_line": ["CreditMemoLineId", "InvoiceLineId"],
    "credit_memo_inv_application": ["CreditMemoId", "InvoiceId"],
    "transaction_journal": [
        "Name", "UsageResourceId", "ReferenceTransactionRecordId",
        "TransactionType", "ActivityDate", "UsageType", "Credit", "Debit",
    ],
}


# ---------------------------------------------------------------------------
# 1. bu / did resolution — see module docstring for the per-TransactionType path
# ---------------------------------------------------------------------------

def resolve_bu_did(tj: pd.DataFrame, invoice_line: pd.DataFrame,
                    invoice_line_tax: pd.DataFrame, payment_line_invoice_line: pd.DataFrame,
                    payment_line_invoice: pd.DataFrame, credit_memo_line: pd.DataFrame,
                    credit_memo_line_invoice_line: pd.DataFrame,
                    credit_memo_inv_application: pd.DataFrame) -> pd.DataFrame:
    """
    Resolves bu/did/Product2Id per TransactionType. Confirmed TransactionType
    values: InvoiceLineTax, Payment, CreditMemo, InvoiceLine.

      InvoiceLine     -> ReferenceTransactionRecordId = InvoiceLine.Id directly
      InvoiceLineTax   -> ReferenceTransactionRecordId = InvoiceLineTax.Id,
                          then InvoiceLineTax.InvoiceLineId -> InvoiceLine
      Payment          -> ReferenceTransactionRecordId = Payment.Id (header). Two
                          possible paths:
                            (a) PaymentLineInvoiceLine.PaymentId -> InvoiceLineId -> InvoiceLine
                                (line-level — confirmed 0 rows in current data)
                            (b) PaymentLineInvoice.PaymentId -> InvoiceId ->
                                InvoiceLine.InvoiceId -> InvoiceLine
                                (header-level — confirmed 837 rows, this is what
                                actually works today)
      CreditMemo       -> ReferenceTransactionRecordId = CreditMemo.Id (header). Two
                          possible paths, same shape as Payment:
                            (a) CreditMemoLine -> CreditMemoLineInvoiceLine -> InvoiceLine
                                (line-level — confirmed 0 rows in current data)
                            (b) CreditMemoInvApplication.CreditMemoId -> InvoiceId ->
                                InvoiceLine.InvoiceId -> InvoiceLine
                                (header-level — confirmed 42 rows, matches CreditMemo
                                row count, this is what actually works today)

    Line-level paths are tried first (kept in case they get populated later);
    header-level paths are the fallback and are what's actually resolving
    Payment/CreditMemo rows right now.

    CAVEAT: both header-level fallbacks go through Invoice -> InvoiceLine,
    and an Invoice can have multiple lines. Where an invoice's lines span
    more than one bu/did, this takes the FIRST match — confirmed via SQL
    that PaymentLineInvoiceLine/CreditMemoLineInvoiceLine currently have 0
    rows, so the line-level fan-out check couldn't be run against real data;
    worth a similar check at the Invoice level if this matters later.
    """
    def empty_lookup():
        return pd.DataFrame(columns=["ReferenceTransactionRecordId", "Product2Id", "bu", "did"])

    def il_fields():
        return invoice_line[["Id", "InvoiceId", "Product2Id", "Business_Unit_BU__c", "Department_ID_DID__c"]]

    # InvoiceLine direct
    if invoice_line.empty:
        direct_lines = empty_lookup()
    else:
        direct_lines = il_fields().drop(columns=["InvoiceId"]).rename(
            columns={"Id": "ReferenceTransactionRecordId", "Business_Unit_BU__c": "bu",
                     "Department_ID_DID__c": "did"}
        )

    # InvoiceLineTax -> InvoiceLine
    if invoice_line_tax.empty or invoice_line.empty:
        via_tax = empty_lookup()
    else:
        joined = invoice_line_tax[["Id", "InvoiceLineId"]].merge(
            il_fields().drop(columns=["InvoiceId"]).rename(columns={"Id": "InvoiceLineId"}),
            on="InvoiceLineId", how="left",
        )
        via_tax = joined.rename(columns={"Id": "ReferenceTransactionRecordId", "Business_Unit_BU__c": "bu",
                                          "Department_ID_DID__c": "did"})[
            ["ReferenceTransactionRecordId", "Product2Id", "bu", "did"]
        ]

    # Payment (a): line-level, currently 0 rows
    if payment_line_invoice_line.empty or invoice_line.empty:
        via_payment_line = empty_lookup()
    else:
        joined = payment_line_invoice_line[["PaymentId", "InvoiceLineId"]].merge(
            il_fields().drop(columns=["InvoiceId"]).rename(columns={"Id": "InvoiceLineId"}),
            on="InvoiceLineId", how="left",
        )
        via_payment_line = joined.rename(columns={"PaymentId": "ReferenceTransactionRecordId",
                                                    "Business_Unit_BU__c": "bu",
                                                    "Department_ID_DID__c": "did"})[
            ["ReferenceTransactionRecordId", "Product2Id", "bu", "did"]
        ].drop_duplicates(subset="ReferenceTransactionRecordId", keep="first")

    # Payment (b): header-level fallback, this is what actually resolves today
    if payment_line_invoice.empty or invoice_line.empty:
        via_payment_header = empty_lookup()
    else:
        joined = payment_line_invoice[["PaymentId", "InvoiceId"]].merge(
            il_fields().drop(columns=["Id"]), on="InvoiceId", how="left",
        )
        via_payment_header = joined.rename(columns={"PaymentId": "ReferenceTransactionRecordId",
                                                      "Business_Unit_BU__c": "bu",
                                                      "Department_ID_DID__c": "did"})[
            ["ReferenceTransactionRecordId", "Product2Id", "bu", "did"]
        ].drop_duplicates(subset="ReferenceTransactionRecordId", keep="first")

    # CreditMemo (a): line-level, currently 0 rows
    if credit_memo_line.empty or credit_memo_line_invoice_line.empty or invoice_line.empty:
        via_cm_line = empty_lookup()
    else:
        joined = credit_memo_line[["Id", "CreditMemoId"]].rename(columns={"Id": "CreditMemoLineId"}).merge(
            credit_memo_line_invoice_line[["CreditMemoLineId", "InvoiceLineId"]],
            on="CreditMemoLineId", how="left",
        ).merge(
            il_fields().drop(columns=["InvoiceId"]).rename(columns={"Id": "InvoiceLineId"}),
            on="InvoiceLineId", how="left",
        )
        via_cm_line = joined.rename(columns={"CreditMemoId": "ReferenceTransactionRecordId",
                                              "Business_Unit_BU__c": "bu",
                                              "Department_ID_DID__c": "did"})[
            ["ReferenceTransactionRecordId", "Product2Id", "bu", "did"]
        ].drop_duplicates(subset="ReferenceTransactionRecordId", keep="first")

    # CreditMemo (b): header-level fallback, this is what actually resolves today
    if credit_memo_inv_application.empty or invoice_line.empty:
        via_cm_header = empty_lookup()
    else:
        joined = credit_memo_inv_application[["CreditMemoId", "InvoiceId"]].merge(
            il_fields().drop(columns=["Id"]), on="InvoiceId", how="left",
        )
        via_cm_header = joined.rename(columns={"CreditMemoId": "ReferenceTransactionRecordId",
                                                "Business_Unit_BU__c": "bu",
                                                "Department_ID_DID__c": "did"})[
            ["ReferenceTransactionRecordId", "Product2Id", "bu", "did"]
        ].drop_duplicates(subset="ReferenceTransactionRecordId", keep="first")

    # Line-level results come first so they win over header-level fallback
    # wherever both happen to exist for the same ReferenceTransactionRecordId.
    lookup = pd.concat(
        [direct_lines, via_tax, via_payment_line, via_payment_header, via_cm_line, via_cm_header],
        ignore_index=True,
    ).drop_duplicates(subset="ReferenceTransactionRecordId", keep="first")

    return tj.merge(lookup, on="ReferenceTransactionRecordId", how="left")


# ---------------------------------------------------------------------------
# 2. Account name via polymorphic ReferenceTransactionRecordId
# ---------------------------------------------------------------------------

def build_reference_to_account_lookup(invoice: pd.DataFrame, credit_memo: pd.DataFrame,
                                       payment: pd.DataFrame, refund: pd.DataFrame,
                                       invoice_line: pd.DataFrame, invoice_line_tax: pd.DataFrame,
                                       account: pd.DataFrame) -> pd.DataFrame:
    """
    BUG FIX: this originally only handled ReferenceTransactionRecordId values
    that point directly at a header record (Invoice.Id, CreditMemo.Id,
    Payment.Id, Refund.Id). That was correct for the 'Payment' and
    'CreditMemo' TransactionTypes, but 'InvoiceLine' and 'InvoiceLineTax' —
    two of the four confirmed TransactionType values, and likely the bulk of
    the data — point at InvoiceLine.Id / InvoiceLineTax.Id instead, which
    never match a header Id. Those rows were silently getting a null account
    name. Fixed by adding the same InvoiceLine -> Invoice walk used for
    bu/did resolution.

    Confirmed field names:
      Invoice.BillingAccountId -> Account.Id
      CreditMemo.BillingAccountId -> Account.Id
      Payment.AccountId -> Account.Id
      Refund.AccountId -> Account.Id
      InvoiceLine.InvoiceId -> Invoice.Id -> Invoice.BillingAccountId -> Account.Id
      InvoiceLineTax.InvoiceLineId -> InvoiceLine.Id -> (same as above)
      Account.AccountNumber
    """
    def prep(df, id_col_source, rename_to="AccountId"):
        if df.empty:
            return pd.DataFrame(columns=["Id", rename_to])
        return df[["Id", id_col_source]].rename(columns={id_col_source: rename_to})

    ref_frames = [
        prep(invoice, "BillingAccountId"),
        prep(credit_memo, "BillingAccountId"),
        prep(payment, "AccountId"),
        prep(refund, "AccountId"),
    ]

    # InvoiceLine -> Invoice -> BillingAccountId
    if not invoice_line.empty and not invoice.empty:
        il_to_account = invoice_line[["Id", "InvoiceId"]].merge(
            invoice[["Id", "BillingAccountId"]].rename(columns={"Id": "InvoiceId"}),
            on="InvoiceId", how="left",
        )[["Id", "BillingAccountId"]].rename(columns={"BillingAccountId": "AccountId"})
        ref_frames.append(il_to_account)
    else:
        ref_frames.append(pd.DataFrame(columns=["Id", "AccountId"]))

    # InvoiceLineTax -> InvoiceLine -> Invoice -> BillingAccountId
    if not invoice_line_tax.empty and not invoice_line.empty and not invoice.empty:
        ilt_to_il = invoice_line_tax[["Id", "InvoiceLineId"]].merge(
            invoice_line[["Id", "InvoiceId"]].rename(columns={"Id": "InvoiceLineId"}),
            on="InvoiceLineId", how="left",
        )
        ilt_to_account = ilt_to_il.merge(
            invoice[["Id", "BillingAccountId"]].rename(columns={"Id": "InvoiceId"}),
            on="InvoiceId", how="left",
        )[["Id", "BillingAccountId"]].rename(columns={"BillingAccountId": "AccountId"})
        ref_frames.append(ilt_to_account)
    else:
        ref_frames.append(pd.DataFrame(columns=["Id", "AccountId"]))

    ref_to_account_id = pd.concat(ref_frames, ignore_index=True).drop_duplicates(subset="Id")
    ref_to_account_id = ref_to_account_id.rename(columns={"Id": "ReferenceTransactionRecordId"})

    if account.empty:
        ref_to_account_id["AccountNumber"] = None
        return ref_to_account_id

    acct = account[["Id", "AccountNumber"]].rename(columns={"Id": "AccountId"})
    return ref_to_account_id.merge(acct, on="AccountId", how="left")


def clean_account_number(account_number: str, prefix_pattern: str = r"^A") -> str:
    """
    Strip the leading "A" from Account.AccountNumber. CONFIRMED via
    Salesforce API response (screenshot): real values look like "A00000213"
    — stripping the leading "A" gives "00000213", an 8-digit code that fits
    the Journal Line Account field's 10-character width exactly, matching
    that field's own spec label ("Salesforce Account Number").

    This field was originally mapped to Account.Name (the descriptive
    company name) per an earlier instruction, but that doesn't fit: full
    account names routinely exceed 10 characters and were being silently
    truncated in the actual output file. AccountNumber is the correct
    source field — confirmed, not a guess.
    """
    if pd.isna(account_number):
        return account_number
    return re.sub(prefix_pattern, "", str(account_number))


# ---------------------------------------------------------------------------
# 3. Amount: Credit (negative) or Debit (positive), whichever is populated
# ---------------------------------------------------------------------------

def resolve_amount(tj: pd.DataFrame) -> pd.Series:
    # tj.get() returns None (not an empty Series) when the column is missing
    # entirely — that breaks pd.to_numeric/.apply downstream, so fall back to
    # an all-null Series of the right length in that case.
    def numeric_column_or_nulls(col_name):
        if col_name in tj.columns:
            return pd.to_numeric(tj[col_name], errors="coerce")
        return pd.Series([None] * len(tj), index=tj.index, dtype="float64")

    credit = numeric_column_or_nulls("Credit")
    debit = numeric_column_or_nulls("Debit")
    return credit.apply(lambda v: -v if pd.notna(v) else None).combine_first(debit)


# ---------------------------------------------------------------------------
# 4. Orchestrate: build the final flat frame that feeds build_gl_file()
# ---------------------------------------------------------------------------

def build_source_dataframe(s3_client, bucket: str, source_prefix: str = "salesforce/reports",
                            log=None) -> pd.DataFrame:
    """
    Reads every source table by its dataset_id (see DATASET_IDS above) rather
    than by table name, matching how the ingester actually keys its S3
    output. Tables with 0 rows (Refund, PaymentLineInvoiceLine, and possibly
    others per the current data) come back as empty dataframes instead of
    raising, and the joins below are written to tolerate that.

    s3_client must be authenticated (see run()'s __main__ block in
    gl_journal_builder_pandas_s3.py for how this gets built via
    new_session(service_credential)).

    source_prefix: the S3 prefix each dataset_id folder sits under (e.g.
    "salesforce/reports") — comes from the DAB job YAML's source_key_prefix
    parameter, not hardcoded here, specifically so a future path change is
    a config edit rather than another code guess.

    log: optional Logger — if provided, logs progress through table reads and
    each join step. Omit for quiet/test usage.
    """
    def load(table_key):
        if log:
            log.info(f"reading {table_key} (dataset_id={DATASET_IDS[table_key]})...")
        df = read_table_by_dataset_id(
            s3_client, bucket, DATASET_IDS[table_key],
            expected_columns=EXPECTED_COLUMNS.get(table_key),
            source_prefix=source_prefix,
        )
        if log:
            log.info(f"reading {table_key}...complete ({len(df)} rows)")
        return df

    tj = load("transaction_journal")
    invoice_line = load("invoice_line")
    invoice_line_tax = load("invoice_line_tax")
    payment_line_invoice_line = load("payment_line_invoice_line")
    payment_line_invoice = load("payment_line_invoice")
    credit_memo_line = load("credit_memo_line")
    credit_memo_line_invoice_line = load("credit_memo_line_invoice_line")
    credit_memo_inv_application = load("credit_memo_inv_application")
    invoice = load("invoice")
    credit_memo = load("credit_memo")
    payment = load("payment")
    refund = load("refund")
    account = load("account")

    if log:
        log.info("resolving business_unit/department_id...")
    tj = resolve_bu_did(tj, invoice_line, invoice_line_tax, payment_line_invoice_line,
                         payment_line_invoice, credit_memo_line, credit_memo_line_invoice_line,
                         credit_memo_inv_application)
    if log:
        null_bu_count = int(tj["bu"].isna().sum())
        log.info(f"resolving business_unit/department_id...complete ({null_bu_count} rows with null bu)")

    if log:
        log.info("resolving account numbers...")
    ref_lookup = build_reference_to_account_lookup(invoice, credit_memo, payment, refund,
                                                    invoice_line, invoice_line_tax, account)
    tj = tj.merge(ref_lookup[["ReferenceTransactionRecordId", "AccountNumber"]],
                  on="ReferenceTransactionRecordId", how="left")
    if log:
        null_account_count = int(tj["AccountNumber"].isna().sum())
        log.info(f"resolving account numbers...complete ({null_account_count} rows with null account number)")

    tj["AccountNumber"] = tj["AccountNumber"].apply(clean_account_number)
    tj["amount"] = resolve_amount(tj)

    # Column names are Table.Column style directly — moved upstream from a
    # separate validation-only relabeling step, per Dakota, since keeping one
    # consistent naming scheme everywhere was preferred over a two-step
    # rename. Two of these aren't strict 1:1 source mappings, worth knowing:
    #   InvoiceLine.Business_Unit / .Department_Id: resolved
    #     polymorphically (InvoiceLine directly, or via Payment/CreditMemo
    #     fallback chains) but always ultimately InvoiceLine's own fields
    #     regardless of path.
    #   TransactionJournal.CreditDebit: derived from EITHER
    #     TransactionJournal.Credit or .Debit (whichever is populated,
    #     sign-flipped for Credit) — not a single literal field.
    result = tj[[
        "bu", "did", "ActivityDate", "TransactionType",
        "AccountNumber", "UsageType", "amount", "Name",
    ]].rename(columns={
        "bu": "InvoiceLine.Business_Unit",
        "did": "InvoiceLine.Department_Id",
        "ActivityDate": "TransactionJournal.ActivityDate",
        "TransactionType": "TransactionJournal.TransactionType",
        "AccountNumber": "Account.AccountNumber",
        "UsageType": "TransactionJournal.UsageType",
        "amount": "TransactionJournal.CreditDebit",
        "Name": "TransactionJournal.Name",  # -> Journal Header Description
    })

    return result


if __name__ == "__main__":
    # Standalone testing only — normally s3_client comes from
    # new_session(service_credential).client("s3") (see
    # gl_journal_builder_pandas_s3.py's __main__ for the real pattern).
    import boto3
    df = build_source_dataframe(boto3.client("s3"), "your-ingester-bucket",
                                 source_prefix="salesforce/reports")
    print(df.head())

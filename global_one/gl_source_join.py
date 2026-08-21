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

     Per Dakota, on top of the above, bu/did get two more passes applied
     (see apply_did_overrides()):
       - bu: defaults to "10901" for any row that resolve_bu_did() above
         couldn't match to an InvoiceLine (previously left null).
       - did: overridden to "16637" if InvoiceLine.Name contains
         "slingshot", or "16635" if it contains "databolt" (case-
         insensitive) — then, taking priority over BOTH of the above,
         overridden to "16605" whenever gl_accounting_number_c (see #2
         below) is exactly "10040049", a special caveat account per
         Dakota. GL-pipeline-specific; salesforce_ofac.py does NOT apply
         any of this.

  2. gl_accounting_number_c via TransactionJournal's DebitGeneralLedgerAccountId
     or CreditGeneralLedgerAccountId (whichever is populated — a journal
     entry is either a debit or a credit, per Dakota) ->
     GeneralLedgerAccount.Id -> GeneralLedgerAccount.GL_Accounting_Number__c.

     Replaces the old Account.AccountNumber lookup (previously via
     build_reference_to_account_lookup() + clean_account_number(), now
     removed — per Dakota, that account-number code is no longer needed).
     DATASET_IDS["account"] itself stays defined below, unrelated to this
     removal: salesforce_ofac.py and salesforce_ofac_billing_preview.py
     both still read it directly for their own separate Account/
     BillingAccount join (Account.Name / Account.AccountNumber ->
     account_name / account_id, nothing to do with GL journal building).
     They never called build_reference_to_account_lookup() or
     clean_account_number() — those two were only ever used by this file's
     own build_source_dataframe(), so once it stopped calling them they
     were confirmed fully dead code, not just unused-by-the-GL-pipeline.

NOTE: TransactionJournal.AccountId exists on the object but is NOT populated
in practice — confirmed. This is unrelated to the gl_accounting_number_c
resolution above (which goes through Debit/CreditGeneralLedgerAccountId, not
AccountId) — noted here since it was relevant to the now-removed Account path.

All field names below are confirmed against the actual Salesforce object
schemas (not placeholders).
"""

from datetime import datetime, timezone

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
    # Still used by salesforce_ofac.py and salesforce_ofac_billing_preview.py
    # (both independently read DATASET_IDS["account"]) — NOT removed even
    # though the GL journal pipeline (build_source_dataframe() below) no
    # longer loads or joins it. Per Dakota: Account is no longer needed for
    # the GL journal specifically, not for OFAC.
    "account": "96f8954f-ad71-4cb5-9dbb-4d9355fb3d38",
    "credit_memo": "c3d391d6-6e3d-4d18-83a5-7d41ce223b6e",
    "credit_memo_inv_application": "beac368e-b5e0-418f-9643-63c7ae746711",
    "credit_memo_line": "3ddfc4de-6efe-4994-b104-a2ccd929754c",
    "credit_memo_line_invoice_line": "0134e58d-be02-4f45-9a77-4f91ccad31fd",
    # GeneralLedgerAccount dataset_id, confirmed by Dakota.
    "general_ledger_account": "78e7dd64-9999-42f1-a44a-38bf5157375e",
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
    "general_ledger_account": ["Id", "GL_Accounting_Number__c"],
    # "Name" added for the slingshot/databolt did override (see
    # apply_did_overrides()) — every other column here was already loaded
    # for bu/did resolution.
    "invoice_line": [
        "Id", "InvoiceId", "Product2Id", "Business_Unit_BU__c",
        "Department_ID_DID__c", "Name",
    ],
    "invoice_line_tax": ["Id", "InvoiceLineId"],
    "payment_line_invoice_line": ["PaymentId", "InvoiceLineId"],
    "payment_line_invoice": ["PaymentId", "InvoiceId"],
    "credit_memo_line": ["Id", "CreditMemoId"],
    "credit_memo_line_invoice_line": ["CreditMemoLineId", "InvoiceLineId"],
    "credit_memo_inv_application": ["CreditMemoId", "InvoiceId"],
    "transaction_journal": [
        "Name", "UsageResourceId", "ReferenceTransactionRecordId",
        "TransactionType", "ActivityDate", "UsageType", "Credit", "Debit",
        # Added for gl_accounting_number_c resolution (see
        # resolve_gl_accounting_number()).
        "DebitGeneralLedgerAccountId", "CreditGeneralLedgerAccountId",
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

    Also carries InvoiceLine.Name through as "InvoiceLineName" (renamed to
    avoid colliding with TransactionJournal's own "Name" column once merged
    back into tj) — needed by apply_did_overrides()'s slingshot/databolt
    check. Populated defensively: if invoice_line doesn't have a "Name"
    column at all (older callers/tests that predate this), InvoiceLineName
    just comes back all-null instead of raising, same tolerance-for-missing-
    columns pattern used elsewhere in this file (e.g. resolve_amount's
    tj.get()).
    """
    def empty_lookup():
        return pd.DataFrame(columns=["ReferenceTransactionRecordId", "Product2Id", "bu", "did", "InvoiceLineName"])

    def il_fields():
        cols = ["Id", "InvoiceId", "Product2Id", "Business_Unit_BU__c", "Department_ID_DID__c"]
        fields = invoice_line[cols].copy()
        fields["Name"] = invoice_line["Name"] if "Name" in invoice_line.columns else None
        return fields

    rename_map = {
        "Id": "ReferenceTransactionRecordId",
        "Business_Unit_BU__c": "bu",
        "Department_ID_DID__c": "did",
        "Name": "InvoiceLineName",
    }
    result_cols = ["ReferenceTransactionRecordId", "Product2Id", "bu", "did", "InvoiceLineName"]

    # InvoiceLine direct
    if invoice_line.empty:
        direct_lines = empty_lookup()
    else:
        direct_lines = il_fields().drop(columns=["InvoiceId"]).rename(columns=rename_map)

    # InvoiceLineTax -> InvoiceLine
    if invoice_line_tax.empty or invoice_line.empty:
        via_tax = empty_lookup()
    else:
        joined = invoice_line_tax[["Id", "InvoiceLineId"]].merge(
            il_fields().drop(columns=["InvoiceId"]).rename(columns={"Id": "InvoiceLineId"}),
            on="InvoiceLineId", how="left",
        )
        via_tax = joined.rename(columns=rename_map)[result_cols]

    # Payment (a): line-level, currently 0 rows
    if payment_line_invoice_line.empty or invoice_line.empty:
        via_payment_line = empty_lookup()
    else:
        joined = payment_line_invoice_line[["PaymentId", "InvoiceLineId"]].merge(
            il_fields().drop(columns=["InvoiceId"]).rename(columns={"Id": "InvoiceLineId"}),
            on="InvoiceLineId", how="left",
        )
        via_payment_line = joined.rename(columns={**rename_map, "PaymentId": "ReferenceTransactionRecordId"})[
            result_cols
        ].drop_duplicates(subset="ReferenceTransactionRecordId", keep="first")

    # Payment (b): header-level fallback, this is what actually resolves today
    if payment_line_invoice.empty or invoice_line.empty:
        via_payment_header = empty_lookup()
    else:
        joined = payment_line_invoice[["PaymentId", "InvoiceId"]].merge(
            il_fields().drop(columns=["Id"]), on="InvoiceId", how="left",
        )
        via_payment_header = joined.rename(columns={**rename_map, "PaymentId": "ReferenceTransactionRecordId"})[
            result_cols
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
        via_cm_line = joined.rename(columns={**rename_map, "CreditMemoId": "ReferenceTransactionRecordId"})[
            result_cols
        ].drop_duplicates(subset="ReferenceTransactionRecordId", keep="first")

    # CreditMemo (b): header-level fallback, this is what actually resolves today
    if credit_memo_inv_application.empty or invoice_line.empty:
        via_cm_header = empty_lookup()
    else:
        joined = credit_memo_inv_application[["CreditMemoId", "InvoiceId"]].merge(
            il_fields().drop(columns=["Id"]), on="InvoiceId", how="left",
        )
        via_cm_header = joined.rename(columns={**rename_map, "CreditMemoId": "ReferenceTransactionRecordId"})[
            result_cols
        ].drop_duplicates(subset="ReferenceTransactionRecordId", keep="first")

    # Line-level results come first so they win over header-level fallback
    # wherever both happen to exist for the same ReferenceTransactionRecordId.
    lookup = pd.concat(
        [direct_lines, via_tax, via_payment_line, via_payment_header, via_cm_line, via_cm_header],
        ignore_index=True,
    ).drop_duplicates(subset="ReferenceTransactionRecordId", keep="first")

    return tj.merge(lookup, on="ReferenceTransactionRecordId", how="left")


# ---------------------------------------------------------------------------
# 2b. gl_accounting_number_c via TransactionJournal's Debit/CreditGeneralLedgerAccountId
# ---------------------------------------------------------------------------

def resolve_gl_accounting_number(tj: pd.DataFrame, general_ledger_account: pd.DataFrame) -> pd.Series:
    """
    Resolves gl_accounting_number_c: TransactionJournal.DebitGeneralLedgerAccountId
    OR .CreditGeneralLedgerAccountId (whichever is populated — a journal
    entry is either a debit or a credit, per Dakota) ->
    GeneralLedgerAccount.Id -> GeneralLedgerAccount.GL_Accounting_Number__c.

    Replaces the old Account.AccountNumber lookup for the GL journal
    pipeline specifically (see module docstring). Unlike that old path, no
    leading-character stripping is applied here — GL_Accounting_Number__c
    is used directly, since (per Dakota) it isn't a Salesforce Account ID
    and the "A"-prefix quirk clean_account_number() handled doesn't apply
    to it.

    general_ledger_account.empty -> an all-null Series (same
    empty-table tolerance pattern as the rest of this file). tj.empty ->
    an empty Series, so callers can always safely do
    tj["gl_accounting_number_c"] = resolve_gl_accounting_number(tj, ...)
    regardless of whether tj has any rows.

    tj missing DebitGeneralLedgerAccountId/CreditGeneralLedgerAccountId
    entirely (e.g. an older tj shape) is also tolerated rather than
    raising — same tj.get()-style fallback resolve_amount() uses for
    Credit/Debit.
    """
    if tj.empty:
        return pd.Series([], index=tj.index, dtype="object")

    if general_ledger_account.empty:
        return pd.Series([None] * len(tj), index=tj.index, dtype="object")

    def column_or_nulls(col_name):
        if col_name in tj.columns:
            return tj[col_name]
        return pd.Series([None] * len(tj), index=tj.index, dtype="object")

    gl_account_id = column_or_nulls("DebitGeneralLedgerAccountId").combine_first(
        column_or_nulls("CreditGeneralLedgerAccountId")
    )

    id_to_number = (
        general_ledger_account.drop_duplicates(subset="Id", keep="first")
        .set_index("Id")["GL_Accounting_Number__c"]
    )
    return gl_account_id.map(id_to_number)


# ---------------------------------------------------------------------------
# 2c. did overrides applied on top of resolve_bu_did()'s output
# ---------------------------------------------------------------------------

def apply_did_overrides(df: pd.DataFrame) -> pd.DataFrame:
    """
    Applies, in priority order (each step can overwrite the one before it —
    listed low to high priority):

      1. bu default: any row where resolve_bu_did() couldn't match an
         InvoiceLine (null "bu") falls back to "10901" — per Dakota, "For
         the most part, all BUs should be 10901 as the default value, or
         pulled from invoice line like it currently does."
      2. did override by product name: "did" -> "16637" if InvoiceLineName
         contains "slingshot" (case-insensitive), or "16635" if it contains
         "databolt".
      3. did override by GL account (HIGHEST priority — applied last, so it
         wins over #2 above): "did" -> "16605" whenever
         gl_accounting_number_c is exactly "10040049" — a special caveat
         account per Dakota ("The account number 10040049 should supersede
         whatever the DID is").

    Requires "bu", "did", "InvoiceLineName" (from resolve_bu_did()) and
    "gl_accounting_number_c" (from resolve_gl_accounting_number()) to
    already be present as columns on df.

    Blank/whitespace-only string values in "bu" or "did" are normalized to
    a real null BEFORE step 1 runs — confirmed with Dakota: Salesforce can
    return an optional text field as "" rather than a true null, and that
    should get the same treatment (bu default / normal did resolution) as
    an actual null, not pass through untouched as a literal empty string.

    GL-journal-pipeline-specific — per Dakota, "OFAC doesn't need it".
    salesforce_ofac.py doesn't call this.
    """
    df = df.copy()

    def blank_to_null_inplace(col: str):
        # Targeted .loc assignment on only the blank-string cells, not a
        # whole-column .apply()/.map() reconstruction — confirmed that
        # .apply() on an all-null object-dtype column silently re-infers
        # its dtype as float64 (losing object dtype), which then breaks
        # the string assignments below ("Invalid value '16637' for dtype
        # 'float64'"). Assigning None into only the matching cells avoids
        # rebuilding the column at all when there's nothing blank to fix,
        # and is safe even when there is: a column can only actually
        # contain a blank string if it's already object-dtype, and None
        # is always a safe value to assign into an object-dtype column.
        is_blank = df[col].map(lambda v: isinstance(v, str) and v.strip() == "")
        if is_blank.any():
            df.loc[is_blank, col] = None

    blank_to_null_inplace("bu")
    blank_to_null_inplace("did")

    df["bu"] = df["bu"].fillna("10901")

    name_lower = df["InvoiceLineName"].fillna("").str.lower()
    df.loc[name_lower.str.contains("slingshot"), "did"] = "16637"
    df.loc[name_lower.str.contains("databolt"), "did"] = "16635"

    df.loc[df["gl_accounting_number_c"] == "10040049", "did"] = "16605"

    return df


# ---------------------------------------------------------------------------
# 2d. ActivityDate window — month-to-date by default, or an explicit range
# ---------------------------------------------------------------------------

def resolve_date_window(start_date: str = None, end_date: str = None) -> "tuple[datetime, datetime]":
    """
    Resolves the ActivityDate filter window for TransactionJournal.

    TransactionJournal.ActivityDate is a full datetime in Salesforce (e.g.
    "2026-08-12T17:47:25.000+0000") — confirmed via Salesforce API
    response, not a plain date — so the window bounds returned here are
    datetimes, not dates.

    Default (start_date/end_date both None): month-to-date — midnight UTC
    on the 1st of the current month through the moment this function runs.
    Per Dakota: "the data needed should only be from the beginning of the
    current month until the current day" by default, or an explicit
    start/end can be passed in (e.g. from a Databricks job parameter).

    If provided, start_date/end_date are YYYY-MM-DD strings, each taken at
    midnight UTC. Providing only one of the two raises — either both are
    given, or neither (for the month-to-date default).
    """
    now = datetime.now(timezone.utc)
    if start_date is None and end_date is None:
        window_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        return window_start, now
    if start_date is None or end_date is None:
        raise ValueError(
            "start_date and end_date must both be provided, or both omitted "
            "for the month-to-date default."
        )
    window_start = datetime.fromisoformat(start_date).replace(tzinfo=timezone.utc)
    window_end = datetime.fromisoformat(end_date).replace(tzinfo=timezone.utc)
    return window_start, window_end


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
                            log=None, start_date: str = None, end_date: str = None) -> pd.DataFrame:
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

    start_date / end_date: optional YYYY-MM-DD strings bounding
    TransactionJournal.ActivityDate — see resolve_date_window(). Both
    omitted (the default) filters to month-to-date. Applied right after
    transaction_journal is loaded and before any of the joins below, so a
    narrow window also means less data flowing through the rest of this
    function.

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

    window_start, window_end = resolve_date_window(start_date, end_date)
    if log:
        log.info(f"filtering transaction_journal to ActivityDate {window_start}..{window_end}...")
    if not tj.empty:
        tj["ActivityDate"] = pd.to_datetime(tj["ActivityDate"], utc=True)
        tj = tj[(tj["ActivityDate"] >= window_start) & (tj["ActivityDate"] <= window_end)]
    if log:
        log.info(f"filtering transaction_journal to ActivityDate {window_start}..{window_end}...complete ({len(tj)} rows)")

    invoice_line = load("invoice_line")
    invoice_line_tax = load("invoice_line_tax")
    payment_line_invoice_line = load("payment_line_invoice_line")
    payment_line_invoice = load("payment_line_invoice")
    credit_memo_line = load("credit_memo_line")
    credit_memo_line_invoice_line = load("credit_memo_line_invoice_line")
    credit_memo_inv_application = load("credit_memo_inv_application")
    # invoice/credit_memo/payment/refund/account (Account itself) are no
    # longer loaded here — per Dakota, Account is no longer needed for the
    # GL journal pipeline. general_ledger_account replaces it as the source
    # of the journal line's account field. See module docstring for why
    # build_reference_to_account_lookup()/clean_account_number() are still
    # defined in this file even though this function no longer calls them.
    general_ledger_account = load("general_ledger_account")

    if log:
        log.info("resolving business_unit/department_id...")
    tj = resolve_bu_did(tj, invoice_line, invoice_line_tax, payment_line_invoice_line,
                         payment_line_invoice, credit_memo_line, credit_memo_line_invoice_line,
                         credit_memo_inv_application)
    if log:
        null_bu_count = int(tj["bu"].isna().sum())
        log.info(f"resolving business_unit/department_id...complete ({null_bu_count} rows with null bu)")

    if log:
        log.info("resolving gl accounting numbers...")
    tj["gl_accounting_number_c"] = resolve_gl_accounting_number(tj, general_ledger_account)
    if log:
        null_gl_count = int(tj["gl_accounting_number_c"].isna().sum())
        log.info(f"resolving gl accounting numbers...complete ({null_gl_count} rows with null gl accounting number)")

    if log:
        log.info("applying bu default and did overrides...")
    tj = apply_did_overrides(tj)
    if log:
        log.info("applying bu default and did overrides...complete")

    tj["amount"] = resolve_amount(tj)

    # Column names are Table.Column style directly — moved upstream from a
    # separate validation-only relabeling step, per Dakota, since keeping one
    # consistent naming scheme everywhere was preferred over a two-step
    # rename. Two of these aren't strict 1:1 source mappings, worth knowing:
    #   InvoiceLine.Business_Unit / .Department_Id: resolved
    #     polymorphically (InvoiceLine directly, or via Payment/CreditMemo
    #     fallback chains) but always ultimately InvoiceLine's own fields
    #     regardless of path — then bu defaulted / did overridden per
    #     apply_did_overrides() above.
    #   TransactionJournal.CreditDebit: derived from EITHER
    #     TransactionJournal.Credit or .Debit (whichever is populated,
    #     sign-flipped for Credit) — not a single literal field.
    #   GeneralLedgerAccount.GL_Accounting_Number__c: resolved via
    #     TransactionJournal's Debit/CreditGeneralLedgerAccountId (see
    #     resolve_gl_accounting_number()), not a direct TransactionJournal
    #     field either.
    result = tj[[
        "bu", "did", "ActivityDate", "TransactionType",
        "gl_accounting_number_c", "UsageType", "amount", "Name",
    ]].rename(columns={
        "bu": "InvoiceLine.Business_Unit",
        "did": "InvoiceLine.Department_Id",
        "ActivityDate": "TransactionJournal.ActivityDate",
        "TransactionType": "TransactionJournal.TransactionType",
        "gl_accounting_number_c": "GeneralLedgerAccount.GL_Accounting_Number__c",
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

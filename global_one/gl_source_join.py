"""
Source-side join: builds the flat dataframe that feeds into the GL journal
file builder. Pulls TransactionJournal + related billing objects (as JSONL
landed in S3 by api_ingester) and resolves:

  0. TransactionType coverage: the field's full picklist has 15 confirmed
     values (per Dakota's screenshots of the field's picklistValues) —
     every one of them now has a resolution path built (see
     resolve_product2_id()'s docstring for the full per-type join chain).
     DebitMemoLineTax is the one exception worth knowing about: per
     Dakota, no corresponding table exists in Salesforce for it YET — the
     join chain is still fully built (DATASET_IDS["debit_memo_line_tax"]
     is a PLACEHOLDER), so nothing needs to change here once the table
     goes live except that one value.

  1. bu / did — Product2 is the SOLE source, universally, across every
     TransactionType, per Dakota: "Product2 should not have a null or
     blank did/bu... it's okay to remove the invoice line resolution."
     (InvoiceLine's own Business_Unit_BU__c/Department_ID_DID__c fields
     are no longer read or joined for bu/did purposes at all — an earlier
     version of this module used InvoiceLine as a two-tier fallback; that
     layer is gone.)

     Resolution is two steps:
       a) resolve_product2_id() — finds a Product2Id per TransactionType,
          via whichever chain reaches Product2 for that type (see that
          function's own docstring for the full breakdown per type).
       b) resolve_product2_fields() — Product2.Id -> .Business_Unit_BU__c /
          .Department_ID_DID__c / .Name (the .Name is used for the
          slingshot/databolt did override below — Product2.Name replaces
          InvoiceLine.Name for that check entirely, per Dakota).

     NOTE: this is NOT the same Product2 path abandoned much earlier (see
     the old note this replaces) — that one went
     TransactionJournal -> UsageResource -> RateCardEntry -> Product2,
     confirmed to always land on the same product regardless of
     transaction, a dead end. This path goes directly from each type's own
     Product2Id (either straight off the entry-point table, e.g.
     CreditMemoLine.Product2Id, or via one hop to a table that has one,
     e.g. InvoiceLine.Product2Id) — a different, confirmed-viable source.

     Per Dakota, on top of (a)/(b) above, bu/did get two more passes
     applied (see apply_did_overrides()):
       - bu: defaults to "10901" for any row that still has a null bu
         after Product2 resolution (expected to rarely fire in practice,
         per Dakota — "Product2 should not have a null or blank did/bu" —
         but kept as a defensive fallback, not removed).
       - did: overridden to "16637" if Product2.Name contains
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

  3. validate_required_tables_present() — per Dakota: "If the transaction
     [journal], product2, general ledger account tables, or any table
     under a distinct list from transaction type are empty it should
     fail. That would mean a data issue is present." transaction_journal,
     product2, and general_ledger_account are unconditionally mandatory.
     For every OTHER table, only the TransactionType values ACTUALLY
     PRESENT in a given run's transaction_journal data determine what's
     required — and a type is only considered unresolvable (and thus
     failing) if EVERY ONE of its possible resolution paths is dead, not
     if just one of several fallback paths happens to be empty (confirmed
     with Dakota directly — e.g. PaymentLineInvoiceLine has been 0 rows
     this whole project, which is known/expected state, not a data issue,
     since Payment's header-level path resolves fine on its own).

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
#    build_source_dataframe() (or, for "account", by salesforce_ofac.py/
#    salesforce_ofac_billing_preview.py) are listed here.
#    usage_resource/rate_card_entry were removed earlier (the abandoned
#    Product2 path — see module docstring above). "invoice"/"credit_memo"
#    were removed here too — leftover from the deleted
#    build_reference_to_account_lookup() (see module docstring's #2 —
#    confirmed genuinely dead, not referenced by the GL pipeline OR by
#    OFAC, unlike "account" which OFAC still needs directly).
# ---------------------------------------------------------------------------

DATASET_IDS = {
    # Still used by salesforce_ofac.py and salesforce_ofac_billing_preview.py
    # (both independently read DATASET_IDS["account"]) — NOT removed even
    # though the GL journal pipeline (build_source_dataframe() below) no
    # longer loads or joins it. Per Dakota: Account is no longer needed for
    # the GL journal specifically, not for OFAC.
    "account": "96f8954f-ad71-4cb5-9dbb-4d9355fb3d38",
    "credit_memo_inv_application": "beac368e-b5e0-418f-9643-63c7ae746711",
    "credit_memo_line": "3ddfc4de-6efe-4994-b104-a2ccd929754c",
    "credit_memo_line_invoice_line": "0134e58d-be02-4f45-9a77-4f91ccad31fd",
    # Confirmed by Dakota — real dataset_id from the ingest_revcloud.yml
    # for_each_task inputs.
    "debit_memo_line": "70a38b7c-47b7-4664-8f7b-14c1e49d358e",
    # GeneralLedgerAccount dataset_id, confirmed by Dakota.
    "general_ledger_account": "78e7dd64-9999-42f1-a44a-38bf5157375e",
    "invoice_line": "54ac4870-eeec-4fe5-b27a-1a1bde0a3ad2",
    "invoice_line_tax": "cd087f0b-57f4-4c15-9e35-fa228dbed0b9",
    "payment": "cfeab860-7d21-4dac-a6ee-a808aab35ba8",
    "payment_line_invoice": "684a8f4f-21cb-4261-a9d1-69ca6b6c66e9",
    "payment_line_invoice_line": "ee3ff156-133d-4165-9a00-f752b1fc32a4",
    "refund": "2e0bb7aa-a383-47a3-8d46-205739c680d0",
    # PLACEHOLDER — RefundLinePayment's dataset_id registration hasn't gone
    # through yet, per Dakota (confirmed via the ingest_revcloud.yml
    # screenshot: "dataset_id": ""). Until this is a real value, the Refund
    # and RefundLinePayment TransactionType resolution paths below are
    # structurally complete but will always resolve to null bu/did (falling
    # to the "10901" default) — see
    # TestDatasetConfig.test_refund_line_payment_dataset_id_still_needs_real_value
    # in test_gl_source_join.py, which fails on purpose until this is fixed.
    "refund_line_payment": "00000000-0000-0000-0000-000000000000",
    # Confirmed by Dakota — real dataset_id from the ingest_revcloud.yml
    # for_each_task inputs. Product2 is now the SOLE bu/did source (see
    # resolve_product2_fields()) — InvoiceLine's own bu/did is no longer
    # used at all, per Dakota: "Product2 should not have a null or blank
    # did/bu... it's okay to remove the invoice line resolution."
    "product2": "29351664-7f3c-4266-8937-018cc5a7dd44",
    "transaction_journal": "ffa0bf8e-1c98-49c4-9935-fb6e88efae8f",
    # Real dataset_id, from the same ingest_revcloud.yml for_each_task
    # inputs as everything else above (was present from the very first
    # screenshot, just never wired up until CreditMemoLineTax needed it as
    # its own TransactionType — see resolve_product2_id()).
    "credit_memo_line_tax": "1fbd49b9-7417-484c-acd8-8abf7208b670",
    # PLACEHOLDER — per Dakota, no corresponding table exists in Salesforce
    # for DebitMemoLineTax AT ALL yet (distinct from refund_line_payment's
    # situation above, which has a real table just not yet registered).
    # "I'll still need to have the mapping in case it goes live" — built
    # structurally complete below so nothing needs to change except this
    # one value once the table exists. Until then, validate_required_tables_present()
    # only requires this table's presence if a real TransactionJournal row
    # actually has TransactionType="DebitMemoLineTax" — which can't happen
    # today since no such table/data exists to produce one.
    "debit_memo_line_tax": "00000000-0000-0000-0000-000000000002",
}

# Expected columns per table — used so an empty/never-populated table (e.g.
# Refund, PaymentLineInvoiceLine currently have 0 rows) still returns a
# dataframe with the right shape instead of breaking downstream merges.
EXPECTED_COLUMNS = {
    # "invoice"/"credit_memo" removed — leftover from the deleted
    # build_reference_to_account_lookup(), confirmed unused anywhere (not
    # the GL pipeline, not OFAC) — see the DATASET_IDS comment above.
    # AccountId dropped from payment/refund below — these two were only
    # ever needed by that same now-deleted Account lookup. Both are loaded
    # again now, but for a different reason: Payment/Refund are join hops
    # in the Refund/RefundLinePayment TransactionType resolution below
    # (see resolve_product2_id()) — only "Id" is needed for that.
    "payment": ["Id"],
    "refund": ["Id"],
    "refund_line_payment": ["Id", "RefundId", "PaymentId"],
    "account": ["Id", "AccountNumber"],
    "general_ledger_account": ["Id", "GL_Accounting_Number__c"],
    # "Name" added for the slingshot/databolt did override — moved to
    # Product2.Name, per Dakota ("we shouldn't need invoice line.name
    # anymore for the name check"). See apply_did_overrides().
    "product2": ["Id", "Business_Unit_BU__c", "Department_ID_DID__c", "Name"],
    "invoice_line": [
        "Id", "InvoiceId", "Product2Id",
    ],
    "invoice_line_tax": ["Id", "InvoiceLineId"],
    # "Id" added — PaymentLineInvoiceLine is now ALSO its own standalone
    # TransactionType entry point (ReferenceTransactionRecordId =
    # PaymentLineInvoiceLine.Id directly), not just an intermediate hop
    # for TransactionType="Payment".
    "payment_line_invoice_line": ["Id", "PaymentId", "InvoiceLineId"],
    # "Id" added — same reasoning as payment_line_invoice_line above, for
    # TransactionType="PaymentLineInvoice".
    "payment_line_invoice": ["Id", "PaymentId", "InvoiceId"],
    # Product2Id — CreditMemoLine carries its own Product2Id directly,
    # letting Product2 resolution skip CreditMemoLineInvoiceLine entirely
    # (see resolve_product2_id()'s credit_memo_line_direct candidate).
    "credit_memo_line": ["Id", "CreditMemoId", "Product2Id"],
    # "Id" added — CreditMemoLineInvoiceLine is now ALSO its own
    # standalone TransactionType entry point, not just an intermediate hop
    # for TransactionType="CreditMemo".
    "credit_memo_line_invoice_line": ["Id", "CreditMemoLineId", "InvoiceLineId"],
    # "Id" added — same reasoning, for TransactionType="CreditMemoInvApplication".
    "credit_memo_inv_application": ["Id", "CreditMemoId", "InvoiceId"],
    # New table — TransactionType="CreditMemoLineTax" -> CreditMemoLineId
    # -> CreditMemoLine.Product2Id.
    "credit_memo_line_tax": ["Id", "CreditMemoLineId"],
    # Product2Id — same reasoning as credit_memo_line above, skips the
    # ReferenceRecordId -> InvoiceLine hop entirely for Product2 resolution.
    "debit_memo_line": ["Id", "ReferenceRecordId", "Product2Id"],
    # New table, doesn't exist in Salesforce yet (per Dakota) — built
    # structurally complete for when it does. TransactionType=
    # "DebitMemoLineTax" -> DebitMemoLineId -> DebitMemoLine.Product2Id.
    "debit_memo_line_tax": ["Id", "DebitMemoLineId"],
    "transaction_journal": [
        "Name", "UsageResourceId", "ReferenceTransactionRecordId",
        "TransactionType", "ActivityDate", "UsageType", "Credit", "Debit",
        # Added for gl_accounting_number_c resolution (see
        # resolve_gl_accounting_number()).
        "DebitGeneralLedgerAccountId", "CreditGeneralLedgerAccountId",
    ],
}


# ---------------------------------------------------------------------------
# 1. Product2Id resolution — see module docstring for the bu/did strategy.
#    REQUIRED_TABLES_BY_TRANSACTION_TYPE below doubles as both the
#    validation config for validate_required_tables_present() AND
#    documentation of exactly which tables back each TransactionType.
# ---------------------------------------------------------------------------

# Each TransactionType maps to a list of "OR paths" — each inner list is
# an "AND" of table keys that together form ONE complete resolution path.
# A type is resolvable if AT LEAST ONE of its paths has every table in it
# non-empty (see validate_required_tables_present()). This list is also
# the authoritative map of what resolve_product2_id() actually joins
# below — keep the two in sync if either changes.
REQUIRED_TABLES_BY_TRANSACTION_TYPE = {
    "InvoiceLine": [["invoice_line"]],
    "InvoiceLineTax": [["invoice_line_tax", "invoice_line"]],
    "DebitMemoLine": [["debit_memo_line"]],
    "Payment": [
        ["payment_line_invoice_line", "invoice_line"],
        ["payment_line_invoice", "invoice_line"],
    ],
    "CreditMemo": [
        ["credit_memo_line"],
        ["credit_memo_inv_application", "invoice_line"],
    ],
    "RefundLinePayment": [
        ["refund_line_payment", "payment", "payment_line_invoice_line", "invoice_line"],
        ["refund_line_payment", "payment", "payment_line_invoice", "invoice_line"],
    ],
    "Refund": [
        ["refund", "refund_line_payment", "payment", "payment_line_invoice_line", "invoice_line"],
        ["refund", "refund_line_payment", "payment", "payment_line_invoice", "invoice_line"],
    ],
    "Invoice": [["invoice_line"]],
    "CreditMemoLine": [["credit_memo_line"]],
    "CreditMemoLineTax": [["credit_memo_line_tax", "credit_memo_line"]],
    "PaymentLineInvoice": [["payment_line_invoice", "invoice_line"]],
    "PaymentLineInvoiceLine": [["payment_line_invoice_line", "invoice_line"]],
    "CreditMemoInvApplication": [["credit_memo_inv_application", "credit_memo_line"]],
    "CreditMemoLineInvoiceLine": [["credit_memo_line_invoice_line", "credit_memo_line"]],
    "DebitMemoLineTax": [["debit_memo_line_tax", "debit_memo_line"]],
}

# Unconditionally required regardless of which TransactionType values are
# present — per Dakota: "if [any of these] are empty it should fail. That
# would mean a data issue is present."
MANDATORY_TABLES = ["transaction_journal", "product2", "general_ledger_account"]


def resolve_product2_id(tj: pd.DataFrame, invoice_line: pd.DataFrame,
                         invoice_line_tax: pd.DataFrame, payment_line_invoice_line: pd.DataFrame,
                         payment_line_invoice: pd.DataFrame, credit_memo_line: pd.DataFrame,
                         credit_memo_line_invoice_line: pd.DataFrame,
                         credit_memo_inv_application: pd.DataFrame,
                         debit_memo_line: pd.DataFrame, payment: pd.DataFrame,
                         refund: pd.DataFrame, refund_line_payment: pd.DataFrame,
                         credit_memo_line_tax: pd.DataFrame,
                         debit_memo_line_tax: pd.DataFrame) -> pd.DataFrame:
    """
    Resolves Product2Id per TransactionType — the sole remaining job of
    this function (previously named resolve_bu_did(); renamed because it
    no longer resolves bu/did directly — see resolve_product2_fields() /
    apply_product2_bu_did() for that, now a separate step since Product2
    is the sole bu/did source, not an InvoiceLine-priority merge).

    All 15 confirmed TransactionType picklist values are covered — see
    REQUIRED_TABLES_BY_TRANSACTION_TYPE above for the authoritative table
    dependencies per type, kept in sync with the joins below:

      InvoiceLine       -> InvoiceLine.Product2Id directly
      InvoiceLineTax    -> InvoiceLineTax.InvoiceLineId -> InvoiceLine.Product2Id
      DebitMemoLine     -> DebitMemoLine.Product2Id directly
      Payment           -> (a) PaymentLineInvoiceLine.PaymentId -> InvoiceLineId
                            -> InvoiceLine.Product2Id (line-level, currently 0 rows)
                            OR (b) PaymentLineInvoice.PaymentId -> InvoiceId ->
                            InvoiceLine.InvoiceId -> InvoiceLine.Product2Id
                            (header-level, what resolves data today)
      CreditMemo        -> tried in this order: (a) DIRECT —
                            CreditMemoLine.CreditMemoId = ReferenceTransactionRecordId
                            -> CreditMemoLine.Product2Id (bypasses both
                            junction tables entirely); (b) line —
                            CreditMemoLine -> CreditMemoLineInvoiceLine ->
                            InvoiceLine.Product2Id; (c) header —
                            CreditMemoInvApplication -> InvoiceLine.Product2Id
      RefundLinePayment -> direct entry (RefundLinePayment.Id) -> Payment
                            (explicit join hop, per Dakota) -> same (a)/(b)
                            split as Payment above
      Refund            -> Refund.Id -> RefundLinePayment.RefundId (one hop
                            earlier than RefundLinePayment above) -> Payment
                            -> same (a)/(b) split
      Invoice           -> Invoice.Id (= ReferenceTransactionRecordId
                            directly, no separate Invoice table load needed
                            — same "skip the header object" pattern as
                            PaymentLineInvoice/CreditMemoInvApplication
                            elsewhere) -> InvoiceLine.InvoiceId ->
                            InvoiceLine.Product2Id
      CreditMemoLine    -> direct entry (CreditMemoLine.Id) ->
                            CreditMemoLine.Product2Id
      CreditMemoLineTax -> CreditMemoLineTax.CreditMemoLineId ->
                            CreditMemoLine.Id -> CreditMemoLine.Product2Id
      PaymentLineInvoice -> direct entry (PaymentLineInvoice.Id) ->
                            PaymentLineInvoice.InvoiceId ->
                            InvoiceLine.InvoiceId -> InvoiceLine.Product2Id
      PaymentLineInvoiceLine -> direct entry (PaymentLineInvoiceLine.Id) ->
                            PaymentLineInvoiceLine.InvoiceLineId ->
                            InvoiceLine.Id -> InvoiceLine.Product2Id
      CreditMemoInvApplication -> direct entry (CreditMemoInvApplication.Id)
                            -> CreditMemoInvApplication.CreditMemoId ->
                            CreditMemoLine (via CreditMemoLine.CreditMemoId)
                            -> CreditMemoLine.Product2Id
      CreditMemoLineInvoiceLine -> direct entry (CreditMemoLineInvoiceLine.Id)
                            -> CreditMemoLineInvoiceLine.CreditMemoLineId ->
                            CreditMemoLine.Id -> CreditMemoLine.Product2Id
      DebitMemoLineTax  -> DebitMemoLineTax.DebitMemoLineId ->
                            DebitMemoLine.Id -> DebitMemoLine.Product2Id.
                            No corresponding table exists in Salesforce YET
                            (per Dakota) — structurally complete, gated on
                            DATASET_IDS["debit_memo_line_tax"] (still a
                            PLACEHOLDER) same as RefundLinePayment/Refund
                            were before their real dataset_ids arrived.

    All chains involving multiple candidate paths for the same header ID
    (e.g. Payment's line/header split, CreditMemo's three-way split) are
    concatenated with the more direct/specific path listed first, then
    deduplicated keeping the first match — matches this file's existing
    "line-level tried first" convention. For CreditMemo/RefundLinePayment/
    Refund, an Invoice or CreditMemo header with multiple matching lines
    takes the first match — confirmed acceptable by Dakota ("just pull
    everything in... it's better to have one row for everything"), not
    something needing further disambiguation.

    Returns tj merged with a single new "Product2Id" column — no more
    "bu"/"did"/"InvoiceLineName" output columns (an earlier version of
    this function produced those directly from InvoiceLine; that layer is
    gone now that Product2 is the sole source — see
    resolve_product2_fields()/apply_product2_bu_did()).
    """
    def empty_candidate():
        return pd.DataFrame(columns=["ReferenceTransactionRecordId", "Product2Id"])

    def il_product2():
        # No internal .empty guard needed -- every call site below already
        # ensures invoice_line is non-empty before calling this (confirmed
        # empirically: removing this guard and running the full test suite
        # + both e2e demos surfaced no failures). Kept as a thin accessor
        # rather than removed outright since it's still a useful shared
        # column-selection helper across many candidates below.
        return invoice_line[["Id", "InvoiceId", "Product2Id"]]

    def cml_product2():
        # Same reasoning as il_product2() above.
        return credit_memo_line[["Id", "CreditMemoId", "Product2Id"]]

    candidates = []

    # InvoiceLine direct
    if not invoice_line.empty:
        candidates.append(
            il_product2()[["Id", "Product2Id"]].rename(columns={"Id": "ReferenceTransactionRecordId"})
        )

    # InvoiceLineTax -> InvoiceLine
    if not invoice_line_tax.empty and not invoice_line.empty:
        joined = invoice_line_tax[["Id", "InvoiceLineId"]].merge(
            il_product2()[["Id", "Product2Id"]].rename(columns={"Id": "InvoiceLineId"}),
            on="InvoiceLineId", how="left",
        )
        candidates.append(
            joined.rename(columns={"Id": "ReferenceTransactionRecordId"})[["ReferenceTransactionRecordId", "Product2Id"]]
        )

    # DebitMemoLine direct
    if not debit_memo_line.empty and "Product2Id" in debit_memo_line.columns:
        candidates.append(
            debit_memo_line[["Id", "Product2Id"]].rename(columns={"Id": "ReferenceTransactionRecordId"})
        )

    def payment_chain(id_df: pd.DataFrame, id_col: str):
        """Shared by RefundLinePayment/Refund ONLY (NOT Payment itself —
        see the comment on Payment's own candidates below for why): given
        [id_col, PaymentId] pairs, resolves via Payment -> line/header ->
        InvoiceLine.Product2Id."""
        results = []
        if id_df.empty or payment.empty or invoice_line.empty:
            return results
        with_payment = id_df.merge(
            payment[["Id"]].rename(columns={"Id": "PaymentId"}), on="PaymentId", how="left",
        )
        if not payment_line_invoice_line.empty:
            joined = with_payment.merge(
                payment_line_invoice_line[["PaymentId", "InvoiceLineId"]], on="PaymentId", how="left",
            ).merge(
                il_product2()[["Id", "Product2Id"]].rename(columns={"Id": "InvoiceLineId"}),
                on="InvoiceLineId", how="left",
            )
            results.append(joined.rename(columns={id_col: "ReferenceTransactionRecordId"})[
                ["ReferenceTransactionRecordId", "Product2Id"]
            ])
        if not payment_line_invoice.empty:
            joined = with_payment.merge(
                payment_line_invoice[["PaymentId", "InvoiceId"]], on="PaymentId", how="left",
            ).merge(
                il_product2()[["InvoiceId", "Product2Id"]], on="InvoiceId", how="left",
            )
            results.append(joined.rename(columns={id_col: "ReferenceTransactionRecordId"})[
                ["ReferenceTransactionRecordId", "Product2Id"]
            ])
        return results

    # Payment: ReferenceTransactionRecordId = Payment.Id directly. Does
    # NOT require the payment table itself to have this Id — that table
    # is only used as a confirmation hop for RefundLinePayment/Refund
    # below (via payment_chain()), not for Payment's own direct
    # resolution: a Payment TransactionType row's
    # ReferenceTransactionRecordId literally already IS the PaymentId,
    # confirmed directly from real fixture data during testing (a
    # regression where this candidate was gated on the payment table
    # having a matching Id — wrong, since payment_line_invoice_line/
    # payment_line_invoice are the actual source of truth for which
    # PaymentIds exist here, same as before this rewrite).
    if not payment_line_invoice_line.empty:
        joined = payment_line_invoice_line[["PaymentId", "InvoiceLineId"]].merge(
            il_product2()[["Id", "Product2Id"]].rename(columns={"Id": "InvoiceLineId"}),
            on="InvoiceLineId", how="left",
        )
        candidates.append(
            joined.rename(columns={"PaymentId": "ReferenceTransactionRecordId"})[
                ["ReferenceTransactionRecordId", "Product2Id"]
            ]
        )
    if not payment_line_invoice.empty:
        joined = payment_line_invoice[["PaymentId", "InvoiceId"]].merge(
            il_product2()[["InvoiceId", "Product2Id"]], on="InvoiceId", how="left",
        )
        candidates.append(
            joined.rename(columns={"PaymentId": "ReferenceTransactionRecordId"})[
                ["ReferenceTransactionRecordId", "Product2Id"]
            ]
        )

    # CreditMemo: (a) direct via CreditMemoLine, (b) header fallback.
    # NOTE: there is deliberately no "line-level via CreditMemoLineInvoiceLine"
    # candidate here anymore — confirmed structurally DEAD, not just
    # untested: it derived from the same CreditMemoLine table, keyed by
    # the same CreditMemoId, as the direct candidate (a) above, which is
    # always listed first. Since the dedup below keeps the first candidate
    # regardless of whether its value is null, that junction-table path
    # could never win — not even in the one case where it would have
    # returned a better (non-null) answer than a null from (a). Confirmed
    # directly by constructing exactly that scenario and checking the
    # result. This was a real candidate before the Product2 rewrite (it
    # existed to reach InvoiceLine's own bu/did, which the pre-rewrite
    # "direct" candidate couldn't do) — the rewrite made it redundant
    # without this being caught until now. CreditMemoLineInvoiceLine
    # itself is NOT removed as a loaded table — it's still needed for its
    # own standalone TransactionType ("CreditMemoLineInvoiceLine", below).
    if not credit_memo_line.empty and "Product2Id" in credit_memo_line.columns:
        candidates.append(
            cml_product2()[["CreditMemoId", "Product2Id"]]
            .dropna(subset=["CreditMemoId"])
            .drop_duplicates(subset="CreditMemoId", keep="first")
            .rename(columns={"CreditMemoId": "ReferenceTransactionRecordId"})
        )
    if not credit_memo_inv_application.empty and not invoice_line.empty:
        joined = credit_memo_inv_application[["CreditMemoId", "InvoiceId"]].merge(
            il_product2()[["InvoiceId", "Product2Id"]], on="InvoiceId", how="left",
        )
        candidates.append(
            joined.rename(columns={"CreditMemoId": "ReferenceTransactionRecordId"})[
                ["ReferenceTransactionRecordId", "Product2Id"]
            ]
        )

    # RefundLinePayment: direct entry
    if not refund_line_payment.empty:
        rlp_id_df = refund_line_payment[["Id", "PaymentId"]].rename(columns={"Id": "RLPId"})
        candidates.extend(payment_chain(rlp_id_df, "RLPId"))

    # Refund: one hop earlier via RefundId
    if not refund.empty and not refund_line_payment.empty:
        refund_to_payment = refund[["Id"]].rename(columns={"Id": "RefundId"}).merge(
            refund_line_payment[["RefundId", "PaymentId"]], on="RefundId", how="left",
        )
        candidates.extend(payment_chain(refund_to_payment, "RefundId"))

    # Invoice: ReferenceTransactionRecordId = Invoice.Id directly, no
    # separate Invoice table load needed
    if not invoice_line.empty:
        candidates.append(
            il_product2()[["InvoiceId", "Product2Id"]]
            .dropna(subset=["InvoiceId"])
            .drop_duplicates(subset="InvoiceId", keep="first")
            .rename(columns={"InvoiceId": "ReferenceTransactionRecordId"})
        )

    # CreditMemoLine: direct entry
    if not credit_memo_line.empty and "Product2Id" in credit_memo_line.columns:
        candidates.append(
            cml_product2()[["Id", "Product2Id"]].rename(columns={"Id": "ReferenceTransactionRecordId"})
        )

    # CreditMemoLineTax -> CreditMemoLine
    if not credit_memo_line_tax.empty and not credit_memo_line.empty:
        joined = credit_memo_line_tax[["Id", "CreditMemoLineId"]].merge(
            cml_product2()[["Id", "Product2Id"]].rename(columns={"Id": "CreditMemoLineId"}),
            on="CreditMemoLineId", how="left",
        )
        candidates.append(
            joined.rename(columns={"Id": "ReferenceTransactionRecordId"})[["ReferenceTransactionRecordId", "Product2Id"]]
        )

    # PaymentLineInvoice: direct entry
    if not payment_line_invoice.empty and "Id" in payment_line_invoice.columns and not invoice_line.empty:
        joined = payment_line_invoice[["Id", "InvoiceId"]].merge(
            il_product2()[["InvoiceId", "Product2Id"]], on="InvoiceId", how="left",
        )
        candidates.append(
            joined.rename(columns={"Id": "ReferenceTransactionRecordId"})[["ReferenceTransactionRecordId", "Product2Id"]]
        )

    # PaymentLineInvoiceLine: direct entry
    if not payment_line_invoice_line.empty and "Id" in payment_line_invoice_line.columns and not invoice_line.empty:
        joined = payment_line_invoice_line[["Id", "InvoiceLineId"]].merge(
            il_product2()[["Id", "Product2Id"]].rename(columns={"Id": "InvoiceLineId"}), on="InvoiceLineId", how="left",
        )
        candidates.append(
            joined.rename(columns={"Id": "ReferenceTransactionRecordId"})[["ReferenceTransactionRecordId", "Product2Id"]]
        )

    # CreditMemoInvApplication: direct entry
    if not credit_memo_inv_application.empty and "Id" in credit_memo_inv_application.columns and not credit_memo_line.empty:
        joined = credit_memo_inv_application[["Id", "CreditMemoId"]].merge(
            cml_product2()[["CreditMemoId", "Product2Id"]]
            .dropna(subset=["CreditMemoId"]).drop_duplicates(subset="CreditMemoId", keep="first"),
            on="CreditMemoId", how="left",
        )
        candidates.append(
            joined.rename(columns={"Id": "ReferenceTransactionRecordId"})[["ReferenceTransactionRecordId", "Product2Id"]]
        )

    # CreditMemoLineInvoiceLine: direct entry
    if not credit_memo_line_invoice_line.empty and "Id" in credit_memo_line_invoice_line.columns and not credit_memo_line.empty:
        joined = credit_memo_line_invoice_line[["Id", "CreditMemoLineId"]].merge(
            cml_product2()[["Id", "Product2Id"]].rename(columns={"Id": "CreditMemoLineId"}),
            on="CreditMemoLineId", how="left",
        )
        candidates.append(
            joined.rename(columns={"Id": "ReferenceTransactionRecordId"})[["ReferenceTransactionRecordId", "Product2Id"]]
        )

    # DebitMemoLineTax -> DebitMemoLine (no table in Salesforce yet, per
    # Dakota — structurally complete, always empty in practice today)
    if not debit_memo_line_tax.empty and not debit_memo_line.empty:
        joined = debit_memo_line_tax[["Id", "DebitMemoLineId"]].merge(
            debit_memo_line[["Id", "Product2Id"]].rename(columns={"Id": "DebitMemoLineId"}),
            on="DebitMemoLineId", how="left",
        )
        candidates.append(
            joined.rename(columns={"Id": "ReferenceTransactionRecordId"})[["ReferenceTransactionRecordId", "Product2Id"]]
        )

    if not candidates:
        lookup = empty_candidate()
    else:
        lookup = pd.concat(candidates, ignore_index=True).drop_duplicates(
            subset="ReferenceTransactionRecordId", keep="first"
        )

    return tj.merge(lookup, on="ReferenceTransactionRecordId", how="left")


def validate_required_tables_present(transaction_journal: pd.DataFrame, loaded_tables: dict) -> None:
    """
    Raises ValueError if:
      - transaction_journal, product2, or general_ledger_account (via
        loaded_tables) is empty — unconditionally mandatory, per Dakota:
        "if [any of these] are empty it should fail. That would mean a
        data issue is present."
      - any TransactionType value ACTUALLY PRESENT in transaction_journal
        has EVERY one of its possible resolution paths dead (every table
        in every OR-path in REQUIRED_TABLES_BY_TRANSACTION_TYPE is empty)
        — confirmed directly with Dakota: a single empty table in one of
        several fallback paths must NOT fail the whole run by itself
        (e.g. PaymentLineInvoiceLine has been confirmed 0 rows this whole
        project — known, expected state, not a data issue, since
        Payment's header-level path resolves fine on its own). Only fails
        when NONE of a present type's paths can resolve anything at all.

    loaded_tables: dict of table_key -> already-loaded DataFrame, same
    keys as DATASET_IDS/EXPECTED_COLUMNS (e.g. {"invoice_line": df, ...}).

    A TransactionType present in the data but missing from
    REQUIRED_TABLES_BY_TRANSACTION_TYPE entirely is NOT this function's
    concern — that would be a genuinely new, unhandled TransactionType
    value never seen before, a different problem from "the tables for a
    KNOWN type are all empty."
    """
    missing_mandatory = [t for t in MANDATORY_TABLES if loaded_tables.get(t, pd.DataFrame()).empty]
    if missing_mandatory:
        raise ValueError(
            f"Required table(s) empty: {', '.join(missing_mandatory)}. "
            "These are always mandatory regardless of TransactionType — "
            "an empty result here indicates a data issue, per Dakota."
        )

    # transaction_journal.empty is NOT checked here — MANDATORY_TABLES
    # above already guarantees that when build_source_dataframe() is the
    # caller (transaction_journal and loaded_tables["transaction_journal"]
    # are always the same object there), an empty transaction_journal
    # already raised before this line is ever reached. Only the
    # missing-column case is a genuinely distinct, reachable scenario
    # worth guarding here.
    if "TransactionType" not in transaction_journal.columns:
        return

    present_types = set(transaction_journal["TransactionType"].dropna().unique())
    dead_types = []
    for txn_type in present_types:
        paths = REQUIRED_TABLES_BY_TRANSACTION_TYPE.get(txn_type)
        if paths is None:
            continue
        any_path_alive = any(
            all(not loaded_tables.get(t, pd.DataFrame()).empty for t in path)
            for path in paths
        )
        if not any_path_alive:
            dead_types.append(txn_type)

    if dead_types:
        raise ValueError(
            "TransactionType value(s) present in the data with NO viable "
            "resolution path (every table in every fallback is empty): "
            f"{', '.join(sorted(dead_types))}. This indicates a data issue, "
            "per Dakota."
        )


def resolve_product2_fields(tj: pd.DataFrame, product2: pd.DataFrame) -> pd.DataFrame:
    """
    Resolves Product2.Business_Unit_BU__c / .Department_ID_DID__c / .Name
    via tj's own "Product2Id" column (from resolve_product2_id() above).
    Confirmed real fields: Product2.Business_Unit_BU__c /
    .Department_ID_DID__c — same field names as InvoiceLine's had.

    .Name is new here (an earlier version of this function only resolved
    bu/did) — needed for the slingshot/databolt did override, which now
    checks Product2.Name instead of InvoiceLine.Name entirely, per Dakota
    ("we shouldn't need invoice line.name anymore for the name check").

    Adds three new columns: "product2_bu"/"product2_did"/"product2_name".
    product2.empty -> all three come back all-null (matters while a
    dataset_id could still be a placeholder, or the table is genuinely
    empty for another reason — validate_required_tables_present() is what
    actually guards against that reaching this function silently).
    """
    result = tj.copy()
    if tj.empty or product2.empty or "Product2Id" not in tj.columns:
        for col in ("product2_bu", "product2_did", "product2_name"):
            result[col] = pd.Series([None] * len(tj), index=tj.index, dtype="object")
        return result

    p2 = product2.drop_duplicates(subset="Id", keep="first").set_index("Id")
    result["product2_bu"] = (
        result["Product2Id"].map(p2["Business_Unit_BU__c"]) if "Business_Unit_BU__c" in p2.columns else None
    )
    result["product2_did"] = (
        result["Product2Id"].map(p2["Department_ID_DID__c"]) if "Department_ID_DID__c" in p2.columns else None
    )
    result["product2_name"] = (
        result["Product2Id"].map(p2["Name"]) if "Name" in p2.columns else None
    )
    return result


def apply_product2_bu_did(tj: pd.DataFrame) -> pd.DataFrame:
    """
    Assigns "bu"/"did" directly from "product2_bu"/"product2_did" —
    Product2 is the SOLE bu/did source now, per Dakota: "Product2 should
    not have a null or blank did/bu... it's okay to remove the invoice
    line resolution." An earlier version of this function
    (apply_product2_priority()) combine_first'd against an InvoiceLine
    fallback; that fallback is gone, so this is now a direct assignment,
    not a priority merge — renamed to reflect that.

    Requires "product2_bu"/"product2_did" (from resolve_product2_fields())
    to already be present as columns on tj.

    Must run BEFORE apply_did_overrides() — the "10901" default and
    slingshot/databolt/10040049 overrides apply to whatever Product2
    resolved here, not the other way around. The "10901" default is
    expected to rarely fire in practice (per Dakota, Product2 shouldn't
    have null/blank bu/did) but is kept as a defensive fallback for
    whatever Product2Id itself didn't resolve at all — not removed.
    """
    tj = tj.copy()
    tj["bu"] = tj["product2_bu"]
    tj["did"] = tj["product2_did"]
    return tj


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

      1. bu default: any row where Product2 resolution left "bu" null
         falls back to "10901" — per Dakota, "For the most part, all BUs
         should be 10901 as the default value." Expected to rarely fire
         in practice now (Product2 shouldn't have null/blank bu), kept as
         a defensive fallback, not removed.
      2. did override by product name: "did" -> "16637" if Product2.Name
         contains "slingshot" (case-insensitive), or "16635" if it
         contains "databolt". Checks Product2.Name now, not
         InvoiceLine.Name — per Dakota, "we shouldn't need invoice line
         name anymore for the name check."
      3. did override by GL account (HIGHEST priority — applied last, so it
         wins over #2 above): "did" -> "16605" whenever
         gl_accounting_number_c is exactly "10040049" — a special caveat
         account per Dakota ("The account number 10040049 should supersede
         whatever the DID is").

    Requires "bu", "did" (from apply_product2_bu_did()), "product2_name"
    (from resolve_product2_fields()), and "gl_accounting_number_c" (from
    resolve_gl_accounting_number()) to already be present as columns on df.

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

    name_lower = df["product2_name"].fillna("").str.lower()
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
    debit_memo_line = load("debit_memo_line")
    # payment/refund (the header objects themselves) are loaded again here —
    # NOT for the old Account lookup (gone, see module docstring), but as
    # explicit join hops in the Refund/RefundLinePayment TransactionType
    # resolution below, per Dakota ("keep payment just to make sure there's
    # nothing lost in the joins"). invoice/credit_memo/account itself remain
    # unloaded — nothing currently needs them.
    payment = load("payment")
    refund = load("refund")
    refund_line_payment = load("refund_line_payment")
    # Product2 is now the SOLE bu/did source (see resolve_product2_fields()/
    # apply_product2_bu_did()) — InvoiceLine's own bu/did fields are no
    # longer read for this purpose at all. Per Dakota.
    product2 = load("product2")
    general_ledger_account = load("general_ledger_account")
    credit_memo_line_tax = load("credit_memo_line_tax")
    debit_memo_line_tax = load("debit_memo_line_tax")

    if log:
        log.info("validating required tables are present...")
    validate_required_tables_present(tj, {
        "transaction_journal": tj, "product2": product2,
        "general_ledger_account": general_ledger_account,
        "invoice_line": invoice_line, "invoice_line_tax": invoice_line_tax,
        "payment_line_invoice_line": payment_line_invoice_line,
        "payment_line_invoice": payment_line_invoice,
        "credit_memo_line": credit_memo_line,
        "credit_memo_line_invoice_line": credit_memo_line_invoice_line,
        "credit_memo_inv_application": credit_memo_inv_application,
        "debit_memo_line": debit_memo_line, "payment": payment,
        "refund": refund, "refund_line_payment": refund_line_payment,
        "credit_memo_line_tax": credit_memo_line_tax,
        "debit_memo_line_tax": debit_memo_line_tax,
    })
    if log:
        log.info("validating required tables are present...complete")

    if log:
        log.info("resolving product2id...")
    tj = resolve_product2_id(tj, invoice_line, invoice_line_tax, payment_line_invoice_line,
                              payment_line_invoice, credit_memo_line, credit_memo_line_invoice_line,
                              credit_memo_inv_application, debit_memo_line, payment, refund,
                              refund_line_payment, credit_memo_line_tax, debit_memo_line_tax)
    if log:
        null_p2_count = int(tj["Product2Id"].isna().sum())
        log.info(f"resolving product2id...complete ({null_p2_count} rows with null Product2Id)")

    if log:
        log.info("resolving product2 bu/did/name...")
    tj = resolve_product2_fields(tj, product2)
    tj = apply_product2_bu_did(tj)
    if log:
        null_bu_count = int(tj["bu"].isna().sum())
        log.info(f"resolving product2 bu/did/name...complete ({null_bu_count} rows with null bu)")

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
    #   InvoiceLine.Business_Unit / .Department_Id: DESPITE THE COLUMN
    #     NAME, this is now sourced ENTIRELY from Product2 —
    #     Product2.Business_Unit_BU__c/.Department_ID_DID__c — not
    #     InvoiceLine at all (see resolve_product2_fields()/
    #     apply_product2_bu_did()). The "InvoiceLine." prefix is a
    #     historical holdover from before Product2 became the sole source
    #     and is kept here as-is (rather than cascading a rename through
    #     salesforce_global_one.py's field-reading code and its full test
    #     suite, a separate, larger change) — the file spec's own field
    #     label is just "Business Unit"/"Department ID", not a literal
    #     claim about which table supplied the value. Then bu defaulted /
    #     did overridden per apply_did_overrides() above.
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

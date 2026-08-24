"""
Tests for gl_source_join.py — the source-side join logic that builds the
flat dataframe consumed by the GL journal file builder.

Run with: pytest test_gl_source_join.py -v

Covers:
  - resolve_amount(): Credit/Debit sign resolution
  - resolve_gl_accounting_number(): TransactionJournal's Debit/Credit
    GeneralLedgerAccountId -> GeneralLedgerAccount.GL_Accounting_Number__c,
    the GL pipeline's replacement for the old Account.AccountNumber lookup
    (build_reference_to_account_lookup() + clean_account_number(), removed
    — confirmed fully unused anywhere once the GL pipeline stopped calling
    them, not just unused by it — salesforce_ofac.py and
    salesforce_ofac_billing_preview.py have their own separate, unrelated
    Account/BillingAccount join and never called either function)
  - apply_did_overrides(): bu default, slingshot/databolt did overrides
    (via Product2.Name), and the 10040049 -> 16605 override (highest priority)
  - resolve_date_window(): month-to-date default vs. explicit start/end
  - resolve_product2_id(): per-TransactionType Product2Id resolution, all
    15 confirmed TransactionType values, including the line-level vs.
    header-level fallback priority
  - resolve_product2_fields() / apply_product2_bu_did(): Product2.Id ->
    bu/did/Name, then a direct assignment (no InvoiceLine fallback)
  - validate_required_tables_present(): mandatory tables + per-type
    OR-path liveness checks
  - DATASET_IDS / EXPECTED_COLUMNS sanity checks
  - build_source_dataframe(): full orchestration, mocked S3 reads
"""

from datetime import datetime, timezone
import re
from unittest.mock import MagicMock

import pandas as pd
import pytest

import src.salesforce.resources.scripts.helpers.gl_source_join as glsj
from src.salesforce.resources.scripts.helpers.gl_source_join import (
    resolve_product2_id,
    resolve_amount,
    resolve_gl_accounting_number,
    resolve_product2_fields,
    apply_product2_bu_did,
    apply_did_overrides,
    resolve_date_window,
    validate_required_tables_present,
    build_source_dataframe,
    DATASET_IDS,
    EXPECTED_COLUMNS,
    REQUIRED_TABLES_BY_TRANSACTION_TYPE,
    MANDATORY_TABLES,
)


# ---------------------------------------------------------------------------
# resolve_amount()
# ---------------------------------------------------------------------------

class TestResolveAmount:
    def test_credit_becomes_negative(self):
        tj = pd.DataFrame([{"Credit": 100.0, "Debit": None}])
        result = resolve_amount(tj)
        assert result.iloc[0] == -100.0

    def test_debit_stays_positive(self):
        tj = pd.DataFrame([{"Credit": None, "Debit": 250.0}])
        result = resolve_amount(tj)
        assert result.iloc[0] == 250.0

    def test_credit_takes_priority_when_both_populated(self):
        # per spec: "if one is null chose the other" — credit checked first
        tj = pd.DataFrame([{"Credit": 50.0, "Debit": 75.0}])
        result = resolve_amount(tj)
        assert result.iloc[0] == -50.0

    def test_both_null_returns_null(self):
        tj = pd.DataFrame([{"Credit": None, "Debit": None}])
        result = resolve_amount(tj)
        assert pd.isna(result.iloc[0])

    def test_zero_credit_treated_as_populated_not_null(self):
        tj = pd.DataFrame([{"Credit": 0.0, "Debit": 5.0}])
        result = resolve_amount(tj)
        assert result.iloc[0] == 0.0  # -0.0 == 0.0

    def test_multiple_rows_resolved_independently(self):
        tj = pd.DataFrame([
            {"Credit": 10.0, "Debit": None},
            {"Credit": None, "Debit": 20.0},
            {"Credit": None, "Debit": None},
        ])
        result = resolve_amount(tj)
        assert list(result) == [-10.0, 20.0] or (result.iloc[0] == -10.0 and result.iloc[1] == 20.0
                                                   and pd.isna(result.iloc[2]))

    def test_missing_credit_debit_columns_entirely(self):
        # tj.get("Credit") on a dataframe with neither column returns None,
        # which pd.to_numeric should handle without raising
        tj = pd.DataFrame([{"other_col": 1}])
        result = resolve_amount(tj)
        assert pd.isna(result.iloc[0])



# ---------------------------------------------------------------------------
# resolve_gl_accounting_number()
# ---------------------------------------------------------------------------

class TestResolveGlAccountingNumber:
    def _gla(self):
        return pd.DataFrame([
            {"Id": "GLA1", "GL_Accounting_Number__c": "10012345"},
            {"Id": "GLA2", "GL_Accounting_Number__c": "20099999"},
        ])

    def test_resolves_via_debit_id(self):
        tj = pd.DataFrame([{
            "DebitGeneralLedgerAccountId": "GLA1", "CreditGeneralLedgerAccountId": None,
        }])
        result = resolve_gl_accounting_number(tj, self._gla())
        assert result.iloc[0] == "10012345"

    def test_resolves_via_credit_id_when_debit_null(self):
        tj = pd.DataFrame([{
            "DebitGeneralLedgerAccountId": None, "CreditGeneralLedgerAccountId": "GLA2",
        }])
        result = resolve_gl_accounting_number(tj, self._gla())
        assert result.iloc[0] == "20099999"

    def test_debit_takes_priority_when_both_populated(self):
        # a journal entry is either a debit or a credit per Dakota, but if
        # both somehow came through populated, debit wins (combine_first
        # order) — documenting this rather than leaving it implicit.
        tj = pd.DataFrame([{
            "DebitGeneralLedgerAccountId": "GLA1", "CreditGeneralLedgerAccountId": "GLA2",
        }])
        result = resolve_gl_accounting_number(tj, self._gla())
        assert result.iloc[0] == "10012345"

    def test_both_null_returns_null(self):
        tj = pd.DataFrame([{
            "DebitGeneralLedgerAccountId": None, "CreditGeneralLedgerAccountId": None,
        }])
        result = resolve_gl_accounting_number(tj, self._gla())
        assert pd.isna(result.iloc[0])

    def test_no_matching_gl_account_returns_null(self):
        tj = pd.DataFrame([{
            "DebitGeneralLedgerAccountId": "GLA_MISSING", "CreditGeneralLedgerAccountId": None,
        }])
        result = resolve_gl_accounting_number(tj, self._gla())
        assert pd.isna(result.iloc[0])

    def test_empty_general_ledger_account_returns_all_null(self):
        tj = pd.DataFrame([{
            "DebitGeneralLedgerAccountId": "GLA1", "CreditGeneralLedgerAccountId": None,
        }])
        empty_gla = pd.DataFrame(columns=["Id", "GL_Accounting_Number__c"])
        result = resolve_gl_accounting_number(tj, empty_gla)
        assert pd.isna(result.iloc[0])

    def test_empty_tj_returns_empty_series(self):
        tj = pd.DataFrame(columns=["DebitGeneralLedgerAccountId", "CreditGeneralLedgerAccountId"])
        result = resolve_gl_accounting_number(tj, self._gla())
        assert len(result) == 0

    def test_missing_debit_credit_columns_entirely_does_not_raise(self):
        # same tolerance pattern as resolve_amount()'s tj.get() fallback —
        # an older/unrelated tj shape shouldn't blow up here
        tj = pd.DataFrame([{"other_col": 1}])
        result = resolve_gl_accounting_number(tj, self._gla())
        assert pd.isna(result.iloc[0])

    def test_multiple_rows_resolved_independently(self):
        tj = pd.DataFrame([
            {"DebitGeneralLedgerAccountId": "GLA1", "CreditGeneralLedgerAccountId": None},
            {"DebitGeneralLedgerAccountId": None, "CreditGeneralLedgerAccountId": "GLA2"},
            {"DebitGeneralLedgerAccountId": None, "CreditGeneralLedgerAccountId": None},
        ])
        result = resolve_gl_accounting_number(tj, self._gla())
        assert result.iloc[0] == "10012345"
        assert result.iloc[1] == "20099999"
        assert pd.isna(result.iloc[2])

    def test_duplicate_gl_account_ids_keeps_first(self):
        gla = pd.DataFrame([
            {"Id": "GLA1", "GL_Accounting_Number__c": "FIRST"},
            {"Id": "GLA1", "GL_Accounting_Number__c": "SECOND"},
        ])
        tj = pd.DataFrame([{
            "DebitGeneralLedgerAccountId": "GLA1", "CreditGeneralLedgerAccountId": None,
        }])
        result = resolve_gl_accounting_number(tj, gla)
        assert result.iloc[0] == "FIRST"


# ---------------------------------------------------------------------------
# apply_did_overrides()
# ---------------------------------------------------------------------------

class TestApplyDidOverrides:
    def _base_df(self, **overrides):
        row = {
            "bu": "US001", "did": "10500", "product2_name": "Widget Plan",
            "gl_accounting_number_c": "10099999",
        }
        row.update(overrides)
        return pd.DataFrame([row])

    def test_null_bu_defaults_to_10901(self):
        df = self._base_df(bu=None)
        result = apply_did_overrides(df)
        assert result.iloc[0]["bu"] == "10901"

    def test_non_null_bu_left_unchanged(self):
        df = self._base_df(bu="US002")
        result = apply_did_overrides(df)
        assert result.iloc[0]["bu"] == "US002"

    def test_blank_string_bu_defaults_to_10901(self):
        # Salesforce can return an optional text field as "" rather than a
        # true null — confirmed with Dakota this should get the same
        # treatment as a real null, not pass through as a literal "".
        df = self._base_df(bu="")
        result = apply_did_overrides(df)
        assert result.iloc[0]["bu"] == "10901"

    def test_whitespace_only_bu_defaults_to_10901(self):
        df = self._base_df(bu="   ")
        result = apply_did_overrides(df)
        assert result.iloc[0]["bu"] == "10901"

    def test_blank_string_did_normalized_to_null_not_left_as_empty_string(self):
        df = self._base_df(did="")
        result = apply_did_overrides(df)
        assert pd.isna(result.iloc[0]["did"])

    def test_blank_bu_in_an_entirely_unresolved_row_does_not_raise(self):
        # REGRESSION: on a row where InvoiceLine never resolved at all (bu
        # AND did both null/blank across the whole dataframe), the
        # blank-to-null normalization must not corrupt the column's dtype
        # in a way that breaks the later slingshot/databolt/10040049
        # string assignments — confirmed this happens if blank-to-null is
        # implemented via a whole-column .apply()/.map() (which silently
        # re-infers an all-null object column as float64), rather than a
        # targeted .loc assignment on only the blank cells.
        df = pd.DataFrame([{
            "bu": None, "did": None, "product2_name": "Slingshot Plan",
            "gl_accounting_number_c": None,
        }])
        result = apply_did_overrides(df)  # must not raise
        assert result.iloc[0]["bu"] == "10901"
        assert result.iloc[0]["did"] == "16637"

    def test_slingshot_in_name_overrides_did(self):
        df = self._base_df(product2_name="Slingshot Enterprise Plan")
        result = apply_did_overrides(df)
        assert result.iloc[0]["did"] == "16637"

    def test_slingshot_match_is_case_insensitive(self):
        df = self._base_df(product2_name="SLINGSHOT basic")
        result = apply_did_overrides(df)
        assert result.iloc[0]["did"] == "16637"

    def test_databolt_in_name_overrides_did(self):
        df = self._base_df(product2_name="DataBolt Pro")
        result = apply_did_overrides(df)
        assert result.iloc[0]["did"] == "16635"

    def test_databolt_match_is_case_insensitive(self):
        df = self._base_df(product2_name="databolt starter")
        result = apply_did_overrides(df)
        assert result.iloc[0]["did"] == "16635"

    def test_name_without_either_keyword_leaves_did_unchanged(self):
        df = self._base_df(product2_name="Something Else Entirely")
        result = apply_did_overrides(df)
        assert result.iloc[0]["did"] == "10500"

    def test_null_name_does_not_raise_and_leaves_did_unchanged(self):
        df = self._base_df(product2_name=None)
        result = apply_did_overrides(df)
        assert result.iloc[0]["did"] == "10500"

    def test_gl_account_10040049_overrides_did_to_16605(self):
        df = self._base_df(gl_accounting_number_c="10040049")
        result = apply_did_overrides(df)
        assert result.iloc[0]["did"] == "16605"

    def test_gl_account_10040049_beats_slingshot_override(self):
        # highest priority per Dakota: "should supersede whatever the DID is"
        df = self._base_df(gl_accounting_number_c="10040049", product2_name="Slingshot Plan")
        result = apply_did_overrides(df)
        assert result.iloc[0]["did"] == "16605"

    def test_gl_account_10040049_beats_databolt_override(self):
        df = self._base_df(gl_accounting_number_c="10040049", product2_name="DataBolt Plan")
        result = apply_did_overrides(df)
        assert result.iloc[0]["did"] == "16605"

    def test_different_gl_account_number_does_not_trigger_override(self):
        df = self._base_df(gl_accounting_number_c="99999999")
        result = apply_did_overrides(df)
        assert result.iloc[0]["did"] == "10500"

    def test_null_gl_account_number_does_not_trigger_override(self):
        df = self._base_df(gl_accounting_number_c=None)
        result = apply_did_overrides(df)
        assert result.iloc[0]["did"] == "10500"

    def test_caveat_int_typed_gl_account_number_does_not_trigger_override(self):
        # DOCUMENTED CAVEAT, not a bug in apply_did_overrides() itself:
        # this compares gl_accounting_number_c == "10040049" as a STRING.
        # If the column were ever int64-typed (e.g. from a JSON reader
        # that silently coerces numeric-looking strings — see
        # test_s3_utils.py's read_jsonl_from_s3 regression tests, which is
        # exactly how this was actually caught, via an end-to-end run
        # against realistic fixture data), the override would silently
        # never fire. read_jsonl_from_s3() uses dtype=False specifically
        # to prevent that upstream. This test exists so that if that
        # protection is ever removed, there's still a test making the
        # silent-failure mode visible at this layer too, not just upstream.
        df = self._base_df(gl_accounting_number_c=10040049)  # int, not str
        result = apply_did_overrides(df)
        assert result.iloc[0]["did"] == "10500"  # override does NOT fire

    def test_does_not_mutate_caller_dataframe(self):
        df = self._base_df(bu=None)
        original = df.copy()
        apply_did_overrides(df)
        assert pd.isna(df.loc[0, "bu"]) == pd.isna(original.loc[0, "bu"])

    def test_multiple_rows_overridden_independently(self):
        df = pd.DataFrame([
            {"bu": None, "did": "1", "product2_name": "Slingshot", "gl_accounting_number_c": "111"},
            {"bu": "US001", "did": "2", "product2_name": "DataBolt", "gl_accounting_number_c": "222"},
            {"bu": "US002", "did": "3", "product2_name": "Plain Plan", "gl_accounting_number_c": "10040049"},
        ])
        result = apply_did_overrides(df)
        assert result.iloc[0]["bu"] == "10901"
        assert result.iloc[0]["did"] == "16637"
        assert result.iloc[1]["did"] == "16635"
        assert result.iloc[2]["did"] == "16605"


# ---------------------------------------------------------------------------
# resolve_date_window()
# ---------------------------------------------------------------------------

class TestResolveDateWindow:
    def test_default_start_is_first_of_current_month_at_midnight_utc(self):
        start, end = resolve_date_window()
        now = datetime.now(timezone.utc)
        assert start.year == now.year
        assert start.month == now.month
        assert start.day == 1
        assert start.hour == 0 and start.minute == 0 and start.second == 0

    def test_default_end_is_approximately_now(self):
        before = datetime.now(timezone.utc)
        _, end = resolve_date_window()
        after = datetime.now(timezone.utc)
        assert before <= end <= after

    def test_default_window_is_timezone_aware_utc(self):
        start, end = resolve_date_window()
        assert start.tzinfo is not None
        assert end.tzinfo is not None

    def test_explicit_start_and_end_returned_as_datetimes(self):
        start, end = resolve_date_window("2026-01-01", "2026-01-31")
        assert start == datetime(2026, 1, 1, tzinfo=timezone.utc)
        assert end == datetime(2026, 1, 31, tzinfo=timezone.utc)

    def test_explicit_window_is_timezone_aware_utc(self):
        start, end = resolve_date_window("2026-01-01", "2026-01-31")
        assert start.tzinfo == timezone.utc
        assert end.tzinfo == timezone.utc

    def test_only_start_date_given_raises(self):
        with pytest.raises(ValueError):
            resolve_date_window(start_date="2026-01-01")

    def test_only_end_date_given_raises(self):
        with pytest.raises(ValueError):
            resolve_date_window(end_date="2026-01-31")


# ---------------------------------------------------------------------------
# resolve_product2_id()
# ---------------------------------------------------------------------------

class TestResolveProduct2Id:
    def _empties(self):
        return dict(
            ilt=pd.DataFrame([], columns=["Id", "InvoiceLineId"]),
            pli_line=pd.DataFrame([], columns=["Id", "PaymentId", "InvoiceLineId"]),
            pli_header=pd.DataFrame([], columns=["Id", "PaymentId", "InvoiceId"]),
            cml=pd.DataFrame([], columns=["Id", "CreditMemoId", "Product2Id"]),
            cmli=pd.DataFrame([], columns=["Id", "CreditMemoLineId", "InvoiceLineId"]),
            cmia=pd.DataFrame([], columns=["Id", "CreditMemoId", "InvoiceId"]),
            dml=pd.DataFrame([], columns=["Id", "ReferenceRecordId", "Product2Id"]),
            payment=pd.DataFrame([], columns=["Id"]),
            refund=pd.DataFrame([], columns=["Id"]),
            rlp=pd.DataFrame([], columns=["Id", "RefundId", "PaymentId"]),
            cmlt=pd.DataFrame([], columns=["Id", "CreditMemoLineId"]),
            dmlt=pd.DataFrame([], columns=["Id", "DebitMemoLineId"]),
        )

    def _call(self, tj, invoice_line, e, **overrides):
        args = dict(
            invoice_line_tax=e["ilt"], payment_line_invoice_line=e["pli_line"],
            payment_line_invoice=e["pli_header"], credit_memo_line=e["cml"],
            credit_memo_line_invoice_line=e["cmli"], credit_memo_inv_application=e["cmia"],
            debit_memo_line=e["dml"], payment=e["payment"], refund=e["refund"],
            refund_line_payment=e["rlp"], credit_memo_line_tax=e["cmlt"],
            debit_memo_line_tax=e["dmlt"],
        )
        args.update(overrides)
        return resolve_product2_id(tj, invoice_line, **args)

    def test_invoice_line_direct(self):
        tj = pd.DataFrame([{"ReferenceTransactionRecordId": "IL1", "TransactionType": "InvoiceLine"}])
        invoice_line = pd.DataFrame([{"Id": "IL1", "InvoiceId": "INV1", "Product2Id": "PROD1"}])
        result = self._call(tj, invoice_line, self._empties())
        assert result.iloc[0]["Product2Id"] == "PROD1"

    def test_invoice_line_tax_one_hop(self):
        tj = pd.DataFrame([{"ReferenceTransactionRecordId": "ILT1", "TransactionType": "InvoiceLineTax"}])
        invoice_line = pd.DataFrame([{"Id": "IL1", "InvoiceId": "INV1", "Product2Id": "PROD1"}])
        e = self._empties()
        e["ilt"] = pd.DataFrame([{"Id": "ILT1", "InvoiceLineId": "IL1"}])
        result = self._call(tj, invoice_line, e)
        assert result.iloc[0]["Product2Id"] == "PROD1"

    def test_debit_memo_line_direct(self):
        tj = pd.DataFrame([{"ReferenceTransactionRecordId": "DML1", "TransactionType": "DebitMemoLine"}])
        invoice_line = pd.DataFrame([], columns=["Id", "InvoiceId", "Product2Id"])
        e = self._empties()
        e["dml"] = pd.DataFrame([{"Id": "DML1", "ReferenceRecordId": "IL1", "Product2Id": "PROD1"}])
        result = self._call(tj, invoice_line, e)
        assert result.iloc[0]["Product2Id"] == "PROD1"

    def test_payment_header_level(self):
        tj = pd.DataFrame([{"ReferenceTransactionRecordId": "PAY1", "TransactionType": "Payment"}])
        invoice_line = pd.DataFrame([{"Id": "IL1", "InvoiceId": "INV1", "Product2Id": "PROD1"}])
        e = self._empties()
        e["payment"] = pd.DataFrame([{"Id": "PAY1"}])
        e["pli_header"] = pd.DataFrame([{"Id": "PLI1", "PaymentId": "PAY1", "InvoiceId": "INV1"}])
        result = self._call(tj, invoice_line, e)
        assert result.iloc[0]["Product2Id"] == "PROD1"

    def test_payment_resolves_without_a_matching_row_in_payment_table(self):
        # REGRESSION: caught via a real end-to-end run against fixture
        # data, not a unit test — an earlier version of this code gated
        # Payment's OWN resolution on the PaymentId also existing in the
        # separately-loaded `payment` table (a leftover of copying the
        # RefundLinePayment/Refund "confirm via Payment" pattern where it
        # didn't belong). For TransactionType="Payment" specifically,
        # ReferenceTransactionRecordId already IS the PaymentId directly —
        # payment_line_invoice_line/payment_line_invoice are the actual
        # source of truth for which PaymentIds exist, same as before this
        # whole Product2 rewrite. `payment` here is deliberately EMPTY.
        tj = pd.DataFrame([{"ReferenceTransactionRecordId": "PAY1", "TransactionType": "Payment"}])
        invoice_line = pd.DataFrame([{"Id": "IL1", "InvoiceId": "INV1", "Product2Id": "PROD1"}])
        e = self._empties()
        e["pli_header"] = pd.DataFrame([{"Id": "PLI1", "PaymentId": "PAY1", "InvoiceId": "INV1"}])
        result = self._call(tj, invoice_line, e)
        assert result.iloc[0]["Product2Id"] == "PROD1"

    def test_payment_line_level_wins_over_header(self):
        tj = pd.DataFrame([{"ReferenceTransactionRecordId": "PAY1", "TransactionType": "Payment"}])
        invoice_line = pd.DataFrame([
            {"Id": "IL_LINE", "InvoiceId": "INV_LINE", "Product2Id": "PROD_LINE"},
            {"Id": "IL_HEADER", "InvoiceId": "INV_HEADER", "Product2Id": "PROD_HEADER"},
        ])
        e = self._empties()
        e["payment"] = pd.DataFrame([{"Id": "PAY1"}])
        e["pli_line"] = pd.DataFrame([{"Id": "PLIL1", "PaymentId": "PAY1", "InvoiceLineId": "IL_LINE"}])
        e["pli_header"] = pd.DataFrame([{"Id": "PLI1", "PaymentId": "PAY1", "InvoiceId": "INV_HEADER"}])
        result = self._call(tj, invoice_line, e)
        assert result.iloc[0]["Product2Id"] == "PROD_LINE"

    def test_credit_memo_direct_via_credit_memo_line(self):
        tj = pd.DataFrame([{"ReferenceTransactionRecordId": "CM1", "TransactionType": "CreditMemo"}])
        invoice_line = pd.DataFrame([], columns=["Id", "InvoiceId", "Product2Id"])
        e = self._empties()
        e["cml"] = pd.DataFrame([{"Id": "CML1", "CreditMemoId": "CM1", "Product2Id": "PROD_DIRECT"}])
        result = self._call(tj, invoice_line, e)
        assert result.iloc[0]["Product2Id"] == "PROD_DIRECT"

    def test_credit_memo_direct_wins_even_when_null_no_line_level_fallback(self):
        # REGRESSION / documents a deliberate design decision: there is NO
        # "line-level via CreditMemoLineInvoiceLine" fallback for
        # TransactionType="CreditMemo" (removed -- see resolve_product2_id()'s
        # comment for why it was confirmed structurally dead: it derived
        # from the same CreditMemoLine table, keyed by the same
        # CreditMemoId, as the direct candidate, which is always listed
        # first -- so it could never win, even when it would have
        # returned a better answer). This means if CreditMemoLine.Product2Id
        # is null, the result stays null even though a junction-table path
        # COULD in principle have found a real answer via InvoiceLine.
        tj = pd.DataFrame([{"ReferenceTransactionRecordId": "CM1", "TransactionType": "CreditMemo"}])
        invoice_line = pd.DataFrame([{"Id": "IL1", "InvoiceId": "INV1", "Product2Id": "PROD_VIA_JUNCTION"}])
        e = self._empties()
        e["cml"] = pd.DataFrame([{"Id": "CML1", "CreditMemoId": "CM1", "Product2Id": None}])
        e["cmli"] = pd.DataFrame([{"Id": "CMLI1", "CreditMemoLineId": "CML1", "InvoiceLineId": "IL1"}])
        result = self._call(tj, invoice_line, e)
        assert pd.isna(result.iloc[0]["Product2Id"])

    def test_credit_memo_header_level_fallback_when_credit_memo_line_absent_entirely(self):
        # (b) — activates when CreditMemoLine has no matching row for this
        # CreditMemoId at all (so the direct candidate produces NOTHING,
        # not even a null-valued row) -- this is the scenario where the
        # header-level fallback actually gets a chance to win.
        tj = pd.DataFrame([{"ReferenceTransactionRecordId": "CM1", "TransactionType": "CreditMemo"}])
        invoice_line = pd.DataFrame([{"Id": "IL1", "InvoiceId": "INV1", "Product2Id": "PROD_HEADER"}])
        e = self._empties()
        # credit_memo_line has NO row with CreditMemoId="CM1" -- direct
        # candidate produces nothing for CM1 at all.
        e["cmia"] = pd.DataFrame([{"CreditMemoId": "CM1", "InvoiceId": "INV1"}])
        result = self._call(tj, invoice_line, e)
        assert result.iloc[0]["Product2Id"] == "PROD_HEADER"

    def test_credit_memo_line_provides_product2id_when_junction_paths_empty(self):
        # REGRESSION carried forward from the earlier InvoiceLine-based
        # design: a CreditMemo with NO match in either junction table
        # still resolves via CreditMemoLine's own direct Product2Id.
        tj = pd.DataFrame([{"ReferenceTransactionRecordId": "CM1", "TransactionType": "CreditMemo"}])
        invoice_line = pd.DataFrame([{"Id": "IL1", "InvoiceId": "INV1", "Product2Id": "PROD_WRONG"}])
        e = self._empties()
        e["cml"] = pd.DataFrame([{"Id": "CML1", "CreditMemoId": "CM1", "Product2Id": "PROD_DIRECT"}])
        # cmli and cmia both stay empty -- no junction match at all
        result = self._call(tj, invoice_line, e)
        assert result.iloc[0]["Product2Id"] == "PROD_DIRECT"

    def test_refund_line_payment_direct_entry(self):
        tj = pd.DataFrame([{"ReferenceTransactionRecordId": "RLP1", "TransactionType": "RefundLinePayment"}])
        invoice_line = pd.DataFrame([{"Id": "IL1", "InvoiceId": "INV1", "Product2Id": "PROD1"}])
        e = self._empties()
        e["payment"] = pd.DataFrame([{"Id": "PAY1"}])
        e["pli_header"] = pd.DataFrame([{"Id": "PLI1", "PaymentId": "PAY1", "InvoiceId": "INV1"}])
        e["rlp"] = pd.DataFrame([{"Id": "RLP1", "RefundId": "REF1", "PaymentId": "PAY1"}])
        result = self._call(tj, invoice_line, e)
        assert result.iloc[0]["Product2Id"] == "PROD1"

    def test_refund_via_refund_line_payment_hop(self):
        tj = pd.DataFrame([{"ReferenceTransactionRecordId": "REF1", "TransactionType": "Refund"}])
        invoice_line = pd.DataFrame([{"Id": "IL1", "InvoiceId": "INV1", "Product2Id": "PROD1"}])
        e = self._empties()
        e["refund"] = pd.DataFrame([{"Id": "REF1"}])
        e["payment"] = pd.DataFrame([{"Id": "PAY1"}])
        e["pli_header"] = pd.DataFrame([{"Id": "PLI1", "PaymentId": "PAY1", "InvoiceId": "INV1"}])
        e["rlp"] = pd.DataFrame([{"Id": "RLP1", "RefundId": "REF1", "PaymentId": "PAY1"}])
        result = self._call(tj, invoice_line, e)
        assert result.iloc[0]["Product2Id"] == "PROD1"

    def test_refund_line_payment_via_line_level_when_present(self):
        # Line-level path (payment_line_invoice_line) for
        # RefundLinePayment/Refund's shared payment_chain() helper --
        # otherwise only exercised via the header-level path in the two
        # tests above.
        tj = pd.DataFrame([{"ReferenceTransactionRecordId": "RLP1", "TransactionType": "RefundLinePayment"}])
        invoice_line = pd.DataFrame([{"Id": "IL1", "InvoiceId": "INV1", "Product2Id": "PROD_LINE"}])
        e = self._empties()
        e["payment"] = pd.DataFrame([{"Id": "PAY1"}])
        e["pli_line"] = pd.DataFrame([{"Id": "PLL1", "PaymentId": "PAY1", "InvoiceLineId": "IL1"}])
        e["rlp"] = pd.DataFrame([{"Id": "RLP1", "RefundId": "REF1", "PaymentId": "PAY1"}])
        result = self._call(tj, invoice_line, e)
        assert result.iloc[0]["Product2Id"] == "PROD_LINE"

    def test_refund_line_payment_returns_nothing_when_payment_table_empty(self):
        # payment_chain()'s early-return guard -- RefundLinePayment/Refund
        # can't resolve anything at all if the Payment table itself comes
        # back empty (distinct from Payment's OWN resolution, which
        # deliberately does NOT depend on this table at all -- see the
        # regression test above near the Payment candidates).
        tj = pd.DataFrame([{"ReferenceTransactionRecordId": "RLP1", "TransactionType": "RefundLinePayment"}])
        invoice_line = pd.DataFrame([{"Id": "IL1", "InvoiceId": "INV1", "Product2Id": "PROD1"}])
        e = self._empties()
        e["pli_header"] = pd.DataFrame([{"Id": "PLI1", "PaymentId": "PAY1", "InvoiceId": "INV1"}])
        e["rlp"] = pd.DataFrame([{"Id": "RLP1", "RefundId": "REF1", "PaymentId": "PAY1"}])
        # e["payment"] deliberately stays empty
        result = self._call(tj, invoice_line, e)
        assert pd.isna(result.iloc[0]["Product2Id"])

    def test_invoice_direct_no_separate_invoice_table_needed(self):
        tj = pd.DataFrame([{"ReferenceTransactionRecordId": "INV1", "TransactionType": "Invoice"}])
        invoice_line = pd.DataFrame([{"Id": "IL1", "InvoiceId": "INV1", "Product2Id": "PROD1"}])
        result = self._call(tj, invoice_line, self._empties())
        assert result.iloc[0]["Product2Id"] == "PROD1"

    def test_credit_memo_line_standalone_type_direct_entry(self):
        tj = pd.DataFrame([{"ReferenceTransactionRecordId": "CML1", "TransactionType": "CreditMemoLine"}])
        invoice_line = pd.DataFrame([], columns=["Id", "InvoiceId", "Product2Id"])
        e = self._empties()
        e["cml"] = pd.DataFrame([{"Id": "CML1", "CreditMemoId": "CM1", "Product2Id": "PROD1"}])
        result = self._call(tj, invoice_line, e)
        assert result.iloc[0]["Product2Id"] == "PROD1"

    def test_credit_memo_line_tax(self):
        tj = pd.DataFrame([{"ReferenceTransactionRecordId": "CMLT1", "TransactionType": "CreditMemoLineTax"}])
        invoice_line = pd.DataFrame([], columns=["Id", "InvoiceId", "Product2Id"])
        e = self._empties()
        e["cml"] = pd.DataFrame([{"Id": "CML1", "CreditMemoId": "CM1", "Product2Id": "PROD1"}])
        e["cmlt"] = pd.DataFrame([{"Id": "CMLT1", "CreditMemoLineId": "CML1"}])
        result = self._call(tj, invoice_line, e)
        assert result.iloc[0]["Product2Id"] == "PROD1"

    def test_payment_line_invoice_standalone_type(self):
        tj = pd.DataFrame([{"ReferenceTransactionRecordId": "PLI1", "TransactionType": "PaymentLineInvoice"}])
        invoice_line = pd.DataFrame([{"Id": "IL1", "InvoiceId": "INV1", "Product2Id": "PROD1"}])
        e = self._empties()
        e["pli_header"] = pd.DataFrame([{"Id": "PLI1", "PaymentId": "PAY1", "InvoiceId": "INV1"}])
        result = self._call(tj, invoice_line, e)
        assert result.iloc[0]["Product2Id"] == "PROD1"

    def test_payment_line_invoice_line_standalone_type(self):
        tj = pd.DataFrame([{"ReferenceTransactionRecordId": "PLIL1", "TransactionType": "PaymentLineInvoiceLine"}])
        invoice_line = pd.DataFrame([{"Id": "IL1", "InvoiceId": "INV1", "Product2Id": "PROD1"}])
        e = self._empties()
        e["pli_line"] = pd.DataFrame([{"Id": "PLIL1", "PaymentId": "PAY1", "InvoiceLineId": "IL1"}])
        result = self._call(tj, invoice_line, e)
        assert result.iloc[0]["Product2Id"] == "PROD1"

    def test_credit_memo_inv_application_standalone_type(self):
        tj = pd.DataFrame([{"ReferenceTransactionRecordId": "CMIA1", "TransactionType": "CreditMemoInvApplication"}])
        invoice_line = pd.DataFrame([], columns=["Id", "InvoiceId", "Product2Id"])
        e = self._empties()
        e["cml"] = pd.DataFrame([{"Id": "CML1", "CreditMemoId": "CM1", "Product2Id": "PROD1"}])
        e["cmia"] = pd.DataFrame([{"Id": "CMIA1", "CreditMemoId": "CM1", "InvoiceId": "INV1"}])
        result = self._call(tj, invoice_line, e)
        assert result.iloc[0]["Product2Id"] == "PROD1"

    def test_credit_memo_line_invoice_line_standalone_type(self):
        tj = pd.DataFrame([{"ReferenceTransactionRecordId": "CMLI1", "TransactionType": "CreditMemoLineInvoiceLine"}])
        invoice_line = pd.DataFrame([], columns=["Id", "InvoiceId", "Product2Id"])
        e = self._empties()
        e["cml"] = pd.DataFrame([{"Id": "CML1", "CreditMemoId": "CM1", "Product2Id": "PROD1"}])
        e["cmli"] = pd.DataFrame([{"Id": "CMLI1", "CreditMemoLineId": "CML1", "InvoiceLineId": "IL1"}])
        result = self._call(tj, invoice_line, e)
        assert result.iloc[0]["Product2Id"] == "PROD1"

    def test_debit_memo_line_tax_no_real_table_yet(self):
        # Structurally complete per Dakota ("I'll still need to have the
        # mapping in case it goes live"), even though no real table
        # exists yet -- proves the chain is correct when data IS present.
        tj = pd.DataFrame([{"ReferenceTransactionRecordId": "DMLT1", "TransactionType": "DebitMemoLineTax"}])
        invoice_line = pd.DataFrame([], columns=["Id", "InvoiceId", "Product2Id"])
        e = self._empties()
        e["dml"] = pd.DataFrame([{"Id": "DML1", "ReferenceRecordId": "IL1", "Product2Id": "PROD1"}])
        e["dmlt"] = pd.DataFrame([{"Id": "DMLT1", "DebitMemoLineId": "DML1"}])
        result = self._call(tj, invoice_line, e)
        assert result.iloc[0]["Product2Id"] == "PROD1"

    def test_unmatched_row_yields_null_product2id(self):
        tj = pd.DataFrame([{"ReferenceTransactionRecordId": "UNKNOWN", "TransactionType": "InvoiceLine"}])
        invoice_line = pd.DataFrame([{"Id": "IL1", "InvoiceId": "INV1", "Product2Id": "PROD1"}])
        result = self._call(tj, invoice_line, self._empties())
        assert pd.isna(result.iloc[0]["Product2Id"])

    def test_all_tables_empty_does_not_raise(self):
        tj = pd.DataFrame([{"ReferenceTransactionRecordId": "X1", "TransactionType": "InvoiceLine"}])
        invoice_line = pd.DataFrame([], columns=["Id", "InvoiceId", "Product2Id"])
        result = self._call(tj, invoice_line, self._empties())  # must not raise
        assert pd.isna(result.iloc[0]["Product2Id"])

    def test_output_has_no_bu_did_or_invoice_line_name_columns(self):
        # An earlier version of this function (resolve_bu_did()) output
        # "bu"/"did"/"InvoiceLineName" directly -- confirms that layer is
        # fully gone, not just unused.
        tj = pd.DataFrame([{"ReferenceTransactionRecordId": "IL1", "TransactionType": "InvoiceLine"}])
        invoice_line = pd.DataFrame([{"Id": "IL1", "InvoiceId": "INV1", "Product2Id": "PROD1"}])
        result = self._call(tj, invoice_line, self._empties())
        assert "bu" not in result.columns
        assert "did" not in result.columns
        assert "InvoiceLineName" not in result.columns


# ---------------------------------------------------------------------------
# validate_required_tables_present()
# ---------------------------------------------------------------------------

class TestValidateRequiredTablesPresent:
    def _full_tables(self):
        """A dict with every table key non-empty (1 row each) -- baseline for tests to null out specific ones."""
        keys = set(MANDATORY_TABLES)
        for paths in REQUIRED_TABLES_BY_TRANSACTION_TYPE.values():
            for path in paths:
                keys.update(path)
        return {k: pd.DataFrame([{"Id": "x"}]) for k in keys}

    def test_all_mandatory_and_type_tables_present_does_not_raise(self):
        tj = pd.DataFrame([{"TransactionType": "InvoiceLine"}])
        validate_required_tables_present(tj, self._full_tables())  # must not raise

    def test_empty_transaction_journal_raises(self):
        tables = self._full_tables()
        tables["transaction_journal"] = pd.DataFrame([], columns=["TransactionType"])
        with pytest.raises(ValueError, match="transaction_journal"):
            validate_required_tables_present(pd.DataFrame([], columns=["TransactionType"]), tables)

    def test_empty_product2_raises(self):
        tables = self._full_tables()
        tables["product2"] = pd.DataFrame([], columns=["Id"])
        tj = pd.DataFrame([{"TransactionType": "InvoiceLine"}])
        with pytest.raises(ValueError, match="product2"):
            validate_required_tables_present(tj, tables)

    def test_empty_general_ledger_account_raises(self):
        tables = self._full_tables()
        tables["general_ledger_account"] = pd.DataFrame([], columns=["Id"])
        tj = pd.DataFrame([{"TransactionType": "InvoiceLine"}])
        with pytest.raises(ValueError, match="general_ledger_account"):
            validate_required_tables_present(tj, tables)

    def test_one_dead_or_path_does_not_raise_when_other_path_alive(self):
        # Confirmed with Dakota directly: PaymentLineInvoiceLine has been
        # 0 rows this whole project -- known, expected state, NOT a data
        # issue, since Payment's header-level path resolves fine on its
        # own. This is the exact scenario the OR-path design exists for.
        tables = self._full_tables()
        tables["payment_line_invoice_line"] = pd.DataFrame([], columns=["Id"])
        tj = pd.DataFrame([{"TransactionType": "Payment"}])
        validate_required_tables_present(tj, tables)  # must not raise

    def test_all_paths_dead_for_a_present_type_raises(self):
        tables = self._full_tables()
        tables["payment_line_invoice_line"] = pd.DataFrame([], columns=["Id"])
        tables["payment_line_invoice"] = pd.DataFrame([], columns=["Id"])
        tj = pd.DataFrame([{"TransactionType": "Payment"}])
        with pytest.raises(ValueError, match="Payment"):
            validate_required_tables_present(tj, tables)

    def test_dead_table_for_a_type_not_present_in_data_does_not_raise(self):
        # A TransactionType's tables being empty only matters if that type
        # actually appears in transaction_journal's data.
        tables = self._full_tables()
        tables["debit_memo_line"] = pd.DataFrame([], columns=["Id"])
        tj = pd.DataFrame([{"TransactionType": "InvoiceLine"}])  # DebitMemoLine not present
        validate_required_tables_present(tj, tables)  # must not raise

    def test_debit_memo_line_tax_placeholder_state_does_not_raise_when_type_absent(self):
        # DebitMemoLineTax's table doesn't exist in Salesforce yet, per
        # Dakota -- always empty in practice. Fine, as long as no real
        # TransactionJournal row actually has this TransactionType (which
        # can't happen today, since there's no table to produce one).
        tables = self._full_tables()
        tables["debit_memo_line_tax"] = pd.DataFrame([], columns=["Id"])
        tj = pd.DataFrame([{"TransactionType": "InvoiceLine"}])
        validate_required_tables_present(tj, tables)  # must not raise

    def test_multiple_types_present_checks_each_independently(self):
        tables = self._full_tables()
        tables["debit_memo_line"] = pd.DataFrame([], columns=["Id"])  # kills DebitMemoLine's only path
        tj = pd.DataFrame([
            {"TransactionType": "InvoiceLine"},  # fine
            {"TransactionType": "DebitMemoLine"},  # dead
        ])
        with pytest.raises(ValueError, match="DebitMemoLine"):
            validate_required_tables_present(tj, tables)

    def test_unhandled_transaction_type_not_in_map_is_ignored(self):
        # Not this function's job -- a genuinely new, never-seen
        # TransactionType is a different problem from "known type, tables
        # empty."
        tables = self._full_tables()
        tj = pd.DataFrame([{"TransactionType": "SomeFutureNewType"}])
        validate_required_tables_present(tj, tables)  # must not raise

    def test_null_transaction_type_values_ignored(self):
        tables = self._full_tables()
        tj = pd.DataFrame([{"TransactionType": None}])
        validate_required_tables_present(tj, tables)  # must not raise

    def test_transaction_journal_missing_transaction_type_column_entirely(self):
        # Defensive guard: a transaction_journal with real rows but no
        # TransactionType column at all (a malformed/older-shaped source)
        # -- distinct from transaction_journal being empty (already
        # covered by the mandatory-tables check above, which raises
        # before this line is ever reached in the real call path).
        tables = self._full_tables()
        tj = pd.DataFrame([{"SomeOtherColumn": "x"}])
        validate_required_tables_present(tj, tables)  # must not raise

    def test_missing_table_key_in_loaded_tables_treated_as_empty(self):
        tables = self._full_tables()
        del tables["invoice_line"]
        tj = pd.DataFrame([{"TransactionType": "InvoiceLine"}])
        with pytest.raises(ValueError, match="InvoiceLine"):
            validate_required_tables_present(tj, tables)


# ---------------------------------------------------------------------------
# resolve_product2_fields()
# ---------------------------------------------------------------------------

class TestResolveProduct2Fields:
    def test_resolves_bu_did_name_from_product2id(self):
        tj = pd.DataFrame([{"Product2Id": "PROD1"}])
        product2 = pd.DataFrame([{
            "Id": "PROD1", "Business_Unit_BU__c": "10902",
            "Department_ID_DID__c": "20500", "Name": "Slingshot Plan",
        }])
        result = resolve_product2_fields(tj, product2)
        assert result.iloc[0]["product2_bu"] == "10902"
        assert result.iloc[0]["product2_did"] == "20500"
        assert result.iloc[0]["product2_name"] == "Slingshot Plan"

    def test_no_matching_product2_yields_null(self):
        tj = pd.DataFrame([{"Product2Id": "PROD_UNKNOWN"}])
        product2 = pd.DataFrame([{"Id": "PROD1", "Business_Unit_BU__c": "10902",
                                   "Department_ID_DID__c": "20500", "Name": "x"}])
        result = resolve_product2_fields(tj, product2)
        assert pd.isna(result.iloc[0]["product2_bu"])
        assert pd.isna(result.iloc[0]["product2_name"])

    def test_null_product2_id_yields_null(self):
        tj = pd.DataFrame([{"Product2Id": None}])
        product2 = pd.DataFrame([{"Id": "PROD1", "Business_Unit_BU__c": "10902",
                                   "Department_ID_DID__c": "20500", "Name": "x"}])
        result = resolve_product2_fields(tj, product2)
        assert pd.isna(result.iloc[0]["product2_bu"])

    def test_empty_product2_table_yields_null_columns(self):
        tj = pd.DataFrame([{"Product2Id": "PROD1"}])
        product2 = pd.DataFrame([], columns=["Id", "Business_Unit_BU__c", "Department_ID_DID__c", "Name"])
        result = resolve_product2_fields(tj, product2)
        assert pd.isna(result.iloc[0]["product2_bu"])
        assert pd.isna(result.iloc[0]["product2_did"])
        assert pd.isna(result.iloc[0]["product2_name"])

    def test_missing_product2id_column_on_tj_yields_null(self):
        tj = pd.DataFrame([{"SomeOtherColumn": "x"}])
        product2 = pd.DataFrame([{"Id": "PROD1", "Business_Unit_BU__c": "10902",
                                   "Department_ID_DID__c": "20500", "Name": "x"}])
        result = resolve_product2_fields(tj, product2)
        assert pd.isna(result.iloc[0]["product2_bu"])

    def test_duplicate_product2_ids_take_first(self):
        tj = pd.DataFrame([{"Product2Id": "PROD1"}])
        product2 = pd.DataFrame([
            {"Id": "PROD1", "Business_Unit_BU__c": "FIRST", "Department_ID_DID__c": "FIRST_D", "Name": "First"},
            {"Id": "PROD1", "Business_Unit_BU__c": "SECOND", "Department_ID_DID__c": "SECOND_D", "Name": "Second"},
        ])
        result = resolve_product2_fields(tj, product2)
        assert result.iloc[0]["product2_bu"] == "FIRST"

    def test_does_not_mutate_input_tj(self):
        tj = pd.DataFrame([{"Product2Id": "PROD1"}])
        product2 = pd.DataFrame([{"Id": "PROD1", "Business_Unit_BU__c": "10902",
                                   "Department_ID_DID__c": "20500", "Name": "x"}])
        resolve_product2_fields(tj, product2)
        assert "product2_bu" not in tj.columns


# ---------------------------------------------------------------------------
# apply_product2_bu_did()
# ---------------------------------------------------------------------------

class TestApplyProduct2BuDid:
    def test_assigns_bu_did_directly_from_product2(self):
        tj = pd.DataFrame([{"product2_bu": "10902", "product2_did": "20500"}])
        result = apply_product2_bu_did(tj)
        assert result.iloc[0]["bu"] == "10902"
        assert result.iloc[0]["did"] == "20500"

    def test_null_product2_fields_yield_null_bu_did_no_fallback(self):
        # No more InvoiceLine fallback, per Dakota -- a direct assignment,
        # not a priority merge. apply_did_overrides()'s "10901" default
        # is what catches this downstream, not this function.
        tj = pd.DataFrame([{"product2_bu": None, "product2_did": None}])
        result = apply_product2_bu_did(tj)
        assert pd.isna(result.iloc[0]["bu"])
        assert pd.isna(result.iloc[0]["did"])

    def test_does_not_mutate_input(self):
        tj = pd.DataFrame([{"product2_bu": "A", "product2_did": "B"}])
        apply_product2_bu_did(tj)
        assert "bu" not in tj.columns


# ---------------------------------------------------------------------------
# DATASET_IDS / EXPECTED_COLUMNS sanity checks
# ---------------------------------------------------------------------------

class TestDatasetConfig:
    UUID_PATTERN = re.compile(
        r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.IGNORECASE
    )

    def test_all_dataset_ids_are_nonempty_strings(self):
        for key, value in DATASET_IDS.items():
            assert isinstance(value, str) and len(value) > 0, f"{key} has an invalid dataset_id"

    def test_no_duplicate_dataset_ids(self):
        ids = list(DATASET_IDS.values())
        assert len(ids) == len(set(ids)), "duplicate dataset_id found across table entries"

    def test_dataset_ids_look_like_uuids(self):
        for key, value in DATASET_IDS.items():
            assert self.UUID_PATTERN.match(value), f"{key}'s dataset_id doesn't look like a UUID: {value}"

    def test_expected_columns_keys_are_known_tables(self):
        # every table with an EXPECTED_COLUMNS entry should also have a DATASET_IDS entry
        for key in EXPECTED_COLUMNS:
            assert key in DATASET_IDS, f"EXPECTED_COLUMNS has '{key}' but DATASET_IDS does not"

    def test_tables_used_by_build_source_dataframe_have_expected_columns(self):
        # the tables actually loaded by build_source_dataframe should all have
        # an EXPECTED_COLUMNS entry, so empty results come back correctly shaped.
        # NOTE: "account" is deliberately NOT in this list — per Dakota,
        # Account is no longer loaded by build_source_dataframe (replaced by
        # general_ledger_account). "account" stays in DATASET_IDS/
        # EXPECTED_COLUMNS regardless, since salesforce_ofac.py and
        # salesforce_ofac_billing_preview.py still read it independently.
        used_tables = [
            "transaction_journal", "invoice_line", "invoice_line_tax",
            "payment_line_invoice_line", "payment_line_invoice", "credit_memo_line",
            "credit_memo_line_invoice_line", "credit_memo_inv_application",
            "general_ledger_account", "debit_memo_line", "payment", "refund",
            "refund_line_payment", "product2", "credit_memo_line_tax",
            "debit_memo_line_tax",
        ]
        for table in used_tables:
            assert table in EXPECTED_COLUMNS, f"{table} is loaded but has no EXPECTED_COLUMNS entry"
            assert table in DATASET_IDS, f"{table} is loaded but has no DATASET_IDS entry"

    def test_credit_memo_line_tax_dataset_id_is_confirmed_real_value(self):
        # Confirmed by Dakota — was present in the very first
        # ingest_revcloud.yml screenshot, just never wired up until
        # CreditMemoLineTax needed it as its own TransactionType.
        assert DATASET_IDS["credit_memo_line_tax"] == "1fbd49b9-7417-484c-acd8-8abf7208b670"

    def test_debit_memo_line_tax_dataset_id_still_needs_real_value(self):
        # PLACEHOLDER — per Dakota, no corresponding table exists in
        # Salesforce for DebitMemoLineTax AT ALL yet (distinct from
        # refund_line_payment above, which has a real table just not yet
        # registered). This test fails on purpose until the table exists
        # and a real dataset_id is confirmed — same pattern as
        # refund_line_payment above. Delete this test once a real value is
        # in, and add real-data resolution tests to
        # TestResolveProduct2Id's DebitMemoLineTax coverage.
        assert DATASET_IDS["debit_memo_line_tax"] != "00000000-0000-0000-0000-000000000002", (
            "debit_memo_line_tax is still using a placeholder dataset_id "
            "— no corresponding table exists in Salesforce yet, per Dakota"
        )

    def test_general_ledger_account_dataset_id_is_confirmed_real_value(self):
        # Confirmed by Dakota — replaces the earlier placeholder
        # ("00000000-0000-0000-0000-000000000000") that a previous version
        # of this test intentionally failed on.
        assert DATASET_IDS["general_ledger_account"] == "78e7dd64-9999-42f1-a44a-38bf5157375e"

    def test_debit_memo_line_dataset_id_is_confirmed_real_value(self):
        assert DATASET_IDS["debit_memo_line"] == "70a38b7c-47b7-4664-8f7b-14c1e49d358e"

    def test_refund_line_payment_dataset_id_still_needs_real_value(self):
        # PLACEHOLDER — RefundLinePayment's dataset_id registration hasn't
        # gone through yet, per Dakota. This test fails on purpose until
        # it's filled in with a real value, so CI keeps this visible rather
        # than silently shipping a fake dataset_id — same pattern as the
        # earlier general_ledger_account placeholder. Delete this test once
        # a real value is in (see test_general_ledger_account_dataset_id_is_confirmed_real_value
        # above for what that looks like), and un-skip/rewrite the Refund/
        # RefundLinePayment resolution tests in TestResolveBuDid that
        # currently only exercise the empty-table (unregistered) state.
        assert DATASET_IDS["refund_line_payment"] != "00000000-0000-0000-0000-000000000000", (
            "refund_line_payment is still using a placeholder dataset_id "
            "— fill in the real value once registration completes"
        )

    def test_product2_dataset_id_is_confirmed_real_value(self):
        # Confirmed by Dakota — replaces the earlier placeholder
        # ("00000000-0000-0000-0000-000000000001").
        assert DATASET_IDS["product2"] == "29351664-7f3c-4266-8937-018cc5a7dd44"

    def test_dataset_ids_keys_are_snake_case(self):
        for key in DATASET_IDS:
            assert key == key.lower(), f"{key} should be lowercase/snake_case"
            assert " " not in key


# ---------------------------------------------------------------------------
# build_source_dataframe() — full orchestration, mocked S3
# ---------------------------------------------------------------------------

class TestBuildSourceDataframeOrchestration:
    def _patch_read_table(self, monkeypatch, table_data: dict):
        """
        table_data: dict of table_key -> DataFrame to return.
        Missing keys return an empty DataFrame with EXPECTED_COLUMNS shape.
        """
        def fake_read_table(s3_client, bucket, dataset_id, expected_columns=None,
                             source_prefix="salesforce/reports"):
            # Reverse-lookup the table_key from dataset_id so the fake doesn't
            # need to duplicate DATASET_IDS itself.
            table_key = next(k for k, v in DATASET_IDS.items() if v == dataset_id)
            if table_key in table_data:
                return table_data[table_key]
            return pd.DataFrame(columns=expected_columns or [])

        monkeypatch.setattr(glsj, "read_table_by_dataset_id", fake_read_table)

    # Wide, fixed window so these orchestration tests are deterministic
    # regardless of what "today" happens to be when they run — the
    # ActivityDate filter defaults to month-to-date, which would otherwise
    # make every fixture using a hardcoded date (e.g. "2026-08-01") flaky.
    WIDE_WINDOW = dict(start_date="2000-01-01", end_date="2100-01-01")

    def _product2(self, product2_id="PROD1", bu="US001", did="10500", name="Widget Subscription"):
        return pd.DataFrame([{"Id": product2_id, "Business_Unit_BU__c": bu,
                               "Department_ID_DID__c": did, "Name": name}])

    def test_end_to_end_with_invoice_line_transaction(self, monkeypatch):
        table_data = {
            "transaction_journal": pd.DataFrame([{
                "Name": "Batch 1", "UsageResourceId": None,
                "ReferenceTransactionRecordId": "IL1", "TransactionType": "InvoiceLine",
                "ActivityDate": "2026-08-01", "UsageType": "Storage",
                "Credit": None, "Debit": 100.0,
                "DebitGeneralLedgerAccountId": "GLA1", "CreditGeneralLedgerAccountId": None,
            }]),
            "invoice_line": pd.DataFrame([{"Id": "IL1", "InvoiceId": "INV1", "Product2Id": "PROD1"}]),
            "product2": self._product2(bu="US001", did="10500", name="Widget Subscription"),
            "general_ledger_account": pd.DataFrame([{"Id": "GLA1", "GL_Accounting_Number__c": "20012345"}]),
        }
        self._patch_read_table(monkeypatch, table_data)

        result = build_source_dataframe(MagicMock(), "bucket", **self.WIDE_WINDOW)

        assert len(result) == 1
        row = result.iloc[0]
        assert row["InvoiceLine.Business_Unit"] == "US001"
        assert row["InvoiceLine.Department_Id"] == "10500"
        assert row["GeneralLedgerAccount.GL_Accounting_Number__c"] == "20012345"
        assert row["TransactionJournal.CreditDebit"] == 100.0
        assert row["TransactionJournal.Name"] == "Batch 1"

    def test_end_to_end_with_payment_transaction_via_header_fallback(self, monkeypatch):
        table_data = {
            "transaction_journal": pd.DataFrame([{
                "Name": "Payment Batch", "UsageResourceId": None,
                "ReferenceTransactionRecordId": "PAY1", "TransactionType": "Payment",
                "ActivityDate": "2026-08-02", "UsageType": None,
                "Credit": 50.0, "Debit": None,
                "DebitGeneralLedgerAccountId": None, "CreditGeneralLedgerAccountId": "GLA2",
            }]),
            "invoice_line": pd.DataFrame([{"Id": "IL1", "InvoiceId": "INV1", "Product2Id": "PROD1"}]),
            "payment": pd.DataFrame([{"Id": "PAY1"}]),
            "payment_line_invoice": pd.DataFrame([{"Id": "PLI1", "PaymentId": "PAY1", "InvoiceId": "INV1"}]),
            "product2": self._product2(bu="US002", did="20500", name="Support Plan"),
            "general_ledger_account": pd.DataFrame([{"Id": "GLA2", "GL_Accounting_Number__c": "20099999"}]),
        }
        self._patch_read_table(monkeypatch, table_data)

        result = build_source_dataframe(MagicMock(), "bucket", **self.WIDE_WINDOW)

        row = result.iloc[0]
        assert row["InvoiceLine.Business_Unit"] == "US002"
        assert row["GeneralLedgerAccount.GL_Accounting_Number__c"] == "20099999"
        assert row["TransactionJournal.CreditDebit"] == -50.0  # credit -> negative

    def test_end_to_end_with_credit_memo_transaction_direct_product2(self, monkeypatch):
        table_data = {
            "transaction_journal": pd.DataFrame([{
                "Name": "Credit Batch", "UsageResourceId": None,
                "ReferenceTransactionRecordId": "CM1", "TransactionType": "CreditMemo",
                "ActivityDate": "2026-08-03", "UsageType": None,
                "Credit": 25.0, "Debit": None,
                "DebitGeneralLedgerAccountId": None, "CreditGeneralLedgerAccountId": "GLA3",
            }]),
            "credit_memo_line": pd.DataFrame([{"Id": "CML1", "CreditMemoId": "CM1", "Product2Id": "PROD1"}]),
            "product2": self._product2(bu="US003", did="40500", name="Enterprise Plan"),
            "general_ledger_account": pd.DataFrame([{"Id": "GLA3", "GL_Accounting_Number__c": "20077777"}]),
        }
        self._patch_read_table(monkeypatch, table_data)

        result = build_source_dataframe(MagicMock(), "bucket", **self.WIDE_WINDOW)

        row = result.iloc[0]
        assert row["InvoiceLine.Business_Unit"] == "US003"
        assert row["GeneralLedgerAccount.GL_Accounting_Number__c"] == "20077777"
        assert row["TransactionJournal.CreditDebit"] == -25.0

    def test_end_to_end_bu_defaults_to_10901_when_unresolvable(self, monkeypatch):
        # per Dakota: "For the most part, all BUs should be 10901 as the
        # default value" -- this row's ReferenceTransactionRecordId
        # doesn't match anything in invoice_line, so Product2Id (and thus
        # bu) never resolves, but invoice_line itself IS non-empty (so
        # InvoiceLine's resolution path is technically "alive" — this is
        # a per-row miss, not a whole-table-empty data issue).
        table_data = {
            "transaction_journal": pd.DataFrame([{
                "Name": "Orphan", "UsageResourceId": None,
                "ReferenceTransactionRecordId": "UNKNOWN1", "TransactionType": "InvoiceLine",
                "ActivityDate": "2026-08-03", "UsageType": None,
                "Credit": None, "Debit": 10.0,
                "DebitGeneralLedgerAccountId": None, "CreditGeneralLedgerAccountId": None,
            }]),
            "invoice_line": pd.DataFrame([{"Id": "IL_OTHER", "InvoiceId": "INV1", "Product2Id": "PROD1"}]),
            "product2": self._product2(),
            "general_ledger_account": pd.DataFrame([{"Id": "GLA_X", "GL_Accounting_Number__c": "1"}]),
        }
        self._patch_read_table(monkeypatch, table_data)

        result = build_source_dataframe(MagicMock(), "bucket", **self.WIDE_WINDOW)

        assert result.iloc[0]["InvoiceLine.Business_Unit"] == "10901"

    def test_end_to_end_did_override_by_gl_account_beats_product_name_override(self, monkeypatch):
        # 10040049 -> 16605 must win even when Product2.Name would
        # otherwise trigger the slingshot override — highest priority per
        # Dakota ("should supersede whatever the DID is").
        table_data = {
            "transaction_journal": pd.DataFrame([{
                "Name": "Batch", "UsageResourceId": None,
                "ReferenceTransactionRecordId": "IL1", "TransactionType": "InvoiceLine",
                "ActivityDate": "2026-08-01", "UsageType": None,
                "Credit": None, "Debit": 1.0,
                "DebitGeneralLedgerAccountId": "GLA_SPECIAL", "CreditGeneralLedgerAccountId": None,
            }]),
            "invoice_line": pd.DataFrame([{"Id": "IL1", "InvoiceId": "INV1", "Product2Id": "PROD1"}]),
            "product2": self._product2(bu="US001", did="10500", name="Slingshot Pro Subscription"),
            "general_ledger_account": pd.DataFrame(
                [{"Id": "GLA_SPECIAL", "GL_Accounting_Number__c": "10040049"}]
            ),
        }
        self._patch_read_table(monkeypatch, table_data)

        result = build_source_dataframe(MagicMock(), "bucket", **self.WIDE_WINDOW)

        assert result.iloc[0]["InvoiceLine.Department_Id"] == "16605"

    def test_missing_gl_account_match_leaves_gl_number_null_without_failing(self, monkeypatch):
        # general_ledger_account is non-empty (mandatory table satisfied)
        # but doesn't contain THIS row's specific GLA id -- a per-row
        # miss, not a whole-table-empty data issue, so this must not raise.
        table_data = {
            "transaction_journal": pd.DataFrame([{
                "Name": "Orphan GL", "UsageResourceId": None,
                "ReferenceTransactionRecordId": "IL1", "TransactionType": "InvoiceLine",
                "ActivityDate": "2026-08-03", "UsageType": None,
                "Credit": None, "Debit": 10.0,
                "DebitGeneralLedgerAccountId": "GLA_UNKNOWN", "CreditGeneralLedgerAccountId": None,
            }]),
            "invoice_line": pd.DataFrame([{"Id": "IL1", "InvoiceId": "INV1", "Product2Id": "PROD1"}]),
            "product2": self._product2(),
            "general_ledger_account": pd.DataFrame([{"Id": "GLA_OTHER", "GL_Accounting_Number__c": "1"}]),
        }
        self._patch_read_table(monkeypatch, table_data)

        result = build_source_dataframe(MagicMock(), "bucket", **self.WIDE_WINDOW)

        assert len(result) == 1
        assert pd.isna(result.iloc[0]["GeneralLedgerAccount.GL_Accounting_Number__c"])
        assert result.iloc[0]["TransactionJournal.CreditDebit"] == 10.0  # amount still resolves independently

    def test_empty_transaction_journal_raises(self, monkeypatch):
        # transaction_journal is unconditionally mandatory, per Dakota —
        # an earlier version of this test expected an empty (not raised)
        # result; that's no longer the correct behavior.
        self._patch_read_table(monkeypatch, {})
        with pytest.raises(ValueError, match="transaction_journal"):
            build_source_dataframe(MagicMock(), "bucket", **self.WIDE_WINDOW)

    def test_empty_product2_raises_even_with_valid_transaction_journal(self, monkeypatch):
        table_data = {
            "transaction_journal": pd.DataFrame([{
                "Name": "X", "UsageResourceId": None,
                "ReferenceTransactionRecordId": "IL1", "TransactionType": "InvoiceLine",
                "ActivityDate": "2026-08-01", "UsageType": None,
                "Credit": None, "Debit": 5.0,
                "DebitGeneralLedgerAccountId": None, "CreditGeneralLedgerAccountId": None,
            }]),
            "invoice_line": pd.DataFrame([{"Id": "IL1", "InvoiceId": "INV1", "Product2Id": "PROD1"}]),
            "general_ledger_account": pd.DataFrame([{"Id": "GLA1", "GL_Accounting_Number__c": "1"}]),
            # product2 deliberately omitted -- comes back empty
        }
        self._patch_read_table(monkeypatch, table_data)
        with pytest.raises(ValueError, match="product2"):
            build_source_dataframe(MagicMock(), "bucket", **self.WIDE_WINDOW)

    def test_present_transaction_type_with_no_viable_path_raises(self, monkeypatch):
        # A Payment TransactionType row present, but BOTH
        # payment_line_invoice_line AND payment_line_invoice are empty --
        # no viable resolution path at all for Payment. Confirmed with
        # Dakota this SHOULD raise (distinct from just one of the two
        # being empty, which should NOT raise — see
        # TestValidateRequiredTablesPresent for the unit-level version of
        # this same distinction).
        table_data = {
            "transaction_journal": pd.DataFrame([{
                "Name": "X", "UsageResourceId": None,
                "ReferenceTransactionRecordId": "PAY1", "TransactionType": "Payment",
                "ActivityDate": "2026-08-01", "UsageType": None,
                "Credit": None, "Debit": 5.0,
                "DebitGeneralLedgerAccountId": None, "CreditGeneralLedgerAccountId": None,
            }]),
            "invoice_line": pd.DataFrame([{"Id": "IL1", "InvoiceId": "INV1", "Product2Id": "PROD1"}]),
            "product2": self._product2(),
            "general_ledger_account": pd.DataFrame([{"Id": "GLA1", "GL_Accounting_Number__c": "1"}]),
            # payment_line_invoice_line and payment_line_invoice both omitted
        }
        self._patch_read_table(monkeypatch, table_data)
        with pytest.raises(ValueError, match="Payment"):
            build_source_dataframe(MagicMock(), "bucket", **self.WIDE_WINDOW)

    def test_result_has_expected_final_columns(self, monkeypatch):
        table_data = {
            "transaction_journal": pd.DataFrame([{
                "Name": "X", "UsageResourceId": None,
                "ReferenceTransactionRecordId": "IL1", "TransactionType": "InvoiceLine",
                "ActivityDate": "2026-08-01", "UsageType": "Storage",
                "Credit": None, "Debit": 5.0,
                "DebitGeneralLedgerAccountId": None, "CreditGeneralLedgerAccountId": None,
            }]),
            "invoice_line": pd.DataFrame([{"Id": "IL1", "InvoiceId": "INV1", "Product2Id": "PROD1"}]),
            "product2": self._product2(),
            "general_ledger_account": pd.DataFrame([{"Id": "GLA1", "GL_Accounting_Number__c": "1"}]),
        }
        self._patch_read_table(monkeypatch, table_data)

        result = build_source_dataframe(MagicMock(), "bucket", **self.WIDE_WINDOW)

        expected_cols = {
            "InvoiceLine.Business_Unit", "InvoiceLine.Department_Id",
            "TransactionJournal.ActivityDate", "TransactionJournal.TransactionType",
            "GeneralLedgerAccount.GL_Accounting_Number__c", "TransactionJournal.UsageType",
            "TransactionJournal.CreditDebit", "TransactionJournal.Name",
        }
        assert expected_cols == set(result.columns)

    def test_multiple_transaction_journal_rows_all_present_in_output(self, monkeypatch):
        table_data = {
            "transaction_journal": pd.DataFrame([
                {"Name": "A", "UsageResourceId": None, "ReferenceTransactionRecordId": "IL1",
                 "TransactionType": "InvoiceLine", "ActivityDate": "2026-08-01",
                 "UsageType": "Storage", "Credit": None, "Debit": 1.0,
                 "DebitGeneralLedgerAccountId": None, "CreditGeneralLedgerAccountId": None},
                {"Name": "B", "UsageResourceId": None, "ReferenceTransactionRecordId": "IL2",
                 "TransactionType": "InvoiceLine", "ActivityDate": "2026-08-02",
                 "UsageType": "Compute", "Credit": None, "Debit": 2.0,
                 "DebitGeneralLedgerAccountId": None, "CreditGeneralLedgerAccountId": None},
            ]),
            "invoice_line": pd.DataFrame([
                {"Id": "IL1", "InvoiceId": "INV1", "Product2Id": "PROD1"},
                {"Id": "IL2", "InvoiceId": "INV2", "Product2Id": "PROD2"},
            ]),
            "product2": pd.DataFrame([
                {"Id": "PROD1", "Business_Unit_BU__c": "US001", "Department_ID_DID__c": "10500", "Name": "A"},
                {"Id": "PROD2", "Business_Unit_BU__c": "US002", "Department_ID_DID__c": "20500", "Name": "B"},
            ]),
            "general_ledger_account": pd.DataFrame([{"Id": "GLA1", "GL_Accounting_Number__c": "1"}]),
        }
        self._patch_read_table(monkeypatch, table_data)

        result = build_source_dataframe(MagicMock(), "bucket", **self.WIDE_WINDOW)
        assert len(result) == 2

    def test_logging_fires_when_log_provided(self, monkeypatch):
        # Covers the `if log:` branches -- otherwise never executed by any
        # other test in this class, since every other test omits log
        # entirely (log=None is already covered a dozen times over by
        # every test that doesn't touch this parameter, so a dedicated
        # "no logging when log is None" test would be redundant -- not
        # included here on purpose). Checks the branch actually fires and
        # includes a couple of representative messages, without pinning
        # every log line's exact wording -- that's an implementation
        # detail more than a behavior worth locking down line-by-line.
        table_data = {
            "transaction_journal": pd.DataFrame([{
                "Name": "X", "UsageResourceId": None,
                "ReferenceTransactionRecordId": "IL1", "TransactionType": "InvoiceLine",
                "ActivityDate": "2026-08-01", "UsageType": "Storage",
                "Credit": None, "Debit": 5.0,
                "DebitGeneralLedgerAccountId": None, "CreditGeneralLedgerAccountId": None,
            }]),
            "invoice_line": pd.DataFrame([{"Id": "IL1", "InvoiceId": "INV1", "Product2Id": "PROD1"}]),
            "product2": self._product2(),
            "general_ledger_account": pd.DataFrame([{"Id": "GLA1", "GL_Accounting_Number__c": "1"}]),
        }
        self._patch_read_table(monkeypatch, table_data)
        fake_log = MagicMock()

        build_source_dataframe(MagicMock(), "bucket", log=fake_log, **self.WIDE_WINDOW)

        assert fake_log.info.called
        messages = [call.args[0] for call in fake_log.info.call_args_list]
        assert any("reading transaction_journal" in m for m in messages)
        assert any("resolving product2id" in m for m in messages)

    def test_passes_through_source_prefix(self, monkeypatch):
        captured_args = {}

        def fake_read_table(s3_client, bucket, dataset_id, expected_columns=None,
                             source_prefix="salesforce/reports"):
            captured_args["source_prefix"] = source_prefix
            return pd.DataFrame(columns=expected_columns or [])

        monkeypatch.setattr(glsj, "read_table_by_dataset_id", fake_read_table)

        # No table data provided at all -- transaction_journal (loaded
        # first, before validation runs) still captures source_prefix,
        # even though the call ultimately raises on the mandatory-table
        # check right after.
        with pytest.raises(ValueError):
            build_source_dataframe(MagicMock(), "bucket", source_prefix="custom/reports/path")

        assert captured_args["source_prefix"] == "custom/reports/path"


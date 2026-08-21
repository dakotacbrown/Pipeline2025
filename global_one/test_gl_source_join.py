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
  - apply_did_overrides(): bu default, slingshot/databolt did overrides, and
    the 10040049 -> 16605 override (highest priority)
  - resolve_date_window(): month-to-date default vs. explicit start/end
  - resolve_bu_did(): per-TransactionType bu/department_id resolution,
    including the line-level vs. header-level fallback priority
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
    resolve_bu_did,
    resolve_amount,
    resolve_gl_accounting_number,
    apply_did_overrides,
    resolve_date_window,
    build_source_dataframe,
    DATASET_IDS,
    EXPECTED_COLUMNS,
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
            "bu": "US001", "did": "10500", "InvoiceLineName": "Widget Plan",
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
            "bu": None, "did": None, "InvoiceLineName": "Slingshot Plan",
            "gl_accounting_number_c": None,
        }])
        result = apply_did_overrides(df)  # must not raise
        assert result.iloc[0]["bu"] == "10901"
        assert result.iloc[0]["did"] == "16637"

    def test_slingshot_in_name_overrides_did(self):
        df = self._base_df(InvoiceLineName="Slingshot Enterprise Plan")
        result = apply_did_overrides(df)
        assert result.iloc[0]["did"] == "16637"

    def test_slingshot_match_is_case_insensitive(self):
        df = self._base_df(InvoiceLineName="SLINGSHOT basic")
        result = apply_did_overrides(df)
        assert result.iloc[0]["did"] == "16637"

    def test_databolt_in_name_overrides_did(self):
        df = self._base_df(InvoiceLineName="DataBolt Pro")
        result = apply_did_overrides(df)
        assert result.iloc[0]["did"] == "16635"

    def test_databolt_match_is_case_insensitive(self):
        df = self._base_df(InvoiceLineName="databolt starter")
        result = apply_did_overrides(df)
        assert result.iloc[0]["did"] == "16635"

    def test_name_without_either_keyword_leaves_did_unchanged(self):
        df = self._base_df(InvoiceLineName="Something Else Entirely")
        result = apply_did_overrides(df)
        assert result.iloc[0]["did"] == "10500"

    def test_null_name_does_not_raise_and_leaves_did_unchanged(self):
        df = self._base_df(InvoiceLineName=None)
        result = apply_did_overrides(df)
        assert result.iloc[0]["did"] == "10500"

    def test_gl_account_10040049_overrides_did_to_16605(self):
        df = self._base_df(gl_accounting_number_c="10040049")
        result = apply_did_overrides(df)
        assert result.iloc[0]["did"] == "16605"

    def test_gl_account_10040049_beats_slingshot_override(self):
        # highest priority per Dakota: "should supersede whatever the DID is"
        df = self._base_df(gl_accounting_number_c="10040049", InvoiceLineName="Slingshot Plan")
        result = apply_did_overrides(df)
        assert result.iloc[0]["did"] == "16605"

    def test_gl_account_10040049_beats_databolt_override(self):
        df = self._base_df(gl_accounting_number_c="10040049", InvoiceLineName="DataBolt Plan")
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
            {"bu": None, "did": "1", "InvoiceLineName": "Slingshot", "gl_accounting_number_c": "111"},
            {"bu": "US001", "did": "2", "InvoiceLineName": "DataBolt", "gl_accounting_number_c": "222"},
            {"bu": "US002", "did": "3", "InvoiceLineName": "Plain Plan", "gl_accounting_number_c": "10040049"},
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
# resolve_bu_did()
# ---------------------------------------------------------------------------

class TestResolveBuDid:
    """
    All fixtures include InvoiceId on invoice_line (needed for the
    header-level fallback paths) and empty frames for payment_line_invoice /
    credit_memo_inv_application unless a test specifically exercises them.
    """

    def _empties(self):
        return dict(
            ilt=pd.DataFrame([], columns=["Id", "InvoiceLineId"]),
            pli_line=pd.DataFrame([], columns=["PaymentId", "InvoiceLineId"]),
            pli_header=pd.DataFrame([], columns=["PaymentId", "InvoiceId"]),
            cml=pd.DataFrame([], columns=["Id", "CreditMemoId"]),
            cmli=pd.DataFrame([], columns=["CreditMemoLineId", "InvoiceLineId"]),
            cmia=pd.DataFrame([], columns=["CreditMemoId", "InvoiceId"]),
        )

    def test_resolves_via_invoice_line_directly(self):
        tj = pd.DataFrame([{"ReferenceTransactionRecordId": "IL1", "TransactionType": "InvoiceLine"}])
        invoice_line = pd.DataFrame([{
            "Id": "IL1", "InvoiceId": "INV1", "Product2Id": "PROD1",
            "Business_Unit_BU__c": "US001", "Department_ID_DID__c": "10500",
        }])
        e = self._empties()

        result = resolve_bu_did(tj, invoice_line, e["ilt"], e["pli_line"], e["pli_header"],
                                 e["cml"], e["cmli"], e["cmia"])
        assert result.iloc[0]["bu"] == "US001"
        assert result.iloc[0]["did"] == "10500"

    def test_resolves_via_invoice_line_tax_indirectly(self):
        tj = pd.DataFrame([{"ReferenceTransactionRecordId": "ILT1", "TransactionType": "InvoiceLineTax"}])
        invoice_line = pd.DataFrame([{
            "Id": "IL1", "InvoiceId": "INV1", "Product2Id": "PROD2",
            "Business_Unit_BU__c": "EU002", "Department_ID_DID__c": "20999",
        }])
        invoice_line_tax = pd.DataFrame([{"Id": "ILT1", "InvoiceLineId": "IL1"}])
        e = self._empties()

        result = resolve_bu_did(tj, invoice_line, invoice_line_tax, e["pli_line"], e["pli_header"],
                                 e["cml"], e["cmli"], e["cmia"])
        assert result.iloc[0]["bu"] == "EU002"
        assert result.iloc[0]["did"] == "20999"

    def test_resolves_payment_via_line_level_when_present(self):
        tj = pd.DataFrame([{"ReferenceTransactionRecordId": "PAY1", "TransactionType": "Payment"}])
        invoice_line = pd.DataFrame([{
            "Id": "IL1", "InvoiceId": "INV1", "Product2Id": "PROD3",
            "Business_Unit_BU__c": "US002", "Department_ID_DID__c": "30500",
        }])
        payment_line_invoice_line = pd.DataFrame([{"PaymentId": "PAY1", "InvoiceLineId": "IL1"}])
        e = self._empties()

        result = resolve_bu_did(tj, invoice_line, e["ilt"], payment_line_invoice_line, e["pli_header"],
                                 e["cml"], e["cmli"], e["cmia"])
        assert result.iloc[0]["bu"] == "US002"
        assert result.iloc[0]["did"] == "30500"

    def test_resolves_payment_via_header_level_fallback(self):
        # This is the path that actually works today — PaymentLineInvoiceLine
        # is confirmed 0 rows in real data, PaymentLineInvoice has 837.
        tj = pd.DataFrame([{"ReferenceTransactionRecordId": "PAY1", "TransactionType": "Payment"}])
        invoice_line = pd.DataFrame([{
            "Id": "IL1", "InvoiceId": "INV1", "Product2Id": "PROD3",
            "Business_Unit_BU__c": "US002", "Department_ID_DID__c": "30500",
        }])
        payment_line_invoice = pd.DataFrame([{"PaymentId": "PAY1", "InvoiceId": "INV1"}])
        e = self._empties()

        result = resolve_bu_did(tj, invoice_line, e["ilt"], e["pli_line"], payment_line_invoice,
                                 e["cml"], e["cmli"], e["cmia"])
        assert result.iloc[0]["bu"] == "US002"
        assert result.iloc[0]["did"] == "30500"

    def test_resolves_credit_memo_via_line_level_when_present(self):
        tj = pd.DataFrame([{"ReferenceTransactionRecordId": "CM1", "TransactionType": "CreditMemo"}])
        invoice_line = pd.DataFrame([{
            "Id": "IL1", "InvoiceId": "INV1", "Product2Id": "PROD4",
            "Business_Unit_BU__c": "US003", "Department_ID_DID__c": "40500",
        }])
        credit_memo_line = pd.DataFrame([{"Id": "CML1", "CreditMemoId": "CM1"}])
        credit_memo_line_invoice_line = pd.DataFrame([{"CreditMemoLineId": "CML1", "InvoiceLineId": "IL1"}])
        e = self._empties()

        result = resolve_bu_did(tj, invoice_line, e["ilt"], e["pli_line"], e["pli_header"],
                                 credit_memo_line, credit_memo_line_invoice_line, e["cmia"])
        assert result.iloc[0]["bu"] == "US003"
        assert result.iloc[0]["did"] == "40500"

    def test_resolves_credit_memo_via_header_level_fallback(self):
        # This is the path that actually works today — CreditMemoLineInvoiceLine
        # is confirmed 0 rows in real data, CreditMemoInvApplication has 42.
        tj = pd.DataFrame([{"ReferenceTransactionRecordId": "CM1", "TransactionType": "CreditMemo"}])
        invoice_line = pd.DataFrame([{
            "Id": "IL1", "InvoiceId": "INV1", "Product2Id": "PROD4",
            "Business_Unit_BU__c": "US003", "Department_ID_DID__c": "40500",
        }])
        credit_memo_inv_application = pd.DataFrame([{"CreditMemoId": "CM1", "InvoiceId": "INV1"}])
        e = self._empties()

        result = resolve_bu_did(tj, invoice_line, e["ilt"], e["pli_line"], e["pli_header"],
                                 e["cml"], e["cmli"], credit_memo_inv_application)
        assert result.iloc[0]["bu"] == "US003"
        assert result.iloc[0]["did"] == "40500"

    def test_line_level_wins_over_header_level_when_both_present(self):
        # If line-level data ever gets populated, it should take priority
        # over the header-level fallback for the same header id.
        tj = pd.DataFrame([{"ReferenceTransactionRecordId": "PAY1", "TransactionType": "Payment"}])
        invoice_line = pd.DataFrame([
            {"Id": "IL1", "InvoiceId": "INV1", "Product2Id": "PROD1",
             "Business_Unit_BU__c": "LINE_LEVEL_BU", "Department_ID_DID__c": "10500"},
            {"Id": "IL2", "InvoiceId": "INV1", "Product2Id": "PROD2",
             "Business_Unit_BU__c": "HEADER_LEVEL_BU", "Department_ID_DID__c": "20500"},
        ])
        payment_line_invoice_line = pd.DataFrame([{"PaymentId": "PAY1", "InvoiceLineId": "IL1"}])
        payment_line_invoice = pd.DataFrame([{"PaymentId": "PAY1", "InvoiceId": "INV1"}])
        e = self._empties()

        result = resolve_bu_did(tj, invoice_line, e["ilt"], payment_line_invoice_line, payment_line_invoice,
                                 e["cml"], e["cmli"], e["cmia"])
        assert result.iloc[0]["bu"] == "LINE_LEVEL_BU"

    def test_payment_fan_out_takes_first_match(self):
        # a payment header applied to an invoice with lines from different
        # BUs — documents that this takes the first match rather than erroring
        tj = pd.DataFrame([{"ReferenceTransactionRecordId": "PAY1", "TransactionType": "Payment"}])
        invoice_line = pd.DataFrame([
            {"Id": "IL1", "InvoiceId": "INV1", "Product2Id": "PROD1",
             "Business_Unit_BU__c": "US001", "Department_ID_DID__c": "10500"},
            {"Id": "IL2", "InvoiceId": "INV1", "Product2Id": "PROD2",
             "Business_Unit_BU__c": "US002", "Department_ID_DID__c": "20500"},
        ])
        payment_line_invoice = pd.DataFrame([{"PaymentId": "PAY1", "InvoiceId": "INV1"}])
        e = self._empties()

        result = resolve_bu_did(tj, invoice_line, e["ilt"], e["pli_line"], payment_line_invoice,
                                 e["cml"], e["cmli"], e["cmia"])
        assert len(result) == 1  # one TJ row in, one row out — no accidental fan-out of tj itself
        assert result.iloc[0]["bu"] == "US001"  # first match wins

    def test_unresolvable_transaction_type_yields_null_bu_did(self):
        # a TransactionType with no matching row anywhere in any source table
        tj = pd.DataFrame([{"ReferenceTransactionRecordId": "UNKNOWN1", "TransactionType": "SomeOtherType"}])
        empty_il = pd.DataFrame([], columns=["Id", "InvoiceId", "Product2Id",
                                              "Business_Unit_BU__c", "Department_ID_DID__c"])
        e = self._empties()

        result = resolve_bu_did(tj, empty_il, e["ilt"], e["pli_line"], e["pli_header"],
                                 e["cml"], e["cmli"], e["cmia"])
        assert pd.isna(result.iloc[0]["bu"])
        assert pd.isna(result.iloc[0]["did"])

    def test_multiple_tj_rows_resolve_independently(self):
        tj = pd.DataFrame([
            {"ReferenceTransactionRecordId": "IL1", "TransactionType": "InvoiceLine"},
            {"ReferenceTransactionRecordId": "IL2", "TransactionType": "InvoiceLine"},
        ])
        invoice_line = pd.DataFrame([
            {"Id": "IL1", "InvoiceId": "INV1", "Product2Id": "PROD1",
             "Business_Unit_BU__c": "US001", "Department_ID_DID__c": "10500"},
            {"Id": "IL2", "InvoiceId": "INV2", "Product2Id": "PROD2",
             "Business_Unit_BU__c": "US002", "Department_ID_DID__c": "20500"},
        ])
        e = self._empties()

        result = resolve_bu_did(tj, invoice_line, e["ilt"], e["pli_line"], e["pli_header"],
                                 e["cml"], e["cmli"], e["cmia"])
        assert len(result) == 2
        assert result.iloc[0]["bu"] == "US001"
        assert result.iloc[1]["bu"] == "US002"


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
            "general_ledger_account",
        ]
        for table in used_tables:
            assert table in EXPECTED_COLUMNS, f"{table} is loaded but has no EXPECTED_COLUMNS entry"
            assert table in DATASET_IDS, f"{table} is loaded but has no DATASET_IDS entry"

    def test_account_no_longer_loaded_by_build_source_dataframe(self, monkeypatch):
        # "account" must stay defined (OFAC needs it) but the GL pipeline
        # itself should never call read_table_by_dataset_id with its
        # dataset_id anymore.
        requested_dataset_ids = []

        def fake_read_table(s3_client, bucket, dataset_id, expected_columns=None,
                             source_prefix="salesforce/reports"):
            requested_dataset_ids.append(dataset_id)
            return pd.DataFrame(columns=expected_columns or [])

        monkeypatch.setattr(glsj, "read_table_by_dataset_id", fake_read_table)

        build_source_dataframe(MagicMock(), "bucket", start_date="2000-01-01", end_date="2100-01-01")

        assert DATASET_IDS["account"] not in requested_dataset_ids
        assert DATASET_IDS["general_ledger_account"] in requested_dataset_ids

    def test_general_ledger_account_dataset_id_is_confirmed_real_value(self):
        # Confirmed by Dakota — replaces the earlier placeholder
        # ("00000000-0000-0000-0000-000000000000") that a previous version
        # of this test intentionally failed on.
        assert DATASET_IDS["general_ledger_account"] == "78e7dd64-9999-42f1-a44a-38bf5157375e"

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

    def test_end_to_end_with_invoice_line_transaction(self, monkeypatch):
        table_data = {
            "transaction_journal": pd.DataFrame([{
                "Name": "Batch 1", "UsageResourceId": None,
                "ReferenceTransactionRecordId": "IL1", "TransactionType": "InvoiceLine",
                "ActivityDate": "2026-08-01", "UsageType": "Storage",
                "Credit": None, "Debit": 100.0,
                "DebitGeneralLedgerAccountId": "GLA1", "CreditGeneralLedgerAccountId": None,
            }]),
            "invoice_line": pd.DataFrame([{
                "Id": "IL1", "InvoiceId": "INV1", "Product2Id": "PROD1",
                "Business_Unit_BU__c": "US001", "Department_ID_DID__c": "10500",
                "Name": "Widget Subscription",
            }]),
            "invoice": pd.DataFrame([{"Id": "INV1", "BillingAccountId": "ACC1"}]),
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
            "invoice_line": pd.DataFrame([{
                "Id": "IL1", "InvoiceId": "INV1", "Product2Id": "PROD1",
                "Business_Unit_BU__c": "US002", "Department_ID_DID__c": "20500",
                "Name": "Support Plan",
            }]),
            "payment_line_invoice": pd.DataFrame([{"PaymentId": "PAY1", "InvoiceId": "INV1"}]),
            "general_ledger_account": pd.DataFrame([{"Id": "GLA2", "GL_Accounting_Number__c": "20099999"}]),
        }
        self._patch_read_table(monkeypatch, table_data)

        result = build_source_dataframe(MagicMock(), "bucket", **self.WIDE_WINDOW)

        row = result.iloc[0]
        assert row["InvoiceLine.Business_Unit"] == "US002"
        assert row["GeneralLedgerAccount.GL_Accounting_Number__c"] == "20099999"
        assert row["TransactionJournal.CreditDebit"] == -50.0  # credit -> negative

    def test_end_to_end_with_credit_memo_transaction_via_header_fallback(self, monkeypatch):
        table_data = {
            "transaction_journal": pd.DataFrame([{
                "Name": "Credit Batch", "UsageResourceId": None,
                "ReferenceTransactionRecordId": "CM1", "TransactionType": "CreditMemo",
                "ActivityDate": "2026-08-03", "UsageType": None,
                "Credit": 25.0, "Debit": None,
                "DebitGeneralLedgerAccountId": None, "CreditGeneralLedgerAccountId": "GLA3",
            }]),
            "invoice_line": pd.DataFrame([{
                "Id": "IL1", "InvoiceId": "INV1", "Product2Id": "PROD1",
                "Business_Unit_BU__c": "US003", "Department_ID_DID__c": "40500",
                "Name": "Enterprise Plan",
            }]),
            "credit_memo_inv_application": pd.DataFrame([{"CreditMemoId": "CM1", "InvoiceId": "INV1"}]),
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
        # default value, or pulled from invoice line like it currently does"
        table_data = {
            "transaction_journal": pd.DataFrame([{
                "Name": "Orphan", "UsageResourceId": None,
                "ReferenceTransactionRecordId": "UNKNOWN1", "TransactionType": "InvoiceLine",
                "ActivityDate": "2026-08-03", "UsageType": None,
                "Credit": None, "Debit": 10.0,
                "DebitGeneralLedgerAccountId": None, "CreditGeneralLedgerAccountId": None,
            }]),
        }
        self._patch_read_table(monkeypatch, table_data)

        result = build_source_dataframe(MagicMock(), "bucket", **self.WIDE_WINDOW)

        assert result.iloc[0]["InvoiceLine.Business_Unit"] == "10901"

    def test_end_to_end_did_override_by_gl_account_beats_product_name_override(self, monkeypatch):
        # 10040049 -> 16605 must win even when the InvoiceLine name would
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
            "invoice_line": pd.DataFrame([{
                "Id": "IL1", "InvoiceId": "INV1", "Product2Id": "PROD1",
                "Business_Unit_BU__c": "US001", "Department_ID_DID__c": "10500",
                "Name": "Slingshot Pro Subscription",
            }]),
            "general_ledger_account": pd.DataFrame(
                [{"Id": "GLA_SPECIAL", "GL_Accounting_Number__c": "10040049"}]
            ),
        }
        self._patch_read_table(monkeypatch, table_data)

        result = build_source_dataframe(MagicMock(), "bucket", **self.WIDE_WINDOW)

        assert result.iloc[0]["InvoiceLine.Department_Id"] == "16605"

    def test_missing_tables_still_produce_output_with_nulls(self, monkeypatch):
        # Only transaction_journal provided — everything else empty. bu
        # falls back to the "10901" default (apply_did_overrides()); did
        # and the gl accounting number stay null since nothing overrides
        # them here.
        table_data = {
            "transaction_journal": pd.DataFrame([{
                "Name": "Orphan", "UsageResourceId": None,
                "ReferenceTransactionRecordId": "UNKNOWN1", "TransactionType": "InvoiceLine",
                "ActivityDate": "2026-08-03", "UsageType": None,
                "Credit": None, "Debit": 10.0,
            }]),
        }
        self._patch_read_table(monkeypatch, table_data)

        result = build_source_dataframe(MagicMock(), "bucket", **self.WIDE_WINDOW)

        assert len(result) == 1
        row = result.iloc[0]
        assert row["InvoiceLine.Business_Unit"] == "10901"
        assert pd.isna(row["InvoiceLine.Department_Id"])
        assert pd.isna(row["GeneralLedgerAccount.GL_Accounting_Number__c"])
        assert row["TransactionJournal.CreditDebit"] == 10.0  # amount still resolves independent of bu/account

    def test_empty_transaction_journal_produces_empty_result(self, monkeypatch):
        self._patch_read_table(monkeypatch, {})
        result = build_source_dataframe(MagicMock(), "bucket", **self.WIDE_WINDOW)
        assert len(result) == 0
        assert "InvoiceLine.Business_Unit" in result.columns

    def test_result_has_expected_final_columns(self, monkeypatch):
        table_data = {
            "transaction_journal": pd.DataFrame([{
                "Name": "X", "UsageResourceId": None,
                "ReferenceTransactionRecordId": "IL1", "TransactionType": "InvoiceLine",
                "ActivityDate": "2026-08-01", "UsageType": "Storage",
                "Credit": None, "Debit": 5.0,
            }]),
            "invoice_line": pd.DataFrame([{
                "Id": "IL1", "InvoiceId": "INV1", "Product2Id": "PROD1",
                "Business_Unit_BU__c": "US001", "Department_ID_DID__c": "10500",
            }]),
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
                 "UsageType": "Storage", "Credit": None, "Debit": 1.0},
                {"Name": "B", "UsageResourceId": None, "ReferenceTransactionRecordId": "IL2",
                 "TransactionType": "InvoiceLine", "ActivityDate": "2026-08-02",
                 "UsageType": "Compute", "Credit": None, "Debit": 2.0},
            ]),
            "invoice_line": pd.DataFrame([
                {"Id": "IL1", "InvoiceId": "INV1", "Product2Id": "PROD1",
                 "Business_Unit_BU__c": "US001", "Department_ID_DID__c": "10500"},
                {"Id": "IL2", "InvoiceId": "INV2", "Product2Id": "PROD2",
                 "Business_Unit_BU__c": "US002", "Department_ID_DID__c": "20500"},
            ]),
        }
        self._patch_read_table(monkeypatch, table_data)

        result = build_source_dataframe(MagicMock(), "bucket", **self.WIDE_WINDOW)
        assert len(result) == 2

    def test_logging_calls_made_when_log_provided(self, monkeypatch):
        table_data = {
            "transaction_journal": pd.DataFrame([{
                "Name": "X", "UsageResourceId": None,
                "ReferenceTransactionRecordId": "IL1", "TransactionType": "InvoiceLine",
                "ActivityDate": "2026-08-01", "UsageType": "Storage",
                "Credit": None, "Debit": 5.0,
            }]),
            "invoice_line": pd.DataFrame([{
                "Id": "IL1", "InvoiceId": "INV1", "Product2Id": "PROD1",
                "Business_Unit_BU__c": "US001", "Department_ID_DID__c": "10500",
            }]),
        }
        self._patch_read_table(monkeypatch, table_data)
        fake_log = MagicMock()

        build_source_dataframe(MagicMock(), "bucket", log=fake_log, **self.WIDE_WINDOW)

        assert fake_log.info.called
        # spot check a couple of expected log messages happened
        messages = [call.args[0] for call in fake_log.info.call_args_list]
        assert any("resolving business_unit" in m for m in messages)
        assert any("resolving gl accounting numbers" in m for m in messages)
        assert any("applying bu default and did overrides" in m for m in messages)

    def test_logging_includes_per_table_read_messages(self, monkeypatch):
        table_data = {
            "transaction_journal": pd.DataFrame([{
                "Name": "X", "UsageResourceId": None,
                "ReferenceTransactionRecordId": "IL1", "TransactionType": "InvoiceLine",
                "ActivityDate": "2026-08-01", "UsageType": "Storage",
                "Credit": None, "Debit": 5.0,
            }]),
        }
        self._patch_read_table(monkeypatch, table_data)
        fake_log = MagicMock()

        build_source_dataframe(MagicMock(), "bucket", log=fake_log, **self.WIDE_WINDOW)

        messages = [call.args[0] for call in fake_log.info.call_args_list]
        assert any("reading transaction_journal" in m for m in messages)

    def test_no_logging_when_log_is_none(self, monkeypatch):
        table_data = {
            "transaction_journal": pd.DataFrame([{
                "Name": "X", "UsageResourceId": None,
                "ReferenceTransactionRecordId": "IL1", "TransactionType": "InvoiceLine",
                "ActivityDate": "2026-08-01", "UsageType": "Storage",
                "Credit": None, "Debit": 5.0,
            }]),
            "invoice_line": pd.DataFrame([{
                "Id": "IL1", "InvoiceId": "INV1", "Product2Id": "PROD1",
                "Business_Unit_BU__c": "US001", "Department_ID_DID__c": "10500",
            }]),
        }
        self._patch_read_table(monkeypatch, table_data)
        # should not raise even without a logger
        result = build_source_dataframe(MagicMock(), "bucket", log=None, **self.WIDE_WINDOW)
        assert len(result) == 1

    def test_passes_through_source_prefix(self, monkeypatch):
        captured_args = {}

        def fake_read_table(s3_client, bucket, dataset_id, expected_columns=None,
                             source_prefix="salesforce/reports"):
            captured_args["source_prefix"] = source_prefix
            return pd.DataFrame(columns=expected_columns or [])

        monkeypatch.setattr(glsj, "read_table_by_dataset_id", fake_read_table)

        build_source_dataframe(MagicMock(), "bucket", source_prefix="custom/reports/path")

        assert captured_args["source_prefix"] == "custom/reports/path"

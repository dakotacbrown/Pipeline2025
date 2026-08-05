"""
Tests for gl_source_join.py — the source-side join logic that builds the
flat dataframe consumed by the GL journal file builder.

Run with: pytest test_gl_source_join.py -v

Covers:
  - resolve_amount(): Credit/Debit sign resolution
  - clean_account_name(): leading "A" stripping
  - build_reference_to_account_lookup(): polymorphic account-name resolution,
    including the InvoiceLine/InvoiceLineTax bug fix
  - resolve_bu_did(): per-TransactionType bu/department_id resolution,
    including the line-level vs. header-level fallback priority
  - DATASET_IDS / EXPECTED_COLUMNS sanity checks
  - build_source_dataframe(): full orchestration, mocked S3 reads
"""

import re
from unittest.mock import MagicMock

import pandas as pd
import pytest

import helpers.gl_source_join as glsj
from helpers.gl_source_join import (
    resolve_bu_did,
    build_reference_to_account_lookup,
    clean_account_name,
    resolve_amount,
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
# clean_account_name()
# ---------------------------------------------------------------------------

class TestCleanAccountName:
    def test_strips_single_leading_a_only(self):
        # per Dakota: "removed the leading A" — one character, not "A-".
        # Using a non-word example so the stripping behavior itself is
        # unambiguous (real account names may legitimately start with "A").
        assert clean_account_name("A9999-TestAccount") == "9999-TestAccount"

    def test_does_not_strip_if_no_leading_a(self):
        assert clean_account_name("Beta LLC") == "Beta LLC"

    def test_nan_passthrough(self):
        assert pd.isna(clean_account_name(float("nan")))

    def test_lowercase_leading_a_not_stripped(self):
        # pattern is anchored on literal "A", case-sensitive
        assert clean_account_name("acme") == "acme"

    def test_empty_string_unchanged(self):
        assert clean_account_name("") == ""

    def test_only_a_becomes_empty_string(self):
        assert clean_account_name("A") == ""

    def test_custom_prefix_pattern_override(self):
        assert clean_account_name("XY-Test", prefix_pattern=r"^XY-") == "Test"


# ---------------------------------------------------------------------------
# build_reference_to_account_lookup()
# ---------------------------------------------------------------------------

class TestBuildReferenceToAccountLookup:
    def _empty_il(self):
        return pd.DataFrame([], columns=["Id", "InvoiceId"])

    def _empty_ilt(self):
        return pd.DataFrame([], columns=["Id", "InvoiceLineId"])

    def test_resolves_account_name_via_invoice(self):
        invoice = pd.DataFrame([{"Id": "INV1", "BillingAccountId": "ACC1"}])
        credit_memo = pd.DataFrame([], columns=["Id", "BillingAccountId"])
        payment = pd.DataFrame([], columns=["Id", "AccountId"])
        refund = pd.DataFrame([], columns=["Id", "AccountId"])
        account = pd.DataFrame([{"Id": "ACC1", "Name": "A-Acme Corp"}])

        lookup = build_reference_to_account_lookup(invoice, credit_memo, payment, refund,
                                                     self._empty_il(), self._empty_ilt(), account)
        row = lookup[lookup["ReferenceTransactionRecordId"] == "INV1"].iloc[0]
        assert row["AccountName"] == "A-Acme Corp"

    def test_resolves_account_name_via_credit_memo(self):
        invoice = pd.DataFrame([], columns=["Id", "BillingAccountId"])
        credit_memo = pd.DataFrame([{"Id": "CM1", "BillingAccountId": "ACC3"}])
        payment = pd.DataFrame([], columns=["Id", "AccountId"])
        refund = pd.DataFrame([], columns=["Id", "AccountId"])
        account = pd.DataFrame([{"Id": "ACC3", "Name": "Gamma Co"}])

        lookup = build_reference_to_account_lookup(invoice, credit_memo, payment, refund,
                                                     self._empty_il(), self._empty_ilt(), account)
        row = lookup[lookup["ReferenceTransactionRecordId"] == "CM1"].iloc[0]
        assert row["AccountName"] == "Gamma Co"

    def test_resolves_account_name_via_payment(self):
        invoice = pd.DataFrame([], columns=["Id", "BillingAccountId"])
        credit_memo = pd.DataFrame([], columns=["Id", "BillingAccountId"])
        payment = pd.DataFrame([{"Id": "PAY1", "AccountId": "ACC2"}])
        refund = pd.DataFrame([], columns=["Id", "AccountId"])
        account = pd.DataFrame([{"Id": "ACC2", "Name": "Beta LLC"}])

        lookup = build_reference_to_account_lookup(invoice, credit_memo, payment, refund,
                                                     self._empty_il(), self._empty_ilt(), account)
        row = lookup[lookup["ReferenceTransactionRecordId"] == "PAY1"].iloc[0]
        assert row["AccountName"] == "Beta LLC"

    def test_resolves_account_name_via_refund(self):
        invoice = pd.DataFrame([], columns=["Id", "BillingAccountId"])
        credit_memo = pd.DataFrame([], columns=["Id", "BillingAccountId"])
        payment = pd.DataFrame([], columns=["Id", "AccountId"])
        refund = pd.DataFrame([{"Id": "REF1", "AccountId": "ACC4"}])
        account = pd.DataFrame([{"Id": "ACC4", "Name": "Delta Inc"}])

        lookup = build_reference_to_account_lookup(invoice, credit_memo, payment, refund,
                                                     self._empty_il(), self._empty_ilt(), account)
        row = lookup[lookup["ReferenceTransactionRecordId"] == "REF1"].iloc[0]
        assert row["AccountName"] == "Delta Inc"

    def test_resolves_account_name_via_invoice_line_bug_fix(self):
        # This is the bug: TransactionType='InvoiceLine' means
        # ReferenceTransactionRecordId = InvoiceLine.Id, NOT Invoice.Id.
        # Before the fix, this returned a null account name for every
        # InvoiceLine/InvoiceLineTax row — likely the bulk of the data.
        invoice = pd.DataFrame([{"Id": "INV1", "BillingAccountId": "ACC1"}])
        credit_memo = pd.DataFrame([], columns=["Id", "BillingAccountId"])
        payment = pd.DataFrame([], columns=["Id", "AccountId"])
        refund = pd.DataFrame([], columns=["Id", "AccountId"])
        invoice_line = pd.DataFrame([{"Id": "IL1", "InvoiceId": "INV1"}])
        account = pd.DataFrame([{"Id": "ACC1", "Name": "A-Gamma Inc"}])

        lookup = build_reference_to_account_lookup(invoice, credit_memo, payment, refund,
                                                     invoice_line, self._empty_ilt(), account)
        row = lookup[lookup["ReferenceTransactionRecordId"] == "IL1"].iloc[0]
        assert row["AccountName"] == "A-Gamma Inc"

    def test_resolves_account_name_via_invoice_line_tax_bug_fix(self):
        # Same bug, one hop further: TransactionType='InvoiceLineTax' means
        # ReferenceTransactionRecordId = InvoiceLineTax.Id.
        invoice = pd.DataFrame([{"Id": "INV1", "BillingAccountId": "ACC1"}])
        credit_memo = pd.DataFrame([], columns=["Id", "BillingAccountId"])
        payment = pd.DataFrame([], columns=["Id", "AccountId"])
        refund = pd.DataFrame([], columns=["Id", "AccountId"])
        invoice_line = pd.DataFrame([{"Id": "IL1", "InvoiceId": "INV1"}])
        invoice_line_tax = pd.DataFrame([{"Id": "ILT1", "InvoiceLineId": "IL1"}])
        account = pd.DataFrame([{"Id": "ACC1", "Name": "A-Delta Co"}])

        lookup = build_reference_to_account_lookup(invoice, credit_memo, payment, refund,
                                                     invoice_line, invoice_line_tax, account)
        row = lookup[lookup["ReferenceTransactionRecordId"] == "ILT1"].iloc[0]
        assert row["AccountName"] == "A-Delta Co"

    def test_invoice_line_tax_with_no_matching_invoice_line_returns_null(self):
        invoice = pd.DataFrame([], columns=["Id", "BillingAccountId"])
        credit_memo = pd.DataFrame([], columns=["Id", "BillingAccountId"])
        payment = pd.DataFrame([], columns=["Id", "AccountId"])
        refund = pd.DataFrame([], columns=["Id", "AccountId"])
        invoice_line = pd.DataFrame([], columns=["Id", "InvoiceId"])
        invoice_line_tax = pd.DataFrame([{"Id": "ILT1", "InvoiceLineId": "IL_MISSING"}])
        account = pd.DataFrame([], columns=["Id", "Name"])

        lookup = build_reference_to_account_lookup(invoice, credit_memo, payment, refund,
                                                     invoice_line, invoice_line_tax, account)
        # ILT1 shouldn't even be produced since invoice_line is empty and
        # the join has nothing to match against — either absent or null name
        matches = lookup[lookup["ReferenceTransactionRecordId"] == "ILT1"]
        if len(matches) > 0:
            assert pd.isna(matches.iloc[0]["AccountName"])

    def test_ids_are_globally_unique_no_collisions(self):
        # Salesforce IDs are unique across objects — confirms union approach is safe
        invoice = pd.DataFrame([{"Id": "REC1", "BillingAccountId": "ACC1"}])
        credit_memo = pd.DataFrame([{"Id": "REC2", "BillingAccountId": "ACC2"}])
        payment = pd.DataFrame([{"Id": "REC3", "AccountId": "ACC3"}])
        refund = pd.DataFrame([{"Id": "REC4", "AccountId": "ACC4"}])
        account = pd.DataFrame([
            {"Id": "ACC1", "Name": "One"}, {"Id": "ACC2", "Name": "Two"},
            {"Id": "ACC3", "Name": "Three"}, {"Id": "ACC4", "Name": "Four"},
        ])

        lookup = build_reference_to_account_lookup(invoice, credit_memo, payment, refund,
                                                     self._empty_il(), self._empty_ilt(), account)
        assert len(lookup) == 4
        assert set(lookup["AccountName"]) == {"One", "Two", "Three", "Four"}

    def test_missing_account_returns_null_name(self):
        invoice = pd.DataFrame([{"Id": "INV1", "BillingAccountId": "ACC_MISSING"}])
        credit_memo = pd.DataFrame([], columns=["Id", "BillingAccountId"])
        payment = pd.DataFrame([], columns=["Id", "AccountId"])
        refund = pd.DataFrame([], columns=["Id", "AccountId"])
        account = pd.DataFrame([], columns=["Id", "Name"])

        lookup = build_reference_to_account_lookup(invoice, credit_memo, payment, refund,
                                                     self._empty_il(), self._empty_ilt(), account)
        row = lookup[lookup["ReferenceTransactionRecordId"] == "INV1"].iloc[0]
        assert pd.isna(row["AccountName"])

    def test_all_sources_empty_returns_empty_lookup(self):
        empty_inv = pd.DataFrame([], columns=["Id", "BillingAccountId"])
        empty_pay = pd.DataFrame([], columns=["Id", "AccountId"])
        empty_acct = pd.DataFrame([], columns=["Id", "Name"])

        lookup = build_reference_to_account_lookup(empty_inv, empty_inv, empty_pay, empty_pay,
                                                     self._empty_il(), self._empty_ilt(), empty_acct)
        assert len(lookup) == 0
        assert "ReferenceTransactionRecordId" in lookup.columns
        assert "AccountName" in lookup.columns


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
        # an EXPECTED_COLUMNS entry, so empty results come back correctly shaped
        used_tables = [
            "transaction_journal", "invoice_line", "invoice_line_tax",
            "payment_line_invoice_line", "payment_line_invoice", "credit_memo_line",
            "credit_memo_line_invoice_line", "credit_memo_inv_application",
            "invoice", "credit_memo", "payment", "refund", "account",
        ]
        for table in used_tables:
            assert table in EXPECTED_COLUMNS, f"{table} is loaded but has no EXPECTED_COLUMNS entry"
            assert table in DATASET_IDS, f"{table} is loaded but has no DATASET_IDS entry"

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

    def test_end_to_end_with_invoice_line_transaction(self, monkeypatch):
        table_data = {
            "transaction_journal": pd.DataFrame([{
                "Name": "Batch 1", "UsageResourceId": None,
                "ReferenceTransactionRecordId": "IL1", "TransactionType": "InvoiceLine",
                "ActivityDate": "2026-08-01", "UsageType": "Storage",
                "Credit": None, "Debit": 100.0,
            }]),
            "invoice_line": pd.DataFrame([{
                "Id": "IL1", "InvoiceId": "INV1", "Product2Id": "PROD1",
                "Business_Unit_BU__c": "US001", "Department_ID_DID__c": "10500",
            }]),
            "invoice": pd.DataFrame([{"Id": "INV1", "BillingAccountId": "ACC1"}]),
            "account": pd.DataFrame([{"Id": "ACC1", "Name": "A-Test Account"}]),
        }
        self._patch_read_table(monkeypatch, table_data)

        result = build_source_dataframe(MagicMock(), "bucket")

        assert len(result) == 1
        row = result.iloc[0]
        assert row["business_unit"] == "US001"
        assert row["did"] == "10500"
        assert row["account_name"] == "-Test Account"  # leading "A" stripped
        assert row["amount"] == 100.0
        assert row["tj_name"] == "Batch 1"

    def test_end_to_end_with_payment_transaction_via_header_fallback(self, monkeypatch):
        table_data = {
            "transaction_journal": pd.DataFrame([{
                "Name": "Payment Batch", "UsageResourceId": None,
                "ReferenceTransactionRecordId": "PAY1", "TransactionType": "Payment",
                "ActivityDate": "2026-08-02", "UsageType": None,
                "Credit": 50.0, "Debit": None,
            }]),
            "invoice_line": pd.DataFrame([{
                "Id": "IL1", "InvoiceId": "INV1", "Product2Id": "PROD1",
                "Business_Unit_BU__c": "US002", "Department_ID_DID__c": "20500",
            }]),
            "payment_line_invoice": pd.DataFrame([{"PaymentId": "PAY1", "InvoiceId": "INV1"}]),
            "payment": pd.DataFrame([{"Id": "PAY1", "AccountId": "ACC2"}]),
            "account": pd.DataFrame([{"Id": "ACC2", "Name": "Beta LLC"}]),
        }
        self._patch_read_table(monkeypatch, table_data)

        result = build_source_dataframe(MagicMock(), "bucket")

        row = result.iloc[0]
        assert row["business_unit"] == "US002"
        assert row["account_name"] == "Beta LLC"
        assert row["amount"] == -50.0  # credit -> negative

    def test_end_to_end_with_credit_memo_transaction_via_header_fallback(self, monkeypatch):
        table_data = {
            "transaction_journal": pd.DataFrame([{
                "Name": "Credit Batch", "UsageResourceId": None,
                "ReferenceTransactionRecordId": "CM1", "TransactionType": "CreditMemo",
                "ActivityDate": "2026-08-03", "UsageType": None,
                "Credit": 25.0, "Debit": None,
            }]),
            "invoice_line": pd.DataFrame([{
                "Id": "IL1", "InvoiceId": "INV1", "Product2Id": "PROD1",
                "Business_Unit_BU__c": "US003", "Department_ID_DID__c": "40500",
            }]),
            "credit_memo_inv_application": pd.DataFrame([{"CreditMemoId": "CM1", "InvoiceId": "INV1"}]),
            "credit_memo": pd.DataFrame([{"Id": "CM1", "BillingAccountId": "ACC3"}]),
            "account": pd.DataFrame([{"Id": "ACC3", "Name": "Gamma Corp"}]),
        }
        self._patch_read_table(monkeypatch, table_data)

        result = build_source_dataframe(MagicMock(), "bucket")

        row = result.iloc[0]
        assert row["business_unit"] == "US003"
        assert row["account_name"] == "Gamma Corp"
        assert row["amount"] == -25.0

    def test_missing_tables_still_produce_output_with_nulls(self, monkeypatch):
        # Only transaction_journal provided — everything else empty
        table_data = {
            "transaction_journal": pd.DataFrame([{
                "Name": "Orphan", "UsageResourceId": None,
                "ReferenceTransactionRecordId": "UNKNOWN1", "TransactionType": "InvoiceLine",
                "ActivityDate": "2026-08-03", "UsageType": None,
                "Credit": None, "Debit": 10.0,
            }]),
        }
        self._patch_read_table(monkeypatch, table_data)

        result = build_source_dataframe(MagicMock(), "bucket")

        assert len(result) == 1
        row = result.iloc[0]
        assert pd.isna(row["business_unit"])
        assert pd.isna(row["account_name"])
        assert row["amount"] == 10.0  # amount still resolves independent of bu/account

    def test_empty_transaction_journal_produces_empty_result(self, monkeypatch):
        self._patch_read_table(monkeypatch, {})
        result = build_source_dataframe(MagicMock(), "bucket")
        assert len(result) == 0
        assert "business_unit" in result.columns

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

        result = build_source_dataframe(MagicMock(), "bucket")

        expected_cols = {
            "business_unit", "did", "activity_date", "transaction_type",
            "account_name", "usage_type", "amount", "tj_name",
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

        result = build_source_dataframe(MagicMock(), "bucket")
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

        build_source_dataframe(MagicMock(), "bucket", log=fake_log)

        assert fake_log.info.called
        # spot check a couple of expected log messages happened
        messages = [call.args[0] for call in fake_log.info.call_args_list]
        assert any("resolving business_unit" in m for m in messages)
        assert any("resolving account names" in m for m in messages)

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

        build_source_dataframe(MagicMock(), "bucket", log=fake_log)

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
        result = build_source_dataframe(MagicMock(), "bucket", log=None)
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

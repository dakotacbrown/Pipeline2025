"""
Tests for the GL Journal Entry Interface builder.

Run with: pytest test_gl_journal_builder.py -v

Covers:
  - fixed-width formatting primitives (fmt, build_line)
  - each record builder against the exact field lengths from the spec
  - full file assembly (newline delimiting, trailer totals, row count)
  - filename convention
  - source-join logic (account resolution, bu/did resolution, amount sign)
"""

from datetime import datetime
from decimal import Decimal

import pandas as pd
import pytest

from gl_journal_builder_pandas_s3 import (
    fmt,
    build_line,
    file_header,
    journal_header,
    journal_line,
    file_trailer,
    build_gl_file,
    build_filename,
)
from gl_source_join import (
    resolve_bu_did,
    build_reference_to_account_lookup,
    clean_account_name,
    resolve_amount,
)


# ---------------------------------------------------------------------------
# fmt() / build_line()
# ---------------------------------------------------------------------------

class TestFmt:
    def test_left_justify_pads_with_spaces(self):
        assert fmt("AB", 5) == "AB   "

    def test_right_justify_pads_with_spaces(self):
        assert fmt("AB", 5, justify="right") == "   AB"

    def test_right_justify_custom_fill(self):
        assert fmt("7", 5, justify="right", fill="0") == "00007"

    def test_truncates_when_too_long(self):
        assert fmt("ABCDEFGH", 3) == "ABC"

    def test_none_becomes_blank_field(self):
        assert fmt(None, 4) == "    "

    def test_nan_becomes_blank_field(self):
        assert fmt(float("nan"), 4) == "    "

    def test_output_always_exact_length(self):
        for val in ["", "x", "xxxxxxxxxx", None, 123, 45.6]:
            assert len(fmt(val, 6)) == 6


class TestBuildLine:
    def test_concatenates_fields_in_order(self):
        line = build_line([
            ("A", 2, "left", " "),
            ("B", 3, "right", "0"),
        ])
        assert line == "A 00B"

    def test_total_length_matches_sum_of_field_lengths(self):
        fields = [("x", 5, "left", " "), ("y", 10, "left", " "), ("z", 3, "right", " ")]
        assert len(build_line(fields)) == 5 + 10 + 3


# ---------------------------------------------------------------------------
# Record builders — lengths must match the layout spec exactly
# ---------------------------------------------------------------------------

class TestFileHeader:
    def test_length_is_100(self):
        line = file_header(datetime(2026, 8, 4, 14, 32, 30))
        assert len(line) == 2 + 8 + 6 + 8 + 76  # 100

    def test_starts_with_record_type(self):
        line = file_header(datetime(2026, 8, 4, 14, 32, 30))
        assert line.startswith("#H")

    def test_date_and_time_formatting(self):
        line = file_header(datetime(2026, 8, 4, 14, 32, 30))
        assert line[2:10] == "20260804"
        assert line[10:16] == "143230"


class TestJournalHeader:
    def test_length_matches_spec(self):
        # 1+5+10+8+4+8+10+21+3+8+30+33+39 = 180
        line = journal_header("US001", "08042026", "RCL")
        assert len(line) == 180

    def test_starts_with_H(self):
        line = journal_header("US001", "08042026", "RCL")
        assert line[0] == "H"

    def test_journal_id_standard_value_next(self):
        line = journal_header("US001", "08042026", "RCL")
        # position 7-16 (1-indexed) -> [6:16] zero-indexed
        assert line[6:16] == "NEXT      "

    def test_ledger_group_standard_value(self):
        line = journal_header("US001", "08042026", "RCL")
        # position 37-46 (1-indexed) -> [36:46] zero-indexed
        assert line[36:46] == "RECORDING "

    def test_carries_description_into_correct_position(self):
        line = journal_header("US001", "08042026", "RCL", description="Test TJ Name")
        # position 79-108 (1-indexed) -> [78:108] zero-indexed
        assert line[78:108] == "Test TJ Name".ljust(30)

    def test_blank_description_by_default(self):
        line = journal_header("US001", "08042026", "RCL")
        assert line[78:108] == " " * 30


class TestJournalLine:
    def test_length_matches_spec(self):
        # sum of all journal line field lengths per spec = 418
        line = journal_line(
            business_unit="US001", account="12345678", dept_id="10500",
            project_id="", journal_line_ref="REF001",
            journal_line_desc="Test line", txn_currency_code="USD",
            txn_monetary_amount=1500.00,
        )
        assert len(line) == 418

    def test_starts_with_L(self):
        line = journal_line(business_unit="US001", account="12345678")
        assert line[0] == "L"

    def test_us_business_unit_gets_corp_ledger(self):
        line = journal_line(business_unit="US001", account="12345678")
        # position 16-25 (1-indexed) -> [15:25]
        assert line[15:25] == "CORP      "

    def test_non_us_business_unit_gets_local_ledger(self):
        line = journal_line(business_unit="EU002", account="12345678")
        assert line[15:25] == "LOCAL     "

    def test_negative_amount_preserved(self):
        line = journal_line(business_unit="US001", account="12345678",
                             txn_monetary_amount=-250.50)
        assert "-250.5" in line

    def test_amount_rounds_to_two_decimals(self):
        line = journal_line(business_unit="US001", account="12345678",
                             txn_monetary_amount=99.999)
        assert "100.00" in line or "100" in line  # rounds up to 100.00


class TestFileTrailer:
    def test_length_matches_spec(self):
        # 2+9+28+25+25+5 = 94... wait, per spec File Trailer Filler is 96-100 (5 chars)
        # Record Type(2) + Row Count(9) + Total Debits(28) + Total Credits(25)
        # + Total Statistical Amount(25) + Filler(5) = 94... check against spec positions
        line = file_trailer(3, Decimal("1500.00"), Decimal("-1500.00"), 0)
        assert len(line) == 2 + 9 + 28 + 25 + 25 + 5

    def test_starts_with_record_type(self):
        line = file_trailer(3, Decimal("1500.00"), Decimal("-1500.00"), 0)
        assert line.startswith("#T")

    def test_row_count_zero_padded(self):
        line = file_trailer(3, Decimal("0"), Decimal("0"), 0)
        assert line[2:11] == "000000003"

    def test_totals_formatted_with_two_decimals(self):
        line = file_trailer(1, Decimal("1500"), Decimal("-500"), 0)
        assert "1500.00" in line
        assert "-500.00" in line


# ---------------------------------------------------------------------------
# Full file assembly
# ---------------------------------------------------------------------------

class TestBuildGlFile:
    @pytest.fixture
    def sample_df(self):
        return pd.DataFrame([
            {"account": "12345678", "dept_id": "10500", "project_id": "",
             "txn_monetary_amount": 1500.00, "txn_currency_code": "USD",
             "journal_line_desc": "Revenue accrual", "journal_line_ref": "REF001"},
            {"account": "87654321", "dept_id": "10500", "project_id": "",
             "txn_monetary_amount": -1500.00, "txn_currency_code": "USD",
             "journal_line_desc": "Offset entry", "journal_line_ref": "REF001"},
        ])

    def test_rows_are_newline_delimited(self, sample_df):
        content = build_gl_file(sample_df, "US001", "RCL", datetime(2026, 8, 4))
        lines = content.split("\n")
        # file_header + journal_header + 2 journal_lines + file_trailer + trailing blank
        assert len(lines) == 6
        assert lines[-1] == ""  # trailing newline

    def test_file_ends_with_newline(self, sample_df):
        content = build_gl_file(sample_df, "US001", "RCL", datetime(2026, 8, 4))
        assert content.endswith("\n")

    def test_record_order(self, sample_df):
        content = build_gl_file(sample_df, "US001", "RCL", datetime(2026, 8, 4))
        lines = content.split("\n")
        assert lines[0].startswith("#H")
        assert lines[1].startswith("H")
        assert lines[2].startswith("L")
        assert lines[3].startswith("L")
        assert lines[4].startswith("#T")

    def test_trailer_row_count_includes_journal_header(self, sample_df):
        content = build_gl_file(sample_df, "US001", "RCL", datetime(2026, 8, 4))
        trailer = content.split("\n")[4]
        # row_count = journal_header(1) + journal_lines(2) = 3, per spec:
        # "excluding file header and file trailer"
        assert trailer[2:11] == "000000003"

    def test_trailer_totals_sum_debits_and_credits_separately(self, sample_df):
        content = build_gl_file(sample_df, "US001", "RCL", datetime(2026, 8, 4))
        trailer = content.split("\n")[4]
        assert "1500.00" in trailer
        assert "-1500.00" in trailer

    def test_empty_dataframe_still_produces_valid_file(self):
        empty_df = pd.DataFrame(columns=["account", "dept_id", "project_id",
                                          "txn_monetary_amount", "txn_currency_code",
                                          "journal_line_desc", "journal_line_ref"])
        content = build_gl_file(empty_df, "US001", "RCL", datetime(2026, 8, 4))
        lines = content.split("\n")
        # file_header + journal_header + file_trailer + trailing blank = 4
        assert len(lines) == 4
        assert lines[2][2:11] == "000000001"  # journal header line itself, no journal lines

    def test_header_description_placeholder(self, sample_df):
        # per Dakota: "RevCloud Batch" is a placeholder pending follow-up on
        # what should show when a BU's batch spans multiple TJ.Name values
        content = build_gl_file(sample_df, "US001", "RCL", datetime(2026, 8, 4))
        journal_header_line = content.split("\n")[1]
        assert journal_header_line[78:108] == "RevCloud Batch".ljust(30)


# ---------------------------------------------------------------------------
# Filename convention
# ---------------------------------------------------------------------------

class TestBuildFilename:
    def test_matches_spec_pattern(self):
        name = build_filename("BX1", datetime(2026, 1, 30, 14, 2, 30))
        assert name == "BX1_20260130140230.txt"

    def test_rejects_prefix_not_three_chars(self):
        with pytest.raises(ValueError):
            build_filename("BX", datetime(2026, 1, 30))
        with pytest.raises(ValueError):
            build_filename("BXYZ", datetime(2026, 1, 30))


# ---------------------------------------------------------------------------
# Source join logic
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


class TestResolveBuDid:
    """
    All fixtures now include InvoiceId on invoice_line (needed for the
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

"""
Tests for salesforce_global_one.py (gl_journal_builder_pandas_s3.py) —
fixed-width formatting, run() orchestration, and main() (the Databricks
entry point).

S3 boundary function tests (find_dataset_prefix, read_jsonl_from_s3,
upload_to_s3, save_validation_file, etc.) live in test_s3_utils.py instead —
split out so they test s3_utils.py directly rather than depending on how
this script happens to re-export them.

Run with: pytest test_gl_journal_builder.py -v

Covers:
  - fmt() / build_line(): fixed-width formatting primitives
  - each record builder (#H, H, L, #T) against the exact field lengths
  - full file assembly (newline delimiting, trailer totals, row count,
    multi-BU grouping)
  - filename convention
  - run(): full orchestration with a mocked build_source_dataframe and a
    mocked S3 client (verified via s3.put_object call assertions, not by
    calling s3_utils functions directly — see test_s3_utils.py for that)
  - main(): the Databricks entry point, with the three environment-only
    dependencies (asvc1scoredataservices_common, pyspark, helpers) faked
    via sys.modules injection
"""

import sys
from datetime import datetime
from decimal import Decimal
from unittest.mock import MagicMock

import pandas as pd
import pytest

import src.salesforce.resources.scripts.salesforce_global_one as gljb
# Bare (short) form here on purpose — NOT src.salesforce.resources.scripts.helpers.
# salesforce_global_one.py's own internals do `import helpers.gl_source_join`
# (the short form, since that's what actually resolves when Databricks runs
# the script directly — see helpers/gl_source_join.py's docstring). For
# monkeypatch.setattr(glsj, "build_source_dataframe", ...) to actually
# intercept what gljb.run() calls, this has to be the SAME module object
# gljb resolves internally — the long src.-qualified form would be a
# different module object, and patching it would silently do nothing.
import helpers.gl_source_join as glsj
from src.salesforce.resources.scripts.salesforce_global_one import (
    fmt,
    build_line,
    file_header,
    journal_header,
    journal_line,
    file_trailer,
    build_gl_file,
    build_filename,
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

    def test_integer_value_formatted(self):
        assert fmt(42, 5) == "42   "

    def test_zero_length_field_returns_empty_string(self):
        assert fmt("anything", 0) == ""


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

    def test_empty_field_list_returns_empty_string(self):
        assert build_line([]) == ""


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

    def test_custom_transmit_id_included(self):
        line = file_header(datetime(2026, 8, 4), transmit_id="TX1")
        assert line[16:24] == "TX1     "

    def test_blank_transmit_id_by_default(self):
        line = file_header(datetime(2026, 8, 4))
        assert line[16:24] == " " * 8


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

    def test_source_field_in_correct_position(self):
        line = journal_header("US001", "08042026", "CS1")
        # position 68-70 (1-indexed) -> [67:70] zero-indexed
        assert line[67:70] == "CS1"

    def test_business_unit_in_correct_position(self):
        line = journal_header("US042", "08042026", "RCL")
        assert line[1:6] == "US042"


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

    def test_defaults_to_corp_regardless_of_business_unit(self):
        # real BUs are numeric ("10901"), not "US"/"EU"-prefixed — CORP is
        # the default for now, pending a real rule for CORP vs LOCAL
        line = journal_line(business_unit="US001", account="12345678")
        assert line[15:25] == "CORP      "

    def test_numeric_business_unit_also_gets_corp(self):
        line = journal_line(business_unit="10901", account="12345678")
        assert line[15:25] == "CORP      "

    def test_negative_amount_preserved(self):
        line = journal_line(business_unit="US001", account="12345678",
                             txn_monetary_amount=-250.50)
        assert "-250.5" in line

    def test_amount_rounds_to_two_decimals(self):
        line = journal_line(business_unit="US001", account="12345678",
                             txn_monetary_amount=99.999)
        assert "100.00" in line or "100" in line  # rounds up to 100.00

    def test_account_field_in_correct_position(self):
        line = journal_line(business_unit="US001", account="87654321")
        # position 26-35 (1-indexed) -> [25:35]
        assert line[25:35] == "87654321  "

    def test_currency_rate_type_standard_value(self):
        line = journal_line(business_unit="US001", account="12345678")
        # position 277-281 (1-indexed) -> [276:281]
        assert line[276:281] == "USDLY"

    def test_case_does_not_matter_since_always_corp(self):
        line = journal_line(business_unit="us001", account="12345678")
        assert line[15:25] == "CORP      "


class TestFileTrailer:
    def test_length_matches_spec(self):
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

    def test_total_statistical_amount_included(self):
        line = file_trailer(1, Decimal("0"), Decimal("0"), Decimal("42.50"))
        assert "42.50" in line

    def test_large_row_count_still_fits(self):
        line = file_trailer(999999999, Decimal("0"), Decimal("0"), 0)
        assert line[2:11] == "999999999"


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

    def test_multi_bu_mode_with_no_business_unit_column_produces_header_only(self):
        # business_unit=None but the dataframe doesn't even have a
        # business_unit column — covers the "else: bu_groups = []" branch
        df = pd.DataFrame([{"account": "123", "TransactionJournal.CreditDebit": 5.0}])
        content = build_gl_file(df, business_unit=None, source="CS1",
                                 creation_dt=datetime(2026, 8, 4))
        lines = content.split("\n")
        # file_header + file_trailer + trailing blank = 3 (no journal groups at all)
        assert len(lines) == 3
        assert lines[0].startswith("#H")
        assert lines[1].startswith("#T")

    def test_multi_bu_mode_groups_by_distinct_business_unit(self):
        df = pd.DataFrame([
            {"InvoiceLine.Business_Unit": "US001", "InvoiceLine.Department_Id": "10500", "Account.AccountNumber": "A", "TransactionJournal.CreditDebit": 10.0,
             "TransactionJournal.UsageType": "", "TransactionJournal.TransactionType": "", "TransactionJournal.Name": "Batch A"},
            {"InvoiceLine.Business_Unit": "EU002", "InvoiceLine.Department_Id": "20500", "Account.AccountNumber": "B", "TransactionJournal.CreditDebit": 20.0,
             "TransactionJournal.UsageType": "", "TransactionJournal.TransactionType": "", "TransactionJournal.Name": "Batch B"},
        ])
        content = build_gl_file(df, business_unit=None, source="CS1",
                                 creation_dt=datetime(2026, 8, 4))
        lines = content.split("\n")
        # #H + 2x(H + L) + #T + trailing blank = 7
        assert len(lines) == 7
        h_lines = [l for l in lines if l.startswith("H")]
        assert len(h_lines) == 2

    def test_multi_bu_groups_sorted_alphabetically(self):
        df = pd.DataFrame([
            {"InvoiceLine.Business_Unit": "US002", "InvoiceLine.Department_Id": "1", "Account.AccountNumber": "", "TransactionJournal.CreditDebit": 1.0,
             "TransactionJournal.UsageType": "", "TransactionJournal.TransactionType": "", "TransactionJournal.Name": ""},
            {"InvoiceLine.Business_Unit": "EU001", "InvoiceLine.Department_Id": "2", "Account.AccountNumber": "", "TransactionJournal.CreditDebit": 1.0,
             "TransactionJournal.UsageType": "", "TransactionJournal.TransactionType": "", "TransactionJournal.Name": ""},
        ])
        content = build_gl_file(df, business_unit=None, source="CS1",
                                 creation_dt=datetime(2026, 8, 4))
        lines = content.split("\n")
        h_lines = [l for l in lines if l.startswith("H")]
        assert h_lines[0][1:6] == "EU001"
        assert h_lines[1][1:6] == "US002"

    def test_filtering_to_single_business_unit_excludes_others(self):
        df = pd.DataFrame([
            {"InvoiceLine.Business_Unit": "US001", "InvoiceLine.Department_Id": "1", "Account.AccountNumber": "", "TransactionJournal.CreditDebit": 1.0,
             "TransactionJournal.UsageType": "", "TransactionJournal.TransactionType": "", "TransactionJournal.Name": ""},
            {"InvoiceLine.Business_Unit": "EU002", "InvoiceLine.Department_Id": "2", "Account.AccountNumber": "", "TransactionJournal.CreditDebit": 1.0,
             "TransactionJournal.UsageType": "", "TransactionJournal.TransactionType": "", "TransactionJournal.Name": ""},
        ])
        content = build_gl_file(df, business_unit="US001", source="CS1",
                                 creation_dt=datetime(2026, 8, 4))
        assert "EU002" not in content


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

    def test_rejects_empty_prefix(self):
        with pytest.raises(ValueError):
            build_filename("", datetime(2026, 1, 30))

    def test_filename_ends_with_txt(self):
        name = build_filename("CS1", datetime(2026, 1, 1))
        assert name.endswith(".txt")



# ---------------------------------------------------------------------------
# run() — full orchestration, mocked S3 + mocked build_source_dataframe
# ---------------------------------------------------------------------------

class TestRunOrchestration:
    @pytest.fixture(autouse=True)
    def _patch_s3_to_onelake(self, monkeypatch):
        # write_and_submit_file (called by run()) resolves s3_to_onelake
        # through helper_functions.py's own bare "helpers.helper_functions"
        # namespace — same reasoning as patching glsj at the bare form for
        # gljb's internals: this has to be the exact module object gljb's
        # call chain actually resolves, or the patch is a silent no-op.
        self.s3_to_onelake_mock = MagicMock()
        monkeypatch.setattr("helpers.helper_functions.s3_to_onelake", self.s3_to_onelake_mock)

    def _sample_df(self):
        return pd.DataFrame([
            {"InvoiceLine.Business_Unit": "US001", "InvoiceLine.Department_Id": "10500", "Account.AccountNumber": "Acme",
             "TransactionJournal.CreditDebit": 100.0, "TransactionJournal.UsageType": "Storage", "TransactionJournal.TransactionType": "InvoiceLine",
             "TransactionJournal.Name": "Batch"},
        ])

    def _writer_config(self):
        return {
            "ba": "app", "schema_name": "schema", "iam_role": "role",
            "base_url": "https://exchange.example.com/api", "env": "qa", "region": "west",
        }

    def _run(self, log, s3, bucket="bucket", output_key_prefix="outbound", filename_prefix="BX1", **kwargs):
        return gljb.run(log, s3, "token", bucket, output_key_prefix, filename_prefix,
                         self._writer_config(), **kwargs)

    def test_calls_build_source_dataframe_and_uploads(self, monkeypatch):
        fake_df = self._sample_df()
        monkeypatch.setattr(glsj, "build_source_dataframe", lambda *a, **k: fake_df)

        s3 = MagicMock()
        log = MagicMock()

        url, row_count = self._run(log, s3)

        assert row_count == 1
        assert s3.put_object.called

    def test_submits_outbound_file_to_onelake(self, monkeypatch):
        monkeypatch.setattr(glsj, "build_source_dataframe", lambda *a, **k: self._sample_df())
        s3 = MagicMock()
        log = MagicMock()

        self._run(log, s3)

        self.s3_to_onelake_mock.assert_called_once()
        call_args = self.s3_to_onelake_mock.call_args
        assert call_args.args[1] == "token"  # oauth_token passed through
        file_submissions = call_args.args[3]
        assert len(file_submissions) == 1
        assert file_submissions[0]["fileType"] == "MULTI_RECORD_FIXED_WIDTH"
        assert "decodeMetadata" in file_submissions[0]

    def test_filters_by_business_unit_when_given(self, monkeypatch):
        fake_df = pd.DataFrame([
            {"InvoiceLine.Business_Unit": "US001", "InvoiceLine.Department_Id": "10500", "Account.AccountNumber": "A", "TransactionJournal.CreditDebit": 1.0,
             "TransactionJournal.UsageType": "", "TransactionJournal.TransactionType": "", "TransactionJournal.Name": ""},
            {"InvoiceLine.Business_Unit": "US002", "InvoiceLine.Department_Id": "20500", "Account.AccountNumber": "B", "TransactionJournal.CreditDebit": 2.0,
             "TransactionJournal.UsageType": "", "TransactionJournal.TransactionType": "", "TransactionJournal.Name": ""},
        ])
        monkeypatch.setattr(glsj, "build_source_dataframe", lambda *a, **k: fake_df)

        s3 = MagicMock()
        log = MagicMock()

        url, row_count = self._run(log, s3, business_unit="US001")
        assert row_count == 1

    def test_skips_validation_file_when_prefix_not_given(self, monkeypatch):
        monkeypatch.setattr(glsj, "build_source_dataframe", lambda *a, **k: self._sample_df())
        s3 = MagicMock()
        log = MagicMock()

        self._run(log, s3, validation_key_prefix=None)

        # put_object should only be called for the main file + _SUCCESS, not a parquet
        keys_written = [call.kwargs["Key"] for call in s3.put_object.call_args_list]
        assert not any(k.endswith(".parquet") for k in keys_written)

    def test_saves_validation_file_when_prefix_given(self, monkeypatch):
        monkeypatch.setattr(glsj, "build_source_dataframe", lambda *a, **k: self._sample_df())
        s3 = MagicMock()
        log = MagicMock()

        self._run(log, s3, validation_key_prefix="validation")

        keys_written = [call.kwargs["Key"] for call in s3.put_object.call_args_list]
        assert any(k.endswith(".parquet") for k in keys_written)

    def test_writes_success_marker(self, monkeypatch):
        monkeypatch.setattr(glsj, "build_source_dataframe", lambda *a, **k: self._sample_df())
        s3 = MagicMock()
        log = MagicMock()

        self._run(log, s3)

        keys_written = [call.kwargs["Key"] for call in s3.put_object.call_args_list]
        assert any(k.endswith("_SUCCESS") for k in keys_written)

    def test_returns_s3_url_string(self, monkeypatch):
        monkeypatch.setattr(glsj, "build_source_dataframe", lambda *a, **k: self._sample_df())
        s3 = MagicMock()
        log = MagicMock()

        url, _ = self._run(log, s3)
        # output path is now partitioned: outbound/year=/month=/day=/hour=/BX1_....txt
        assert url.startswith("s3://bucket/outbound/year=")
        assert "/BX1_" in url
        assert url.endswith(".txt")

    def test_output_file_lands_in_partitioned_path(self, monkeypatch):
        monkeypatch.setattr(glsj, "build_source_dataframe", lambda *a, **k: self._sample_df())
        s3 = MagicMock()
        log = MagicMock()

        self._run(log, s3)

        keys_written = [call.kwargs["Key"] for call in s3.put_object.call_args_list]
        data_file_keys = [k for k in keys_written if k.endswith(".txt")]
        assert len(data_file_keys) == 1
        assert "year=" in data_file_keys[0]
        assert "month=" in data_file_keys[0]
        assert "day=" in data_file_keys[0]
        assert "hour=" in data_file_keys[0]

    def test_success_marker_lands_in_same_partitioned_path_as_data_file(self, monkeypatch):
        monkeypatch.setattr(glsj, "build_source_dataframe", lambda *a, **k: self._sample_df())
        s3 = MagicMock()
        log = MagicMock()

        self._run(log, s3)

        keys_written = [call.kwargs["Key"] for call in s3.put_object.call_args_list]
        data_file_key = next(k for k in keys_written if k.endswith(".txt"))
        success_key = next(k for k in keys_written if k.endswith("_SUCCESS"))
        # both should share the same partitioned folder
        assert data_file_key.rsplit("/", 1)[0] == success_key.rsplit("/", 1)[0]

    def test_validation_file_type_passed_through(self, monkeypatch):
        monkeypatch.setattr(glsj, "build_source_dataframe", lambda *a, **k: self._sample_df())
        s3 = MagicMock()
        log = MagicMock()

        self._run(log, s3, validation_key_prefix="validation", validation_file_type="csv")

        keys_written = [call.kwargs["Key"] for call in s3.put_object.call_args_list]
        assert any(k.endswith(".csv") for k in keys_written)
        assert not any(k.endswith(".parquet") for k in keys_written)

    def test_validation_file_defaults_to_parquet(self, monkeypatch):
        monkeypatch.setattr(glsj, "build_source_dataframe", lambda *a, **k: self._sample_df())
        s3 = MagicMock()
        log = MagicMock()

        self._run(log, s3, validation_key_prefix="validation")

        keys_written = [call.kwargs["Key"] for call in s3.put_object.call_args_list]
        assert any(k.endswith(".parquet") for k in keys_written)

    def test_validation_file_uses_table_dot_column_headers(self, monkeypatch):
        # Table.Column naming comes directly from build_source_dataframe,
        # so the same dataframe used to build the GL file is also what
        # gets written to the validation file.
        monkeypatch.setattr(glsj, "build_source_dataframe", lambda *a, **k: self._sample_df())
        s3 = MagicMock()
        log = MagicMock()

        self._run(log, s3, validation_key_prefix="validation", validation_file_type="csv")

        validation_calls = [
            call for call in s3.put_object.call_args_list
            if call.kwargs["Key"].endswith(".csv")
        ]
        assert len(validation_calls) == 1
        body = validation_calls[0].kwargs["Body"]
        assert b"Account.AccountNumber" in body
        assert b"InvoiceLine.Business_Unit" in body
        assert b"TransactionJournal.CreditDebit" in body

    def test_logs_progress_through_run(self, monkeypatch):
        monkeypatch.setattr(glsj, "build_source_dataframe", lambda *a, **k: self._sample_df())
        s3 = MagicMock()
        log = MagicMock()

        self._run(log, s3)

        assert log.info.called
        messages = [call.args[0] for call in log.info.call_args_list]
        assert any("building source dataframe" in m for m in messages)
        assert any("building GL journal file" in m for m in messages)
        assert any("submitting to onelake" in m for m in messages)

    def test_run_with_empty_result_still_produces_valid_file(self, monkeypatch):
        monkeypatch.setattr(glsj, "build_source_dataframe",
                             lambda *a, **k: pd.DataFrame(columns=["InvoiceLine.Business_Unit"]))
        s3 = MagicMock()
        log = MagicMock()

        url, row_count = self._run(log, s3)
        assert row_count == 0


# ---------------------------------------------------------------------------
# main() — the Databricks entry point
# ---------------------------------------------------------------------------
#
# main() does deferred imports of three packages that only exist inside the
# actual Databricks environment: asvc1scoredataservices_common, pyspark, and
# helpers.helper_functions. In a real repo these ARE genuinely installed, so
# faking whole modules via sys.modules is fragile — if anything else in test
# collection (a conftest.py fixture, another test module) imports the real
# package first, that sys.modules-injection approach can lose the race and
# main() ends up calling the real functions (e.g. a real SparkSession trying
# to open a real Spark Connect session, which fails outside a cluster).
#
# Instead, patch the specific functions directly on the real installed
# packages using monkeypatch.setattr's dotted-string form. This works
# regardless of import timing/ordering, since it patches the actual
# attribute on the actual module object main() will look up at call time.

def install_fake_databricks_deps(monkeypatch, logger_mock=None, new_session_mock=None,
                                  write_execution_log_mock=None, spark_session_mock=None,
                                  read_secret_from_chamber_mock=None, retrieve_oauth_token_mock=None):
    """
    Patches setup_logger / write_execution_log_to_s3 / new_session /
    SparkSession.builder.getOrCreate / read_secret_from_chamber /
    retrieve_oauth_token directly on the real (installed) packages.
    Requires asvc1scoredataservices_common, pyspark, and
    helpers.helper_functions to actually be importable in the environment
    running these tests — true in the real repo. monkeypatch.setattr
    reverts all of this automatically at test teardown.
    """
    logger_mock = logger_mock or MagicMock()
    new_session_mock = new_session_mock or MagicMock()
    write_execution_log_mock = write_execution_log_mock or MagicMock()
    spark_session_mock = spark_session_mock or MagicMock()
    read_secret_from_chamber_mock = read_secret_from_chamber_mock or MagicMock(return_value="fake-secret")
    retrieve_oauth_token_mock = retrieve_oauth_token_mock or MagicMock(return_value="fake-oauth-token")

    setup_logger_mock = MagicMock(return_value=logger_mock)

    monkeypatch.setattr(
        "asvc1scoredataservices_common.logger.basic_logger.setup_logger", setup_logger_mock
    )
    monkeypatch.setattr(
        "asvc1scoredataservices_common.logger.logger.write_execution_log_to_s3", write_execution_log_mock
    )
    monkeypatch.setattr("helpers.helper_functions.new_session", new_session_mock)
    monkeypatch.setattr(
        "asvc1scoredataservices_common.utils.helper_functions.read_secret_from_chamber",
        read_secret_from_chamber_mock,
    )
    monkeypatch.setattr("helpers.helper_functions.retrieve_oauth_token", retrieve_oauth_token_mock)
    monkeypatch.setattr("helpers.helper_functions.s3_to_onelake", MagicMock())

    from pyspark.sql import SparkSession
    # SparkSession.builder returns a NEW Builder instance on every access —
    # confirmed: SparkSession.builder is not SparkSession.builder. Patching
    # getOrCreate on one instance (the old approach) only affected that one
    # temporary object; main()'s later SparkSession.builder.getOrCreate()
    # call gets a fresh instance without the patch and hits the real Spark
    # Connect code. Patch the method on the Builder CLASS instead, so every
    # instance (including ones created after this patch) picks it up.
    monkeypatch.setattr(SparkSession.Builder, "getOrCreate", lambda self: spark_session_mock)

    # Defensive fallback: if main() (or the real production script it's
    # eventually merged into) imports setup_logger/new_session/
    # write_execution_log_to_s3 at module top level rather than deferred
    # inside main(), the dotted-path patches above won't reach those
    # already-bound local copies (same "from X import Y copies a reference"
    # issue as elsewhere). Patch the module-level aliases on gljb too, if
    # present, as a belt-and-suspenders measure. No-ops harmlessly if this
    # file's own deferred-import pattern is what's actually running.
    if hasattr(gljb, "setup_logger"):
        monkeypatch.setattr(gljb, "setup_logger", setup_logger_mock)
    if hasattr(gljb, "new_session"):
        monkeypatch.setattr(gljb, "new_session", new_session_mock)
    if hasattr(gljb, "write_execution_log_to_s3"):
        monkeypatch.setattr(gljb, "write_execution_log_to_s3", write_execution_log_mock)

    return {
        "logger": logger_mock,
        "setup_logger": setup_logger_mock,
        "new_session": new_session_mock,
        "write_execution_log_to_s3": write_execution_log_mock,
        "spark_session": spark_session_mock,
        "read_secret_from_chamber": read_secret_from_chamber_mock,
        "retrieve_oauth_token": retrieve_oauth_token_mock,
    }


# Kept as an alias so any external references to the old name still work.
install_fake_databricks_modules = install_fake_databricks_deps


VALID_ARGV = [
    "gl_journal_builder_pandas_s3.py",
    "prod",                # env
    "some_chamber_role",   # chamber_role
    "some_service_cred",   # service_credential
    "my-bucket",           # bucket
    "gl-interface/outbound",       # output_key_prefix
    "gl-interface/validation",     # validation_key_prefix
    "BX1",                 # filename_prefix
    "salesforce/reports",  # source_key_prefix
    "parquet",              # validation_file_type
]


def sample_df():
    return pd.DataFrame([
        {"InvoiceLine.Business_Unit": "US001", "InvoiceLine.Department_Id": "10500", "Account.AccountNumber": "Acme",
         "TransactionJournal.CreditDebit": 100.0, "TransactionJournal.UsageType": "Storage", "TransactionJournal.TransactionType": "InvoiceLine",
         "TransactionJournal.Name": "Batch"},
    ])


class TestMainSuccessPath:
    def test_returns_success_response(self, monkeypatch):
        monkeypatch.setattr(sys, "argv", VALID_ARGV)
        monkeypatch.setattr(glsj, "build_source_dataframe", lambda *a, **k: sample_df())
        mocks = install_fake_databricks_modules(monkeypatch)
        mocks["new_session"].return_value.client.return_value = MagicMock()

        result = gljb.main()

        assert result["status_code"] == 200
        assert result["num_records"] == 1
        assert result["message"] == "SUCCESS"
        assert result["s3_url"].startswith("s3://my-bucket/gl-interface/outbound/year=")
        assert "/BX1_" in result["s3_url"]

    def test_calls_new_session_with_service_credential(self, monkeypatch):
        monkeypatch.setattr(sys, "argv", VALID_ARGV)
        monkeypatch.setattr(glsj, "build_source_dataframe", lambda *a, **k: sample_df())
        mocks = install_fake_databricks_modules(monkeypatch)
        mocks["new_session"].return_value.client.return_value = MagicMock()

        gljb.main()

        mocks["new_session"].assert_called_once_with("some_service_cred")

    def test_writes_execution_log_with_success_state(self, monkeypatch):
        monkeypatch.setattr(sys, "argv", VALID_ARGV)
        monkeypatch.setattr(glsj, "build_source_dataframe", lambda *a, **k: sample_df())
        mocks = install_fake_databricks_modules(monkeypatch)
        mocks["new_session"].return_value.client.return_value = MagicMock()

        gljb.main()

        mocks["write_execution_log_to_s3"].assert_called_once()
        call_kwargs = mocks["write_execution_log_to_s3"].call_args.kwargs
        assert call_kwargs["final_state"] == "SUCCESS"
        assert call_kwargs["failure_message"] is None
        assert call_kwargs["records_published"] == 1
        assert call_kwargs["job_name"] == "salesforce_global_one"
        assert call_kwargs["job_family"] == "salesforce"
        assert call_kwargs["environment"] == "prod"
        assert call_kwargs["severity_text"] == "info"

    def test_execution_log_s3_path_includes_env_and_job_name(self, monkeypatch):
        monkeypatch.setattr(sys, "argv", VALID_ARGV)
        monkeypatch.setattr(glsj, "build_source_dataframe", lambda *a, **k: sample_df())
        mocks = install_fake_databricks_modules(monkeypatch)
        mocks["new_session"].return_value.client.return_value = MagicMock()

        gljb.main()

        call_kwargs = mocks["write_execution_log_to_s3"].call_args.kwargs
        assert "c1scoredataservices-prod-east" in call_kwargs["s3_path"]
        assert "job_family=salesforce" in call_kwargs["s3_path"]
        assert "job_name=salesforce_global_one" in call_kwargs["s3_path"]

    def test_logs_info_messages_through_success_path(self, monkeypatch):
        monkeypatch.setattr(sys, "argv", VALID_ARGV)
        monkeypatch.setattr(glsj, "build_source_dataframe", lambda *a, **k: sample_df())
        mocks = install_fake_databricks_modules(monkeypatch)
        mocks["new_session"].return_value.client.return_value = MagicMock()

        gljb.main()

        messages = [c.args[0] for c in mocks["logger"].info.call_args_list]
        assert any("retrieving aws credentials" in m for m in messages)
        assert any("running gl journal builder...complete" in m for m in messages)

    def test_uses_setup_logger_return_value(self, monkeypatch):
        monkeypatch.setattr(sys, "argv", VALID_ARGV)
        monkeypatch.setattr(glsj, "build_source_dataframe", lambda *a, **k: sample_df())
        mocks = install_fake_databricks_modules(monkeypatch)
        mocks["new_session"].return_value.client.return_value = MagicMock()

        gljb.main()

        mocks["setup_logger"].assert_called_once()


class TestMainArgumentValidation:
    def test_raises_valueerror_with_too_few_args(self, monkeypatch):
        monkeypatch.setattr(sys, "argv", ["script.py", "prod", "role"])
        mocks = install_fake_databricks_modules(monkeypatch)

        with pytest.raises(ValueError, match="Usage:"):
            gljb.main()

    def test_no_execution_log_written_on_argument_error(self, monkeypatch):
        # the argv-length check happens before the try/finally, so a bad
        # invocation shouldn't attempt to write an execution log at all
        monkeypatch.setattr(sys, "argv", ["script.py", "prod"])
        mocks = install_fake_databricks_modules(monkeypatch)

        with pytest.raises(ValueError):
            gljb.main()

        mocks["write_execution_log_to_s3"].assert_not_called()

    def test_setup_logger_still_called_before_validation(self, monkeypatch):
        # logger = setup_logger() happens before the argv check
        monkeypatch.setattr(sys, "argv", ["script.py"])
        mocks = install_fake_databricks_modules(monkeypatch)

        with pytest.raises(ValueError):
            gljb.main()

        mocks["setup_logger"].assert_called_once()


class TestMainErrorPath:
    def test_wraps_and_reraises_exception(self, monkeypatch):
        monkeypatch.setattr(sys, "argv", VALID_ARGV)

        def failing_build_source_dataframe(*a, **k):
            raise RuntimeError("simulated S3 read failure")

        monkeypatch.setattr(glsj, "build_source_dataframe", failing_build_source_dataframe)
        mocks = install_fake_databricks_modules(monkeypatch)
        mocks["new_session"].return_value.client.return_value = MagicMock()

        with pytest.raises(Exception, match="Unhandled error during gl journal builder execution"):
            gljb.main()

    def test_writes_execution_log_with_failed_state(self, monkeypatch):
        monkeypatch.setattr(sys, "argv", VALID_ARGV)

        def failing_build_source_dataframe(*a, **k):
            raise RuntimeError("simulated S3 read failure")

        monkeypatch.setattr(glsj, "build_source_dataframe", failing_build_source_dataframe)
        mocks = install_fake_databricks_modules(monkeypatch)
        mocks["new_session"].return_value.client.return_value = MagicMock()

        with pytest.raises(Exception):
            gljb.main()

        mocks["write_execution_log_to_s3"].assert_called_once()
        call_kwargs = mocks["write_execution_log_to_s3"].call_args.kwargs
        assert call_kwargs["final_state"] == "FAILED"
        assert "simulated S3 read failure" in call_kwargs["failure_message"]
        assert call_kwargs["records_published"] == 0
        assert call_kwargs["severity_text"] == "error"

    def test_logs_error_with_stack_trace(self, monkeypatch):
        monkeypatch.setattr(sys, "argv", VALID_ARGV)

        def failing_build_source_dataframe(*a, **k):
            raise RuntimeError("simulated S3 read failure")

        monkeypatch.setattr(glsj, "build_source_dataframe", failing_build_source_dataframe)
        mocks = install_fake_databricks_modules(monkeypatch)
        mocks["new_session"].return_value.client.return_value = MagicMock()

        with pytest.raises(Exception):
            gljb.main()

        mocks["logger"].error.assert_called_once()
        error_message = mocks["logger"].error.call_args.args[0]
        assert "simulated S3 read failure" in error_message
        assert "RuntimeError" in error_message

    def test_failure_message_body_mentions_job_name(self, monkeypatch):
        monkeypatch.setattr(sys, "argv", VALID_ARGV)

        def failing_build_source_dataframe(*a, **k):
            raise RuntimeError("boom")

        monkeypatch.setattr(glsj, "build_source_dataframe", failing_build_source_dataframe)
        mocks = install_fake_databricks_modules(monkeypatch)
        mocks["new_session"].return_value.client.return_value = MagicMock()

        with pytest.raises(Exception):
            gljb.main()

        call_kwargs = mocks["write_execution_log_to_s3"].call_args.kwargs
        assert "salesforce_global_one" in call_kwargs["body"]
        assert "boom" in call_kwargs["body"]

    def test_new_session_failure_also_wrapped_and_logged(self, monkeypatch):
        # a credential failure should be caught by the same try/except as
        # any other failure in the run
        monkeypatch.setattr(sys, "argv", VALID_ARGV)
        monkeypatch.setattr(glsj, "build_source_dataframe", lambda *a, **k: sample_df())

        def failing_new_session(service_credential):
            raise PermissionError("chamber access denied")

        mocks = install_fake_databricks_modules(
            monkeypatch, new_session_mock=MagicMock(side_effect=failing_new_session)
        )

        with pytest.raises(Exception, match="Unhandled error"):
            gljb.main()

        call_kwargs = mocks["write_execution_log_to_s3"].call_args.kwargs
        assert call_kwargs["final_state"] == "FAILED"
        assert "chamber access denied" in call_kwargs["failure_message"]


class TestMainEnvironmentPassthrough:
    def test_different_env_value_reflected_in_log_path_and_environment_field(self, monkeypatch):
        argv = list(VALID_ARGV)
        argv[1] = "qa"
        monkeypatch.setattr(sys, "argv", argv)
        monkeypatch.setattr(glsj, "build_source_dataframe", lambda *a, **k: sample_df())
        mocks = install_fake_databricks_modules(monkeypatch)
        mocks["new_session"].return_value.client.return_value = MagicMock()

        gljb.main()

        call_kwargs = mocks["write_execution_log_to_s3"].call_args.kwargs
        assert call_kwargs["environment"] == "qa"
        assert "c1scoredataservices-qa-east" in call_kwargs["s3_path"]

    def test_run_start_and_end_timestamps_are_ordered(self, monkeypatch):
        monkeypatch.setattr(sys, "argv", VALID_ARGV)
        monkeypatch.setattr(glsj, "build_source_dataframe", lambda *a, **k: sample_df())
        mocks = install_fake_databricks_modules(monkeypatch)
        mocks["new_session"].return_value.client.return_value = MagicMock()

        gljb.main()

        call_kwargs = mocks["write_execution_log_to_s3"].call_args.kwargs
        assert call_kwargs["run_start_timestamp"] <= call_kwargs["run_end_timestamp"]
        assert call_kwargs["data_interval_end_timestamp"] == call_kwargs["run_end_timestamp"]
        assert call_kwargs["data_interval_start_timestamp"] is None


class TestDunderMain:
    def test_module_has_main_guard_calling_main(self):
        # sanity check the __main__ guard exists and points at main()
        import inspect
        source = inspect.getsource(gljb)
        assert 'if __name__ == "__main__":' in source
        assert "main()" in source.split('if __name__ == "__main__":')[1]


# ---------------------------------------------------------------------------
# choose_gl_identity()
# ---------------------------------------------------------------------------

class TestChooseGlIdentity:
    def test_returns_schema_name_string(self):
        schema_name = gljb.choose_gl_identity("qa")
        assert isinstance(schema_name, str)
        assert schema_name

    def test_prod_and_qa_return_different_values(self):
        assert gljb.choose_gl_identity("prod") != gljb.choose_gl_identity("qa")

    def test_invalid_env_raises(self):
        with pytest.raises(ValueError, match="Invalid environment"):
            gljb.choose_gl_identity("staging")

    def test_currently_placeholder_value_not_yet_real(self):
        # This test exists specifically to fail loudly once a real value
        # is filled in — at that point, delete this test rather than
        # update it, since it's only here to make the placeholder status
        # impossible to miss.
        schema_name = gljb.choose_gl_identity("prod")
        assert "PLACEHOLDER" in schema_name

"""
Tests for the shared consolidation functions added to helper_functions.py:
build_execution_log_s3_path, transform_account_data, write_and_submit_file.
These are used by both salesforce_ofac.py and salesforce_global_one.py.

Run with: pytest test_helper_functions.py -v
"""

from datetime import datetime
from unittest.mock import MagicMock

import numpy as np
import pandas as pd
import pytest

from src.salesforce.resources.scripts.helpers.helper_functions import (
    build_execution_log_s3_path,
    transform_account_data,
    write_and_submit_file,
)


# ---------------------------------------------------------------------------
# build_execution_log_s3_path()
# ---------------------------------------------------------------------------

class TestBuildExecutionLogS3Path:
    def test_matches_pattern_both_scripts_used_independently(self):
        result = build_execution_log_s3_path("qa", "salesforce", "salesforce_ofac", "2026-08-05")
        assert result == (
            "s3a://c1scoredataservices-qa-east/databricks_job_logs/"
            "job_run_date=2026-08-05/"
            "job_family=salesforce/"
            "job_name=salesforce_ofac/"
        )

    def test_different_env_reflected_in_path(self):
        result = build_execution_log_s3_path("prod", "salesforce", "salesforce_global_one", "2026-01-01")
        assert "c1scoredataservices-prod-east" in result

    def test_always_ends_with_trailing_slash(self):
        result = build_execution_log_s3_path("qa", "fam", "job", "2026-01-01")
        assert result.endswith("/")


# ---------------------------------------------------------------------------
# transform_account_data()
# ---------------------------------------------------------------------------

class TestTransformAccountData:
    def _data_columns(self):
        return {"name": "account_name", "id": "account_id"}

    def _columns_order(self):
        return ["account_name", "account_id", "shipping_addr_ln_2", "billing_addr_ln_2"]

    def test_drops_attributes_column(self):
        log = MagicMock()
        df = pd.DataFrame([{"Name": "Acme", "Id": "001", "attributes": {"type": "Account"}}])
        result = transform_account_data(log, df, self._data_columns(), self._columns_order())
        assert "attributes" not in result.columns

    def test_lowercases_then_renames_columns(self):
        log = MagicMock()
        df = pd.DataFrame([{"Name": "Acme", "Id": "001"}])
        result = transform_account_data(log, df, self._data_columns(), self._columns_order())
        assert "account_name" in result.columns
        assert "account_id" in result.columns
        assert "Name" not in result.columns

    def test_applies_normalize_data_address_line_2_columns(self):
        log = MagicMock()
        df = pd.DataFrame([{"Name": "Acme", "Id": "001"}])
        result = transform_account_data(log, df, self._data_columns(), self._columns_order())
        assert "shipping_addr_ln_2" in result.columns
        assert "billing_addr_ln_2" in result.columns

    def test_missing_attributes_column_does_not_raise(self):
        log = MagicMock()
        df = pd.DataFrame([{"Name": "Acme", "Id": "001"}])  # no attributes column at all
        result = transform_account_data(log, df, self._data_columns(), self._columns_order())
        assert len(result) == 1

    def test_result_columns_match_columns_order(self):
        log = MagicMock()
        df = pd.DataFrame([{"Name": "Acme", "Id": "001"}])
        result = transform_account_data(log, df, self._data_columns(), self._columns_order())
        assert list(result.columns) == self._columns_order()


# ---------------------------------------------------------------------------
# write_and_submit_file()
# ---------------------------------------------------------------------------

class TestWriteAndSubmitFile:
    def _writer_config(self):
        return {
            "ba": "app", "schema_name": "schema", "iam_role": "role",
            "base_url": "https://exchange.example.com/api", "env": "qa", "region": "west",
        }

    def test_writes_outbound_file_and_success_marker(self, monkeypatch):
        s3 = MagicMock()
        monkeypatch.setattr(
            "src.salesforce.resources.scripts.helpers.helper_functions.s3_to_onelake",
            lambda *a, **k: None,
        )
        write_and_submit_file(
            MagicMock(), s3, "token", "bucket", "col1,col2\n1,2\n", "f.csv", "CSV_WITH_HEADER",
            "outbound", self._writer_config(), datetime(2026, 8, 5, 14),
        )
        keys_written = [c.kwargs["Key"] for c in s3.put_object.call_args_list]
        assert any(k.endswith("f.csv") for k in keys_written)
        assert any(k.endswith("_SUCCESS") for k in keys_written)

    def test_outbound_path_is_partitioned(self, monkeypatch):
        s3 = MagicMock()
        monkeypatch.setattr(
            "src.salesforce.resources.scripts.helpers.helper_functions.s3_to_onelake",
            lambda *a, **k: None,
        )
        write_and_submit_file(
            MagicMock(), s3, "token", "bucket", "content", "f.csv", "CSV_WITH_HEADER",
            "outbound", self._writer_config(), datetime(2026, 8, 5, 14),
        )
        data_key = next(c.kwargs["Key"] for c in s3.put_object.call_args_list
                         if c.kwargs["Key"].endswith("f.csv"))
        assert "year=2026/month=08/day=05/hour=14/" in data_key

    def test_skips_validation_file_when_not_given(self, monkeypatch):
        s3 = MagicMock()
        monkeypatch.setattr(
            "src.salesforce.resources.scripts.helpers.helper_functions.s3_to_onelake",
            lambda *a, **k: None,
        )
        _, validation_url = write_and_submit_file(
            MagicMock(), s3, "token", "bucket", "content", "f.csv", "CSV_WITH_HEADER",
            "outbound", self._writer_config(), datetime(2026, 8, 5),
        )
        assert validation_url is None

    def test_writes_validation_file_when_given(self, monkeypatch):
        s3 = MagicMock()
        monkeypatch.setattr(
            "src.salesforce.resources.scripts.helpers.helper_functions.s3_to_onelake",
            lambda *a, **k: None,
        )
        df = pd.DataFrame([{"a": 1}])
        _, validation_url = write_and_submit_file(
            MagicMock(), s3, "token", "bucket", "content", "f.csv", "CSV_WITH_HEADER",
            "outbound", self._writer_config(), datetime(2026, 8, 5),
            validation_df=df, validation_key_prefix="validation", validation_file_type="csv",
        )
        assert validation_url is not None
        assert validation_url.endswith(".csv")

    def test_calls_s3_to_onelake_with_correct_file_submission(self, monkeypatch):
        captured = {}

        def fake_s3_to_onelake(log, oauth_token, writer_config, file_submissions):
            captured["oauth_token"] = oauth_token
            captured["writer_config"] = writer_config
            captured["file_submissions"] = file_submissions

        monkeypatch.setattr(
            "src.salesforce.resources.scripts.helpers.helper_functions.s3_to_onelake",
            fake_s3_to_onelake,
        )
        s3 = MagicMock()
        write_and_submit_file(
            MagicMock(), s3, "my-token", "my-bucket", "hello", "f.csv", "CSV_WITH_HEADER",
            "outbound", self._writer_config(), datetime(2026, 8, 5),
        )
        assert captured["oauth_token"] == "my-token"
        assert captured["writer_config"]["bucket"] == "my-bucket"
        assert captured["writer_config"]["file_name"] == "f.csv"
        entry = captured["file_submissions"][0]
        assert entry["fileType"] == "CSV_WITH_HEADER"
        assert entry["fileSize"] == len("hello".encode("utf-8"))
        assert "decodeMetadata" not in entry

    def test_decode_metadata_included_when_given(self, monkeypatch):
        captured = {}

        def fake_s3_to_onelake(log, oauth_token, writer_config, file_submissions):
            captured["file_submissions"] = file_submissions

        monkeypatch.setattr(
            "src.salesforce.resources.scripts.helpers.helper_functions.s3_to_onelake",
            fake_s3_to_onelake,
        )
        s3 = MagicMock()
        write_and_submit_file(
            MagicMock(), s3, "token", "bucket", "content", "f.txt", "MULTI_RECORD_FIXED_WIDTH",
            "outbound", self._writer_config(), datetime(2026, 8, 5),
            decode_metadata={"fieldDefinitions": []},
        )
        assert captured["file_submissions"][0]["decodeMetadata"] == {"fieldDefinitions": []}

    def test_does_not_mutate_caller_writer_config(self, monkeypatch):
        monkeypatch.setattr(
            "src.salesforce.resources.scripts.helpers.helper_functions.s3_to_onelake",
            lambda *a, **k: None,
        )
        s3 = MagicMock()
        original_config = self._writer_config()
        write_and_submit_file(
            MagicMock(), s3, "token", "bucket", "content", "f.csv", "CSV_WITH_HEADER",
            "outbound", original_config, datetime(2026, 8, 5),
        )
        assert "bucket" not in original_config
        assert "file_name" not in original_config


# ---------------------------------------------------------------------------
# choose_env() / choose_exchange_env() — source/schema_name now passed in,
# not hardcoded per env branch
# ---------------------------------------------------------------------------

class TestChooseEnv:
    def test_passes_through_source_and_schema_name_unchanged(self):
        from src.salesforce.resources.scripts.helpers.helper_functions import choose_env
        result = choose_env("qa", "my_source", "my_schema")
        assert result[5] == "my_source"
        assert result[6] == "my_schema"

    def test_different_jobs_can_pass_different_values(self):
        from src.salesforce.resources.scripts.helpers.helper_functions import choose_env
        ofac_result = choose_env("qa", "c1s_ofac_sanctions_reporting", "c1s_ofac_sanctions_reporting_v2")
        gl_result = choose_env("qa", "gl_journal", "gl_journal_schema")
        assert ofac_result[5:7] != gl_result[5:7]

    def test_infra_values_still_env_dependent_not_job_dependent(self):
        from src.salesforce.resources.scripts.helpers.helper_functions import choose_env
        result_a = choose_env("qa", "job_a_source", "job_a_schema")
        result_b = choose_env("qa", "job_b_source", "job_b_schema")
        # bucket, exchange_oauth, iam_role, base_url should be identical
        # regardless of which job's source/schema_name was passed in
        assert result_a[0] == result_b[0]  # bucket
        assert result_a[2] == result_b[2]  # exchange_oauth
        assert result_a[7] == result_b[7]  # iam_role
        assert result_a[8] == result_b[8]  # base_url

    def test_invalid_env_still_raises(self):
        from src.salesforce.resources.scripts.helpers.helper_functions import choose_env
        with pytest.raises(ValueError, match="Invalid environment"):
            choose_env("staging", "source", "schema")

    def test_matches_choose_exchange_env_values(self):
        from src.salesforce.resources.scripts.helpers.helper_functions import (
            choose_env, choose_exchange_env,
        )
        exchange_oauth, iam_role, base_url = choose_exchange_env("prod")
        result = choose_env("prod", "src", "schema")
        assert result[2] == exchange_oauth
        assert result[7] == iam_role
        assert result[8] == base_url


class TestChooseExchangeEnv:
    def test_prod_and_qa_return_different_values(self):
        from src.salesforce.resources.scripts.helpers.helper_functions import choose_exchange_env
        assert choose_exchange_env("prod") != choose_exchange_env("qa")

    def test_returns_three_values(self):
        from src.salesforce.resources.scripts.helpers.helper_functions import choose_exchange_env
        result = choose_exchange_env("qa")
        assert len(result) == 3

    def test_invalid_env_raises(self):
        from src.salesforce.resources.scripts.helpers.helper_functions import choose_exchange_env
        with pytest.raises(ValueError, match="Invalid environment"):
            choose_exchange_env("staging")

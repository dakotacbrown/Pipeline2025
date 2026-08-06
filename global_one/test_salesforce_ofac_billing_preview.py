"""
Tests for salesforce_ofac_billing_preview.py — the Account + BillingAccount
join, not yet wired into the shipped salesforce_ofac.py.

Run with: pytest test_salesforce_ofac_billing_preview.py -v
"""

from unittest.mock import MagicMock

import pandas as pd
import pytest

from src.salesforce.resources.scripts.salesforce_ofac_billing_preview import (
    BILLING_ACCOUNT_DATASET_ID,
    ACCOUNT_DATA_COLUMNS,
    BILLING_DATA_COLUMNS,
    COLUMNS_ORDER,
    build_normalized_account_and_billing_data,
)
from src.salesforce.resources.scripts.helpers.gl_source_join import DATASET_IDS


class TestConstants:
    def test_billing_account_dataset_id_matches_provided_value(self):
        assert BILLING_ACCOUNT_DATASET_ID == "78073a60-0e7a-40a5-b59a-fced1d6a89aa"

    def test_account_data_columns_maps_id_to_account_id(self):
        assert ACCOUNT_DATA_COLUMNS["id"] == "account_id"

    def test_billing_data_columns_maps_accountid_to_account_id(self):
        # both sides must land on the same join-key column name
        assert BILLING_DATA_COLUMNS["accountid"] == "account_id"

    def test_columns_order_leads_with_billing_account_name(self):
        assert COLUMNS_ORDER[0] == "billing_account_name"

    def test_columns_order_includes_all_original_account_columns(self):
        original_order = [
            "account_name", "parent_account_id", "shipping_addr_ln_1",
            "shipping_addr_ln_2", "billing_addr_ln_1", "billing_addr_ln_2",
            "billing_city", "billing_state_providence", "zip_code",
            "billing_country", "account_id", "type",
        ]
        for col in original_order:
            assert col in COLUMNS_ORDER


class TestBuildNormalizedAccountAndBillingData:
    def _mock_s3_returning(self, monkeypatch, account_df, billing_df):
        def fake_read_table(s3_client, bucket, dataset_id, source_prefix=None):
            if dataset_id == DATASET_IDS["account"]:
                return account_df
            if dataset_id == BILLING_ACCOUNT_DATASET_ID:
                return billing_df
            raise ValueError(f"unexpected dataset_id: {dataset_id}")

        monkeypatch.setattr(
            "src.salesforce.resources.scripts.salesforce_ofac_billing_preview.read_table_by_dataset_id",
            fake_read_table,
        )

    def _account_df(self):
        return pd.DataFrame([
            {"Id": "ACC1", "Name": "Acme Corp", "ParentId": None, "ShippingStreet": "1 Main St",
             "BillingStreet": "1 Main St", "BillingCity": "Denver", "BillingStateCode": "CO",
             "BillingPostalCode": "80202", "BillingCountryCode": "US", "Type": "Customer"},
            {"Id": "ACC2", "Name": "Beta LLC", "ParentId": None, "ShippingStreet": "2 Oak Ave",
             "BillingStreet": "2 Oak Ave", "BillingCity": "Aurora", "BillingStateCode": "CO",
             "BillingPostalCode": "80010", "BillingCountryCode": "US", "Type": "Customer"},
        ])

    def _billing_df(self):
        return pd.DataFrame([
            {"AccountId": "ACC1", "Name": "Acme Billing Entity"},
            {"AccountId": "ACC2", "Name": "Beta Billing Entity"},
        ])

    def test_joins_billing_account_name_onto_matching_account(self, monkeypatch):
        self._mock_s3_returning(monkeypatch, self._account_df(), self._billing_df())
        log = MagicMock()

        result = build_normalized_account_and_billing_data(log, MagicMock(), "bucket", "salesforce/reports")

        acc1_row = result[result["account_id"] == "ACC1"].iloc[0]
        assert acc1_row["billing_account_name"] == "Acme Billing Entity"

    def test_result_columns_match_columns_order(self, monkeypatch):
        self._mock_s3_returning(monkeypatch, self._account_df(), self._billing_df())
        log = MagicMock()

        result = build_normalized_account_and_billing_data(log, MagicMock(), "bucket", "salesforce/reports")

        assert list(result.columns) == COLUMNS_ORDER

    def test_account_with_no_matching_billing_row_gets_null_billing_name(self, monkeypatch):
        account_df = self._account_df()
        billing_df = pd.DataFrame([{"AccountId": "ACC1", "Name": "Acme Billing Entity"}])  # ACC2 missing
        self._mock_s3_returning(monkeypatch, account_df, billing_df)
        log = MagicMock()

        result = build_normalized_account_and_billing_data(log, MagicMock(), "bucket", "salesforce/reports")

        acc2_row = result[result["account_id"] == "ACC2"].iloc[0]
        # Confirmed directly: normalize_data()'s astype(str) does NOT
        # stringify a None/NaN cell here — it stays a real float NaN, not
        # the string "nan". Worth knowing if this ever gets treated as a
        # string downstream (e.g. written to CSV) without an explicit
        # null check first.
        assert pd.isna(acc2_row["billing_account_name"])

    def test_row_count_matches_account_when_billing_is_unique_per_account(self, monkeypatch):
        self._mock_s3_returning(monkeypatch, self._account_df(), self._billing_df())
        log = MagicMock()

        result = build_normalized_account_and_billing_data(log, MagicMock(), "bucket", "salesforce/reports")

        assert len(result) == 2  # same as account_df's row count

    def test_fan_out_when_billing_account_id_not_unique(self, monkeypatch):
        # ASSUMPTION CHECK: if BillingAccount has more than one row per
        # account_id, the merge fans out — this test documents that
        # behavior rather than silently hiding it. If this ever fires
        # unexpectedly against real data, it means the "unique per
        # account" assumption in the module docstring doesn't hold.
        account_df = self._account_df()
        billing_df = pd.DataFrame([
            {"AccountId": "ACC1", "Name": "Acme Billing Entity 1"},
            {"AccountId": "ACC1", "Name": "Acme Billing Entity 2"},  # duplicate account_id
        ])
        self._mock_s3_returning(monkeypatch, account_df, billing_df)
        log = MagicMock()

        result = build_normalized_account_and_billing_data(log, MagicMock(), "bucket", "salesforce/reports")

        acc1_rows = result[result["account_id"] == "ACC1"]
        assert len(acc1_rows) == 2  # fanned out, not deduplicated

    def test_applies_address_line_2_columns_from_normalize_data(self, monkeypatch):
        self._mock_s3_returning(monkeypatch, self._account_df(), self._billing_df())
        log = MagicMock()

        result = build_normalized_account_and_billing_data(log, MagicMock(), "bucket", "salesforce/reports")

        assert "shipping_addr_ln_2" in result.columns
        assert "billing_addr_ln_2" in result.columns

    def test_reads_both_datasets_with_correct_ids_and_prefix(self, monkeypatch):
        captured = []

        def fake_read_table(s3_client, bucket, dataset_id, source_prefix=None):
            captured.append((dataset_id, source_prefix))
            if dataset_id == DATASET_IDS["account"]:
                return self._account_df()
            return self._billing_df()

        monkeypatch.setattr(
            "src.salesforce.resources.scripts.salesforce_ofac_billing_preview.read_table_by_dataset_id",
            fake_read_table,
        )
        log = MagicMock()

        build_normalized_account_and_billing_data(log, MagicMock(), "bucket", "salesforce/reports")

        assert (DATASET_IDS["account"], "salesforce/reports") in captured
        assert (BILLING_ACCOUNT_DATASET_ID, "salesforce/reports") in captured

"""
Tests for helpers/s3_utils.py — the S3 read/write boundary shared between
salesforce_global_one.py and gl_source_join.py.

Run with: pytest test_s3_utils.py -v

Imports directly from src.salesforce.resources.scripts.helpers.s3_utils
rather than through salesforce_global_one.py's re-export of these
functions — these tests exercise s3_utils's own functions directly (same
pattern as test_gl_source_join.py testing gl_source_join directly), so
they don't depend on how any other script happens to import/re-export
them. Depending on that re-export was fragile: if salesforce_global_one.py's
own import block ever changes, tests relying on its re-export break even
though s3_utils.py itself never changed.

Covers:
  - find_dataset_prefix() / read_table_by_dataset_id(): dataset_id -> S3 key
  - read_jsonl_from_s3() / read_jsonl_prefix_from_s3(): mocked S3 reads,
    multi-file concat, empty-prefix handling
  - upload_to_s3() / write_success_file(): mocked S3 writes
  - build_partitioned_prefix(): year=/month=/day=/hour=/ path construction
  - save_validation_file(): parquet/csv validation output, partitioned path
"""

from datetime import datetime
from io import BytesIO
from unittest.mock import MagicMock

import pandas as pd
import pytest

from src.salesforce.resources.scripts.helpers.s3_utils import (
    read_jsonl_from_s3,
    read_jsonl_prefix_from_s3,
    find_dataset_prefix,
    read_table_by_dataset_id,
    upload_to_s3,
    write_success_file,
    build_partitioned_prefix,
    save_validation_file,
)


# ---------------------------------------------------------------------------
# find_dataset_prefix() — pure function
# ---------------------------------------------------------------------------

class TestFindDatasetPrefix:
    def test_default_pattern_matches_confirmed_s3_structure(self):
        # confirmed via S3 console browse: salesforce/reports/{dataset_id}/
        result = find_dataset_prefix("my-bucket", "abc-123")
        assert result == "salesforce/reports/abc-123/"

    def test_custom_source_prefix(self):
        # this is now the whole point — source_prefix is a single configurable
        # value from the YAML's source_key_prefix parameter, not assembled
        # here from separate vendor/segment guesses
        result = find_dataset_prefix("my-bucket", "abc-123", source_prefix="other/path")
        assert result == "other/path/abc-123/"

    def test_source_prefix_with_leading_and_trailing_slashes_stripped(self):
        result = find_dataset_prefix("my-bucket", "abc-123", source_prefix="/raw/salesforce/reports/")
        assert result == "raw/salesforce/reports/abc-123/"

    def test_always_ends_with_trailing_slash(self):
        result = find_dataset_prefix("my-bucket", "abc-123", source_prefix="anything")
        assert result.endswith("/")

    def test_no_double_slash_when_source_prefix_already_has_trailing_slash(self):
        result = find_dataset_prefix("my-bucket", "abc-123", source_prefix="salesforce/reports/")
        assert "//" not in result


# ---------------------------------------------------------------------------
# read_jsonl_from_s3() — mocked S3 client
# ---------------------------------------------------------------------------

class TestReadJsonlFromS3:
    def _make_s3_client(self, jsonl_bytes):
        s3 = MagicMock()
        s3.get_object.return_value = {"Body": BytesIO(jsonl_bytes)}
        return s3

    def test_reads_single_line_jsonl(self):
        content = b'{"Id": "A1", "Name": "Test"}\n'
        s3 = self._make_s3_client(content)
        df = read_jsonl_from_s3(s3, "bucket", "key.jsonl")
        assert len(df) == 1
        assert df.iloc[0]["Id"] == "A1"

    def test_reads_multi_line_jsonl(self):
        content = b'{"Id": "A1"}\n{"Id": "A2"}\n{"Id": "A3"}\n'
        s3 = self._make_s3_client(content)
        df = read_jsonl_from_s3(s3, "bucket", "key.jsonl")
        assert len(df) == 3
        assert list(df["Id"]) == ["A1", "A2", "A3"]

    def test_calls_get_object_with_correct_bucket_and_key(self):
        s3 = self._make_s3_client(b'{"Id": "A1"}\n')
        read_jsonl_from_s3(s3, "my-bucket", "path/to/file.jsonl")
        s3.get_object.assert_called_once_with(Bucket="my-bucket", Key="path/to/file.jsonl")

    def test_numeric_looking_string_column_stays_a_string(self):
        # REGRESSION: pandas' default JSON dtype inference silently
        # converts a column where every value looks like a plain integer
        # (e.g. a business unit code) into int64 — caught via an
        # end-to-end run against realistic fixture data, not by a
        # hand-built test DataFrame (which never round-trips through
        # pd.read_json and so never hits this coercion). Confirmed this
        # both drops leading zeros (see the next test) and breaks any
        # downstream string comparison against that column (e.g.
        # gl_source_join.py's apply_did_overrides() comparing
        # gl_accounting_number_c == "10040049").
        content = b'{"Business_Unit_BU__c": "10902"}\n{"Business_Unit_BU__c": "20450"}\n'
        s3 = self._make_s3_client(content)
        df = read_jsonl_from_s3(s3, "bucket", "key.jsonl")
        assert df["Business_Unit_BU__c"].iloc[0] == "10902"
        assert isinstance(df["Business_Unit_BU__c"].iloc[0], str)

    def test_leading_zeros_preserved_in_numeric_looking_column(self):
        content = b'{"Department_ID_DID__c": "00450"}\n'
        s3 = self._make_s3_client(content)
        df = read_jsonl_from_s3(s3, "bucket", "key.jsonl")
        assert df["Department_ID_DID__c"].iloc[0] == "00450"

    def test_all_columns_are_pandas_string_dtype(self):
        # Per Dakota: everything from these JSON files should be viewed as
        # dtype string. Actual columnar types (varchar/bool/timestamp_tz/
        # double/date/int per the DDL spreadsheets) are each downstream
        # consumer's responsibility to apply, not this reader's.
        content = b'{"IsDeleted": false, "Amount": 100.5, "Id": "A1", "Count": 3}\n'
        s3 = self._make_s3_client(content)
        df = read_jsonl_from_s3(s3, "bucket", "key.jsonl")
        for col in df.columns:
            assert df[col].dtype == "string", f"{col} is {df[col].dtype}, expected string"

    def test_native_json_boolean_is_stringified_not_left_as_bool(self):
        # dtype=False alone doesn't touch this -- a native JSON true/false
        # comes back as numpy bool without an explicit string cast.
        content = b'{"IsDeleted": false}\n{"IsDeleted": true}\n'
        s3 = self._make_s3_client(content)
        df = read_jsonl_from_s3(s3, "bucket", "key.jsonl")
        assert isinstance(df["IsDeleted"].iloc[0], str)
        assert isinstance(df["IsDeleted"].iloc[1], str)

    def test_native_json_number_is_stringified_not_left_as_float(self):
        content = b'{"Amount": 100.5}\n'
        s3 = self._make_s3_client(content)
        df = read_jsonl_from_s3(s3, "bucket", "key.jsonl")
        assert isinstance(df["Amount"].iloc[0], str)
        assert df["Amount"].iloc[0] == "100.5"

    def test_null_stays_a_real_null_not_stringified_to_none_text(self):
        # .astype("string") (StringDtype), not .astype(str) (python str) --
        # confirmed the latter would turn an actual null into the literal
        # text "None"/"nan", which would then look like real data
        # downstream instead of being caught by .isna()/.fillna().
        content = b'{"Description": null}\n'
        s3 = self._make_s3_client(content)
        df = read_jsonl_from_s3(s3, "bucket", "key.jsonl")
        assert df["Description"].isna().iloc[0]

    def test_downstream_numeric_conversion_still_works_on_string_dtype(self):
        # pd.to_numeric(errors="coerce") -- resolve_amount()'s own pattern
        # -- must still work against a StringDtype column with real nulls.
        content = b'{"Debit": "100.50"}\n{"Debit": null}\n'
        s3 = self._make_s3_client(content)
        df = read_jsonl_from_s3(s3, "bucket", "key.jsonl")
        numeric = pd.to_numeric(df["Debit"], errors="coerce")
        assert numeric.iloc[0] == 100.50
        assert pd.isna(numeric.iloc[1])

    def test_downstream_date_conversion_still_works_on_string_dtype(self):
        # pd.to_datetime() -- resolve_date_window()'s own pattern -- must
        # still work against a StringDtype ActivityDate column.
        content = b'{"ActivityDate": "2026-08-12T17:47:25.000+0000"}\n'
        s3 = self._make_s3_client(content)
        df = read_jsonl_from_s3(s3, "bucket", "key.jsonl")
        parsed = pd.to_datetime(df["ActivityDate"], utc=True)
        assert parsed.iloc[0].year == 2026
        assert parsed.iloc[0].month == 8
        assert parsed.iloc[0].day == 12


# ---------------------------------------------------------------------------
# read_jsonl_prefix_from_s3() — mocked paginator
# ---------------------------------------------------------------------------

class TestReadJsonlPrefixFromS3:
    def _make_s3_client(self, pages, file_contents):
        """
        pages: list of {"Contents": [{"Key": ...}, ...]} dicts
        file_contents: dict of key -> jsonl bytes
        """
        s3 = MagicMock()
        paginator = MagicMock()
        paginator.paginate.return_value = pages
        s3.get_paginator.return_value = paginator

        def get_object(Bucket, Key):
            return {"Body": BytesIO(file_contents[Key])}

        s3.get_object.side_effect = get_object
        return s3

    def test_concats_multiple_files_under_prefix(self):
        pages = [{"Contents": [{"Key": "p/part1.jsonl"}, {"Key": "p/part2.jsonl"}]}]
        contents = {
            "p/part1.jsonl": b'{"Id": "A1"}\n',
            "p/part2.jsonl": b'{"Id": "A2"}\n',
        }
        s3 = self._make_s3_client(pages, contents)
        df = read_jsonl_prefix_from_s3(s3, "bucket", "p/")
        assert len(df) == 2
        assert set(df["Id"]) == {"A1", "A2"}

    def test_handles_multiple_pages(self):
        pages = [
            {"Contents": [{"Key": "p/part1.jsonl"}]},
            {"Contents": [{"Key": "p/part2.jsonl"}]},
        ]
        contents = {
            "p/part1.jsonl": b'{"Id": "A1"}\n',
            "p/part2.jsonl": b'{"Id": "A2"}\n',
        }
        s3 = self._make_s3_client(pages, contents)
        df = read_jsonl_prefix_from_s3(s3, "bucket", "p/")
        assert len(df) == 2

    def test_ignores_non_jsonl_files(self):
        pages = [{"Contents": [{"Key": "p/data.jsonl"}, {"Key": "p/_SUCCESS"}, {"Key": "p/README.txt"}]}]
        contents = {"p/data.jsonl": b'{"Id": "A1"}\n'}
        s3 = self._make_s3_client(pages, contents)
        df = read_jsonl_prefix_from_s3(s3, "bucket", "p/")
        assert len(df) == 1

    def test_accepts_json_extension_too(self):
        pages = [{"Contents": [{"Key": "p/data.json"}]}]
        contents = {"p/data.json": b'{"Id": "A1"}\n'}
        s3 = self._make_s3_client(pages, contents)
        df = read_jsonl_prefix_from_s3(s3, "bucket", "p/")
        assert len(df) == 1

    def test_empty_prefix_returns_empty_df_with_expected_columns(self):
        pages = [{"Contents": []}]
        s3 = self._make_s3_client(pages, {})
        df = read_jsonl_prefix_from_s3(s3, "bucket", "p/", expected_columns=["Id", "Name"])
        assert len(df) == 0
        assert list(df.columns) == ["Id", "Name"]

    def test_empty_prefix_no_expected_columns_returns_empty_df(self):
        pages = [{"Contents": []}]
        s3 = self._make_s3_client(pages, {})
        df = read_jsonl_prefix_from_s3(s3, "bucket", "p/")
        assert len(df) == 0

    def test_zero_byte_file_reshapes_to_expected_columns(self):
        # REGRESSION: a 0-byte (or whitespace-only) file still counts as
        # "an object exists" to the paginator -- the `if not frames` check
        # alone doesn't catch this. Confirmed directly:
        # pd.read_json("", lines=True) returns shape (0, 0), zero COLUMNS
        # not just zero rows -- without this fix, df["Id"] below would
        # raise KeyError instead of behaving like any other empty table.
        pages = [{"Contents": [{"Key": "p/empty.jsonl"}]}]
        contents = {"p/empty.jsonl": b""}
        s3 = self._make_s3_client(pages, contents)
        df = read_jsonl_prefix_from_s3(s3, "bucket", "p/", expected_columns=["Id", "Name"])
        assert len(df) == 0
        assert list(df.columns) == ["Id", "Name"]
        assert len(df["Id"]) == 0  # does not raise KeyError

    def test_whitespace_only_file_reshapes_to_expected_columns(self):
        # Same failure mode as the 0-byte case above, via a nonzero-size
        # file (a stray newline/whitespace) rather than a literal 0 bytes
        # -- confirmed pd.read_json treats both identically (shape (0, 0)).
        pages = [{"Contents": [{"Key": "p/blank.jsonl"}]}]
        contents = {"p/blank.jsonl": b"\n"}
        s3 = self._make_s3_client(pages, contents)
        df = read_jsonl_prefix_from_s3(s3, "bucket", "p/", expected_columns=["Id", "Name"])
        assert len(df) == 0
        assert list(df.columns) == ["Id", "Name"]

    def test_all_files_under_prefix_empty_reshapes_to_expected_columns(self):
        # Multiple files, ALL empty -- not just a single empty file.
        pages = [{"Contents": [{"Key": "p/empty1.jsonl"}, {"Key": "p/empty2.jsonl"}]}]
        contents = {"p/empty1.jsonl": b"", "p/empty2.jsonl": b""}
        s3 = self._make_s3_client(pages, contents)
        df = read_jsonl_prefix_from_s3(s3, "bucket", "p/", expected_columns=["Id"])
        assert len(df) == 0
        assert list(df.columns) == ["Id"]

    def test_one_empty_file_mixed_with_real_data_does_not_lose_real_rows(self):
        # A real file alongside an empty one -- the empty file should
        # contribute nothing, not corrupt the real data's columns.
        pages = [{"Contents": [{"Key": "p/real.jsonl"}, {"Key": "p/empty.jsonl"}]}]
        contents = {"p/real.jsonl": b'{"Id": "A1", "Name": "x"}\n', "p/empty.jsonl": b""}
        s3 = self._make_s3_client(pages, contents)
        df = read_jsonl_prefix_from_s3(s3, "bucket", "p/", expected_columns=["Id", "Name"])
        assert len(df) == 1
        assert df.iloc[0]["Id"] == "A1"

    def test_page_with_no_contents_key_handled_gracefully(self):
        # Contents key can be absent entirely (e.g. a truly empty listing)
        pages = [{}]
        s3 = self._make_s3_client(pages, {})
        df = read_jsonl_prefix_from_s3(s3, "bucket", "p/", expected_columns=["Id"])
        assert len(df) == 0


# ---------------------------------------------------------------------------
# read_table_by_dataset_id() — combines find_dataset_prefix + prefix read
# ---------------------------------------------------------------------------

class TestReadTableByDatasetId:
    def test_builds_correct_prefix_and_reads(self):
        s3 = MagicMock()
        paginator = MagicMock()
        paginator.paginate.return_value = [{"Contents": [{"Key": "salesforce/reports/DSID1/data.jsonl"}]}]
        s3.get_paginator.return_value = paginator
        s3.get_object.return_value = {"Body": BytesIO(b'{"Id": "X1"}\n')}

        df = read_table_by_dataset_id(s3, "bucket", "DSID1")

        paginator.paginate.assert_called_once_with(Bucket="bucket", Prefix="salesforce/reports/DSID1/")
        assert len(df) == 1

    def test_passes_through_expected_columns_on_empty_result(self):
        s3 = MagicMock()
        paginator = MagicMock()
        paginator.paginate.return_value = [{"Contents": []}]
        s3.get_paginator.return_value = paginator

        df = read_table_by_dataset_id(s3, "bucket", "DSID1", expected_columns=["Id", "Name"])
        assert list(df.columns) == ["Id", "Name"]


# ---------------------------------------------------------------------------
# upload_to_s3() / write_success_file()
# ---------------------------------------------------------------------------

class TestUploadToS3:
    def test_uploads_with_correct_key(self):
        s3 = MagicMock()
        url = upload_to_s3(s3, "file content", "my-bucket", "outbound", "BX1_20260804.txt")
        s3.put_object.assert_called_once_with(
            Bucket="my-bucket", Key="outbound/BX1_20260804.txt", Body=b"file content"
        )
        assert url == "s3://my-bucket/outbound/BX1_20260804.txt"

    def test_strips_trailing_slash_from_prefix(self):
        s3 = MagicMock()
        upload_to_s3(s3, "x", "bucket", "outbound/", "file.txt")
        s3.put_object.assert_called_once_with(Bucket="bucket", Key="outbound/file.txt", Body=b"x")

    def test_encodes_content_as_utf8_bytes(self):
        s3 = MagicMock()
        upload_to_s3(s3, "héllo", "bucket", "prefix", "file.txt")
        call_kwargs = s3.put_object.call_args.kwargs
        assert call_kwargs["Body"] == "héllo".encode("utf-8")


class TestWriteSuccessFile:
    def test_writes_empty_success_marker(self):
        s3 = MagicMock()
        url = write_success_file(s3, "my-bucket", "outbound")
        s3.put_object.assert_called_once_with(Bucket="my-bucket", Key="outbound/_SUCCESS", Body=b"")
        assert url == "s3://my-bucket/outbound/_SUCCESS"

    def test_strips_trailing_slash(self):
        s3 = MagicMock()
        write_success_file(s3, "bucket", "outbound/")
        s3.put_object.assert_called_once_with(Bucket="bucket", Key="outbound/_SUCCESS", Body=b"")


# ---------------------------------------------------------------------------
# build_partitioned_prefix()
# ---------------------------------------------------------------------------

class TestBuildPartitionedPrefix:
    def test_includes_year_month_day_hour(self):
        result = build_partitioned_prefix("outbound", datetime(2026, 8, 4, 14, 32, 30))
        assert result == "outbound/year=2026/month=08/day=04/hour=14/"

    def test_strips_trailing_slash_from_base_prefix(self):
        result = build_partitioned_prefix("outbound/", datetime(2026, 1, 1, 5))
        assert "//" not in result

    def test_zero_pads_single_digit_hour(self):
        result = build_partitioned_prefix("outbound", datetime(2026, 1, 1, 3))
        assert "hour=03/" in result

    def test_different_hours_produce_different_partitions(self):
        result1 = build_partitioned_prefix("outbound", datetime(2026, 1, 1, 5))
        result2 = build_partitioned_prefix("outbound", datetime(2026, 1, 1, 17))
        assert "hour=05" in result1
        assert "hour=17" in result2
        assert result1 != result2

    def test_always_ends_with_trailing_slash(self):
        result = build_partitioned_prefix("outbound", datetime(2026, 1, 1))
        assert result.endswith("/")


# ---------------------------------------------------------------------------
# save_validation_file()
# ---------------------------------------------------------------------------

class TestSaveValidationFile:
    def test_key_follows_year_month_day_hour_partition_pattern(self):
        s3 = MagicMock()
        df = pd.DataFrame([{"a": 1}])
        dt = datetime(2026, 8, 4, 14, 32, 30)
        url = save_validation_file(s3, df, "bucket", "validation", creation_dt=dt)
        assert "year=2026/month=08/day=04/hour=14/" in url
        assert url.startswith("s3://bucket/validation/year=2026/month=08/day=04/hour=14/general_ledger_")
        assert url.endswith(".parquet")

    def test_filename_uses_epoch_timestamp(self):
        s3 = MagicMock()
        df = pd.DataFrame([{"a": 1}])
        dt = datetime(2026, 1, 30, 14, 2, 30)
        url = save_validation_file(s3, df, "bucket", "validation", creation_dt=dt)
        expected_epoch = int(dt.timestamp())
        assert f"general_ledger_{expected_epoch}.parquet" in url

    def test_strips_trailing_slash_from_base_prefix(self):
        s3 = MagicMock()
        df = pd.DataFrame([{"a": 1}])
        dt = datetime(2026, 1, 1)
        url = save_validation_file(s3, df, "bucket", "validation/", creation_dt=dt)
        assert "//" not in url.replace("s3://", "")

    def test_calls_put_object_with_parquet_bytes_by_default(self):
        s3 = MagicMock()
        df = pd.DataFrame([{"a": 1, "b": "x"}])
        save_validation_file(s3, df, "bucket", "validation", creation_dt=datetime(2026, 1, 1))
        call_kwargs = s3.put_object.call_args.kwargs
        assert call_kwargs["Bucket"] == "bucket"
        assert isinstance(call_kwargs["Body"], bytes)
        assert len(call_kwargs["Body"]) > 0

    def test_different_dates_produce_different_partitions(self):
        s3 = MagicMock()
        df = pd.DataFrame([{"a": 1}])
        url1 = save_validation_file(s3, df, "bucket", "validation", creation_dt=datetime(2026, 1, 1))
        url2 = save_validation_file(s3, df, "bucket", "validation", creation_dt=datetime(2026, 12, 25))
        assert "month=01/day=01" in url1
        assert "month=12/day=25" in url2

    def test_csv_file_type_produces_csv_extension(self):
        s3 = MagicMock()
        df = pd.DataFrame([{"a": 1}])
        url = save_validation_file(s3, df, "bucket", "validation", file_type="csv",
                                    creation_dt=datetime(2026, 1, 1))
        assert url.endswith(".csv")

    def test_csv_file_type_writes_actual_csv_content(self):
        s3 = MagicMock()
        df = pd.DataFrame([{"a": 1, "b": "x"}])
        save_validation_file(s3, df, "bucket", "validation", file_type="csv",
                              creation_dt=datetime(2026, 1, 1))
        body = s3.put_object.call_args.kwargs["Body"]
        assert b"a,b" in body  # CSV header row
        assert b"1,x" in body

    def test_file_type_case_insensitive(self):
        s3 = MagicMock()
        df = pd.DataFrame([{"a": 1}])
        url = save_validation_file(s3, df, "bucket", "validation", file_type="CSV",
                                    creation_dt=datetime(2026, 1, 1))
        assert url.endswith(".csv")

    def test_invalid_file_type_raises(self):
        s3 = MagicMock()
        df = pd.DataFrame([{"a": 1}])
        with pytest.raises(ValueError, match="Unsupported validation file_type"):
            save_validation_file(s3, df, "bucket", "validation", file_type="excel",
                                  creation_dt=datetime(2026, 1, 1))


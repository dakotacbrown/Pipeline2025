"""
Tests for helpers/onelake_writer.py — build_multipart_submission_body() (the
generic payload builder), s3_to_onelake()'s required file_submissions
parameter (no default — raises if empty/missing), and write_s3_to_onelake()'s
unchanged external behavior despite its internal call to s3_to_onelake
changing shape.

Run with: pytest test_onelake_writer.py -v
"""

import json
from unittest.mock import MagicMock, patch

import pytest

from src.salesforce.resources.scripts.helpers.onelake_writer import (
    build_multipart_submission_body,
    s3_to_onelake,
    write_s3_to_onelake,
)


# ---------------------------------------------------------------------------
# build_multipart_submission_body()
# ---------------------------------------------------------------------------

class TestBuildMultipartSubmissionBody:
    def test_requires_at_least_one_file_submission(self):
        with pytest.raises(ValueError, match="file_submissions"):
            build_multipart_submission_body("app", "schema", "role", [])

    def test_entry_missing_filename_raises(self):
        with pytest.raises(ValueError, match="fileName"):
            build_multipart_submission_body(
                "app", "schema", "role", [{"fileType": "CSV_WITH_HEADER"}]
            )

    def test_entry_missing_filetype_raises(self):
        with pytest.raises(ValueError, match="fileType"):
            build_multipart_submission_body(
                "app", "schema", "role", [{"fileName": "x.csv"}]
            )

    def test_top_level_shape_matches_confirmed_production_structure(self):
        # No "fileSubmissionDefinition" wrapper — confirmed against the
        # real, currently-working request body this file has always sent
        body = build_multipart_submission_body(
            "my_app", "my_schema", "my_role",
            [{"fileName": "f.csv", "fileSize": 100, "fileType": "CSV_WITH_HEADER"}],
        )
        assert body == {
            "businessApplication": "my_app",
            "schemaName": "my_schema",
            "awsIamRole": "my_role",
            "fileSubmissions": [
                {"fileName": "f.csv", "fileSize": 100, "fileType": "CSV_WITH_HEADER"}
            ],
        }

    def test_no_wrapper_around_each_entry(self):
        body = build_multipart_submission_body(
            "app", "schema", "role",
            [{"fileName": "f.csv", "fileSize": 1, "fileType": "CSV_WITH_HEADER"}],
        )
        entry = body["fileSubmissions"][0]
        assert "fileSubmissionDefinition" not in entry
        assert entry["fileName"] == "f.csv"  # top-level, not nested

    def test_two_files_different_types_in_one_body(self):
        # the actual motivating case: GL journal .txt (MULTI_RECORD_FIXED_WIDTH)
        # + validation .csv (CSV_WITH_HEADER) submitted together
        body = build_multipart_submission_body(
            "cadet_gl", "gl_journal_schema", "arn:aws:iam::role",
            [
                {
                    "fileName": "CS1_20260805144639.txt",
                    "fileSize": 45000,
                    "fileType": "MULTI_RECORD_FIXED_WIDTH",
                    "sourceFilePath": "s3://bucket/outbound/CS1_20260805144639.txt",
                    "decodeMetadata": {"fieldDefinitions": []},
                },
                {
                    "fileName": "general_ledger_1767243625.csv",
                    "fileSize": 12000,
                    "fileType": "CSV_WITH_HEADER",
                    "sourceFilePath": "s3://bucket/validation/general_ledger_1767243625.csv",
                },
            ],
        )
        assert len(body["fileSubmissions"]) == 2
        types = [e["fileType"] for e in body["fileSubmissions"]]
        assert types == ["MULTI_RECORD_FIXED_WIDTH", "CSV_WITH_HEADER"]

    def test_dict_decode_metadata_gets_json_serialized(self):
        body = build_multipart_submission_body(
            "app", "schema", "role",
            [{
                "fileName": "f.txt", "fileSize": 1, "fileType": "MULTI_RECORD_FIXED_WIDTH",
                "decodeMetadata": {"fieldDefinitions": [{"fieldName": "x", "position": 0, "width": 1}]},
            }],
        )
        dm = body["fileSubmissions"][0]["decodeMetadata"]
        assert isinstance(dm, str)
        assert json.loads(dm) == {"fieldDefinitions": [{"fieldName": "x", "position": 0, "width": 1}]}

    def test_string_decode_metadata_passed_through_unchanged(self):
        already_serialized = '{"fieldDefinitions": []}'
        body = build_multipart_submission_body(
            "app", "schema", "role",
            [{
                "fileName": "f.txt", "fileSize": 1, "fileType": "MULTI_RECORD_FIXED_WIDTH",
                "decodeMetadata": already_serialized,
            }],
        )
        assert body["fileSubmissions"][0]["decodeMetadata"] == already_serialized

    def test_arbitrary_extra_keys_pass_through(self):
        body = build_multipart_submission_body(
            "app", "schema", "role",
            [{
                "fileName": "f.csv", "fileSize": 1, "fileType": "CSV_WITH_HEADER",
                "overrideMultiPartSize": 0, "sourceFilePath": "s3://bucket/f.csv",
            }],
        )
        entry = body["fileSubmissions"][0]
        assert entry["overrideMultiPartSize"] == 0
        assert entry["sourceFilePath"] == "s3://bucket/f.csv"

    def test_does_not_mutate_caller_original_dict(self):
        original_entry = {
            "fileName": "f.txt", "fileSize": 1, "fileType": "MULTI_RECORD_FIXED_WIDTH",
            "decodeMetadata": {"a": 1},
        }
        build_multipart_submission_body("app", "schema", "role", [original_entry])
        assert original_entry["decodeMetadata"] == {"a": 1}


# ---------------------------------------------------------------------------
# s3_to_onelake() — file_submissions is required, no default
# ---------------------------------------------------------------------------

def _writer_config(**overrides):
    config = {
        "ba": "cadet_gl",
        "schema_name": "gl_journal_schema",
        "iam_role": "arn:aws:iam::role",
        "base_url": "https://exchange.example.com/api",
        "env": "qa",
        "region": "west",
        "file_name": "CS1_20260805144639.txt",
    }
    config.update(overrides)
    return config


class TestS3ToOnelakeRequiresFileSubmissions:
    def test_raises_when_file_submissions_is_none(self):
        log = MagicMock()
        with pytest.raises(ValueError, match="file_submissions"):
            s3_to_onelake(log, "token", _writer_config(), None)

    def test_raises_when_file_submissions_is_empty_list(self):
        log = MagicMock()
        with pytest.raises(ValueError, match="file_submissions"):
            s3_to_onelake(log, "token", _writer_config(), [])

    @patch("src.salesforce.resources.scripts.helpers.onelake_writer.requests.post")
    def test_does_not_call_post_when_file_submissions_missing(self, mock_post):
        log = MagicMock()
        with pytest.raises(ValueError):
            s3_to_onelake(log, "token", _writer_config(), [])
        mock_post.assert_not_called()


class TestS3ToOnelakeWithExplicitFileSubmissions:
    @patch("src.salesforce.resources.scripts.helpers.onelake_writer.requests.get")
    @patch("src.salesforce.resources.scripts.helpers.onelake_writer.requests.post")
    def test_single_file_submission(self, mock_post, mock_get):
        mock_post.return_value = MagicMock(status_code=200, json=lambda: {"fileSubmissionId": "abc"})
        mock_get.return_value = MagicMock(status_code=200, json=lambda: {"submissionStatus": "COMPLETED"})
        log = MagicMock()

        s3_to_onelake(
            log, "token", _writer_config(),
            [{
                "fileName": "f.csv", "fileSize": 100, "fileType": "CSV_WITH_HEADER",
                "sourceFilePath": "s3://bucket/f.csv",
            }],
        )

        sent_body = json.loads(mock_post.call_args.kwargs["data"])
        assert len(sent_body["fileSubmissions"]) == 1
        assert sent_body["fileSubmissions"][0]["sourceFilePath"] == "s3://bucket/f.csv"

    @patch("src.salesforce.resources.scripts.helpers.onelake_writer.requests.get")
    @patch("src.salesforce.resources.scripts.helpers.onelake_writer.requests.post")
    def test_uses_correct_url_and_headers(self, mock_post, mock_get):
        mock_post.return_value = MagicMock(status_code=200, json=lambda: {"fileSubmissionId": "abc"})
        mock_get.return_value = MagicMock(status_code=200, json=lambda: {"submissionStatus": "COMPLETED"})
        log = MagicMock()

        s3_to_onelake(
            log, "my-token", _writer_config(),
            [{"fileName": "f.csv", "fileSize": 1, "fileType": "CSV_WITH_HEADER"}],
        )

        assert mock_post.call_args.kwargs["url"] == "https://exchange.example.com/api/file-pull-submissions"
        headers = mock_post.call_args.kwargs["headers"]
        assert headers["Authorization"] == "Bearer my-token"
        assert headers["X-Upstream-Env"] == "qa-west"

    @patch("src.salesforce.resources.scripts.helpers.onelake_writer.requests.post")
    def test_raises_on_failed_submission_call(self, mock_post):
        mock_post.return_value = MagicMock(status_code=500, text="boom")
        log = MagicMock()

        with pytest.raises(Exception, match="Failed to call the File Puller"):
            s3_to_onelake(
                log, "token", _writer_config(),
                [{"fileName": "f.csv", "fileSize": 1, "fileType": "CSV_WITH_HEADER"}],
            )

    @patch("src.salesforce.resources.scripts.helpers.onelake_writer.requests.get")
    @patch("src.salesforce.resources.scripts.helpers.onelake_writer.requests.post")
    def test_two_files_different_types_in_one_session(self, mock_post, mock_get):
        mock_post.return_value = MagicMock(status_code=200, json=lambda: {"fileSubmissionId": "abc"})
        mock_get.return_value = MagicMock(status_code=200, json=lambda: {"submissionStatus": "COMPLETED"})
        log = MagicMock()

        file_submissions = [
            {
                "fileName": "CS1_20260805144639.txt",
                "fileSize": 45000,
                "fileType": "MULTI_RECORD_FIXED_WIDTH",
                "sourceFilePath": "s3://bucket/outbound/CS1_20260805144639.txt",
                "decodeMetadata": {"fieldDefinitions": []},
            },
            {
                "fileName": "general_ledger_1767243625.csv",
                "fileSize": 12000,
                "fileType": "CSV_WITH_HEADER",
                "sourceFilePath": "s3://bucket/validation/general_ledger_1767243625.csv",
            },
        ]

        s3_to_onelake(log, "token", _writer_config(), file_submissions)

        sent_body = json.loads(mock_post.call_args.kwargs["data"])
        assert len(sent_body["fileSubmissions"]) == 2
        types = [e["fileType"] for e in sent_body["fileSubmissions"]]
        assert types == ["MULTI_RECORD_FIXED_WIDTH", "CSV_WITH_HEADER"]

    @patch("src.salesforce.resources.scripts.helpers.onelake_writer.requests.get")
    @patch("src.salesforce.resources.scripts.helpers.onelake_writer.requests.post")
    def test_still_polls_for_completion(self, mock_post, mock_get):
        mock_post.return_value = MagicMock(status_code=200, json=lambda: {"fileSubmissionId": "xyz"})
        mock_get.return_value = MagicMock(status_code=200, json=lambda: {"submissionStatus": "COMPLETED"})
        log = MagicMock()

        s3_to_onelake(
            log, "token", _writer_config(),
            [{"fileName": "a.csv", "fileSize": 1, "fileType": "CSV_WITH_HEADER"}],
        )

        mock_get.assert_called_once()
        assert "xyz" in mock_get.call_args.kwargs["url"]


# ---------------------------------------------------------------------------
# write_s3_to_onelake() — external behavior unchanged despite s3_to_onelake's
# new required parameter; it now builds file_submissions explicitly internally
# ---------------------------------------------------------------------------

class TestWriteS3ToOnelake:
    def test_builds_single_csv_file_submission_and_calls_s3_to_onelake(self, monkeypatch):
        captured = {}

        def fake_s3_to_onelake(log, oauth_token, writer_config, file_submissions):
            captured["oauth_token"] = oauth_token
            captured["writer_config"] = writer_config
            captured["file_submissions"] = file_submissions

        monkeypatch.setattr(
            "src.salesforce.resources.scripts.helpers.onelake_writer.s3_to_onelake",
            fake_s3_to_onelake,
        )

        fake_df = MagicMock()
        writer_config = {
            "bucket": "my-bucket", "prefix": "outbound", "file_name": "report.csv",
        }

        write_s3_to_onelake(MagicMock(), fake_df, writer_config, "token123")

        fake_df.to_csv.assert_called_once()
        assert captured["oauth_token"] == "token123"
        assert len(captured["file_submissions"]) == 1
        entry = captured["file_submissions"][0]
        assert entry["fileName"] == "report.csv"
        assert entry["fileType"] == "CSV_WITH_HEADER"
        assert entry["sourceFilePath"] == "s3://my-bucket/outbound/report.csv"

    def test_writes_csv_to_correct_s3_location_with_encryption_kwargs(self, monkeypatch):
        monkeypatch.setattr(
            "src.salesforce.resources.scripts.helpers.onelake_writer.s3_to_onelake",
            lambda *a, **k: None,
        )
        fake_df = MagicMock()
        writer_config = {"bucket": "b", "prefix": "p", "file_name": "f.csv"}

        write_s3_to_onelake(MagicMock(), fake_df, writer_config, "token")

        call_args = fake_df.to_csv.call_args
        assert call_args.args[0] == "s3://b/p/f.csv"
        assert call_args.kwargs["storage_options"]["s3_additional_kwargs"]["ServerSideEncryption"] == "AES256"



class TestBuildMultipartSubmissionBodyOptionalFields:
    def test_data_lake_copy_subfolder_omitted_by_default(self):
        body = build_multipart_submission_body(
            "app", "schema", "role",
            [{"fileName": "f.csv", "fileSize": 1, "fileType": "CSV_WITH_HEADER"}],
        )
        assert "dataLakeCopySubfolder" not in body

    def test_data_lake_copy_subfolder_included_when_given(self):
        body = build_multipart_submission_body(
            "app", "schema", "role",
            [{"fileName": "f.csv", "fileSize": 1, "fileType": "CSV_WITH_HEADER"}],
            data_lake_copy_subfolder="some/subfolder",
        )
        assert body["dataLakeCopySubfolder"] == "some/subfolder"

    def test_additional_fields_omitted_by_default(self):
        body = build_multipart_submission_body(
            "app", "schema", "role",
            [{"fileName": "f.csv", "fileSize": 1, "fileType": "CSV_WITH_HEADER"}],
        )
        assert "additionalFields" not in body

    def test_additional_fields_included_when_given(self):
        body = build_multipart_submission_body(
            "app", "schema", "role",
            [{"fileName": "f.csv", "fileSize": 1, "fileType": "CSV_WITH_HEADER"}],
            additional_fields=[{"name": "env", "value": "qa"}],
        )
        assert body["additionalFields"] == [{"name": "env", "value": "qa"}]


class TestS3ToOnelakeOptionalFieldsPassthrough:
    @patch("src.salesforce.resources.scripts.helpers.onelake_writer.requests.get")
    @patch("src.salesforce.resources.scripts.helpers.onelake_writer.requests.post")
    def test_data_lake_copy_subfolder_reaches_request_body(self, mock_post, mock_get):
        mock_post.return_value = MagicMock(status_code=200, json=lambda: {"fileSubmissionId": "abc"})
        mock_get.return_value = MagicMock(status_code=200, json=lambda: {"submissionStatus": "COMPLETED"})
        log = MagicMock()

        s3_to_onelake(
            log, "token", _writer_config(),
            [{"fileName": "f.csv", "fileSize": 1, "fileType": "CSV_WITH_HEADER"}],
            data_lake_copy_subfolder="sub/folder",
        )

        sent_body = json.loads(mock_post.call_args.kwargs["data"])
        assert sent_body["dataLakeCopySubfolder"] == "sub/folder"

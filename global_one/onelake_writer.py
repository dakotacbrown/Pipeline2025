import json
import sys
import time
import uuid
from logging import Logger
from pathlib import Path
from typing import Dict

import numpy as np
import requests
from pandas import DataFrame
from requests import HTTPError

BASE_DIR = Path(__file__).resolve()
BASE_DIR = BASE_DIR.parent.parent.parent.parent

sys.path.insert(0, str(BASE_DIR))

BODY_TYPE = "application/json;v=1"
OS_CERT_PATH = f"{BASE_DIR}/cert/one-stream-cert-bundle.pem"


def write_s3_to_onelake(
    log: Logger,
    dataframe: DataFrame,
    writer_config: Dict[str, str],
    oauth_token: str,
):
    file_name = writer_config["file_name"]

    file_location = (
        f"s3://{writer_config['bucket']}/{writer_config['prefix']}/{file_name}"
    )

    dataframe.to_csv(
        file_location,
        sep=",",
        index=False,
        storage_options={
            "s3_additional_kwargs": {
                "ServerSideEncryption": "AES256",
                "ACL": "bucket-owner-full-control",
            },
        },
    )

    # s3_to_onelake no longer has a default — build the equivalent single-CSV
    # entry explicitly here instead. This keeps write_s3_to_onelake's own
    # behavior and external signature exactly as before; only the internal
    # implementation changed to match s3_to_onelake's new required parameter.
    file_submissions = [
        {
            "fileName": file_name,
            "fileSize": 0,
            "fileType": "CSV_WITH_HEADER",
            "overrideMultiPartSize": 0,
            "sourceFilePath": file_location,
        }
    ]
    s3_to_onelake(log, oauth_token, writer_config, file_submissions)


def build_multipart_submission_body(
    business_application: str,
    schema_name: str,
    aws_iam_role: str,
    file_submissions: list,
    data_lake_copy_subfolder: str = None,
    additional_fields: list = None,
) -> Dict:
    """
    Builds the request body for POST {base_url}/file-pull-submissions.

    No "fileSubmissionDefinition" wrapper around each fileSubmissions entry —
    confirmed against this file's own actual working request body (the
    Exchange docs page's worked example shows a wrapper that doesn't match
    what this function has always sent to production).

    NOTE: awsIamRole is sent here because it's confirmed present in this
    file's actual, working request body — but it isn't listed in the
    documented payload-properties table (businessApplication/schemaName/
    dataLakeCopySubfolder/additionalFields/fileSubmissions only). Kept
    since real working code outranks a possibly-incomplete docs table, but
    worth knowing if a submission ever fails on an unexpected-field
    validation — that mismatch is the first place to look.

    file_submissions: list of dicts, one per file, each with at minimum
    fileName/fileSize/fileType, plus whatever else that fileType needs
    (decodeMetadata for MULTI_RECORD_FIXED_WIDTH, sourceFilePath,
    overrideMultiPartSize, etc.) — passed through unchanged, since this
    function doesn't need to know what a given fileType requires. This is
    what lets one call submit multiple files together (e.g. the GL journal
    .txt as MULTI_RECORD_FIXED_WIDTH alongside a validation .csv as
    CSV_WITH_HEADER) in a single session, since fileSubmissions is an array
    and each entry can have its own fileType.

    decodeMetadata, if present in an entry as a dict, gets JSON-serialized
    to a string automatically — every example seen embeds it as a string
    value, not a nested object.

    data_lake_copy_subfolder / additional_fields: the two remaining
    documented optional top-level properties (subfolder path for Direct
    Write sink schemas; array of additional fields added to every record).
    Neither salesforce_ofac.py nor salesforce_global_one.py currently use
    these — omitted from the request body entirely unless provided, so
    passing nothing here changes nothing about existing behavior.
    """
    if not file_submissions:
        raise ValueError("file_submissions must contain at least one entry")

    prepared_submissions = []
    for entry in file_submissions:
        if "fileName" not in entry or "fileType" not in entry:
            raise ValueError(f"file_submissions entry missing fileName/fileType: {entry!r}")
        prepared = dict(entry)  # shallow copy — don't mutate the caller's dict
        if "decodeMetadata" in prepared and isinstance(prepared["decodeMetadata"], dict):
            prepared["decodeMetadata"] = json.dumps(prepared["decodeMetadata"])
        prepared_submissions.append(prepared)

    body = {
        "businessApplication": business_application,
        "schemaName": schema_name,
        "awsIamRole": aws_iam_role,
        "fileSubmissions": prepared_submissions,
    }
    if data_lake_copy_subfolder is not None:
        body["dataLakeCopySubfolder"] = data_lake_copy_subfolder
    if additional_fields is not None:
        body["additionalFields"] = additional_fields

    return body


def s3_to_onelake(
    log: Logger,
    oauth_token: str,
    writer_config: Dict[str, str],
    file_submissions: list,
    data_lake_copy_subfolder: str = None,
    additional_fields: list = None,
):
    """
    file_submissions: required — a list of dicts, each describing one file
    to submit (see build_multipart_submission_body's docstring for the
    shape). Must contain at least one entry.

    There's no default fallback anymore. There used to be one, built
    implicitly from a file_location parameter this function no longer
    takes — but a silent default here is exactly the kind of implicit
    behavior that's caused real bugs elsewhere in this project (the S3
    prefix guess, the region mismatch): it doesn't fail loudly, it just
    submits whatever the default happened to be, which may not be what the
    caller actually wanted. Every caller now states explicitly what it's
    submitting.

    data_lake_copy_subfolder / additional_fields: passed straight through
    to build_multipart_submission_body — see its docstring. Neither is
    currently used by any caller in this repo.
    """
    if not file_submissions:
        raise ValueError(
            "file_submissions must be provided and contain at least one entry "
            "— there is no default."
        )

    headers = {
        "Authorization": f"Bearer {oauth_token}",
        "Content-Type": BODY_TYPE,
        "Accept": BODY_TYPE,
        "X-Upstream-Env": f"{writer_config['env']}-{writer_config['region']}",
    }

    body = build_multipart_submission_body(
        business_application=writer_config["ba"],
        schema_name=writer_config["schema_name"],
        aws_iam_role=writer_config["iam_role"],
        file_submissions=file_submissions,
        data_lake_copy_subfolder=data_lake_copy_subfolder,
        additional_fields=additional_fields,
    )

    log.info(
        f"Submitting data as file to schema {writer_config['schema_name']}."
    )

    response = requests.post(
        url=f"{writer_config['base_url']}/file-pull-submissions",
        headers=headers,
        data=json.dumps(body),
    )
    if response.status_code >= 300:
        raise HTTPError(f"Failed to call the File Puller: {response.text}")

    file_submission_id = response.json()["fileSubmissionId"]

    _poll_file_submission_until_complete(
        log, file_submission_id, headers, writer_config["base_url"]
    )
    log.info(
        f"Successfully submitted data to schema {writer_config['schema_name']}."
    )


def write_direct_to_onelake(
    log: Logger,
    dataframe: DataFrame,
    writer_config: Dict[str, str],
    oauth_token: str,
):
    headers = {
        "Authorization": f"Bearer {oauth_token}",
        "Content-Type": BODY_TYPE,
        "Accept": BODY_TYPE,
        "X-Upstream-Env": f"{writer_config['env']}-{writer_config['region']}",
    }

    batches = np.array_split(dataframe, np.ceil(len(dataframe) / 500))
    session_id = str(uuid.uuid4())
    sequence_number = 0
    for batch in batches:

        messages = batch.to_dict(orient="records")

        body = {
            "businessApplication": writer_config["ba"],
            "schemaName": writer_config["schema_name"],
            "sessionId": session_id,
            "sessionStartingSequenceNumber": sequence_number,
            "messages": messages,
        }

        log.info(
            f"Submitting batch of {len(messages)} records to schema {writer_config['schema_name']}."  # noqa
        )

        response = requests.post(
            url=f"{writer_config['base_url']}/submit-data",
            headers=headers,
            data=json.dumps(body),
        )

        if response.status_code >= 300:
            raise HTTPError(f"Failed to submit data directly: {response.text}")

        log.info(
            f"Successfully submitted data to schema {writer_config['schema_name']}."
        )

        sequence_number += len(messages)


def _poll_file_submission_until_complete(
    log: Logger, file_submission_id: str, headers: Dict[str, str], base_url: str
):
    status = _get_status_of_submission(
        log, file_submission_id, headers, base_url
    )
    while status != "COMPLETED":
        if status in [
            "TOKENIZATION_FAILED",
            "VALIDATION_FAILED",
            "FAILED",
            "USER_CANCELED",
            "ABORTED",
            "QUARANTINED",
            "UNDER_PLATFORM_REVIEW",
        ]:
            raise HTTPError(f"Failed to upload file: {status}")
        time.sleep(5)
        status = _get_status_of_submission(
            log, file_submission_id, headers, base_url
        )


def _get_status_of_submission(
    log: Logger, file_submission_id: str, headers: Dict[str, str], base_url: str
) -> str:
    response = requests.get(
        url=f"{base_url}/file-submissions/{file_submission_id}",
        headers=headers,
    )

    if response.status_code < 300:
        status = response.json()["submissionStatus"]
        log.info(f"Submission status response: {response.json()}")
        return status
    else:
        raise HTTPError(f"Failed to get status of submission: {response.text}")

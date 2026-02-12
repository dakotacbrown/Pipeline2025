import unittest
from unittest.mock import MagicMock, patch

from dags.common.dag_utilities import _generate_job_execution_status


class TestGenerateJobExecutionStatus(unittest.TestCase):
    @patch("dags.common.dag_utilities.print_dt_utc")
    @patch("dags.common.dag_utilities.time")
    def test_generate_job_execution_status_success(
        self, mock_time, mock_print_dt_utc
    ):
        # deterministic time
        mock_time.time_ns.return_value = 123_000_000  # ns
        mock_print_dt_utc.return_value = "1970-01-01 00:00:00.123"

        task_group_id = "task_group_id"

        # XComs
        generate_params_xcom = [
            {"job_family": "test_family", "job_name": "test_job"}
        ]
        try_sql_xcom = [
            ("start_ts", "end_ts", 10, 2)
        ]  # list form (your handlers use sql_xcom[0])

        def xcom_side_effect(*args, **kwargs):
            task_ids = kwargs.get("task_ids")
            if task_ids == f"{task_group_id}.generate_params":
                return generate_params_xcom
            if task_ids == f"{task_group_id}.try_sql":
                return try_sql_xcom
            return None

        mock_ti = MagicMock()
        mock_ti.xcom_pull.side_effect = xcom_side_effect

        captured = {}

        def mock_success_handler(**kwargs):
            # confirm sql_xcom gets passed on success
            captured["sql_xcom"] = kwargs.get("sql_xcom")
            captured["job_run_params"] = kwargs.get("job_run_params")
            return {"final_state": "success", "records_published": 12}

        def mock_failure_handler(**kwargs):
            raise AssertionError(
                "Failure handler should not run on success path"
            )

        result = _generate_job_execution_status(
            task_group_id=task_group_id,
            on_success=mock_success_handler,
            job_execution_status_table="test_table",
            on_failure=mock_failure_handler,
            ti=mock_ti,
        )

        expected = [
            {
                "job_family": "test_family",
                "job_name": "test_job",
                "run_end_timestamp": "1970-01-01 00:00:00.123",
                "job_execution_status_table": "test_table",
                "final_state": "success",
                "records_published": 12,
            }
        ]

        self.assertEqual(result, expected)

        # ensure we passed through the list-form xcom untouched (for sql_success_handler compatibility)
        self.assertEqual(captured["sql_xcom"], try_sql_xcom)
        self.assertEqual(captured["job_run_params"], result[0])

    @patch("dags.common.dag_utilities.print_dt_utc")
    @patch("dags.common.dag_utilities.time")
    def test_generate_job_execution_status_failure(
        self, mock_time, mock_print_dt_utc
    ):
        mock_time.time_ns.return_value = 123_000_000
        mock_print_dt_utc.return_value = "1970-01-01 00:00:00.123"

        task_group_id = "task_group_id"

        # generate_params exists but try_sql produced no XCom => treat as failure
        generate_params_xcom = [
            {"job_family": "test_family", "job_name": "test_job"}
        ]
        try_sql_xcom = None

        def xcom_side_effect(*args, **kwargs):
            task_ids = kwargs.get("task_ids")
            if task_ids == f"{task_group_id}.generate_params":
                return generate_params_xcom
            if task_ids == f"{task_group_id}.try_sql":
                return try_sql_xcom
            return None

        mock_ti = MagicMock()
        mock_ti.xcom_pull.side_effect = xcom_side_effect

        captured = {}

        def mock_success_handler(**kwargs):
            raise AssertionError(
                "Success handler should not run on failure path"
            )

        def mock_failure_handler(**kwargs):
            captured["job_run_params"] = kwargs.get("job_run_params")
            # sql_xcom should NOT be required on failure path
            self.assertNotIn("sql_xcom", kwargs)
            return {
                "final_state": "ERROR",
                "failure_message": "SQL task failed",
            }

        result = _generate_job_execution_status(
            task_group_id=task_group_id,
            on_success=mock_success_handler,
            job_execution_status_table="test_table",
            on_failure=mock_failure_handler,
            ti=mock_ti,
        )

        expected = [
            {
                "job_family": "test_family",
                "job_name": "test_job",
                "run_end_timestamp": "1970-01-01 00:00:00.123",
                "job_execution_status_table": "test_table",
                "final_state": "ERROR",
                "failure_message": "SQL task failed",
            }
        ]

        self.assertEqual(result, expected)
        self.assertEqual(captured["job_run_params"], result[0])

    @patch("dags.common.dag_utilities.print_dt_utc")
    @patch("dags.common.dag_utilities.time")
    def test_generate_job_execution_status_raises_when_generate_params_empty_list(
        self, mock_time, mock_print_dt_utc
    ):
        mock_time.time_ns.return_value = 123_000_000
        mock_print_dt_utc.return_value = "1970-01-01 00:00:00.123"

        task_group_id = "task_group_id"

        def xcom_side_effect(*args, **kwargs):
            task_ids = kwargs.get("task_ids")
            if task_ids == f"{task_group_id}.generate_params":
                return []  # invalid
            if task_ids == f"{task_group_id}.try_sql":
                return None
            return None

        mock_ti = MagicMock()
        mock_ti.xcom_pull.side_effect = xcom_side_effect

        with self.assertRaises(ValueError):
            _generate_job_execution_status(
                task_group_id=task_group_id,
                on_success=lambda **_: {},
                job_execution_status_table="test_table",
                on_failure=lambda **_: {},
                ti=mock_ti,
            )

    @patch("dags.common.dag_utilities.print_dt_utc")
    @patch("dags.common.dag_utilities.time")
    def test_generate_job_execution_status_raises_when_handler_returns_non_dict(
        self, mock_time, mock_print_dt_utc
    ):
        mock_time.time_ns.return_value = 123_000_000
        mock_print_dt_utc.return_value = "1970-01-01 00:00:00.123"

        task_group_id = "task_group_id"

        def xcom_side_effect(*args, **kwargs):
            task_ids = kwargs.get("task_ids")
            if task_ids == f"{task_group_id}.generate_params":
                return [{"job_family": "test_family", "job_name": "test_job"}]
            if task_ids == f"{task_group_id}.try_sql":
                return [("start_ts", "end_ts", 10, 2)]
            return None

        mock_ti = MagicMock()
        mock_ti.xcom_pull.side_effect = xcom_side_effect

        def bad_success_handler(**kwargs):
            return ["not", "a", "dict"]

        with self.assertRaises(ValueError):
            _generate_job_execution_status(
                task_group_id=task_group_id,
                on_success=bad_success_handler,
                job_execution_status_table="test_table",
                on_failure=lambda **_: {},
                ti=mock_ti,
            )

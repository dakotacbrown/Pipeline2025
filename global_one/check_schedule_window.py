"""
Entry point for the CHECK_SCHEDULE_WINDOW task — determines whether
today (Eastern time, matching the job's own schedule timezone) falls
within the scheduled run window (the first 3 or last 5 business days of
the current month, excluding weekends and US federal holidays — see
helpers/schedule_window.py) and sets a Databricks task value that
EVALUATE_SCHEDULE_WINDOW (a condition_task) reads to decide whether
INGEST_SALESFORCE (and everything that depends on it) should run today.

Accepts start_date/end_date as optional positional parameters — per
Dakota, "[start_date/end_date] should also feed into this job." A
deliberate manual/backfill run that supplies an explicit date range is
NOT the same thing as the daily automatic schedule; the business-day/
holiday window check exists to skip unnecessary AUTOMATIC daily runs, not
to block a human's explicit request to run for a specific range. So: if
either start_date or end_date is non-empty, the window check is bypassed
entirely and the job always proceeds. Same "" (not omitted) shape as
salesforce_global_one.py's own start_date/end_date handling — Databricks
job parameters always render as strings, this job's default to "", and
that "" must be treated as "no override," not passed through as a
literal value.

Sets task value "in_run_window" to the literal string "true" or "false"
(not a Python bool) — condition_task's comparison operates on the
rendered Jinja string ("{{tasks.CHECK_SCHEDULE_WINDOW.values.in_run_window}}"),
so an explicit lowercase string avoids any ambiguity about how a bool
would render.
"""

import sys
from datetime import date, datetime
from zoneinfo import ZoneInfo

from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils

import helpers.schedule_window as schedule_window


def resolve_should_run(start_date: str, end_date: str, today_eastern: date) -> "tuple[bool, str]":
    """
    Pure decision logic, no Databricks dependencies — kept separate from
    main() so it's directly unit-testable without mocking Spark/dbutils.

    Returns (should_run, reason) — reason is a human-readable string for
    the task's log output, not consumed by anything downstream.
    """
    if start_date or end_date:
        return True, f"explicit date override present (start_date={start_date!r}, end_date={end_date!r})"
    should_run = schedule_window.in_scheduled_run_window(today_eastern)
    return should_run, f"{today_eastern} ({today_eastern.strftime('%A')}, America/New_York)"


def main():
    spark = SparkSession.builder.getOrCreate()
    dbutils = DBUtils(spark)

    args = sys.argv[1:]
    start_date = args[0] if len(args) > 0 else ""
    end_date = args[1] if len(args) > 1 else ""
    today_eastern = datetime.now(ZoneInfo("America/New_York")).date()

    should_run, reason = resolve_should_run(start_date, end_date, today_eastern)
    print(f"{reason}: in_run_window={should_run}")

    dbutils.jobs.taskValues.set(key="in_run_window", value="true" if should_run else "false")


if __name__ == "__main__":
    main()

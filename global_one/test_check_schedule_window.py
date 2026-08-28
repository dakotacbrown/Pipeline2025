"""
Tests for check_schedule_window.py's resolve_should_run() — the pure
decision logic behind the CHECK_SCHEDULE_WINDOW task, kept separate from
main() so it's testable without mocking Spark/dbutils.
"""

from datetime import date

from src.salesforce.resources.scripts.check_schedule_window import resolve_should_run


class TestResolveShouldRun:
    def test_no_override_in_window_day_runs(self):
        should_run, reason = resolve_should_run("", "", date(2026, 1, 2))  # 1st business day of Jan 2026
        assert should_run is True
        assert "explicit date override" not in reason

    def test_no_override_out_of_window_day_does_not_run(self):
        should_run, reason = resolve_should_run("", "", date(2026, 1, 15))  # mid-month
        assert should_run is False
        assert "explicit date override" not in reason

    def test_start_date_override_always_runs_even_on_out_of_window_day(self):
        # A deliberate manual/backfill run with an explicit date range
        # must NOT be blocked by the automatic-schedule window check --
        # per Dakota: "[start_date/end_date] should also feed into this
        # job." date(2026, 1, 15) is deliberately a day that would
        # otherwise fail the window check on its own.
        should_run, reason = resolve_should_run("2026-06-01", "", date(2026, 1, 15))
        assert should_run is True
        assert "explicit date override" in reason

    def test_end_date_override_alone_also_bypasses_the_check(self):
        # Only end_date supplied (start_date empty) still counts as an
        # override -- either one present is enough, matching
        # salesforce_global_one.py's own "both or neither" pairing being
        # a separate concern (resolve_date_window() validates that; this
        # function only needs to know whether ANY override is present).
        should_run, reason = resolve_should_run("", "2026-06-05", date(2026, 1, 15))
        assert should_run is True
        assert "explicit date override" in reason

    def test_both_dates_override_bypasses_the_check(self):
        should_run, reason = resolve_should_run("2026-06-01", "2026-06-05", date(2026, 1, 15))
        assert should_run is True

    def test_override_on_an_in_window_day_still_runs_for_the_override_reason(self):
        # Even when today WOULD have passed the window check anyway, an
        # explicit override should still be reported as the reason --
        # confirms the override branch is checked first, not as a
        # fallback only consulted after the window check fails.
        should_run, reason = resolve_should_run("2026-06-01", "2026-06-05", date(2026, 1, 2))
        assert should_run is True
        assert "explicit date override" in reason

    def test_weekend_with_no_override_does_not_run(self):
        should_run, reason = resolve_should_run("", "", date(2026, 1, 17))  # a Saturday
        assert should_run is False

    def test_federal_holiday_with_no_override_does_not_run(self):
        should_run, reason = resolve_should_run("", "", date(2026, 1, 19))  # MLK Day 2026
        assert should_run is False

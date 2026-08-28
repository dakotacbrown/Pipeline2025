"""
Tests for helpers/schedule_window.py — US federal holidays computed via
pure date-arithmetic rules, and the business-day / scheduled-run-window
logic built on top of them.
"""

from datetime import date

from src.salesforce.resources.scripts.helpers.schedule_window import (
    us_federal_holidays,
    is_business_day,
    business_days_of_month,
    in_scheduled_run_window,
)


class TestUsFederalHolidays:
    def test_2026_matches_known_dates_exactly(self):
        # Independently hand-computed against the actual 2026 calendar —
        # not derived from this module's own logic, so this is a real
        # check, not a tautology.
        expected = {
            date(2026, 1, 1),   # New Year's Day (Thursday, no shift)
            date(2026, 1, 19),  # MLK Day (3rd Monday of January)
            date(2026, 2, 16),  # Washington's Birthday (3rd Monday of February)
            date(2026, 5, 25),  # Memorial Day (last Monday of May)
            date(2026, 6, 19),  # Juneteenth (Friday, no shift)
            date(2026, 7, 3),   # Independence Day (Jul 4 is Saturday -> observed Friday)
            date(2026, 9, 7),   # Labor Day (1st Monday of September)
            date(2026, 10, 12), # Columbus Day (2nd Monday of October)
            date(2026, 11, 11), # Veterans Day (Wednesday, no shift)
            date(2026, 11, 26), # Thanksgiving (4th Thursday of November)
            date(2026, 12, 25), # Christmas Day (Friday, no shift)
        }
        assert us_federal_holidays(2026) == expected

    def test_returns_exactly_11_holidays(self):
        assert len(us_federal_holidays(2026)) == 11

    def test_saturday_holiday_observed_on_preceding_friday(self):
        # Independence Day 2026 falls on a Saturday.
        assert date(2026, 7, 4).weekday() == 5
        assert date(2026, 7, 3) in us_federal_holidays(2026)
        assert date(2026, 7, 4) not in us_federal_holidays(2026)

    def test_sunday_holiday_observed_on_following_monday(self):
        # Juneteenth 2027 falls on a Saturday and Christmas 2033 falls on
        # a Sunday -- use a year with a confirmed Sunday-fixed-holiday.
        # Christmas 2022 (Dec 25) was a Sunday.
        assert date(2022, 12, 25).weekday() == 6
        assert date(2022, 12, 26) in us_federal_holidays(2022)
        assert date(2022, 12, 25) not in us_federal_holidays(2022)

    def test_nth_weekday_holidays_never_fall_on_a_weekend(self):
        # MLK/Presidents/Memorial/Labor/Columbus/Thanksgiving are defined
        # as "nth weekday of month" -- structurally can never land on a
        # weekend, so there's nothing to observe-shift for them. All 11
        # holidays (including the fixed-date ones, which DO get shifted
        # off weekends) should stay off weekends across several years.
        for year in (2024, 2025, 2026, 2027, 2030):
            assert all(h.weekday() < 5 for h in us_federal_holidays(year))


class TestIsBusinessDay:
    def test_ordinary_weekday_is_a_business_day(self):
        assert is_business_day(date(2026, 8, 24)) is True  # a Monday

    def test_saturday_is_not_a_business_day(self):
        assert is_business_day(date(2026, 8, 22)) is False

    def test_sunday_is_not_a_business_day(self):
        assert is_business_day(date(2026, 8, 23)) is False

    def test_federal_holiday_is_not_a_business_day(self):
        assert is_business_day(date(2026, 1, 19)) is False  # MLK Day 2026

    def test_cross_year_boundary_new_years_shift(self):
        # REGRESSION-STYLE CHECK: New Year's Day 2022 (Jan 1, a Saturday)
        # was observed Friday, Dec 31, 2021 -- a date whose OWN year
        # (2021) doesn't naturally include this holiday in
        # us_federal_holidays(2021); it only appears in
        # us_federal_holidays(2022). Confirmed directly this is handled.
        assert date(2022, 1, 1).weekday() == 5
        assert is_business_day(date(2021, 12, 31)) is False
        # An ordinary nearby Thursday is unaffected.
        assert is_business_day(date(2021, 12, 30)) is True

    def test_ordinary_december_day_unaffected_by_cross_year_check(self):
        assert is_business_day(date(2026, 12, 15)) is True  # an ordinary Tuesday


class TestBusinessDaysOfMonth:
    def test_returns_only_weekdays_and_excludes_holidays(self):
        days = business_days_of_month(2026, 1)
        assert date(2026, 1, 1) not in days   # New Year's Day
        assert date(2026, 1, 19) not in days  # MLK Day
        assert date(2026, 1, 17) not in days  # a Saturday
        assert date(2026, 1, 2) in days       # first business day of the month

    def test_days_are_in_calendar_order(self):
        days = business_days_of_month(2026, 8)
        assert days == sorted(days)

    def test_all_returned_days_are_in_the_requested_month(self):
        days = business_days_of_month(2026, 2)
        assert all(d.month == 2 and d.year == 2026 for d in days)


class TestInScheduledRunWindow:
    def test_first_business_day_of_month_is_in_window(self):
        assert in_scheduled_run_window(date(2026, 1, 2)) is True  # Jan 1 is a holiday

    def test_third_business_day_is_in_window(self):
        assert in_scheduled_run_window(date(2026, 1, 6)) is True

    def test_fourth_business_day_is_not_in_window(self):
        assert in_scheduled_run_window(date(2026, 1, 7)) is False

    def test_last_business_day_of_month_is_in_window(self):
        assert in_scheduled_run_window(date(2026, 1, 30)) is True

    def test_fifth_from_last_business_day_is_in_window(self):
        assert in_scheduled_run_window(date(2026, 1, 26)) is True

    def test_sixth_from_last_business_day_is_not_in_window(self):
        days = business_days_of_month(2026, 1)
        sixth_from_last = days[-6]
        assert in_scheduled_run_window(sixth_from_last) is False

    def test_middle_of_month_business_day_is_not_in_window(self):
        assert in_scheduled_run_window(date(2026, 1, 15)) is False

    def test_weekend_day_is_not_in_window(self):
        assert in_scheduled_run_window(date(2026, 1, 17)) is False  # a Saturday

    def test_federal_holiday_is_not_in_window(self):
        assert in_scheduled_run_window(date(2026, 1, 19)) is False  # MLK Day

    def test_short_month_first_3_and_last_5_do_not_overlap_incorrectly(self):
        # February 2026 has 20 business days -- well clear of any
        # first-3/last-5 overlap ambiguity, but worth a direct check on a
        # short month specifically.
        days = business_days_of_month(2026, 2)
        assert len(days) >= 8  # sanity: first-3 and last-5 don't overlap
        assert in_scheduled_run_window(days[0]) is True
        assert in_scheduled_run_window(days[3]) is False
        assert in_scheduled_run_window(days[-5]) is True
        assert in_scheduled_run_window(days[-6]) is False

    def test_date_not_a_business_day_at_all_returns_false_not_error(self):
        # A date that isn't a business day (weekend/holiday) should
        # cleanly return False, not raise, even though it can never be
        # "found" in business_days_of_month's list.
        assert in_scheduled_run_window(date(2026, 1, 1)) is False  # New Year's Day

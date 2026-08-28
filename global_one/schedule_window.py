"""
US federal holiday calendar + business-day / scheduled-run-window logic
for the Salesforce GL journal pipeline's job schedule gate.

Computed via pure date-arithmetic rules (5 U.S.C. section 6103's
"nth weekday of month" / fixed-date-with-weekend-observance rules) — no
external dependency (e.g. the `holidays` package) needed, per Dakota: "is
there a way to do that using python's time?" Since every rule below is
defined in terms of weekday/month POSITION (not a literal date that needs
updating every year), this needs ZERO annual maintenance — it's correct
for any year automatically, and arguably more auditable for a financial
pipeline than depending on an external package's own data table (nothing
to trust beyond the rule itself, which is public law).

Only Python's standard library is used: datetime, calendar.
"""

from datetime import date, timedelta
import calendar

MONDAY, THURSDAY = 0, 3


def _observed(d: date) -> date:
    """
    Applies the federal weekend-observance rule (5 U.S.C. section 6103):
    a holiday that falls on Saturday is observed the preceding Friday;
    one that falls on Sunday is observed the following Monday.
    """
    if d.weekday() == 5:  # Saturday
        return d - timedelta(days=1)
    if d.weekday() == 6:  # Sunday
        return d + timedelta(days=1)
    return d


def _nth_weekday_of_month(year: int, month: int, weekday: int, n: int) -> date:
    """weekday: Monday=0 .. Sunday=6 (matches date.weekday()). n: 1 for the
    first occurrence of that weekday in the month, 2 for the second, etc."""
    first_of_month = date(year, month, 1)
    first_weekday_offset = (weekday - first_of_month.weekday()) % 7
    day = 1 + first_weekday_offset + (n - 1) * 7
    return date(year, month, day)


def _last_weekday_of_month(year: int, month: int, weekday: int) -> date:
    last_day = calendar.monthrange(year, month)[1]
    last_of_month = date(year, month, last_day)
    offset = (last_of_month.weekday() - weekday) % 7
    return last_of_month - timedelta(days=offset)


def us_federal_holidays(year: int) -> set:
    """
    Returns the set of US federal holiday OBSERVANCE dates for a given
    year — the actual day off, after the weekend-observance rule is
    applied, which may not be the holiday's nominal calendar date. The
    current 11 federal holidays (Juneteenth added 2021), per
    5 U.S.C. section 6103.
    """
    return {
        _observed(date(year, 1, 1)),                    # New Year's Day
        _nth_weekday_of_month(year, 1, MONDAY, 3),       # MLK Day
        _nth_weekday_of_month(year, 2, MONDAY, 3),       # Washington's Birthday
        _last_weekday_of_month(year, 5, MONDAY),         # Memorial Day
        _observed(date(year, 6, 19)),                    # Juneteenth
        _observed(date(year, 7, 4)),                     # Independence Day
        _nth_weekday_of_month(year, 9, MONDAY, 1),       # Labor Day
        _nth_weekday_of_month(year, 10, MONDAY, 2),      # Columbus Day
        _observed(date(year, 11, 11)),                   # Veterans Day
        _nth_weekday_of_month(year, 11, THURSDAY, 4),    # Thanksgiving
        _observed(date(year, 12, 25)),                   # Christmas Day
    }


def is_business_day(d: date) -> bool:
    """
    Not a weekend, not a US federal holiday observance date.

    Handles one cross-year-boundary edge case: when January 1st falls on
    a Saturday, its OBSERVED date shifts backward to December 31st of the
    PRIOR calendar year — e.g. New Year's Day 2022 (Jan 1, a Saturday)
    was observed Friday, Dec 31, 2021. That means
    us_federal_holidays(2022) contains a date whose own .year is 2021, so
    checking December 31st correctly requires also looking at NEXT year's
    holiday set, not just its own year's. No other federal holiday's
    observance rule can cross a year boundary: Christmas's Saturday/Sunday
    shift stays within December, and MLK/Presidents/Memorial/Labor/
    Columbus/Thanksgiving are all defined as "nth weekday of month," so
    they're never adjusted at all.
    """
    if d.weekday() >= 5:  # Saturday=5, Sunday=6
        return False
    holidays = us_federal_holidays(d.year)
    if d.month == 12:
        holidays = holidays | us_federal_holidays(d.year + 1)
    return d not in holidays


def business_days_of_month(year: int, month: int) -> list:
    """All business days in the given month, in calendar order."""
    _, last_day = calendar.monthrange(year, month)
    all_days = [date(year, month, day) for day in range(1, last_day + 1)]
    return [d for d in all_days if is_business_day(d)]


def in_scheduled_run_window(d: date) -> bool:
    """
    True if d is one of the first 3 OR last 5 business days of its own
    month (weekends and US federal holidays excluded) — per Dakota: "the
    last 5 business days of the current month... also run on the first
    three business days of the next month but that can be seen as the
    first three business days of the current month too" — i.e. checked
    purely against d's own month, no cross-month reasoning needed.
    """
    days = business_days_of_month(d.year, d.month)
    if d not in days:
        return False
    idx = days.index(d)
    return idx < 3 or idx >= len(days) - 5

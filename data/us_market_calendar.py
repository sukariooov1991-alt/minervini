"""NYSE/Nasdaq regular-session holiday and early-close calendar.

This intentionally covers recurring full-market holidays and common early closes.
It does not attempt to model one-off national days of mourning or emergency closures.
"""
from __future__ import annotations

from datetime import date, time, timedelta


def _nth_weekday(year: int, month: int, weekday: int, occurrence: int) -> date:
    first = date(year, month, 1)
    offset = (weekday - first.weekday()) % 7
    return first + timedelta(days=offset + 7 * (occurrence - 1))


def _last_weekday(year: int, month: int, weekday: int) -> date:
    if month == 12:
        cursor = date(year + 1, 1, 1) - timedelta(days=1)
    else:
        cursor = date(year, month + 1, 1) - timedelta(days=1)
    return cursor - timedelta(days=(cursor.weekday() - weekday) % 7)


def _observed(day: date) -> date:
    if day.weekday() == 5:
        return day - timedelta(days=1)
    if day.weekday() == 6:
        return day + timedelta(days=1)
    return day


def _easter_sunday(year: int) -> date:
    """Gregorian Easter algorithm (Meeus/Jones/Butcher)."""
    a = year % 19
    b, c = divmod(year, 100)
    d, e = divmod(b, 4)
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i, k = divmod(c, 4)
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    month = (h + l - 7 * m + 114) // 31
    day = ((h + l - 7 * m + 114) % 31) + 1
    return date(year, month, day)


def us_equity_market_holidays(year: int) -> set[date]:
    holidays = {
        _observed(date(year, 1, 1)),
        _nth_weekday(year, 1, 0, 3),       # Martin Luther King Jr. Day
        _nth_weekday(year, 2, 0, 3),       # Presidents' Day
        _easter_sunday(year) - timedelta(days=2),  # Good Friday
        _last_weekday(year, 5, 0),         # Memorial Day
        _observed(date(year, 7, 4)),       # Independence Day
        _nth_weekday(year, 9, 0, 1),       # Labor Day
        _nth_weekday(year, 11, 3, 4),      # Thanksgiving
        _observed(date(year, 12, 25)),     # Christmas
    }
    if year >= 2022:
        holidays.add(_observed(date(year, 6, 19)))  # Juneteenth

    # New Year's Day may be observed on December 31 of this calendar year.
    next_new_year_observed = _observed(date(year + 1, 1, 1))
    if next_new_year_observed.year == year:
        holidays.add(next_new_year_observed)
    return holidays


def get_us_equity_market_close(day: date) -> time | None:
    """Return regular close time, or None on weekends and recurring full holidays."""
    if day.weekday() >= 5 or day in us_equity_market_holidays(day.year):
        return None

    early_closes: set[date] = {
        _nth_weekday(day.year, 11, 3, 4) + timedelta(days=1),  # Black Friday
    }
    christmas_eve = date(day.year, 12, 24)
    if christmas_eve.weekday() < 5 and christmas_eve not in us_equity_market_holidays(day.year):
        early_closes.add(christmas_eve)

    # The last trading day before July 4 is commonly an early close.
    independence_eve = date(day.year, 7, 4) - timedelta(days=1)
    while (
        independence_eve.weekday() >= 5
        or independence_eve in us_equity_market_holidays(day.year)
    ):
        independence_eve -= timedelta(days=1)
    early_closes.add(independence_eve)

    return time(13, 0) if day in early_closes else time(16, 0)

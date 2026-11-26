"""Central US equity market clock: exchange calendar, sessions and boundaries.

All datetimes returned by this module are timezone-aware in America/New_York.
The calendar models recurring NYSE/Nasdaq holidays and standard early closes.
Exceptional closures should be added to EXTRA_CLOSURES / EXTRA_EARLY_CLOSES when announced.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from enum import Enum
from zoneinfo import ZoneInfo

NEW_YORK = ZoneInfo("America/New_York")
UTC = ZoneInfo("UTC")


class MarketSession(str, Enum):
    CLOSED = "closed"
    PRE_MARKET = "pre-market"
    REGULAR = "regular"
    POST_MARKET = "post-market"
    OVERNIGHT = "overnight"


@dataclass(frozen=True)
class MarketTimeState:
    now: datetime
    trading_date: date
    session: MarketSession
    is_trading_day: bool
    regular_open: datetime | None
    regular_close: datetime | None
    next_event: datetime | None
    next_event_name: str | None

    @property
    def is_regular_session(self) -> bool:
        return self.session is MarketSession.REGULAR

    @property
    def is_extended_session(self) -> bool:
        return self.session in {
            MarketSession.PRE_MARKET, MarketSession.POST_MARKET,
            MarketSession.OVERNIGHT,
        }


# Exceptional exchange closures are deliberately explicit and auditable.
EXTRA_CLOSURES: set[date] = set()
EXTRA_EARLY_CLOSES: set[date] = set()


def _nth_weekday(year: int, month: int, weekday: int, occurrence: int) -> date:
    first = date(year, month, 1)
    return first + timedelta(days=(weekday - first.weekday()) % 7 + 7 * (occurrence - 1))


def _last_weekday(year: int, month: int, weekday: int) -> date:
    first_next = date(year + (month == 12), month % 12 + 1, 1)
    last = first_next - timedelta(days=1)
    return last - timedelta(days=(last.weekday() - weekday) % 7)


def _observed(day: date) -> date:
    if day.weekday() == 5:
        return day - timedelta(days=1)
    if day.weekday() == 6:
        return day + timedelta(days=1)
    return day


def _easter_sunday(year: int) -> date:
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
    day = (h + l - 7 * m + 114) % 31 + 1
    return date(year, month, day)


def us_equity_market_holidays(year: int) -> set[date]:
    """Recurring full-day NYSE/Nasdaq closures for a calendar year."""
    holidays = {
        _observed(date(year, 1, 1)),
        _nth_weekday(year, 1, 0, 3),       # MLK Day
        _nth_weekday(year, 2, 0, 3),       # Presidents Day
        _easter_sunday(year) - timedelta(days=2),  # Good Friday
        _last_weekday(year, 5, 0),         # Memorial Day
        _observed(date(year, 7, 4)),
        _nth_weekday(year, 9, 0, 1),       # Labor Day
        _nth_weekday(year, 11, 3, 4),      # Thanksgiving
        _observed(date(year, 12, 25)),
    }
    if year >= 2022:
        holidays.add(_observed(date(year, 6, 19)))
    next_new_year = _observed(date(year + 1, 1, 1))
    if next_new_year.year == year:
        holidays.add(next_new_year)
    return holidays | {day for day in EXTRA_CLOSURES if day.year == year}


def is_trading_day(day: date) -> bool:
    return day.weekday() < 5 and day not in us_equity_market_holidays(day.year)


def _previous_trading_day(day: date) -> date:
    cursor = day - timedelta(days=1)
    while not is_trading_day(cursor):
        cursor -= timedelta(days=1)
    return cursor


def us_equity_early_closes(year: int) -> set[date]:
    """Standard 13:00 ET early closes, plus explicitly configured exceptions."""
    thanksgiving = _nth_weekday(year, 11, 3, 4)
    early = {thanksgiving + timedelta(days=1)}
    # Christmas Eve is an early close when it is a trading day.
    christmas_eve = date(year, 12, 24)
    if is_trading_day(christmas_eve):
        early.add(christmas_eve)
    # The last trading day before the observed Independence Day closure.
    independence_observed = _observed(date(year, 7, 4))
    early.add(_previous_trading_day(independence_observed))
    return {day for day in early if is_trading_day(day)} | {
        day for day in EXTRA_EARLY_CLOSES if day.year == year and is_trading_day(day)
    }


def get_us_equity_market_close(day: date) -> time | None:
    """Compatibility helper: regular-session close in New York time."""
    if not is_trading_day(day):
        return None
    return time(13, 0) if day in us_equity_early_closes(day.year) else time(16, 0)


class MarketTime:
    """Single source of truth for US equity calendar and session classification."""

    PRE_MARKET_OPEN = time(4, 0)
    REGULAR_OPEN = time(9, 30)
    POST_MARKET_END = time(20, 0)
    OVERNIGHT_END = time(4, 0)

    @staticmethod
    def now() -> datetime:
        return datetime.now(NEW_YORK)

    @staticmethod
    def to_new_york(value: datetime | None = None) -> datetime:
        value = value or datetime.now(NEW_YORK)
        if value.tzinfo is None:
            # Naive values are interpreted as New York local time, never server time.
            return value.replace(tzinfo=NEW_YORK)
        return value.astimezone(NEW_YORK)

    @classmethod
    def state(cls, value: datetime | None = None) -> MarketTimeState:
        now = cls.to_new_york(value)
        day = now.date()
        close_time = get_us_equity_market_close(day)
        trading = close_time is not None
        open_dt = datetime.combine(day, cls.REGULAR_OPEN, tzinfo=NEW_YORK) if trading else None
        close_dt = datetime.combine(day, close_time, tzinfo=NEW_YORK) if trading else None
        clock = now.timetz().replace(tzinfo=None)

        if not trading:
            session = MarketSession.CLOSED
        elif cls.PRE_MARKET_OPEN <= clock < cls.REGULAR_OPEN:
            session = MarketSession.PRE_MARKET
        elif cls.REGULAR_OPEN <= clock < close_time:
            session = MarketSession.REGULAR
        elif close_time <= clock < cls.POST_MARKET_END:
            session = MarketSession.POST_MARKET
        else:
            # Label only: actual overnight execution depends on broker/product support.
            session = MarketSession.OVERNIGHT

        events: list[tuple[datetime, str]] = []
        if trading:
            if now < open_dt:
                events.append((open_dt, "regular_open"))
            if now < close_dt:
                events.append((close_dt, "regular_close"))
        for offset in range(0, 11):
            candidate = day + timedelta(days=offset)
            if is_trading_day(candidate):
                candidate_open = datetime.combine(candidate, cls.REGULAR_OPEN, tzinfo=NEW_YORK)
                if candidate_open > now:
                    events.append((candidate_open, "regular_open"))
                    break
        next_event, event_name = min(events, default=(None, None), key=lambda item: item[0])
        return MarketTimeState(
            now=now, trading_date=day, session=session, is_trading_day=trading,
            regular_open=open_dt, regular_close=close_dt,
            next_event=next_event, next_event_name=event_name,
        )

    @classmethod
    def session_label_ar(cls, value: datetime | None = None) -> str:
        labels = {
            MarketSession.CLOSED: "مغلق",
            MarketSession.PRE_MARKET: "قبل الافتتاح",
            MarketSession.REGULAR: "مفتوح",
            MarketSession.POST_MARKET: "بعد الإغلاق",
            MarketSession.OVERNIGHT: "تداول ليلي",
        }
        return labels[cls.state(value).session]

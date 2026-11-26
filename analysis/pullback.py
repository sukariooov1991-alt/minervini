"""Pullback detection with an explicit, minimal price-turn-up proxy.

Minervini describes buying a pullback only after price begins turning upward.
His cited material does not prescribe a universal numeric trigger, moving-average
tolerance, ATR contraction ratio, or fixed stop. This module therefore avoids
those invented filters. The operational proxy used here is transparent:
a recent decline in completed closes, followed by a current candle closing
above both its open and the previous close. The setup remains independent of VCP.
"""
from __future__ import annotations

import pandas as pd


def _ordered_frame(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if isinstance(out.index, pd.DatetimeIndex) and not out.index.is_monotonic_increasing:
        out = out.sort_index()
    if "date" in out.columns:
        out["date"] = pd.to_datetime(out["date"], errors="coerce")
        out = out.sort_values("date", kind="stable")
    return out.reset_index(drop=True)


def check_pullback(df: pd.DataFrame, **_unused) -> dict:
    """Detect a recent pullback and a simple current-candle turn-up.

    This is an operational interpretation of Minervini's qualitative guidance,
    not a claim that he specified this exact candle formula.
    """
    result = {
        "passed": False,
        "candidate": False,
        "entry_eligible": False,
        "turning_up": False,
        "pullback_detected": False,
        "entry_trigger_price": None,
        "support_low": None,
        "reason": "insufficient or invalid price history",
    }
    required = {"close", "low"}
    if df is None or getattr(df, "empty", True) or not required.issubset(df.columns):
        return result

    x = _ordered_frame(df)
    close = pd.to_numeric(x["close"], errors="coerce")
    low = pd.to_numeric(x["low"], errors="coerce")
    if len(x) < 6 or close.iloc[-6:].isna().any() or low.iloc[-6:].isna().any():
        return result

    # Look only at the five bars preceding the current/evolving candle when
    # identifying the pullback; the current candle is the potential turn-up.
    prior_closes = close.iloc[-6:-1]
    prior_decline = bool(
        prior_closes.diff().lt(0).any()
        and prior_closes.iloc[-1] < prior_closes.max()
    )
    support_low = float(low.iloc[-6:].min())
    current_close = float(close.iloc[-1])
    previous_close = float(close.iloc[-2])

    if "open" in x.columns and pd.notna(x["open"].iloc[-1]):
        current_open = float(pd.to_numeric(x["open"].iloc[-1], errors="coerce"))
        turning_up = current_close > current_open and current_close > previous_close
    else:
        turning_up = current_close > previous_close

    valid_support = support_low > 0 and support_low < current_close
    result.update({
        "candidate": prior_decline,
        "pullback_detected": prior_decline,
        "turning_up": bool(turning_up),
        "support_low": support_low if valid_support else None,
        "last_close": current_close,
        "entry_trigger_price": current_close if prior_decline and turning_up and valid_support else None,
        "passed": bool(prior_decline and turning_up and valid_support),
        "entry_eligible": bool(prior_decline and turning_up and valid_support),
    })
    if not prior_decline:
        result["reason"] = "no recent pullback detected in the preceding five bars"
    elif not turning_up:
        result["reason"] = "pullback detected; waiting for price to turn upward"
    elif not valid_support:
        result["reason"] = "pullback low is not below the turn-up price; no structural stop can be anchored"
    else:
        result["reason"] = (
            "pullback followed by a positive current candle; "
            "turn-up is an operational proxy, not a source-defined numeric rule"
        )
    return result


def filter_pullbacks(prices: dict, **kwargs) -> list:
    return [
        symbol for symbol, frame in prices.items()
        if check_pullback(frame, **kwargs)["passed"]
    ]

"""Relative Strength rating: cross-sectional performance rank, not RSI."""
import numpy as np
import pandas as pd


def relative_strength_score(close: pd.Series) -> float:
    if close is None:
        return float("nan")
    s = (
        close.sort_index()
        if isinstance(close.index, pd.DatetimeIndex) and not close.index.is_monotonic_increasing
        else close
    )
    c = pd.to_numeric(s, errors="coerce").dropna().reset_index(drop=True)
    if len(c) < 252:
        return float("nan")

    last_price = float(c.iloc[-1])
    if not np.isfinite(last_price) or last_price <= 0:
        return float("nan")

    # Use up to (63, 126, 189, 252) trading days; if 252 bars are present and 253rd prior is missing,
    # anchor the 252-day leg at index 0 so 252-bar series are never dropped unnecessarily.
    periods_and_weights = [
        (63, 0.40),
        (126, 0.20),
        (189, 0.20),
        (252, 0.20),
    ]

    weighted_sum = 0.0
    total_weight = 0.0
    n = len(c)
    for period, weight in periods_and_weights:
        idx = n - 1 - period
        if idx < 0 and period == 252 and n >= 252:
            idx = 0
        if idx >= 0:
            base_price = float(c.iloc[idx])
            if np.isfinite(base_price) and base_price > 0:
                weighted_sum += weight * ((last_price / base_price) - 1.0)
                total_weight += weight

    if total_weight <= 0:
        return float("nan")
    return float(weighted_sum / total_weight)


def rank_relative_strength(prices: dict) -> pd.DataFrame:
    scores = {symbol: relative_strength_score(series) for symbol, series in prices.items()}
    valid = pd.Series(scores, dtype="float64").dropna()
    if valid.empty:
        return pd.DataFrame(columns=["symbol", "rs_score", "rs_rating"])

    percentile = valid.rank(method="average", pct=True)
    ratings = (percentile * 99).round().clip(1, 99).astype(int)
    result = pd.DataFrame({
        "symbol": valid.index,
        "rs_score": valid.values,
        "rs_rating": ratings.values,
    })
    return result.sort_values(["rs_rating", "rs_score"], ascending=False).reset_index(drop=True)


def passes_relative_strength(rs_rating: float, minimum: int = 70) -> bool:
    try:
        return bool(pd.notna(rs_rating) and float(rs_rating) >= minimum)
    except (TypeError, ValueError):
        return False

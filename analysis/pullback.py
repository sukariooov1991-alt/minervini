"""Pullback filter: looks for support, volatility contraction, and a turn back up."""
import numpy as np
import pandas as pd


def _prepare_df(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if isinstance(out.index, pd.DatetimeIndex) and not out.index.is_monotonic_increasing:
        out = out.sort_index()
    return out.reset_index(drop=True)


def _compute_atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    high = pd.to_numeric(df["high"], errors="coerce")
    low = pd.to_numeric(df["low"], errors="coerce")
    close = pd.to_numeric(df["close"], errors="coerce")
    prev_close = close.shift(1)

    tr = pd.concat(
        [
            high - low,
            (high - prev_close).abs(),
            (low - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    return tr.rolling(period, min_periods=period).mean()


def check_pullback(
    df: pd.DataFrame,
    ma_period: int = 50,
    proximity_pct: float = 3.0,
    require_turn_up: bool = True,
    require_volatility_contraction: bool = True,
    atr_period: int = 14,
    max_atr_ratio: float = 0.85,
) -> dict:
    out = {
        "passed": False,
        "near_average": False,
        "turning_up": False,
        "volatility_contracted": not require_volatility_contraction,
    }
    min_len = max(ma_period + 5, atr_period + 20)
    if df is None or "close" not in df.columns or len(df) < min_len:
        out["reason"] = "insufficient close history"
        return out

    x = _prepare_df(df)
    close = pd.to_numeric(x["close"], errors="coerce")
    if close.iloc[-5:].isna().any():
        out["reason"] = "missing recent close data"
        return out

    ma = close.rolling(ma_period, min_periods=ma_period).mean()
    if pd.isna(ma.iloc[-1]):
        out["reason"] = "insufficient valid data for MA"
        return out

    price = float(close.iloc[-1])
    average = float(ma.iloc[-1])

    recent_closes = close.iloc[-3:]
    recent_mas = ma.iloc[-3:]
    within_band = ((recent_closes - recent_mas).abs() / recent_mas * 100.0) <= proximity_pct
    near = bool(
        (recent_mas > 0).all()
        and within_band.any()
        and price >= average * (1.0 - proximity_pct / 100.0)
    )

    recent_window = close.iloc[-5:-1]
    had_dip = bool((recent_window < recent_window.shift(1)).iloc[1:].any())
    # Confirm a reclaim of the prior session high after the pullback.
    previous_high = (
        float(pd.to_numeric(x["high"].iloc[-2], errors="coerce"))
        if "high" in x.columns and pd.notna(x["high"].iloc[-2]) else float("nan")
    )
    turn = bool(np.isfinite(previous_high) and price > previous_high and had_dip)

    vol_contracted = True
    atr_ratio = float("nan")
    if require_volatility_contraction:
        if not {"high", "low"}.issubset(x.columns):
            out["reason"] = "high/low columns required for ATR contraction check"
            return out
        atr = _compute_atr(x, period=atr_period)
        recent_atr = float(atr.iloc[-1]) if pd.notna(atr.iloc[-1]) else float("nan")
        prior_atr = float(atr.iloc[-20:-5].mean()) if len(atr) >= 20 else float("nan")
        if pd.notna(recent_atr) and pd.notna(prior_atr) and prior_atr > 0:
            atr_ratio = recent_atr / prior_atr
            vol_contracted = bool(atr_ratio <= max_atr_ratio)
        else:
            vol_contracted = False

    support_low = (
        float(pd.to_numeric(x["low"].iloc[-5:], errors="coerce").min())
        if "low" in x.columns and pd.to_numeric(x["low"].iloc[-5:], errors="coerce").notna().all()
        else float("nan")
    )
    out.update({
        "near_average": bool(near),
        "turning_up": turn,
        "volatility_contracted": vol_contracted,
        "moving_average": average,
        "last_close": price,
        "entry_trigger_price": previous_high if np.isfinite(previous_high) else None,
        "support_low": support_low if np.isfinite(support_low) and support_low > 0 else None,
        "atr_ratio": atr_ratio,
    })
    out["passed"] = bool(
        near and (turn or not require_turn_up) and vol_contracted
    )
    return out


def filter_pullbacks(prices: dict, **kwargs) -> list:
    return [symbol for symbol, df in prices.items() if check_pullback(df, **kwargs)["passed"]]

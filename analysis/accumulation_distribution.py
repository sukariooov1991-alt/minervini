"""Accumulation/distribution filter based on price-volume behavior."""

import numpy as np
import pandas as pd


from analysis.frame_utils import ordered_frame

def accumulation_distribution_summary(
    df: pd.DataFrame,
    lookback: int = 20,
    window_lookback: int = 50,
    power_move_pct: float = 1.5,
    require_structural: bool = True,
) -> dict:
    required = {"close", "volume"}
    out = {
        "accumulation": False,
        "distribution": False,
        "up_day_volume_ratio": float("nan"),
        "down_day_volume_ratio": float("nan"),
        "up_down_volume_ratio": float("nan"),
        "power_up_days": 0,
        "power_down_days": 0,
    }
    min_bars = max(lookback + 1, window_lookback + 1)
    if df is None or not required.issubset(df.columns) or len(df) < min_bars:
        return out

    clean_df = ordered_frame(df, reset_index=True)

    x = clean_df.tail(lookback + 1).copy()
    close = pd.to_numeric(x["close"], errors="coerce")
    volume = pd.to_numeric(x["volume"], errors="coerce")
    change = close.diff()

    up = volume.iloc[1:][change.iloc[1:] > 0].mean()
    down = volume.iloc[1:][change.iloc[1:] < 0].mean()
    avg = volume.iloc[:-1].mean()
    last_change = change.iloc[-1]
    last_volume = volume.iloc[-1]

    up_ratio = float(up / avg) if pd.notna(up) and pd.notna(avg) and avg > 0 else float("nan")
    down_ratio = float(down / avg) if pd.notna(down) and pd.notna(avg) and avg > 0 else float("nan")

    w = clean_df.tail(window_lookback + 1).copy()
    w_close = pd.to_numeric(w["close"], errors="coerce")
    w_vol = pd.to_numeric(w["volume"], errors="coerce")
    w_pct = w_close.pct_change() * 100.0
    w_avg_vol = w_vol.iloc[:-1].mean()

    up_vol_sum = float(w_vol.iloc[1:][w_pct.iloc[1:] > 0].sum())
    down_vol_sum = float(w_vol.iloc[1:][w_pct.iloc[1:] < 0].sum())
    up_down_ratio = (
        float(up_vol_sum / down_vol_sum)
        if down_vol_sum > 0
        else (float("inf") if up_vol_sum > 0 else float("nan"))
    )

    power_up_days = (
        int(((w_pct >= power_move_pct) & (w_vol > w_avg_vol)).iloc[1:].sum())
        if pd.notna(w_avg_vol) and w_avg_vol > 0
        else 0
    )

    power_down_days = (
        int(((w_pct <= -power_move_pct) & (w_vol > w_avg_vol)).iloc[1:].sum())
        if pd.notna(w_avg_vol) and w_avg_vol > 0
        else 0
    )

    acc_session = bool(
        pd.notna(last_change)
        and last_change > 0
        and pd.notna(last_volume)
        and pd.notna(avg)
        and avg > 0
        and last_volume > avg
        and (pd.isna(down) or (pd.notna(up) and up >= down))
    )
    # A positive infinite ratio means there was buying volume but no
    # down-day volume; treat that as strong accumulation, not invalid data.
    ratio_is_strong = (
        (np.isfinite(up_down_ratio) or np.isposinf(up_down_ratio))
        and up_down_ratio >= 1.25
    )
    acc_structural = bool(
        ratio_is_strong
        and power_up_days >= power_down_days
    )

    accumulation = acc_structural if require_structural else bool(acc_session or acc_structural)

    dist = bool(
        pd.notna(last_change)
        and last_change < 0
        and pd.notna(last_volume)
        and pd.notna(avg)
        and avg > 0
        and last_volume > avg
    )

    out.update({
        "accumulation": accumulation,
        "distribution": dist,
        "up_day_volume_ratio": up_ratio,
        "down_day_volume_ratio": down_ratio,
        "up_down_volume_ratio": up_down_ratio,
        "power_up_days": power_up_days,
        "power_down_days": power_down_days,
    })
    return out


def check_accumulation(df: pd.DataFrame, lookback: int = 20, require_structural: bool = True) -> bool:
    return accumulation_distribution_summary(
        df, lookback=lookback, require_structural=require_structural
    )["accumulation"]


def check_distribution(df: pd.DataFrame, lookback: int = 20) -> bool:
    return accumulation_distribution_summary(df, lookback=lookback)["distribution"]

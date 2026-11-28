"""Mathematical VCP structure, volume dry-up, and breakout verification."""
import numpy as np
import pandas as pd


from analysis.frame_utils import ordered_frame

def _find_swing_pivots(df: pd.DataFrame, order: int = 3) -> list[dict]:
    highs = pd.to_numeric(df["high"], errors="coerce").to_numpy(dtype=float)
    lows = pd.to_numeric(df["low"], errors="coerce").to_numpy(dtype=float)
    n = len(df)
    raw_pivots = []

    for i in range(order, n - order):
        win_h = highs[i - order : i + order + 1]
        win_l = lows[i - order : i + order + 1]
        valid_h = win_h[np.isfinite(win_h)]
        valid_l = win_l[np.isfinite(win_l)]
        is_swing_high = bool(valid_h.size and np.isfinite(highs[i]) and highs[i] == valid_h.max())
        is_swing_low = bool(valid_l.size and np.isfinite(lows[i]) and lows[i] == valid_l.min())
        # A single wide candle cannot establish both sides of a contraction pivot.
        if is_swing_high == is_swing_low:
            continue
        if is_swing_high:
            raw_pivots.append({"idx": i, "type": "high", "price": float(highs[i])})
        else:
            raw_pivots.append({"idx": i, "type": "low", "price": float(lows[i])})

    raw_pivots.sort(key=lambda p: p["idx"])

    alternating = []
    for p in raw_pivots:
        if not alternating:
            alternating.append(p)
            continue
        prev = alternating[-1]
        if p["type"] != prev["type"]:
            alternating.append(p)
        else:
            if p["type"] == "high" and p["price"] >= prev["price"]:
                alternating[-1] = p
            elif p["type"] == "low" and p["price"] <= prev["price"]:
                alternating[-1] = p
    return alternating


def _extract_contractions(pivots: list[dict]) -> list[dict]:
    contractions = []
    for i in range(len(pivots) - 1):
        p1, p2 = pivots[i], pivots[i + 1]
        if p1["type"] == "high" and p2["type"] == "low" and p1["price"] > 0:
            depth_pct = ((p1["price"] - p2["price"]) / p1["price"]) * 100.0
            if depth_pct > 0:
                contractions.append({
                    "high_idx": p1["idx"],
                    "low_idx": p2["idx"],
                    "high": p1["price"],
                    "low": p2["price"],
                    "depth_pct": float(depth_pct),
                })
    return contractions


def _volume_slope(vol_series: pd.Series) -> float:
    v = pd.to_numeric(vol_series, errors="coerce").dropna().to_numpy(dtype=float)
    if len(v) < 5:
        return float("nan")
    x = np.arange(len(v), dtype=float)
    slope, _ = np.polyfit(x, v, 1)
    mean_v = np.mean(v)
    return float(slope / mean_v) if mean_v > 0 else float("nan")


def check_vcp(
    df: pd.DataFrame,
    lookback_bars: int = 90,
    pivot_order: int = 3,
    shrink_ratio_max: float = 0.75,
    max_final_contraction_pct: float | None = None,
    require_volume_dry_up: bool = True,
) -> dict:
    required = {"high", "low", "close", "volume"}
    out = {
        "passed": False,
        "contraction_depths": [],
        "contractions": [],
        "pivot_price": float("nan"),
        "base_high": float("nan"),
        "final_low": float("nan"),
        "volume_dry_up": False,
        "volume_slope_norm": float("nan"),
        "failed": [],
    }
    if df is None or not required.issubset(df.columns) or len(df) < 50:
        out["failed"] = ["requires high, low, close, volume and at least 50 bars"]
        return out

    base = ordered_frame(df, reset_index=True).tail(min(len(df), lookback_bars)).reset_index(drop=True)
    pivots = _find_swing_pivots(base, order=pivot_order)
    all_contractions = _extract_contractions(pivots)

    if len(all_contractions) < 2:
        out["failed"] = ["fewer than 2 swing contractions detected"]
        return out

    seq = [all_contractions[-1]]
    for c in reversed(all_contractions[:-1]):
        if len(seq) >= 6:
            break
        if c["depth_pct"] > seq[0]["depth_pct"]:
            seq.insert(0, c)
        else:
            break

    depths = [round(c["depth_pct"], 2) for c in seq]
    count_ok = 2 <= len(seq) <= 6
    shrinking = count_ok and all(
        seq[i]["depth_pct"] < seq[i - 1]["depth_pct"] for i in range(1, len(seq))
    )
    ratio_ok = shrinking and all(
        seq[i]["depth_pct"] <= seq[i - 1]["depth_pct"] * shrink_ratio_max
        for i in range(1, len(seq))
    )
    # Minervini defines the final T by its contraction relative to the prior
    # T and by volume drying up at the pivot; he does not prescribe a universal
    # absolute 10% ceiling. Keep an optional override for explicit experiments,
    # but do not reject an otherwise valid relative contraction by default.
    final_tight_ok = (
        max_final_contraction_pct is None
        or seq[-1]["depth_pct"] <= float(max_final_contraction_pct)
    )

    last_c = seq[-1]
    right_side_high = float(
        pd.to_numeric(base["high"].iloc[last_c["high_idx"] :], errors="coerce").max()
    )
    pivot_price = right_side_high if np.isfinite(right_side_high) else float(last_c["high"])
    base_high = float(max(c["high"] for c in seq))

    vol_all = pd.to_numeric(base["volume"], errors="coerce")
    sma50_vol = float(vol_all.tail(min(len(vol_all), 50)).mean())
    slope_norm = _volume_slope(vol_all.tail(min(len(vol_all), 40)))

    # Minervini specifies that the final contraction should trade below its
    # 50-day average volume, with one or two exceptionally quiet days around
    # the tight right-side/pivot area. Do not substitute a negative 40-day
    # volume slope: earlier high-volume sessions can keep that slope positive
    # even when the final contraction has dried up correctly.
    final_seg = base.iloc[last_c["high_idx"] : last_c["low_idx"] + 1]
    final_vol_mean = float(pd.to_numeric(final_seg["volume"], errors="coerce").mean())

    # The quiet-day check must belong to the final contraction/right-side
    # pivot region. A low-volume day elsewhere in the base is not evidence
    # that supply has dried up at the current buy point.
    pivot_window_start = max(int(last_c["high_idx"]), len(base) - 8)
    pivot_window = pd.to_numeric(base["volume"].iloc[pivot_window_start:], errors="coerce")
    dry_pocket_days = int((pivot_window < 0.55 * sma50_vol).sum()) if sma50_vol > 0 else 0

    vol_dry = bool(
        sma50_vol > 0
        and np.isfinite(final_vol_mean)
        and final_vol_mean < sma50_vol
        and dry_pocket_days >= 1
    )

    out.update({
        "contraction_depths": depths,
        "contractions": seq,
        "pivot_price": pivot_price,
        "base_high": base_high,
        "final_low": float(last_c["low"]),
        "volume_dry_up": vol_dry,
        "volume_slope_norm": slope_norm,
    })

    if not count_ok:
        out["failed"].append("needs 2 to 6 contractions")
    if not ratio_ok:
        out["failed"].append("contractions do not shrink progressively by required ratio")
    if not final_tight_ok:
        out["failed"].append(
            f"final contraction ({seq[-1]['depth_pct']:.2f}%) exceeds "
            f"the explicitly configured {max_final_contraction_pct}% limit"
        )
    if require_volume_dry_up and not vol_dry:
        out["failed"].append("volume has not dried up sufficiently on the right side")

    out["passed"] = bool(
        count_ok and ratio_ok and final_tight_ok and (vol_dry or not require_volume_dry_up)
    )
    return out


def check_breakout(
    df: pd.DataFrame,
    pivot: float = None,
    volume_average_period: int = 50,
    min_volume_mult: float = 1.0,
    min_close_pos: float | None = None,
    breakout_buffer_pct: float = 0.0,
    require_vcp_structure: bool = True,
) -> bool:
    required = {"high", "low", "close", "volume"}
    if df is None or not required.issubset(df.columns):
        return False
    if len(df) < volume_average_period + 1:
        return False

    x = ordered_frame(df, reset_index=True)
    if pivot is None or not np.isfinite(pivot):
        vcp_info = check_vcp(x.iloc[:-1])
        if require_vcp_structure and not vcp_info["passed"]:
            return False
        pivot = vcp_info.get("pivot_price", float("nan"))
        if not np.isfinite(pivot):
            pivot = pd.to_numeric(
                x["high"].iloc[-volume_average_period - 1 : -1], errors="coerce"
            ).max()

    high = float(pd.to_numeric(x["high"].iloc[-1], errors="coerce"))
    low = float(pd.to_numeric(x["low"].iloc[-1], errors="coerce"))
    close = float(pd.to_numeric(x["close"].iloc[-1], errors="coerce"))
    volume = float(pd.to_numeric(x["volume"].iloc[-1], errors="coerce"))
    avg_vol = float(
        pd.to_numeric(
            x["volume"].iloc[-volume_average_period - 1 : -1], errors="coerce"
        ).mean()
    )

    if not all(np.isfinite(v) for v in (pivot, high, low, close, volume, avg_vol)):
        return False
    if avg_vol <= 0 or pivot <= 0:
        return False

    price_ok = close > pivot * (1.0 + breakout_buffer_pct / 100.0)
    # Minervini's stated baseline is volume above its own 50-day average;
    # larger surges are preferable but are not a universal +35% requirement.
    vol_ok = volume > avg_vol * min_volume_mult
    # The source requires price to clear the pivot on above-average volume.
    # It does not prescribe a universal 0.25% buffer or a fixed candle-close
    # position threshold. Keep both as optional research overrides only.
    close_pos_ok = True
    if min_close_pos is not None:
        try:
            threshold = float(min_close_pos)
        except (TypeError, ValueError):
            return False
        if not np.isfinite(threshold) or not 0.0 <= threshold <= 1.0:
            return False
        candle_range = high - low
        close_pos_ok = bool(
            candle_range > 0 and ((close - low) / candle_range) >= threshold
        )

    return bool(price_ok and vol_ok and close_pos_ok)

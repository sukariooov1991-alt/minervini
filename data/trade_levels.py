"""Independent module for entry zones, dynamic ATR targets, and shared stop loss.

Calculates automatically:
- 5 entry zones distributed across the 5% range from pivot_price (max 5% range; no chasing above range).
- 5 dynamic targets (one per entry zone) driven by ATR and daily stock movement.
- 1 shared stop loss across all 5 entry zones anchored below the strategy base / final_low / pivot structure.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

import numpy as np
import pandas as pd


MAX_ENTRY_RANGE_PCT: float = 5.0
NUM_ZONES: int = 5


@dataclass
class TradeLevelsPlan:
    symbol: str
    current_price: float | None
    pivot_price: float | None
    final_low: float | None
    atr_value: float | None
    daily_move_pct: float | None
    within_entry_range: bool | None
    entry_zones: list[float]
    targets: list[float]
    shared_stop_loss: float | None
    status: str
    notes: list[str]

    def to_dict(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "current_price": self.current_price,
            "pivot_price": self.pivot_price,
            "final_low": self.final_low,
            "atr_value": self.atr_value,
            "daily_move_pct": self.daily_move_pct,
            "within_entry_range": self.within_entry_range,
            "entry_zones": self.entry_zones,
            "targets": self.targets,
            "shared_stop_loss": self.shared_stop_loss,
            "status": self.status,
            "notes": self.notes,
        }


def compute_atr(df: pd.DataFrame, period: int = 14) -> float | None:
    required = {"high", "low", "close"}
    if df is None or not required.issubset(df.columns) or len(df) < period + 1:
        return None

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

    atr_series = tr.rolling(period, min_periods=period).mean().dropna()
    if atr_series.empty:
        return None
    val = float(atr_series.iloc[-1])
    return val if np.isfinite(val) and val > 0 else None


def compute_daily_move_pct(df: pd.DataFrame) -> float | None:
    if df is None or "close" not in df.columns or len(df) < 2:
        return None
    close = pd.to_numeric(df["close"], errors="coerce").dropna()
    if len(close) < 2 or close.iloc[-2] <= 0:
        return None
    val = float(((close.iloc[-1] / close.iloc[-2]) - 1.0) * 100.0)
    return val if np.isfinite(val) else None


def evaluate_entry_range(
    current_price: float | None,
    pivot_price: float | None,
) -> tuple[bool | None, str | None]:
    valid_current = (
        current_price is not None
        and np.isfinite(current_price)
        and float(current_price) > 0
    )
    valid_pivot = (
        pivot_price is not None
        and np.isfinite(pivot_price)
        and float(pivot_price) > 0
    )
    if not (valid_current and valid_pivot):
        return None, None
    cur_p = float(current_price)
    piv_p = float(pivot_price)
    max_allowed = piv_p * (1.0 + MAX_ENTRY_RANGE_PCT / 100.0)
    within_range = bool(piv_p <= cur_p <= max_allowed)
    if within_range:
        return True, None
    note = (
        f"السعر ({cur_p:.4f}) خارج نطاق الـ 5% من نقطة الارتكاز ({piv_p:.4f}..{max_allowed:.4f})؛ لا تطارد السعر."
    )
    return False, note


def default_entry_zones(
    cur_p: float | None,
    piv_p: float | None,
    atr_val: float | None = None,
    daily_move: float | None = None,
    candles_df: pd.DataFrame | None = None,
    final_low: float | None = None,
) -> list[float]:
    # Never fabricate a strategy pivot from the current quote. Without a
    # validated pivot, entry zones cannot be anchored to the setup.
    if piv_p is None or not np.isfinite(piv_p) or piv_p <= 0:
        return []
    base_f = float(piv_p)
    max_pct = MAX_ENTRY_RANGE_PCT / 100.0
    step_pct = max_pct / (NUM_ZONES - 1) if NUM_ZONES > 1 else max_pct
    return [round(base_f * (1.0 + step_pct * idx), 4) for idx in range(NUM_ZONES)]


def default_targets(
    entry_zones: list[float],
    cur_p: float | None,
    piv_p: float | None,
    atr_val: float | None = None,
    daily_move: float | None = None,
    candles_df: pd.DataFrame | None = None,
    final_low: float | None = None,
) -> list[float]:
    if (
        not entry_zones
        or atr_val is None
        or not np.isfinite(atr_val)
        or float(atr_val) <= 0
    ):
        return []

    atr_f = float(atr_val)
    move_abs = abs(float(daily_move)) if (daily_move is not None and np.isfinite(daily_move)) else 0.0
    move_factor = float(np.clip(1.0 + (move_abs / 10.0) - 0.15, 0.75, 1.50))

    multipliers = (1.0, 1.5, 2.0, 2.75, 3.5)
    calculated_targets: list[float] = []
    for idx, zone_price in enumerate(entry_zones):
        z_f = float(zone_price)
        mult = multipliers[min(idx, len(multipliers) - 1)]
        target_val = z_f + (atr_f * mult * move_factor)
        calculated_targets.append(round(target_val, 4))

    return calculated_targets


def default_shared_stop_loss(
    entry_zones: list[float],
    cur_p: float | None,
    piv_p: float | None,
    atr_val: float | None = None,
    daily_move: float | None = None,
    candles_df: pd.DataFrame | None = None,
    final_low: float | None = None,
) -> float | None:
    # A stop must be anchored to the setup pivot, never a fabricated entry.
    anchor = (
        float(piv_p)
        if (piv_p is not None and np.isfinite(piv_p) and piv_p > 0)
        else None
    )
    if anchor is None or not np.isfinite(anchor) or anchor <= 0:
        return None

    # Prefer the setup's structural low. If it is unavailable, use one ATR
    # below the anchor; never invent a percentage-based volatility estimate.
    if final_low is not None and np.isfinite(final_low) and 0 < float(final_low) < anchor:
        stop_val = float(final_low) * 0.995
    elif atr_val is not None and np.isfinite(atr_val) and atr_val > 0:
        stop_val = anchor - float(atr_val)
    else:
        return None

    if not np.isfinite(stop_val) or stop_val <= 0:
        return None

    return round(float(stop_val), 4)


def build_trade_levels(
    symbol: str,
    current_price: float | None,
    pivot_price: float | None,
    candles_df: pd.DataFrame | None = None,
    final_low: float | None = None,
    entry_calculator: Callable[..., list[float]] | None = None,
    target_calculator: Callable[..., list[float]] | None = None,
    stop_calculator: Callable[..., float | None] | None = None,
) -> TradeLevelsPlan:
    notes: list[str] = []
    atr_val = compute_atr(candles_df) if candles_df is not None else None
    daily_move = compute_daily_move_pct(candles_df) if candles_df is not None else None

    valid_current = (
        current_price is not None
        and np.isfinite(current_price)
        and float(current_price) > 0
    )
    valid_pivot = (
        pivot_price is not None
        and np.isfinite(pivot_price)
        and float(pivot_price) > 0
    )
    valid_low = (
        final_low is not None
        and np.isfinite(final_low)
        and float(final_low) > 0
    )

    cur_p = float(current_price) if valid_current else None
    piv_p = float(pivot_price) if valid_pivot else None
    low_p = float(final_low) if valid_low else None

    within_range, range_note = evaluate_entry_range(cur_p, piv_p)
    if range_note:
        notes.append(range_note)

    if piv_p is None:
        notes.append("لا يمكن إنشاء مستويات تداول موثوقة دون نقطة ارتكاز صالحة للنموذج.")
        return TradeLevelsPlan(
            symbol=symbol.upper().strip(),
            current_price=cur_p,
            pivot_price=None,
            final_low=low_p,
            atr_value=atr_val,
            daily_move_pct=daily_move,
            within_entry_range=None,
            entry_zones=[],
            targets=[],
            shared_stop_loss=None,
            status="غير مكتمل — نقطة الارتكاز غير متوفرة",
            notes=notes,
        )

    active_entry_calc = entry_calculator or default_entry_zones
    active_target_calc = target_calculator or default_targets
    active_stop_calc = stop_calculator or default_shared_stop_loss

    entry_zones: list[float] = [
        float(x) for x in active_entry_calc(cur_p, piv_p, atr_val, daily_move, candles_df, low_p)
    ]
    targets: list[float] = [
        float(x) for x in active_target_calc(entry_zones, cur_p, piv_p, atr_val, daily_move, candles_df, low_p)
    ]
    stop_val = active_stop_calc(entry_zones, cur_p, piv_p, atr_val, daily_move, candles_df, low_p)
    shared_stop: float | None = (
        float(stop_val) if stop_val is not None and np.isfinite(stop_val) else None
    )

    entries_valid = (
        len(entry_zones) == NUM_ZONES
        and all(np.isfinite(v) and v > 0 for v in entry_zones)
        and all(a < b for a, b in zip(entry_zones, entry_zones[1:]))
    )
    targets_valid = (
        len(targets) == NUM_ZONES
        and all(np.isfinite(v) and v > entry for v, entry in zip(targets, entry_zones))
    ) if entries_valid else False
    stop_valid = (
        shared_stop is not None
        and np.isfinite(shared_stop)
        and shared_stop > 0
        and entries_valid
        and shared_stop < entry_zones[0]
    )
    has_complete_levels = entries_valid and targets_valid and stop_valid
    if not entries_valid:
        notes.append("مستويات الدخول غير صالحة أو غير مرتبة.")
    if entries_valid and not targets_valid:
        if atr_val is None or not np.isfinite(atr_val) or float(atr_val) <= 0:
            notes.append("الأهداف غير متاحة لعدم توفر ATR صالح؛ لم يتم توليد أهداف تقديرية.")
        else:
            notes.append("يوجد هدف غير صالح أو لا يتجاوز منطقة دخوله.")
    if not stop_valid:
        notes.append("وقف الخسارة غير صالح أو ليس أسفل جميع مناطق الدخول.")
    status = "جاهز" if has_complete_levels else "غير مكتمل"

    return TradeLevelsPlan(
        symbol=symbol.upper().strip(),
        current_price=cur_p,
        pivot_price=piv_p,
        final_low=low_p,
        atr_value=atr_val,
        daily_move_pct=daily_move,
        within_entry_range=within_range,
        entry_zones=entry_zones,
        targets=targets,
        shared_stop_loss=shared_stop,
        status=status,
        notes=notes,
    )

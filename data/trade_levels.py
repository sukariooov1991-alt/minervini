"""Trade-plan calculations: staged entries, risk-based target projections, and shared stop.

The 2:1-to-3:1 reward/risk ladder and 10% maximum stop distance reflect Minervini's
published risk framework. Five intermediate levels and the structural stop buffer
are implementation choices, not individually prescribed Minervini rules.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

import numpy as np
import pandas as pd


MAX_ENTRY_RANGE_PCT: float = 5.0
MAX_INITIAL_STOP_LOSS_PCT: float = 10.0
ATR_STOP_MULTIPLE: float = 2.0
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
    stop_source: str | None
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
            "stop_source": self.stop_source,
            "status": self.status,
            "notes": self.notes,
        }


def compute_atr(df: pd.DataFrame, period: int = 14) -> float | None:
    required = {"high", "low", "close"}
    # The first bar contributes its high-low as the initial true range,
    # so exactly `period` valid OHLC rows are sufficient for the Wilder seed.
    if df is None or not required.issubset(df.columns) or len(df) < period:
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

    tr = pd.to_numeric(tr, errors="coerce").replace([np.inf, -np.inf], np.nan).dropna()
    if len(tr) < period:
        return None

    # Wilder's ATR: seed with the first period's simple average, then apply
    # the recursive 1/period smoothing used by the standard ATR definition.
    values = tr.to_numpy(dtype=float)
    atr_value = float(np.mean(values[:period]))
    for true_range in values[period:]:
        atr_value = ((atr_value * (period - 1)) + float(true_range)) / period

    return atr_value if np.isfinite(atr_value) and atr_value > 0 else None


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
    *,
    shared_stop_loss: float | None = None,
) -> list[float]:
    if len(entry_zones) != NUM_ZONES:
        return []

    # Minervini's risk framework targets at least 2:1 reward/risk and aims
    # for 3:1. Build the five UI reference levels from the actual stop distance,
    # rather than unrelated ATR multiples. These intermediate steps are a
    # presentation ladder, not five separately prescribed Minervini targets.
    stop_val = shared_stop_loss
    if stop_val is None:
        stop_val = default_shared_stop_loss(
            entry_zones, cur_p, piv_p, atr_val, daily_move, candles_df, final_low
        )
    if stop_val is None or not np.isfinite(stop_val) or stop_val <= 0:
        return []

    reward_risk_multiples = (2.0, 2.25, 2.5, 2.75, 3.0)
    calculated_targets: list[float] = []
    for idx, zone_price in enumerate(entry_zones):
        entry = float(zone_price)
        risk_per_share = entry - float(stop_val)
        if not np.isfinite(risk_per_share) or risk_per_share <= 0:
            return []
        target_val = entry + risk_per_share * reward_risk_multiples[idx]
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

    # Prefer the setup's own structural invalidation low. If absent or
    # unusable, use a transparent volatility fallback: 2 x Wilder ATR(14)
    # below the setup pivot. This is a project setting, not a universal
    # Minervini-prescribed ATR formula.
    if final_low is not None and np.isfinite(final_low) and 0 < float(final_low) < anchor:
        stop_val = float(final_low)
    else:
        if atr_val is None or not np.isfinite(atr_val) or float(atr_val) <= 0:
            return None
        stop_val = anchor - (ATR_STOP_MULTIPLE * float(atr_val))

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
            stop_source=None,
            status="غير مكتمل — نقطة الارتكاز غير متوفرة",
            notes=notes,
        )

    active_entry_calc = entry_calculator or default_entry_zones
    active_target_calc = target_calculator or default_targets
    active_stop_calc = stop_calculator or default_shared_stop_loss

    entry_zones: list[float] = [
        float(x) for x in active_entry_calc(cur_p, piv_p, atr_val, daily_move, candles_df, low_p)
    ]
    stop_val = active_stop_calc(entry_zones, cur_p, piv_p, atr_val, daily_move, candles_df, low_p)
    shared_stop: float | None = (
        float(stop_val) if stop_val is not None and np.isfinite(stop_val) else None
    )
    structural_low_valid = bool(
        low_p is not None and np.isfinite(low_p) and 0 < low_p < piv_p
    )
    stop_source = (
        "structural" if shared_stop is not None and structural_low_valid
        else "atr" if shared_stop is not None and not structural_low_valid
        else None
    )
    if target_calculator is None:
        targets = [
            float(x) for x in default_targets(
                entry_zones, cur_p, piv_p, atr_val, daily_move, candles_df, low_p,
                shared_stop_loss=shared_stop,
            )
        ]
    else:
        targets = [
            float(x) for x in active_target_calc(
                entry_zones, cur_p, piv_p, atr_val, daily_move, candles_df, low_p
            )
        ]

    entries_valid = (
        len(entry_zones) == NUM_ZONES
        and all(np.isfinite(v) and v > 0 for v in entry_zones)
        and all(a < b for a, b in zip(entry_zones, entry_zones[1:]))
    )
    targets_valid = (
        len(targets) == NUM_ZONES
        and all(np.isfinite(v) and v > entry for v, entry in zip(targets, entry_zones))
        and all(a < b for a, b in zip(targets, targets[1:]))
    ) if entries_valid else False
    stop_beyond_entries = bool(
        shared_stop is not None
        and np.isfinite(shared_stop)
        and shared_stop > 0
        and entries_valid
        and shared_stop < entry_zones[0]
    )
    stop_risk_pct = (
        max((entry - shared_stop) / entry * 100.0 for entry in entry_zones)
        if stop_beyond_entries else None
    )
    stop_within_max_loss = bool(
        stop_risk_pct is not None
        and np.isfinite(stop_risk_pct)
        and stop_risk_pct <= MAX_INITIAL_STOP_LOSS_PCT + 1e-9
    )
    stop_valid = stop_beyond_entries and stop_within_max_loss
    reward_risk_valid = bool(
        stop_beyond_entries
        and targets_valid
        and all(
            (target - entry) / (entry - shared_stop) >= 2.0 - 1e-9
            for entry, target in zip(entry_zones, targets)
        )
    )
    has_complete_levels = entries_valid and targets_valid and stop_valid and reward_risk_valid
    if not entries_valid:
        notes.append("مستويات الدخول غير صالحة أو غير مرتبة.")
    if entries_valid and not targets_valid:
        if atr_val is None or not np.isfinite(atr_val) or float(atr_val) <= 0:
            notes.append("الأهداف غير متاحة لعدم توفر ATR صالح؛ لم يتم توليد أهداف تقديرية.")
        else:
            notes.append("يوجد هدف غير صالح أو لا يتجاوز منطقة دخوله.")
    elif targets_valid and stop_beyond_entries and not reward_risk_valid:
        notes.append("نسبة العائد إلى المخاطرة في أحد الأهداف أقل من 2R؛ لا تُعتمد خطة الدخول.")
    if not stop_beyond_entries:
        if stop_source is None and (atr_val is None or not np.isfinite(atr_val) or float(atr_val) <= 0):
            notes.append("لا يوجد قاع هيكلي صالح ولا تتوفر قيمة ATR صالحة؛ الفرصة تبقى معروضة لكن وقف الخسارة غير متاح.")
        else:
            notes.append("وقف الخسارة المحسوب غير صالح أو ليس أسفل جميع مناطق الدخول.")
    elif not stop_within_max_loss:
        notes.append(
            f"أعلى نسبة مخاطرة بين مناطق الدخول إلى الوقف تبلغ {stop_risk_pct:.2f}%؛ "
            f"تتجاوز الحد الأقصى {MAX_INITIAL_STOP_LOSS_PCT:.0f}% ولا تُعتمد الخطة."
        )
    status = "جاهز" if has_complete_levels else "غير مكتمل"
    if stop_source == "atr":
        notes.append(f"لا يوجد قاع هيكلي صالح؛ استُخدم وقف ATR بديل بمقدار {ATR_STOP_MULTIPLE:.1f} × ATR(14) أسفل نقطة الارتكاز.")

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
        stop_source=stop_source,
        status=status,
        notes=notes,
    )

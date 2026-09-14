"""
Volatility Contraction Breakout (VCB) strategia.

Logika:
  1. Konszolidáció detektálás: BB-bandwidth + ATR alacsony (percentilis-based)
  2. Belépés: az ár kitör a konszolidációs sáv (High/Low a szűk időszakban) fölé
  3. Trend filter (opcionális): HTF EMA-slope
  4. SL: az ellentétes konszolidációs szint, vagy ATR
  5. TP: ATR-arányos vagy fixed R:R

Ritkább mint a Donchian, de magasabb R:R.
"""
from __future__ import annotations

from typing import Dict, List, Optional

import numpy as np
import pandas as pd

from ..indicators.candle_indicators import (
    atr as _atr, bollinger_bands, bb_squeeze_percentile, ema)
from .base import Decision, Strategy, StrategyContext


def _tf_td64(tf: str) -> np.timedelta64:
    return np.timedelta64(int(pd.Timedelta(tf).value), "ns")


class VolatilityContraction(Strategy):
    name = "volatility_contraction"

    def __init__(self, params: dict):
        super().__init__(params)
        p = params
        self.candle_tf: str = p["candle_tf"]
        self.bb_period: int = int(p.get("bb_period", 20))
        self.bb_std: float = float(p.get("bb_std", 2.0))
        self.atr_period: int = int(p.get("atr_period", 14))
        self.contraction_lookback: int = int(p.get("contraction_lookback", 50))
        self.squeeze_pct: float = float(p.get("squeeze_pct", 0.25))    # BB-bandwidth ez ALATT = szűk
        self.atr_pct: float = float(p.get("atr_pct", 0.30))            # ATR ez ALATT = alacsony vol
        self.range_lookback: int = int(p.get("range_lookback", 20))    # a konszolidáció HL sávja

        self.sl_atr_mult: float = float(p.get("sl_atr_mult", 1.5))
        self.tp_atr_mult: float = float(p.get("tp_atr_mult", 4.0))
        self.breakout_buffer: float = float(p.get("breakout_buffer", 0.0))

        self.trend_filter_enabled: bool = bool(p.get("trend_filter_enabled", False))
        self.trend_filter_tf: str = p.get("trend_filter_tf", "4h")
        self.trend_ema_fast: int = int(p.get("trend_ema_fast", 9))
        self.trend_ema_slow: int = int(p.get("trend_ema_slow", 21))

        self.cooldown_bars: int = int(p.get("cooldown_bars", 10))

        # State
        self._high_arr: Optional[np.ndarray] = None
        self._low_arr: Optional[np.ndarray] = None
        self._atr_arr: Optional[np.ndarray] = None
        self._atr_pct_arr: Optional[np.ndarray] = None
        self._squeeze_pct_arr: Optional[np.ndarray] = None
        self._ltf_close_ts: Optional[np.ndarray] = None
        self._trend_fast_arr: Optional[np.ndarray] = None
        self._trend_slow_arr: Optional[np.ndarray] = None
        self._trend_close_ts: Optional[np.ndarray] = None
        self._last_signal_idx: int = -1_000_000

    def required_timeframes(self) -> List[str]:
        tfs = [self.candle_tf]
        if self.trend_filter_enabled and self.trend_filter_tf not in tfs:
            tfs.append(self.trend_filter_tf)
        return tfs

    def on_segment_start(self, ctx: StrategyContext) -> None:
        self.refresh_candles(ctx.candles_mtf)

    def refresh_candles(self, candles_mtf) -> None:
        ltf = candles_mtf[self.candle_tf].copy()
        h, l, c = ltf["high"], ltf["low"], ltf["close"]

        # A konszolidáció HL sávja: az utolsó range_lookback gyertya HIGH/LOW-ja
        self._high_arr = h.shift(1).rolling(self.range_lookback).max().to_numpy(dtype=float)
        self._low_arr  = l.shift(1).rolling(self.range_lookback).min().to_numpy(dtype=float)

        atr_series = _atr(h, l, c, self.atr_period)
        self._atr_arr = atr_series.to_numpy(dtype=float)
        # ATR percentilis a contraction_lookback ablakon
        self._atr_pct_arr = atr_series.rolling(self.contraction_lookback).rank(pct=True).to_numpy(dtype=float)

        # BB bandwidth percentilis
        _, _, _, bw = bollinger_bands(c, self.bb_period, self.bb_std)
        self._squeeze_pct_arr = bb_squeeze_percentile(bw, lookback=self.contraction_lookback).to_numpy(dtype=float)

        self._ltf_close_ts = ltf["timestamp"].values + _tf_td64(self.candle_tf)

        if self.trend_filter_enabled:
            tdf = candles_mtf[self.trend_filter_tf].copy()
            self._trend_fast_arr = ema(tdf["close"], self.trend_ema_fast).to_numpy(dtype=float)
            self._trend_slow_arr = ema(tdf["close"], self.trend_ema_slow).to_numpy(dtype=float)
            self._trend_close_ts = tdf["timestamp"].values + _tf_td64(self.trend_filter_tf)

    def _trend_direction(self, ts_np) -> Optional[str]:
        if self._trend_fast_arr is None:
            return None
        i = int(np.searchsorted(self._trend_close_ts, ts_np, side="right") - 1)
        if i < 0 or i >= len(self._trend_fast_arr):
            return None
        f = float(self._trend_fast_arr[i])
        s = float(self._trend_slow_arr[i])
        if not (np.isfinite(f) and np.isfinite(s)):
            return None
        if f > s: return "BUY"
        if f < s: return "SELL"
        return None

    def on_tick(self, ts: pd.Timestamp, bid: float, ask: float) -> Optional[Decision]:
        ts_np = ts.to_datetime64()
        i = int(np.searchsorted(self._ltf_close_ts, ts_np, side="right") - 1)
        warmup = max(self.contraction_lookback, self.bb_period, self.atr_period, self.range_lookback) + 2
        if i < warmup:
            return None

        high_n = float(self._high_arr[i])
        low_n  = float(self._low_arr[i])
        atr_v  = float(self._atr_arr[i])
        squeeze_p = float(self._squeeze_pct_arr[i])
        atr_p = float(self._atr_pct_arr[i])
        if not all(np.isfinite(x) for x in (high_n, low_n, atr_v, squeeze_p, atr_p)) or atr_v <= 0:
            return None

        # Cooldown
        if (i - self._last_signal_idx) < self.cooldown_bars:
            return None

        # Konszolidáció volt-e az előző időszakban? (percentilis alacsony)
        # Nézzük hogy a MEGELŐZŐ candle-oknál (i-1..i-cooldown_bars) VOLT-e squeeze
        # — vagyis egy CONTRACTION → EXPANSION setup keresés
        lookback_range = 10  # nézzük az utolsó 10 gyertyát: volt-e köztük squeeze
        start = max(0, i - lookback_range)
        recent_squeeze = self._squeeze_pct_arr[start:i]
        recent_atr = self._atr_pct_arr[start:i]
        had_contraction = (
            (np.nanmin(recent_squeeze) < self.squeeze_pct)
            and (np.nanmin(recent_atr) < self.atr_pct)
        )
        if not had_contraction:
            return None

        # Kitörés az előző N gyertya HL-sávjából
        direction: Optional[str] = None
        if ask > high_n + self.breakout_buffer:
            direction = "BUY"
        elif bid < low_n - self.breakout_buffer:
            direction = "SELL"

        if direction is None:
            return None

        # Trend filter
        if self.trend_filter_enabled:
            trend = self._trend_direction(ts_np)
            if trend is None:
                return Decision(ts=ts, allow_trade=False, reason="trend_unknown",
                                direction=direction)
            if trend != direction:
                return Decision(ts=ts, allow_trade=False, reason=f"counter_trend({trend})",
                                direction=direction)

        self._last_signal_idx = i
        sl_dist = self.sl_atr_mult * atr_v
        tp_dist = self.tp_atr_mult * atr_v
        return Decision(
            ts=ts, allow_trade=True, reason="vcb_breakout",
            direction=direction, score=1.0, size=1.0,
            sl_distance=sl_dist, tp_distance=tp_dist,
            indicators={
                "high_n": high_n, "low_n": low_n, "atr": atr_v,
                "squeeze_pct": squeeze_p, "atr_pct": atr_p,
            },
        )

"""
MicroScalp — high-frequency mean-reversion scalping rövid távú oldalazásokra.

Filozófia:
- A tick-stream-ben **mikro-oldalazás**: az utolsó N tick min-max szétterjedése
  kicsi (kis volatilitás-ablak). Itt a piac "lapos" másodperces időtávon.
- **Z-score belépés**: ha a tick ár a rolling mean-től σ-arányosan messze van,
  visszaesésre lép be (mean-reversion).
- **Kicsi TP** (0.3–1.5$, XAUUSD ≈ 3–15 pip), **szoros SL**. Negatív RR-t a
  hit-rate-tel ellensúlyozzuk.
- **Anti-trend védelem**: ha a HTF (5min) EMA-slope erős, vagy nem lépünk be,
  vagy csak trend-irányba lépünk.
- **Cooldown másodpercben**, nem percben — gyors iteráció.
- Pozíció méret kívülről jön az engine sizer-jéből (fixed_risk + magas
  max_order_size mellett a kis SL → nagy lot).
"""
from __future__ import annotations

from typing import List, Optional

import numpy as np
import pandas as pd

from ..indicators.candle_indicators import atr, ema
from .base import Decision, Strategy, StrategyContext


VALID_TREND_MODES = ("off", "block", "trend_only")
VALID_SL_TP_MODES = ("fixed_dollars", "atr_mult")


def _tf_td64(tf: str) -> np.timedelta64:
    return np.timedelta64(int(pd.Timedelta(tf).value), "ns")


class MicroScalp(Strategy):
    name = "micro_scalp"

    def __init__(self, params: dict):
        super().__init__(params)
        p = params

        # ── Trend-context (HTF) ──
        self.candle_tf: str = p.get("candle_tf", "5min")
        self.ema_period: int = p.get("ema_period", 20)
        self.atr_period: int = p.get("atr_period", 14)
        self.trend_mode: str = p.get("trend_mode", "block")
        if self.trend_mode not in VALID_TREND_MODES:
            raise ValueError(f"trend_mode: {self.trend_mode} ({VALID_TREND_MODES})")
        self.trend_strong_atr_mult: float = p.get("trend_strong_atr_mult", 0.5)
        self.ema_slope_lookback: int = p.get("ema_slope_lookback", 3)

        # ── Tick-szintű mikro-range ──
        self.micro_window_ticks: int = p.get("micro_window_ticks", 600)
        self.max_micro_range: float = p.get("max_micro_range", 1.5)  # $

        # ── Z-score belépés ──
        self.zscore_window_ticks: int = p.get("zscore_window_ticks", 200)
        self.zscore_entry: float = p.get("zscore_entry", 2.0)
        self.min_std: float = p.get("min_std", 0.05)  # min sigma a robusztussághoz

        # ── TP / SL ──
        self.sl_mode: str = p.get("sl_mode", "fixed_dollars")
        self.tp_mode: str = p.get("tp_mode", "fixed_dollars")
        if self.sl_mode not in VALID_SL_TP_MODES:
            raise ValueError(f"sl_mode: {self.sl_mode} ({VALID_SL_TP_MODES})")
        if self.tp_mode not in VALID_SL_TP_MODES:
            raise ValueError(f"tp_mode: {self.tp_mode} ({VALID_SL_TP_MODES})")
        self.tp_dollars: float = p.get("tp_dollars", 0.5)
        self.sl_dollars: float = p.get("sl_dollars", 0.8)
        self.tp_atr_mult: float = p.get("tp_atr_mult", 0.5)
        self.sl_atr_mult: float = p.get("sl_atr_mult", 0.8)
        self.min_sl_distance: float = p.get("min_sl_distance", 0.1)

        # ── Cooldown ──
        self.cooldown_seconds: float = p.get("cooldown_seconds", 60.0)

        # ── Session ──
        self.enable_session_filter: bool = p.get("enable_session_filter", False)
        self.session_start: int = p.get("session_start", 0)
        self.session_end: int = p.get("session_end", 24)

        # ── State (segment lokális) ──
        self._mid_arr: Optional[np.ndarray] = None
        self._micro_max: Optional[np.ndarray] = None
        self._micro_min: Optional[np.ndarray] = None
        self._z_mean: Optional[np.ndarray] = None
        self._z_std: Optional[np.ndarray] = None
        self._htf_idx_per_tick: Optional[np.ndarray] = None
        self._htf_ema: Optional[np.ndarray] = None
        self._htf_atr: Optional[np.ndarray] = None
        self._htf_slope_per_atr: Optional[np.ndarray] = None
        self._segment_tick_ts: Optional[np.ndarray] = None
        self._warmup: int = 0

        self._last_sl_ts_buy: Optional[pd.Timestamp] = None
        self._last_sl_ts_sell: Optional[pd.Timestamp] = None

    def required_timeframes(self) -> List[str]:
        return [self.candle_tf]

    # ─────────────────────────────────────────────────────────────────────────
    def on_segment_start(self, ctx: StrategyContext) -> None:
        if ctx.segment_ticks is None:
            raise RuntimeError("MicroScalp: a StrategyContext-ben kell legyen segment_ticks")
        ticks = ctx.segment_ticks

        mid = ticks["mid"].to_numpy(dtype=float)
        ts_arr = ticks["timestamp_utc"].values

        # Mikro-range: rolling min/max az utolsó N tick-en
        mid_s = pd.Series(mid)
        self._mid_arr = mid
        self._micro_max = mid_s.rolling(self.micro_window_ticks).max().to_numpy()
        self._micro_min = mid_s.rolling(self.micro_window_ticks).min().to_numpy()

        # Z-score rolling mean/std
        self._z_mean = mid_s.rolling(self.zscore_window_ticks).mean().to_numpy()
        self._z_std = mid_s.rolling(self.zscore_window_ticks).std().to_numpy()

        # HTF (trend-context)
        htf = ctx.candles_mtf[self.candle_tf]
        ema_v = ema(htf["close"], self.ema_period)
        atr_v = atr(htf["high"], htf["low"], htf["close"], self.atr_period)
        slope = ema_v.diff(self.ema_slope_lookback) / self.ema_slope_lookback
        slope_per_atr = slope / atr_v
        self._htf_ema = ema_v.to_numpy(dtype=float)
        self._htf_atr = atr_v.to_numpy(dtype=float)
        self._htf_slope_per_atr = slope_per_atr.to_numpy(dtype=float)
        # Csak lezárt HTF gyertyára indexelünk: a candle nyitóideje + tf a
        # zárás időpontja, így nincs look-ahead.
        htf_ts_close = htf["timestamp"].values + _tf_td64(self.candle_tf)
        self._htf_idx_per_tick = (np.searchsorted(htf_ts_close, ts_arr, side="right") - 1).astype(np.int64)

        self._segment_tick_ts = ts_arr
        self._warmup = max(self.micro_window_ticks, self.zscore_window_ticks)
        self._last_sl_ts_buy = None
        self._last_sl_ts_sell = None

    # ─────────────────────────────────────────────────────────────────────────
    def on_tick(self, ts: pd.Timestamp, bid: float, ask: float) -> Optional[Decision]:
        # A runner kihagyhatja az on_tick-et (max_open_positions limit), ezért
        # `ts`-ből keressük a tick-indexet — nem belső számlálóval.
        i = int(np.searchsorted(self._segment_tick_ts, ts.to_datetime64(), side="right") - 1)
        if i < self._warmup:
            return None

        mid = (bid + ask) / 2.0
        spread = ask - bid

        # ── Z-score ──
        m = self._z_mean[i]
        s = self._z_std[i]
        if not (np.isfinite(m) and np.isfinite(s)) or s < self.min_std:
            return None
        z = (mid - m) / s

        # Csak akkor logolunk, ha tényleg küszöböt elért — nem near-miss.
        # (Egyébként a tick-stream miatt millió sor lenne.)
        if abs(z) < self.zscore_entry:
            return None

        direction = "BUY" if z <= -self.zscore_entry else "SELL"

        reasons: List[str] = []

        # ── Mikro-range filter ──
        micro_range = float(self._micro_max[i] - self._micro_min[i])
        if not np.isfinite(micro_range) or micro_range > self.max_micro_range:
            reasons.append("not_micro_ranging")

        # ── Trend protection ──
        htf_idx = int(self._htf_idx_per_tick[i])
        slope_per_atr = 0.0
        if 0 <= htf_idx < len(self._htf_slope_per_atr):
            v = self._htf_slope_per_atr[htf_idx]
            if np.isfinite(v):
                slope_per_atr = float(v)
        trend_strong = abs(slope_per_atr) >= self.trend_strong_atr_mult
        if trend_strong and self.trend_mode == "block":
            reasons.append("trend_too_strong")
        elif trend_strong and self.trend_mode == "trend_only":
            # csak trend-irányú mean-reversion (pl. uptrend-ben csak BUY-dip)
            if (slope_per_atr > 0 and direction != "BUY") or (slope_per_atr < 0 and direction != "SELL"):
                reasons.append("anti_trend")

        # ── Session filter ──
        if not self._within_hours(ts):
            reasons.append("outside_hours")

        # ── Cooldown ──
        last_sl = self._last_sl_ts_buy if direction == "BUY" else self._last_sl_ts_sell
        if last_sl is not None and self.cooldown_seconds > 0:
            elapsed = (ts - last_sl).total_seconds()
            if elapsed < self.cooldown_seconds:
                reasons.append("cooldown_after_sl")

        # ── SL/TP ──
        htf_atr = 0.0
        if 0 <= htf_idx < len(self._htf_atr):
            v = self._htf_atr[htf_idx]
            if np.isfinite(v):
                htf_atr = float(v)
        sl_distance = self._distance_for_mode(self.sl_mode, htf_atr,
                                              dollars=self.sl_dollars, atr_mult=self.sl_atr_mult)
        tp_distance = self._distance_for_mode(self.tp_mode, htf_atr,
                                              dollars=self.tp_dollars, atr_mult=self.tp_atr_mult)
        if sl_distance < self.min_sl_distance:
            reasons.append("sl_too_small")
        if tp_distance <= 0:
            reasons.append("tp_invalid")

        allow_trade = not reasons

        return Decision(
            ts=ts,
            allow_trade=allow_trade,
            reason="ok" if allow_trade else ";".join(reasons),
            direction=direction,
            score=min(1.0, abs(z) / max(self.zscore_entry, 1e-9)),
            size=1.0 if allow_trade else 0.0,
            sl_distance=sl_distance,
            tp_distance=tp_distance,
            indicators={
                "z": round(z, 3),
                "rolling_mean": round(m, 4),
                "rolling_std": round(s, 4),
                "micro_range": round(micro_range, 3) if np.isfinite(micro_range) else None,
                "htf_slope_per_atr": round(slope_per_atr, 4),
                "htf_atr": round(htf_atr, 4),
                "trend_strong": trend_strong,
                "trend_mode": self.trend_mode,
                "sl_mode": self.sl_mode,
                "tp_mode": self.tp_mode,
                "spread": spread,
            },
        )

    # ─────────────────────────────────────────────────────────────────────────
    def on_position_closed(self, position) -> None:
        if position.exit_reason != "SL":
            return
        if position.direction == "BUY":
            self._last_sl_ts_buy = position.exit_ts
        elif position.direction == "SELL":
            self._last_sl_ts_sell = position.exit_ts

    def _within_hours(self, ts: pd.Timestamp) -> bool:
        if not self.enable_session_filter:
            return True
        return self.session_start <= ts.hour < self.session_end

    @staticmethod
    def _distance_for_mode(mode: str, atr_value: float,
                            dollars: float, atr_mult: float) -> float:
        if mode == "fixed_dollars":
            return float(dollars)
        if mode == "atr_mult":
            return float(atr_value) * float(atr_mult)
        raise ValueError(f"Ismeretlen mode: {mode}")

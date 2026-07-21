from __future__ import annotations

from collections import deque
from typing import List, Optional, Tuple

import numpy as np
import pandas as pd

from ..indicators.candle_indicators import adx, atr, ema, rsi
from .base import Decision, Strategy, StrategyContext


VALID_SL_TP_MODES = ("fixed_dollars", "atr_mult")
VALID_ENTRY_MODES = ("immediate", "pullback")


class TrendReversal(Strategy):
    """
    EMA + RSI alapú trend stratégia tick imbalance confirmation-nel.
    Opcionális: HTF trend filter, ADX szűrő, igazi reversal logika,
    pull-back belépés, cooldown SL után, spread anomaly z-score.
    """
    name = "trend_reversal"

    def __init__(self, params: dict):
        super().__init__(params)
        p = params

        # ── Timeframe ──
        self.candle_tf: str = p["candle_tf"]
        self.htf_candle_tf: str = p.get("htf_candle_tf", "1h")
        self.require_htf_align: bool = p.get("require_htf_align", True)

        # ── Indikátorok ──
        self.ema_fast: int = p["ema_fast"]
        self.ema_slow: int = p["ema_slow"]
        self.htf_ema_fast: int = p.get("htf_ema_fast", 9)
        self.htf_ema_slow: int = p.get("htf_ema_slow", 21)
        self.rsi_period: int = p["rsi_period"]
        self.atr_period: int = p["atr_period"]
        self.adx_period: int = p.get("adx_period", 14)
        self.min_adx: float = p.get("min_adx", 20.0)

        # ── Trend & reversal mode ──
        self.enable_trend_mode: bool = p.get("enable_trend_mode", True)
        self.enable_reversal_mode: bool = p.get("enable_reversal_mode", False)
        self.rsi_long_level: float = p["rsi_long_level"]
        self.rsi_short_level: float = p["rsi_short_level"]
        self.rsi_oversold: float = p.get("rsi_oversold", 30.0)
        self.rsi_overbought: float = p.get("rsi_overbought", 70.0)
        self.rsi_recovery_min_delta: float = p.get("rsi_recovery_min_delta", 1.0)

        # ── Belépés mode ──
        self.entry_mode: str = p.get("entry_mode", "immediate")
        if self.entry_mode not in VALID_ENTRY_MODES:
            raise ValueError(f"entry_mode: {self.entry_mode} (választható: {VALID_ENTRY_MODES})")
        self.pullback_max_dist_ema_atr: float = p.get("pullback_max_dist_ema_atr", 0.5)

        # ── Tick szűrők ──
        self.tick_buffer_size: int = p["tick_buffer_size"]
        self.tick_confirm_window: int = p["tick_confirm_window"]
        self.tick_min_imbalance: float = p["tick_min_imbalance"]
        self.max_spread_atr: float = p["max_spread_atr"]
        self.spread_z_window: int = p.get("spread_z_window", 200)
        self.spread_z_max: float = p.get("spread_z_max", 2.5)

        # ── Score ──
        # NB: a tényleges méret az engine.sizing_mode szerint dől el.
        # A stratégia csak score-t számol; a sizer pl. score_scaled módban használja.
        self.score_threshold: float = p.get("score_threshold", 0.35)
        self.adx_strong_threshold: float = p.get("adx_strong_threshold", 35.0)
        self.htf_align_score_bonus: float = p.get("htf_align_score_bonus", 0.15)
        self.atr_extreme_pct: float = p.get("atr_extreme_pct", 0.95)
        self.atr_extreme_penalty: float = p.get("atr_extreme_penalty", 0.10)

        # ── SL/TP ──
        self.sl_mode: str = p.get("sl_mode", "atr_mult")
        self.tp_mode: str = p.get("tp_mode", "atr_mult")
        if self.sl_mode not in VALID_SL_TP_MODES:
            raise ValueError(f"sl_mode: {self.sl_mode} (választható: {VALID_SL_TP_MODES})")
        if self.tp_mode not in VALID_SL_TP_MODES:
            raise ValueError(f"tp_mode: {self.tp_mode} (választható: {VALID_SL_TP_MODES})")
        self.sl_dollars: float = p.get("sl_dollars", 0.0)
        self.tp_dollars: float = p.get("tp_dollars", 0.0)
        self.sl_atr_mult: float = p.get("sl_atr_mult", 1.5)
        self.tp_atr_mult: float = p.get("tp_atr_mult", 2.0)
        self.min_sl_distance: float = p.get("min_sl_distance", 0.1)

        # ── Cooldown ──
        self.cooldown_minutes: float = p.get("cooldown_minutes", 0.0)

        # ── Session ──
        self.enable_session_filter: bool = p.get("enable_session_filter", False)
        self.trading_start_hour_utc: int = p.get("trading_start_hour_utc", 0)
        self.trading_end_hour_utc: int = p.get("trading_end_hour_utc", 24)

        self.decision_interval_sec: float = p.get("decision_interval_sec", 15.0)

        # ── State ──
        self._ltf_arr: dict = {}                 # ema_fast, ema_slow, rsi, atr, adx
        self._ltf_ts: Optional[np.ndarray] = None
        self._htf_align_arr: Optional[np.ndarray] = None  # +1 BUY / -1 SELL / 0 neutral
        self._htf_ts: Optional[np.ndarray] = None
        self._atr_extreme_thr: Optional[float] = None

        self._tick_signs: deque = deque(maxlen=self.tick_buffer_size)
        self._last_mid: Optional[float] = None
        self._last_decision_ts: Optional[pd.Timestamp] = None

        self._spread_window: deque = deque(maxlen=self.spread_z_window)

        self._last_sl_ts_buy: Optional[pd.Timestamp] = None
        self._last_sl_ts_sell: Optional[pd.Timestamp] = None

    def required_timeframes(self) -> List[str]:
        return list(dict.fromkeys([self.candle_tf, self.htf_candle_tf]))

    # ─────────────────────────────────────────────────────────────────────────
    def on_segment_start(self, ctx: StrategyContext) -> None:
        ltf = ctx.candles_mtf[self.candle_tf].copy()
        ltf["ema_fast"] = ema(ltf["close"], self.ema_fast)
        ltf["ema_slow"] = ema(ltf["close"], self.ema_slow)
        ltf["rsi"] = rsi(ltf["close"], self.rsi_period)
        ltf["atr"] = atr(ltf["high"], ltf["low"], ltf["close"], self.atr_period)
        adx_v, _, _ = adx(ltf["high"], ltf["low"], ltf["close"], self.adx_period)
        ltf["adx"] = adx_v

        self._ltf_arr = {
            "close": ltf["close"].to_numpy(dtype=float),
            "ema_fast": ltf["ema_fast"].to_numpy(dtype=float),
            "ema_slow": ltf["ema_slow"].to_numpy(dtype=float),
            "rsi": ltf["rsi"].to_numpy(dtype=float),
            "atr": ltf["atr"].to_numpy(dtype=float),
            "adx": ltf["adx"].to_numpy(dtype=float),
        }
        self._ltf_ts = ltf["timestamp"].values
        self._atr_extreme_thr = float(np.nanquantile(self._ltf_arr["atr"], self.atr_extreme_pct))

        htf = ctx.candles_mtf[self.htf_candle_tf].copy()
        htf_ema_f = ema(htf["close"], self.htf_ema_fast).to_numpy(dtype=float)
        htf_ema_s = ema(htf["close"], self.htf_ema_slow).to_numpy(dtype=float)
        htf_close = htf["close"].to_numpy(dtype=float)
        htf_align = np.zeros(len(htf), dtype=int)
        htf_align[(htf_close > htf_ema_s) & (htf_ema_f > htf_ema_s)] = 1
        htf_align[(htf_close < htf_ema_s) & (htf_ema_f < htf_ema_s)] = -1
        self._htf_align_arr = htf_align
        self._htf_ts = htf["timestamp"].values

        self._tick_signs.clear()
        self._spread_window.clear()
        self._last_mid = None
        self._last_decision_ts = None
        self._last_sl_ts_buy = None
        self._last_sl_ts_sell = None

    # ─────────────────────────────────────────────────────────────────────────
    def on_tick(self, ts: pd.Timestamp, bid: float, ask: float) -> Optional[Decision]:
        mid = (bid + ask) / 2.0
        spread = ask - bid

        if self._last_mid is not None:
            d = mid - self._last_mid
            self._tick_signs.append(1 if d > 0 else -1 if d < 0 else 0)
        self._last_mid = mid
        self._spread_window.append(spread)

        if self._last_decision_ts is not None:
            if (ts - self._last_decision_ts).total_seconds() < self.decision_interval_sec:
                return None
        self._last_decision_ts = ts

        candle_idx = int(np.searchsorted(self._ltf_ts, ts.to_datetime64(), side="right") - 1)
        warmup = max(self.ema_slow, self.atr_period, self.adx_period) + 2
        if candle_idx < warmup:
            return None

        ef = float(self._ltf_arr["ema_fast"][candle_idx])
        es = float(self._ltf_arr["ema_slow"][candle_idx])
        rv = float(self._ltf_arr["rsi"][candle_idx])
        rv_prev = float(self._ltf_arr["rsi"][candle_idx - 1])
        atr_v = float(self._ltf_arr["atr"][candle_idx])
        adx_v = float(self._ltf_arr["adx"][candle_idx])
        close = float(self._ltf_arr["close"][candle_idx])
        if not (np.isfinite(ef) and np.isfinite(es) and np.isfinite(rv)
                and np.isfinite(atr_v) and np.isfinite(adx_v)):
            return None

        htf_idx = int(np.searchsorted(self._htf_ts, ts.to_datetime64(), side="right") - 1)
        htf_align = int(self._htf_align_arr[htf_idx]) if htf_idx >= 0 else 0

        action, direction = self._classify_signal(close, ef, es, rv, rv_prev)
        if direction is None:
            return None  # near-miss only: csak akkor logolunk, ha lenne irány

        tick_bias, imbalance = self._tick_summary()
        spread_ratio = (spread / atr_v) if atr_v > 0 else float("inf")
        spread_z = self._spread_zscore(spread)

        # ── Score ──
        score = self._compute_score(action, imbalance, spread_ratio, adx_v, htf_align, direction, atr_v)
        sl_distance, tp_distance = self._compute_sl_tp(atr_v)

        # ── Pull-back belépés (D) ──
        pullback_ok = self._pullback_ok(direction, ask, bid, ef, atr_v)

        # ── Reason gyűjtés ──
        reasons: List[str] = []
        hours_ok = self._within_hours(ts)
        if not hours_ok:
            reasons.append("outside_hours")

        tick_match = tick_bias == direction
        if not tick_match:
            reasons.append("tick_mismatch")

        if spread_ratio > self.max_spread_atr:
            reasons.append("spread_too_wide")
        if np.isfinite(spread_z) and spread_z > self.spread_z_max:
            reasons.append("spread_anomaly")

        if adx_v < self.min_adx:
            reasons.append("adx_too_low")

        if self.require_htf_align and htf_align != self._direction_to_sign(direction):
            reasons.append("htf_mismatch")

        if not pullback_ok:
            reasons.append("no_pullback")

        if score < self.score_threshold:
            reasons.append("score_too_low")

        if sl_distance < self.min_sl_distance:
            reasons.append("sl_too_small")
        if tp_distance is not None and tp_distance <= 0:
            reasons.append("tp_invalid")

        last_sl = self._last_sl_ts_buy if direction == "BUY" else self._last_sl_ts_sell
        if last_sl is not None and self.cooldown_minutes > 0:
            elapsed_min = (ts - last_sl).total_seconds() / 60.0
            if elapsed_min < self.cooldown_minutes:
                reasons.append("cooldown_after_sl")

        allow_trade = not reasons
        size = 1.0 if allow_trade else 0.0

        return Decision(
            ts=ts,
            allow_trade=allow_trade,
            reason="ok" if allow_trade else ";".join(reasons),
            direction=direction,
            score=round(score, 4),
            size=round(size, 4),
            sl_distance=round(sl_distance, 4),
            tp_distance=round(tp_distance, 4) if tp_distance else None,
            indicators={
                "price": close,
                "ema_fast": ef,
                "ema_slow": es,
                "rsi": rv,
                "atr": atr_v,
                "adx": adx_v,
                "htf_align": htf_align,
                "spread": spread,
                "spread_atr_ratio": round(spread_ratio, 4) if np.isfinite(spread_ratio) else None,
                "spread_z": round(spread_z, 4) if np.isfinite(spread_z) else None,
                "action": action,
                "tick_bias": tick_bias,
                "tick_imbalance": round(imbalance, 4),
                "sl_mode": self.sl_mode,
                "tp_mode": self.tp_mode,
                "entry_mode": self.entry_mode,
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

    # ─────────────────────────────────────────────────────────────────────────
    def _classify_signal(self, price, ef, es, rv, rv_prev) -> Tuple[str, Optional[str]]:
        # Trend
        if self.enable_trend_mode:
            if price > es and ef > es and rv >= self.rsi_long_level:
                return "BUY_TREND", "BUY"
            if price < es and ef < es and rv <= self.rsi_short_level:
                return "SELL_TREND", "SELL"
        # Igazi reversal: oversold/overbought + RSI fordul
        if self.enable_reversal_mode:
            if price < es and rv <= self.rsi_oversold and (rv - rv_prev) >= self.rsi_recovery_min_delta:
                return "BUY_REVERSAL", "BUY"
            if price > es and rv >= self.rsi_overbought and (rv_prev - rv) >= self.rsi_recovery_min_delta:
                return "SELL_REVERSAL", "SELL"
        return "HOLD", None

    @staticmethod
    def _direction_to_sign(direction: str) -> int:
        return 1 if direction == "BUY" else -1

    def _pullback_ok(self, direction: str, ask: float, bid: float, ef: float, atr_v: float) -> bool:
        if self.entry_mode != "pullback":
            return True
        max_dist = self.pullback_max_dist_ema_atr * atr_v
        if direction == "BUY":
            return abs(ask - ef) <= max_dist
        return abs(bid - ef) <= max_dist

    def _tick_summary(self):
        if len(self._tick_signs) < max(3, self.tick_confirm_window):
            return "NEUTRAL", 0.0
        window = list(self._tick_signs)[-self.tick_confirm_window:]
        nz = [x for x in window if x != 0]
        if not nz:
            return "NEUTRAL", 0.0
        imbalance = sum(nz) / len(nz)
        if imbalance >= self.tick_min_imbalance:
            return "BUY", imbalance
        if imbalance <= -self.tick_min_imbalance:
            return "SELL", imbalance
        return "NEUTRAL", imbalance

    def _spread_zscore(self, spread: float) -> float:
        if len(self._spread_window) < self.spread_z_window:
            return float("nan")
        sp = np.fromiter(self._spread_window, dtype=float)
        mean = sp.mean()
        std = sp.std()
        return (spread - mean) / (std + 1e-10)

    def _compute_score(self, action, imbalance, spread_ratio, adx_v, htf_align, direction, atr_v) -> float:
        score = 0.0
        if action in ("BUY_TREND", "SELL_TREND"):
            score += 0.40
        elif action in ("BUY_REVERSAL", "SELL_REVERSAL"):
            score += 0.25
        score += min(0.25, abs(imbalance) * 0.25)
        if np.isfinite(spread_ratio):
            score += max(0.0, 0.15 - spread_ratio)

        # ADX bonus (trend strength)
        if np.isfinite(adx_v) and adx_v >= self.min_adx:
            extra = min(0.15, (adx_v - self.min_adx) / max(self.adx_strong_threshold - self.min_adx, 1e-9) * 0.15)
            score += extra

        # HTF align bonus
        if htf_align == self._direction_to_sign(direction):
            score += self.htf_align_score_bonus

        # Volatility regime penalty (extreme ATR)
        if self._atr_extreme_thr is not None and atr_v > self._atr_extreme_thr:
            score -= self.atr_extreme_penalty

        return max(0.0, min(1.0, score))

    def _compute_sl_tp(self, atr_value: float) -> tuple[float, float]:
        sl = self._distance_for_mode(self.sl_mode, atr_value,
                                     dollars=self.sl_dollars, atr_mult=self.sl_atr_mult)
        tp = self._distance_for_mode(self.tp_mode, atr_value,
                                     dollars=self.tp_dollars, atr_mult=self.tp_atr_mult)
        return sl, tp

    @staticmethod
    def _distance_for_mode(mode: str, atr_value: float, dollars: float, atr_mult: float) -> float:
        if mode == "fixed_dollars":
            return float(dollars)
        if mode == "atr_mult":
            return float(atr_value) * float(atr_mult)
        raise ValueError(f"Ismeretlen mode: {mode}")

    def _within_hours(self, ts: pd.Timestamp) -> bool:
        if not self.enable_session_filter:
            return True
        h = ts.hour
        return self.trading_start_hour_utc <= h < self.trading_end_hour_utc

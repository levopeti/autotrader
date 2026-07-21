from __future__ import annotations

from typing import List, Optional

import numpy as np
import pandas as pd

from ..indicators.candle_indicators import (
    adx,
    atr,
    bb_squeeze_percentile,
    bollinger_bands,
    rolling_range,
)
from .base import Decision, Strategy, StrategyContext


VALID_SL_TP_MODES = ("fixed_dollars", "atr_mult", "range_pct")


class RangeScalp(Strategy):
    """
    Mean-reversion range scalping: ADX + BB squeeze + rolling support/resistance.
    LTF + HTF range filter, belépés a S/R közelében entry_buffer-rel.
    """
    name = "range_scalp"

    def __init__(self, params: dict):
        super().__init__(params)
        p = params
        self.ltf: str = p["ltf_candle_tf"]
        self.htf: str = p["htf_candle_tf"]

        self.adx_period: int = p["adx_period"]
        self.adx_threshold: float = p["adx_threshold"]
        self.bb_period: int = p["bb_period"]
        self.bb_std: float = p["bb_std"]
        self.bb_squeeze_pct: float = p["bb_squeeze_pct"]
        self.range_lookback: int = p["range_lookback"]
        self.entry_buffer: float = p["entry_buffer"]

        self.tick_vel_window: int = p["tick_vel_window"]
        self.tick_vel_max: float = p["tick_vel_max"]
        self.spread_z_window: int = p["spread_z_window"]
        self.spread_z_max: float = p["spread_z_max"]

        self.entry_zone_pct: float = p.get("entry_zone_pct", 0.30)
        self.min_range_atr_mult: float = p.get("min_range_atr_mult", 1.5)
        self.max_range_atr_mult: float = p.get("max_range_atr_mult", 30.0)
        self.cooldown_minutes: float = p.get("cooldown_minutes", 30.0)
        self.tp_max_range_pct: float = p.get("tp_max_range_pct", 1.0)
        self.tick_imbalance_window: int = p.get("tick_imbalance_window", 10)
        self.tick_imbalance_max_opposite: float = p.get("tick_imbalance_max_opposite", 0.5)

        # NB: a tényleges méret az engine.sizing_mode szerint dől el. A stratégia csak
        # azt jelzi (Decision.size = 1.0), hogy "lenne nyitási szándék". A score-t
        # ad át a sizer-nek (de a range_scalp nem számol score-t, így fixed_lot vagy
        # fixed_risk módot érdemes használni).

        self.sl_mode: str = p.get("sl_mode", "fixed_dollars")
        self.tp_mode: str = p.get("tp_mode", "fixed_dollars")
        if self.sl_mode not in VALID_SL_TP_MODES:
            raise ValueError(f"sl_mode: {self.sl_mode} (válaszható: {VALID_SL_TP_MODES})")
        if self.tp_mode not in VALID_SL_TP_MODES:
            raise ValueError(f"tp_mode: {self.tp_mode} (válaszható: {VALID_SL_TP_MODES})")

        self.sl_dollars: float = p.get("sl_dollars", 0.0)
        self.tp_dollars: float = p.get("tp_dollars", 0.0)
        self.sl_atr_mult: float = p.get("sl_atr_mult", 1.0)
        self.tp_atr_mult: float = p.get("tp_atr_mult", 2.0)
        self.sl_range_pct: float = p.get("sl_range_pct", 0.5)
        self.tp_range_pct: float = p.get("tp_range_pct", 0.7)
        self.atr_period: int = p.get("atr_period", 14)
        self.min_sl_distance: float = p.get("min_sl_distance", 0.1)

        self.enable_session_filter: bool = p.get("enable_session_filter", False)
        self.session_start: int = p.get("session_start", 0)
        self.session_end: int = p.get("session_end", 24)

        self._support_arr: Optional[np.ndarray] = None
        self._resistance_arr: Optional[np.ndarray] = None
        self._adx_ltf_arr: Optional[np.ndarray] = None
        self._bb_bw_ltf_arr: Optional[np.ndarray] = None
        self._atr_ltf_arr: Optional[np.ndarray] = None
        self._ranging_ltf_arr: Optional[np.ndarray] = None
        self._ranging_htf_arr: Optional[np.ndarray] = None
        self._ltf_idx_per_tick: Optional[np.ndarray] = None
        self._htf_idx_per_tick: Optional[np.ndarray] = None

        self._tick_vel_arr: Optional[np.ndarray] = None
        self._spread_z_arr: Optional[np.ndarray] = None
        self._tick_imbalance_arr: Optional[np.ndarray] = None
        self._tick_i: int = 0
        self._warmup: int = 0

        # Cooldown state (direction-szerű): legutóbbi SL időpontja BUY / SELL irányba
        self._last_sl_ts_buy: Optional[pd.Timestamp] = None
        self._last_sl_ts_sell: Optional[pd.Timestamp] = None

    def required_timeframes(self) -> List[str]:
        return list(dict.fromkeys([self.ltf, self.htf]))

    def on_segment_start(self, ctx: StrategyContext) -> None:
        sig_ltf = self._build_signals(ctx.candles_mtf[self.ltf])
        sig_htf = self._build_signals(ctx.candles_mtf[self.htf])

        self._support_arr = sig_ltf["support"].to_numpy(dtype=float)
        self._resistance_arr = sig_ltf["resistance"].to_numpy(dtype=float)
        self._adx_ltf_arr = sig_ltf["adx"].to_numpy(dtype=float)
        self._bb_bw_ltf_arr = sig_ltf["bb_bw"].to_numpy(dtype=float)
        self._atr_ltf_arr = sig_ltf["atr"].to_numpy(dtype=float)
        self._ranging_ltf_arr = sig_ltf["is_ranging"].to_numpy(dtype=bool)
        self._ranging_htf_arr = sig_htf["is_ranging"].to_numpy(dtype=bool)
        ltf_ts = sig_ltf["timestamp"].values
        htf_ts = sig_htf["timestamp"].values

        if ctx.segment_ticks is None:
            raise RuntimeError("RangeScalp: a StrategyContext-ben kell legyen segment_ticks")
        ticks = ctx.segment_ticks
        ts_arr = ticks["timestamp_utc"].values
        mid = ticks["mid"].to_numpy(dtype=float)
        spread = ticks["spread"].to_numpy(dtype=float)

        self._ltf_idx_per_tick = (np.searchsorted(ltf_ts, ts_arr, side="right") - 1).astype(np.int64)
        self._htf_idx_per_tick = (np.searchsorted(htf_ts, ts_arr, side="right") - 1).astype(np.int64)

        mid_diff = pd.Series(mid).diff()
        self._tick_vel_arr = mid_diff.abs().rolling(self.tick_vel_window).sum().to_numpy()

        sign_arr = np.sign(mid_diff.to_numpy())
        self._tick_imbalance_arr = (
            pd.Series(sign_arr).rolling(self.tick_imbalance_window).mean().to_numpy()
        )

        sp = pd.Series(spread)
        sp_mean = sp.rolling(self.spread_z_window).mean()
        sp_std = sp.rolling(self.spread_z_window).std()
        self._spread_z_arr = ((sp - sp_mean) / (sp_std + 1e-10)).to_numpy()

        self._tick_i = 0
        self._warmup = max(self.tick_vel_window, self.spread_z_window, self.tick_imbalance_window)

        self._last_sl_ts_buy = None
        self._last_sl_ts_sell = None

    def on_tick(self, ts: pd.Timestamp, bid: float, ask: float) -> Optional[Decision]:
        mid = (bid + ask) / 2.0
        spread = ask - bid

        i = self._tick_i
        self._tick_i += 1
        if i < self._warmup:
            return None

        tick_vel = float(self._tick_vel_arr[i])
        spread_z = float(self._spread_z_arr[i])
        tick_imb = float(self._tick_imbalance_arr[i])
        if not (np.isfinite(tick_vel) and np.isfinite(spread_z) and np.isfinite(tick_imb)):
            return None

        ltf_idx = int(self._ltf_idx_per_tick[i])
        htf_idx = int(self._htf_idx_per_tick[i])
        if ltf_idx < self.range_lookback or htf_idx < self.range_lookback:
            return None

        support = float(self._support_arr[ltf_idx])
        resistance = float(self._resistance_arr[ltf_idx])
        adx_l = float(self._adx_ltf_arr[ltf_idx])
        bb_bw_l = float(self._bb_bw_ltf_arr[ltf_idx])
        atr_l = float(self._atr_ltf_arr[ltf_idx])
        ranging_ltf = bool(self._ranging_ltf_arr[ltf_idx])
        ranging_htf = bool(self._ranging_htf_arr[htf_idx])
        range_size = max(resistance - support, 0.0)

        reasons: List[str] = []
        hours_ok = self._within_hours(ts)
        if not hours_ok:
            reasons.append("outside_hours")
        if not (ranging_ltf and ranging_htf):
            reasons.append("not_ranging")
        if tick_vel > self.tick_vel_max:
            reasons.append("tick_vel_too_high")
        if spread_z > self.spread_z_max:
            reasons.append("spread_anomaly")

        # A) Range-pozíció: csak az alsó / felső entry_zone_pct sávban
        buy_zone_high = support + range_size * self.entry_zone_pct
        sell_zone_low = resistance - range_size * self.entry_zone_pct

        direction = None
        if ask <= support + self.entry_buffer and ask <= buy_zone_high:
            direction = "BUY"
        elif bid >= resistance - self.entry_buffer and bid >= sell_zone_low:
            direction = "SELL"

        if direction is None:
            return None

        # B) Range size sanity (ATR-arányos)
        range_atr_ratio = (range_size / atr_l) if atr_l > 1e-9 else float("inf")
        if range_atr_ratio < self.min_range_atr_mult:
            reasons.append("range_too_narrow")
        if range_atr_ratio > self.max_range_atr_mult:
            reasons.append("range_too_wide")

        # C) Cooldown SL után
        last_sl = self._last_sl_ts_buy if direction == "BUY" else self._last_sl_ts_sell
        if last_sl is not None:
            elapsed_min = (ts - last_sl).total_seconds() / 60.0
            if elapsed_min < self.cooldown_minutes:
                reasons.append("cooldown_after_sl")

        # E) Tick imbalance: ellentétes momentum tilt
        if direction == "BUY" and tick_imb < -self.tick_imbalance_max_opposite:
            reasons.append("opposite_momentum")
        elif direction == "SELL" and tick_imb > self.tick_imbalance_max_opposite:
            reasons.append("opposite_momentum")

        sl_dist, tp_dist = self._compute_sl_tp(atr_l, range_size)
        if not np.isfinite(sl_dist) or sl_dist < self.min_sl_distance:
            reasons.append("sl_too_small")
        if tp_dist is not None and (not np.isfinite(tp_dist) or tp_dist <= 0):
            reasons.append("tp_invalid")

        # D) TP a range-en belül legyen (kivéve range_pct mode, ami eleve range-arányos)
        if self.tp_mode != "range_pct" and tp_dist is not None and range_size > 0:
            if tp_dist > range_size * self.tp_max_range_pct:
                reasons.append("tp_outside_range")

        allow_trade = (direction is not None) and (not reasons)
        return Decision(
            ts=ts,
            allow_trade=allow_trade,
            reason="ok" if allow_trade else (";".join(reasons) if reasons else "no_zone"),
            direction=direction,
            score=None,
            size=1.0 if allow_trade else 0.0,
            sl_distance=sl_dist,
            tp_distance=tp_dist,
            indicators={
                "price_mid": mid,
                "support": support,
                "resistance": resistance,
                "spread": spread,
                "tick_vel": tick_vel,
                "tick_imbalance": tick_imb,
                "spread_z": float(spread_z),
                "range_atr_ratio": round(range_atr_ratio, 4) if np.isfinite(range_atr_ratio) else None,
                "adx_ltf": adx_l,
                "atr_ltf": atr_l,
                "bb_bw_ltf": bb_bw_l,
                "range_size": range_size,
                "sl_mode": self.sl_mode,
                "tp_mode": self.tp_mode,
                "is_ranging_ltf": ranging_ltf,
                "is_ranging_htf": ranging_htf,
            },
        )

    def _compute_sl_tp(self, atr_value: float, range_size: float) -> tuple[float, Optional[float]]:
        sl = self._distance_for_mode(self.sl_mode, atr_value, range_size,
                                     dollars=self.sl_dollars, atr_mult=self.sl_atr_mult, range_pct=self.sl_range_pct)
        tp = self._distance_for_mode(self.tp_mode, atr_value, range_size,
                                     dollars=self.tp_dollars, atr_mult=self.tp_atr_mult, range_pct=self.tp_range_pct)
        return sl, tp

    @staticmethod
    def _distance_for_mode(mode: str, atr_value: float, range_size: float,
                            dollars: float, atr_mult: float, range_pct: float) -> float:
        if mode == "fixed_dollars":
            return float(dollars)
        if mode == "atr_mult":
            return float(atr_value) * float(atr_mult)
        if mode == "range_pct":
            return float(range_size) * float(range_pct)
        raise ValueError(f"Ismeretlen mode: {mode}")

    def _build_signals(self, candles: pd.DataFrame) -> pd.DataFrame:
        df = candles.copy()
        c, h, l = df["close"], df["high"], df["low"]
        adx_v, _, _ = adx(h, l, c, self.adx_period)
        _, _, _, bw = bollinger_bands(c, self.bb_period, self.bb_std)
        squeeze = bb_squeeze_percentile(bw)
        df["adx"] = adx_v
        df["bb_bw"] = bw
        df["bb_squeeze_pct"] = squeeze
        df["atr"] = atr(h, l, c, self.atr_period)
        df["resistance"], df["support"] = rolling_range(h, l, self.range_lookback)
        df["is_ranging"] = (adx_v < self.adx_threshold) & (squeeze < self.bb_squeeze_pct)
        return df

    def _within_hours(self, ts: pd.Timestamp) -> bool:
        if not self.enable_session_filter:
            return True
        h = ts.hour
        return self.session_start <= h < self.session_end

    def on_position_closed(self, position) -> None:
        if position.exit_reason != "SL":
            return
        if position.direction == "BUY":
            self._last_sl_ts_buy = position.exit_ts
        elif position.direction == "SELL":
            self._last_sl_ts_sell = position.exit_ts
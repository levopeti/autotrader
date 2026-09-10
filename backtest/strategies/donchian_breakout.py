"""
Donchian breakout ("Turtle Traders") strategia.

- Long, ha az ár átüti az utolsó N periódus HIGH-ját
- Short, ha átüti az utolsó N periódus LOW-ját
- SL: ATR × sl_atr_mult (opcionálisan az ellentétes Donchian sáv)
- TP: ATR × tp_atr_mult (fixed R/R)
- Trailing stop-ot az engine biztosítja (atr_trail vagy break_even)

Klasszikus, jól tesztelt kriptón. Trending piacokra optimális, whipsaw-érzékeny.
"""
from __future__ import annotations

from typing import Dict, List, Optional

import numpy as np
import pandas as pd

from ..indicators.candle_indicators import atr as _atr, ema
from .base import Decision, Strategy, StrategyContext


def _tf_td64(tf: str) -> np.timedelta64:
    return np.timedelta64(int(pd.Timedelta(tf).value), "ns")


class DonchianBreakout(Strategy):
    name = "donchian_breakout"

    def __init__(self, params: dict):
        super().__init__(params)
        p = params
        self.candle_tf: str = p["candle_tf"]
        self.donchian_period: int = int(p.get("donchian_period", 20))
        self.atr_period: int = int(p.get("atr_period", 14))
        self.sl_atr_mult: float = float(p.get("sl_atr_mult", 2.0))
        self.tp_atr_mult: float = float(p.get("tp_atr_mult", 5.0))
        self.buffer: float = float(p.get("breakout_buffer", 0.0))  # $ buffer a kitörés fölött

        # Trend filter (opcionális)
        self.trend_filter_enabled: bool = bool(p.get("trend_filter_enabled", False))
        self.trend_filter_tf: str = p.get("trend_filter_tf", "4h")
        self.trend_ema_fast: int = int(p.get("trend_ema_fast", 9))
        self.trend_ema_slow: int = int(p.get("trend_ema_slow", 21))

        # Cooldown re-entry ellen
        self.cooldown_bars: int = int(p.get("cooldown_bars", 5))

        # Equity-curve filter: K egymás utáni VALÓS veszteség után papír-módba
        # vált (a signalok papíron szimulálódnak, éles nyitás nincs), és az
        # első papír-NYERTES után tér vissza élesbe. A stratégia maga a saját
        # legjobb chop-detektora: a sorozatos SL a ranging-rezsim jele.
        # 2y validáció: K=3..6 plató, K=4: +19% PnL, max DD -20%→-11%.
        # 0 = kikapcsolva.
        self.eq_filter_k: int = int(p.get("eq_filter_k", 0))
        self.eq_filter_max_hold_h: float = float(p.get("eq_filter_max_hold_h", 72.0))

        self._consec_losses: int = 0
        self._paper_mode: bool = False
        self._paper_pos: Optional[dict] = None

        # State
        self._high_arr: Optional[np.ndarray] = None
        self._low_arr: Optional[np.ndarray] = None
        self._atr_arr: Optional[np.ndarray] = None
        self._ltf_close_ts: Optional[np.ndarray] = None
        self._trend_fast_arr: Optional[np.ndarray] = None
        self._trend_slow_arr: Optional[np.ndarray] = None
        self._trend_close_ts: Optional[np.ndarray] = None
        self._last_signal_idx: int = -1_000_000

    def on_position_closed(self, position) -> None:
        if not self.eq_filter_k:
            return
        pnl = getattr(position, "pnl", 0.0) or 0.0
        if pnl > 0:
            self._consec_losses = 0
        else:
            self._consec_losses += 1
            if self._consec_losses >= self.eq_filter_k and not self._paper_mode:
                self._paper_mode = True

    def _paper_update(self, ts: pd.Timestamp, bid: float, ask: float) -> None:
        """Papír-pozíció szimuláció (engine-azonos szemantika: ATR-trail + max_hold)."""
        pp = self._paper_pos
        if pp is None:
            return
        atr_v = pp["atr"]
        trail = 2.0 * atr_v            # engine trail_atr_mult=2.0 (donchian_live)
        exit_price = None
        if pp["direction"] == "BUY":
            new_sl = bid - trail
            if new_sl > pp["sl"]:
                pp["sl"] = new_sl
            if bid <= pp["sl"]:
                exit_price = pp["sl"]
        else:
            new_sl = ask + trail
            if new_sl < pp["sl"]:
                pp["sl"] = new_sl
            if ask >= pp["sl"]:
                exit_price = pp["sl"]
        held_h = (ts - pp["entry_ts"]).total_seconds() / 3600.0
        if exit_price is None and held_h >= self.eq_filter_max_hold_h:
            exit_price = bid if pp["direction"] == "BUY" else ask
        if exit_price is None:
            return
        win = (exit_price > pp["entry"]) if pp["direction"] == "BUY" else (exit_price < pp["entry"])
        self._paper_pos = None
        if win:
            self._paper_mode = False
            self._consec_losses = 0
        # papír-loss: papír-módban maradunk, jön a következő papír-trade

    def required_timeframes(self) -> List[str]:
        tfs = [self.candle_tf]
        if self.trend_filter_enabled and self.trend_filter_tf not in tfs:
            tfs.append(self.trend_filter_tf)
        return tfs

    def on_segment_start(self, ctx: StrategyContext) -> None:
        self.refresh_candles(ctx.candles_mtf)

    def refresh_candles(self, candles_mtf) -> None:
        ltf = candles_mtf[self.candle_tf].copy()
        # Az N-periódus rolling high/low CSAK az AKTUÁLIS gyertya ELŐTTI N gyertyát
        # nézi — a mostani nyitóár nem szerepel benne, hogy no-look-ahead legyen.
        self._high_arr = ltf["high"].shift(1).rolling(self.donchian_period).max().to_numpy(dtype=float)
        self._low_arr  = ltf["low"].shift(1).rolling(self.donchian_period).min().to_numpy(dtype=float)
        self._atr_arr  = _atr(ltf["high"], ltf["low"], ltf["close"], self.atr_period).to_numpy(dtype=float)
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
        if self._paper_pos is not None:
            self._paper_update(ts, bid, ask)
        ts_np = ts.to_datetime64()
        i = int(np.searchsorted(self._ltf_close_ts, ts_np, side="right") - 1)
        if i < self.donchian_period + self.atr_period:
            return None

        high_n = float(self._high_arr[i])
        low_n  = float(self._low_arr[i])
        atr_v  = float(self._atr_arr[i])
        if not (np.isfinite(high_n) and np.isfinite(low_n) and np.isfinite(atr_v) and atr_v > 0):
            return None

        # Cooldown: N gyertya per irány
        if (i - self._last_signal_idx) < self.cooldown_bars:
            return None

        direction: Optional[str] = None
        if ask > high_n + self.buffer:
            direction = "BUY"
        elif bid < low_n - self.buffer:
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

        # Equity-filter: papír-módban a signal PAPÍR-pozíciót nyit, élest nem
        if self.eq_filter_k and self._paper_mode:
            if self._paper_pos is None:
                entry = ask if direction == "BUY" else bid
                sl0 = entry - sl_dist if direction == "BUY" else entry + sl_dist
                self._paper_pos = {"direction": direction, "entry": entry,
                                   "sl": sl0, "atr": atr_v, "entry_ts": ts}
            return Decision(ts=ts, allow_trade=False, reason="equity_filter_paper",
                            direction=direction,
                            indicators={"consec_losses": self._consec_losses})

        return Decision(
            ts=ts, allow_trade=True, reason="donchian_breakout",
            direction=direction, score=1.0, size=1.0,
            sl_distance=sl_dist, tp_distance=tp_dist,
            indicators={
                "donchian_high": high_n, "donchian_low": low_n,
                "atr": atr_v, "n_period": self.donchian_period,
            },
        )

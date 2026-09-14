"""
HTF trend + LTF pullback entry strategia.

Logika:
  1. HTF (pl. 1h/4h) EMA-slope adja a fő irányt: BUY ha EMA_fast > EMA_slow
  2. LTF (pl. 5min) pullback: az ár visszatér az LTF gyors EMA-hoz (vagy alá/fölé)
  3. Belépés az irány folytatásában, amikor az LTF ár VISSZAFORDUL az EMA-tól
  4. SL: az EMA + ATR-alapú buffer (ellentétes irányba)
  5. TP: ATR-arányos vagy trailing

Jobb R:R mint a naiv trend-follow, mert a pullback-belépés kedvezőbb áron.
"""
from __future__ import annotations

from typing import Dict, List, Optional

import numpy as np
import pandas as pd

from ..indicators.candle_indicators import atr as _atr, ema
from .base import Decision, Strategy, StrategyContext


def _tf_td64(tf: str) -> np.timedelta64:
    return np.timedelta64(int(pd.Timedelta(tf).value), "ns")


class HtfTrendPullback(Strategy):
    name = "htf_trend_pullback"

    def __init__(self, params: dict):
        super().__init__(params)
        p = params
        self.candle_tf: str = p["candle_tf"]           # LTF
        self.htf_candle_tf: str = p["htf_candle_tf"]   # HTF trend definícióhoz

        self.htf_ema_fast: int = int(p.get("htf_ema_fast", 21))
        self.htf_ema_slow: int = int(p.get("htf_ema_slow", 50))
        self.ltf_ema_fast: int = int(p.get("ltf_ema_fast", 9))
        self.ltf_ema_slow: int = int(p.get("ltf_ema_slow", 21))

        self.atr_period: int = int(p.get("atr_period", 14))
        self.sl_atr_mult: float = float(p.get("sl_atr_mult", 2.0))
        self.tp_atr_mult: float = float(p.get("tp_atr_mult", 4.0))

        # Pullback küszöb: az ár mennyire közelítse meg a LTF gyors EMA-t (ATR-arányos)
        self.pullback_max_dist_atr: float = float(p.get("pullback_max_dist_atr", 0.5))

        # Reverse-tick küszöb: hány tick vissza az EMA felé (min)
        self.rebound_min_atr: float = float(p.get("rebound_min_atr", 0.2))

        self.cooldown_bars: int = int(p.get("cooldown_bars", 10))

        # State
        self._ltf_ema_fast_arr: Optional[np.ndarray] = None
        self._ltf_ema_slow_arr: Optional[np.ndarray] = None
        self._ltf_atr_arr: Optional[np.ndarray] = None
        self._ltf_close_ts: Optional[np.ndarray] = None
        self._htf_ema_fast_arr: Optional[np.ndarray] = None
        self._htf_ema_slow_arr: Optional[np.ndarray] = None
        self._htf_close_ts: Optional[np.ndarray] = None

        # Pullback állapot: legutóbbi "közelség" pillanata (i, direction, extreme_price)
        self._pullback_state: Optional[dict] = None
        self._last_signal_idx: int = -1_000_000

    def required_timeframes(self) -> List[str]:
        return list(dict.fromkeys([self.candle_tf, self.htf_candle_tf]))

    def on_segment_start(self, ctx: StrategyContext) -> None:
        self.refresh_candles(ctx.candles_mtf)

    def refresh_candles(self, candles_mtf) -> None:
        ltf = candles_mtf[self.candle_tf].copy()
        htf = candles_mtf[self.htf_candle_tf].copy()
        self._ltf_ema_fast_arr = ema(ltf["close"], self.ltf_ema_fast).to_numpy(dtype=float)
        self._ltf_ema_slow_arr = ema(ltf["close"], self.ltf_ema_slow).to_numpy(dtype=float)
        self._ltf_atr_arr = _atr(ltf["high"], ltf["low"], ltf["close"], self.atr_period).to_numpy(dtype=float)
        self._ltf_close_ts = ltf["timestamp"].values + _tf_td64(self.candle_tf)

        self._htf_ema_fast_arr = ema(htf["close"], self.htf_ema_fast).to_numpy(dtype=float)
        self._htf_ema_slow_arr = ema(htf["close"], self.htf_ema_slow).to_numpy(dtype=float)
        self._htf_close_ts = htf["timestamp"].values + _tf_td64(self.htf_candle_tf)

    def _htf_trend(self, ts_np) -> Optional[str]:
        i = int(np.searchsorted(self._htf_close_ts, ts_np, side="right") - 1)
        if i < 0 or i >= len(self._htf_ema_fast_arr):
            return None
        f = float(self._htf_ema_fast_arr[i])
        s = float(self._htf_ema_slow_arr[i])
        if not (np.isfinite(f) and np.isfinite(s)):
            return None
        if f > s: return "BUY"
        if f < s: return "SELL"
        return None

    def on_tick(self, ts: pd.Timestamp, bid: float, ask: float) -> Optional[Decision]:
        ts_np = ts.to_datetime64()
        i = int(np.searchsorted(self._ltf_close_ts, ts_np, side="right") - 1)
        warmup = max(self.ltf_ema_slow, self.atr_period) + 2
        if i < warmup:
            return None

        atr_v = float(self._ltf_atr_arr[i])
        ema_f = float(self._ltf_ema_fast_arr[i])
        ema_s = float(self._ltf_ema_slow_arr[i])
        if not all(np.isfinite(x) for x in (atr_v, ema_f, ema_s)) or atr_v <= 0:
            return None

        htf_trend = self._htf_trend(ts_np)
        if htf_trend is None:
            return None

        mid = (bid + ask) / 2.0

        # Pullback detection: az ár közel van (vagy át-lépi) az LTF EMA-fastot
        # az irány ELLENÉBEN
        dist_to_fast_atr = abs(mid - ema_f) / atr_v

        # Cooldown
        if (i - self._last_signal_idx) < self.cooldown_bars:
            self._pullback_state = None
            return None

        # BUY setup: HTF up-trend + ár pullback-ban a fast EMA felé/alá
        # SELL setup: HTF down-trend + ár pullback-ban a fast EMA felé/fölé
        if htf_trend == "BUY":
            # A pullback állapotot fenntartjuk, amíg az ár közel/alul van
            if mid <= ema_f + self.pullback_max_dist_atr * atr_v:
                if self._pullback_state is None or self._pullback_state.get("dir") != "BUY":
                    self._pullback_state = {"dir": "BUY", "low": mid, "start_i": i}
                else:
                    if mid < self._pullback_state["low"]:
                        self._pullback_state["low"] = mid
            else:
                # Ha volt pullback ÉS az ár most rebound-ol (mid − low >= rebound_min_atr × ATR)
                if (self._pullback_state is not None
                        and self._pullback_state.get("dir") == "BUY"
                        and (mid - self._pullback_state["low"]) >= self.rebound_min_atr * atr_v):
                    self._last_signal_idx = i
                    self._pullback_state = None
                    return Decision(
                        ts=ts, allow_trade=True, reason="pullback_rebound",
                        direction="BUY", score=1.0, size=1.0,
                        sl_distance=self.sl_atr_mult * atr_v,
                        tp_distance=self.tp_atr_mult * atr_v,
                        indicators={"ema_f": ema_f, "ema_s": ema_s, "atr": atr_v,
                                    "htf_trend": htf_trend},
                    )
        else:  # SELL
            if mid >= ema_f - self.pullback_max_dist_atr * atr_v:
                if self._pullback_state is None or self._pullback_state.get("dir") != "SELL":
                    self._pullback_state = {"dir": "SELL", "high": mid, "start_i": i}
                else:
                    if mid > self._pullback_state["high"]:
                        self._pullback_state["high"] = mid
            else:
                if (self._pullback_state is not None
                        and self._pullback_state.get("dir") == "SELL"
                        and (self._pullback_state["high"] - mid) >= self.rebound_min_atr * atr_v):
                    self._last_signal_idx = i
                    self._pullback_state = None
                    return Decision(
                        ts=ts, allow_trade=True, reason="pullback_rebound",
                        direction="SELL", score=1.0, size=1.0,
                        sl_distance=self.sl_atr_mult * atr_v,
                        tp_distance=self.tp_atr_mult * atr_v,
                        indicators={"ema_f": ema_f, "ema_s": ema_s, "atr": atr_v,
                                    "htf_trend": htf_trend},
                    )

        return None

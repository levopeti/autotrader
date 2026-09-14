"""
London Breakout (LOB) — Ázsia-range kitörés stratégia.

Szakirodalom-alapú session-stratégia (lásd memory/gold_strategy_literature.md):
  - Ázsiai session (00:00-07:00 UTC) high/low = range
  - London-nyitás után (07-13 UTC belépő-ablak) kitörés + buffer → entry
  - SL: a range túloldala
  - Exit: session-zárás 20:00 UTC (TIME_EXIT) vagy SL
  - Napi trend-szűrő: csak az EMA20(daily) irányába
  - Max 1 trade / nap

2 éves Dukascopy validáció (standalone sim, slip 0.30):
  buf=0.05: +$29.5/hó, 4/4 félév pozitív, PF 1.27, WR 52%
"""
from __future__ import annotations

from typing import Dict, List, Optional

import numpy as np
import pandas as pd

from .base import Decision, Strategy, StrategyContext


class LondonBreakout(Strategy):
    name = "london_breakout"

    def __init__(self, params: dict):
        super().__init__(params)
        p = params
        self.asia_end_hour: int = int(p.get("asia_end_hour", 7))
        self.entry_end_hour: int = int(p.get("entry_end_hour", 13))
        self.session_close_hour: int = int(p.get("session_close_hour", 20))
        self.buffer_datr_frac: float = float(p.get("buffer_datr_frac", 0.05))
        self.max_range_datr_mult: float = float(p.get("max_range_datr_mult", 1.2))
        self.datr_period: int = int(p.get("datr_period", 14))
        self.trend_ema: int = int(p.get("trend_ema", 20))
        self.trend_filter_enabled: bool = bool(p.get("trend_filter_enabled", True))
        self.min_asia_bars: int = int(p.get("min_asia_bars", 6))
        # Belépési órák whitelistje (UTC). Ha None, minden asia_end_hour..entry_end_hour
        # közötti óra engedélyezett. Pl. [7,9,10,11,12] → hour 8 kizárva.
        eha = p.get("entry_hours_allowed")
        self.entry_hours_allowed: Optional[set] = set(int(x) for x in eha) if eha else None
        # TP-t a napi range (dATR) többszöröseként; None → nincs TP (default).
        # Csak akkor van értelme, ha tp_layers is konfigurált (partial-TP).
        self.tp_datr_mult: Optional[float] = p.get("tp_datr_mult")

        self._asia_hi: Dict = {}
        self._asia_lo: Dict = {}
        self._asia_n: Dict = {}
        self._datr: Dict = {}
        self._trend: Dict = {}
        self._last_trade_date = None

    def required_timeframes(self) -> List[str]:
        return ["1h", "1d"]

    def on_segment_start(self, ctx: StrategyContext) -> None:
        self.refresh_candles(ctx.candles_mtf)

    def refresh_candles(self, candles_mtf) -> None:
        h1 = candles_mtf.get("1h")
        d1 = candles_mtf.get("1d")
        if h1 is None or d1 is None or len(h1) == 0 or len(d1) == 0:
            return

        ts = pd.to_datetime(h1["timestamp"])
        dates = ts.dt.date
        hours = ts.dt.hour
        asia = h1[hours < self.asia_end_hour]
        asia_dates = dates[hours < self.asia_end_hour]
        grp_hi = asia.groupby(asia_dates)["high"].max()
        grp_lo = asia.groupby(asia_dates)["low"].min()
        grp_n = asia.groupby(asia_dates).size()
        self._asia_hi = grp_hi.to_dict()
        self._asia_lo = grp_lo.to_dict()
        self._asia_n = grp_n.to_dict()

        dts = pd.to_datetime(d1["timestamp"]).dt.date
        # Napi range-átlag (dATR proxy) — TEGNAPI állapot (shift(1), no-look-ahead)
        rng = (d1["high"] - d1["low"]).rolling(self.datr_period).mean().shift(1)
        self._datr = dict(zip(dts, rng))
        # Napi trend: sign(close - EMA20) — szintén tegnapi
        ema_v = d1["close"].ewm(span=self.trend_ema, adjust=False).mean()
        tr = np.sign(d1["close"] - ema_v).shift(1)
        self._trend = dict(zip(dts, tr))

    def on_tick(self, ts: pd.Timestamp, bid: float, ask: float) -> Optional[Decision]:
        h = ts.hour
        if not (self.asia_end_hour <= h < self.entry_end_hour):
            return None
        if self.entry_hours_allowed is not None and h not in self.entry_hours_allowed:
            return None
        d = ts.date()
        if self._last_trade_date == d:
            return None
        if self._asia_n.get(d, 0) < self.min_asia_bars:
            return None
        hi = self._asia_hi.get(d)
        lo = self._asia_lo.get(d)
        datr = self._datr.get(d)
        if hi is None or lo is None or datr is None:
            return None
        try:
            datr = float(datr)
        except (TypeError, ValueError):
            return None
        if not np.isfinite(datr) or datr <= 0:
            return None
        rng = hi - lo
        if rng <= 0 or rng > self.max_range_datr_mult * datr:
            return None

        buf = self.buffer_datr_frac * datr
        trend = self._trend.get(d)
        try:
            trend = float(trend) if trend is not None else None
        except (TypeError, ValueError):
            trend = None

        direction: Optional[str] = None
        sl_dist: float = 0.0
        if ask >= hi + buf:
            if self.trend_filter_enabled and (trend is None or trend <= 0):
                return Decision(ts=ts, allow_trade=False, reason="counter_trend_up",
                                direction="BUY")
            direction = "BUY"
            sl_dist = ask - lo
        elif bid <= lo - buf:
            if self.trend_filter_enabled and (trend is None or trend >= 0):
                return Decision(ts=ts, allow_trade=False, reason="counter_trend_dn",
                                direction="SELL")
            direction = "SELL"
            sl_dist = hi - bid

        if direction is None or sl_dist <= 0:
            return None

        self._last_trade_date = d
        exit_at = pd.Timestamp(d) + pd.Timedelta(hours=self.session_close_hour)
        tp_dist = float(self.tp_datr_mult * datr) if self.tp_datr_mult else None
        return Decision(
            ts=ts, allow_trade=True, reason="london_breakout",
            direction=direction, score=1.0, size=1.0,
            sl_distance=sl_dist, tp_distance=tp_dist,
            exit_at_ts=exit_at,
            indicators={
                "asia_hi": hi, "asia_lo": lo, "asia_range": rng,
                # `atr` mező a trailing-motorhoz (napi range-proxy — órás holdokhoz
                # ez a helyes vol-referencia, nem az órás ATR). `datr` marad
                # is diagnosztikai célra.
                "atr": datr, "datr": datr,
                "buffer": buf,
                "trend": trend if trend is not None else 0.0,
            },
        )

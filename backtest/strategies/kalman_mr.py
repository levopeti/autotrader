"""
Kalman Adaptive Mean-Reversion (KAMR)

1D Kalman szűrő becsli a "fair price" rejtett állapotot a bid/ask
midpoint zajos megfigyelésekből. Belépés amikor `|obs - fair|` > K_entry × σ.
Kilépés amikor obs visszatér a fair-hez, vagy K_exit × σ-nál is jobban eltér.

State model:
    x_{t+1} = x_t + w_t    (random walk),  w ~ N(0, Q)
    z_t     = x_t + v_t    (observation),  v ~ N(0, R_t)

R_t adaptív: `R_scale × spread²` — magasabb spread = zajosabb megfigyelés.

Fő paraméterek:
    Q               — process noise (adaptálás sebessége; 1e-5 lassabb, 1e-3 gyorsabb)
    R_scale         — spread²-hoz szorzó (default 1.0)
    R_min           — R alsó korlát ($²), tick-tökéletes spread ne osszunk 0-val
    K_entry         — belépési küszöb σ egységben (default 2.5)
    K_exit          — kilépési küszöb (bail-out), default 3.5
    sl_atr_mult     — SL távolság ATR-egységben
    atr_period      — ATR időszak
    warmup_ticks    — Kalman konvergencia-idő (default 500)
    cooldown_min    — SL utáni cooldown percben
    enable_session_filter, session_start, session_end
"""
from __future__ import annotations

from typing import List, Optional

import numpy as np
import pandas as pd

from ..indicators.candle_indicators import atr
from .base import Decision, Strategy, StrategyContext


class KalmanMeanReversion(Strategy):
    name = "kalman_mr"

    def __init__(self, params: dict):
        super().__init__(params)
        p = params
        # LTF-en számoljuk az ATR-t (SL-hez)
        self.ltf: str = p.get("ltf_candle_tf", "5min")

        # Kalman paraméterek
        self.Q: float = float(p.get("Q", 1e-4))
        self.R_scale: float = float(p.get("R_scale", 1.0))
        self.R_min: float = float(p.get("R_min", 0.01))    # min variance ($²)

        # Signal küszöbök (σ egységben)
        self.K_entry: float = float(p.get("K_entry", 2.5))
        self.K_exit: float = float(p.get("K_exit", 3.5))

        # SL — ATR-alapú, mint a range_scalp-nál
        self.atr_period: int = int(p.get("atr_period", 14))
        self.sl_atr_mult: float = float(p.get("sl_atr_mult", 1.5))
        self.min_sl_distance: float = float(p.get("min_sl_distance", 0.1))

        # Warmup + cooldown
        self.warmup_ticks: int = int(p.get("warmup_ticks", 500))
        self.cooldown_min: float = float(p.get("cooldown_min", 15.0))

        # Session filter
        self.enable_session_filter: bool = bool(p.get("enable_session_filter", True))
        self.session_start: int = int(p.get("session_start", 7))
        self.session_end: int = int(p.get("session_end", 21))

        # LTF ATR állapot (candle-array + close_ts array a searchsorted-hez)
        self._atr_ltf_arr: Optional[np.ndarray] = None
        self._ltf_ts_close_arr: Optional[np.ndarray] = None

        # Kalman állapot
        self._x: Optional[float] = None       # fair_price estimate
        self._P: float = 1.0                   # covariance (init)
        self._tick_count: int = 0

        # Signal streak — SL cooldown
        self._last_sl_ts: Optional[pd.Timestamp] = None

    # ── standard API ──

    def required_timeframes(self) -> List[str]:
        return [self.ltf]

    def on_segment_start(self, ctx: StrategyContext) -> None:
        ldf = ctx.candles_mtf[self.ltf].copy()
        c, h, l = ldf["close"], ldf["high"], ldf["low"]
        self._atr_ltf_arr = atr(h, l, c, self.atr_period).to_numpy(dtype=float)
        self._ltf_ts_close_arr = ldf["timestamp"].values + np.timedelta64(
            int(pd.Timedelta(self.ltf).value), "ns"
        )
        # Kalman reset szegmens kezdésre
        self._x = None
        self._P = 1.0
        self._tick_count = 0

    def refresh_candles(self, candles_mtf) -> None:
        """Live runner periodikusan hívja. ATR array frissítés, Kalman state marad."""
        ldf = candles_mtf[self.ltf].copy()
        c, h, l = ldf["close"], ldf["high"], ldf["low"]
        self._atr_ltf_arr = atr(h, l, c, self.atr_period).to_numpy(dtype=float)
        self._ltf_ts_close_arr = ldf["timestamp"].values + np.timedelta64(
            int(pd.Timedelta(self.ltf).value), "ns"
        )

    def on_position_closed(self, position) -> None:
        exit_reason = getattr(position, "exit_reason", None)
        if exit_reason == "SL":
            self._last_sl_ts = getattr(position, "exit_ts", None)

    def _within_hours(self, ts: pd.Timestamp) -> bool:
        if not self.enable_session_filter:
            return True
        h = ts.hour
        return self.session_start <= h < self.session_end

    def _lookup_atr(self, ts: pd.Timestamp) -> Optional[float]:
        if self._atr_ltf_arr is None or self._ltf_ts_close_arr is None:
            return None
        idx = int(np.searchsorted(self._ltf_ts_close_arr, ts.to_datetime64(), side="right") - 1)
        if idx < 0 or idx >= len(self._atr_ltf_arr):
            return None
        v = float(self._atr_ltf_arr[idx])
        return v if np.isfinite(v) and v > 0 else None

    def on_tick(self, ts: pd.Timestamp, bid: float, ask: float) -> Optional[Decision]:
        mid = 0.5 * (bid + ask)
        spread = max(ask - bid, 1e-6)

        # ── Kalman update ──
        # R adaptív: spread²-alapú, alsó korláttal
        R = max(self.R_scale * (spread ** 2), self.R_min)

        if self._x is None:
            # Első tick — inicializálás
            self._x = mid
            self._P = 1.0
            self._tick_count = 1
            return None

        # Predict
        x_pred = self._x
        P_pred = self._P + self.Q

        # Update
        S = P_pred + R                      # innováció-variancia
        K = P_pred / S
        residual = mid - x_pred
        self._x = x_pred + K * residual
        self._P = (1 - K) * P_pred
        self._tick_count += 1

        # ── Signal ──
        if self._tick_count < self.warmup_ticks:
            return None

        # Standardizált eltérés (z-score)
        sigma = np.sqrt(S)
        z_score = residual / sigma if sigma > 0 else 0.0

        # Küszöb-check — csak akkor logolunk decision-t ha z_score > K_entry
        # (különben minden tick decision-t generálna → 5GB+ CSV)
        direction: Optional[str] = None
        if z_score > self.K_entry:
            direction = "SELL"      # ár magasan → várunk vissza-lejjebbet
        elif z_score < -self.K_entry:
            direction = "BUY"       # ár alacsonyan → várunk fel-vissza

        if direction is None:
            return None             # nincs signal, nem loggolunk

        # Reasons collection
        reasons: List[str] = []
        if not self._within_hours(ts):
            reasons.append("outside_hours")

        # K_exit felett túl messze — ne lépj be
        if abs(z_score) > self.K_exit:
            reasons.append("too_extreme")

        # Cooldown SL után
        if self._last_sl_ts is not None:
            elapsed = (ts - self._last_sl_ts).total_seconds() / 60.0
            if elapsed < self.cooldown_min:
                reasons.append("cooldown_after_sl")

        # SL — ATR-alapú
        atr_v = self._lookup_atr(ts)
        if atr_v is None:
            reasons.append("atr_unknown")
            sl_dist = 0.0
        else:
            sl_dist = max(self.sl_atr_mult * atr_v, self.min_sl_distance)

        # TP — fair_price-ig vissza (residual = 0)
        # Ha SELL: TP = fair_price (jelenlegi becslés); tp_dist = mid - fair (pozitív, mert mid > fair)
        # Ha BUY:  tp_dist = fair - mid (pozitív, mert mid < fair)
        tp_dist: Optional[float] = None
        if direction == "SELL":
            tp_dist = max(mid - self._x, 0.0)
        elif direction == "BUY":
            tp_dist = max(self._x - mid, 0.0)

        if tp_dist is not None and tp_dist < self.min_sl_distance:
            reasons.append("tp_too_small")

        allow_trade = direction is not None and not reasons

        return Decision(
            ts=ts,
            allow_trade=allow_trade,
            reason="ok" if allow_trade else (";".join(reasons) if reasons else "no_signal"),
            direction=direction,
            score=abs(z_score) if direction else None,
            size=1.0 if allow_trade else 0.0,
            sl_distance=sl_dist if direction else None,
            tp_distance=tp_dist,
            indicators={
                "mid": mid,
                "spread": spread,
                "fair_price": self._x,
                "z_score": round(z_score, 3),
                "sigma_kalman": round(float(sigma), 4),
                "P_covariance": round(self._P, 6),
                "atr_ltf": atr_v,
                "tick_count": self._tick_count,
            },
        )

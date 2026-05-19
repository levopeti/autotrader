"""
Backtest Engine
===============
Eseményvezérelt, tick-szintű szimulátor.

Workflow minden tick-nél:
  1. Nyitott pozíciók SL / TP / Timeout ellenőrzése  (bid/ask-szal)
  2. Session filter, daily loss limit, max open trades
  3. Tick-szintű szűrők (velocity, spread anomália)
  4. Gyertya-szintű jelzések lekérése (range, support, resistance)
  5. Belépési logika → pozíció nyitás
"""

from __future__ import annotations

import numpy as np
import pandas as pd
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple
from enum import Enum

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.indicators import add_candle_indicators


# ─────────────────────────────────────────────────────────────────────────────
class Side(Enum):
    BUY  = 1
    SELL = -1


@dataclass
class Trade:
    id:           int
    side:         Side
    entry_time:   pd.Timestamp
    entry_price:  float
    sl:           float
    tp:           float
    lot_size:     float
    exit_time:    Optional[pd.Timestamp] = None
    exit_price:   Optional[float]        = None
    exit_reason:  Optional[str]          = None   # TP | SL | TIMEOUT | EOD
    pnl:          float                  = 0.0

    @property
    def is_open(self) -> bool:
        return self.exit_time is None

    @property
    def duration_min(self) -> Optional[float]:
        if self.exit_time and self.entry_time:
            return (self.exit_time - self.entry_time).total_seconds() / 60
        return None


# ─────────────────────────────────────────────────────────────────────────────
@dataclass
class EngineConfig:
    # ── Pozíció méret ─────────────────────────────────────────────────────────
    lot_size:          float = 1.0    # 1 lot XAUUSD = 100 oz

    # ── SL / TP ($-ban kifejezve, arany 1 pip ≈ $0.10) ───────────────────────
    sl_dollars:        float = 5.0
    tp_dollars:        float = 8.0

    # ── Gyertya TF-ek ─────────────────────────────────────────────────────────
    candle_tf:         str   = "5min"
    htf_candle_tf:     str   = "1h"

    # ── Range detekció ────────────────────────────────────────────────────────
    adx_period:        int   = 14
    adx_threshold:     float = 25.0
    bb_period:         int   = 20
    bb_std:            float = 2.0
    bb_squeeze_pct:    float = 0.35
    range_lookback:    int   = 50

    # ── Belépés ───────────────────────────────────────────────────────────────
    entry_buffer:      float = 0.30   # $-os buffer support/resistance-tól

    # ── Tick szűrők ───────────────────────────────────────────────────────────
    tick_vel_window:   int   = 20
    tick_vel_max:      float = 3.0    # max tick velocity belépés előtt
    spread_z_window:   int   = 200
    spread_z_max:      float = 2.5    # max spread z-score

    # ── Kockázatkezelés ───────────────────────────────────────────────────────
    max_open_trades:   int   = 2
    max_daily_loss:    float = 200.0  # $  →  ennél több napi veszteségnél a bot leáll
    max_trade_dur_min: int   = 120    # perc után kényszerzárás

    # ── Session (UTC óra) ─────────────────────────────────────────────────────
    session_start:     int   = 7      # London open
    session_end:       int   = 21     # NY close

    # ── Költségek ─────────────────────────────────────────────────────────────
    slippage:          float = 0.10   # $ belépési csúszás/trade
    commission:        float = 0.0    # $ fix jutalék/trade (opcionális)


# ─────────────────────────────────────────────────────────────────────────────
@dataclass
class BacktestResult:
    trades:       List[Trade]  = field(default_factory=list)
    equity_curve: pd.Series    = field(default_factory=pd.Series)
    daily_pnl:    pd.Series    = field(default_factory=pd.Series)
    metrics:      dict         = field(default_factory=dict)

    def trades_df(self) -> pd.DataFrame:
        if not self.trades:
            return pd.DataFrame()
        return pd.DataFrame([{
            "id":           t.id,
            "side":         t.side.name,
            "entry_time":   t.entry_time,
            "exit_time":    t.exit_time,
            "entry_price":  t.entry_price,
            "exit_price":   t.exit_price,
            "sl":           t.sl,
            "tp":           t.tp,
            "lot_size":     t.lot_size,
            "pnl":          t.pnl,
            "exit_reason":  t.exit_reason,
            "duration_min": t.duration_min,
        } for t in self.trades])


# ─────────────────────────────────────────────────────────────────────────────
class BacktestEngine:

    def __init__(self, config: EngineConfig):
        self.cfg = config
        self._trade_counter = 0

    # ── FŐ FUTTATÁS ───────────────────────────────────────────────────────────

    def run(
        self,
        ticks:   pd.DataFrame,
        candles: Dict[str, pd.DataFrame],
    ) -> BacktestResult:
        cfg    = self.cfg
        result = BacktestResult()

        # 1. Jelzések gyertya-szinten (vektorizált, egyszer fut)
        signals_ltf = self._build_signals(candles[cfg.candle_tf])
        signals_htf = self._build_signals(candles[cfg.htf_candle_tf])

        # Gyertya timestamp-ok rendezett tömbként (gyors kereséshez)
        ltf_ts = signals_ltf.index.values   # numpy datetime64 array
        htf_ts = signals_htf.index.values

        # 2. Tick-szintű oszlopok előre számítása
        ticks = ticks.copy().reset_index(drop=True)
        if "mid" not in ticks.columns:
            ticks["mid"] = (ticks["bid"] + ticks["ask"]) / 2
        if "spread" not in ticks.columns:
            ticks["spread"] = ticks["ask"] - ticks["bid"]

        ticks["tick_vel"] = ticks["mid"].diff().abs().rolling(cfg.tick_vel_window).sum()

        sp_mean = ticks["spread"].rolling(cfg.spread_z_window).mean()
        sp_std  = ticks["spread"].rolling(cfg.spread_z_window).std()
        ticks["spread_z"] = (ticks["spread"] - sp_mean) / (sp_std + 1e-10)

        # 3. Állapotváltozók
        open_trades: List[Trade] = []
        equity  = 100_000.0
        daily_pnl_map: Dict[str, float] = {}
        eq_records = []

        # 4. Fő tick loop
        ts_arr   = ticks["timestamp"].values
        bid_arr  = ticks["bid"].values
        ask_arr  = ticks["ask"].values
        vel_arr  = ticks["tick_vel"].values
        spz_arr  = ticks["spread_z"].values

        for i in range(len(ticks)):
            ts  = pd.Timestamp(ts_arr[i])
            bid = bid_arr[i]
            ask = ask_arr[i]

            # ── A) SL / TP / Timeout ──────────────────────────────────────
            still_open = []
            for tr in open_trades:
                done, pnl, reason = self._check_exit(tr, bid, ask, ts)
                if done:
                    tr.exit_time   = ts
                    tr.pnl         = pnl - cfg.commission
                    tr.exit_reason = reason
                    equity        += tr.pnl
                    day = ts.date().isoformat()
                    daily_pnl_map[day] = daily_pnl_map.get(day, 0.0) + tr.pnl
                    result.trades.append(tr)
                else:
                    still_open.append(tr)
            open_trades = still_open

            # Equity snapshot (minden 200. tick)
            if i % 200 == 0:
                eq_records.append((ts, equity))

            # ── B) Szűrők ─────────────────────────────────────────────────
            if not (cfg.session_start <= ts.hour < cfg.session_end):
                continue

            day = ts.date().isoformat()
            if daily_pnl_map.get(day, 0.0) < -cfg.max_daily_loss:
                continue

            if len(open_trades) >= cfg.max_open_trades:
                continue

            vel = vel_arr[i]
            spz = spz_arr[i]
            if np.isnan(vel) or vel > cfg.tick_vel_max:
                continue
            if np.isnan(spz) or spz > cfg.spread_z_max:
                continue

            # ── C) Gyertya jelzés keresése ────────────────────────────────
            ts64  = ts.to_datetime64()
            idx_l = self._find_candle_idx(ltf_ts, ts64)
            idx_h = self._find_candle_idx(htf_ts, ts64)

            if idx_l < 1 or idx_h < 1:
                continue

            sig_l = signals_ltf.iloc[idx_l]
            sig_h = signals_htf.iloc[idx_h]

            # HTF + LTF mindkettő oldalazzon
            if not (sig_l["is_ranging"] and sig_h["is_ranging"]):
                continue

            support    = sig_l["support"]
            resistance = sig_l["resistance"]
            buf        = cfg.entry_buffer

            existing_sides = {t.side for t in open_trades}

            # ── D) Belépés ────────────────────────────────────────────────
            # BUY: ask <= support + buffer
            if ask <= support + buf and Side.BUY not in existing_sides:
                entry = ask + cfg.slippage
                sl    = entry - cfg.sl_dollars
                tp    = entry + cfg.tp_dollars
                open_trades.append(self._new_trade(Side.BUY, ts, entry, sl, tp))

            # SELL: bid >= resistance - buffer
            elif bid >= resistance - buf and Side.SELL not in existing_sides:
                entry = bid - cfg.slippage
                sl    = entry + cfg.sl_dollars
                tp    = entry - cfg.tp_dollars
                open_trades.append(self._new_trade(Side.SELL, ts, entry, sl, tp))

        # ── 5. Kényszerzárás az adatsor végén ────────────────────────────────
        if len(ticks) > 0:
            last   = ticks.iloc[-1]
            last_ts = pd.Timestamp(last["timestamp"])
            for tr in open_trades:
                close_price = last["ask"] if tr.side == Side.BUY else last["bid"]
                pnl = self._calc_pnl(tr, close_price)
                tr.exit_time   = last_ts
                tr.exit_price  = close_price
                tr.pnl         = pnl - cfg.commission
                tr.exit_reason = "EOD"
                equity        += tr.pnl
                result.trades.append(tr)

        # ── 6. Equity görbe + napi PnL ────────────────────────────────────────
        if eq_records:
            idx_ts, idx_eq = zip(*eq_records)
            result.equity_curve = pd.Series(
                list(idx_eq), index=pd.DatetimeIndex(list(idx_ts)), name="equity"
            )

        if daily_pnl_map:
            result.daily_pnl = pd.Series(daily_pnl_map, name="daily_pnl")
            result.daily_pnl.index = pd.to_datetime(result.daily_pnl.index)

        result.metrics = compute_metrics(result)
        return result

    # ── JELZÉSEK ELŐRE SZÁMÍTÁSA ──────────────────────────────────────────────

    def _build_signals(self, candles: pd.DataFrame) -> pd.DataFrame:
        cfg = self.cfg
        df  = candles.copy()
        df  = add_candle_indicators(
            df,
            adx_period     = cfg.adx_period,
            adx_threshold  = cfg.adx_threshold,
            bb_period      = cfg.bb_period,
            bb_std         = cfg.bb_std,
            squeeze_pct    = cfg.bb_squeeze_pct,
            range_lookback = cfg.range_lookback,
        )
        df  = df.set_index("timestamp")
        return df[["is_ranging", "support", "resistance", "adx", "bb_bw"]]

    # ── TRADE MŰVELETEK ───────────────────────────────────────────────────────

    def _new_trade(self, side, ts, entry, sl, tp) -> Trade:
        self._trade_counter += 1
        return Trade(
            id=self._trade_counter, side=side,
            entry_time=ts, entry_price=entry,
            sl=sl, tp=tp, lot_size=self.cfg.lot_size,
        )

    def _check_exit(
        self, tr: Trade, bid: float, ask: float, ts: pd.Timestamp
    ) -> Tuple[bool, float, str]:
        cfg  = self.cfg
        done, reason, exit_price = False, "", 0.0

        if tr.side == Side.BUY:
            if bid <= tr.sl:
                done, reason, exit_price = True, "SL", tr.sl
            elif ask >= tr.tp:
                done, reason, exit_price = True, "TP", tr.tp
        else:
            if ask >= tr.sl:
                done, reason, exit_price = True, "SL", tr.sl
            elif bid <= tr.tp:
                done, reason, exit_price = True, "TP", tr.tp

        # Timeout
        if not done and tr.entry_time:
            if (ts - tr.entry_time).total_seconds() / 60 > cfg.max_trade_dur_min:
                price = ask if tr.side == Side.BUY else bid
                done, reason, exit_price = True, "TIMEOUT", price

        if done:
            tr.exit_price = exit_price
            return True, self._calc_pnl(tr, exit_price), reason
        return False, 0.0, ""

    def _calc_pnl(self, tr: Trade, exit_price: float) -> float:
        """1 lot XAUUSD: 1 $ elmozdulás = $100 PnL (100 oz * $1/oz)."""
        direction = 1 if tr.side == Side.BUY else -1
        return direction * (exit_price - tr.entry_price) * tr.lot_size * 100

    @staticmethod
    def _find_candle_idx(ts_array: np.ndarray, target: np.datetime64) -> int:
        """Legutóbbi gyertya indexe, ami <= target.  Bináris keresés."""
        idx = np.searchsorted(ts_array, target, side="right") - 1
        return int(idx)


# ─────────────────────────────────────────────────────────────────────────────
def compute_metrics(result: BacktestResult) -> dict:
    """Teljesítmény metrikák számítása a BacktestResult-ból."""
    if not result.trades:
        return {"error": "no_trades", "n_trades": 0}

    df = result.trades_df()
    pnl_series = df["pnl"]
    n     = len(df)
    n_win = (pnl_series > 0).sum()
    n_los = (pnl_series <= 0).sum()
    wr    = n_win / n if n > 0 else 0.0

    gross_profit = pnl_series[pnl_series > 0].sum()
    gross_loss   = pnl_series[pnl_series < 0].sum().clip(upper=-1e-10)
    pf           = gross_profit / abs(gross_loss)

    avg_win  = pnl_series[pnl_series > 0].mean() if n_win  > 0 else 0.0
    avg_loss = pnl_series[pnl_series <= 0].mean() if n_los > 0 else 0.0
    expect   = wr * avg_win + (1 - wr) * avg_loss

    # Sharpe (annualizált napi PnL alapján)
    if not result.daily_pnl.empty and result.daily_pnl.std() > 0:
        sharpe = (result.daily_pnl.mean() / result.daily_pnl.std()) * np.sqrt(252)
    else:
        sharpe = 0.0

    # Sortino
    neg_daily = result.daily_pnl[result.daily_pnl < 0]
    if len(neg_daily) > 1:
        sortino = (result.daily_pnl.mean() / neg_daily.std()) * np.sqrt(252)
    else:
        sortino = 0.0

    # Max Drawdown
    if not result.equity_curve.empty:
        eq  = result.equity_curve
        mdd = ((eq - eq.cummax()) / eq.cummax()).min() * 100
    else:
        mdd = 0.0

    # Calmar
    calmar = (pnl_series.sum() / abs(mdd + 1e-10)) if mdd < 0 else 0.0

    exit_counts = df["exit_reason"].value_counts().to_dict()

    return {
        "total_pnl":        round(float(pnl_series.sum()),  2),
        "n_trades":         int(n),
        "win_rate":         round(float(wr),  4),
        "profit_factor":    round(float(pf),  4),
        "avg_win":          round(float(avg_win),  2),
        "avg_loss":         round(float(avg_loss), 2),
        "expectancy":       round(float(expect),   2),
        "sharpe_ratio":     round(float(sharpe),   4),
        "sortino_ratio":    round(float(sortino),  4),
        "calmar_ratio":     round(float(calmar),   4),
        "max_drawdown_pct": round(float(mdd),      2),
        "n_tp":             int(exit_counts.get("TP",      0)),
        "n_sl":             int(exit_counts.get("SL",      0)),
        "n_timeout":        int(exit_counts.get("TIMEOUT", 0)),
        "n_eod":            int(exit_counts.get("EOD",     0)),
        "avg_duration_min": round(float(df["duration_min"].mean()), 1),
        "gross_profit":     round(float(gross_profit), 2),
        "gross_loss":       round(float(gross_loss),   2),
    }

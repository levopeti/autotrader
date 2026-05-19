"""
Indicators
==========
Gyors, NumPy/Pandas vektorizált technikai indikátorok.
Minden függvény pd.Series bemenet → pd.Series kimenet.
"""

import numpy as np
import pandas as pd
from typing import Tuple


# ── ALAP ─────────────────────────────────────────────────────────────────────

def ema(s: pd.Series, period: int) -> pd.Series:
    return s.ewm(span=period, adjust=False).mean()


def sma(s: pd.Series, period: int) -> pd.Series:
    return s.rolling(period).mean()


def atr(high: pd.Series, low: pd.Series, close: pd.Series, period: int = 14) -> pd.Series:
    prev_close = close.shift(1)
    tr = pd.concat([
        high - low,
        (high - prev_close).abs(),
        (low  - prev_close).abs(),
    ], axis=1).max(axis=1)
    return tr.ewm(span=period, adjust=False).mean()


def rsi(close: pd.Series, period: int = 14) -> pd.Series:
    delta = close.diff()
    gain  = delta.clip(lower=0).ewm(span=period, adjust=False).mean()
    loss  = (-delta.clip(upper=0)).ewm(span=period, adjust=False).mean()
    return 100 - (100 / (1 + gain / (loss + 1e-10)))


# ── BOLLINGER ─────────────────────────────────────────────────────────────────

def bollinger_bands(
    close: pd.Series, period: int = 20, std_dev: float = 2.0
) -> Tuple[pd.Series, pd.Series, pd.Series, pd.Series]:
    """Returns: (upper, mid, lower, bandwidth)  —  bandwidth = (upper-lower)/mid"""
    mid       = sma(close, period)
    std       = close.rolling(period).std()
    upper     = mid + std_dev * std
    lower     = mid - std_dev * std
    bandwidth = (upper - lower) / (mid + 1e-10)
    return upper, mid, lower, bandwidth


def bb_squeeze_percentile(bandwidth: pd.Series, lookback: int = 50) -> pd.Series:
    """
    0 = maximum squeeze (oldalazás)
    1 = maximum expansion (kitörés)
    """
    bb_min = bandwidth.rolling(lookback).min()
    bb_max = bandwidth.rolling(lookback).max()
    return (bandwidth - bb_min) / (bb_max - bb_min + 1e-10)


# ── ADX ───────────────────────────────────────────────────────────────────────

def adx(
    high: pd.Series, low: pd.Series, close: pd.Series, period: int = 14
) -> Tuple[pd.Series, pd.Series, pd.Series]:
    """Returns: (ADX, +DI, -DI)"""
    up   = high.diff()
    down = -low.diff()

    plus_dm  = np.where((up > down) & (up > 0),   up,   0.0)
    minus_dm = np.where((down > up) & (down > 0), down, 0.0)

    tr_val   = atr(high, low, close, period)
    plus_di  = 100 * pd.Series(plus_dm,  index=close.index).ewm(span=period, adjust=False).mean() / (tr_val + 1e-10)
    minus_di = 100 * pd.Series(minus_dm, index=close.index).ewm(span=period, adjust=False).mean() / (tr_val + 1e-10)

    dx      = 100 * (plus_di - minus_di).abs() / (plus_di + minus_di + 1e-10)
    adx_val = dx.ewm(span=period, adjust=False).mean()
    return adx_val, plus_di, minus_di


# ── RANGE SZINTEK ─────────────────────────────────────────────────────────────

def rolling_range(
    high: pd.Series, low: pd.Series, lookback: int = 50
) -> Tuple[pd.Series, pd.Series]:
    """Rolling support (min low) és resistance (max high)."""
    return high.rolling(lookback).max(), low.rolling(lookback).min()


# ── MARKET REGIME ─────────────────────────────────────────────────────────────

def compute_regime(
    close: pd.Series,
    high: pd.Series,
    low: pd.Series,
    adx_period: int = 14,
    adx_threshold: float = 25.0,
    bb_period: int = 20,
    bb_std: float = 2.0,
    squeeze_pct_threshold: float = 0.35,
) -> pd.Series:
    """
    Boolean Series — True ahol oldalaz a piac.
    Feltételek (mind egyszerre kell):
      1. ADX < adx_threshold
      2. BB squeeze percentile < squeeze_pct_threshold
    """
    adx_val, _, _ = adx(high, low, close, adx_period)
    _, _, _, bw   = bollinger_bands(close, bb_period, bb_std)
    squeeze       = bb_squeeze_percentile(bw)
    return (adx_val < adx_threshold) & (squeeze < squeeze_pct_threshold)


# ── TICK-SZINTŰ INDIKÁTOROK ───────────────────────────────────────────────────

def tick_velocity(mid: pd.Series, window: int = 20) -> pd.Series:
    """
    Összesített abszolút árváltozás az utolsó N tickben.
    Magas értéknél NE lépj be (breakout / spike veszély).
    """
    return mid.diff().abs().rolling(window).sum()


def tick_imbalance(bid: pd.Series, ask: pd.Series, window: int = 20) -> pd.Series:
    """
    Order flow imbalance proxy.
    Pozitív = vételi nyomás, negatív = eladási nyomás.
    """
    mid    = (bid + ask) / 2
    change = mid.diff()
    return change.rolling(window).sum()


def spread_zscore(spread: pd.Series, window: int = 200) -> pd.Series:
    """Z-score a spread anomália detektáláshoz. >2.5 = ne lépj be."""
    mean = spread.rolling(window).mean()
    std  = spread.rolling(window).std()
    return (spread - mean) / (std + 1e-10)


# ── SEGÉD: minden gyertyaindikátor egy DataFrame-re ──────────────────────────

def add_candle_indicators(
    df: pd.DataFrame,
    adx_period: int = 14,
    adx_threshold: float = 25.0,
    bb_period: int = 20,
    bb_std: float = 2.0,
    squeeze_pct: float = 0.35,
    range_lookback: int = 50,
) -> pd.DataFrame:
    """
    Helyben hozzáadja az összes indikátort a gyertya DataFrame-hez.
    Elvár: open, high, low, close, volume oszlopok.
    """
    df = df.copy()
    c, h, l = df["close"], df["high"], df["low"]

    df["atr"]      = atr(h, l, c, adx_period)
    df["rsi"]      = rsi(c)
    adx_s, pdi, mdi = adx(h, l, c, adx_period)
    df["adx"]      = adx_s
    df["plus_di"]  = pdi
    df["minus_di"] = mdi

    df["bb_upper"], df["bb_mid"], df["bb_lower"], df["bb_bw"] = bollinger_bands(c, bb_period, bb_std)
    df["bb_squeeze_pct"] = bb_squeeze_percentile(df["bb_bw"])

    df["resistance"], df["support"] = rolling_range(h, l, range_lookback)

    df["is_ranging"] = compute_regime(
        c, h, l, adx_period, adx_threshold, bb_period, bb_std, squeeze_pct
    )
    return df

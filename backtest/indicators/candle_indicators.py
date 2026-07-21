from __future__ import annotations

from typing import Tuple

import numpy as np
import pandas as pd


def ema(s: pd.Series, period: int) -> pd.Series:
    return s.ewm(span=period, adjust=False).mean()


def sma(s: pd.Series, period: int) -> pd.Series:
    return s.rolling(period).mean()


def rsi(close: pd.Series, period: int = 14) -> pd.Series:
    delta = close.diff()
    gain = delta.clip(lower=0).ewm(alpha=1 / period, adjust=False).mean()
    loss = (-delta.clip(upper=0)).ewm(alpha=1 / period, adjust=False).mean()
    rs = gain / loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs))


def atr(high: pd.Series, low: pd.Series, close: pd.Series, period: int = 14) -> pd.Series:
    prev_close = close.shift(1)
    tr = pd.concat([
        high - low,
        (high - prev_close).abs(),
        (low - prev_close).abs(),
    ], axis=1).max(axis=1)
    return tr.ewm(alpha=1 / period, adjust=False).mean()


def bollinger_bands(
    close: pd.Series, period: int = 20, std_dev: float = 2.0
) -> Tuple[pd.Series, pd.Series, pd.Series, pd.Series]:
    mid = sma(close, period)
    std = close.rolling(period).std()
    upper = mid + std_dev * std
    lower = mid - std_dev * std
    bandwidth = (upper - lower) / (mid + 1e-10)
    return upper, mid, lower, bandwidth


def bb_squeeze_percentile(bandwidth: pd.Series, lookback: int = 50) -> pd.Series:
    bb_min = bandwidth.rolling(lookback).min()
    bb_max = bandwidth.rolling(lookback).max()
    return (bandwidth - bb_min) / (bb_max - bb_min + 1e-10)


def adx(
    high: pd.Series, low: pd.Series, close: pd.Series, period: int = 14
) -> Tuple[pd.Series, pd.Series, pd.Series]:
    up = high.diff()
    down = -low.diff()
    plus_dm = np.where((up > down) & (up > 0), up, 0.0)
    minus_dm = np.where((down > up) & (down > 0), down, 0.0)

    tr = atr(high, low, close, period)
    plus_di = 100 * pd.Series(plus_dm, index=close.index).ewm(alpha=1 / period, adjust=False).mean() / (tr + 1e-10)
    minus_di = 100 * pd.Series(minus_dm, index=close.index).ewm(alpha=1 / period, adjust=False).mean() / (tr + 1e-10)

    dx = 100 * (plus_di - minus_di).abs() / (plus_di + minus_di + 1e-10)
    adx_v = dx.ewm(alpha=1 / period, adjust=False).mean()
    return adx_v, plus_di, minus_di


def rolling_range(
    high: pd.Series, low: pd.Series, lookback: int = 50
) -> Tuple[pd.Series, pd.Series]:
    """Visszaad (resistance, support) sorozatot."""
    return high.rolling(lookback).max(), low.rolling(lookback).min()
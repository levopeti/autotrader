from __future__ import annotations

from typing import Dict, Iterable

import pandas as pd

from .gap_detector import Segment


def build_candles(ticks: pd.DataFrame, timeframe: str) -> pd.DataFrame:
    """
    Tick → OHLCV gyertyák a mid áron.
    A timestamp a gyertya NYITÓ ideje (label='left'), így a live decision-szal konzisztens
    (a gyertya akkor "kész", amikor az időablak vége elmúlt).
    """
    if ticks.empty:
        return pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "volume", "n_ticks", "spread_avg"])

    df = ticks.set_index("timestamp_utc")
    mid = df["mid"]

    candles = mid.resample(timeframe, label="left", closed="left").agg(
        open="first", high="max", low="min", close="last"
    )
    candles["volume"] = mid.resample(timeframe, label="left", closed="left").count()
    candles["n_ticks"] = candles["volume"]
    candles["spread_avg"] = df["spread"].resample(timeframe, label="left", closed="left").mean()

    candles = candles.dropna(subset=["open"]).reset_index()
    candles = candles.rename(columns={"timestamp_utc": "timestamp"})
    return candles


def build_mtf_for_segment(
    ticks: pd.DataFrame,
    segment: Segment,
    timeframes: Iterable[str],
) -> Dict[str, pd.DataFrame]:
    seg_ticks = ticks.iloc[segment.start_idx:segment.end_idx]
    return {tf: build_candles(seg_ticks, tf) for tf in timeframes}


def build_global_mtf(
    ticks: pd.DataFrame,
    timeframes: Iterable[str],
) -> Dict[str, pd.DataFrame]:
    """
    A teljes tick stream-ből épít candle-eket. Az indikátor warmup így
    nem reset-elődik szegmensenként.
    """
    return {tf: build_candles(ticks, tf) for tf in timeframes}
from __future__ import annotations

from pathlib import Path
from typing import Iterable

import pandas as pd


REQUIRED_COLUMNS = ("timestamp_utc", "bid", "ask")


def load_ticks(paths: str | Path | Iterable[str | Path]) -> pd.DataFrame:
    if isinstance(paths, (str, Path)):
        paths = [paths]
    paths = [Path(p) for p in paths]

    frames = []
    for p in paths:
        if not p.exists():
            raise FileNotFoundError(p)
        df = pd.read_parquet(p)
        frames.append(_normalize(df, source=str(p)))

    ticks = pd.concat(frames, ignore_index=True)
    ticks = (
        ticks.sort_values("timestamp_utc")
             .drop_duplicates(subset=["timestamp_utc"], keep="last")
             .reset_index(drop=True)
    )
    return ticks


def _normalize(df: pd.DataFrame, source: str) -> pd.DataFrame:
    df = df.copy()
    df.columns = [c.lower().strip() for c in df.columns]

    rename = {}
    for alt in ("timestamp", "time", "datetime", "ts"):
        if alt in df.columns and "timestamp_utc" not in df.columns:
            rename[alt] = "timestamp_utc"
    if rename:
        df = df.rename(columns=rename)

    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"{source}: hiányzó oszlopok {missing}")

    df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], utc=True, errors="coerce")
    df = df.dropna(subset=["timestamp_utc"]).copy()

    df["bid"] = pd.to_numeric(df["bid"], errors="coerce")
    df["ask"] = pd.to_numeric(df["ask"], errors="coerce")
    df = df.dropna(subset=["bid", "ask"])

    if "mid" not in df.columns:
        df["mid"] = (df["bid"] + df["ask"]) / 2.0
    if "spread" not in df.columns:
        df["spread"] = df["ask"] - df["bid"]

    for col in ("instrument", "epic"):
        if col not in df.columns:
            df[col] = None

    return df[["timestamp_utc", "instrument", "epic", "bid", "ask", "mid", "spread"]]


def time_range(ticks: pd.DataFrame) -> tuple[pd.Timestamp, pd.Timestamp]:
    return ticks["timestamp_utc"].min(), ticks["timestamp_utc"].max()
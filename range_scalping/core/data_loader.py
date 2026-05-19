"""
Data Loader
===========
Parquet tick adatok betöltése, normalizálása és gyertyává konvertálása.

Elvárt parquet séma (rugalmas, auto-detektál):
  - timestamp / time / datetime / date  →  datetime64[ns, UTC]
  - bid, ask  VAGY  price / close  (bid=ask lesz)
  - volume  (opcionális)

Tick DataFrame kimenete:
  timestamp (UTC), bid, ask, mid, spread, volume
"""

import pandas as pd
import numpy as np
from pathlib import Path
from typing import Union


# ─────────────────────────────────────────────────────────────────────────────
class TickDataLoader:

    def __init__(self, data_dir: Union[str, Path] = "."):
        self.data_dir = Path(data_dir)

    # ── BETÖLTÉS ──────────────────────────────────────────────────────────────

    def load_ticks(self, pattern: str = "*.parquet") -> pd.DataFrame:
        """Összes parquet fájl betöltése egy DataFrame-be."""
        files = sorted(self.data_dir.glob(pattern))
        if not files:
            raise FileNotFoundError(
                f"Nincs parquet fájl itt: {self.data_dir} (pattern={pattern})"
            )

        dfs = []
        for f in files:
            df = pd.read_parquet(f)
            df.rename(columns={"timestamp_utc": "timestamp"}, inplace=True)
            dfs.append(df)
            print(f"  ✓ {f.name}: {len(df):,} tick")

        ticks = pd.concat(dfs, ignore_index=True)
        ticks = self._normalize_columns(ticks)
        ticks = (
            ticks.sort_values("timestamp")
                 .drop_duplicates(subset=["timestamp"])
                 .reset_index(drop=True)
        )
        ticks["mid"]    = (ticks["bid"] + ticks["ask"]) / 2
        ticks["spread"] = ticks["ask"] - ticks["bid"]

        self._print_summary(ticks)
        return ticks

    def _normalize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df.columns = df.columns.str.lower().str.strip()

        # timestamp
        for col in ["timestamp", "time", "datetime", "date", "ts"]:
            if col in df.columns:
                df = df.rename(columns={col: "timestamp"})
                break
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)

        # bid / ask
        if "bid" not in df.columns:
            for alt in ["price", "close", "last"]:
                if alt in df.columns:
                    df["bid"] = df[alt]
                    df["ask"] = df[alt]
                    break
        if "ask" not in df.columns:
            df["ask"] = df["bid"]

        # volume
        if "volume" not in df.columns:
            df["volume"] = 1.0

        return df[["timestamp", "bid", "ask", "volume"]]

    def _print_summary(self, ticks: pd.DataFrame):
        dur = ticks["timestamp"].max() - ticks["timestamp"].min()
        print(f"\n{'='*56}")
        print(f"  Összesen     : {len(ticks):,} tick")
        print(f"  Időszak      : {ticks['timestamp'].min().date()} "
              f"→ {ticks['timestamp'].max().date()}  ({dur.days} nap)")
        print(f"  Átlag spread : {ticks['spread'].mean():.4f}  "
              f"max: {ticks['spread'].max():.4f}")
        print(f"{'='*56}\n")

    # ── RESAMPLE ──────────────────────────────────────────────────────────────

    def resample_to_candles(
        self, ticks: pd.DataFrame, timeframe: str = "1min"
    ) -> pd.DataFrame:
        """Tick → OHLCV gyertyák.  timeframe: '1min','5min','15min','1h'"""
        df = ticks.set_index("timestamp")
        mid = (df["bid"] + df["ask"]) / 2

        candles = mid.resample(timeframe).agg(
            open="first", high="max", low="min", close="last"
        )
        candles["volume"]     = df["volume"].resample(timeframe).sum()
        candles["tick_count"] = df["bid"].resample(timeframe).count()
        candles["spread_avg"] = df["ask"].sub(df["bid"]).resample(timeframe).mean()
        candles = candles.dropna(subset=["open"]).reset_index()
        candles["timestamp"] = pd.to_datetime(candles["timestamp"])
        return candles

    def build_mtf_data(self, ticks: pd.DataFrame) -> dict:
        """1M / 5M / 15M / 1H egyszerre."""
        print("Gyertyák építése...")
        mtf = {}
        for tf in ["1min", "5min", "15min", "1h"]:
            mtf[tf] = self.resample_to_candles(ticks, tf)
            print(f"  {tf:5s}: {len(mtf[tf]):,} gyertya")
        return mtf


# ── SZINTETIKUS ADAT GENERÁTOR ────────────────────────────────────────────────

def generate_synthetic_ticks(
    n_ticks: int = 300_000,
    start: str = "2025-01-01",
    base_price: float = 2650.0,
    seed: int = 42,
) -> pd.DataFrame:
    """
    Szintetikus XAUUSD tick adatok backtesting teszteléshez.
    Vegyesen tartalmaz ranging + trending periódusokat (70/30 arány).
    """
    rng = np.random.default_rng(seed)
    timestamps = pd.date_range(start, periods=n_ticks, freq="500ms", tz="UTC")

    # Piaci regime-ek generálása
    prices = np.zeros(n_ticks)
    prices[0] = base_price
    regime = np.zeros(n_ticks, dtype=int)

    idx = 0
    while idx < n_ticks:
        r = rng.choice([0, 1], p=[0.70, 0.30])
        length = int(rng.integers(200, 1500))
        end = min(idx + length, n_ticks)
        regime[idx:end] = r
        idx = end

    for i in range(1, n_ticks):
        if regime[i] == 0:          # ranging: mean-reversion
            lookback = prices[max(0, i - 200):i]
            mean = lookback.mean() if len(lookback) > 0 else base_price
            reversion = 0.04 * (mean - prices[i - 1])
            prices[i] = prices[i - 1] + reversion + rng.normal(0, 0.07)
        else:                        # trending: drift + noise
            drift = rng.choice([-1, 1]) * rng.uniform(0.02, 0.08)
            prices[i] = prices[i - 1] + drift + rng.normal(0, 0.12)

    spread = rng.uniform(0.15, 0.45, size=n_ticks)
    ticks = pd.DataFrame({
        "timestamp": timestamps,
        "bid":    prices - spread / 2,
        "ask":    prices + spread / 2,
        "volume": rng.integers(1, 10, size=n_ticks).astype(float),
    })
    ticks["mid"]    = prices
    ticks["spread"] = spread
    return ticks

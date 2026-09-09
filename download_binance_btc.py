#!/usr/bin/env python3
"""
Binance BTCUSDT 1-min klines letöltés — data.binance.vision publikus S3 dumpok.

Havi zip-ek (spot/monthly/klines/BTCUSDT/1m/), 3 év. Nincs API-kulcs, nincs
rate-limit-para. Kimenet:
  - data/binance_btc_1m_3y.parquet (OHLCV)
  - data/tick_pseudo_BTC_3y.parquet (pszeudo-tick a backtest-engine-nek;
    bid/ask a close ± feles spread-becslésből)
"""
from __future__ import annotations
import io
import sys
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests

ROOT = Path(__file__).resolve().parent
OUT_DIR = ROOT / "data"
BASE = "https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/1m"

# Capital BTC spread ~$50 (mérve a tick_data_BTCUSD-ból) — a pszeudo-tickbe
# ezt égetjük bele, hogy a backtest a MI költségeinkkel számoljon.
CAPITAL_SPREAD = 50.0

START = (2023, 10)
END = (2026, 8)   # utolsó teljes hónap

months = []
y, m = START
while (y, m) <= END:
    months.append((y, m))
    m += 1
    if m > 12:
        m = 1; y += 1

print(f"{len(months)} hónap letöltése: {START} → {END}")
frames = []
for y, m in months:
    fn = f"BTCUSDT-1m-{y}-{m:02d}.zip"
    url = f"{BASE}/{fn}"
    try:
        r = requests.get(url, timeout=120)
        if r.status_code != 200:
            print(f"  {fn}: HTTP {r.status_code} — kihagyva", flush=True)
            continue
        with zipfile.ZipFile(io.BytesIO(r.content)) as z:
            csv_name = z.namelist()[0]
            df = pd.read_csv(z.open(csv_name), header=None,
                             usecols=[0, 1, 2, 3, 4, 5],
                             names=["open_time", "open", "high", "low", "close", "volume"])
        # Binance ms vagy us timestamp (2025+ némelyik dump microsec)
        ts = df["open_time"].astype("int64")
        unit = "us" if ts.iloc[0] > 10**14 else "ms"
        df["timestamp"] = pd.to_datetime(ts, unit=unit, utc=True)
        frames.append(df[["timestamp", "open", "high", "low", "close", "volume"]])
        print(f"  {fn}: {len(df):,} bar OK", flush=True)
    except Exception as e:
        print(f"  {fn}: HIBA {e}", flush=True)

if not frames:
    sys.exit("Nincs letöltött adat!")

full = pd.concat(frames, ignore_index=True).sort_values("timestamp").reset_index(drop=True)
full = full.drop_duplicates(subset="timestamp")
out1 = OUT_DIR / "binance_btc_1m_3y.parquet"
full.to_parquet(out1, index=False)
print(f"\nMentve: {out1}  ({len(full):,} bar, {full.timestamp.min()} → {full.timestamp.max()})")

# Pszeudo-tick a backtest-engine-nek (Capital-spread beleégetve)
half = CAPITAL_SPREAD / 2
pseudo = pd.DataFrame({
    "timestamp_utc": full["timestamp"],
    "bid": full["close"] - half,
    "ask": full["close"] + half,
})
out2 = OUT_DIR / "tick_pseudo_BTC_3y.parquet"
pseudo.to_parquet(out2, index=False)
print(f"Mentve: {out2}  (spread={CAPITAL_SPREAD} beleégetve)")

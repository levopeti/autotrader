#!/usr/bin/env python3
"""
Dukascopy USD/JPY 1-min letöltés 3 évre (a download_dukascopy.py mintájára).

Kimenet:
  - data/dukascopy_1min_USD_JPY.parquet (bid/ask/mid)
  - data/tick_pseudo_USDJPY_3y.parquet (pszeudo-tick; a Capital-spread (0.012)
    beleégetve, hogy a backtest a MI költségeinkkel számoljon)
"""
import time
from datetime import datetime, timezone
from pathlib import Path

import dukascopy_python
import pandas as pd
from dukascopy_python.instruments import INSTRUMENT_FX_MAJORS_USD_JPY

ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data"
CAPITAL_SPREAD = 0.012   # Capital USDJPY medián spread (tick-adatból mérve)

START = datetime(2023, 10, 1, tzinfo=timezone.utc)
END = datetime(2026, 9, 1, tzinfo=timezone.utc)


def month_ranges(start, end):
    out = []
    cur = start
    while cur < end:
        if cur.month == 12:
            nxt = cur.replace(year=cur.year + 1, month=1)
        else:
            nxt = cur.replace(month=cur.month + 1)
        out.append((cur, min(nxt, end)))
        cur = nxt
    return out


frames = []
for a, b in month_ranges(START, END):
    for attempt in range(3):
        try:
            df = dukascopy_python.fetch(
                INSTRUMENT_FX_MAJORS_USD_JPY,
                dukascopy_python.INTERVAL_MIN_1,
                dukascopy_python.OFFER_SIDE_BID,
                a, b,
            )
            if df is not None and len(df):
                frames.append(df)
                print(f"  {a.date()}..{b.date()}: {len(df):,} bar", flush=True)
            else:
                print(f"  {a.date()}..{b.date()}: üres", flush=True)
            break
        except Exception as e:
            print(f"  {a.date()}: hiba ({e}) retry {attempt+1}/3", flush=True)
            time.sleep(10)

full = pd.concat(frames).sort_index()
full = full[~full.index.duplicated(keep="first")]
out1 = DATA / "dukascopy_1min_USD_JPY.parquet"
full.to_parquet(out1)
print(f"\nMentve: {out1} ({len(full):,} bar, {full.index.min()} → {full.index.max()})")

# Pszeudo-tick a Capital-spreaddel
mid = full["bidClose"] if "bidClose" in full.columns else full.iloc[:, 0]
# dukascopy_python oszlopok: open/high/low/close (bid-oldal)
close_col = "close" if "close" in full.columns else full.columns[3]
mid = full[close_col].astype(float)
half = CAPITAL_SPREAD / 2
pseudo = pd.DataFrame({
    "timestamp_utc": pd.to_datetime(full.index, utc=True),
    "bid": mid.values - half,
    "ask": mid.values + half,
})
out2 = DATA / "tick_pseudo_USDJPY_3y.parquet"
pseudo.to_parquet(out2, index=False)
print(f"Mentve: {out2} (spread={CAPITAL_SPREAD} beégetve)")

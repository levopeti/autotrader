#!/usr/bin/env python3
"""
Napi GOLD regime detektor 120 nap tick adaton.

Módszer:
  - 1-óra bar-ok mid-price-ből (bid+ask/2)
  - Daily aggregate (close, high, low, return, range/atr)
  - Regime cimke:
      BULL_RALLY:  slope(20h EMA) > 0.5$/h AND daily_return > +0.5%
      BEAR_RALLY:  slope(20h EMA) < -0.5$/h AND daily_return < -0.5%
      HIGH_VOL_CHOP: daily_range > 2× atr AND |slope| < 0.5$/h
      SIDEWAYS:    egyébként

Kimenet: regime_daily.csv (day, close, ret, ema20_slope, range_atr_mult, regime)
"""
import numpy as np
import pandas as pd
from pathlib import Path

ROOT = Path(__file__).resolve().parent

print("Loading GOLD tick data...")
df = pd.read_parquet(ROOT / 'data/tick_data_GOLD.parquet', columns=['timestamp_utc', 'bid', 'ask'])
df['timestamp_utc'] = pd.to_datetime(df['timestamp_utc'], utc=True)
df['mid'] = (df['bid'] + df['ask']) / 2
print(f"  {len(df):,} tick, {df.timestamp_utc.min()} → {df.timestamp_utc.max()}")

# 1-hour bars
h1 = df.set_index('timestamp_utc')['mid'].resample('1h').ohlc().dropna()
h1['range'] = h1['high'] - h1['low']
print(f"  1h bars: {len(h1)}")

# EMA20 (20 hour) + slope
h1['ema20'] = h1['close'].ewm(span=20, adjust=False).mean()
h1['slope'] = h1['ema20'].diff()   # $/h

# ATR proxy (14h)
h1['tr'] = np.maximum(h1['high'] - h1['low'],
                     np.maximum(abs(h1['high'] - h1['close'].shift()),
                               abs(h1['low'] - h1['close'].shift())))
h1['atr14'] = h1['tr'].rolling(14).mean()

# Daily aggregate
d = h1.groupby(h1.index.date).agg(
    close=('close', 'last'), open=('open', 'first'),
    high=('high', 'max'), low=('low', 'min'),
    range_=('range', 'sum'),
    ema20_end=('ema20', 'last'),
    slope_avg=('slope', 'mean'),
    slope_end=('slope', 'last'),
    atr_end=('atr14', 'last'),
)
d.index = pd.to_datetime(d.index)
d['ret_pct'] = (d['close'] / d['open'] - 1) * 100
d['range_atr_mult'] = d['range_'] / (24 * d['atr_end'])   # napi range / (24 * óra ATR)

# Osztályozás
def classify(row):
    slope = row['slope_avg']  # napi átlag óra-slope
    ret = row['ret_pct']
    range_mult = row['range_atr_mult']
    if pd.isna(slope) or pd.isna(ret): return 'unknown'
    # Bull rally: erős + felfelé
    if slope > 0.6 and ret > 0.35:
        return 'BULL_RALLY'
    if slope < -0.6 and ret < -0.35:
        return 'BEAR_RALLY'
    # High-vol chop
    if range_mult > 1.4 and abs(slope) < 0.5:
        return 'HIGH_VOL_CHOP'
    return 'SIDEWAYS'

d['regime'] = d.apply(classify, axis=1)
print(f"\nRegime eloszlás (napok):")
print(d['regime'].value_counts().to_string())

# Napi tabla
print(f"\n=== Napi regime + return ({len(d)} nap) ===")
d_display = d[['close','ret_pct','slope_avg','range_atr_mult','regime']].round(3)
print(d_display.to_string())

# Havi bontás
d['month'] = pd.to_datetime(d.index).to_period('M')
print(f"\n=== Havi regime-eloszlás ===")
mo = d.groupby(['month','regime']).size().unstack(fill_value=0)
mo['total'] = mo.sum(axis=1)
print(mo.to_string())

# Save
out = ROOT / 'regime_daily.csv'
d[['close','open','high','low','ret_pct','slope_avg','range_atr_mult','regime']].to_csv(out)
print(f"\nMentve: {out}")

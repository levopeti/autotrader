#!/usr/bin/env python3
"""
Signal-sweep eredmények elemzése — csatornánkénti + regime-bontásos aggregátumok.

Beolvassa a signal_sweep_summary.csv-t és signal_sweep_trades.csv-t.
regime_daily.csv alapján minden trade entry_ts-hez hozzárendel regime cimkét.

Kimenet stdout:
  - Legjobb config csatornánként (PnL + robusztusság szerint)
  - Regime × config × PnL heatmap-jellegű tábla
  - Havi bontás a legjobb config-hoz
"""
from __future__ import annotations
import numpy as np
import pandas as pd
from pathlib import Path

ROOT = Path(__file__).resolve().parent

summary = pd.read_csv(ROOT / "signal_sweep_summary.csv")
trades = pd.read_csv(ROOT / "signal_sweep_trades.csv")
regime = pd.read_csv(ROOT / "regime_daily.csv", index_col=0, parse_dates=True)
regime.index = pd.to_datetime(regime.index, utc=True)
regime['day'] = regime.index.date
regime_map = dict(zip(regime.day, regime.regime))

trades['entry_ts'] = pd.to_datetime(trades['entry_ts'], errors='coerce')
if trades['entry_ts'].dt.tz is None:
    trades['entry_ts'] = trades['entry_ts'].dt.tz_localize('UTC')
trades['day'] = trades['entry_ts'].dt.date
trades['month'] = trades['entry_ts'].dt.to_period('M')
trades['regime'] = trades['day'].map(regime_map).fillna('unknown')
trades['is_win'] = trades['pnl'] > 0

# ─────────────────────────────────────────────────────────────
# 1. Legjobb configok csatornánként (több szempont szerint)
# ─────────────────────────────────────────────────────────────
print("=" * 100)
print(f"CSATORNA-ELEMZÉS  |  {len(summary)} config futott  |  {len(trades):,} trade")
print("=" * 100)

for ch in sorted(summary.channel.unique()):
    ch_summary = summary[summary.channel == ch].copy()
    ch_trades = trades[trades._channel == ch].copy()
    print(f"\n{'─'*100}\n{ch}  |  {len(ch_summary)} config futott\n{'─'*100}")
    print(f"\nTop-10 PnL szerint:")
    print(ch_summary.nlargest(10, 'pnl')[['name','n_trades','wr','pnl','pf','dd']].to_string(index=False))

# ─────────────────────────────────────────────────────────────
# 2. Regime × config elemzés — csatornánként
# ─────────────────────────────────────────────────────────────
print("\n\n" + "=" * 100)
print("REGIME × CONFIG (PnL, csatornánként)")
print("=" * 100)

for ch in sorted(trades._channel.unique()):
    ch_tr = trades[trades._channel == ch]
    if len(ch_tr) == 0:
        continue
    print(f"\n=== {ch} ===")
    pivot = ch_tr.groupby(['_config', 'regime']).agg(
        n=('pnl', 'count'), pnl=('pnl', 'sum')
    ).unstack(fill_value=0)
    # Csak azok a configok amelyek mindegyik regime-ben pozitívak
    pnl_pivot = pivot['pnl'] if 'pnl' in pivot.columns.get_level_values(0) else pivot
    all_pos = pnl_pivot.apply(lambda row: (row > 0).all(), axis=1)
    robust_configs = pnl_pivot[all_pos]
    if len(robust_configs):
        print(f"\nROBUSZTUS configok (minden regime-ben pozitív) — {len(robust_configs)}:")
        robust_sorted = robust_configs.copy()
        robust_sorted['SUM'] = robust_sorted.sum(axis=1)
        robust_sorted = robust_sorted.sort_values('SUM', ascending=False)
        print(robust_sorted.round(2).head(15).to_string())
    else:
        print(f"\nNINCS mindegyik regime-ben pozitív config.  Top-5 SUM szerint:")
        top = pnl_pivot.copy()
        top['SUM'] = top.sum(axis=1)
        print(top.sort_values('SUM', ascending=False).head(5).round(2).to_string())

# ─────────────────────────────────────────────────────────────
# 3. Havi bontás — a top-1 config csatornánként
# ─────────────────────────────────────────────────────────────
print("\n\n" + "=" * 100)
print("HAVI BONTÁS — top-1 config csatornánként")
print("=" * 100)

for ch in sorted(summary.channel.unique()):
    ch_summary = summary[summary.channel == ch]
    top1 = ch_summary.nlargest(1, 'pnl').iloc[0]
    print(f"\n=== {ch}  |  BEST: {top1['name']}  |  PnL ${top1['pnl']:+.2f}  n={top1['n_trades']}  WR={top1['wr']*100:.0f}%  PF={top1['pf']:.2f}")
    ch_tr = trades[trades._config == top1['name']]
    if len(ch_tr) == 0:
        continue
    monthly = ch_tr.groupby('month').agg(n=('pnl','count'), pnl=('pnl','sum'),
                                          wins=('pnl', lambda s: (s>0).sum())).round(2)
    monthly['wr%'] = (monthly.wins / monthly.n * 100).round(0)
    print(monthly.to_string())

# ─────────────────────────────────────────────────────────────
# 4. Config-attribút elemzés — mi számít
# ─────────────────────────────────────────────────────────────
print("\n\n" + "=" * 100)
print("CONFIG-DIMENZIÓ EFFEKT  (mekkora hatás egy-egy paraméternek)")
print("=" * 100)

for ch in sorted(summary.channel.unique()):
    ch_s = summary[summary.channel == ch]
    if len(ch_s) < 4:
        continue
    print(f"\n=== {ch} ===")
    for dim in ['tp_idx','trend','news','atr']:
        if dim not in ch_s.columns or ch_s[dim].nunique() < 2:
            continue
        by = ch_s.groupby(dim).agg(n_cfg=('pnl','count'), avg_pnl=('pnl','mean'),
                                    med_pnl=('pnl','median'), pos_pct=('pnl', lambda s: (s>0).mean()*100)).round(2)
        print(f"\n  by {dim}:")
        print(by.to_string())

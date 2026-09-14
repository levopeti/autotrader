#!/usr/bin/env python3
"""Aggregálja a bulk futások metrics.json-jait egy összesítő táblába."""
from __future__ import annotations

import json
import re
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent
RUNS = ROOT / "runs"

PAT = re.compile(r"^\d{8}_\d{6}_backtest_(?P<strat>[a-z_]+?)_(?P<epic>[A-Z0-9]+)_bulk$")

rows = []
for d in sorted(RUNS.iterdir()):
    if not d.is_dir():
        continue
    m = PAT.match(d.name)
    if not m:
        continue
    mfile = d / "metrics.json"
    if not mfile.exists():
        continue
    metrics = json.loads(mfile.read_text())
    rows.append({
        "strategy": m.group("strat"),
        "epic":     m.group("epic"),
        "n_trades":       metrics.get("n_trades", 0),
        "win_rate":       metrics.get("win_rate"),
        "total_pnl":      metrics.get("total_pnl"),
        "profit_factor":  metrics.get("profit_factor"),
        "expectancy":     metrics.get("expectancy"),
        "avg_win":        metrics.get("avg_win"),
        "avg_loss":       metrics.get("avg_loss"),
        "sharpe":         metrics.get("sharpe_ratio"),
        "sortino":        metrics.get("sortino_ratio"),
        "max_dd_abs":     metrics.get("max_drawdown_abs"),
        "avg_hold_sec":   metrics.get("avg_hold_sec"),
        "run_dir":        d.name,
    })

# Deduplikálás: ha ugyanaz a (strategy, epic) párosítás többször futott (pl. előző hibás
# kísérlet után új run), a legutolsót tartjuk meg.
df = pd.DataFrame(rows).sort_values(["strategy", "epic", "run_dir"])
df = df.drop_duplicates(subset=["strategy", "epic"], keep="last").reset_index(drop=True)

out_csv = ROOT / "logs_bulk" / "summary_metrics.csv"
df.to_csv(out_csv, index=False)
print(f"\nMentve: {out_csv}\n")

cols = ["strategy", "epic", "n_trades", "win_rate", "total_pnl", "profit_factor",
        "expectancy", "sharpe", "max_dd_abs", "avg_hold_sec"]
print(df[cols].to_string(index=False))

print("\n========== STRATÉGIÁNKÉNT ÖSSZESÍTVE ==========")
agg = (df.groupby("strategy")
         .agg(n_runs=("epic", "count"),
              n_trades=("n_trades", "sum"),
              total_pnl=("total_pnl", "sum"),
              avg_pf=("profit_factor", "mean"),
              n_profitable=("total_pnl", lambda s: int((s > 0).sum())))
         .reset_index())
print(agg.to_string(index=False))

#!/usr/bin/env python3
"""
Per-stratégia élő kiértékelés az AutoTrader fiók algo-stratégiáira.

Minden live run-mappa (runs/*_live_<strategy>_GOLD_live/) saját trades.csv-t ír,
strategy-oszloppal és broker-tranzakciós PnL-lel (pnl_source=transaction) —
a stratégiák így tisztán szétválaszthatók.

Használat:
  python algo_eval.py               # összes algo-stratégia, minden live run
  python algo_eval.py --days 7      # csak az utolsó N nap trade-jei
"""
from __future__ import annotations
import argparse
import glob
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent
# (stratégia, epic) párok — a london_breakout két instrumentumon fut
STRATEGY_RUNS = [("donchian_breakout", "GOLD"), ("london_breakout", "GOLD"),
                 ("london_breakout", "USDJPY")]


def load_all_trades(strategy: str, epic: str = "GOLD") -> pd.DataFrame:
    dfs = []
    for tp in glob.glob(str(ROOT / f"runs/*_live_{strategy}_{epic}_live/trades.csv")):
        try:
            df = pd.read_csv(tp)
            if len(df):
                df["_run"] = Path(tp).parent.name
                dfs.append(df)
        except Exception:
            pass
    if not dfs:
        return pd.DataFrame()
    out = pd.concat(dfs, ignore_index=True)
    out["entry_ts"] = pd.to_datetime(out["entry_ts"], errors="coerce", utc=True)
    return out.sort_values("entry_ts").reset_index(drop=True)


def report(df: pd.DataFrame, name: str, days: int | None) -> float:
    print(f"\n{'='*70}\n  {name}\n{'='*70}")
    if len(df) == 0:
        print("  (még nincs zárt trade)")
        return 0.0
    if days:
        cutoff = pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=days)
        df = df[df.entry_ts >= cutoff]
        if len(df) == 0:
            print(f"  (nincs trade az utolsó {days} napban)")
            return 0.0
    wins = df[df.pnl > 0]
    pf = wins.pnl.sum() / max(1e-9, -df[df.pnl < 0].pnl.sum())
    tx_pct = (df.get("pnl_source") == "transaction").mean() * 100 if "pnl_source" in df else float("nan")
    print(f"  Trade: {len(df)} ({len(wins)}W/{len(df)-len(wins)}L, WR {len(wins)/len(df)*100:.0f}%)")
    print(f"  PnL: {df.pnl.sum():+.2f}  |  PF: {pf:.2f}  |  "
          f"Best: {df.pnl.max():+.2f}  Worst: {df.pnl.min():+.2f}")
    print(f"  PnL-forrás broker-tx: {tx_pct:.0f}%")
    if "exit_reason" in df:
        print(f"  Exit-ek: {df.exit_reason.value_counts().to_dict()}")
    df2 = df.copy()
    df2["week"] = df2.entry_ts.dt.strftime("%G-W%V")
    wk = df2.groupby("week").agg(n=("pnl", "count"), pnl=("pnl", "sum")).round(2)
    print(f"  Heti bontás:\n{wk.to_string()}")
    return float(df.pnl.sum())


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--days", type=int, default=None)
    args = p.parse_args()
    total = 0.0
    for s, epic in STRATEGY_RUNS:
        total += report(load_all_trades(s, epic), f"{s} [{epic}]", args.days)
    print(f"\n{'='*70}\n  ALGO PORTFOLIO ÖSSZESEN: {total:+.2f}\n{'='*70}")


if __name__ == "__main__":
    main()

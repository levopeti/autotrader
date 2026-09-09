#!/usr/bin/env python3
"""
Algoritmus-jelölt sweep a teljes 120d GOLD tick adaton.

Jelöltek a régi gold_master + WF eredmények alapján (1h TF, hosszú hold
prioritás — az élő execution-lag tapasztalat miatt), mindegyik 2 slippage
szinttel:
  - normál (0.10) — a backtest-konvenció
  - pesszimista (0.30) — az élő execution-gap proxy-ja

Kimenet: algo_sweep_summary.csv + algo_sweep_trades.csv
"""
from __future__ import annotations
import copy
import json
import subprocess
import yaml
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent
OUT_DIR = ROOT / "backtest/configs/_algo_sweep"
OUT_DIR.mkdir(exist_ok=True)

# (név, forrás-config, strategy_params override-ok)
CANDIDATES = [
    ("vcb_1h",        "configs_gold_master/vcb_gold_vcb_1h.yaml", {}),
    ("vcb_1h_wide",   "configs_gold_master/vcb_gold_vcb_1h.yaml",
     {"sl_atr_mult": 2.5, "tp_atr_mult": 5.0}),
    ("donch_1h_p20",  "configs_gold_master/donchian_gold_donch_1h_p20.yaml", {}),
    ("donch_1h_p40",  "configs_gold_master/donchian_gold_donch_1h_p20.yaml",
     {"donchian_period": 40}),
    ("tr_ref",        "configs_gold_master/trend_reversal_tr_ref.yaml", {}),
    ("pull_htf_1d",   "configs_gold_master/pullback_gold_pull_htf_1d.yaml", {}),
    ("kalman_mr",     "backtest/configs/kalman_mr_gold.yaml", {}),
]

SLIPPAGES = [0.10, 0.30]


def gen_configs():
    configs = []
    for name, src, overrides in CANDIDATES:
        base = yaml.safe_load((ROOT / src).read_text())
        for slip in SLIPPAGES:
            cfg = copy.deepcopy(base)
            cfg["engine"]["slippage"] = slip
            for k, v in overrides.items():
                cfg["strategy_params"][k] = v
            tag = f"{name}_slip{int(slip*100):02d}"
            p = OUT_DIR / f"{tag}.yaml"
            p.write_text(yaml.safe_dump(cfg))
            configs.append((tag, name, slip, p))
    return configs


def run_one(cfg_path: Path, tag: str):
    result = subprocess.run(
        ["venv/bin/python", "-m", "backtest.run_backtest",
         "--config", str(cfg_path), "--suffix", f"algo_{tag}"],
        cwd=ROOT, capture_output=True, text=True, timeout=900,
    )
    if result.returncode != 0:
        print(f"  [FAIL] {tag}: {result.stderr[-300:]}")
        return None
    for line in result.stdout.splitlines():
        if line.startswith("Run-mappa:"):
            run_dir = ROOT / line.split(":", 1)[1].strip()
            mp = run_dir / "metrics.json"
            tp = run_dir / "trades.csv"
            if mp.exists():
                m = json.loads(mp.read_text())
                m["_trades_csv"] = str(tp) if tp.exists() else None
                return m
    return None


def main():
    configs = gen_configs()
    print(f"=== Algo sweep: {len(configs)} futás ===\n")
    rows, all_trades = [], []
    for i, (tag, name, slip, cfg_path) in enumerate(configs, 1):
        print(f"[{i}/{len(configs)}] {tag}", end=" ... ", flush=True)
        m = run_one(cfg_path, tag)
        if m is None:
            print("SKIP"); continue
        row = {
            "tag": tag, "strategy": name, "slippage": slip,
            "n_trades": m.get("n_trades", 0), "wr": m.get("win_rate", 0),
            "pnl": m.get("total_pnl", 0), "pf": m.get("profit_factor", 0),
            "dd": m.get("max_drawdown_abs", 0),
            "avg_hold_h": (m.get("avg_hold_sec") or 0) / 3600,
        }
        rows.append(row)
        print(f"n={row['n_trades']:>4} WR={row['wr']*100:2.0f}% "
              f"PnL=${row['pnl']:+8.2f} PF={row['pf']:.2f} hold={row['avg_hold_h']:.1f}h")
        if m.get("_trades_csv") and Path(m["_trades_csv"]).exists():
            tr = pd.read_csv(m["_trades_csv"])
            tr["_tag"] = tag; tr["_strategy"] = name; tr["_slippage"] = slip
            all_trades.append(tr)

    df = pd.DataFrame(rows).sort_values("pnl", ascending=False)
    df.to_csv(ROOT / "algo_sweep_summary.csv", index=False)
    print("\n=== SUMMARY (PnL szerint) ===")
    print(df.to_string(index=False))
    if all_trades:
        big = pd.concat(all_trades, ignore_index=True)
        big.to_csv(ROOT / "algo_sweep_trades.csv", index=False)
        print(f"\nMentve: algo_sweep_trades.csv ({len(big):,} sor)")
    print("Mentve: algo_sweep_summary.csv")


if __name__ == "__main__":
    main()

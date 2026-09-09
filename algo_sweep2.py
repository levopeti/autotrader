#!/usr/bin/env python3
"""
Algo sweep 2. kör: TÖBB NAPOS hold variánsok (trendkövető logika).

Kulcs-változtatások az 1. körhöz képest:
  - max_hold_seconds: 4h → 3-5 nap (a trail-stop zár, nem az óra)
  - tp_atr_mult: nagyon távoli (a trend fusson, a trailing ATR véd)
  - trailing atr_trail 2.0-3.0

Megjegyzés: a tick-adat hétvégi gap-jei szegmenshatárok → a pozíció
SEGMENT_END-en kényszer-zár péntekenként. Ez konzervatív (élőben át lehetne
tartani), de realista stressz.

Kimenet: algo_sweep2_summary.csv + algo_sweep2_trades.csv
"""
from __future__ import annotations
import copy
import json
import subprocess
import yaml
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent
OUT_DIR = ROOT / "backtest/configs/_algo_sweep2"
OUT_DIR.mkdir(exist_ok=True)

HOLD_3D = 3 * 24 * 3600
HOLD_5D = 5 * 24 * 3600

# (név, forrás-config, engine-override, strategy_params-override)
CANDIDATES = [
    ("donch_1h_p20_h3d", "configs_gold_master/donchian_gold_donch_1h_p20.yaml",
     {"max_hold_seconds": HOLD_3D, "trail_atr_mult": 2.0},
     {"tp_atr_mult": 20.0}),
    ("donch_1h_p40_h3d", "configs_gold_master/donchian_gold_donch_1h_p20.yaml",
     {"max_hold_seconds": HOLD_3D, "trail_atr_mult": 2.5},
     {"donchian_period": 40, "tp_atr_mult": 20.0}),
    ("donch_4h_p20_h5d", "configs_gold_master/donchian_gold_donch_1h_p20.yaml",
     {"candle_tf": "4h", "max_hold_seconds": HOLD_5D, "trail_atr_mult": 2.5,
      "max_gap_factor": 3.0},
     {"candle_tf": "4h", "tp_atr_mult": 20.0, "sl_atr_mult": 2.5}),
    ("vcb_1h_h3d", "configs_gold_master/vcb_gold_vcb_1h.yaml",
     {"max_hold_seconds": HOLD_3D, "trail_atr_mult": 2.0},
     {"tp_atr_mult": 12.0}),
    ("pull_1d_h3d", "configs_gold_master/pullback_gold_pull_htf_1d.yaml",
     {"max_hold_seconds": HOLD_3D, "trail_atr_mult": 2.0},
     {"tp_atr_mult": 12.0}),
]

SLIPPAGES = [0.10, 0.30]


def main():
    configs = []
    for name, src, eng_ov, sp_ov in CANDIDATES:
        base = yaml.safe_load((ROOT / src).read_text())
        for slip in SLIPPAGES:
            cfg = copy.deepcopy(base)
            cfg["engine"]["slippage"] = slip
            for k, v in eng_ov.items():
                cfg["engine"][k] = v
            for k, v in sp_ov.items():
                cfg["strategy_params"][k] = v
            tag = f"{name}_slip{int(slip*100):02d}"
            p = OUT_DIR / f"{tag}.yaml"
            p.write_text(yaml.safe_dump(cfg))
            configs.append((tag, name, slip, p))

    print(f"=== Algo sweep 2 (multi-day hold): {len(configs)} futás ===\n")
    rows, all_trades = [], []
    for i, (tag, name, slip, cfg_path) in enumerate(configs, 1):
        print(f"[{i}/{len(configs)}] {tag}", end=" ... ", flush=True)
        result = subprocess.run(
            ["venv/bin/python", "-m", "backtest.run_backtest",
             "--config", str(cfg_path), "--suffix", f"algo2_{tag}"],
            cwd=ROOT, capture_output=True, text=True, timeout=1200,
        )
        if result.returncode != 0:
            print(f"FAIL: {result.stderr[-200:]}")
            continue
        m = None
        for line in result.stdout.splitlines():
            if line.startswith("Run-mappa:"):
                run_dir = ROOT / line.split(":", 1)[1].strip()
                mp = run_dir / "metrics.json"
                if mp.exists():
                    m = json.loads(mp.read_text())
                    m["_trades_csv"] = str(run_dir / "trades.csv")
        if m is None:
            print("no metrics"); continue
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
        tp = Path(m["_trades_csv"])
        if tp.exists():
            tr = pd.read_csv(tp)
            tr["_tag"] = tag; tr["_strategy"] = name; tr["_slippage"] = slip
            all_trades.append(tr)

    df = pd.DataFrame(rows).sort_values("pnl", ascending=False)
    df.to_csv(ROOT / "algo_sweep2_summary.csv", index=False)
    print("\n=== SUMMARY (PnL szerint) ===")
    print(df.to_string(index=False))
    if all_trades:
        pd.concat(all_trades, ignore_index=True).to_csv(ROOT / "algo_sweep2_trades.csv", index=False)
    print("Mentve: algo_sweep2_summary.csv + algo_sweep2_trades.csv")


if __name__ == "__main__":
    main()

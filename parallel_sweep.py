#!/usr/bin/env python3
"""
Párhuzamos algo-sweep: batch1 (baseline, 4h hold) + batch2 (multi-day hold)
összes configja egy poolban, MAX_WORKERS párhuzamos run_backtest-tel.

Kimenet: algo_all_summary.csv + algo_all_trades.csv
"""
from __future__ import annotations
import copy
import json
import subprocess
import yaml
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent
OUT_DIR = ROOT / "backtest/configs/_algo_par"
OUT_DIR.mkdir(exist_ok=True)

MAX_WORKERS = 5
HOLD_3D = 3 * 24 * 3600
HOLD_5D = 5 * 24 * 3600
SLIPPAGES = [0.10, 0.30]

# (név, forrás, engine-override, strategy_params-override)
CANDIDATES = [
    # ── batch1: baseline (4h hold, ahogy a régi configok)
    ("vcb_1h",       "configs_gold_master/vcb_gold_vcb_1h.yaml", {}, {}),
    ("vcb_1h_wide",  "configs_gold_master/vcb_gold_vcb_1h.yaml", {},
     {"sl_atr_mult": 2.5, "tp_atr_mult": 5.0}),
    ("donch_1h_p20", "configs_gold_master/donchian_gold_donch_1h_p20.yaml", {}, {}),
    ("donch_1h_p40", "configs_gold_master/donchian_gold_donch_1h_p20.yaml", {},
     {"donchian_period": 40}),
    ("tr_ref",       "configs_gold_master/trend_reversal_tr_ref.yaml", {}, {}),
    ("pull_htf_1d",  "configs_gold_master/pullback_gold_pull_htf_1d.yaml", {}, {}),
    ("kalman_mr",    "backtest/configs/kalman_mr_gold.yaml", {}, {}),
    # ── batch2: multi-day hold, trail-exit
    ("donch_1h_p20_h3d", "configs_gold_master/donchian_gold_donch_1h_p20.yaml",
     {"max_hold_seconds": HOLD_3D, "trail_atr_mult": 2.0}, {"tp_atr_mult": 20.0}),
    ("donch_1h_p40_h3d", "configs_gold_master/donchian_gold_donch_1h_p20.yaml",
     {"max_hold_seconds": HOLD_3D, "trail_atr_mult": 2.5},
     {"donchian_period": 40, "tp_atr_mult": 20.0}),
    ("donch_4h_p20_h5d", "configs_gold_master/donchian_gold_donch_1h_p20.yaml",
     {"candle_tf": "4h", "max_hold_seconds": HOLD_5D, "trail_atr_mult": 2.5,
      "max_gap_factor": 3.0},
     {"candle_tf": "4h", "tp_atr_mult": 20.0, "sl_atr_mult": 2.5}),
    ("vcb_1h_h3d",   "configs_gold_master/vcb_gold_vcb_1h.yaml",
     {"max_hold_seconds": HOLD_3D, "trail_atr_mult": 2.0}, {"tp_atr_mult": 12.0}),
    ("pull_1d_h3d",  "configs_gold_master/pullback_gold_pull_htf_1d.yaml",
     {"max_hold_seconds": HOLD_3D, "trail_atr_mult": 2.0}, {"tp_atr_mult": 12.0}),
]


def gen_all():
    jobs = []
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
            jobs.append((tag, name, slip, p))
    return jobs


def run_one(job):
    tag, name, slip, cfg_path = job
    try:
        result = subprocess.run(
            ["venv/bin/python", "-m", "backtest.run_backtest",
             "--config", str(cfg_path), "--suffix", f"algopar_{tag}"],
            cwd=ROOT, capture_output=True, text=True, timeout=1800,
        )
    except subprocess.TimeoutExpired:
        return (tag, name, slip, None, "TIMEOUT")
    if result.returncode != 0:
        return (tag, name, slip, None, result.stderr[-200:])
    for line in result.stdout.splitlines():
        if line.startswith("Run-mappa:"):
            run_dir = ROOT / line.split(":", 1)[1].strip()
            mp = run_dir / "metrics.json"
            if mp.exists():
                m = json.loads(mp.read_text())
                m["_trades_csv"] = str(run_dir / "trades.csv")
                return (tag, name, slip, m, None)
    return (tag, name, slip, None, "no metrics")


def main():
    jobs = gen_all()
    print(f"=== Párhuzamos sweep: {len(jobs)} futás, {MAX_WORKERS} worker ===\n", flush=True)
    rows, all_trades = [], []
    done = 0
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futs = {ex.submit(run_one, j): j for j in jobs}
        for fut in as_completed(futs):
            tag, name, slip, m, err = fut.result()
            done += 1
            if m is None:
                print(f"[{done}/{len(jobs)}] {tag} FAIL: {err}", flush=True)
                continue
            row = {
                "tag": tag, "strategy": name, "slippage": slip,
                "n_trades": m.get("n_trades", 0), "wr": m.get("win_rate", 0),
                "pnl": m.get("total_pnl", 0), "pf": m.get("profit_factor", 0),
                "dd": m.get("max_drawdown_abs", 0),
                "avg_hold_h": (m.get("avg_hold_sec") or 0) / 3600,
            }
            rows.append(row)
            print(f"[{done}/{len(jobs)}] {tag}: n={row['n_trades']:>4} "
                  f"WR={row['wr']*100:2.0f}% PnL=${row['pnl']:+8.2f} "
                  f"PF={row['pf']:.2f} hold={row['avg_hold_h']:.1f}h", flush=True)
            tp = Path(m["_trades_csv"])
            if tp.exists():
                tr = pd.read_csv(tp)
                tr["_tag"] = tag; tr["_strategy"] = name; tr["_slippage"] = slip
                all_trades.append(tr)

    df = pd.DataFrame(rows).sort_values("pnl", ascending=False)
    df.to_csv(ROOT / "algo_all_summary.csv", index=False)
    print("\n=== SUMMARY (PnL szerint) ===")
    print(df.to_string(index=False))
    if all_trades:
        pd.concat(all_trades, ignore_index=True).to_csv(ROOT / "algo_all_trades.csv", index=False)
    print("KÉSZ: algo_all_summary.csv + algo_all_trades.csv")


if __name__ == "__main__":
    main()

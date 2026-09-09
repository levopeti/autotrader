#!/usr/bin/env python3
"""
Signal-replay sweep: 3 csatorna × több config kombináció.

Config generálás + subprocess futtatás + eredmény-aggregát.
Kimenet:
  - signal_sweep_summary.csv (config → PnL/WR/PF/n_trades)
  - signal_sweep_trades.csv (trade-per-sor, config-cimkével)
"""
from __future__ import annotations
import json
import subprocess
import sys
import yaml
from pathlib import Path
from itertools import product

import pandas as pd

ROOT = Path(__file__).resolve().parent
CONFIGS_DIR = ROOT / "backtest/configs/_sweep"
CONFIGS_DIR.mkdir(exist_ok=True)

CHANNELS = {
    "ANN":     1086101437,
    "VIP":     2001216034,
    "Traderz": 3496306840,
}

BASE_ENGINE = {
    "candle_tf": "5min", "max_gap_factor": 2.0, "min_segment_duration": "1h",
    "max_open_positions": 1, "allow_multiple_directions": False,
    "max_hold_seconds": 7200, "slippage": 0.05, "commission_per_trade": 0.0,
    "trailing_mode": "none",
    "sizing_mode": "fixed_lot", "fixed_lot_size": 1.0,
    "min_order_size": 0.5, "max_order_size": 3.0, "equity": 10000.0,
}


def gen_configs():
    configs = []

    # ── ANN ── 1-TP-s csatorna, kis paraméter tér
    for tp_idx, trend, news, atr in product(
        [0],
        [None, "1h", "4h"],
        [None, [[8, 16]]],
        [False, True],
    ):
        name = f"ANN_tp{tp_idx}_tr{trend or 'off'}_news{'on' if news else 'off'}_atr{'on' if atr else 'off'}"
        params = {
            "signals_dir": "./data/signals", "candle_tf": "1min",
            "channels": [CHANNELS["ANN"]],
            "signal_timeout_minutes": 15.0, "entry_zone_expand": 1.0,
            "tp_strategy": "first", "tp_idx": tp_idx,
        }
        if trend:
            params["trend_filter_enabled"] = True
            params["trend_filter_tf"] = trend
            params["trend_filter_ema_fast"] = 9
            params["trend_filter_ema_slow"] = 21
        if news:
            params["news_blocked_hours"] = news
        if atr:
            params["use_atr_levels"] = True
            params["sl_atr_mult"] = 2.0
            params["tp_atr_mult"] = 3.0
            params["compute_atr"] = True
        configs.append((name, params))

    # ── VIP + Traderz ── több TP, több filter-variáció
    for ch_name, ch_id in [("VIP", CHANNELS["VIP"]), ("Traderz", CHANNELS["Traderz"])]:
        for tp_idx, trend, news in product(
            [0, 1, 2, 3],
            [None, "1h", "4h"],
            [None, [[8, 16]]],
        ):
            name = f"{ch_name}_tp{tp_idx}_tr{trend or 'off'}_news{'on' if news else 'off'}"
            params = {
                "signals_dir": "./data/signals", "candle_tf": "1min",
                "channels": [ch_id],
                "signal_timeout_minutes": 15.0, "entry_zone_expand": 1.0,
                "tp_strategy": "first", "tp_idx": tp_idx,
            }
            if trend:
                params["trend_filter_enabled"] = True
                params["trend_filter_tf"] = trend
                params["trend_filter_ema_fast"] = 9
                params["trend_filter_ema_slow"] = 21
            if news:
                params["news_blocked_hours"] = news
            configs.append((name, params))
    return configs


def write_config(name: str, params: dict) -> Path:
    cfg = {
        "strategy": "signal_replay",
        "data": {"tick_parquet": "./data/tick_data_GOLD.parquet", "epic": "GOLD"},
        "engine": BASE_ENGINE.copy(),
        "strategy_params": params,
    }
    p = CONFIGS_DIR / f"{name}.yaml"
    p.write_text(yaml.safe_dump(cfg))
    return p


def run_backtest(cfg_path: Path, name: str) -> dict:
    result = subprocess.run(
        ["venv/bin/python", "-m", "backtest.run_backtest",
         "--config", str(cfg_path), "--suffix", name],
        cwd=ROOT, capture_output=True, text=True, timeout=600,
    )
    if result.returncode != 0:
        print(f"  [FAIL] {name}: {result.stderr[-200:]}")
        return None
    for line in result.stdout.splitlines():
        if line.startswith("Run-mappa:"):
            run_dir = ROOT / line.split(":", 1)[1].strip()
            metrics_p = run_dir / "metrics.json"
            trades_p = run_dir / "trades.csv"
            if metrics_p.exists():
                m = json.loads(metrics_p.read_text())
                m['_run_dir'] = str(run_dir)
                m['_trades_csv'] = str(trades_p) if trades_p.exists() else None
                return m
    return None


def main():
    configs = gen_configs()
    print(f"=== Sweep terv: {len(configs)} futás ===\n")

    rows = []
    all_trades = []
    for i, (name, params) in enumerate(configs, 1):
        print(f"[{i}/{len(configs)}] {name}", end=" ... ", flush=True)
        cfg_path = write_config(name, params)
        m = run_backtest(cfg_path, name)
        if m is None:
            print("SKIP")
            continue
        row = {
            "name": name,
            "channel": name.split("_")[0],
            "tp_idx": params["tp_idx"],
            "trend": params.get("trend_filter_tf", "off") if params.get("trend_filter_enabled") else "off",
            "news": "on" if params.get("news_blocked_hours") else "off",
            "atr": "on" if params.get("use_atr_levels") else "off",
            "n_trades": m.get("n_trades", 0),
            "wr": m.get("win_rate", 0),
            "pnl": m.get("total_pnl", 0),
            "pf": m.get("profit_factor", 0),
            "dd": m.get("max_drawdown_abs", 0),
        }
        rows.append(row)
        print(f"n={row['n_trades']:>4} WR={row['wr']*100:2.0f}% PnL=${row['pnl']:+7.2f} PF={row['pf']:.2f}")

        if m.get("_trades_csv") and Path(m["_trades_csv"]).exists():
            tr = pd.read_csv(m["_trades_csv"])
            tr["_config"] = name
            tr["_channel"] = row["channel"]
            tr["_tp_idx"] = row["tp_idx"]
            tr["_trend"] = row["trend"]
            tr["_news"] = row["news"]
            tr["_atr"] = row["atr"]
            all_trades.append(tr)

    df = pd.DataFrame(rows).sort_values(["channel", "pnl"], ascending=[True, False])
    df.to_csv(ROOT / "signal_sweep_summary.csv", index=False)
    print(f"\n=== SUMMARY top-5 per csatorna (PnL szerint) ===")
    for ch in sorted(df.channel.unique()):
        print(f"\n{ch}:")
        top = df[df.channel == ch].head(5)
        print(top[["name","n_trades","wr","pnl","pf","dd"]].to_string(index=False))

    if all_trades:
        big = pd.concat(all_trades, ignore_index=True)
        big.to_csv(ROOT / "signal_sweep_trades.csv", index=False)
        print(f"\nMentve: signal_sweep_trades.csv ({len(big):,} sor)")

    print(f"Mentve: signal_sweep_summary.csv ({len(df)} config)")


if __name__ == "__main__":
    main()

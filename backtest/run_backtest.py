#!/usr/bin/env python3
"""
Backtest futtató CLI.

Példa:
  python -m backtest.run_backtest --config backtest/configs/trend_reversal.yaml
  python -m backtest.run_backtest --config backtest/configs/range_scalp.yaml --epic GOLD

Per-futás minden output a runs/<timestamp>_backtest_<strategy>_<epic>/ mappába kerül.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backtest.data.data_summary import compute_data_summary, print_data_summary, save_data_summary
from backtest.data.tick_store import load_ticks, time_range
from backtest.engine.runner import BacktestRunner, EngineConfig, apply_tp_layers_preset
from backtest.runlog.run_logger import RunLogger, make_run_dir
from backtest.strategies.registry import build_strategy


def load_config(path: Path) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def build_engine_cfg(cfg: dict, epic_override: str | None) -> EngineConfig:
    e = cfg["engine"]
    apply_tp_layers_preset(e)
    epic = epic_override or cfg["data"]["epic"]
    return EngineConfig(
        epic=epic,
        candle_tf=e["candle_tf"],
        max_gap_factor=e.get("max_gap_factor", 2.0),
        min_segment_duration=e.get("min_segment_duration", "1h"),
        max_open_positions=e.get("max_open_positions", 1),
        allow_multiple_directions=e.get("allow_multiple_directions", False),
        max_hold_seconds=e.get("max_hold_seconds"),
        slippage=e.get("slippage", 0.0),
        commission_per_trade=e.get("commission_per_trade", 0.0),
        trailing_mode=e.get("trailing_mode", "none"),
        trail_atr_mult=e.get("trail_atr_mult", 1.5),
        break_even_trigger_atr_mult=e.get("break_even_trigger_atr_mult", 1.0),
        sizing_mode=e.get("sizing_mode", "from_strategy"),
        fixed_lot_size=e.get("fixed_lot_size", 1.0),
        min_order_size=e.get("min_order_size", 0.01),
        max_order_size=e.get("max_order_size", 10.0),
        risk_pct=e.get("risk_pct", 0.01),
        equity=e.get("equity", 10000.0),
        tp_layers=e.get("tp_layers"),
        tp_layer_size_pcts=e.get("tp_layer_size_pcts"),
    )


def parse_args():
    p = argparse.ArgumentParser(description="Backtest framework")
    p.add_argument("--config", required=True, help="YAML config útvonal")
    p.add_argument("--data", default=None, help="Parquet override (különben a config-ból)")
    p.add_argument("--epic", default=None, help="Epic override (különben a config-ból)")
    p.add_argument("--runs_dir", default="runs", help="Run-mappák gyökere")
    p.add_argument("--suffix", default=None, help="Run-mappa név végéhez fűzött tag")
    p.add_argument("--from", dest="ts_from", default=None,
                   help="Tick szűrő kezdet (ISO), pl. 2026-06-01 vagy 2026-06-01T00:00:00")
    p.add_argument("--to", dest="ts_to", default=None,
                   help="Tick szűrő vég (exclusive), pl. 2026-07-01")
    return p.parse_args()


def main():
    args = parse_args()

    config_path = Path(args.config)
    cfg = load_config(config_path)

    tick_path = Path(args.data or cfg["data"]["tick_parquet"])
    if not tick_path.is_absolute():
        tick_path = (ROOT / tick_path).resolve()

    strategy = build_strategy(cfg["strategy"], cfg["strategy_params"])
    engine_cfg = build_engine_cfg(cfg, args.epic)

    run_dir = make_run_dir(args.runs_dir, strategy.name, engine_cfg.epic, mode="backtest", suffix=args.suffix)
    logger = RunLogger(run_dir, config={
        **cfg,
        "_runtime": {
            "config_path": str(config_path),
            "tick_parquet": str(tick_path),
            "epic_override": args.epic,
        },
    }, mode="backtest")

    try:
        logger.log_text(f"Tick parquet betöltése: {tick_path}")
        ticks = load_ticks(tick_path)

        # Opcionális time-slice (walk-forward validációhoz)
        if args.ts_from or args.ts_to:
            import pandas as pd
            n_before = len(ticks)
            if args.ts_from:
                ts_from = pd.Timestamp(args.ts_from, tz="UTC")
                ticks = ticks[ticks["timestamp_utc"] >= ts_from]
            if args.ts_to:
                ts_to = pd.Timestamp(args.ts_to, tz="UTC")
                ticks = ticks[ticks["timestamp_utc"] < ts_to]
            ticks = ticks.reset_index(drop=True)
            logger.log_text(
                f"Time-slice szűrő: {args.ts_from or '-inf'} → {args.ts_to or '+inf'} | "
                f"ticks: {n_before:,} → {len(ticks):,}"
            )

        t_min, t_max = time_range(ticks)
        logger.log_text(f"Ticks: {len(ticks):,} | {t_min.isoformat()} → {t_max.isoformat()}")

        timeframes = list(dict.fromkeys([engine_cfg.candle_tf] + strategy.required_timeframes()))
        data_summary = compute_data_summary(
            ticks=ticks,
            parquet_path=tick_path,
            candle_tf=engine_cfg.candle_tf,
            timeframes=timeframes,
            max_gap_factor=engine_cfg.max_gap_factor,
            min_segment_duration=engine_cfg.min_segment_duration,
        )
        print_data_summary(data_summary)
        save_data_summary(data_summary, run_dir)
        logger.log_text(
            f"Data summary mentve | sha256={data_summary['file'].get('sha256', '')[:16]}... | "
            f"segments={data_summary['segments']['count']} | "
            f"usable={data_summary['segments']['usable_pct']}%"
        )

        runner = BacktestRunner(strategy, engine_cfg, logger)
        metrics = runner.run(ticks)

        print(json.dumps(metrics, indent=2, default=str))
        print(f"\nRun-mappa: {run_dir}")
    finally:
        logger.close()


if __name__ == "__main__":
    main()
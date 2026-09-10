#!/usr/bin/env python3
"""
Live trading runner — backtest config-fájl alapján demo-számlán fut.

Példa:
  # Best paraméterekkel (a quick-optimize által írt) demón:
  python -m live.run_live --config runs/<RUN>/final_backtest/config.yaml --epic GOLD

  # Custom konfig, dry-run (nem nyit valódi pozíciót, csak logol):
  python -m live.run_live --config backtest/configs/trend_reversal.yaml --epic GOLD --dry-run

Per-futás:
  runs/<timestamp>_live_<strategy>_<epic>/
    ├── config.yaml          # a futás teljes config-ja (snapshot)
    ├── events.jsonl         # minden event 1 sor (decision/open/close/sl_moved)
    ├── decisions.csv
    ├── trades.csv
    ├── run.log              # human-readable
    └── metrics.json         # leállás után
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backtest.engine.runner import EngineConfig, apply_tp_layers_preset
from backtest.runlog.run_logger import RunLogger, make_run_dir
from backtest.strategies.registry import build_strategy

from live.capital_client import CapitalClient
from live.runner import LiveConfig, LiveRunner


def parse_args():
    p = argparse.ArgumentParser(description="Live trader (Capital.com)")
    p.add_argument("--config", required=True, help="Backtest stratégia YAML")
    p.add_argument("--epic", default=None, help="Epic override (különben a config-ból)")
    p.add_argument("--runs_dir", default="runs")
    p.add_argument("--account", default=None, help="Capital account_id override")
    p.add_argument("--live", action="store_true", help="ÉLES számla használata (default: demo)")
    p.add_argument("--dry-run", action="store_true",
                   help="Nem nyit valódi pozíciót — csak logol")
    p.add_argument("--candle_refresh_sec", type=float, default=60.0)
    p.add_argument("--position_poll_sec", type=float, default=10.0)
    p.add_argument("--candle_max_points", type=int, default=500)
    p.add_argument("--suffix", default=None)
    p.add_argument("--state", default=None,
                   help="Ownership state-file (több stratégia egy fiókon; pl. state_live_donch.json)")
    p.add_argument("--dynamic-equity", action="store_true",
                   help="Compound sizing: fixed_risk equity = élő fiók-balance (5 percenként frissítve)")
    p.add_argument("--equity-conv-price", action="store_true",
                   help="JPY-quote instrumentum: equity_ref = balance × aktuális mid")
    p.add_argument("--size-increment", type=float, default=0.0,
                   help="Order-size kerekítés (pl. USDJPY: 100)")
    return p.parse_args()


def load_config(path: Path) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def build_engine_cfg(cfg: dict, epic: str) -> EngineConfig:
    e = dict(cfg["engine"])
    apply_tp_layers_preset(e)
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


async def main_async() -> None:
    args = parse_args()
    cfg_path = Path(args.config)
    cfg = load_config(cfg_path)
    epic = args.epic or cfg["data"]["epic"]

    strategy = build_strategy(cfg["strategy"], cfg["strategy_params"])
    engine_cfg = build_engine_cfg(cfg, epic)

    mode_suffix = args.suffix or ("dryrun" if args.dry_run else "live")
    run_dir = make_run_dir(args.runs_dir, strategy.name, epic, mode="live", suffix=mode_suffix)
    log = RunLogger(run_dir, config={
        **cfg,
        "_live": {
            "demo": not args.live,
            "dry_run": args.dry_run,
            "epic": epic,
            "account": args.account,
            "config_path": str(cfg_path),
        },
    }, mode="live", echo_console=True)
    log.log_text(f"=== LIVE RUNNER START | strategy={strategy.name} | epic={epic} | "
                 f"mode={'LIVE' if args.live else 'DEMO'} | dry_run={args.dry_run} ===")

    client = CapitalClient(demo=not args.live, account_id=args.account)
    live_cfg = LiveConfig(
        epic=epic,
        candle_max_points=args.candle_max_points,
        candle_refresh_seconds=args.candle_refresh_sec,
        position_poll_seconds=args.position_poll_sec,
        dry_run=args.dry_run,
        ownership_state_path=args.state,
        dynamic_equity=args.dynamic_equity,
        equity_price_conversion=args.equity_conv_price,
        order_size_increment=args.size_increment,
    )
    runner = LiveRunner(strategy, engine_cfg, live_cfg, client, log)
    print(f"Run-mappa: {run_dir}")
    try:
        await runner.run()
    finally:
        log.log_text("=== LIVE RUNNER STOP ===")
        log.close()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
    try:
        asyncio.run(main_async())
    except KeyboardInterrupt:
        print("\n[STOP] Leállítás...")


if __name__ == "__main__":
    main()

"""
run_backtest.py
===============
Alap backtest futtatás parquet tick adatokon.

Használat:
  python run_backtest.py --data_dir ./data --candle_tf 5min
  python run_backtest.py --synthetic            # szintetikus adaton teszt
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from core.data_loader  import TickDataLoader, generate_synthetic_ticks
from core.engine       import BacktestEngine, EngineConfig
from reports.reporter  import print_metrics, generate_html_report


def parse_args():
    p = argparse.ArgumentParser(description="XAUUSD Range Scalper Backtest")
    p.add_argument("--data_dir",   default="./data",   help="Parquet fájlok mappája")
    p.add_argument("--pattern",    default="*.parquet",help="Fájlminta")
    p.add_argument("--candle_tf",  default="5min",     help="LTF gyertya TF")
    p.add_argument("--htf_tf",     default="1h",       help="HTF szűrő TF")
    p.add_argument("--output",     default="backtest_report.html")
    p.add_argument("--synthetic",  action="store_true", help="Szintetikus adat")

    # Strategy params
    p.add_argument("--sl",      type=float, default=5.0)
    p.add_argument("--tp",      type=float, default=8.0)
    p.add_argument("--lot",     type=float, default=1.0)
    p.add_argument("--adx_thr", type=float, default=25.0)
    p.add_argument("--buf",     type=float, default=0.30)
    return p.parse_args()


def main():
    args = parse_args()

    # ── 1. ADAT BETÖLTÉS ──────────────────────────────────────────────────────
    if args.synthetic:
        print("⚙️  Szintetikus adat generálása (300k tick)...")
        ticks = generate_synthetic_ticks(n_ticks=300_000)
    else:
        loader = TickDataLoader(args.data_dir)
        ticks  = loader.load_ticks(args.pattern)

    loader  = TickDataLoader()
    candles = loader.build_mtf_data(ticks)

    # ── 2. KONFIGURÁCIÓ ───────────────────────────────────────────────────────
    cfg = EngineConfig(
        lot_size      = args.lot,
        sl_dollars    = args.sl,
        tp_dollars    = args.tp,
        candle_tf     = args.candle_tf,
        htf_candle_tf = args.htf_tf,
        adx_threshold = args.adx_thr,
        entry_buffer  = args.buf,
    )

    # ── 3. FUTTATÁS ───────────────────────────────────────────────────────────
    print(f"Backtest futtatása...")
    engine = BacktestEngine(cfg)
    result = engine.run(ticks, candles)

    # ── 4. EREDMÉNY ───────────────────────────────────────────────────────────
    print_metrics(result.metrics)

    out = generate_html_report(result, cfg, output_path=args.output)

    # Trade CSV exportálás
    df = result.trades_df()
    if not df.empty:
        csv_path = args.output.replace(".html", "_trades.csv")
        df.to_csv(csv_path, index=False)
        print(f"✓ Trade CSV mentve → {csv_path}")


if __name__ == "__main__":
    main()

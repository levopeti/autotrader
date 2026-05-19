"""
run_optimize.py
===============
Walk-Forward + Optuna optimalizálás futtatása.

Használat:
  python run_optimize.py --data_dir ./data --n_trials 150 --n_splits 5
  python run_optimize.py --synthetic --n_trials 50
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from core.data_loader       import TickDataLoader, generate_synthetic_ticks
from optimization.optimizer import WalkForwardOptimizer, VALID_METRICS
from reports.reporter       import generate_html_report, print_metrics
from core.engine            import BacktestEngine, EngineConfig


def parse_args():
    p = argparse.ArgumentParser(description="XAUUSD Scalper — Walk-Forward Optimizer")
    p.add_argument("--data_dir",  default="./data",        help="Parquet fájlok mappája")
    p.add_argument("--pattern",   default="*.parquet",     help="Fájlszűrő minta")
    p.add_argument("--candle_tf", default="5min",          help="LTF gyertya TF (1min/5min/15min)")
    p.add_argument("--htf_tf",    default="1h",            help="HTF szűrő TF (15min/1h)")
    p.add_argument("--n_splits",  type=int,   default=5,   help="Walk-Forward fold-ok száma")
    p.add_argument("--is_ratio",  type=float, default=0.70,help="In-Sample arány (0.0–1.0)")
    p.add_argument("--n_trials",  type=int,   default=100, help="Optuna trial-ok száma foldonként")
    p.add_argument("--metric",    default="sharpe_ratio",  choices=VALID_METRICS,
                   help="Optimalizálási célfüggvény")
    p.add_argument("--n_jobs",    type=int,   default=1,   help="Párhuzamos trial-ok (-1 = összes CPU)")
    p.add_argument("--output",    default="wf_report.html",help="HTML riport kimeneti fájl")
    p.add_argument("--synthetic", action="store_true",     help="Szintetikus adaton fut")
    return p.parse_args()


def main():
    args = parse_args()

    # ── 1. Adat ───────────────────────────────────────────────────────────────
    if args.synthetic:
        print("⚙️  Szintetikus adat generálása (300k tick)...")
        ticks = generate_synthetic_ticks(n_ticks=300_000)
    else:
        loader = TickDataLoader(args.data_dir)
        ticks  = loader.load_ticks(args.pattern)

    # ── 2. Walk-Forward optimalizálás ─────────────────────────────────────────
    wf = WalkForwardOptimizer(
        ticks             = ticks,
        n_splits          = args.n_splits,
        is_ratio          = args.is_ratio,
        n_trials_per_fold = args.n_trials,
        candle_tf         = args.candle_tf,
        htf_tf            = args.htf_tf,
        metric            = args.metric,
        n_jobs            = args.n_jobs,
    )
    summary_df = wf.run()

    # ── 3. Legjobb paraméterek visszatesztelése a teljes adaton ───────────────
    best_params = wf.best_params_for_live()
    print("\n📌 Legjobb paraméterek (live-ra javasolt):")
    for k, v in best_params.items():
        print(f"   {k:<30} = {v}")

    loader_full = TickDataLoader()
    candles     = loader_full.build_mtf_data(ticks)
    cfg_best    = EngineConfig(
        candle_tf     = args.candle_tf,
        htf_candle_tf = args.htf_tf,
        lot_size      = 1.0,
        **best_params,
    )
    full_result = BacktestEngine(cfg_best).run(ticks, candles)
    print_metrics(full_result.metrics, title="TELJES ADAT — LEGJOBB PARAMÉTEREKKEL")

    # ── 4. HTML riport + CSV ───────────────────────────────────────────────────
    generate_html_report(full_result, cfg_best,
                         output_path=args.output, wf_df=summary_df)

    csv_path = args.output.replace(".html", "_wf_summary.csv")
    summary_df.to_csv(csv_path, index=False)
    print(f"✓ WF összefoglaló CSV → {csv_path}")


if __name__ == "__main__":
    main()

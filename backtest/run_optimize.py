#!/usr/bin/env python3
"""
Optimalizáló CLI.

Példa:
  # gyors felderítés (egy study a teljes adaton)
  python -m backtest.run_optimize \\
      --config backtest/configs/trend_reversal.yaml \\
      --search_space backtest/configs/trend_reversal_search.yaml \\
      --data capital/logs/tick_data.parquet \\
      --mode quick --n_trials 100 --metric sharpe_ratio

  # Walk-Forward — live-ra való hangoláshoz
  python -m backtest.run_optimize --config backtest/configs/range_scalp.yaml  --search_space backtest/configs/range_scalp_search.yaml --data data/tick_data_GOLD.parquet --mode walk_forward --n_trials 100 --n_splits 5 --is_ratio 0.7
  python -m backtest.run_optimize --config backtest/configs/trend_reversal.yaml  --search_space backtest/configs/trend_reversal_search.yaml --data data/tick_data_GOLD.parquet --mode walk_forward --n_trials 100 --n_splits 5 --is_ratio 0.7

Az eredmények a runs/<timestamp>_optimize_<strategy>_<epic>/ mappába kerülnek.
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
from backtest.engine.runner import BacktestRunner
from backtest.optimization.objective import ObjectiveConfig, VALID_METRICS, _build_engine_cfg
from backtest.optimization.param_space import apply_suggestion, load_search_space
from backtest.optimization.quick import QuickOptimizer
from backtest.optimization.walk_forward import WalkForwardOptimizer
from backtest.runlog.run_logger import RunLogger, make_run_dir
from backtest.strategies.registry import build_strategy


def parse_args():
    p = argparse.ArgumentParser(description="Backtest optimizer (quick + walk-forward)")
    p.add_argument("--config", required=True, help="Strategy YAML")
    p.add_argument("--search_space", required=True, help="Search space YAML")
    p.add_argument("--data", default=None, help="Parquet override")
    p.add_argument("--epic", default=None, help="Epic override")
    p.add_argument("--runs_dir", default="runs")
    p.add_argument("--mode", choices=["quick", "walk_forward"], default="quick")
    p.add_argument("--metric", choices=list(VALID_METRICS), default="sharpe_ratio")
    p.add_argument("--n_trials", type=int, default=100)
    p.add_argument("--n_splits", type=int, default=5, help="Only WF")
    p.add_argument("--is_ratio", type=float, default=0.70, help="Only WF")
    p.add_argument("--n_jobs", type=int, default=1)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--min_trades", type=int, default=20)
    p.add_argument("--max_drawdown_abs", type=float, default=None)
    p.add_argument("--min_win_rate", type=float, default=None)
    p.add_argument("--no_progress", action="store_true")
    p.add_argument("--suffix", default=None)
    return p.parse_args()


def load_yaml(path: Path) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def main():
    args = parse_args()

    cfg_path = Path(args.config)
    base_cfg = load_yaml(cfg_path)
    search_space = load_search_space(args.search_space)

    epic = args.epic or base_cfg["data"]["epic"]
    base_cfg.setdefault("data", {})["epic"] = epic

    tick_path = Path(args.data or base_cfg["data"]["tick_parquet"])
    if not tick_path.is_absolute():
        tick_path = (ROOT / tick_path).resolve()

    obj_cfg = ObjectiveConfig(
        metric=args.metric,
        min_trades=args.min_trades,
        max_drawdown_abs=args.max_drawdown_abs,
        min_win_rate=args.min_win_rate,
    )

    strategy_name = base_cfg["strategy"]
    run_dir = make_run_dir(args.runs_dir, strategy_name, epic, mode="optimize", suffix=args.suffix or args.mode)

    logger = RunLogger(run_dir, config={
        **base_cfg,
        "_optimize": {
            "mode": args.mode,
            "metric": args.metric,
            "n_trials": args.n_trials,
            "n_splits": args.n_splits if args.mode == "walk_forward" else None,
            "is_ratio": args.is_ratio if args.mode == "walk_forward" else None,
            "min_trades": args.min_trades,
            "max_drawdown_abs": args.max_drawdown_abs,
            "min_win_rate": args.min_win_rate,
            "seed": args.seed,
            "n_jobs": args.n_jobs,
        },
        "_runtime": {
            "config_path": str(cfg_path),
            "search_space_path": str(args.search_space),
            "tick_parquet": str(tick_path),
        },
    }, mode="optimize")

    try:
        logger.log_text(f"Tick parquet betöltése: {tick_path}")
        ticks = load_ticks(tick_path)
        t_min, t_max = time_range(ticks)
        logger.log_text(f"Ticks: {len(ticks):,} | {t_min.isoformat()} → {t_max.isoformat()}")

        engine_block = base_cfg.get("engine", {})
        # A stratégia required_timeframes-éhez egy átmeneti példányt építünk a base configgal
        try:
            tmp_strategy = build_strategy(base_cfg["strategy"], base_cfg.get("strategy_params", {}))
            strategy_tfs = tmp_strategy.required_timeframes()
        except Exception:
            strategy_tfs = [engine_block.get("candle_tf", "5min")]
        timeframes = list(dict.fromkeys([engine_block.get("candle_tf", "5min")] + strategy_tfs))

        data_summary = compute_data_summary(
            ticks=ticks,
            parquet_path=tick_path,
            candle_tf=engine_block.get("candle_tf", "5min"),
            timeframes=timeframes,
            max_gap_factor=engine_block.get("max_gap_factor", 2.0),
            min_segment_duration=engine_block.get("min_segment_duration", "1h"),
        )
        print_data_summary(data_summary)
        save_data_summary(data_summary, run_dir)
        logger.log_text(
            f"Data summary mentve | sha256={data_summary['file'].get('sha256', '')[:16]}... | "
            f"segments={data_summary['segments']['count']} | "
            f"usable={data_summary['segments']['usable_pct']}%"
        )

        if args.mode == "quick":
            result = _run_quick(args, ticks, base_cfg, search_space, obj_cfg, logger)
        else:
            result = _run_walk_forward(args, ticks, base_cfg, search_space, obj_cfg, logger)

        _final_backtest(base_cfg, result, ticks, run_dir, logger)
        print(f"\nRun-mappa: {run_dir}")
    finally:
        logger.close()


def _run_quick(args, ticks, base_cfg, search_space, obj_cfg, logger):
    logger.log_text(f"Quick optimize | n_trials={args.n_trials} | metric={args.metric}")
    opt = QuickOptimizer(
        ticks=ticks,
        base_config=base_cfg,
        search_space=search_space,
        objective_cfg=obj_cfg,
        n_trials=args.n_trials,
        n_jobs=args.n_jobs,
        seed=args.seed,
    )
    result = opt.run(show_progress=not args.no_progress)

    result.trials_df.to_csv(logger.run_dir / "trials.csv", index=False)
    with open(logger.run_dir / "best_params.yaml", "w", encoding="utf-8") as f:
        yaml.safe_dump(result.best_params, f, sort_keys=False, allow_unicode=True)
    with open(logger.run_dir / "optimize_summary.json", "w", encoding="utf-8") as f:
        json.dump({
            "mode": "quick",
            "n_trials": args.n_trials,
            "metric": args.metric,
            "best_score": float(result.best_score),
            "best_metrics": result.best_metrics,
            "best_params": result.best_params,
        }, f, indent=2, default=str)

    logger.log_text(f"Quick kész. best_{args.metric}={result.best_score:.4f}")
    print(json.dumps({"best_score": result.best_score, "best_metrics": result.best_metrics, "best_params": result.best_params}, indent=2, default=str))
    return result


def _run_walk_forward(args, ticks, base_cfg, search_space, obj_cfg, logger):
    logger.log_text(
        f"WF | n_splits={args.n_splits} | is_ratio={args.is_ratio} | "
        f"n_trials/fold={args.n_trials} | metric={args.metric}"
    )
    opt = WalkForwardOptimizer(
        ticks=ticks,
        base_config=base_cfg,
        search_space=search_space,
        objective_cfg=obj_cfg,
        n_splits=args.n_splits,
        is_ratio=args.is_ratio,
        n_trials_per_fold=args.n_trials,
        n_jobs=args.n_jobs,
        seed=args.seed,
    )
    result = opt.run(show_progress=not args.no_progress)

    result.summary_df.to_csv(logger.run_dir / "wf_summary.csv", index=False)
    result.trials_df.to_csv(logger.run_dir / "trials.csv", index=False)
    with open(logger.run_dir / "best_params.yaml", "w", encoding="utf-8") as f:
        yaml.safe_dump(result.best_params_for_live, f, sort_keys=False, allow_unicode=True)
    with open(logger.run_dir / "optimize_summary.json", "w", encoding="utf-8") as f:
        json.dump({
            "mode": "walk_forward",
            "n_splits": args.n_splits,
            "is_ratio": args.is_ratio,
            "metric": args.metric,
            "n_folds": len(result.folds),
            "best_params_for_live": result.best_params_for_live,
            "oos_scores": [f.oos_score for f in result.folds],
        }, f, indent=2, default=str)

    logger.log_text(
        f"WF kész. folds={len(result.folds)} | "
        f"oos_scores={[round(f.oos_score, 3) if f.oos_score == f.oos_score else None for f in result.folds]}"
    )
    print(result.summary_df.to_string(index=False))
    print(f"\nLive-ra javasolt paraméterek: {json.dumps(result.best_params_for_live, indent=2)}")
    return result


def _final_backtest(base_cfg, result, ticks, run_dir, optimize_logger):
    """A legjobb paraméterekkel egy teljes loggolt backtest a teljes adaton."""
    best = getattr(result, "best_params_for_live", None) or getattr(result, "best_params", None) or {}
    if not best:
        optimize_logger.log_text("Nincs legjobb paraméter → final backtest kihagyva.")
        return

    merged = apply_suggestion(base_cfg, best)
    final_dir = run_dir / "final_backtest"
    final_dir.mkdir(parents=True, exist_ok=True)
    final_logger = RunLogger(final_dir, config=merged, mode="backtest_final")
    try:
        strategy = build_strategy(merged["strategy"], merged["strategy_params"])
        engine_cfg = _build_engine_cfg(merged, merged["data"]["epic"])
        runner = BacktestRunner(strategy, engine_cfg, final_logger)
        final_metrics = runner.run(ticks)
        optimize_logger.log_text(
            f"Final backtest a legjobb paraméterekkel: "
            f"n_trades={final_metrics.get('n_trades')} | pnl={final_metrics.get('total_pnl')}"
        )
    finally:
        final_logger.close()


if __name__ == "__main__":
    main()
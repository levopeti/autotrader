"""
Optimizer
=========
Optuna-alapú paraméter optimalizáló Walk-Forward validálással.

Használat:
  from optimization.optimizer import WalkForwardOptimizer
  wf = WalkForwardOptimizer(ticks, n_splits=5, n_trials_per_fold=150)
  summary_df = wf.run()
"""

from __future__ import annotations

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import numpy as np
import pandas as pd
import optuna
from typing import Dict, List, Optional, Tuple
from dataclasses import asdict

optuna.logging.set_verbosity(optuna.logging.WARNING)

from core.engine   import BacktestEngine, EngineConfig, compute_metrics
from core.data_loader import TickDataLoader


# ── PARAMÉTER TÉR ─────────────────────────────────────────────────────────────

PARAM_SPACE: Dict[str, dict] = {
    "sl_dollars":           {"type": "float", "low": 2.0,  "high": 20.0},
    "tp_dollars":           {"type": "float", "low": 3.0,  "high": 30.0},
    "adx_threshold":        {"type": "float", "low": 15.0, "high": 40.0},
    "bb_period":            {"type": "int",   "low": 10,   "high": 50},
    "bb_std":               {"type": "float", "low": 1.5,  "high": 3.5},
    "bb_squeeze_pct":       {"type": "float", "low": 0.10, "high": 0.60},
    "range_lookback":       {"type": "int",   "low": 20,   "high": 120},
    "entry_buffer":         {"type": "float", "low": 0.05, "high": 2.00},
    "tick_vel_max":         {"type": "float", "low": 0.5,  "high": 15.0},
    "spread_z_max":         {"type": "float", "low": 1.5,  "high": 5.0},
    "max_trade_dur_min":    {"type": "int",   "low": 20,   "high": 300},
}

# Metrikák, amire lehet optimalizálni
VALID_METRICS = [
    "sharpe_ratio", "sortino_ratio", "profit_factor",
    "total_pnl", "expectancy", "calmar_ratio",
]


# ── OBJECTIVE ─────────────────────────────────────────────────────────────────

def _suggest(trial: optuna.Trial, space: dict = PARAM_SPACE) -> dict:
    params = {}
    for name, cfg in space.items():
        if cfg["type"] == "float":
            params[name] = trial.suggest_float(name, cfg["low"], cfg["high"])
        elif cfg["type"] == "int":
            params[name] = trial.suggest_int(name, cfg["low"], cfg["high"])
        elif cfg["type"] == "categorical":
            params[name] = trial.suggest_categorical(name, cfg["choices"])
    return params


def build_objective(
    ticks:           pd.DataFrame,
    candle_tf:       str   = "5min",
    htf_tf:          str   = "1h",
    metric:          str   = "sharpe_ratio",
    min_trades:      int   = 30,
    max_dd_pct:      float = 25.0,
    min_win_rate:    float = 0.38,
):
    loader = TickDataLoader()

    def objective(trial: optuna.Trial) -> float:
        params = _suggest(trial)

        # Hard constraint: TP/SL >= 1.2
        if params["tp_dollars"] < params["sl_dollars"] * 1.2:
            return float("-inf")

        cfg = EngineConfig(
            candle_tf=candle_tf, htf_candle_tf=htf_tf, lot_size=1.0, **params
        )
        candles = loader.build_mtf_data(ticks)
        engine  = BacktestEngine(cfg)

        try:
            result = engine.run(ticks, candles)
        except Exception:
            return float("-inf")

        m = result.metrics
        if m.get("n_trades", 0) < min_trades:
            return float("-inf")

        score = m.get(metric, float("-inf"))
        if not np.isfinite(score):
            return float("-inf")

        # Penalizációk
        dd = abs(m.get("max_drawdown_pct", 0.0))
        if dd > max_dd_pct:
            score *= (max_dd_pct / dd) ** 2

        wr = m.get("win_rate", 0.0)
        if wr < min_win_rate:
            score *= (wr / min_win_rate) ** 2

        # Jutalom: jó RR arány
        if m.get("avg_win", 0) > 0 and m.get("avg_loss", 0) < 0:
            rr = m["avg_win"] / abs(m["avg_loss"])
            if rr > 1.5:
                score *= 1.0 + 0.1 * min(rr - 1.5, 1.5)

        return score

    return objective


# ── WALK-FORWARD ──────────────────────────────────────────────────────────────

class WalkForwardOptimizer:
    """
    Walk-Forward Analysis.

    Az adatot n_splits egyenlő részre osztja.
    Minden részen belül is_ratio arányban IS (train), a maradék OOS (test).
    """

    def __init__(
        self,
        ticks:              pd.DataFrame,
        n_splits:           int   = 5,
        is_ratio:           float = 0.70,
        n_trials_per_fold:  int   = 100,
        candle_tf:          str   = "5min",
        htf_tf:             str   = "1h",
        metric:             str   = "sharpe_ratio",
        n_jobs:             int   = 1,
        min_trades:         int   = 30,
        verbose:            bool  = True,
    ):
        self.ticks   = ticks.copy().reset_index(drop=True)
        self.n_splits  = n_splits
        self.is_ratio  = is_ratio
        self.n_trials  = n_trials_per_fold
        self.candle_tf = candle_tf
        self.htf_tf    = htf_tf
        self.metric    = metric
        self.n_jobs    = n_jobs
        self.min_trades= min_trades
        self.verbose   = verbose
        self.fold_results: List[dict] = []

    # ── FŐFUTTATÁS ────────────────────────────────────────────────────────────

    def run(self) -> pd.DataFrame:
        splits = self._make_splits()
        if self.verbose:
            print(f"\n{'═'*62}")
            print(f"  Walk-Forward: {len(splits)} fold | "
                  f"{self.n_trials} trial/fold | metric={self.metric}")
            print(f"{'═'*62}")

        loader = TickDataLoader()

        for fold_i, (is_t, oos_t) in enumerate(splits):
            if self.verbose:
                print(f"\nFold {fold_i+1}/{len(splits)}")
                print(f"  IS : {is_t['timestamp'].min().date()} "
                      f"→ {is_t['timestamp'].max().date()} ({len(is_t):,} tick)")
                print(f"  OOS: {oos_t['timestamp'].min().date()} "
                      f"→ {oos_t['timestamp'].max().date()} ({len(oos_t):,} tick)")

            # ── IS optimalizálás ──────────────────────────────────────────
            study = optuna.create_study(
                direction="maximize",
                sampler=optuna.samplers.TPESampler(
                    seed=42 + fold_i,
                    n_startup_trials=20,
                    multivariate=True,
                ),
                pruner=optuna.pruners.HyperbandPruner(),
            )
            obj = build_objective(
                is_t, self.candle_tf, self.htf_tf,
                self.metric, self.min_trades,
            )
            study.optimize(obj, n_trials=self.n_trials, n_jobs=self.n_jobs,
                           show_progress_bar=self.verbose)

            best_params   = study.best_params
            best_is_score = study.best_value

            if self.verbose:
                print(f"  IS best {self.metric}: {best_is_score:.4f}")

            # ── OOS validálás ─────────────────────────────────────────────
            candles_oos = loader.build_mtf_data(oos_t)
            cfg         = EngineConfig(
                candle_tf=self.candle_tf, htf_candle_tf=self.htf_tf,
                lot_size=1.0, **best_params
            )
            oos_result  = BacktestEngine(cfg).run(oos_t, candles_oos)
            oos_m       = oos_result.metrics
            oos_score   = oos_m.get(self.metric, float("nan"))

            if self.verbose:
                print(
                    f"  OOS {self.metric}: {oos_score:.4f}  |  "
                    f"trades={oos_m.get('n_trades',0)}  |  "
                    f"win={oos_m.get('win_rate',0):.1%}  |  "
                    f"PnL=${oos_m.get('total_pnl',0):,.0f}"
                )

            self.fold_results.append({
                "fold":       fold_i + 1,
                "is_start":   is_t["timestamp"].min().date().isoformat(),
                "is_end":     is_t["timestamp"].max().date().isoformat(),
                "oos_start":  oos_t["timestamp"].min().date().isoformat(),
                "oos_end":    oos_t["timestamp"].max().date().isoformat(),
                "is_score":   round(best_is_score, 4),
                "oos_score":  round(oos_score, 4) if np.isfinite(oos_score) else None,
                "oos_pnl":    oos_m.get("total_pnl"),
                "oos_trades": oos_m.get("n_trades"),
                "oos_wr":     oos_m.get("win_rate"),
                "oos_pf":     oos_m.get("profit_factor"),
                "oos_dd":     oos_m.get("max_drawdown_pct"),
                "best_params":best_params,
                "oos_metrics":oos_m,
                "study":      study,
            })

        summary = self._summary_df()
        if self.verbose:
            self._print_summary(summary)
        return summary

    # ── SPLITS ────────────────────────────────────────────────────────────────

    def _make_splits(self) -> List[Tuple[pd.DataFrame, pd.DataFrame]]:
        n         = len(self.ticks)
        fold_size = n // self.n_splits
        splits    = []
        for i in range(self.n_splits):
            start = i * fold_size
            end   = start + fold_size if i < self.n_splits - 1 else n
            win   = self.ticks.iloc[start:end]
            split = int(len(win) * self.is_ratio)
            is_t, oos_t = win.iloc[:split], win.iloc[split:]
            if len(is_t) >= 2000 and len(oos_t) >= 500:
                splits.append((is_t.copy(), oos_t.copy()))
        return splits

    # ── ÖSSZEFOGLALÓ ──────────────────────────────────────────────────────────

    def _summary_df(self) -> pd.DataFrame:
        rows = []
        for r in self.fold_results:
            rows.append({k: v for k, v in r.items()
                         if k not in ("best_params", "oos_metrics", "study")})
        return pd.DataFrame(rows)

    def _print_summary(self, df: pd.DataFrame):
        oos_scores = [r["oos_score"] for r in self.fold_results
                      if r["oos_score"] is not None]
        oos_pnls   = [r["oos_pnl"] for r in self.fold_results
                      if r["oos_pnl"] is not None]
        consistent = sum(s > 0 for s in oos_scores) / len(oos_scores) if oos_scores else 0

        print(f"\n{'═'*62}")
        print(f"  WALK-FORWARD ÖSSZEFOGLALÓ")
        print(f"{'═'*62}")
        print(f"  Átlag OOS {self.metric:<20} {np.mean(oos_scores):.4f}")
        print(f"  Szórás OOS {self.metric:<19} {np.std(oos_scores):.4f}")
        print(f"  Konzisztencia (OOS>0)        {consistent:.0%}")
        print(f"  Összes OOS PnL               ${sum(oos_pnls):,.2f}")
        print(f"{'═'*62}\n")

    def best_params_for_live(self) -> dict:
        """A legstabilabb paraméterek: legjobb konzisztenciájú fold."""
        if not self.fold_results:
            return {}
        scores = [(r["oos_score"] or 0.0, i) for i, r in enumerate(self.fold_results)]
        best_i = max(scores, key=lambda x: x[0])[1]
        return self.fold_results[best_i]["best_params"]

    def all_studies(self) -> List[optuna.Study]:
        return [r["study"] for r in self.fold_results if "study" in r]


# ── GYORS GRID SEARCH ─────────────────────────────────────────────────────────

class GridSearchOptimizer:
    """
    Kisebb paramétertérre való gyors grid search.
    Hasznos első felderítéshez, mielőtt Optunát futtatnád.
    """

    def __init__(self, ticks: pd.DataFrame, param_grid: Dict[str, list]):
        self.ticks      = ticks
        self.param_grid = param_grid
        self.loader     = TickDataLoader()

    def run(
        self,
        candle_tf:  str = "5min",
        htf_tf:     str = "1h",
        metric:     str = "sharpe_ratio",
        verbose:    bool = True,
    ) -> pd.DataFrame:
        import itertools
        keys   = list(self.param_grid.keys())
        values = list(self.param_grid.values())
        combos = list(itertools.product(*values))

        candles = self.loader.build_mtf_data(self.ticks)
        results = []

        if verbose:
            print(f"Grid search: {len(combos)} kombináció")

        for i, combo in enumerate(combos):
            params = dict(zip(keys, combo))
            try:
                cfg    = EngineConfig(candle_tf=candle_tf, htf_candle_tf=htf_tf, **params)
                result = BacktestEngine(cfg).run(self.ticks, candles)
                row    = {**params, **result.metrics}
                results.append(row)
            except Exception as e:
                if verbose:
                    print(f"  [{i+1}] Hiba: {e}")

            if verbose and (i + 1) % 20 == 0:
                print(f"  {i+1}/{len(combos)} kész...")

        df = pd.DataFrame(results)
        if metric in df.columns:
            df = df.sort_values(metric, ascending=False).reset_index(drop=True)
        return df

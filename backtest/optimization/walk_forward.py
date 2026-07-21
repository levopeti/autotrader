from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import optuna
import pandas as pd

from ..engine.runner import BacktestRunner
from ..runlog.noop_logger import NoopLogger
from ..strategies.registry import build_strategy
from .objective import ObjectiveConfig, _build_engine_cfg, _score_from_metrics, build_objective
from .param_space import SearchSpace, apply_suggestion
from .quick import _grouped, _records_to_df

optuna.logging.set_verbosity(optuna.logging.WARNING)


@dataclass
class FoldResult:
    fold: int
    is_start: pd.Timestamp
    is_end: pd.Timestamp
    oos_start: pd.Timestamp
    oos_end: pd.Timestamp
    is_score: float
    oos_score: float
    oos_metrics: Dict
    best_params: Dict[str, Dict[str, float]]


@dataclass
class WalkForwardResult:
    folds: List[FoldResult]
    summary_df: pd.DataFrame
    trials_df: pd.DataFrame
    best_params_for_live: Dict[str, Dict[str, float]]


class WalkForwardOptimizer:
    """
    Walk-Forward Analysis. Az adatot időrendben n_splits darabra vágja.
    Minden fold-on belül is_ratio arányban IS (Optuna optimize), maradék OOS (validáció).
    A 'best_params_for_live' annak a foldnak a paraméterszett-je, ahol az OOS metrika
    a legjobb (legkonzisztensebb az IS→OOS átmenetben).
    """

    def __init__(
        self,
        ticks: pd.DataFrame,
        base_config: dict,
        search_space: SearchSpace,
        objective_cfg: ObjectiveConfig,
        n_splits: int = 5,
        is_ratio: float = 0.70,
        n_trials_per_fold: int = 100,
        n_jobs: int = 1,
        seed: int = 42,
        min_is_ticks: int = 5000,
        min_oos_ticks: int = 1000,
    ):
        self.ticks = ticks.reset_index(drop=True)
        self.base_config = base_config
        self.search_space = search_space
        self.objective_cfg = objective_cfg
        self.n_splits = n_splits
        self.is_ratio = is_ratio
        self.n_trials_per_fold = n_trials_per_fold
        self.n_jobs = n_jobs
        self.seed = seed
        self.min_is_ticks = min_is_ticks
        self.min_oos_ticks = min_oos_ticks

    def run(self, show_progress: bool = True) -> WalkForwardResult:
        splits = self._make_splits()
        if not splits:
            raise RuntimeError("Nem keletkezett használható WF fold (kevés a tick?).")

        all_trials: List[Dict] = []
        folds: List[FoldResult] = []

        for i, (is_t, oos_t) in enumerate(splits):
            fold_records: List[Dict] = []

            def on_done(rec: Dict, _i: int = i) -> None:
                rec["fold"] = _i + 1
                fold_records.append(rec)

            obj = build_objective(
                ticks=is_t,
                base_config=self.base_config,
                search_space=self.search_space,
                objective_cfg=self.objective_cfg,
                on_trial_done=on_done,
            )

            study = optuna.create_study(
                direction="maximize",
                sampler=optuna.samplers.TPESampler(seed=self.seed + i, n_startup_trials=20, multivariate=True),
            )
            study.optimize(obj, n_trials=self.n_trials_per_fold, n_jobs=self.n_jobs, show_progress_bar=show_progress)

            best_grouped = _grouped(study.best_params)
            oos_metrics = self._evaluate_oos(oos_t, best_grouped)
            oos_score = _score_from_metrics(oos_metrics, self.objective_cfg) if oos_metrics else float("nan")

            folds.append(FoldResult(
                fold=i + 1,
                is_start=is_t["timestamp_utc"].iloc[0],
                is_end=is_t["timestamp_utc"].iloc[-1],
                oos_start=oos_t["timestamp_utc"].iloc[0],
                oos_end=oos_t["timestamp_utc"].iloc[-1],
                is_score=study.best_value,
                oos_score=oos_score,
                oos_metrics=oos_metrics,
                best_params=best_grouped,
            ))
            all_trials.extend(fold_records)

        summary_df = _folds_to_df(folds)
        trials_df = _records_to_df(all_trials)
        best_live = _pick_best_live(folds)

        return WalkForwardResult(
            folds=folds,
            summary_df=summary_df,
            trials_df=trials_df,
            best_params_for_live=best_live,
        )

    def _make_splits(self) -> List[Tuple[pd.DataFrame, pd.DataFrame]]:
        n = len(self.ticks)
        fold_size = n // self.n_splits
        out = []
        for i in range(self.n_splits):
            start = i * fold_size
            end = start + fold_size if i < self.n_splits - 1 else n
            window = self.ticks.iloc[start:end]
            split_at = int(len(window) * self.is_ratio)
            is_t = window.iloc[:split_at]
            oos_t = window.iloc[split_at:]
            if len(is_t) >= self.min_is_ticks and len(oos_t) >= self.min_oos_ticks:
                out.append((is_t.copy(), oos_t.copy()))
        return out

    def _evaluate_oos(self, oos_t: pd.DataFrame, best_params: Dict[str, Dict[str, float]]) -> Dict:
        merged = apply_suggestion(self.base_config, best_params)
        strategy = build_strategy(merged["strategy"], merged["strategy_params"])
        engine_cfg = _build_engine_cfg(merged, merged["data"]["epic"])
        try:
            return BacktestRunner(strategy, engine_cfg, NoopLogger()).run(oos_t)
        except Exception as e:
            return {"error": str(e)}


def _folds_to_df(folds: List[FoldResult]) -> pd.DataFrame:
    rows = []
    for f in folds:
        rows.append({
            "fold": f.fold,
            "is_start": f.is_start.isoformat(),
            "is_end": f.is_end.isoformat(),
            "oos_start": f.oos_start.isoformat(),
            "oos_end": f.oos_end.isoformat(),
            "is_score": round(f.is_score, 4) if pd.notna(f.is_score) else None,
            "oos_score": round(f.oos_score, 4) if pd.notna(f.oos_score) else None,
            "oos_n_trades": f.oos_metrics.get("n_trades"),
            "oos_total_pnl": f.oos_metrics.get("total_pnl"),
            "oos_win_rate": f.oos_metrics.get("win_rate"),
            "oos_profit_factor": f.oos_metrics.get("profit_factor"),
            "oos_max_dd_abs": f.oos_metrics.get("max_drawdown_abs"),
        })
    return pd.DataFrame(rows)


def _pick_best_live(folds: List[FoldResult]) -> Dict[str, Dict[str, float]]:
    """A legjobb OOS-score-ú fold paraméterei mennek live-ra."""
    valid = [f for f in folds if pd.notna(f.oos_score)]
    if not valid:
        return {}
    best = max(valid, key=lambda f: f.oos_score)
    return best.best_params
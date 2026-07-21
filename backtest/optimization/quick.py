from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

import optuna
import pandas as pd

from .objective import ObjectiveConfig, build_objective
from .param_space import SearchSpace

optuna.logging.set_verbosity(optuna.logging.WARNING)


@dataclass
class QuickResult:
    best_params: Dict[str, Dict[str, float]]
    best_score: float
    best_metrics: Dict
    trials_df: pd.DataFrame
    study: optuna.Study


class QuickOptimizer:
    """
    Egyszerű optimize: egy Optuna study a teljes adaton.
    Gyors felderítésre, paraméter-érzékenységi vizsgálatra.
    """

    def __init__(
        self,
        ticks: pd.DataFrame,
        base_config: dict,
        search_space: SearchSpace,
        objective_cfg: ObjectiveConfig,
        n_trials: int = 100,
        n_jobs: int = 1,
        seed: int = 42,
    ):
        self.ticks = ticks
        self.base_config = base_config
        self.search_space = search_space
        self.objective_cfg = objective_cfg
        self.n_trials = n_trials
        self.n_jobs = n_jobs
        self.seed = seed
        self._trial_records: List[Dict] = []

    def run(self, show_progress: bool = True) -> QuickResult:
        study = optuna.create_study(
            direction="maximize",
            sampler=optuna.samplers.TPESampler(seed=self.seed, n_startup_trials=20, multivariate=True),
        )

        def on_trial_done(rec: Dict) -> None:
            self._trial_records.append(rec)

        obj = build_objective(
            ticks=self.ticks,
            base_config=self.base_config,
            search_space=self.search_space,
            objective_cfg=self.objective_cfg,
            on_trial_done=on_trial_done,
        )

        study.optimize(obj, n_trials=self.n_trials, n_jobs=self.n_jobs, show_progress_bar=show_progress)

        best_trial = study.best_trial
        best_rec = next((r for r in self._trial_records if r["trial_id"] == best_trial.number), None)
        best_metrics = best_rec["metrics"] if best_rec else {}

        return QuickResult(
            best_params=_grouped(study.best_params),
            best_score=study.best_value,
            best_metrics=best_metrics,
            trials_df=_records_to_df(self._trial_records),
            study=study,
        )


def _grouped(flat_params: Dict[str, float]) -> Dict[str, Dict[str, float]]:
    """A 'section.name' kulcsú flat dict-et visszacsoportosítja {section: {name: value}} alakra."""
    out: Dict[str, Dict[str, float]] = {}
    for k, v in flat_params.items():
        if "." in k:
            section, name = k.split(".", 1)
        else:
            section, name = "strategy_params", k
        out.setdefault(section, {})[name] = v
    return out


def _records_to_df(records: List[Dict]) -> pd.DataFrame:
    rows = []
    for r in records:
        row = {"trial_id": r["trial_id"], "score": r["score"]}
        for section, params in (r.get("params") or {}).items():
            for k, v in params.items():
                row[f"param.{section}.{k}"] = v
        for k, v in (r.get("metrics") or {}).items():
            if isinstance(v, (int, float, str)) or v is None:
                row[f"metric.{k}"] = v
        rows.append(row)
    return pd.DataFrame(rows)
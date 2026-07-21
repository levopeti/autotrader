from __future__ import annotations

import copy
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Tuple

import yaml


@dataclass
class SearchSpace:
    """
    YAML formátum:

      strategy_params:
        ema_fast:    {type: int,   low: 5,   high: 25}
        rsi_period:  {type: int,   low: 5,   high: 21}
        tick_min_imbalance: {type: float, low: 0.1, high: 0.9}
        market_type:        {type: categorical, choices: [commodity, crypto]}

      engine:
        max_hold_seconds: {type: int, low: 600, high: 14400}
    """
    raw: Dict[str, Dict[str, Dict[str, Any]]]

    def suggest(self, trial) -> Dict[str, Dict[str, Any]]:
        """{section: {param: value}}, ahol a section: strategy_params | engine."""
        out: Dict[str, Dict[str, Any]] = {}
        for section, params in self.raw.items():
            section_out: Dict[str, Any] = {}
            for name, spec in params.items():
                section_out[name] = _suggest_one(trial, f"{section}.{name}", spec)
            out[section] = section_out
        return out

    def sections(self) -> Tuple[str, ...]:
        return tuple(self.raw.keys())


def load_search_space(path: str | Path) -> SearchSpace:
    with open(path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)
    if not isinstance(raw, dict):
        raise ValueError(f"{path}: a search space YAML root-jának dict-nek kell lennie")
    return SearchSpace(raw=raw)


def apply_suggestion(base_config: dict, suggestion: Dict[str, Dict[str, Any]]) -> dict:
    """
    Visszaad egy mély-másolt config-ot, amibe az Optuna-tól kapott értékek be vannak
    injektálva. section ∈ {strategy_params, engine}.
    """
    merged = copy.deepcopy(base_config)
    for section, params in suggestion.items():
        target = merged.setdefault(section, {})
        for k, v in params.items():
            target[k] = v
    return merged


def _suggest_one(trial, name: str, spec: Dict[str, Any]) -> Any:
    t = spec.get("type", "float")
    if t == "int":
        step = spec.get("step", 1)
        return trial.suggest_int(name, int(spec["low"]), int(spec["high"]), step=int(step))
    if t == "float":
        log = bool(spec.get("log", False))
        step = spec.get("step")
        if step is not None and not log:
            return trial.suggest_float(name, float(spec["low"]), float(spec["high"]), step=float(step))
        return trial.suggest_float(name, float(spec["low"]), float(spec["high"]), log=log)
    if t == "categorical":
        choices = spec["choices"]
        return trial.suggest_categorical(name, choices)
    raise ValueError(f"Ismeretlen paramétertípus: {t} (param: {name})")
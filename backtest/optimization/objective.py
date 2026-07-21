from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Dict, Optional

import numpy as np
import pandas as pd

from ..engine.runner import BacktestRunner, EngineConfig
from ..runlog.noop_logger import NoopLogger
from ..strategies.registry import build_strategy
from .param_space import SearchSpace, apply_suggestion


@dataclass
class ObjectiveConfig:
    metric: str = "sharpe_ratio"
    min_trades: int = 30
    max_drawdown_abs: Optional[float] = None      # ennél nagyobb DD → erős penalty
    min_win_rate: Optional[float] = None          # ennél alacsonyabb → erős penalty
    invalid_value: float = -1e9                   # constraint-megsértés esetén


VALID_METRICS = (
    "sharpe_ratio", "sortino_ratio", "profit_factor",
    "total_pnl", "expectancy", "win_rate",
)


def build_objective(
    ticks: pd.DataFrame,
    base_config: dict,
    search_space: SearchSpace,
    objective_cfg: ObjectiveConfig,
    on_trial_done: Optional[Callable[[Dict], None]] = None,
) -> Callable:
    """
    Optuna-compatible objective függvényt épít. Az on_trial_done callback
    a trial befejezésekor megkapja {trial_id, params, metrics, score}.
    """
    if objective_cfg.metric not in VALID_METRICS:
        raise ValueError(f"Ismeretlen metric: {objective_cfg.metric}. Választható: {VALID_METRICS}")

    epic = base_config["data"]["epic"]
    strategy_name = base_config["strategy"]

    def objective(trial) -> float:
        suggestion = search_space.suggest(trial)
        merged_cfg = apply_suggestion(base_config, suggestion)

        try:
            strategy = build_strategy(strategy_name, merged_cfg["strategy_params"])
            engine_cfg = _build_engine_cfg(merged_cfg, epic)
            runner = BacktestRunner(strategy, engine_cfg, NoopLogger())
            metrics = runner.run(ticks)
        except Exception as e:
            if on_trial_done:
                on_trial_done({"trial_id": trial.number, "params": suggestion, "metrics": {"error": str(e)}, "score": objective_cfg.invalid_value})
            return objective_cfg.invalid_value

        score = _score_from_metrics(metrics, objective_cfg)
        if on_trial_done:
            on_trial_done({"trial_id": trial.number, "params": suggestion, "metrics": metrics, "score": score})
        return score

    return objective


def _score_from_metrics(metrics: Dict, cfg: ObjectiveConfig) -> float:
    n_trades = int(metrics.get("n_trades", 0) or 0)
    if n_trades < cfg.min_trades:
        return cfg.invalid_value

    raw = metrics.get(cfg.metric)
    if raw is None or not np.isfinite(raw):
        return cfg.invalid_value
    score = float(raw)

    if cfg.max_drawdown_abs is not None:
        dd = abs(float(metrics.get("max_drawdown_abs", 0.0) or 0.0))
        if dd > cfg.max_drawdown_abs:
            score *= (cfg.max_drawdown_abs / max(dd, 1e-9)) ** 2

    if cfg.min_win_rate is not None:
        wr = float(metrics.get("win_rate", 0.0) or 0.0)
        if wr < cfg.min_win_rate:
            score *= (wr / cfg.min_win_rate) ** 2 if cfg.min_win_rate > 0 else 0.0

    return score


def _build_engine_cfg(cfg: dict, epic: str) -> EngineConfig:
    e = cfg["engine"]
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
    )
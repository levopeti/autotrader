from __future__ import annotations

from typing import Dict, Type

from .base import Strategy
from .range_scalp import RangeScalp
from .signal_replay import SignalReplay
from .trend_reversal import TrendReversal


STRATEGY_REGISTRY: Dict[str, Type[Strategy]] = {
    "trend_reversal": TrendReversal,
    "range_scalp": RangeScalp,
    "signal_replay": SignalReplay,
}


def build_strategy(name: str, params: dict) -> Strategy:
    if name not in STRATEGY_REGISTRY:
        raise ValueError(f"Ismeretlen stratégia: {name}. Választható: {list(STRATEGY_REGISTRY)}")
    return STRATEGY_REGISTRY[name](params)
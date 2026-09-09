from __future__ import annotations

from typing import Dict, Type

from .base import Strategy
from .donchian_breakout import DonchianBreakout
from .htf_trend_pullback import HtfTrendPullback
from .kalman_mr import KalmanMeanReversion
from .london_breakout import LondonBreakout
from .micro_scalp import MicroScalp
from .range_scalp import RangeScalp
from .signal_replay import SignalReplay
from .trend_reversal import TrendReversal
from .volatility_contraction import VolatilityContraction


STRATEGY_REGISTRY: Dict[str, Type[Strategy]] = {
    "trend_reversal": TrendReversal,
    "range_scalp": RangeScalp,
    "micro_scalp": MicroScalp,
    "signal_replay": SignalReplay,
    "donchian_breakout": DonchianBreakout,
    "volatility_contraction": VolatilityContraction,
    "htf_trend_pullback": HtfTrendPullback,
    "kalman_mr": KalmanMeanReversion,
    "london_breakout": LondonBreakout,
}


def build_strategy(name: str, params: dict) -> Strategy:
    if name not in STRATEGY_REGISTRY:
        raise ValueError(f"Ismeretlen stratégia: {name}. Választható: {list(STRATEGY_REGISTRY)}")
    return STRATEGY_REGISTRY[name](params)
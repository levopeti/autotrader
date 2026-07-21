from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


VALID_SIZING_MODES = ("from_strategy", "fixed_lot", "score_scaled", "fixed_risk")


@dataclass
class SizingConfig:
    mode: str = "from_strategy"
    fixed_lot_size: float = 1.0
    min_order_size: float = 0.01
    max_order_size: float = 10.0
    risk_pct: float = 0.01         # fixed_risk módban: equity × risk_pct = dollár-kockázat trade-enként
    equity: float = 10_000.0


def compute_size(
    sizing: SizingConfig,
    strategy_size: float,
    score: Optional[float],
    sl_distance: Optional[float],
) -> float:
    """
    Visszaadja a tényleges trade size-t a sizing mode alapján.
    Min/max közé clamp-eli a végeredményt.

    - from_strategy: a stratégia által Decision.size-ban javasolt érték
    - fixed_lot: minden trade fixed_lot_size lot-tal nyit
    - score_scaled: lineárisan min..max-ig a score (0..1) szerint
    - fixed_risk: equity × risk_pct / sl_distance ($-kockázat / $-elmozdulás SL-ig)
    """
    if sizing.mode not in VALID_SIZING_MODES:
        raise ValueError(f"Ismeretlen sizing mode: {sizing.mode} ({VALID_SIZING_MODES})")

    if sizing.mode == "from_strategy":
        raw = float(strategy_size)

    elif sizing.mode == "fixed_lot":
        raw = float(sizing.fixed_lot_size)

    elif sizing.mode == "score_scaled":
        s = float(score) if score is not None else 0.5
        s = max(0.0, min(1.0, s))
        span = sizing.max_order_size - sizing.min_order_size
        raw = sizing.min_order_size + span * s

    else:  # fixed_risk
        sl = float(sl_distance) if sl_distance else 0.0
        if sl <= 0:
            return 0.0
        risk_dollars = sizing.equity * sizing.risk_pct
        raw = risk_dollars / sl

    return max(sizing.min_order_size, min(sizing.max_order_size, raw))

"""
Élő pozíció + manuális trailing logika.

Megegyező szemantika a backtest engine/position.py-vel, de itt az SL-mozgatás
visszaadja az új abszolút SL-szintet (a runner ezt REST PUT-tal továbbküldi).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Optional

import pandas as pd


@dataclass
class LivePosition:
    trade_id: int
    deal_id: str
    deal_ref: str
    decision_event_id: str
    direction: str       # "BUY" | "SELL"
    entry_ts: pd.Timestamp
    entry_price: float
    size: float
    sl: float            # aktuális SL szint (mozgatható trailing-gel)
    tp: Optional[float]

    initial_sl: float
    atr_at_open: Optional[float]
    open_event_id: Optional[str] = None
    open_indicators: Dict[str, float] = field(default_factory=dict)
    break_even_done: bool = False

    # Layered TP — egy signal-ból több réteg-pozíció
    parent_trade_id: Optional[int] = None
    layer_idx: int = 0
    layer_count: int = 1
    layer_tp_pct: float = 1.0
    layer_size_pct: float = 1.0

    # Reconcile state — "ghost close" elkerülésére
    verified_open: bool = False            # legalább egyszer láttuk a Capital REST-en
    missing_poll_count: int = 0            # hányszor egymás után hiányzott

    # Idő-alapú kényszer-zárás (session-stratégiák, pl. london_breakout)
    exit_at_ts: Optional[pd.Timestamp] = None

    exit_ts: Optional[pd.Timestamp] = None
    exit_price: Optional[float] = None
    exit_reason: Optional[str] = None
    pnl: float = 0.0


def apply_trailing(
    pos: LivePosition,
    bid: float,
    ask: float,
    trailing_mode: str,
    trail_atr_mult: float,
    break_even_trigger_atr_mult: float,
) -> Optional[float]:
    """
    Visszaadja az ÚJ SL szintet, ha trailing változott. None → nincs változás.
    Megegyező szemantika a backtest position._apply_trailing-jével.
    """
    if trailing_mode == "none" or pos.atr_at_open is None or pos.atr_at_open <= 0:
        return None

    if pos.direction == "BUY":
        profit = bid - pos.entry_price
        atr_v = pos.atr_at_open
        if trailing_mode == "break_even" and not pos.break_even_done:
            if profit >= break_even_trigger_atr_mult * atr_v:
                new_sl = max(pos.sl, pos.entry_price)
                if new_sl != pos.sl:
                    pos.break_even_done = True
                    return new_sl
        elif trailing_mode == "atr_trail":
            new_sl = bid - trail_atr_mult * atr_v
            if new_sl > pos.sl:
                if not pos.break_even_done and new_sl >= pos.entry_price:
                    pos.break_even_done = True
                return new_sl
    else:  # SELL
        profit = pos.entry_price - ask
        atr_v = pos.atr_at_open
        if trailing_mode == "break_even" and not pos.break_even_done:
            if profit >= break_even_trigger_atr_mult * atr_v:
                new_sl = min(pos.sl, pos.entry_price)
                if new_sl != pos.sl:
                    pos.break_even_done = True
                    return new_sl
        elif trailing_mode == "atr_trail":
            new_sl = ask + trail_atr_mult * atr_v
            if new_sl < pos.sl:
                if not pos.break_even_done and new_sl <= pos.entry_price:
                    pos.break_even_done = True
                return new_sl

    return None

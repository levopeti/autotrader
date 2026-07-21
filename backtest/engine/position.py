from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Optional

import pandas as pd

from ..strategies.base import Direction


@dataclass
class Position:
    id: int
    decision_event_id: str
    open_event_id: Optional[str]
    direction: Direction
    entry_ts: pd.Timestamp
    entry_price: float
    size: float
    sl: float
    tp: Optional[float]
    score: float
    open_indicators: Dict[str, float] = field(default_factory=dict)
    open_note: str = ""

    initial_sl: Optional[float] = None         # az eredeti SL, trailing logika a sl-t mozgatja
    atr_at_open: Optional[float] = None        # trailing kalkulációhoz
    break_even_done: bool = False              # break_even módban: már mozgott-e BE-re

    exit_ts: Optional[pd.Timestamp] = None
    exit_price: Optional[float] = None
    exit_reason: Optional[str] = None
    pnl: float = 0.0

    @property
    def is_open(self) -> bool:
        return self.exit_ts is None

    @property
    def hold_seconds(self) -> Optional[float]:
        if self.exit_ts is None:
            return None
        return (self.exit_ts - self.entry_ts).total_seconds()


def calc_pnl(direction: Direction, entry_price: float, exit_price: float, size: float) -> float:
    if direction == "BUY":
        return (exit_price - entry_price) * size
    return (entry_price - exit_price) * size


def check_exit(
    pos: Position,
    bid: float,
    ask: float,
    ts: pd.Timestamp,
    max_hold_seconds: Optional[float] = None,
    trailing_mode: str = "none",
    trail_atr_mult: float = 1.5,
    break_even_trigger_atr_mult: float = 1.0,
) -> Optional[tuple[float, str]]:
    """
    Visszaad (exit_price, reason) ha a pozíció zárandó, különben None.
    BUY-nál a bid-en zárunk (eladunk), SELL-nél az ask-en (visszavásárolunk).

    trailing_mode:
      - "none": fix SL
      - "break_even": ha profit >= break_even_trigger_atr_mult × ATR, SL → entry
      - "atr_trail": folyamatos trail: SL nem lehet messzebb mint trail_atr_mult × ATR
                     a jelenlegi piac-ártól, de csak nyereség irányba mozgatható
    """
    _apply_trailing(pos, bid, ask, trailing_mode, trail_atr_mult, break_even_trigger_atr_mult)

    if pos.direction == "BUY":
        if bid <= pos.sl:
            reason = "BE_STOP" if pos.break_even_done and abs(pos.sl - pos.entry_price) < 1e-6 else "SL"
            return pos.sl, reason
        if pos.tp is not None and bid >= pos.tp:
            return pos.tp, "TP"
    else:
        if ask >= pos.sl:
            reason = "BE_STOP" if pos.break_even_done and abs(pos.sl - pos.entry_price) < 1e-6 else "SL"
            return pos.sl, reason
        if pos.tp is not None and ask <= pos.tp:
            return pos.tp, "TP"

    if max_hold_seconds is not None:
        held = (ts - pos.entry_ts).total_seconds()
        if held >= max_hold_seconds:
            exit_price = bid if pos.direction == "BUY" else ask
            return exit_price, "TIMEOUT"

    return None


def _apply_trailing(
    pos: Position,
    bid: float,
    ask: float,
    mode: str,
    trail_atr_mult: float,
    be_trigger_atr_mult: float,
) -> None:
    if mode == "none" or pos.atr_at_open is None or pos.atr_at_open <= 0:
        return

    if pos.direction == "BUY":
        profit = bid - pos.entry_price
        atr_v = pos.atr_at_open
        if mode == "break_even" and not pos.break_even_done:
            if profit >= be_trigger_atr_mult * atr_v:
                pos.sl = max(pos.sl, pos.entry_price)
                pos.break_even_done = True
        elif mode == "atr_trail":
            new_sl = bid - trail_atr_mult * atr_v
            if new_sl > pos.sl:
                pos.sl = new_sl
                if not pos.break_even_done and new_sl >= pos.entry_price:
                    pos.break_even_done = True
    else:
        profit = pos.entry_price - ask
        atr_v = pos.atr_at_open
        if mode == "break_even" and not pos.break_even_done:
            if profit >= be_trigger_atr_mult * atr_v:
                pos.sl = min(pos.sl, pos.entry_price)
                pos.break_even_done = True
        elif mode == "atr_trail":
            new_sl = ask + trail_atr_mult * atr_v
            if new_sl < pos.sl:
                pos.sl = new_sl
                if not pos.break_even_done and new_sl <= pos.entry_price:
                    pos.break_even_done = True
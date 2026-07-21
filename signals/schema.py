"""
Normalizált jel-formátum, amit minden parser visszaad.

A ParsedSignal-ben minden szám float, nincs string-kódolt érték — backtest, archive
és live oldal mind ezt használja.
"""
from __future__ import annotations

from dataclasses import dataclass, field, asdict
from typing import List, Optional


@dataclass
class ParsedSignal:
    valid: bool
    reason: str                       # "ok" vagy parse hiba-kód
    direction: Optional[str] = None   # "BUY" | "SELL"
    entry_low: Optional[float] = None
    entry_high: Optional[float] = None
    tp_list: List[float] = field(default_factory=list)
    sl: Optional[float] = None
    parser: str = ""                  # melyik parser produkálta (regex|llm|…)

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def invalid(cls, reason: str, parser: str = "") -> "ParsedSignal":
        return cls(valid=False, reason=reason, parser=parser)

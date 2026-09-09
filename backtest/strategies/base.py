from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Dict, List, Literal, Optional

import pandas as pd


Direction = Literal["BUY", "SELL"]


@dataclass
class Decision:
    """
    Minden tick-szintű döntés rekordja — akkor is, amikor NEM nyit pozíciót.
    A logger ebből építi a decisions.csv-t és a "miért nem" elemzést.
    """
    ts: pd.Timestamp
    allow_trade: bool
    reason: str
    direction: Optional[Direction] = None
    score: Optional[float] = None
    size: Optional[float] = None
    sl_distance: Optional[float] = None
    tp_distance: Optional[float] = None
    indicators: Dict[str, float] = field(default_factory=dict)
    # Multi-TP: ha a stratégia abszolút TP-távolságok listáját adja vissza,
    # a runner ezekből rétegezett pozíciókat nyit közös SL-lel.
    # Felülírja az engine.tp_layers-t. None → egyetlen tp_distance, régi mód.
    tp_distances: Optional[List[float]] = None
    # TP-ladder trailing: ha az engine.trailing_mode == "tp_ladder", akkor
    # ezeket a (pozitív) ár-távolságokat használja az SL felhúzásához.
    # Logika: amint a piac a trigger-szintet eléri (favourable irányban),
    # az SL ide kerül: dest_price (a destination-távolságon).
    ladder_trigger_distance: Optional[float] = None
    ladder_dest_distance: Optional[float] = None
    # Idő-alapú exit: ha megadott, a pozíció ebben az abszolút (UTC-naive)
    # időpontban kényszer-zár ("TIME_EXIT") — session-alapú stratégiákhoz
    # (pl. london_breakout: 20:00 UTC session-zárás).
    exit_at_ts: Optional[pd.Timestamp] = None


@dataclass
class Signal:
    """Nyitási parancs: a Decision-ből épül akkor, ha allow_trade=True."""
    ts: pd.Timestamp
    direction: Direction
    size: float
    sl_distance: float
    tp_distance: Optional[float] = None
    score: float = 0.0
    indicators: Dict[str, float] = field(default_factory=dict)
    note: str = ""
    # Multi-TP: ha a stratégia abszolút TP-távolságok listáját adja vissza,
    # a runner ezeket rétegekre bontja (felülírva az engine.tp_layers-t).
    tp_distances: Optional[List[float]] = None
    # Lásd Decision.ladder_*
    ladder_trigger_distance: Optional[float] = None
    ladder_dest_distance: Optional[float] = None


@dataclass
class StrategyContext:
    """A motor adja a stratégiának szegmens kezdéskor."""
    epic: str
    segment_start: pd.Timestamp
    segment_end: pd.Timestamp
    candles_mtf: Dict[str, pd.DataFrame]
    segment_ticks: Optional[pd.DataFrame] = None


class Strategy(ABC):
    name: str = "base"

    def __init__(self, params: dict):
        self.params = params

    @abstractmethod
    def required_timeframes(self) -> List[str]:
        """Mely candle TF-eket kéri a motor (pl. ['5min', '1h'])."""

    @abstractmethod
    def on_segment_start(self, ctx: StrategyContext) -> None:
        """Új szegmens — itt szokás indikátorokat előre kiszámolni."""

    @abstractmethod
    def on_tick(self, ts: pd.Timestamp, bid: float, ask: float) -> Optional[Decision]:
        """
        Egy tick. Visszaad Decision-t akkor, ha érdemleges (potenciális belépés,
        szűrő-letiltás, near-miss). None → nincs log. Ha allow_trade=True, a motor
        a Decision-ből Signal-t épít és nyit.
        """

    def on_segment_end(self) -> None:
        pass

    def on_position_closed(self, position) -> None:
        """A motor minden trade-zárás után meghívja. Default: no-op.
        A stratégia ebből pl. cooldown-t indíthat SL után."""
        pass

    def refresh_candles(self, candles_mtf: Dict[str, pd.DataFrame]) -> None:
        """
        Csak az indikátor-arrays-t frissíti — a tick state érintetlen marad.
        A LIVE runner periodikusan hívja, ha új candle érkezett. A backtest
        nem hívja (a candle-history globálisan, segmentskezdéskor épül fel).
        Default: no-op; a stratégia felülírja ha élesben fut.
        """
        pass
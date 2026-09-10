"""
Broker-abstraction: az élő runner ezen a rétegen keresztül beszél a brókerrel.

A `LiveRunner` csak a `BrokerClient` protocol metódusait hívja, és a
NORMALIZÁLT adatosztályokat (`OpenPosition`, `Confirm`, `ClosingTx`) olvassa —
így ugyanaz a stratégia-motor bármelyik broker-implementáción fut (Capital,
eToro, …), a broker-specifikus payload-particulárok a Broker-osztályon belül
maradnak.

Egy új broker implementálásához:
  1) subclass a `BrokerClient`-től (vagy csak matched-metódusú osztályt írj —
     a runner Protocol-t vár, structural typing van)
  2) töltsd fel a `_to_open_position` / `_to_confirm` / `_to_closing_tx`
     megfelelő normalizáló-fügvényeket
  3) registráld a `broker_factory.make_broker()`-ben
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import AsyncGenerator, Dict, List, Optional, Protocol

import pandas as pd


# ── Normalizált adatosztályok ─────────────────────────────────────────────────

@dataclass
class OpenPosition:
    """Broker-oldali nyitott pozíció normalizált nézete."""
    deal_id: str                    # egyedi pozíció-id a brókernél
    deal_reference: Optional[str]   # a POST-nál megadott / kapott client-referencia
    epic: str
    direction: str                  # "BUY" | "SELL"
    entry_price: float
    size: float
    stop_level: Optional[float]
    profit_level: Optional[float]
    created_utc: Optional[str]      # ISO string
    upl: Optional[float] = None
    currency: Optional[str] = None
    raw: Optional[dict] = None      # nyers payload, ha kellene diagnosztizálni


@dataclass
class Confirm:
    """POST /positions confirmation normalizálva."""
    accepted: bool                  # True = broker megnyitotta a pozíciót
    deal_id: Optional[str]          # a MEGNYITOTT pozíció deal_id-je (nem az order-id!)
    deal_reference: Optional[str]
    level: Optional[float]          # tényleges entry-ár
    status: str = ""                # "OPEN" / "REJECTED" / "PENDING" / broker-specifikus
    reason: str = ""                # ha rejected: broker-oldali ok
    raw: Optional[dict] = None


@dataclass
class ClosingTx:
    """Lezárult pozíció tranzakciója: PnL + close-level."""
    pnl: Optional[float]
    close_level: Optional[float]
    currency: Optional[str] = None
    raw: Optional[dict] = None


# ── A protocol, amit a LiveRunner elvár ───────────────────────────────────────

class BrokerClient(Protocol):
    """Structural typing — bármilyen osztály jó, ha ezt a felületet tudja."""

    # --- Session / account ---
    def ensure_login(self) -> None: ...
    def ensure_account(self) -> None: ...
    def resolve_epic(self, symbol: str) -> str: ...
    def get_balance(self, account_id: Optional[str] = None) -> Optional[float]: ...

    # --- Piaci adat ---
    def get_prices(self, epic: str, resolution: str, max_points: int) -> pd.DataFrame:
        """OHLCV dataframe, oszlopok: timestamp (tz-naive UTC), open, high, low, close, volume."""
        ...

    def stream_ticks(self, epic: str) -> AsyncGenerator[dict, None]:
        """Async generator: {ts, bid, ask, source_ts, raw}."""
        ...

    # --- Pozíciók ---
    def get_open_positions(self) -> List[OpenPosition]: ...

    def create_position(
        self, epic: str, direction: str, size: float,
        stop_distance: float, profit_distance: Optional[float] = None,
        trailing_stop: bool = False,
    ) -> dict:
        """Nyers POST-válasz — utána a runner confirm_position_with_retry-t hív."""
        ...

    def confirm_position_with_retry(
        self, deal_reference: str, attempts: int = 5, delay_seconds: float = 0.5,
    ) -> Confirm: ...

    def update_position(
        self, deal_id: str,
        stop_level: Optional[float] = None,
        profit_level: Optional[float] = None,
    ) -> dict: ...

    def close_position(self, deal_id: str) -> dict: ...

    def get_transaction_for_deal(
        self, deal_id: str, last_period_sec: int = 86400,
    ) -> Optional[ClosingTx]: ...


# ── TF → resolution mapping — a brókerek eltérően nevezik ────────────────────
# A brókerek saját osztályuk `TF_TO_RESOLUTION` konstansát exportálják;
# a runner ott találja meg.

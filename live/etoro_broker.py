"""
EtoroBroker — a BrokerClient protocol eToro (Builders API) implementációja.

**STÁTUSZ (2026-09-10): SKELETON — az API-kulcs megérkezéséig.**

Amit a nyilvános dokumentációból (builders.etoro.com, api-portal.etoro.com)
biztosan tudunk:
  - REST base:  https://api.etoro.com/... + /demo/... a sandbox-hoz
  - WebSocket:  wss://api.etoro.com/... (real-time árstream)
  - Auth:       API-kulcs (X-API-KEY vagy hasonló header) + token flow
  - Demo/sandbox natívan támogatott — `/execution/demo/` prefix a trading-hívásokban
  - 176+ REST endpoint, "positions", "orders", "accounts", "instruments"

Amit az API-kulcs után kell finomítani (a valós doksi alapján):
  - pontos endpoint-URL-ek + payload-struktúrák (az itteni implementáció
    a Capital-mintát követi, hogy a mezők NORMALIZÁLVA legyenek)
  - candle-adat felbontás-elnevezései (TF_TO_RESOLUTION)
  - trading unit: eToro "units"-ban dolgozik, nem "lot"-ban — a
    normalizálást a create_position() `size` paraméterében kell elvégezni
    (config-oldali size-t egységes marad)
  - USDJPY / GOLD instrumentum-ID-k (eToro-n gyakran numerikus instrumentId)

Ha kulcs megvan, a "TODO"-jelzett részeket kell konkretizálni; a normalizált
return-osztályok (`OpenPosition`, `Confirm`, `ClosingTx`) változatlanok, így a
LiveRunner minden módosítás nélkül elfogadja.
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional

import pandas as pd
import requests

from .broker import BrokerClient, ClosingTx, Confirm, OpenPosition

logger = logging.getLogger("live.etoro_broker")


# ── TF-mapping (a valós doksi után finomítandó) ──
# eToro nyilvános docs alapján a candle-endpointok `resolution`-je feltehetően:
#   1m, 5m, 15m, 30m, 1h, 4h, 1D  — de a valós elnevezést kulcs után igazoljuk.
TF_TO_RESOLUTION = {
    "1min": "1m", "5min": "5m", "15min": "15m", "30min": "30m",
    "1h": "1h", "4h": "4h", "1d": "1D",
}

_BASE_URL_PROD = "https://api.etoro.com"     # TODO: a builders portál pontos URL-je
_WS_URL         = "wss://api.etoro.com"      # TODO


def _need_key() -> "NotImplementedError":
    return NotImplementedError(
        "EtoroBroker: API-kulcs + doksi hiányzik. Amint az `etoro_keys.json` "
        "és a builders-portál részletes doksi megvan, a TODO-részek "
        "1-2 óra alatt implementálhatók."
    )


class EtoroBroker(BrokerClient):
    """Skeleton — a TODO-jelzett metódusok az API-kulcs után élnek."""

    def __init__(self, demo: bool = True, account_id: Optional[str] = None,
                 keys_path: str = "etoro_keys.json"):
        try:
            cfg = json.loads(open(keys_path).read())
        except FileNotFoundError:
            cfg = {}
        self.api_key: Optional[str] = cfg.get("etoro_api_key")
        self.api_secret: Optional[str] = cfg.get("etoro_api_secret")   # ha kell (OAuth flow)
        self.account_id: Optional[str] = account_id or cfg.get("etoro_account_id")
        self.demo = demo
        self._prefix = "/execution/demo" if demo else "/execution"
        self.session = requests.Session()
        # TODO: pontos header-név (X-API-KEY vs Authorization: Bearer) a doksiból
        if self.api_key:
            self.session.headers.update({"X-API-KEY": self.api_key,
                                          "Content-Type": "application/json"})
        self._token: Optional[str] = None

    # ── Session ──

    def ensure_login(self) -> None:
        if not self.api_key:
            raise _need_key()
        # TODO: az eToro auth-flow (API-kulcs → token exchange, ha kell). A
        # nyilvános infó szerint API-kulcs elég a legtöbb végponthoz; ha van
        # rövid életű token, itt frissítjük hasonlóan a CapitalClient-hez.
        logger.info("EtoroBroker: login OK (demo=%s)", self.demo)

    def ensure_account(self) -> None:
        if not self.account_id:
            return
        # TODO: /accounts endpoint → account-switch, ha eToro-n többfajta account van

    def resolve_epic(self, symbol: str) -> str:
        # TODO: /instruments?search=<symbol> → instrumentId
        # Egyelőre visszaadjuk a symbolt (a strategia-config epic-ként dolgozza fel)
        raise _need_key()

    def get_balance(self, account_id: Optional[str] = None) -> Optional[float]:
        # TODO: GET /accounts/{id} → balance
        raise _need_key()

    # ── Piaci adat ──

    def get_prices(self, epic: str, resolution: str, max_points: int) -> pd.DataFrame:
        # TODO: GET /marketdata/candles?instrumentId=<epic>&resolution=<r>&count=<n>
        # Cél: ugyanolyan DataFrame mint a CapitalClient-nél
        #   oszlopok: timestamp (tz-naive UTC), open, high, low, close, volume
        raise _need_key()

    async def stream_ticks(self, epic: str):
        # TODO: WebSocket subscribe (a Capital-mintához hasonlóan async generator,
        # amely {ts, bid, ask, source_ts, raw} dictionary-t yield-el).
        # Auto-reconnect + ping.
        raise _need_key()
        # A protocol miatt kell async generator-forma; a raise nem éri el:
        yield  # pragma: no cover

    # ── Pozíciók ──

    def get_open_positions(self) -> List[OpenPosition]:
        # TODO: GET /positions → lista → mindegyiket OpenPosition-ré alakítani
        # (deal_id, direction, entry_price, size, stop_level, profit_level, created_utc)
        raise _need_key()

    def create_position(self, epic: str, direction: str, size: float,
                        stop_distance: float, profit_distance: Optional[float] = None,
                        trailing_stop: bool = False) -> dict:
        # TODO: POST /positions
        #   Fontos: eToro "units"-ban vagy USD-notionalban dolgozhat, nem lot-ban!
        #   A `size` konverziót itt kell elvégezni, hogy a strategia-config
        #   NE változzon broker-váltáskor.
        raise _need_key()

    def confirm_position_with_retry(self, deal_reference: str,
                                     attempts: int = 5,
                                     delay_seconds: float = 0.5) -> Confirm:
        # TODO: eToro válasza a POST /positions-re valószínűleg SZINKRON —
        #   nincs a Capital-féle /confirms race. Ekkor egyszerű return:
        #     return Confirm(accepted=True, deal_id=response["positionId"],
        #                    deal_reference=deal_reference,
        #                    level=response["openRate"], status="OPEN", raw=response)
        raise _need_key()

    def update_position(self, deal_id: str,
                        stop_level: Optional[float] = None,
                        profit_level: Optional[float] = None) -> dict:
        # TODO: PATCH/PUT /positions/{id} — SL/TP mozgatás abszolút szintekkel
        raise _need_key()

    def close_position(self, deal_id: str) -> dict:
        # TODO: DELETE /positions/{id}
        raise _need_key()

    def get_transaction_for_deal(self, deal_id: str,
                                  last_period_sec: int = 86400) -> Optional[ClosingTx]:
        # TODO: GET /history/transactions?positionId=<id> → PnL + close-level
        # Normalizálás: ClosingTx(pnl=..., close_level=..., currency=...)
        raise _need_key()

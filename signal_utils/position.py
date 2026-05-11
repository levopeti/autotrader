import asyncio
import aiohttp
import logging

from datetime import datetime, timezone
from dataclasses import dataclass
from enum import Enum
from typing import Optional

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

POSITION_TIMEOUT_SEC = 15 * 60
TP_DIST_MAX = 50
SL_DIST_MAX = 50


class Direction(str, Enum):
    BUY = "BUY"
    SELL = "SELL"


class PositionState(str, Enum):
    WAITING  = "WAITING"
    OPENING  = "OPENING"
    OPEN     = "OPEN"
    FILLED   = "FILLED"    # TP vagy SL elérve
    CANCELED = "CANCELED"  # Manuális
    EXPIRED  = "EXPIRED"   # 15 perces timeout
    REJECTED = "REJECTED"  # Capital.com API visszautasította
    ERROR    = "ERROR"


@dataclass
class PositionConfig:
    epic:      str
    direction: Direction
    size:      float
    zone_low:  float
    zone_high: float
    tp:        float
    sl:        float
    tp_idx:    int
    send_date: str
    edited:    bool
    chat_id:   int
    raw_text:  str
    chat_name: str


# ─── POSITION ────────────────────────────────────────────────────────────────

class Position:
    def __init__(self, config: PositionConfig, base_url: str,
                 cst: str, token: str, manager: "PositionManager"):
        self.config    = config
        self.base_url  = base_url
        self.cst       = cst
        self.token     = token
        self._manager  = manager
        self.state     = PositionState.WAITING

        self.deal_id:       Optional[str]   = None
        self.deal_ref:      Optional[str]   = None
        self.open_level:    Optional[float] = None
        self.current_level: Optional[float] = None
        self.limit_level:   Optional[float] = None
        self.stop_level:    Optional[float] = None
        self.close_level:   Optional[float] = None
        self.close_reason:  Optional[str]   = None
        self.profit_loss:   Optional[float] = None
        self.realised_pnl:  Optional[float] = None
        self.currency:      Optional[str]   = None
        self.created_date:  Optional[str]   = None

        self.registered_at  = datetime.now(timezone.utc)
        self.opened_at:    Optional[datetime] = None
        self.closed_at:    Optional[datetime] = None
        self.expired_at:   Optional[datetime] = None
        self.last_poll_at: Optional[datetime] = None

        self.error_msg: Optional[str] = None

        self._queue: asyncio.Queue[tuple[float, float]] = asyncio.Queue()

    # ── Nyilvános ─────────────────────────────────────────────────────────────

    def init_check(self):
        mid = (self.config.zone_low + self.config.zone_high) / 2
        tp_dist = abs(self.config.tp - mid)
        sl_dist = abs(self.config.sl - mid)

        if tp_dist > TP_DIST_MAX or sl_dist > SL_DIST_MAX:
            self.state     = PositionState.ERROR
            self.error_msg = "tp/sl dist error"
            logger.error("[POS] tp/sl dist error: %s", self)
            self._manager.csv_update_terminal(self)

    def start(self) -> asyncio.Task:
        return asyncio.create_task(self._monitor_loop())

    def on_price(self, bid: float, ask: float) -> None:
        if self.state == PositionState.WAITING:
            self._queue.put_nowait((bid, ask))

    def cancel(self) -> bool:
        if self.state == PositionState.WAITING:
            self.state        = PositionState.CANCELED
            self.realised_pnl = 0.0
            return True
        return False

    def is_terminal(self) -> bool:
        return self.state in (
            PositionState.FILLED,
            PositionState.CANCELED,
            PositionState.EXPIRED,
            PositionState.REJECTED,
            PositionState.ERROR,
        )

    def to_csv_row(self) -> dict:
        return {
            "log_time":       datetime.now(timezone.utc).isoformat(),
            "deal_id":        self.deal_id or "",
            "deal_reference": self.deal_ref or "",
            "epic":           self.config.epic,
            "direction":      self.config.direction.value,
            "size":           self.config.size,
            "state":          self.state.value,
            "zone_low":       self.config.zone_low,
            "zone_high":      self.config.zone_high,
            "tp_configured":  self.config.tp,
            "sl_configured":  self.config.sl,
            "open_level":     self.open_level    if self.open_level    is not None else "",
            "current_level":  self.current_level if self.current_level is not None else "",
            "limit_level":    self.limit_level   if self.limit_level   is not None else "",
            "stop_level":     self.stop_level    if self.stop_level    is not None else "",
            "profit_loss":    self.profit_loss   if self.profit_loss   is not None else "",
            "realised_pnl":   self.realised_pnl  if self.realised_pnl  is not None else "",
            "currency":       self.currency      or "",
            "created_date":   self.created_date  or "",
            "close_level":    self.close_level   if self.close_level   is not None else "",
            "close_reason":   self.close_reason  or "",
            "registered_at":  self.registered_at.isoformat(),
            "opened_at":      self.opened_at.isoformat()    if self.opened_at    else "",
            "closed_at":      self.closed_at.isoformat()    if self.closed_at    else "",
            "expired_at":     self.expired_at.isoformat()   if self.expired_at   else "",
            "last_poll_at":   self.last_poll_at.isoformat() if self.last_poll_at else "",
            "error_msg":      self.error_msg or "",
            "raw_text":       self.config.raw_text,
            "send_date":      self.config.send_date,
            "edited":         self.config.edited,
            "chat_id":        self.config.chat_id,
            "chat_name":      self.config.chat_name,
            "tp_idx":         self.config.tp_idx,
        }

    def __repr__(self):
        return (
            f"<Position {self.config.direction.value} {self.config.epic} {self.state.value} "
            f"entry: {self.config.zone_low}-{self.config.zone_high} "
            f"tp: {self.config.tp} sl: {self.config.sl} size: {self.config.size}>"
        )

    # ── Belső logika ──────────────────────────────────────────────────────────

    async def _monitor_loop(self) -> None:
        deadline = self.registered_at.timestamp() + POSITION_TIMEOUT_SEC
        log_once = False
        try:
            while self.state == PositionState.WAITING:
                remaining = deadline - datetime.now(timezone.utc).timestamp()
                if remaining <= 0:
                    self.state        = PositionState.EXPIRED
                    self.expired_at   = datetime.now(timezone.utc)
                    self.realised_pnl = 0.0
                    logger.info("[POS] Lejárt: %s", self)
                    self._manager.csv_update_terminal(self)
                    break

                try:
                    bid, ask = await asyncio.wait_for(
                        self._queue.get(), timeout=min(remaining, 5.0)
                    )
                except asyncio.TimeoutError:
                    continue

                trigger = ask if self.config.direction == Direction.BUY else bid
                if self.config.zone_low <= trigger <= self.config.zone_high:
                    if not self._manager.can_open(self.config.chat_id):
                        if not log_once:
                            logger.info(
                                "[POS] ⏸ Zone elérve, limit betelt (%d/%d) — várakozás...",
                                self._manager.open_count(),
                                self._manager.max_open,
                            )
                            log_once = True
                        continue
                    logger.info("[POS] Zone elérve %.5f → nyitás...", trigger)
                    await self._open_position()
                    break
        except asyncio.CancelledError:
            pass

    async def _open_position(self) -> None:
        self.state     = PositionState.OPENING
        self.opened_at = datetime.now(timezone.utc)
        headers = {
            "CST": self.cst, "X-SECURITY-TOKEN": self.token,
            "Content-Type": "application/json",
        }
        body = {
            "epic":           self.config.epic,
            "direction":      self.config.direction.value,
            "size":           self.config.size,
            "guaranteedStop": False,
            "profitLevel":    self.config.tp,
            # "stopLevel":      self.config.sl,
            "trailingStop":   True,
            "stopDistance":   abs(self.config.sl - (self.config.zone_high + self.config.zone_low) / 2),
        }
        try:
            async with aiohttp.ClientSession() as s:
                async with s.post(
                    f"{self.base_url}/api/v1/positions",
                    headers=headers, json=body
                ) as r:
                    data = await r.json()
                    if r.status not in (200, 201):
                        raise RuntimeError(f"HTTP {r.status}: {data}")
                    self.deal_ref = data.get("dealReference")
                    logger.info(
                        "[POS] ✅ Nyitási kérés OK | ref:%s | %s %s | TP:%.2f SL:%.2f",
                        self.deal_ref, self.config.direction.value,
                        self.config.epic, self.config.tp, self.config.sl,
                    )
                    # Confirm lekérés — ez állítja OPEN-re vagy REJECTED-re
                    asyncio.create_task(self._fetch_confirm())
        except Exception as e:
            self.state     = PositionState.ERROR
            self.error_msg = str(e)
            logger.error("[POS] ❌ Nyitási hiba: %s", e)
            self._manager.csv_update_terminal(self)

    async def _fetch_confirm(self) -> None:
        """
        Lekéri a /confirms/{dealReference} végpontot és ellenőrzi a dealStatus-t.
        - ACCEPTED + valós level → OPEN állapot, nyitóár beállítva
        - REJECTED / level == 0 → REJECTED állapot, azonnal terminál
        - 00000000 kezdetű dealId → szintén REJECTED
        """
        await asyncio.sleep(1)
        headers = {"CST": self.cst, "X-SECURITY-TOKEN": self.token}

        for attempt in range(5):
            try:
                async with aiohttp.ClientSession() as s:
                    async with s.get(
                        f"{self.base_url}/api/v1/confirms/{self.deal_ref}",
                        headers=headers
                    ) as r:
                        data = await r.json()

                deal_status = data.get("dealStatus", "")
                deal_id     = data.get("dealId", "")
                reason      = data.get("reason", "ismeretlen")
                level       = data.get("level")

                # ── REJECTED ellenőrzés ────────────────────────────────────
                is_null_uuid = deal_id.startswith("00000000-0000-0000")
                is_rejected  = (
                    deal_status == "REJECTED"
                    or is_null_uuid
                    or level is None
                    or level == 0
                )

                if is_rejected:
                    self.deal_id      = deal_id
                    self.state        = PositionState.REJECTED
                    self.close_reason = f"REJECTED:{reason}"
                    self.realised_pnl = 0.0
                    logger.warning(
                        "[POS] ❌ REJECTED | dealId:%s | reason:%s | %s",
                        deal_id, reason, self,
                    )
                    self._manager.csv_update_terminal(self)
                    return

                # ── ACCEPTED ──────────────────────────────────────────────
                self.deal_id     = deal_id
                self.open_level  = self.open_level or level
                self.limit_level = data.get("limitLevel")
                self.stop_level  = data.get("stopLevel")
                self.state       = PositionState.OPEN
                logger.info(
                    "[POS] Confirm | dealId:%s | nyitóár:%.5f",
                    self.deal_id, self.open_level,
                )
                return

            except Exception as e:
                logger.warning("[POS] Confirm hiba (%d. kísérlet): %s", attempt + 1, e)
                await asyncio.sleep(2 ** attempt)

        # 5 sikertelen kísérlet után ERROR-ba kerül
        logger.error("[POS] Confirm végleg sikertelen: %s", self)
        self.state     = PositionState.ERROR
        self.error_msg = "confirm_timeout"
        self._manager.csv_update_terminal(self)

    def apply_rest_data(self, api_pos: dict) -> None:
        pos = api_pos.get("position", {})
        mkt = api_pos.get("market", {})
        self.current_level = mkt.get("bid") or mkt.get("offer")
        self.open_level    = self.open_level or pos.get("openLevel")
        self.limit_level   = pos.get("limitLevel")
        self.stop_level    = pos.get("stopLevel")
        self.profit_loss   = pos.get("upl")
        self.currency      = pos.get("currency")
        self.created_date  = pos.get("createdDateUTC") or pos.get("createdDate")
        self.last_poll_at  = datetime.now(timezone.utc)

    def apply_opu(self, payload: dict) -> None:
        status = payload.get("status")
        if status == "DELETED":
            self.state        = PositionState.FILLED
            self.close_level  = payload.get("level")
            self.close_reason = payload.get("closeReason", "UNKNOWN")
            self.closed_at    = datetime.now(timezone.utc)
            logger.info("[POS] Zárva (%s) | %s | záróár:%s",
                        self.close_reason, self, self.close_level)
            asyncio.create_task(self._fetch_transactions_and_log())
        elif status == "OPEN":
            self.current_level = payload.get("level")
            self.limit_level   = payload.get("limitLevel")
            self.stop_level    = payload.get("stopLevel")
            self.last_poll_at  = datetime.now(timezone.utc)

    async def _fetch_transactions_and_log(self) -> None:
        """
        Transactions végpontról lekéri az összes záráskori adatot,
        majd frissíti a CSV sort.
        """
        await asyncio.sleep(2)
        headers = {"CST": self.cst, "X-SECURITY-TOKEN": self.token}
        params  = {"dealId": self.deal_id, "lastPeriod": 86400}

        for attempt in range(5):
            try:
                async with aiohttp.ClientSession() as s:
                    async with s.get(
                        f"{self.base_url}/api/v1/history/transactions",
                        headers=headers, params=params
                    ) as r:
                        data  = await r.json()
                        items = data.get("transactions", [])

                        if not items:
                            logger.debug("[CLOSE] Transactions üres, %d. kísérlet...", attempt + 1)
                            await asyncio.sleep(2 ** attempt)
                            continue

                        t = items[0]
                        self.open_level   = self.open_level  or t.get("openLevel")
                        self.close_level  = self.close_level or t.get("closeLevel")
                        self.realised_pnl = t.get("profitAndLoss")
                        self.currency     = self.currency    or t.get("currency")

                        logger.info(
                            "[CLOSE] ✅ | open:%.5f close:%.5f pnl:%s %s",
                            self.open_level  or 0,
                            self.close_level or 0,
                            self.realised_pnl,
                            self.currency or "",
                        )
                        self._manager.csv_update_terminal(self)
                        return

            except Exception as e:
                logger.warning("[CLOSE] Transactions hiba (%d. kísérlet): %s", attempt + 1, e)
                await asyncio.sleep(2)

        # Max kísérlet után is írjuk ami megvan
        logger.warning("[CLOSE] Transactions után hiányos adatok: %s", self)
        self._manager.csv_update_terminal(self)
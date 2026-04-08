import asyncio
import json
import aiohttp
import websockets
import zmq
import zmq.asyncio
from datetime import datetime, timezone
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional
import logging
import csv
import os

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

with open('keys_urls.json', 'r') as f:
    config = json.load(f)

API_KEY    = config["capital_api_key"]
IDENTIFIER = config["capital_login"]
PASSWORD   = config["capital_pw"]
DEMO_MODE  = True

BASE_URL = (
    "https://demo-api-capital.backend-capital.com"
    if DEMO_MODE else
    "https://api-capital.backend-capital.com"
)
WS_URL        = "wss://api-streaming-capital.backend-capital.com/connect"
EPIC          = "GOLD"
ZMQ_PULL_ADDR = "tcp://localhost:5555"

MAX_OPEN_POSITIONS = 3
POSITION_TIMEOUT_SEC = 15 * 60
POLL_INTERVAL_SEC    = 5 * 60    # REST poll + CSV írás gyakorisága
CSV_FILE             = "positions.csv"

CSV_FIELDS = [
    "log_time",          # Mikor lett a CSV sor írva
    "deal_id",
    "deal_reference",
    "epic",
    "direction",
    "size",
    "state",
    # Konfig
    "zone_low",
    "zone_high",
    "tp_configured",
    "sl_configured",
    # Élő adatok a REST-ből
    "open_level",        # Tényleges nyitóár
    "current_level",     # Jelenlegi ár (REST poll)
    "limit_level",       # TP szint az API-ban
    "stop_level",        # SL szint az API-ban
    "profit_loss",       # Aktuális P&L
    "currency",
    "created_date",      # Mikor nyílt a pozíció
    "close_level",       # Záróár (ha zárva)
    "close_reason",      # LIMIT / STOP / MANUAL / EXPIRED / stb.
    # Időbélyegek
    "registered_at",     # Mikor adták hozzá a managerhez
    "opened_at",         # Mikor küldte el az API-nak
    "closed_at",         # Mikor zárult
    "expired_at",        # Mikor járt le (timeout)
    "last_poll_at",      # Utolsó REST frissítés
    # Hiba
    "error_msg",
]


# ─── ENUMS / DATACLASSES ─────────────────────────────────────────────────────

class Direction(str, Enum):
    BUY  = "BUY"
    SELL = "SELL"

class PositionState(str, Enum):
    WAITING  = "WAITING"
    OPENING  = "OPENING"
    OPEN     = "OPEN"
    FILLED   = "FILLED"    # TP vagy SL elérve
    CANCELED = "CANCELED"  # Manuális
    EXPIRED  = "EXPIRED"   # 10 perces timeout
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


# ─── POSITION ────────────────────────────────────────────────────────────────

class Position:
    def __init__(self, config: PositionConfig, base_url: str,
                 cst: str, token: str, manager: "PositionManager"):
        self.config     = config
        self.base_url   = base_url
        self.cst        = cst
        self.token      = token
        self.state      = PositionState.WAITING

        # Azonosítók
        self.deal_id:    Optional[str] = None
        self.deal_ref:   Optional[str] = None

        # Áradatok
        self.open_level:    Optional[float] = None
        self.current_level: Optional[float] = None
        self.limit_level:   Optional[float] = None
        self.stop_level:    Optional[float] = None
        self.close_level:   Optional[float] = None
        self.close_reason:  Optional[str]   = None
        self.profit_loss:   Optional[float] = None
        self.currency:      Optional[str]   = None
        self.created_date:  Optional[str]   = None

        # Időbélyegek
        self.registered_at = datetime.now(timezone.utc)
        self.opened_at:   Optional[datetime] = None
        self.closed_at:   Optional[datetime] = None
        self.expired_at:  Optional[datetime] = None
        self.last_poll_at: Optional[datetime] = None

        self.error_msg: Optional[str] = None

        self._queue: asyncio.Queue[tuple[float, float]] = asyncio.Queue()
        self._manager = manager

    # ── Nyilvános ─────────────────────────────────────────────────────────────

    def start(self) -> asyncio.Task:
        return asyncio.create_task(self._monitor_loop())

    def on_price(self, bid: float, ask: float) -> None:
        if self.state == PositionState.WAITING:
            self._queue.put_nowait((bid, ask))

    def cancel(self) -> bool:
        if self.state == PositionState.WAITING:
            self.state = PositionState.CANCELED
            return True
        return False

    def is_terminal(self) -> bool:
        return self.state in (
            PositionState.FILLED, PositionState.CANCELED,
            PositionState.EXPIRED, PositionState.ERROR,
        )

    def to_csv_row(self) -> dict:
        """Az összes mező egy CSV sorba."""
        now = datetime.now(timezone.utc).isoformat()
        return {
            "log_time":       now,
            "deal_id":        self.deal_id        or "",
            "deal_reference": self.deal_ref       or "",
            "epic":           self.config.epic,
            "direction":      self.config.direction.value,
            "size":           self.config.size,
            "state":          self.state.value,
            "zone_low":       self.config.zone_low,
            "zone_high":      self.config.zone_high,
            "tp_configured":  self.config.tp,
            "sl_configured":  self.config.sl,
            "open_level":     self.open_level     or "",
            "current_level":  self.current_level  or "",
            "limit_level":    self.limit_level     or "",
            "stop_level":     self.stop_level      or "",
            "profit_loss":    self.profit_loss     or "",
            "currency":       self.currency        or "",
            "created_date":   self.created_date    or "",
            "close_level":    self.close_level     or "",
            "close_reason":   self.close_reason    or "",
            "registered_at":  self.registered_at.isoformat(),
            "opened_at":      self.opened_at.isoformat()   if self.opened_at   else "",
            "closed_at":      self.closed_at.isoformat()   if self.closed_at   else "",
            "expired_at":     self.expired_at.isoformat()  if self.expired_at  else "",
            "last_poll_at":   self.last_poll_at.isoformat() if self.last_poll_at else "",
            "error_msg":      self.error_msg or "",
            "raw_text":       self.config.raw_text,
            "send_date":      self.config.send_date,
            "edited":         self.config.edited,
            "chat_id":        self.config.chat_id,
            "tp_idx":         self.config.tp_idx
        }

    def __repr__(self):
        return f"<Position {self.config.direction.value} {self.config.epic} {self.state.value}>"

    # ── Belső logika ──────────────────────────────────────────────────────────

    async def _monitor_loop(self) -> None:
        deadline = self.registered_at.timestamp() + POSITION_TIMEOUT_SEC
        try:
            while self.state == PositionState.WAITING:
                remaining = deadline - datetime.now(timezone.utc).timestamp()
                if remaining <= 0:
                    self.state      = PositionState.EXPIRED
                    self.expired_at = datetime.now(timezone.utc)
                    logger.info("[POS] ⏰ Lejárt: %s", self)
                    break

                try:
                    bid, ask = await asyncio.wait_for(
                        self._queue.get(), timeout=min(remaining, 5.0)
                    )
                except asyncio.TimeoutError:
                    continue

                trigger = ask if self.config.direction == Direction.BUY else bid
                if self.config.zone_low <= trigger <= self.config.zone_high:
                    if not self._manager.can_open():
                        # Limit teljesült — logol és tovább vár
                        logger.info(
                            "[POS] ⏸ Zone elérve, de limit telt (%d/%d) — várakozás...",
                            self._manager.open_count(), self._manager.max_open,
                        )
                        continue   # ← nem nyit, következő tick-re vár
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
            "stopLevel":      self.config.sl,
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
                    self.state    = PositionState.OPEN
                    logger.info("✅ Nyitva | ref:%s | %s %s | TP:%.2f SL:%.2f",
                                self.deal_ref, self.config.direction.value,
                                self.config.epic, self.config.tp, self.config.sl)
                    asyncio.create_task(self._fetch_confirm())
        except Exception as e:
            self.state     = PositionState.ERROR
            self.error_msg = str(e)
            logger.error("❌ Nyitási hiba: %s", e)

    async def _fetch_confirm(self) -> None:
        """deal_id + nyitóár lekérése a confirms végpontról."""
        await asyncio.sleep(1)
        headers = {"CST": self.cst, "X-SECURITY-TOKEN": self.token}
        try:
            async with aiohttp.ClientSession() as s:
                async with s.get(
                    f"{self.base_url}/api/v1/confirms/{self.deal_ref}",
                    headers=headers
                ) as r:
                    data = await r.json()
                    self.deal_id    = data.get("dealId")
                    self.open_level = data.get("level")
                    self.limit_level = data.get("limitLevel")
                    self.stop_level  = data.get("stopLevel")
                    logger.info("[POS] Confirm | dealId:%s | nyitóár:%s",
                                self.deal_id, self.open_level)
        except Exception as e:
            logger.warning("[POS] Confirm hiba: %s", e)

    def apply_rest_data(self, api_pos: dict) -> None:
        """REST /positions válaszból frissíti az élő adatokat."""
        pos  = api_pos.get("position", {})
        mkt  = api_pos.get("market",   {})
        self.current_level  = mkt.get("bid") or mkt.get("offer")
        self.open_level     = self.open_level or pos.get("openLevel")
        self.limit_level    = pos.get("limitLevel")
        self.stop_level     = pos.get("stopLevel")
        self.profit_loss    = pos.get("upl")          # Unrealised P&L
        self.currency       = pos.get("currency")
        self.created_date   = pos.get("createdDateUTC") or pos.get("createdDate")
        self.last_poll_at   = datetime.now(timezone.utc)

    def apply_opu(self, payload: dict) -> None:
        """WebSocket OPU üzenetből frissíti az állapotot."""
        status = payload.get("status")
        if status == "DELETED":
            self.state        = PositionState.FILLED
            self.close_level  = payload.get("level")
            self.close_reason = payload.get("closeReason", "UNKNOWN")
            self.closed_at    = datetime.now(timezone.utc)
            emoji = "🎯" if self.close_reason == "LIMIT" else "🛑"
            logger.info("%s Zárva (%s) | %s | záróár:%s",
                        emoji, self.close_reason, self, self.close_level)
        elif status == "OPEN":
            self.current_level = payload.get("level")
            self.limit_level   = payload.get("limitLevel")
            self.stop_level    = payload.get("stopLevel")
            self.last_poll_at  = datetime.now(timezone.utc)


# ─── CSV LOGGER ──────────────────────────────────────────────────────────────

class CsvLogger:
    """
    Csak lezárt/lejárt pozíciókat ír be — egyszer, append módban.
    Soha nem írja újra az egész fájlt.
    """

    def __init__(self, path: str):
        self.path     = path
        self._written: set[str] = set()   # deal_ref vagy id() — ne kerüljön be kétszer
        self._ensure_header()

    def _ensure_header(self) -> None:
        if not os.path.exists(self.path):
            with open(self.path, "w", newline="", encoding="utf-8") as f:
                csv.DictWriter(f, fieldnames=CSV_FIELDS).writeheader()
            logger.info("[CSV] Fájl létrehozva: %s", self.path)

    def append_if_terminal(self, pos: Position) -> None:
        """
        Ha a pozíció terminális (FILLED / EXPIRED / CANCELED / ERROR)
        és még nem lett kiírva, hozzáfűzi a CSV-hez.
        """
        if not pos.is_terminal():
            return

        # Egyedi kulcs: deal_ref ha van, egyébként Python object id
        key = pos.deal_ref or str(id(pos))
        if key in self._written:
            return

        with open(self.path, "a", newline="", encoding="utf-8") as f:
            csv.DictWriter(f, fieldnames=CSV_FIELDS).writerow(pos.to_csv_row())

        self._written.add(key)
        logger.info("[CSV] ✍️  Mentve (%s): %s", pos.state.value, pos)


# ─── POSITION MANAGER ────────────────────────────────────────────────────────

class PositionManager:
    def __init__(self, base_url: str, cst: str, token: str):
        self.base_url = base_url
        self.cst = cst
        self.token = token
        self.max_open = MAX_OPEN_POSITIONS
        self._positions: list[Position] = []
        self._csv = CsvLogger(CSV_FILE)

    def open_count(self) -> int:
        """Jelenleg OPEN vagy OPENING állapotú pozíciók száma."""
        return sum(1 for p in self._positions
                   if p.state in (PositionState.OPEN, PositionState.OPENING))

    def can_open(self) -> bool:
        return self.open_count() < self.max_open

    def add(self, config: PositionConfig) -> Position:
        pos = Position(config, self.base_url, self.cst, self.token, manager=self)  # ← manager=self
        pos.start()
        self._positions.append(pos)
        logger.info("[MGR] ➕ %s | Zone %.2f–%.2f | TP:%.2f SL:%.2f",
                    pos, config.zone_low, config.zone_high, config.tp, config.sl)
        return pos

    def broadcast(self, bid: float, ask: float) -> None:
        for pos in self._positions:
            pos.on_price(bid, ask)

    def handle_opu(self, payload: dict) -> None:
        deal_id = payload.get("dealId")
        for pos in self._positions:
            if pos.deal_id == deal_id:
                pos.apply_opu(payload)
                self._csv.append_if_terminal(pos)   # ← csak ha FILLED lett
                break

    async def poll_and_log(self) -> None:
        # Timeout ellenőrzés
        for pos in self._positions:
            if pos.state == PositionState.WAITING:
                elapsed = (datetime.now(timezone.utc) - pos.registered_at).total_seconds()
                if elapsed >= POSITION_TIMEOUT_SEC:
                    pos.state      = PositionState.EXPIRED
                    pos.expired_at = datetime.now(timezone.utc)
                    logger.info("[MGR] ⏰ Lejárt: %s", pos)
                    self._csv.append_if_terminal(pos)   # ← EXPIRED → beírjuk

        # REST frissítés az OPEN pozíciókra
        open_positions = [p for p in self._positions if p.state == PositionState.OPEN]
        if open_positions:
            await self._fetch_rest_positions(open_positions)

        # Ha a REST poll közben vált terminálissá valami
        for pos in self._positions:
            self._csv.append_if_terminal(pos)

    async def _fetch_rest_positions(self, targets: list[Position]) -> None:
        headers = {"CST": self.cst, "X-SECURITY-TOKEN": self.token}
        try:
            async with aiohttp.ClientSession() as s:
                async with s.get(f"{self.base_url}/api/v1/positions",
                                 headers=headers) as r:
                    data = await r.json()
                    api_map = {
                        p["position"]["dealId"]: p
                        for p in data.get("positions", [])
                        if "position" in p
                    }
                    for pos in targets:
                        if pos.deal_id in api_map:
                            pos.apply_rest_data(api_map[pos.deal_id])
                        else:
                            if pos.state == PositionState.OPEN:
                                pos.state        = PositionState.FILLED
                                pos.close_reason = "CLOSED_EXTERNALLY"
                                pos.closed_at    = datetime.now(timezone.utc)
                                logger.warning("[MGR] Pozíció eltűnt a REST-ből: %s", pos)
        except Exception as e:
            logger.error("[MGR] REST poll hiba: %s", e)

    @property
    def positions(self) -> list[Position]:
        return list(self._positions)


# ─── POLL LOOP ───────────────────────────────────────────────────────────────

async def poll_loop(manager: PositionManager) -> None:
    """Minden POLL_INTERVAL_SEC másodpercben lekérdezi és menti a pozíciókat."""
    while True:
        await asyncio.sleep(POLL_INTERVAL_SEC)
        await manager.poll_and_log()


# ─── ZMQ LISTENER ────────────────────────────────────────────────────────────

async def zmq_listener(manager: PositionManager) -> None:
    ctx  = zmq.asyncio.Context.instance()
    sock = ctx.socket(zmq.PULL)
    sock.bind(ZMQ_PULL_ADDR)
    logger.info("[ZMQ] Figyelés: %s", ZMQ_PULL_ADDR)
    try:
        while True:
            p = await sock.recv_pyobj()
            try:
                manager.add(PositionConfig(
                    epic      = p["epic"],
                    direction = Direction(p["direction"].upper()),
                    size      = float(p["size"]),
                    zone_low  = float(p["zone_low"]),
                    zone_high = float(p["zone_high"]),
                    tp        = float(p["tp"]),
                    sl        = float(p["sl"]),
                    tp_idx    = int(p["tp_idx"]),
                    raw_text  = str(p["raw_text"]),
                    send_date = str(p["send_date"]),
                    edited    = bool(p["edited"]),
                    chat_id   = int(p["chat_id"]),
                ))
            except (KeyError, ValueError) as e:
                logger.warning("[ZMQ] Hibás üzenet (%s)", e)
    except asyncio.CancelledError:
        sock.close()


# ─── SESSION / PING ──────────────────────────────────────────────────────────

async def create_session() -> tuple[str, str]:
    headers = {"X-CAP-API-KEY": API_KEY, "Content-Type": "application/json"}
    body    = {"identifier": IDENTIFIER, "password": PASSWORD, "encryptionKey": False}
    async with aiohttp.ClientSession() as s:
        async with s.post(f"{BASE_URL}/api/v1/session",
                          headers=headers, json=body) as r:
            r.raise_for_status()
            logger.info("[AUTH] Bejelentkezés OK")
            return r.headers["CST"], r.headers["X-SECURITY-TOKEN"]

async def ping_loop(ws, cst, token):
    while True:
        await asyncio.sleep(9 * 60)
        await ws.send(json.dumps({"destination": "ping", "correlationId": 99,
                                  "cst": cst, "securityToken": token}))


# ─── FŐ STREAM ───────────────────────────────────────────────────────────────

async def stream_xauusd():
    cst, token = await create_session()
    manager    = PositionManager(base_url=BASE_URL, cst=cst, token=token)

    asyncio.create_task(zmq_listener(manager))
    asyncio.create_task(poll_loop(manager))

    async with websockets.connect(WS_URL) as ws:
        await ws.send(json.dumps({
            "destination": "marketData.subscribe", "correlationId": 1,
            "cst": cst, "securityToken": token,
            "payload": {"epics": [EPIC]},
        }))
        asyncio.create_task(ping_loop(ws, cst, token))

        async for raw in ws:
            try:
                data = json.loads(raw)
            except json.JSONDecodeError:
                continue

            dest = data.get("destination", "")

            if dest == "quote":
                payload = data.get("payload", {})
                bid = payload.get("bid")
                ask = payload.get("ofr")
                if bid and ask:
                    manager.broadcast(bid, ask)
                    # ts = datetime.now().strftime("%H:%M:%S.%f")[:-3]
                    # print(f"[{ts}]  Bid:{bid:.2f}  Ask:{ask:.2f}")

            elif dest == "OPU":
                # Valós idejű pozíció-frissítés (TP/SL elérés azonnali jelzése)
                manager.handle_opu(data.get("payload", {}))


if __name__ == "__main__":
    try:
        asyncio.run(stream_xauusd())
    except KeyboardInterrupt:
        print("\n[EXIT] Leállítás...")
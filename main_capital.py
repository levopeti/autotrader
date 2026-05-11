import asyncio
import json
from dataclasses import dataclass
from time import sleep

import aiohttp
import websockets
import zmq
import zmq.asyncio
import logging

from signal_utils.position import Direction, PositionConfig, PositionState
from signal_utils.position_manager import PositionManager

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

POLL_INTERVAL_SEC     = 5 * 60   # REST poll gyakorisága
BACKFILL_INTERVAL_SEC = 15
OPEN_INTERVAL_SEC     = 2.0      # pozíciók közötti puffer (API rate limit)
CONFIRM_TIMEOUT_SEC   = 30.0     # confirm várakozás max (OPENING után)
POSITION_TIMEOUT_SEC  = 15 * 60  # WAITING timeout (zone-ra várakozás)
RETRY_DELAY_SEC       = 10.0     # REJECTED után mennyi ideig vár újrapróbálás előtt
MAX_RETRIES           = 5        # hányszor próbálja újra REJECTED esetén


# ─── QUEUE ITEM ───────────────────────────────────────────────────────────────

@dataclass
class QueueItem:
    config:  PositionConfig
    retries: int = 0


# ─── POLL LOOP ────────────────────────────────────────────────────────────────

async def poll_loop(manager: PositionManager) -> None:
    while True:
        await asyncio.sleep(POLL_INTERVAL_SEC)
        await manager.poll_and_log()


async def backfill_loop(manager: PositionManager) -> None:
    while True:
        await asyncio.sleep(BACKFILL_INTERVAL_SEC)
        await manager.process_pending_csv()


# ─── REJECT WATCHER ───────────────────────────────────────────────────────────

async def _watch_for_reject(pos, item: QueueItem, queue: asyncio.Queue) -> None:
    """
    Háttérben figyeli a pozíciót miután WAITING-ből elindult.
    Ha REJECTED lesz (fedezethiány / platform limit), visszadobja a queue-ba.
    A WAITING állapotot nem blokkolja — azt a _monitor_loop kezeli.
    """
    cfg      = item.config
    loop     = asyncio.get_event_loop()
    # max várakozás: zone timeout + confirm timeout
    deadline = loop.time() + POSITION_TIMEOUT_SEC + CONFIRM_TIMEOUT_SEC

    terminal = (
        PositionState.OPEN,
        PositionState.REJECTED,
        PositionState.ERROR,
        PositionState.EXPIRED,
        PositionState.CANCELED,
    )

    while pos.state not in terminal:
        if loop.time() > deadline:
            logger.warning("[WATCH] ⏰ Watcher timeout: %s", pos)
            return
        await asyncio.sleep(0.5)

    if pos.state == PositionState.REJECTED:
        if item.retries < MAX_RETRIES:
            item.retries += 1
            logger.warning(
                "[WATCH] 🔁 REJECTED → újrapróbálás %d/%d | %.0fs múlva | TP:%.2f | reason:%s",
                item.retries, MAX_RETRIES, RETRY_DELAY_SEC,
                cfg.tp, pos.close_reason,
            )
            await asyncio.sleep(RETRY_DELAY_SEC)
            await queue.put(item)
        else:
            logger.error(
                "[WATCH] ❌ Max retry elérve (%d), végleg eldobva | TP:%.2f | reason:%s",
                MAX_RETRIES, cfg.tp, pos.close_reason,
            )
    else:
        logger.info("[WATCH] ✅ Pozíció lezárult (%s) | TP:%.2f", pos.state, cfg.tp)


# ─── POSITION OPENER WORKER ───────────────────────────────────────────────────

async def position_opener(manager: PositionManager, queue: asyncio.Queue) -> None:
    """
    Szekvenciálisan adja hozzá a pozíciókat a managerhez (WAITING sorba rakja őket).
    A tényleges nyitás aszinkron (zone-ra vár), ezért a worker NEM blokkolja a queue-t.
    Minden pozícióhoz egy háttér-watcher task indul, amely REJECTED esetén újrapróbál.
    """
    logger.info("[OPENER] Worker indult (retry_delay=%.0fs, max_retries=%d)",
                RETRY_DELAY_SEC, MAX_RETRIES)
    while True:
        item: QueueItem = await queue.get()
        cfg = item.config
        try:
            if not manager.can_open(cfg.chat_id):
                logger.warning(
                    "[OPENER] ⛔ Limit elérve (össz: %d/%d, sender: %d/%d), eldobva: TP:%.2f",
                    manager.open_count(), manager.max_open,
                    manager.open_count(cfg.chat_id), manager.sender_max_open,
                    cfg.tp,
                )
                continue

            pos = manager.add(cfg)
            if pos is None:
                continue

            logger.info("[OPENER] ➕ Pozíció WAITING-be rakva, zone-ra vár | TP:%.2f", cfg.tp)

            # Háttérben figyeli: ha REJECTED → visszadobja queue-ba
            asyncio.create_task(_watch_for_reject(pos, item, queue))

            # Kis puffer az API-nak, majd jöhet a következő szignál
            await asyncio.sleep(OPEN_INTERVAL_SEC)

        except Exception as e:
            logger.error("[OPENER] Hiba: %s", e)
        finally:
            queue.task_done()


# ─── ZMQ LISTENER ─────────────────────────────────────────────────────────────

async def zmq_listener(manager: PositionManager, queue: asyncio.Queue) -> None:
    ctx  = zmq.asyncio.Context.instance()
    sock = ctx.socket(zmq.PULL)
    sock.bind(ZMQ_PULL_ADDR)
    logger.info("[ZMQ] Figyelés: %s", ZMQ_PULL_ADDR)
    try:
        while True:
            p = await sock.recv_pyobj()
            try:
                cfg = PositionConfig(
                    epic=p["epic"],
                    direction=Direction(p["direction"].upper()),
                    size=float(p["size"]),
                    zone_low=float(p["zone_low"]),
                    zone_high=float(p["zone_high"]),
                    tp=float(p["tp"]),
                    sl=float(p["sl"]),
                    tp_idx=int(p["tp_idx"]),
                    raw_text=str(p["raw_text"]),
                    send_date=str(p["send_date"]),
                    edited=bool(p["edited"]),
                    chat_id=int(p["chat_id"]),
                    chat_name=str(p["chat_name"]),
                )
                await queue.put(QueueItem(config=cfg))
                logger.warning(
                    "[ZMQ] 📥 Sorba rakva (%s) (%s) (%.2f–%.2f) TP:%.2f SL:%.2f | Queue: %d",
                    p["epic"], p["direction"],
                    p["zone_low"], p["zone_high"],
                    p["tp"], p["sl"],
                    queue.qsize(),
                )
            except (KeyError, ValueError) as e:
                logger.warning("[ZMQ] Hibás üzenet (%s)", e)
    except asyncio.CancelledError:
        sock.close()


# ─── SESSION / PING ───────────────────────────────────────────────────────────

async def create_session() -> tuple[str, str]:
    headers = {"X-CAP-API-KEY": API_KEY, "Content-Type": "application/json"}
    body    = {"identifier": IDENTIFIER, "password": PASSWORD, "encryptionKey": False}
    async with aiohttp.ClientSession() as s:
        async with s.post(f"{BASE_URL}/api/v1/session",
                          headers=headers, json=body) as r:
            r.raise_for_status()
            logger.info("[AUTH] Bejelentkezés OK")
            return r.headers["CST"], r.headers["X-SECURITY-TOKEN"]


async def ping_loop(ws, cst, token) -> None:
    while True:
        await asyncio.sleep(9 * 60)
        await ws.send(json.dumps({
            "destination":   "ping",
            "correlationId": 99,
            "cst":           cst,
            "securityToken": token,
        }))


# ─── FŐ STREAM ────────────────────────────────────────────────────────────────

async def stream_xauusd() -> None:
    cst, token = await create_session()
    manager    = PositionManager(base_url=BASE_URL, cst=cst, token=token)
    open_queue: asyncio.Queue[QueueItem] = asyncio.Queue()

    # TODO
    # await manager.startup_backfill_csv()

    asyncio.create_task(zmq_listener(manager, open_queue))
    asyncio.create_task(position_opener(manager, open_queue))
    asyncio.create_task(poll_loop(manager))
    # asyncio.create_task(backfill_loop(manager))

    async with websockets.connect(WS_URL) as ws:
        await ws.send(json.dumps({
            "destination":   "marketData.subscribe",
            "correlationId": 1,
            "cst":           cst,
            "securityToken": token,
            "payload":       {"epics": [EPIC]},
        }))
        # await ws.send(json.dumps({
        #     "destination": "OHLCMarketData.subscribe",
        #     "correlationId": 2,
        #     "cst": cst, "securityToken": token,
        #     "payload": {"epics": [EPIC]},
        # }))
        logger.info("[WS] OPU feliratkozás aktív")
        asyncio.create_task(ping_loop(ws, cst, token))

        async for raw in ws:
            try:
                data = json.loads(raw)
            except json.JSONDecodeError:
                continue

            dest = data.get("destination", "")

            if dest == "quote":
                payload = data.get("payload", {})
                bid     = payload.get("bid")
                ask     = payload.get("ofr")
                if bid and ask:
                    manager.broadcast(bid, ask)

            elif dest == "OPU":
                manager.handle_opu(data.get("payload", {}))


if __name__ == "__main__":
    while True:
        try:
            asyncio.run(stream_xauusd())
        except websockets.exceptions.ConnectionClosedError:
            print("ConnectionClosedError error. 10 min sleep")
            sleep(10 * 60)
        except KeyboardInterrupt:
            print("\n[EXIT] Leállítás...")
            break
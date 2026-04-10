import asyncio
import json
import aiohttp
import websockets
import zmq
import zmq.asyncio
import logging

from signal_utils.position import Direction, PositionConfig
from signal_utils.position_manager import PositionManager

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

with open('keys_urls.json', 'r') as f:
    config = json.load(f)

API_KEY = config["capital_api_key"]
IDENTIFIER = config["capital_login"]
PASSWORD = config["capital_pw"]
DEMO_MODE = True

BASE_URL = (
    "https://demo-api-capital.backend-capital.com"
    if DEMO_MODE else
    "https://api-capital.backend-capital.com"
)
WS_URL = "wss://api-streaming-capital.backend-capital.com/connect"
EPIC = "GOLD"
ZMQ_PULL_ADDR = "tcp://localhost:5555"
POLL_INTERVAL_SEC = 5 * 60  # REST poll + CSV írás gyakorisága
BACKFILL_INTERVAL_SEC = 15


# ─── POLL LOOP ───────────────────────────────────────────────────────────────

async def poll_loop(manager: PositionManager) -> None:
    """Minden POLL_INTERVAL_SEC másodpercben lekérdezi és menti a pozíciókat."""
    while True:
        await asyncio.sleep(POLL_INTERVAL_SEC)
        await manager.poll_and_log()

async def backfill_loop(manager: PositionManager) -> None:
    while True:
        await asyncio.sleep(BACKFILL_INTERVAL_SEC)
        await manager.process_pending_csv()


# ─── ZMQ LISTENER ────────────────────────────────────────────────────────────

async def zmq_listener(manager: PositionManager) -> None:
    ctx = zmq.asyncio.Context.instance()
    sock = ctx.socket(zmq.PULL)
    sock.bind(ZMQ_PULL_ADDR)
    logger.info("[ZMQ] Figyelés: %s", ZMQ_PULL_ADDR)
    try:
        while True:
            p = await sock.recv_pyobj()
            try:
                manager.add(PositionConfig(
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
                ))
                logger.warning("[ZMQ] New signal (%s) (%s) (%f)-(%f) tp: (%f), sl: (%f)", p["epic"],
                               p["direction"], p["zone_low"], p["zone_high"], p["tp"], p["sl"])
            except (KeyError, ValueError) as e:
                logger.warning("[ZMQ] Hibás üzenet (%s)", e)
    except asyncio.CancelledError:
        sock.close()


# ─── SESSION / PING ──────────────────────────────────────────────────────────

async def create_session() -> tuple[str, str]:
    headers = {"X-CAP-API-KEY": API_KEY, "Content-Type": "application/json"}
    body = {"identifier": IDENTIFIER, "password": PASSWORD, "encryptionKey": False}
    async with aiohttp.ClientSession() as s:
        async with s.post(f"{BASE_URL}/api/v1/session",
                          headers=headers, json=body) as r:
            r.raise_for_status()
            logger.info("[AUTH] Bejelentkezés OK")
            return r.headers["CST"], r.headers["X-SECURITY-TOKEN"]


async def ping_loop(ws, cst, token):
    while True:
        await asyncio.sleep(9 * 60)
        await ws.send(json.dumps({"destination": "ping",
                                  "correlationId": 99,
                                  "cst": cst,
                                  "securityToken": token}))


# ─── FŐ STREAM ───────────────────────────────────────────────────────────────

async def stream_xauusd():
    cst, token = await create_session()
    manager = PositionManager(base_url=BASE_URL, cst=cst, token=token)

    await manager.startup_backfill_csv()

    asyncio.create_task(zmq_listener(manager))
    asyncio.create_task(poll_loop(manager))
    # asyncio.create_task(backfill_loop(manager))

    async with websockets.connect(WS_URL) as ws:
        await ws.send(json.dumps({
            "destination": "marketData.subscribe",
            "correlationId": 1,
            "cst": cst,
            "securityToken": token,
            "payload": {"epics": [EPIC]},
        }))
        # await ws.send(json.dumps({
        #     "destination": "OHLCMarketData.subscribe",  # pozíció/order frissítések
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


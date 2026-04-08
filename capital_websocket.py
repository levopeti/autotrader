import asyncio
import json
import aiohttp
import websockets
from datetime import datetime

with open('keys_urls.json', 'r') as f:
    config = json.load(f)

# ─── KONFIGURÁCIÓ ────────────────────────────────────────────────────────────
API_KEY      = config["capital_api_key"]
IDENTIFIER   = config["capital_login"]     # Capital.com bejelentkezési e-mail
PASSWORD     = config["capital_pw"]               # Fiók jelszó
DEMO_MODE    = True                      # True = demo szerver, False = éles

BASE_URL = (
    "https://demo-api-capital.backend-capital.com"
    if DEMO_MODE else
    "https://api-capital.backend-capital.com"
)
WS_URL = "wss://api-streaming-capital.backend-capital.com/connect"

EPIC = "GOLD"  # XAUUSD epic neve a Capital.com rendszerében
# ─────────────────────────────────────────────────────────────────────────────


async def create_session() -> tuple[str, str]:
    """POST /session → visszaadja a CST és X-SECURITY-TOKEN értékeket."""
    headers = {
        "X-CAP-API-KEY": API_KEY,
        "Content-Type": "application/json",
    }
    body = {
        "identifier": IDENTIFIER,
        "password": PASSWORD,
        "encryptionKey": False,
    }
    async with aiohttp.ClientSession() as session:
        async with session.post(
            f"{BASE_URL}/api/v1/session",
            headers=headers,
            json=body,
        ) as resp:
            resp.raise_for_status()
            cst   = resp.headers["CST"]
            token = resp.headers["X-SECURITY-TOKEN"]
            print(f"[AUTH] Bejelentkezés sikeres – CST: {cst[:10]}…")
            return cst, token


async def ping_loop(websocket, cst: str, token: str):
    """Minden 9 percben küld ping üzenetet, hogy a kapcsolat ne szakadjon meg."""
    while True:
        await asyncio.sleep(9 * 60)
        msg = json.dumps({
            "destination": "ping",
            "correlationId": 99,
            "cst": cst,
            "securityToken": token,
        })
        await websocket.send(msg)
        print("[PING] Ping elküldve")


async def stream_xauusd():
    """Fő függvény: bejelentkezés → WebSocket → GOLD feliratkozás → stream."""
    cst, token = await create_session()

    print(f"[WS] Csatlakozás: {WS_URL}")
    async with websockets.connect(WS_URL) as ws:

        # Feliratkozás az arany árfolyamára
        subscribe_msg = json.dumps({
            "destination": "marketData.subscribe",
            "correlationId": 1,
            "cst": cst,
            "securityToken": token,
            "payload": {
                "epics": [EPIC]
            },
        })
        await ws.send(subscribe_msg)
        print(f"[WS] Feliratkozás elküldve: {EPIC}")

        # Ping loop háttérben
        asyncio.create_task(ping_loop(ws, cst, token))

        # Üzenetek feldolgozása
        async for raw in ws:
            try:
                data = json.loads(raw)
            except json.JSONDecodeError:
                continue

            destination = data.get("destination", "")

            # Feliratkozás visszaigazolása
            if destination == "marketData.subscribe":
                status = data.get("payload", {}).get("subscriptions", {})
                print(f"[SUB] Feliratkozás állapota: {status}")
                continue

            # Ping visszaigazolás
            if destination == "ping":
                continue

            # Valós idejű árfrissítés
            if destination == "quote":
                payload   = data.get("payload", {})
                epic      = payload.get("epic", "?")
                bid       = payload.get("bid")
                ask       = payload.get("ofr")   # Capital.com 'ofr' = ask
                timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]

                if bid and ask:
                    spread = round(ask - bid, 5)
                    print(
                        f"[{timestamp}]  {epic:10s}"
                        f"  Bid: {bid:.2f}"
                        f"  Ask: {ask:.2f}"
                        f"  Spread: {spread:.2f}"
                    )
                else:
                    print(f"[{timestamp}] Adat: {data}")


if __name__ == "__main__":
    try:
        asyncio.run(stream_xauusd())
    except KeyboardInterrupt:
        print("\n[EXIT] Leállítás...")
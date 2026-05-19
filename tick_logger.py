import os
import json
import time
import asyncio
import logging
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests
import websockets

with open('keys_urls.json', 'r') as f:
    config = json.load(f)

API_BASE_URL = "https://demo-api-capital.backend-capital.com"
WS_URL = "wss://api-streaming-capital.backend-capital.com/connect"
API_KEY = config["capital_api_key"]
IDENTIFIER = config["capital_login"]
PASSWORD = config["capital_pw"]
CAPITAL_ACCOUNT_ID = "320258870701535518"

MARKET_SYMBOL = os.getenv("MARKET_SYMBOL", "GOLD")
TICK_PARQUET = os.getenv("TICK_PARQUET", "./data/tick_data_GOLD.parquet")
FLUSH_EVERY_N_TICKS = int(os.getenv("FLUSH_EVERY_N_TICKS", "500"))
FLUSH_EVERY_SEC = int(os.getenv("FLUSH_EVERY_SEC", "30"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("capital_tick_logger")


class CapitalClient:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "X-CAP-API-KEY": API_KEY,
            "Content-Type": "application/json",
        })
        self.cst = None
        self.security_token = None
        self.current_account_id = None

    def _request(self, method, path, **kwargs):
        url = f"{API_BASE_URL}{path}"
        r = self.session.request(method, url, timeout=30, **kwargs)
        r.raise_for_status()
        return r

    def ensure_login(self):
        if self.cst and self.security_token:
            try:
                self._request("GET", "/api/v1/ping")
                return
            except Exception:
                self.cst = None
                self.security_token = None

        payload = {
            "identifier": IDENTIFIER,
            "password": PASSWORD,
            "encryptedPassword": False,
        }
        r = self._request("POST", "/api/v1/session", json=payload)
        self.cst = r.headers.get("CST")
        self.security_token = r.headers.get("X-SECURITY-TOKEN")
        self.session.headers.update({
            "CST": self.cst,
            "X-SECURITY-TOKEN": self.security_token,
        })
        data = r.json()
        self.current_account_id = data.get("accountId") or data.get("currentAccountId")
        logger.info("Logged in. accountId=%s", self.current_account_id)

    def get_accounts(self):
        r = self._request("GET", "/api/v1/accounts")
        data = r.json()
        if isinstance(data, dict):
            return data.get("accounts", data.get("accountInfo", []))
        return data

    def get_session_details(self):
        r = self._request("GET", "/api/v1/session")
        return r.json()

    def switch_account(self, account_id):
        payload = {"accountId": str(account_id)}
        self._request("PUT", "/api/v1/session", json=payload)
        session = self.get_session_details()
        self.current_account_id = session.get("accountId")
        logger.info("Current account after switch: %s", self.current_account_id)

    def ensure_account(self, account_id):
        if not account_id:
            return
        accounts = self.get_accounts()
        account_ids = [str(a.get("accountId")) for a in accounts if isinstance(a, dict)]
        if str(account_id) not in account_ids:
            raise RuntimeError(f"Requested CAPITAL_ACCOUNT_ID {account_id} not found in available accounts: {account_ids}")
        session = self.get_session_details()
        current_account_id = str(session.get("accountId"))
        if current_account_id != str(account_id):
            self.switch_account(str(account_id))

    def resolve_epic(self):
        r = self._request("GET", f"/api/v1/markets?searchTerm={MARKET_SYMBOL}")
        data = r.json()
        markets = data.get("markets", []) if isinstance(data, dict) else []
        if not markets:
            raise RuntimeError(f"No market found for symbol: {MARKET_SYMBOL}")
        for m in markets:
            epic = m.get("epic", "")
            if "GOLD" in epic or epic == MARKET_SYMBOL:
                return epic
        return markets[0]["epic"]


class TickBuffer:
    def __init__(self, parquet_path, flush_every_n_ticks=500):
        self.parquet_path = Path(parquet_path)
        self.flush_every_n_ticks = flush_every_n_ticks
        self.rows = []
        self.last_flush_ts = time.time()
        self.columns = [
            "timestamp_utc", "instrument", "epic", "bid", "ask", "mid", "spread", "tick_source_ts"
        ]

    def add_tick(self, row):
        self.rows.append(row)

    def as_dataframe(self):
        if not self.rows:
            return pd.DataFrame(columns=self.columns)
        df = pd.DataFrame(self.rows, columns=self.columns)
        df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], utc=True, errors="coerce")
        return df

    def should_flush(self, force=False):
        if force:
            return True
        if len(self.rows) >= self.flush_every_n_ticks:
            return True
        if self.rows and (time.time() - self.last_flush_ts >= FLUSH_EVERY_SEC):
            return True
        return False

    def flush(self, force=False):
        if not self.should_flush(force=force) or not self.rows:
            return 0
        df = self.as_dataframe()

        if self.parquet_path.exists():
            existing = pd.read_parquet(self.parquet_path)
            df = pd.concat([existing, df], ignore_index=True)

        df.to_parquet(self.parquet_path, index=False, compression="snappy")
        n = len(self.rows)
        self.rows = []
        self.last_flush_ts = time.time()
        logger.info("Flushed %s ticks to %s", n, self.parquet_path)
        return n


def extract_tick_row(epic, payload):
    now_utc = datetime.now(timezone.utc).isoformat()
    bid = payload.get("bid")
    ask = payload.get("offer") or payload.get("ask") or payload.get("ofr")
    if bid is None or ask is None:
        return None
    bid = float(bid)
    ask = float(ask)
    mid = (bid + ask) / 2.0
    spread = ask - bid
    tick_source_ts = payload.get("updateTimestamp") or payload.get("timestamp") or payload.get("utm") or payload.get("t")
    return {
        "timestamp_utc": now_utc,
        "instrument": epic,
        "epic": epic,
        "bid": bid,
        "ask": ask,
        "mid": mid,
        "spread": spread,
        "tick_source_ts": tick_source_ts,
    }


async def ping_loop(ws, cst, security_token):
    while True:
        await asyncio.sleep(20)
        await ws.send(json.dumps({
            "destination": "ping",
            "correlationId": int(time.time()),
            "cst": cst,
            "securityToken": security_token,
        }))


async def flush_loop(buffer):
    while True:
        await asyncio.sleep(5)
        buffer.flush(force=False)


async def ws_collect_ticks(client, epic, buffer):
    while True:
        try:
            client.ensure_login()
            async with websockets.connect(WS_URL, ping_interval=20, ping_timeout=20) as ws:
                sub = {
                    "destination": "marketData.subscribe",
                    "correlationId": 1,
                    "cst": client.cst,
                    "securityToken": client.security_token,
                    "payload": {"epics": [epic]},
                }
                await ws.send(json.dumps(sub))
                logger.info("Subscribed to %s tick stream", epic)
                ping_task = asyncio.create_task(ping_loop(ws, client.cst, client.security_token))
                try:
                    async for message in ws:
                        data = json.loads(message)
                        payload = data.get("payload", {})
                        if isinstance(payload, dict) and epic in payload:
                            tick_payload = payload[epic]
                            row = extract_tick_row(epic, tick_payload)
                            if row is not None:
                                buffer.add_tick(row)
                                buffer.flush(force=False)
                        elif isinstance(payload, dict):
                            row = extract_tick_row(epic, payload)
                            if row is not None:
                                buffer.add_tick(row)
                                buffer.flush(force=False)
                finally:
                    ping_task.cancel()
        except Exception as e:
            logger.warning("WebSocket loop error: %s", e)
            buffer.flush(force=True)
            await asyncio.sleep(5)


def reconstruct_fill_from_ticks(tick_parquet, signal_time_utc, direction):
    df = pd.read_parquet(tick_parquet)
    if df.empty:
        raise ValueError("Tick Parquet is empty")
    df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], utc=True, errors="coerce")
    signal_time = pd.to_datetime(signal_time_utc, utc=True, errors="coerce")
    df = df.dropna(subset=["timestamp_utc"]).sort_values("timestamp_utc")
    candidates = df[df["timestamp_utc"] >= signal_time]
    if candidates.empty:
        raise ValueError("No tick found at or after signal time")
    tick = candidates.iloc[0]
    exec_price = tick["ask"] if str(direction).upper() == "BUY" else tick["bid"]
    return {
        "timestamp_utc": tick["timestamp_utc"].isoformat(),
        "direction": direction,
        "exec_price": float(exec_price),
        "bid": float(tick["bid"]),
        "ask": float(tick["ask"]),
        "spread": float(tick["spread"]),
    }


async def main():
    if not API_KEY or not IDENTIFIER or not PASSWORD:
        raise RuntimeError("Missing CAPITAL_API_KEY / CAPITAL_IDENTIFIER / CAPITAL_PASSWORD")

    client = CapitalClient()
    client.ensure_login()
    client.ensure_account(CAPITAL_ACCOUNT_ID)
    epic = client.resolve_epic()
    buffer = TickBuffer(TICK_PARQUET, flush_every_n_ticks=FLUSH_EVERY_N_TICKS)

    logger.info("Tick logger started | epic=%s | parquet=%s | flush_n=%s | flush_sec=%s", epic, TICK_PARQUET, FLUSH_EVERY_N_TICKS, FLUSH_EVERY_SEC)

    try:
        await asyncio.gather(
            ws_collect_ticks(client, epic, buffer),
            flush_loop(buffer),
        )
    finally:
        buffer.flush(force=True)


if __name__ == "__main__":
    while True:
        try:
            asyncio.run(main())
        except KeyboardInterrupt:
            break
        except Exception as e:
            print(e)

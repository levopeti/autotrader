"""
Multi-instrument tick gyűjtő — egy folyamatból több epic-re iratkozik fel.

Konfigurálás: data/tick_logger_config.yaml (minden instrumentum + enabled flag).
Output: data/tick_data_{name}.parquet, atomikus write-tal.

Indítás:
  python -m tick_logger
  python tick_logger.py --config data/tick_logger_config.yaml
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
import requests
import websockets
import yaml

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from utils.keys import load_keys


API_BASE_URL_DEMO = "https://demo-api-capital.backend-capital.com"
API_BASE_URL_LIVE = "https://api-capital.backend-capital.com"
WS_URL = "wss://api-streaming-capital.backend-capital.com/connect"


logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("tick_logger")


# ─── Konfig ────────────────────────────────────────────────────────────────────

@dataclass
class InstrumentCfg:
    name: str                       # parquet-fájlnév + log-azonosító
    epic: Optional[str] = None      # ha None, auto-resolve a name-mel
    enabled: bool = True


@dataclass
class TickLoggerConfig:
    output_dir: Path
    flush_every_n_ticks: int
    flush_every_sec: int
    demo: bool
    instruments: List[InstrumentCfg]


def load_config(path: Path) -> TickLoggerConfig:
    with open(path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}
    return TickLoggerConfig(
        output_dir=Path(raw.get("output_dir", "./data")),
        flush_every_n_ticks=int(raw.get("flush_every_n_ticks", 500)),
        flush_every_sec=int(raw.get("flush_every_sec", 30)),
        demo=bool(raw.get("demo", True)),
        instruments=[
            InstrumentCfg(
                name=str(i["name"]),
                epic=i.get("epic"),
                enabled=bool(i.get("enabled", True)),
            )
            for i in raw.get("instruments", [])
        ],
    )


# ─── Capital REST kliens (csak login + resolve + ping) ─────────────────────────

class CapitalClient:
    def __init__(self, demo: bool = True):
        cfg = load_keys()
        self.api_key = cfg["capital_api_key"]
        self.identifier = cfg["capital_login"]
        self.password = cfg["capital_pw"]
        self.api_base_url = API_BASE_URL_DEMO if demo else API_BASE_URL_LIVE
        self.session = requests.Session()
        self.session.headers.update({"X-CAP-API-KEY": self.api_key, "Content-Type": "application/json"})
        self.cst: Optional[str] = None
        self.security_token: Optional[str] = None

    def _request(self, method, path, **kwargs):
        url = f"{self.api_base_url}{path}"
        r = self.session.request(method, url, timeout=30, **kwargs)
        r.raise_for_status()
        return r

    def ensure_login(self) -> None:
        if self.cst and self.security_token:
            try:
                self._request("GET", "/api/v1/ping")
                return
            except Exception:
                self.cst = None
                self.security_token = None
        payload = {"identifier": self.identifier, "password": self.password, "encryptedPassword": False}
        r = self._request("POST", "/api/v1/session", json=payload)
        self.cst = r.headers.get("CST")
        self.security_token = r.headers.get("X-SECURITY-TOKEN")
        self.session.headers.update({"CST": self.cst, "X-SECURITY-TOKEN": self.security_token})
        logger.info("Login OK")

    def resolve_epic(self, symbol: str) -> Optional[str]:
        try:
            data = self._request("GET", f"/api/v1/markets?searchTerm={symbol}").json()
        except Exception as e:
            logger.warning("resolve_epic(%s) hiba: %s", symbol, e)
            return None
        markets = data.get("markets", []) if isinstance(data, dict) else []
        if not markets:
            return None
        for m in markets:
            epic = m.get("epic", "")
            if symbol.upper() in epic.upper() or m.get("symbol", "").upper() == symbol.upper():
                return epic
        return markets[0].get("epic")


# ─── Tick buffer (instrumentumonként egy) ──────────────────────────────────────

class TickBuffer:
    COLUMNS = ["timestamp_utc", "instrument", "epic", "bid", "ask", "mid", "spread", "tick_source_ts"]

    def __init__(self, name: str, parquet_path: Path, flush_every_n_ticks: int, flush_every_sec: int):
        self.name = name
        self.parquet_path = parquet_path
        self.flush_every_n_ticks = flush_every_n_ticks
        self.flush_every_sec = flush_every_sec
        self.rows: List[Dict] = []
        self.last_flush_ts = time.time()
        self.total_ticks = 0

    def add(self, row: Dict) -> None:
        self.rows.append(row)
        self.total_ticks += 1

    def _should_flush(self, force: bool) -> bool:
        if force and self.rows:
            return True
        if len(self.rows) >= self.flush_every_n_ticks:
            return True
        if self.rows and (time.time() - self.last_flush_ts >= self.flush_every_sec):
            return True
        return False

    def flush(self, force: bool = False) -> int:
        if not self._should_flush(force):
            return 0
        df = pd.DataFrame(self.rows, columns=self.COLUMNS)
        df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], utc=True, errors="coerce")

        if self.parquet_path.exists():
            try:
                existing = pd.read_parquet(self.parquet_path)
                df = pd.concat([existing, df], ignore_index=True)
            except Exception as e:
                logger.warning("[%s] parquet read hiba (lehet konkurens write): %s — átugorjuk a flush-t",
                               self.name, e)
                return 0

        # Atomikus write: temp.parquet.tmp + os.replace
        tmp_path = self.parquet_path.with_suffix(self.parquet_path.suffix + ".tmp")
        df.to_parquet(tmp_path, index=False, compression="snappy")
        os.replace(tmp_path, self.parquet_path)

        n = len(self.rows)
        self.rows = []
        self.last_flush_ts = time.time()
        logger.info("[%s] flushed %s ticks → %s (total %s)", self.name, n, self.parquet_path.name, self.total_ticks)
        return n


# ─── Tick row extraction ───────────────────────────────────────────────────────

def extract_tick_row(name: str, epic: str, payload: Dict) -> Optional[Dict]:
    bid = payload.get("bid")
    ask = payload.get("offer") or payload.get("ask") or payload.get("ofr")
    if bid is None or ask is None:
        return None
    try:
        bid = float(bid)
        ask = float(ask)
    except (TypeError, ValueError):
        return None
    mid = (bid + ask) / 2.0
    spread = ask - bid
    return {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "instrument": name,
        "epic": epic,
        "bid": bid,
        "ask": ask,
        "mid": mid,
        "spread": spread,
        "tick_source_ts": payload.get("updateTimestamp") or payload.get("timestamp")
                          or payload.get("utm") or payload.get("t"),
    }


# ─── Main loop ─────────────────────────────────────────────────────────────────

async def ping_loop(ws, cst, security_token):
    while True:
        await asyncio.sleep(20)
        try:
            await ws.send(json.dumps({
                "destination": "ping",
                "correlationId": int(time.time()),
                "cst": cst,
                "securityToken": security_token,
            }))
        except Exception:
            return


async def flush_loop(buffers: Dict[str, TickBuffer]):
    while True:
        await asyncio.sleep(5)
        for buf in buffers.values():
            try:
                buf.flush(force=False)
            except Exception as e:
                logger.warning("[%s] flush hiba: %s", buf.name, e)


async def ws_collect_ticks(client: CapitalClient,
                           epic_to_name: Dict[str, str],
                           buffers: Dict[str, TickBuffer]) -> None:
    epics = list(epic_to_name.keys())
    while True:
        try:
            client.ensure_login()
            async with websockets.connect(WS_URL, ping_interval=20, ping_timeout=20) as ws:
                # Egyetlen subscribe-üzenet, az összes epic-kel
                await ws.send(json.dumps({
                    "destination": "marketData.subscribe",
                    "correlationId": int(time.time()),
                    "cst": client.cst,
                    "securityToken": client.security_token,
                    "payload": {"epics": epics},
                }))
                logger.info("Subscribed to %d epics: %s", len(epics), epics)

                ping_task = asyncio.create_task(ping_loop(ws, client.cst, client.security_token))
                try:
                    async for message in ws:
                        data = json.loads(message)
                        payload = data.get("payload", {})
                        if not isinstance(payload, dict):
                            continue
                        # 1) Kulcsolt forma: payload[epic] = {bid, ofr, ...}
                        any_matched = False
                        for e, name in epic_to_name.items():
                            if e in payload and isinstance(payload[e], dict):
                                row = extract_tick_row(name, e, payload[e])
                                if row is not None:
                                    buffers[name].add(row)
                                    any_matched = True
                        if any_matched:
                            continue
                        # 2) Flat forma: payload {epic, bid, ofr, ...}
                        flat_epic = payload.get("epic")
                        if flat_epic and flat_epic in epic_to_name:
                            row = extract_tick_row(epic_to_name[flat_epic], flat_epic, payload)
                            if row is not None:
                                buffers[epic_to_name[flat_epic]].add(row)
                finally:
                    ping_task.cancel()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.warning("WS loop error: %s — 5s múlva újra", e)
            # Vészflush, hogy ne veszítsük a memóriában lévő tickeket
            for buf in buffers.values():
                try:
                    buf.flush(force=True)
                except Exception:
                    pass
            await asyncio.sleep(5)


async def main_async(cfg_path: Path) -> None:
    cfg = load_config(cfg_path)
    cfg.output_dir.mkdir(parents=True, exist_ok=True)
    client = CapitalClient(demo=cfg.demo)
    client.ensure_login()

    enabled = [i for i in cfg.instruments if i.enabled]
    if not enabled:
        logger.error("Nincs enabled instrumentum a configban (%s)", cfg_path)
        return

    epic_to_name: Dict[str, str] = {}
    buffers: Dict[str, TickBuffer] = {}
    for inst in enabled:
        epic = inst.epic or client.resolve_epic(inst.name)
        if epic is None:
            logger.warning("Nem találom az epic-et: %s → kihagyva", inst.name)
            continue
        parquet_path = cfg.output_dir / f"tick_data_{inst.name}.parquet"
        buffers[inst.name] = TickBuffer(inst.name, parquet_path, cfg.flush_every_n_ticks, cfg.flush_every_sec)
        epic_to_name[epic] = inst.name
        logger.info("Instrument loaded: %s → epic=%s, parquet=%s", inst.name, epic, parquet_path.name)

    if not buffers:
        logger.error("Egy instrumentumhoz se sikerült epic-et felodani — leállás")
        return

    try:
        await asyncio.gather(
            ws_collect_ticks(client, epic_to_name, buffers),
            flush_loop(buffers),
        )
    finally:
        # Végleges flush
        for buf in buffers.values():
            try:
                buf.flush(force=True)
            except Exception:
                pass


def main() -> None:
    p = argparse.ArgumentParser(description="Multi-instrument Capital.com tick logger")
    p.add_argument("--config", default=str(ROOT / "data" / "tick_logger_config.yaml"),
                   help="YAML config útvonal")
    args = p.parse_args()
    cfg_path = Path(args.config)
    if not cfg_path.is_absolute():
        cfg_path = (ROOT / cfg_path).resolve()
    while True:
        try:
            asyncio.run(main_async(cfg_path))
        except KeyboardInterrupt:
            logger.info("Leállítás (Ctrl-C)")
            break
        except Exception as e:
            logger.exception("Fatal hiba: %s — 60s múlva újra", e)
            time.sleep(60)


if __name__ == "__main__":
    main()

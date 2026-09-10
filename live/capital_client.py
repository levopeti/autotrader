"""
Capital.com REST + WebSocket kliens.

A backtest engine config-jából érkező signal-ok itt válnak valódi pozíciókká.
A kliens **demo-számlán** dolgozik default-ban (API_BASE_URL_DEMO).
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
import websockets

from utils.keys import load_keys


logger = logging.getLogger("live.capital_client")

API_BASE_URL_DEMO = "https://demo-api-capital.backend-capital.com"
API_BASE_URL_LIVE = "https://api-capital.backend-capital.com"
WS_URL = "wss://api-streaming-capital.backend-capital.com/connect"


# A backtest TF-string → Capital REST resolution
TF_TO_RESOLUTION = {
    "1min": "MINUTE",
    "5min": "MINUTE_5",
    "15min": "MINUTE_15",
    "30min": "MINUTE_30",
    "1h": "HOUR",
    "4h": "HOUR_4",
    "1d": "DAY",
}


class CapitalClient:
    def __init__(self, demo: bool = True, account_id: Optional[str] = None):
        cfg = load_keys()
        self.api_key = cfg["capital_api_key"]
        self.identifier = cfg["capital_login"]
        self.password = cfg["capital_pw"]
        self.account_id = account_id or cfg.get("capital_account_id")
        self.api_base_url = (API_BASE_URL_DEMO if demo else API_BASE_URL_LIVE).rstrip("/")
        self.session = requests.Session()
        self.session.headers.update({"X-CAP-API-KEY": self.api_key, "Content-Type": "application/json"})
        self.cst: Optional[str] = None
        self.security_token: Optional[str] = None

    # ── REST ──

    def _request(self, method: str, path: str, _relogin_ok: bool = True, **kwargs):
        url = f"{self.api_base_url}{path}"
        r = self.session.request(method, url, timeout=30, **kwargs)
        # Lejárt session (401) → egyszeri re-login + retry. A /session POST-ra
        # magára nem retry-olunk (rekurzió-védelem).
        if r.status_code == 401 and _relogin_ok and path != "/api/v1/session":
            logger.warning("401 %s %s — session lejárt, re-login + retry", method, path)
            self.cst = None
            self.security_token = None
            self.ensure_login()
            if self.account_id:
                try:
                    self.ensure_account()
                except Exception as e:
                    logger.warning("re-login utáni account-switch hiba: %s", e)
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

    def ensure_account(self) -> None:
        if not self.account_id:
            return
        data = self._request("GET", "/api/v1/accounts").json()
        accounts = data.get("accounts", data) if isinstance(data, dict) else data
        valid = [str(a.get("accountId")) for a in accounts if isinstance(a, dict)]
        if str(self.account_id) not in valid:
            raise RuntimeError(f"Account not found: {self.account_id}, available: {valid}")
        current = str(self._request("GET", "/api/v1/session").json().get("accountId"))
        if current != str(self.account_id):
            self._request("PUT", "/api/v1/session", json={"accountId": str(self.account_id)})
            logger.info("Switched account to %s", self.account_id)

    def resolve_epic(self, symbol: str) -> str:
        data = self._request("GET", f"/api/v1/markets?searchTerm={symbol}").json()
        markets = data.get("markets", []) if isinstance(data, dict) else []
        if not markets:
            raise RuntimeError(f"No market for {symbol}")
        for m in markets:
            epic = m.get("epic", "")
            if symbol in epic or m.get("symbol") == symbol:
                return epic
        return markets[0].get("epic")

    def get_prices(self, epic: str, resolution: str, max_points: int = 200) -> pd.DataFrame:
        data = self._request(
            "GET", f"/api/v1/prices/{epic}?resolution={resolution}&max={max_points}"
        ).json()
        prices = data.get("prices", []) if isinstance(data, dict) else []
        rows = []
        for p in prices:
            o = p.get("openPrice", {})
            h = p.get("highPrice", {})
            l = p.get("lowPrice", {})
            c = p.get("closePrice", {})
            rows.append({
                "timestamp": p.get("snapshotTimeUTC") or p.get("snapshotTime"),
                "open":   o.get("bid") if o.get("bid") is not None else o.get("ask"),
                "high":   h.get("bid") if h.get("bid") is not None else h.get("ask"),
                "low":    l.get("bid") if l.get("bid") is not None else l.get("ask"),
                "close":  c.get("bid") if c.get("bid") is not None else c.get("ask"),
                "volume": p.get("lastTradedVolume", 0),
            })
        df = pd.DataFrame(rows)
        if df.empty:
            return df
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
        df = df.dropna(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)
        # Live runner-be tz-naive jön, hogy egyezzen a backtest searchsorted-jával
        df["timestamp"] = df["timestamp"].dt.tz_convert("UTC").dt.tz_localize(None)
        return df

    def get_balance(self, account_id: Optional[str] = None) -> Optional[float]:
        """Az adott (vagy a kliens) account aktuális balance-a."""
        aid = str(account_id or self.account_id or "")
        data = self._request("GET", "/api/v1/accounts").json()
        for a in data.get("accounts", []):
            if not aid or str(a.get("accountId")) == aid:
                b = a.get("balance", {})
                v = b.get("balance")
                return float(v) if v is not None else None
        return None

    def get_open_positions(self) -> List[Dict]:
        return self._request("GET", "/api/v1/positions").json().get("positions", [])

    def create_position(self, epic: str, direction: str, size: float,
                        stop_distance: float, profit_distance: Optional[float] = None,
                        trailing_stop: bool = False) -> Dict:
        """
        Pozíció nyitása. Distance-paramétereket adunk meg (a Capital az entry-től méri).
        Trailing stop: ha True, a Capital saját trailingStop-ja megy. Manuális
        break-even-hez False, és a runner.trailing mozgatja a SL-t REST PUT-tal.
        """
        payload = {
            "epic": epic,
            "direction": direction,
            "size": float(size),
            "trailingStop": bool(trailing_stop),
            "stopDistance": float(stop_distance),
        }
        if profit_distance is not None:
            payload["profitDistance"] = float(profit_distance)
        return self._request("POST", "/api/v1/positions", json=payload).json()

    def confirm_position(self, deal_reference: str) -> Dict:
        return self._request("GET", f"/api/v1/confirms/{deal_reference}").json()

    def confirm_position_with_retry(self, deal_reference: str,
                                     attempts: int = 5,
                                     delay_seconds: float = 0.5) -> Dict:
        """
        `/confirms/{ref}` race-safe wrapper. A Capital REST 404-et ad amíg a
        POST /positions után a confirmation nem elérhető — csak 404-en retry-olunk.
        """
        last_exc: Optional[Exception] = None
        for attempt in range(attempts):
            try:
                return self._request("GET", f"/api/v1/confirms/{deal_reference}").json()
            except requests.HTTPError as e:
                status = e.response.status_code if e.response is not None else None
                if status == 404 and attempt < attempts - 1:
                    last_exc = e
                    time.sleep(delay_seconds)
                    continue
                raise
        raise last_exc if last_exc else RuntimeError("confirm retry exhausted")

    def update_position(self, deal_id: str, stop_level: Optional[float] = None,
                        profit_level: Optional[float] = None) -> Dict:
        """SL/TP **abszolút szint**-mozgatás. Erre épül a manuális break-even/atr_trail."""
        payload: Dict = {}
        if stop_level is not None:
            payload["stopLevel"] = float(stop_level)
        if profit_level is not None:
            payload["profitLevel"] = float(profit_level)
        return self._request("PUT", f"/api/v1/positions/{deal_id}", json=payload).json()

    def close_position(self, deal_id: str) -> Dict:
        return self._request("DELETE", f"/api/v1/positions/{deal_id}").json()

    def get_transaction_for_deal(self, deal_id: str, last_period_sec: int = 86400) -> Optional[Dict]:
        """
        Egy lezárult pozíció pontos záró-adata: closeLevel, profitAndLoss, currency.
        Az utolsó N sec transactions-okon keres dealId-t. None ha nincs találat.
        """
        try:
            data = self._request(
                "GET",
                f"/api/v1/history/transactions?lastPeriod={int(last_period_sec)}&dealId={deal_id}",
            ).json()
        except Exception:
            return None
        items = data.get("transactions", []) if isinstance(data, dict) else []
        return items[0] if items else None

    # ── WebSocket tick subscribe ──

    async def stream_ticks(self, epic: str):
        """
        Async generátor: tick-stream az adott epic-re.
        Yield: dict {ts, bid, ask, source_ts, raw}
        A login + reconnect automatikus.
        """
        while True:
            try:
                self.ensure_login()
                async with websockets.connect(WS_URL, ping_interval=20, ping_timeout=20) as ws:
                    await ws.send(json.dumps({
                        "destination": "marketData.subscribe",
                        "correlationId": int(time.time()),
                        "cst": self.cst,
                        "securityToken": self.security_token,
                        "payload": {"epics": [epic]},
                    }))
                    ping_task = asyncio.create_task(self._ping_loop(ws))
                    try:
                        async for msg in ws:
                            data = json.loads(msg)
                            payload = data.get("payload", {})
                            if not isinstance(payload, dict):
                                continue
                            # Lehet hogy az `epic` kulcs alá van rakva, vagy közvetlen
                            tick = payload.get(epic) if epic in payload else payload
                            if not isinstance(tick, dict):
                                continue
                            bid = tick.get("bid")
                            ask = tick.get("offer") or tick.get("ask") or tick.get("ofr")
                            if bid is None or ask is None:
                                continue
                            yield {
                                "ts": datetime.now(timezone.utc),
                                "bid": float(bid),
                                "ask": float(ask),
                                "source_ts": tick.get("updateTimestamp") or tick.get("timestamp"),
                                "raw": tick,
                            }
                    finally:
                        ping_task.cancel()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning("WS stream hiba (%s) — 5s múlva újra", e)
                await asyncio.sleep(5)

    async def _ping_loop(self, ws):
        while True:
            await asyncio.sleep(20)
            try:
                await ws.send(json.dumps({
                    "destination": "ping",
                    "correlationId": int(time.time()),
                    "cst": self.cst,
                    "securityToken": self.security_token,
                }))
            except Exception:
                return

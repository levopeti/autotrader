import os
import json
import time
import math
import asyncio
import logging
from dataclasses import dataclass, asdict
from collections import deque
from datetime import datetime, timezone
from pathlib import Path

import requests
import pandas as pd
import websockets
import yaml

with open('../keys_urls.json', 'r') as f:
    config = json.load(f)

API_BASE_URL = "https://demo-api-capital.backend-capital.com"
WS_URL = "wss://api-streaming-capital.backend-capital.com/connect"
API_KEY = config["capital_api_key"]
IDENTIFIER = config["capital_login"]
PASSWORD = config["capital_pw"]
CAPITAL_ACCOUNT_ID = "320258870701535518"
CONFIG_PATH = os.getenv("BOT_CONFIG_PATH", "multi_instrument_config.yaml")

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("capital_multi_bot_v2")


@dataclass
class InstrumentConfig:
    symbol: str
    enabled: bool
    market_type: str
    order_size: float
    min_order_size: float
    max_order_size: float
    ema_fast: int
    ema_slow: int
    rsi_period: int
    atr_period: int
    rsi_long_level: float
    rsi_short_level: float
    reversal_rsi_long: float
    reversal_rsi_short: float
    tick_buffer_size: int
    tick_confirm_window: int
    tick_min_imbalance: float
    max_spread_atr: float
    trailing_stop_atr_mult: float
    min_stop_distance_abs: float
    enable_session_filter: bool
    trading_start_hour_utc: int
    trading_end_hour_utc: int


class ConfigLoader:
    def __init__(self, path):
        with open(path, "r", encoding="utf-8") as f:
            self.raw = yaml.safe_load(f)
        self.global_cfg = self.raw.get("global", {})
        self.instrument_cfgs = [InstrumentConfig(**x) for x in self.raw.get("instruments", []) if
                                x.get("enabled", False)]


class CapitalClient:
    def __init__(self, api_base_url):
        self.api_base_url = api_base_url.rstrip("/")
        self.session = requests.Session()
        self.session.headers.update({"X-CAP-API-KEY": API_KEY, "Content-Type": "application/json"})
        self.cst = None
        self.security_token = None

    def _request(self, method, path, **kwargs):
        url = f"{self.api_base_url}{path}"
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
        payload = {"identifier": IDENTIFIER, "password": PASSWORD, "encryptedPassword": False}
        r = self._request("POST", "/api/v1/session", json=payload)
        self.cst = r.headers.get("CST")
        self.security_token = r.headers.get("X-SECURITY-TOKEN")
        self.session.headers.update({"CST": self.cst, "X-SECURITY-TOKEN": self.security_token})
        logger.info("Logged in")

    def get_accounts(self):
        return self._request("GET", "/api/v1/accounts").json()

    def get_session(self):
        return self._request("GET", "/api/v1/session").json()

    def ensure_account(self, account_id):
        if not account_id:
            return
        accounts_raw = self.get_accounts()
        accounts = accounts_raw.get("accounts", accounts_raw) if isinstance(accounts_raw, dict) else accounts_raw
        valid = [str(x.get("accountId")) for x in accounts if isinstance(x, dict)]
        if str(account_id) not in valid:
            raise RuntimeError(f"Requested account not found: {account_id}; available={valid}")
        current = str(self.get_session().get("accountId"))
        if current != str(account_id):
            self._request("PUT", "/api/v1/session", json={"accountId": str(account_id)})
            logger.info("Switched account to %s", account_id)

    def resolve_epic(self, symbol):
        data = self._request("GET", f"/api/v1/markets?searchTerm={symbol}").json()
        markets = data.get("markets", []) if isinstance(data, dict) else []
        if not markets:
            raise RuntimeError(f"No market found for {symbol}")
        for m in markets:
            epic = m.get("epic", "")
            if symbol in epic or m.get("symbol") == symbol:
                return epic
        return markets[0].get("epic")

    def get_prices(self, epic, resolution, max_points):
        data = self._request("GET", f"/api/v1/prices/{epic}?resolution={resolution}&max={max_points}").json()
        prices = data.get("prices", []) if isinstance(data, dict) else []
        rows = []
        for p in prices:
            o = p.get("openPrice", {})
            h = p.get("highPrice", {})
            l = p.get("lowPrice", {})
            c = p.get("closePrice", {})
            rows.append({
                "timestamp": p.get("snapshotTimeUTC") or p.get("snapshotTime"),
                "Open": o.get("bid") if o.get("bid") is not None else o.get("ask"),
                "High": h.get("bid") if h.get("bid") is not None else h.get("ask"),
                "Low": l.get("bid") if l.get("bid") is not None else l.get("ask"),
                "Close": c.get("bid") if c.get("bid") is not None else c.get("ask"),
                "Volume": p.get("lastTradedVolume", 0),
            })
        df = pd.DataFrame(rows)
        if df.empty:
            return df
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
        return df.dropna().sort_values("timestamp").reset_index(drop=True)

    def get_open_positions(self):
        data = self._request("GET", "/api/v1/positions").json()
        return data.get("positions", []) if isinstance(data, dict) else []

    def create_position(self, epic, direction, size, stop_distance, trailing_stop=True):
        payload = {
            "epic": epic,
            "direction": direction,
            "size": float(size),
            "trailingStop": bool(trailing_stop),
            "stopDistance": float(stop_distance),
        }
        return self._request("POST", "/api/v1/positions", json=payload).json()

    def confirm_position(self, deal_reference):
        return self._request("GET", f"/api/v1/confirms/{deal_reference}").json()


class Journal:
    def __init__(self, cfg):
        self.log_dir = Path(cfg.get("log_dir", "."))
        self.log_dir.mkdir(parents=True, exist_ok=True)

        self.state_csv = str(self.log_dir / cfg["state_csv"])
        self.trade_journal_csv = str(self.log_dir / cfg["trade_journal_csv"])
        self.closed_trades_csv = str(self.log_dir / cfg["closed_trades_csv"])
        self.position_tracker_csv = str(self.log_dir / cfg["position_tracker_csv"])
        self.tick_csv = str(self.log_dir / cfg["tick_csv"])

        self._ensure_headers()

    def _ensure_headers(self):
        files = {
            self.state_csv: ["timestamp_utc", "instrument", "epic", "action", "bias", "direction", "price", "atr",
                             "spread", "tick_bias", "tick_imbalance", "score", "size", "allow_trade", "reason"],
            self.trade_journal_csv: ["timestamp_utc", "instrument", "epic", "event", "direction", "price", "size",
                                     "atr", "spread", "score", "note"],
            self.closed_trades_csv: ["open_time_utc", "close_time_utc", "instrument", "epic", "direction", "size",
                                     "entry_price", "exit_price", "pnl", "hold_minutes", "deal_id", "deal_reference",
                                     "source"],
            self.position_tracker_csv: ["deal_reference", "deal_id", "open_time_utc", "close_time_utc", "instrument",
                                        "epic", "direction", "size", "entry_price", "exit_price", "atr",
                                        "stop_distance", "score", "status", "reason"],
            self.tick_csv: ["timestamp_utc", "instrument", "epic", "bid", "ask", "mid", "spread", "tick_source_ts",
                            "raw_payload"],
        }
        for p, cols in files.items():
            if not Path(p).exists():
                pd.DataFrame(columns=cols).to_csv(p, index=False)

    def append(self, path, row):
        pd.DataFrame([row]).to_csv(path, mode="a", header=False, index=False)

    def load_df(self, path):
        if not Path(path).exists():
            return pd.DataFrame()
        return pd.read_csv(path, dtype=str, keep_default_na=False)

    def upsert_position(self, row):
        df = self.load_df(self.position_tracker_csv)
        if df.empty:
            pd.DataFrame([row]).to_csv(self.position_tracker_csv, index=False)
            return
        for k in row.keys():
            if k in df.columns:
                df[k] = df[k].astype("object")
        mask = df["deal_reference"].astype(str) == str(row["deal_reference"])
        if mask.any():
            for k, v in row.items():
                df.loc[mask, k] = v
        else:
            df = pd.concat([df, pd.DataFrame([row])], ignore_index=True)
        df.to_csv(self.position_tracker_csv, index=False)

    def close_position_row(self, deal_reference, exit_price, close_time, reason):
        df = self.load_df(self.position_tracker_csv)
        if df.empty:
            return None
        for col in ["close_time_utc", "exit_price", "reason", "status"]:
            if col in df.columns:
                df[col] = df[col].astype("object")
        mask = (df["deal_reference"].astype(str) == str(deal_reference)) & (df["status"] == "OPEN")
        if not mask.any():
            return None
        idx = df[mask].index[0]
        row = df.loc[idx].to_dict()
        df.loc[idx, "status"] = "CLOSED"
        df.loc[idx, "close_time_utc"] = close_time
        df.loc[idx, "exit_price"] = exit_price
        df.loc[idx, "reason"] = reason
        df.to_csv(self.position_tracker_csv, index=False)
        row["close_time_utc"] = close_time
        row["exit_price"] = exit_price
        row["reason"] = reason
        row["status"] = "CLOSED"
        return row

    def get_open_position_rows(self, instrument=None):
        df = self.load_df(self.position_tracker_csv)
        if df.empty:
            return df
        df = df[df["status"] == "OPEN"].copy()
        if instrument is not None:
            df = df[df["instrument"] == instrument].copy()
        return df


def ema(series, span):
    return series.ewm(span=span, adjust=False).mean()


def rsi(series, period):
    delta = series.diff()
    up = delta.clip(lower=0)
    down = -delta.clip(upper=0)
    ma_up = up.ewm(alpha=1 / period, adjust=False).mean()
    ma_down = down.ewm(alpha=1 / period, adjust=False).mean()
    rs = ma_up / ma_down.replace(0, math.nan)
    return 100 - (100 / (1 + rs))


def atr(df, period):
    high = df["High"]
    low = df["Low"]
    close = df["Close"]
    prev_close = close.shift(1)
    tr = pd.concat([high - low, (high - prev_close).abs(), (low - prev_close).abs()], axis=1).max(axis=1)
    return tr.rolling(period).mean()


def clamp(x, lo, hi):
    return max(lo, min(hi, x))


class TickMomentum:
    def __init__(self, cfg):
        self.cfg = cfg
        self.signs = deque(maxlen=cfg.tick_buffer_size)
        self.mid_prices = deque(maxlen=cfg.tick_buffer_size)
        self.last_mid = None
        self.last_spread = None

    def update(self, bid, ask):
        mid = (bid + ask) / 2.0
        spread = ask - bid
        if self.last_mid is not None:
            d = mid - self.last_mid
            self.signs.append(1 if d > 0 else -1 if d < 0 else 0)
        self.mid_prices.append(mid)
        self.last_mid = mid
        self.last_spread = spread

    def summary(self):
        if len(self.signs) < max(3, self.cfg.tick_confirm_window):
            return {"tick_bias": "NEUTRAL", "imbalance": 0.0, "spread": self.last_spread}
        window = list(self.signs)[-self.cfg.tick_confirm_window:]
        nz = [x for x in window if x != 0]
        if not nz:
            return {"tick_bias": "NEUTRAL", "imbalance": 0.0, "spread": self.last_spread}
        imbalance = sum(nz) / len(nz)
        bias = "BUY" if imbalance >= self.cfg.tick_min_imbalance else "SELL" if imbalance <= -self.cfg.tick_min_imbalance else "NEUTRAL"
        return {"tick_bias": bias, "imbalance": imbalance, "spread": self.last_spread}


class InstrumentRuntime:
    def __init__(self, cfg, global_cfg, epic):
        self.cfg = cfg
        self.global_cfg = global_cfg
        self.epic = epic
        self.tick_state = TickMomentum(cfg)
        self.tick_buffer = []
        self.last_tick_flush = time.time()
        self.last_trade_ts = 0

    def within_hours(self):
        if not self.cfg.enable_session_filter:
            return True
        hour = datetime.now(timezone.utc).hour
        return self.cfg.trading_start_hour_utc <= hour < self.cfg.trading_end_hour_utc


def derive_signal(df, cfg):
    x = df.copy()
    x["ema_fast"] = ema(x["Close"], cfg.ema_fast)
    x["ema_slow"] = ema(x["Close"], cfg.ema_slow)
    x["rsi"] = rsi(x["Close"], cfg.rsi_period)
    x["atr"] = atr(x, cfg.atr_period)
    row = x.iloc[-1]
    price = float(row["Close"])
    atr_v = float(row["atr"])
    ef = float(row["ema_fast"])
    es = float(row["ema_slow"])
    rv = float(row["rsi"])
    action, bias, direction = "HOLD", "NO_EDGE", None
    if price > es and ef > es and rv >= cfg.rsi_long_level:
        action, bias, direction = "BUY_TREND", "LONG_BIAS", "BUY"
    elif price < es and ef < es and rv <= cfg.rsi_short_level:
        action, bias, direction = "SELL_TREND", "SHORT_BIAS", "SELL"
    elif price > es and rv >= cfg.reversal_rsi_long:
        action, bias, direction = "BUY_REVERSAL", "LONG_BIAS", "BUY"
    elif price < es and rv <= cfg.reversal_rsi_short:
        action, bias, direction = "SELL_REVERSAL", "SHORT_BIAS", "SELL"
    return {"action": action, "bias": bias, "direction": direction, "price": price, "atr": atr_v, "ema_fast": ef,
            "ema_slow": es, "rsi": rv}


def compute_score(signal, tick, spread_ratio):
    score = 0.0
    if signal["action"] in ["BUY_TREND", "SELL_TREND"]:
        score += 0.45
    elif signal["action"] in ["BUY_REVERSAL", "SELL_REVERSAL"]:
        score += 0.25
    score += min(0.30, abs(float(tick.get("imbalance", 0.0))) * 0.30)
    score += max(0.0, 0.25 - spread_ratio)
    return clamp(score, 0.0, 1.0)


def size_from_score(score, cfg):
    span = cfg.max_order_size - cfg.min_order_size
    return round(cfg.min_order_size + span * score, 6)


def stop_distance_from_atr(atr_value, cfg):
    return max(cfg.min_stop_distance_abs, atr_value * cfg.trailing_stop_atr_mult)


def calc_pnl(direction, entry_price, exit_price, size):
    if direction == "BUY":
        return (exit_price - entry_price) * size
    return (entry_price - exit_price) * size


def flush_ticks(runtime, journal):
    if not runtime.tick_buffer:
        return
    enough_rows = len(runtime.tick_buffer) >= runtime.global_cfg["flush_every_n_ticks"]
    enough_time = (time.time() - runtime.last_tick_flush) >= runtime.global_cfg["flush_every_sec"]
    if not (enough_rows or enough_time):
        return
    pd.DataFrame(runtime.tick_buffer).to_csv(journal.tick_csv, mode="a", header=False, index=False)
    runtime.tick_buffer = []
    runtime.last_tick_flush = time.time()


def positions_by_instrument(client):
    out = {}
    try:
        positions = client.get_open_positions()
    except Exception:
        return out
    for p in positions:
        pos = p.get("position", {}) if isinstance(p, dict) else {}
        market = p.get("market", {}) if isinstance(p, dict) else {}
        epic = market.get("epic") or pos.get("epic")
        out.setdefault(epic, []).append(p)
    return out


async def ping_loop(ws, cst, security_token):
    while True:
        await asyncio.sleep(20)
        await ws.send(json.dumps(
            {"destination": "ping", "correlationId": int(time.time()), "cst": cst, "securityToken": security_token}))


async def ws_loop(client, runtime, journal, ws_url):
    while True:
        try:
            client.ensure_login()
            async with websockets.connect(ws_url, ping_interval=20, ping_timeout=20) as ws:
                await ws.send(json.dumps({
                    "destination": "marketData.subscribe",
                    "correlationId": int(time.time()),
                    "cst": client.cst,
                    "securityToken": client.security_token,
                    "payload": {"epics": [runtime.epic]},
                }))
                ping_task = asyncio.create_task(ping_loop(ws, client.cst, client.security_token))
                try:
                    async for message in ws:
                        data = json.loads(message)
                        payload = data.get("payload", {})
                        tick_payload = payload.get(runtime.epic) if isinstance(payload,
                                                                               dict) and runtime.epic in payload else payload if isinstance(
                            payload, dict) else None
                        if not tick_payload:
                            continue
                        bid = tick_payload.get("bid")
                        ask = tick_payload.get("offer") or tick_payload.get("ask") or tick_payload.get("ofr")
                        if bid is None or ask is None:
                            continue
                        bid, ask = float(bid), float(ask)
                        runtime.tick_state.update(bid, ask)
                        runtime.tick_buffer.append({
                            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
                            "instrument": runtime.cfg.symbol,
                            "epic": runtime.epic,
                            "bid": bid,
                            "ask": ask,
                            "mid": (bid + ask) / 2.0,
                            "spread": ask - bid,
                            "tick_source_ts": tick_payload.get("updateTimestamp") or tick_payload.get("timestamp"),
                            "raw_payload": json.dumps(tick_payload, ensure_ascii=False),
                        })
                        flush_ticks(runtime, journal)
                finally:
                    ping_task.cancel()
        except Exception as e:
            logger.warning("WS loop error %s | %s", runtime.cfg.symbol, e)
            flush_ticks(runtime, journal)
            await asyncio.sleep(5)


async def reconcile_loop(client, runtime, journal, interval_sec=30):
    while True:
        try:
            live = positions_by_instrument(client).get(runtime.epic, [])
            live_refs = set()
            for p in live:
                pos = p.get("position", {})
                market = p.get("market", {})
                deal_ref = pos.get("dealReference") or pos.get("dealId")
                live_refs.add(str(deal_ref))
                row = {
                    "deal_reference": deal_ref,
                    "deal_id": pos.get("dealId"),
                    "open_time_utc": pos.get("createdDateUTC") or pos.get("createdDate"),
                    "close_time_utc": None,
                    "instrument": runtime.cfg.symbol,
                    "epic": runtime.epic,
                    "direction": pos.get("direction"),
                    "size": pos.get("size"),
                    "entry_price": pos.get("level"),
                    "exit_price": None,
                    "atr": None,
                    "stop_distance": pos.get("stopDistance"),
                    "score": None,
                    "status": "OPEN",
                    "reason": "broker_sync",
                }
                journal.upsert_position(row)
            open_rows = journal.get_open_position_rows(runtime.cfg.symbol)
            if not open_rows.empty:
                for _, row in open_rows.iterrows():
                    deal_ref = str(row["deal_reference"])
                    if deal_ref not in live_refs:
                        exit_price = runtime.tick_state.last_mid if runtime.tick_state.last_mid is not None else row.get(
                            "entry_price")
                        closed = journal.close_position_row(deal_ref, exit_price,
                                                            datetime.now(timezone.utc).isoformat(),
                                                            "reconcile_missing_from_live")
                        if closed:
                            entry_price = float(closed.get("entry_price") or 0.0)
                            exit_price = float(closed.get("exit_price") or entry_price)
                            size = float(closed.get("size") or 0.0)
                            pnl = calc_pnl(closed.get("direction"), entry_price, exit_price, size)
                            open_ts = pd.to_datetime(closed.get("open_time_utc"), utc=True, errors="coerce")
                            close_ts = pd.to_datetime(closed.get("close_time_utc"), utc=True, errors="coerce")
                            hold_minutes = (close_ts - open_ts).total_seconds() / 60 if pd.notna(open_ts) and pd.notna(
                                close_ts) else None
                            journal.append(journal.closed_trades_csv, {
                                "open_time_utc": closed.get("open_time_utc"),
                                "close_time_utc": closed.get("close_time_utc"),
                                "instrument": closed.get("instrument"),
                                "epic": closed.get("epic"),
                                "direction": closed.get("direction"),
                                "size": size,
                                "entry_price": entry_price,
                                "exit_price": exit_price,
                                "pnl": pnl,
                                "hold_minutes": hold_minutes,
                                "deal_id": closed.get("deal_id"),
                                "deal_reference": closed.get("deal_reference"),
                                "source": "reconcile_loop",
                            })
        except Exception as e:
            logger.warning("Reconcile error %s | %s", runtime.cfg.symbol, e)
        await asyncio.sleep(interval_sec)


async def decision_loop(client, runtime, journal, global_cfg):
    cooldown_sec = global_cfg.get("decision_interval_sec", 15)
    while True:
        try:
            client.ensure_login()
            prices = client.get_prices(runtime.epic, global_cfg["resolution"], global_cfg["num_points"])
            if prices.empty or len(prices) < max(runtime.cfg.ema_slow, runtime.cfg.atr_period) + 5:
                await asyncio.sleep(cooldown_sec)
                continue
            signal = derive_signal(prices, runtime.cfg)
            tick = runtime.tick_state.summary()
            spread = float(tick.get("spread") or 0.0)
            atr_v = float(signal.get("atr") or 0.0)
            spread_ratio = (spread / atr_v) if atr_v > 0 else 999.0
            score = compute_score(signal, tick, spread_ratio)
            size = size_from_score(score, runtime.cfg)
            open_tracker = journal.get_open_position_rows(runtime.cfg.symbol)
            allow_one = global_cfg.get("allow_only_one_position_per_instrument", True)
            has_open_tracker = not open_tracker.empty
            broker_positions = positions_by_instrument(client).get(runtime.epic, [])
            has_open_broker = len(broker_positions) > 0
            already_open = has_open_tracker or has_open_broker
            tick_match = tick.get("tick_bias") == signal.get("direction")
            signal_ok = signal.get("direction") in ["BUY", "SELL"]
            spread_ok = spread_ratio <= runtime.cfg.max_spread_atr
            hours_ok = runtime.within_hours()
            allow_trade = hours_ok and signal_ok and tick_match and spread_ok and (not already_open or not allow_one)
            reason = []
            if not hours_ok:
                reason.append("outside_hours")
            if not signal_ok:
                reason.append("no_direction")
            if signal_ok and not tick_match:
                reason.append("tick_mismatch")
            if not spread_ok:
                reason.append("spread_too_wide")
            if already_open and allow_one:
                reason.append("position_exists")
            if score < 0.35:
                reason.append("score_too_low")
                allow_trade = False
            reason_s = ";".join(reason) if reason else "ok"
            journal.append(journal.state_csv, {
                "timestamp_utc": datetime.now(timezone.utc).isoformat(),
                "instrument": runtime.cfg.symbol,
                "epic": runtime.epic,
                "action": signal["action"],
                "bias": signal["bias"],
                "direction": signal["direction"],
                "price": signal["price"],
                "atr": signal["atr"],
                "spread": spread,
                "tick_bias": tick["tick_bias"],
                "tick_imbalance": tick["imbalance"],
                "score": score,
                "size": size,
                "allow_trade": allow_trade,
                "reason": reason_s,
            })
            if allow_trade:
                stop_distance = stop_distance_from_atr(atr_v, runtime.cfg)
                note = f"score={score:.3f};spread_ratio={spread_ratio:.4f};stop_distance={stop_distance:.4f};cfg={json.dumps(asdict(runtime.cfg), ensure_ascii=False)}"
                if global_cfg.get("dry_run", True):
                    deal_reference = f"DRY-{runtime.cfg.symbol}-{int(time.time())}"
                    open_time = datetime.now(timezone.utc).isoformat()
                    journal.append(journal.trade_journal_csv, {
                        "timestamp_utc": open_time,
                        "instrument": runtime.cfg.symbol,
                        "epic": runtime.epic,
                        "event": "OPEN",
                        "direction": signal["direction"],
                        "price": signal["price"],
                        "size": size,
                        "atr": atr_v,
                        "spread": spread,
                        "score": score,
                        "note": f"dry_run=true;{note}",
                    })
                    journal.upsert_position({
                        "deal_reference": deal_reference,
                        "deal_id": None,
                        "open_time_utc": open_time,
                        "close_time_utc": None,
                        "instrument": runtime.cfg.symbol,
                        "epic": runtime.epic,
                        "direction": signal["direction"],
                        "size": size,
                        "entry_price": signal["price"],
                        "exit_price": None,
                        "atr": atr_v,
                        "stop_distance": stop_distance,
                        "score": score,
                        "status": "OPEN",
                        "reason": "dry_run_open",
                    })
                else:
                    response = client.create_position(runtime.epic, signal["direction"], size, stop_distance,
                                                      trailing_stop=global_cfg.get("use_trailing_stop", True))
                    deal_reference = response.get("dealReference")
                    confirm = client.confirm_position(deal_reference) if deal_reference else {}
                    deal_id = confirm.get("dealId")
                    entry_level = confirm.get("level", signal["price"])
                    open_time = datetime.now(timezone.utc).isoformat()
                    journal.append(journal.trade_journal_csv, {
                        "timestamp_utc": open_time,
                        "instrument": runtime.cfg.symbol,
                        "epic": runtime.epic,
                        "event": "OPEN",
                        "direction": signal["direction"],
                        "price": entry_level,
                        "size": size,
                        "atr": atr_v,
                        "spread": spread,
                        "score": score,
                        "note": f"deal_reference={deal_reference};confirm={json.dumps(confirm, ensure_ascii=False)};{note}",
                    })
                    journal.upsert_position({
                        "deal_reference": deal_reference,
                        "deal_id": deal_id,
                        "open_time_utc": open_time,
                        "close_time_utc": None,
                        "instrument": runtime.cfg.symbol,
                        "epic": runtime.epic,
                        "direction": signal["direction"],
                        "size": size,
                        "entry_price": entry_level,
                        "exit_price": None,
                        "atr": atr_v,
                        "stop_distance": stop_distance,
                        "score": score,
                        "status": "OPEN",
                        "reason": "broker_open",
                    })
        except Exception as e:
            logger.exception("Decision loop error %s | %s", runtime.cfg.symbol, e)
        await asyncio.sleep(cooldown_sec)


async def main():
    if not API_KEY or not IDENTIFIER or not PASSWORD:
        raise RuntimeError("Missing CAPITAL_API_KEY / CAPITAL_IDENTIFIER / CAPITAL_PASSWORD")
    cfg = ConfigLoader(CONFIG_PATH)
    client = CapitalClient(cfg.global_cfg["api_base_url"])
    client.ensure_login()
    client.ensure_account(CAPITAL_ACCOUNT_ID)
    journal = Journal(cfg.global_cfg)
    tasks = []
    for inst in cfg.instrument_cfgs:
        epic = client.resolve_epic(inst.symbol)
        runtime = InstrumentRuntime(inst, cfg.global_cfg, epic)
        logger.info("Instrument loaded | %s | epic=%s", inst.symbol, epic)
        tasks.extend([
            asyncio.create_task(ws_loop(client, runtime, journal, cfg.global_cfg["ws_url"])),
            asyncio.create_task(decision_loop(client, runtime, journal, cfg.global_cfg)),
            asyncio.create_task(reconcile_loop(client, runtime, journal, interval_sec=30)),
        ])
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())

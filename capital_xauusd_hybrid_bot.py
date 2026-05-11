import os
import time
import math
import csv
import json
import asyncio
import logging
from datetime import datetime, timezone, date
from collections import deque

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
RESOLUTION = os.getenv("RESOLUTION", "MINUTE")
NUM_POINTS = int(os.getenv("NUM_POINTS", "200"))

EMA_FAST = int(os.getenv("EMA_FAST", "9"))
EMA_SLOW = int(os.getenv("EMA_SLOW", "21"))
RSI_PERIOD = int(os.getenv("RSI_PERIOD", "7"))
ATR_PERIOD = int(os.getenv("ATR_PERIOD", "14"))
RSI_LONG_LEVEL = float(os.getenv("RSI_LONG_LEVEL", "55"))
RSI_SHORT_LEVEL = float(os.getenv("RSI_SHORT_LEVEL", "45"))
REVERSAL_RSI_LONG = float(os.getenv("REVERSAL_RSI_LONG", "56"))
REVERSAL_RSI_SHORT = float(os.getenv("REVERSAL_RSI_SHORT", "44"))

TICK_BUFFER_SIZE = int(os.getenv("TICK_BUFFER_SIZE", "20"))
TICK_CONFIRM_WINDOW = int(os.getenv("TICK_CONFIRM_WINDOW", "5"))
TICK_MIN_IMBALANCE = float(os.getenv("TICK_MIN_IMBALANCE", "0.50"))
MAX_SPREAD_ATR = float(os.getenv("MAX_SPREAD_ATR", "0.20"))

BASE_ORDER_SIZE = float(os.getenv("ORDER_SIZE", "1"))
MIN_ORDER_SIZE = float(os.getenv("MIN_ORDER_SIZE", "0.5"))
MAX_ORDER_SIZE = float(os.getenv("MAX_ORDER_SIZE", "1.5"))
DRY_RUN = False
DECISION_INTERVAL_SEC = int(os.getenv("DECISION_INTERVAL_SEC", "15"))
MAX_DAILY_TRADES = int(os.getenv("MAX_DAILY_TRADES", "10"))
ALLOW_ONLY_ONE_POSITION = os.getenv("ALLOW_ONLY_ONE_POSITION", "true").lower() == "true"

USE_TRAILING_STOP = os.getenv("USE_TRAILING_STOP", "true").lower() == "true"
TRAILING_STOP_ATR_MULT = float(os.getenv("TRAILING_STOP_ATR_MULT", "1.5"))
MIN_STOP_DISTANCE_ABS = float(os.getenv("MIN_STOP_DISTANCE_ABS", "0.5"))

TRADING_START_HOUR_UTC = int(os.getenv("TRADING_START_HOUR_UTC", "7"))
TRADING_END_HOUR_UTC = int(os.getenv("TRADING_END_HOUR_UTC", "18"))
ENABLE_SESSION_FILTER = os.getenv("ENABLE_SESSION_FILTER", "false").lower() == "true"

BOT_STATE_CSV = os.getenv("BOT_STATE_CSV", "bot_state.csv")
TRADE_JOURNAL_CSV = os.getenv("TRADE_JOURNAL_CSV", "trade_journal.csv")
CLOSED_TRADES_CSV = os.getenv("CLOSED_TRADES_CSV", "closed_trades.csv")
POSITION_TRACKER_CSV = os.getenv("POSITION_TRACKER_CSV", "position_tracker.csv")

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("capital_xauusd_hybrid_bot_full")


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
        logger.info("Logged in. accountId=%s trailingStopsEnabled=%s", self.current_account_id, data.get("trailingStopsEnabled"))

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
        r = self._request("PUT", "/api/v1/session", json=payload)
        body = r.json() if r.content else {}
        logger.info("Switch response: %s", body)
        session = self.get_session_details()
        self.current_account_id = session.get("accountId")
        logger.info("Current account after switch: %s", self.current_account_id)
        return body

    def ensure_account(self, account_id):
        if not account_id:
            return
        accounts = self.get_accounts()
        account_ids = [str(a.get("accountId")) for a in accounts if isinstance(a, dict)]
        if str(account_id) not in account_ids:
            raise RuntimeError(f"Requested CAPITAL_ACCOUNT_ID {account_id} not found in available accounts: {account_ids}")
        session = self.get_session_details()
        current_account_id = str(session.get("accountId"))
        if current_account_id == str(account_id):
            logger.info("Already on requested accountId=%s, no switch needed", account_id)
            self.current_account_id = current_account_id
            return
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

    def get_prices(self, epic, resolution=RESOLUTION, max_points=NUM_POINTS):
        r = self._request("GET", f"/api/v1/prices/{epic}?resolution={resolution}&max={max_points}")
        data = r.json()
        prices = data.get("prices", [])
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
        df = df.dropna().sort_values("timestamp").reset_index(drop=True)
        return df

    def get_open_positions(self):
        r = self._request("GET", "/api/v1/positions")
        data = r.json()
        if isinstance(data, dict):
            return data.get("positions", [])
        return []

    def get_activity_history(self, last_period=3600, detailed=True, deal_id=None):
        params = {
            "lastPeriod": int(last_period),
            "detailed": str(detailed).lower(),
        }
        if deal_id:
            params["dealId"] = deal_id
        q = "&".join(f"{k}={v}" for k, v in params.items())
        r = self._request("GET", f"/api/v1/history/activity?{q}")
        return r.json()

    def create_position(self, epic, direction, size, stop_distance, trailing_stop=True):
        payload = {
            "epic": epic,
            "direction": direction,
            "size": size,
            "trailingStop": bool(trailing_stop),
            "stopDistance": float(stop_distance),
        }
        r = self._request("POST", "/api/v1/positions", json=payload)
        body = r.json()
        logger.info("Create position response: %s", body)
        return body

    def confirm_position(self, deal_reference):
        r = self._request("GET", f"/api/v1/confirms/{deal_reference}")
        return r.json()


def ema(series, span):
    return series.ewm(span=span, adjust=False).mean()


def rsi(series, period=14):
    delta = series.diff()
    up = delta.clip(lower=0)
    down = -delta.clip(upper=0)
    ma_up = up.ewm(alpha=1 / period, adjust=False).mean()
    ma_down = down.ewm(alpha=1 / period, adjust=False).mean()
    rs = ma_up / ma_down.replace(0, math.nan)
    return 100 - (100 / (1 + rs))


def atr(df, period=14):
    high = df["High"]
    low = df["Low"]
    close = df["Close"]
    prev_close = close.shift(1)
    tr = pd.concat([
        high - low,
        (high - prev_close).abs(),
        (low - prev_close).abs(),
    ], axis=1).max(axis=1)
    return tr.rolling(period).mean()


class TickMomentum:
    def __init__(self, maxlen=TICK_BUFFER_SIZE):
        self.signs = deque(maxlen=maxlen)
        self.mid_prices = deque(maxlen=maxlen)
        self.last_mid = None
        self.last_spread = None

    def update(self, bid, ask):
        mid = (bid + ask) / 2.0
        spread = ask - bid
        if self.last_mid is not None:
            delta = mid - self.last_mid
            if delta > 0:
                self.signs.append(1)
            elif delta < 0:
                self.signs.append(-1)
            else:
                self.signs.append(0)
        self.mid_prices.append(mid)
        self.last_mid = mid
        self.last_spread = spread

    def summary(self):
        if len(self.signs) < max(3, TICK_CONFIRM_WINDOW):
            return {"tick_bias": "NEUTRAL", "imbalance": 0.0, "momentum": 0.0, "spread": self.last_spread}
        window = list(self.signs)[-TICK_CONFIRM_WINDOW:]
        non_zero = [s for s in window if s != 0]
        if not non_zero:
            return {"tick_bias": "NEUTRAL", "imbalance": 0.0, "momentum": 0.0, "spread": self.last_spread}
        imbalance = sum(non_zero) / len(non_zero)
        if imbalance >= TICK_MIN_IMBALANCE:
            tick_bias = "BUY"
        elif imbalance <= -TICK_MIN_IMBALANCE:
            tick_bias = "SELL"
        else:
            tick_bias = "NEUTRAL"
        prices = list(self.mid_prices)[-TICK_CONFIRM_WINDOW:]
        momentum = prices[-1] - prices[0] if len(prices) >= 2 else 0.0
        return {"tick_bias": tick_bias, "imbalance": imbalance, "momentum": momentum, "spread": self.last_spread}


class Journal:
    def __init__(self, state_csv=BOT_STATE_CSV, trade_csv=TRADE_JOURNAL_CSV, closed_csv=CLOSED_TRADES_CSV, tracker_csv=POSITION_TRACKER_CSV):
        self.state_csv = state_csv
        self.trade_csv = trade_csv
        self.closed_csv = closed_csv
        self.tracker_csv = tracker_csv
        self._ensure_headers()

    def _ensure_headers(self):
        if not os.path.exists(self.state_csv):
            pd.DataFrame(columns=[
                "timestamp_utc", "epic", "action", "bias", "fresh_reversal", "direction",
                "price", "atr", "spread", "tick_bias", "tick_imbalance", "daily_trades", "daily_pnl_est"
            ]).to_csv(self.state_csv, index=False)
        if not os.path.exists(self.trade_csv):
            pd.DataFrame(columns=[
                "timestamp_utc", "event", "epic", "direction", "price", "size", "atr", "spread", "note"
            ]).to_csv(self.trade_csv, index=False)
        if not os.path.exists(self.closed_csv):
            pd.DataFrame(columns=[
                "open_time_utc", "close_time_utc", "epic", "direction", "size", "entry_price", "exit_price",
                "pnl", "hold_minutes", "deal_id", "deal_reference", "source"
            ]).to_csv(self.closed_csv, index=False)
        if not os.path.exists(self.tracker_csv):
            pd.DataFrame(columns=[
                "deal_reference", "deal_id", "open_time_utc", "epic", "direction", "size", "entry_price",
                "atr", "stop_distance", "signal_score", "size_multiplier", "status", "close_time_utc"
            ]).to_csv(self.tracker_csv, index=False)

    def log_state(self, row):
        pd.DataFrame([row]).to_csv(self.state_csv, mode="a", header=False, index=False)

    def log_trade(self, row):
        pd.DataFrame([row]).to_csv(self.trade_csv, mode="a", header=False, index=False)

    def daily_trade_count(self):
        if not os.path.exists(self.trade_csv):
            return 0
        df = pd.read_csv(self.trade_csv)
        if df.empty:
            return 0
        df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], utc=True, errors="coerce")
        today = datetime.now(timezone.utc).date()
        return int(((df["timestamp_utc"].dt.date == today) & (df["event"] == "OPEN")).sum())

    def daily_pnl_est(self):
        if not os.path.exists(self.closed_csv):
            return 0.0
        df = pd.read_csv(self.closed_csv)
        if df.empty or "close_time_utc" not in df.columns or "pnl" not in df.columns:
            return 0.0
        df["close_time_utc"] = pd.to_datetime(df["close_time_utc"], utc=True, errors="coerce")
        df["pnl"] = pd.to_numeric(df["pnl"], errors="coerce")
        today = datetime.now(timezone.utc).date()
        return float(df.loc[df["close_time_utc"].dt.date == today, "pnl"].sum())

    def add_position_tracker(self, row):
        pd.DataFrame([row]).to_csv(self.tracker_csv, mode="a", header=False, index=False)

    def load_position_tracker(self):
        if not os.path.exists(self.tracker_csv):
            return pd.DataFrame()
        df = pd.read_csv(self.tracker_csv)
        if df.empty:
            return df
        for col in ["open_time_utc", "close_time_utc"]:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], utc=True, errors="coerce")
        return df

    def save_position_tracker(self, df):
        df.to_csv(self.tracker_csv, index=False)

    def append_closed_trade(self, row):
        pd.DataFrame([row]).to_csv(self.closed_csv, mode="a", header=False, index=False)


def derive_signal(df):
    df = df.copy()
    df["ema_fast"] = ema(df["Close"], EMA_FAST)
    df["ema_slow"] = ema(df["Close"], EMA_SLOW)
    df["rsi"] = rsi(df["Close"], RSI_PERIOD)
    df["atr"] = atr(df, ATR_PERIOD)
    row = df.iloc[-1]
    price = float(row["Close"])
    ema_fast_v = float(row["ema_fast"])
    ema_slow_v = float(row["ema_slow"])
    rsi_v = float(row["rsi"])
    atr_v = float(row["atr"])

    action = "HOLD"
    bias = "NO_EDGE"
    fresh_reversal = False
    direction = None

    if price > ema_slow_v and ema_fast_v > ema_slow_v and rsi_v >= RSI_LONG_LEVEL:
        action = "BUY_TREND"
        bias = "LONG_BIAS"
        direction = "BUY"
    elif price < ema_slow_v and ema_fast_v < ema_slow_v and rsi_v <= RSI_SHORT_LEVEL:
        action = "SELL_TREND"
        bias = "SHORT_BIAS"
        direction = "SELL"
    elif price > ema_slow_v and rsi_v >= REVERSAL_RSI_LONG:
        action = "BUY_REVERSAL"
        bias = "LONG_BIAS"
        direction = "BUY"
        fresh_reversal = True
    elif price < ema_slow_v and rsi_v <= REVERSAL_RSI_SHORT:
        action = "SELL_REVERSAL"
        bias = "SHORT_BIAS"
        direction = "SELL"
        fresh_reversal = True

    return {
        "action": action,
        "bias": bias,
        "fresh_reversal": fresh_reversal,
        "direction": direction,
        "price": price,
        "atr": atr_v,
    }


def calc_stop_distance(atr_value):
    return max(MIN_STOP_DISTANCE_ABS, atr_value * TRAILING_STOP_ATR_MULT)


def is_within_trading_hours(now_utc):
    if not ENABLE_SESSION_FILTER:
        return True
    hour = now_utc.hour
    return TRADING_START_HOUR_UTC <= hour < TRADING_END_HOUR_UTC


def clamp(v, lo, hi):
    return max(lo, min(hi, v))


def compute_signal_score(signal, tick, spread_atr_ratio):
    score = 0
    if signal["action"] in ["BUY_TREND", "SELL_TREND"]:
        score += 2
    elif signal["action"] in ["BUY_REVERSAL", "SELL_REVERSAL"]:
        score += 1

    abs_imb = abs(float(tick.get("imbalance") or 0.0))
    if abs_imb >= 1.0:
        score += 2
    elif abs_imb >= 0.6:
        score += 1

    if spread_atr_ratio <= 0.13:
        score += 1
    elif spread_atr_ratio <= 0.18:
        score += 0.5

    return float(score)


def size_from_score(score):
    if score >= 5:
        mult = 1.5
    elif score >= 3:
        mult = 1.0
    elif score >= 2:
        mult = 0.5
    else:
        mult = 0.0
    size = clamp(BASE_ORDER_SIZE * mult, MIN_ORDER_SIZE, MAX_ORDER_SIZE) if mult > 0 else 0.0
    return mult, size


def flatten_positions(raw_positions):
    rows = []
    for item in raw_positions:
        if not isinstance(item, dict):
            continue
        pos = item.get("position", item)
        market = item.get("market", {})
        rows.append({
            "dealId": pos.get("dealId"),
            "direction": pos.get("direction"),
            "size": pos.get("size"),
            "level": pos.get("level"),
            "createdDateUTC": pos.get("createdDateUTC") or pos.get("createdDate"),
            "epic": market.get("epic") or pos.get("epic"),
        })
    return pd.DataFrame(rows)


def extract_confirm_deal_id(confirm):
    if not isinstance(confirm, dict):
        return None
    affected = confirm.get("affectedDeals", [])
    if affected and isinstance(affected, list):
        first = affected[0]
        if isinstance(first, dict):
            return first.get("dealId")
    return confirm.get("dealId")


def build_activity_index(activity_json):
    acts = activity_json.get("activities", []) if isinstance(activity_json, dict) else []
    rows = []
    for a in acts:
        details = a.get("details", {}) if isinstance(a, dict) else {}
        rows.append({
            "dealId": details.get("dealId") or a.get("dealId"),
            "date": a.get("date") or a.get("timestamp"),
            "description": details.get("description") or a.get("description"),
            "details": details,
            "raw": a,
        })
    df = pd.DataFrame(rows)
    if not df.empty and "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], utc=True, errors="coerce")
    return df


def detect_and_log_closed_positions(client, journal):
    tracker = journal.load_position_tracker()
    if tracker.empty:
        return
    if "status" not in tracker.columns:
        tracker["status"] = "OPEN"

    open_positions = flatten_positions(client.get_open_positions())
    open_deal_ids = set(open_positions["dealId"].dropna().astype(str)) if not open_positions.empty and "dealId" in open_positions else set()
    activity_idx = build_activity_index(client.get_activity_history(last_period=86400, detailed=True))

    changed = False
    for i, row in tracker.iterrows():
        if str(row.get("status")) == "CLOSED":
            continue
        deal_id = row.get("deal_id")
        if pd.isna(deal_id) or str(deal_id) == "":
            continue
        deal_id = str(deal_id)
        if deal_id in open_deal_ids:
            continue

        entry_price = float(row.get("entry_price", 0.0) or 0.0)
        direction = row.get("direction")
        size = float(row.get("size", 0.0) or 0.0)
        close_time = datetime.now(timezone.utc)
        exit_price = math.nan
        pnl = math.nan
        source = "position_disappeared"

        if not activity_idx.empty:
            matches = activity_idx[activity_idx["dealId"].astype(str) == deal_id]
            if not matches.empty:
                last = matches.sort_values("date").iloc[-1]
                close_time = last["date"] if pd.notna(last["date"]) else close_time
                details = last["details"] if isinstance(last["details"], dict) else {}
                for k in ["level", "closeLevel", "price", "stopLevel"]:
                    if details.get(k) is not None:
                        try:
                            exit_price = float(details.get(k))
                            break
                        except Exception:
                            pass
                for k in ["profitAndLoss", "pnl", "profit", "netProfit"]:
                    if details.get(k) is not None:
                        try:
                            pnl = float(str(details.get(k)).replace(',', ''))
                            break
                        except Exception:
                            pass
                source = "activity_history"

        if pd.isna(exit_price):
            exit_price = entry_price
            if pd.isna(pnl):
                pnl = 0.0
        elif pd.isna(pnl):
            if direction == "BUY":
                pnl = (exit_price - entry_price) * size
            elif direction == "SELL":
                pnl = (entry_price - exit_price) * size
            else:
                pnl = 0.0

        open_time = pd.to_datetime(row.get("open_time_utc"), utc=True, errors="coerce")
        hold_minutes = (close_time - open_time).total_seconds() / 60.0 if pd.notna(open_time) else math.nan

        journal.append_closed_trade({
            "open_time_utc": open_time.isoformat() if pd.notna(open_time) else None,
            "close_time_utc": close_time.isoformat() if pd.notna(close_time) else None,
            "epic": row.get("epic"),
            "direction": direction,
            "size": size,
            "entry_price": entry_price,
            "exit_price": exit_price,
            "pnl": pnl,
            "hold_minutes": hold_minutes,
            "deal_id": deal_id,
            "deal_reference": row.get("deal_reference"),
            "source": source,
        })
        tracker.loc[i, "status"] = "CLOSED"
        tracker.loc[i, "close_time_utc"] = pd.Timestamp(close_time).floor("s")
        changed = True
        logger.info("CLOSED TRADE LOGGED | deal_id=%s source=%s pnl=%s", deal_id, source, pnl)

    if changed:
        journal.save_position_tracker(tracker)


async def ping_loop(ws, cst, security_token):
    while True:
        await asyncio.sleep(20)
        await ws.send(json.dumps({
            "destination": "ping",
            "correlationId": int(time.time()),
            "cst": cst,
            "securityToken": security_token,
        }))


async def ws_subscribe_loop(client, epic, tick_state):
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
                logger.info("Subscribed to websocket market data for %s", epic)
                ping_task = asyncio.create_task(ping_loop(ws, client.cst, client.security_token))
                try:
                    async for message in ws:
                        data = json.loads(message)
                        payload = data.get("payload", {})
                        if isinstance(payload, dict) and epic in payload:
                            p = payload[epic]
                            bid = p.get("bid")
                            ask = p.get("offer") or p.get("ask") or p.get("ofr")
                            if bid is not None and ask is not None:
                                tick_state.update(float(bid), float(ask))
                        elif isinstance(payload, dict):
                            bid = payload.get("bid")
                            ask = payload.get("offer") or payload.get("ask") or payload.get("ofr")
                            if bid is not None and ask is not None:
                                tick_state.update(float(bid), float(ask))
                finally:
                    ping_task.cancel()
        except Exception as e:
            logger.warning("WebSocket loop error: %s", e)
            await asyncio.sleep(5)


async def decision_loop(client, epic, tick_state, journal):
    while True:
        try:
            client.ensure_login()
            detect_and_log_closed_positions(client, journal)
            prices = client.get_prices(epic)
            if prices.empty or len(prices) < max(EMA_SLOW, ATR_PERIOD) + 5:
                logger.info("Not enough candle data yet.")
                await asyncio.sleep(DECISION_INTERVAL_SEC)
                continue

            signal = derive_signal(prices)
            tick = tick_state.summary()
            spread = float(tick.get("spread") or 0.0)
            atr_val = float(signal["atr"] or 0.0)
            spread_atr_ratio = spread / atr_val if atr_val > 0 else math.inf
            spread_atr_ok = atr_val > 0 and spread_atr_ratio <= MAX_SPREAD_ATR
            daily_trades = journal.daily_trade_count()
            daily_pnl = journal.daily_pnl_est()
            now_utc = datetime.now(timezone.utc)
            within_hours = is_within_trading_hours(now_utc)

            score = compute_signal_score(signal, tick, spread_atr_ratio)
            size_multiplier, final_order_size = size_from_score(score)

            logger.info(
                "HT=%s | tick=%s | imb=%.2f | price=%.4f | atr=%.4f | spread=%.4f | score=%.1f | size=%.2f | within_hours=%s",
                signal["action"], tick["tick_bias"], tick["imbalance"], signal["price"], signal["atr"], spread, score, final_order_size, within_hours
            )

            journal.log_state({
                "timestamp_utc": now_utc.isoformat(),
                "epic": epic,
                "action": signal["action"],
                "bias": signal["bias"],
                "fresh_reversal": signal["fresh_reversal"],
                "direction": signal["direction"],
                "price": signal["price"],
                "atr": signal["atr"],
                "spread": spread,
                "tick_bias": tick["tick_bias"],
                "tick_imbalance": tick["imbalance"],
                "daily_trades": daily_trades,
                "daily_pnl_est": daily_pnl,
            })

            open_positions = client.get_open_positions() if ALLOW_ONLY_ONE_POSITION else []
            already_has_position = len(open_positions) > 0 if ALLOW_ONLY_ONE_POSITION else False

            can_trade = (
                within_hours and
                signal["direction"] in ["BUY", "SELL"] and
                tick["tick_bias"] == signal["direction"] and
                spread_atr_ok and
                daily_trades < MAX_DAILY_TRADES and
                not already_has_position and
                final_order_size > 0
            )

            if can_trade:
                stop_distance = calc_stop_distance(atr_val)
                note = (
                    f"DRY_RUN={DRY_RUN};imb={tick['imbalance']:.2f};spread_atr={spread_atr_ratio:.3f};"
                    f"stopDistance={stop_distance:.4f};trailingStop={USE_TRAILING_STOP};withinHours={within_hours};"
                    f"window={TRADING_START_HOUR_UTC}-{TRADING_END_HOUR_UTC}UTC;score={score:.1f};"
                    f"sizeMultiplier={size_multiplier:.2f};finalSize={final_order_size:.2f}"
                )

                if DRY_RUN:
                    journal.log_trade({
                        "timestamp_utc": now_utc.isoformat(),
                        "event": "OPEN",
                        "epic": epic,
                        "direction": signal["direction"],
                        "price": signal["price"],
                        "size": final_order_size,
                        "atr": signal["atr"],
                        "spread": spread,
                        "note": note,
                    })
                    logger.info("DRY RUN OPEN SIGNAL | direction=%s | %s", signal["direction"], note)
                else:
                    response = client.create_position(
                        epic=epic,
                        direction=signal["direction"],
                        size=final_order_size,
                        stop_distance=stop_distance,
                        trailing_stop=USE_TRAILING_STOP,
                    )
                    deal_reference = response.get("dealReference")
                    confirm = client.confirm_position(deal_reference) if deal_reference else {}
                    deal_id = extract_confirm_deal_id(confirm)
                    logger.info("CONFIRM RESPONSE | %s", confirm)

                    journal.log_trade({
                        "timestamp_utc": now_utc.isoformat(),
                        "event": "OPEN",
                        "epic": epic,
                        "direction": signal["direction"],
                        "price": signal["price"],
                        "size": final_order_size,
                        "atr": signal["atr"],
                        "spread": spread,
                        "note": f"dealReference={deal_reference};dealId={deal_id};confirm={json.dumps(confirm)};{note}",
                    })
                    if deal_id:
                        journal.add_position_tracker({
                            "deal_reference": deal_reference,
                            "deal_id": deal_id,
                            "open_time_utc": now_utc.isoformat(),
                            "epic": epic,
                            "direction": signal["direction"],
                            "size": final_order_size,
                            "entry_price": signal["price"],
                            "atr": signal["atr"],
                            "stop_distance": stop_distance,
                            "signal_score": score,
                            "size_multiplier": size_multiplier,
                            "status": "OPEN",
                            "close_time_utc": None,
                        })
                    logger.info("LIVE POSITION REQUESTED | direction=%s dealReference=%s dealId=%s size=%.2f", signal["direction"], deal_reference, deal_id, final_order_size)
            else:
                logger.info(
                    "NO TRADE | direction=%s tick=%s spread_ok=%s daily_trades=%s already_has_position=%s within_hours=%s score=%.1f size=%.2f",
                    signal["direction"], tick["tick_bias"], spread_atr_ok, daily_trades, already_has_position, within_hours, score, final_order_size
                )

        except Exception as e:
            logger.exception("Decision loop error: %s", e)

        await asyncio.sleep(DECISION_INTERVAL_SEC)


async def main():
    if not API_KEY or not IDENTIFIER or not PASSWORD:
        raise RuntimeError("Missing CAPITAL_API_KEY / CAPITAL_IDENTIFIER / CAPITAL_PASSWORD")

    logger.info(
        "BOOT | DRY_RUN=%s | SESSION_FILTER=%s | HOURS=%s-%s UTC | BASE_SIZE=%.2f | MIN_SIZE=%.2f | MAX_SIZE=%.2f | TRAIL_ATR=%.2f | files=%s,%s,%s,%s",
        DRY_RUN, ENABLE_SESSION_FILTER, TRADING_START_HOUR_UTC, TRADING_END_HOUR_UTC,
        BASE_ORDER_SIZE, MIN_ORDER_SIZE, MAX_ORDER_SIZE, TRAILING_STOP_ATR_MULT,
        BOT_STATE_CSV, TRADE_JOURNAL_CSV, CLOSED_TRADES_CSV, POSITION_TRACKER_CSV
    )

    client = CapitalClient()
    client.ensure_login()
    client.ensure_account(CAPITAL_ACCOUNT_ID)
    epic = client.resolve_epic()
    tick_state = TickMomentum(maxlen=TICK_BUFFER_SIZE)
    journal = Journal()

    await asyncio.gather(
        ws_subscribe_loop(client, epic, tick_state),
        decision_loop(client, epic, tick_state, journal),
    )


if __name__ == "__main__":
    while True:
        try:
            asyncio.run(main())
        except KeyboardInterrupt:
            break
        except Exception as e:
            logger.exception("Decision loop error: %s", e)
            time.sleep(60)

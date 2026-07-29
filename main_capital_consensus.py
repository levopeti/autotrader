"""
Live trader a per-channel + consensus szabályokkal.

Ez egy `main_capital.py`-variáns: ugyanaz a ZMQ→queue→PositionManager
architektúra, de a bejövő signalokat a `live.consensus_gate.ConsensusGate`
szűri/transzformálja:

  - per-channel tp_idx filter (ANN→0, VIP→1, Traderz→2)
  - ATR-alapú SL/TP felülírás ANN signalokon
  - ANN+Traderz consensus → emelt size (2.0×)
  - VIP nem vesz részt a consensusban

A háttérben egy ATR-loop 60mp-enként pull-olja az utolsó 5-perces gyertyákat
és frissíti az ATR(14)-et az ANN-recepthez.

Futtatás:
  python main_capital_consensus.py              # demo, BIZTONSÁGI DRY-RUN
  python main_capital_consensus.py --no-dry-run # demo, valódi pozíció
  python main_capital_consensus.py --live --no-dry-run  # ÉLES SZÁMLA (VIGYÁZZ)
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import aiohttp
import numpy as np
import pandas as pd
import websockets
import zmq
import zmq.asyncio

from live.consensus_gate import ConsensusGate, GateConfig
from signals.live.position import Direction, PositionConfig, PositionState
from signals.live.position_manager import PositionManager


logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(message)s")
logger = logging.getLogger(__name__)


# ─── BOOT CONFIG ─────────────────────────────────────────────────────────────

ROOT = Path(__file__).resolve().parent
with open(ROOT / "keys_urls.json", "r") as f:
    _cfg = json.load(f)

API_KEY    = _cfg["capital_api_key"]
IDENTIFIER = _cfg["capital_login"]
PASSWORD   = _cfg["capital_pw"]

EPIC          = "GOLD"
ZMQ_PULL_ADDR = "tcp://localhost:5555"

POLL_INTERVAL_SEC     = 5 * 60
BACKFILL_INTERVAL_SEC = 15
OPEN_INTERVAL_SEC     = 2.0
CONFIRM_TIMEOUT_SEC   = 30.0
POSITION_TIMEOUT_SEC  = 60 * 60    # combo_2 deploy 2026-07-29: 15 → 60 min
RETRY_DELAY_SEC       = 10.0
MAX_RETRIES           = 5

# ATR frissítés
ATR_TF_RESOLUTION   = "MINUTE_5"   # capital.com TF kulcs
ATR_PERIOD          = 14
ATR_REFRESH_SEC     = 60.0
ATR_BARS_FETCH      = 100          # elég sok a warm-up + smoothinghoz

# Trend-filter (Traderz-en aktív; lásd RecipeRules.trend_filter):
# 1h EMA9 vs EMA21 alapján. Backtest sweep szerint a (1h, 9, 21) a legjobb
# kombináció Traderz-re.
TREND_TF_RESOLUTION = "HOUR_4"     # combo_2 deploy 2026-07-29: 1h → 4h (WF optimum)
TREND_EMA_FAST      = 9
TREND_EMA_SLOW      = 21
TREND_REFRESH_SEC   = 900.0        # 15 percenként újraszámol (4h candle-ek)
TREND_BARS_FETCH    = 100          # bőven elég a 21-periódusú EMA-hoz

# Sizing — a ZMQ signal `size` mezőjét ezzel szorozzuk a consensus multipliker
# UTÁN. A backtest 1.0 base lot-tal volt; ha kisebb startot szeretnél, állítsd
# át. (`fixed_lot_size` analóg.)
BASE_SIZE_MULT = 1.0


# ─── QUEUE ITEM ──────────────────────────────────────────────────────────────

@dataclass
class QueueItem:
    config:  PositionConfig
    retries: int = 0
    is_consensus: bool = False
    dry_run: bool = False


# ─── ATR HOLDER ──────────────────────────────────────────────────────────────

class AtrHolder:
    """Egy aktuális ATR($) érték container, async-safe write/read."""
    def __init__(self) -> None:
        self._atr: Optional[float] = None
        self._ts: Optional[pd.Timestamp] = None
        self._lock = asyncio.Lock()

    async def set(self, atr: float, ts: pd.Timestamp) -> None:
        async with self._lock:
            self._atr = float(atr)
            self._ts = ts

    def get(self) -> Optional[float]:
        return self._atr

    def callable_for_gate(self):
        # A gate sync callable-t vár → minimális wrapper
        def _get(_ts):
            return self._atr
        return _get


def _compute_atr_from_prices(prices: list[dict], period: int) -> Optional[float]:
    """Capital.com /prices válaszból ATR(period) — utolsó érték."""
    if not prices or len(prices) < period + 2:
        return None
    rows = []
    for p in prices:
        try:
            rows.append({
                "high":  float(p["highPrice"]["bid"]),
                "low":   float(p["lowPrice"]["bid"]),
                "close": float(p["closePrice"]["bid"]),
            })
        except (KeyError, TypeError):
            return None
    df = pd.DataFrame(rows)
    prev_close = df["close"].shift(1)
    tr = pd.concat([
        df["high"] - df["low"],
        (df["high"] - prev_close).abs(),
        (df["low"]  - prev_close).abs(),
    ], axis=1).max(axis=1)
    atr = tr.ewm(alpha=1.0 / period, adjust=False).mean().iloc[-1]
    return float(atr) if np.isfinite(atr) else None


class TrendHolder:
    """Aktuális trend-irány ("BUY"/"SELL"/None) tárolója, async-safe."""
    def __init__(self) -> None:
        self._trend: Optional[str] = None
        self._lock = asyncio.Lock()

    async def set(self, t: Optional[str]) -> None:
        async with self._lock:
            self._trend = t

    def get(self) -> Optional[str]:
        return self._trend

    def callable_for_gate(self):
        def _get():
            return self._trend
        return _get


def _compute_trend_from_prices(prices: list[dict], fast: int, slow: int) -> Optional[str]:
    """Capital /prices válaszból EMA-trend irány — 'BUY' / 'SELL' / None."""
    if not prices or len(prices) < slow + 2:
        return None
    closes = []
    for p in prices:
        try:
            closes.append(float(p["closePrice"]["bid"]))
        except (KeyError, TypeError):
            return None
    series = pd.Series(closes)
    ema_fast = series.ewm(span=fast, adjust=False).mean().iloc[-1]
    ema_slow = series.ewm(span=slow, adjust=False).mean().iloc[-1]
    if not (np.isfinite(ema_fast) and np.isfinite(ema_slow)):
        return None
    if ema_fast > ema_slow:
        return "BUY"
    if ema_fast < ema_slow:
        return "SELL"
    return None


async def trend_refresh_loop(
    cst: str, token: str, base_url: str, epic: str, holder: TrendHolder
) -> None:
    """Periodikusan pull-olja az 1h candle-eket és frissíti a trend irányt."""
    headers = {"CST": cst, "X-SECURITY-TOKEN": token, "Content-Type": "application/json"}
    url = f"{base_url}/api/v1/prices/{epic}?resolution={TREND_TF_RESOLUTION}&max={TREND_BARS_FETCH}"
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                async with session.get(url, headers=headers) as r:
                    if r.status != 200:
                        logger.warning("[TREND] HTTP %d", r.status)
                    else:
                        data = await r.json()
                        prices = data.get("prices", []) if isinstance(data, dict) else []
                        t = _compute_trend_from_prices(prices, TREND_EMA_FAST, TREND_EMA_SLOW)
                        await holder.set(t)
                        logger.info("[TREND] frissítve | %s %s EMA%d/%d → %s",
                                    epic, TREND_TF_RESOLUTION, TREND_EMA_FAST, TREND_EMA_SLOW, t or "?")
            except Exception as e:
                logger.warning("[TREND] hiba: %s", e)
            await asyncio.sleep(TREND_REFRESH_SEC)


async def atr_refresh_loop(
    cst: str, token: str, base_url: str, epic: str, holder: AtrHolder
) -> None:
    """60 mp-enként pull-olja a candle-eket és frissíti az ATR-t."""
    headers = {"CST": cst, "X-SECURITY-TOKEN": token, "Content-Type": "application/json"}
    url = f"{base_url}/api/v1/prices/{epic}?resolution={ATR_TF_RESOLUTION}&max={ATR_BARS_FETCH}"
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                async with session.get(url, headers=headers) as r:
                    if r.status != 200:
                        logger.warning("[ATR] HTTP %d — skip", r.status)
                    else:
                        data = await r.json()
                        prices = data.get("prices", []) if isinstance(data, dict) else []
                        atr = _compute_atr_from_prices(prices, ATR_PERIOD)
                        if atr is not None:
                            await holder.set(atr, pd.Timestamp.now("UTC").tz_localize(None))
                            logger.info("[ATR] frissítve | %s ATR(%d) = %.3f", epic, ATR_PERIOD, atr)
                        else:
                            logger.warning("[ATR] számítás sikertelen (kevés gyertya?)")
            except Exception as e:
                logger.warning("[ATR] hiba: %s", e)
            await asyncio.sleep(ATR_REFRESH_SEC)


# ─── POLL LOOP ───────────────────────────────────────────────────────────────

async def poll_loop(manager: PositionManager) -> None:
    while True:
        await asyncio.sleep(POLL_INTERVAL_SEC)
        await manager.poll_and_log()


# ─── REJECT WATCHER ──────────────────────────────────────────────────────────

async def _watch_for_reject(pos, item: QueueItem, queue: asyncio.Queue) -> None:
    cfg      = item.config
    loop     = asyncio.get_event_loop()
    deadline = loop.time() + POSITION_TIMEOUT_SEC + CONFIRM_TIMEOUT_SEC
    terminal = (
        PositionState.OPEN, PositionState.REJECTED, PositionState.ERROR,
        PositionState.EXPIRED, PositionState.CANCELED,
    )
    while pos.state not in terminal:
        if loop.time() > deadline:
            logger.warning("[WATCH] ⏰ timeout: %s", pos)
            return
        await asyncio.sleep(0.5)
    if pos.state == PositionState.REJECTED and item.retries < MAX_RETRIES:
        item.retries += 1
        logger.warning("[WATCH] 🔁 REJECTED → újrapróba %d/%d", item.retries, MAX_RETRIES)
        await asyncio.sleep(RETRY_DELAY_SEC)
        await queue.put(item)


# ─── POSITION OPENER ─────────────────────────────────────────────────────────

async def position_opener(manager: PositionManager, queue: asyncio.Queue) -> None:
    logger.info("[OPENER] indult (retry_delay=%.0fs, max_retries=%d)",
                RETRY_DELAY_SEC, MAX_RETRIES)
    while True:
        item: QueueItem = await queue.get()
        cfg = item.config
        try:
            if item.dry_run:
                logger.warning(
                    "[OPENER] 🧪 DRY-RUN — NEM nyitunk (%s %s @ %.2f-%.2f TP %.2f SL %.2f size %.2f%s)",
                    cfg.chat_name, cfg.direction.value, cfg.zone_low, cfg.zone_high,
                    cfg.tp, cfg.sl, cfg.size,
                    " [CONSENSUS]" if item.is_consensus else "",
                )
                continue

            if not manager.can_open(cfg.chat_id):
                logger.warning("[OPENER] ⛔ limit elérve, dobjuk: TP %.2f", cfg.tp)
                continue

            pos = manager.add(cfg)
            if pos is None:
                continue
            logger.info("[OPENER] ➕ WAITING | TP %.2f size %.2f%s",
                        cfg.tp, cfg.size, " [CONSENSUS]" if item.is_consensus else "")
            asyncio.create_task(_watch_for_reject(pos, item, queue))
            await asyncio.sleep(OPEN_INTERVAL_SEC)

        except Exception as e:
            logger.error("[OPENER] hiba: %s", e)
        finally:
            queue.task_done()


# ─── ZMQ LISTENER + GATE ─────────────────────────────────────────────────────

async def zmq_listener(
    manager: PositionManager,
    queue: asyncio.Queue,
    gate: ConsensusGate,
    dry_run: bool,
) -> None:
    ctx  = zmq.asyncio.Context.instance()
    sock = ctx.socket(zmq.PULL)
    sock.bind(ZMQ_PULL_ADDR)
    logger.info("[ZMQ] figyelés: %s", ZMQ_PULL_ADDR)
    try:
        while True:
            p = await sock.recv_pyobj()
            try:
                # A parser ZMQ tp_idx-e 1-based, és a parsed.tp_list
                # PRICE-ASCENDING sorrendjében indexel. A gate maga számolja
                # a DISTANCE-rank szerinti indexet a direction + per-channel
                # expected_tp_count alapján.
                raw_tp_idx = int(p["tp_idx"])

                # ── Gate hívás ──
                out = gate.process(
                    chat_id=int(p["chat_id"]),
                    chat_name=str(p["chat_name"]),
                    direction=str(p["direction"]).upper(),
                    zone_low=float(p["zone_low"]),
                    zone_high=float(p["zone_high"]),
                    tp=float(p["tp"]),
                    sl=float(p["sl"]),
                    tp_idx=raw_tp_idx,       # legacy mező, ignorált ha raw_tp_idx adott
                    raw_tp_idx=raw_tp_idx,
                    message_id=int(p["message_id"]) if p.get("message_id") is not None else None,
                )
                if not out.accept:
                    logger.info("[GATE] eldobva (%s) | %s tp_idx=%d | reason=%s",
                                p["chat_name"], p["direction"], p["tp_idx"], out.reason)
                    continue

                size_final = float(p["size"]) * BASE_SIZE_MULT * out.size_mult

                cfg = PositionConfig(
                    epic=p["epic"],
                    direction=Direction(out.direction),
                    size=size_final,
                    zone_low=out.zone_low,
                    zone_high=out.zone_high,
                    tp=out.tp,
                    sl=out.sl,
                    tp_idx=int(p["tp_idx"]),
                    raw_text=str(p["raw_text"]),
                    send_date=str(p["send_date"]),
                    edited=bool(p.get("edited", False)),
                    chat_id=int(p["chat_id"]),
                    chat_name=str(p["chat_name"]),
                    message_id=int(p["message_id"]) if p.get("message_id") is not None else None,
                    ladder_trigger_price=out.ladder_trigger_price,
                    ladder_dest_price=out.ladder_dest_price,
                )
                # De-dup: minden gate által elfogadott signalnál idempotensen
                # cancel-eljük az ugyanazon message_id korábbi WAITING-jeit.
                # Ez kezeli:
                #   - edited=true → tényleges edit re-emit (a régi obsolete)
                #   - edited=false dup → ZMQ-backlog flush / parser-retry esete
                # OPEN pozíciókat NEM nyúlja, csak WAITING-eket.
                if cfg.message_id is not None:
                    n_cancelled = manager.cancel_waiting_by_message_id(cfg.message_id)
                    if n_cancelled:
                        logger.info("[GATE] cancel-replace: %d WAITING eldobva (msg_id=%s, edited=%s)",
                                    n_cancelled, cfg.message_id, cfg.edited)

                await queue.put(QueueItem(config=cfg, is_consensus=out.is_consensus, dry_run=dry_run))
                logger.warning(
                    "[ZMQ] 📥 SORBA (%s %s %.2f-%.2f TP %.2f SL %.2f size %.2f%s) | q=%d",
                    p["epic"], out.direction, out.zone_low, out.zone_high,
                    out.tp, out.sl, size_final,
                    " ✨CONSENSUS" if out.is_consensus else "",
                    queue.qsize(),
                )
            except (KeyError, ValueError) as e:
                logger.warning("[ZMQ] hibás üzenet (%s)", e)
    except asyncio.CancelledError:
        sock.close()


# ─── AUTH ────────────────────────────────────────────────────────────────────

async def create_session(base_url: str) -> tuple[str, str]:
    headers = {"X-CAP-API-KEY": API_KEY, "Content-Type": "application/json"}
    body    = {"identifier": IDENTIFIER, "password": PASSWORD, "encryptionKey": False}
    async with aiohttp.ClientSession() as s:
        async with s.post(f"{base_url}/api/v1/session", headers=headers, json=body) as r:
            r.raise_for_status()
            logger.info("[AUTH] bejelentkezés OK")
            return r.headers["CST"], r.headers["X-SECURITY-TOKEN"]


async def ping_loop(ws, cst, token) -> None:
    while True:
        await asyncio.sleep(9 * 60)
        await ws.send(json.dumps({"destination": "ping", "correlationId": 99,
                                  "cst": cst, "securityToken": token}))


# ─── FŐ STREAM ───────────────────────────────────────────────────────────────

async def stream(epic: str, base_url: str, ws_url: str, dry_run: bool) -> None:
    cst, token = await create_session(base_url)
    manager    = PositionManager(base_url=base_url, cst=cst, token=token)
    open_queue: asyncio.Queue[QueueItem] = asyncio.Queue()

    atr_holder = AtrHolder()
    trend_holder = TrendHolder()
    gate = ConsensusGate(
        cfg=GateConfig(consensus_window_min=60.0,
                       consensus_price_tol=5.0,
                       consensus_size_mult=2.0,
                       # combo_2 deploy 2026-07-29: NEWS FILTER KIKAPCSOLVA.
                       # Naiv baseline sweep megmutatta hogy VIP-en +$52 vs -$22
                       # a szűk 11-15 UTC vs 08-16 UTC filter; Traderz-en a semmi
                       # a legjobb (+$35 vs +$12 08-16-tal). VIP+Traderz combo
                       # WF: +$162/hó OOS filter nélkül.
                       news_blocked_hours=[]),
        atr_provider=atr_holder.callable_for_gate(),
        trend_provider=trend_holder.callable_for_gate(),
    )

    asyncio.create_task(zmq_listener(manager, open_queue, gate, dry_run))
    asyncio.create_task(position_opener(manager, open_queue))
    asyncio.create_task(poll_loop(manager))
    asyncio.create_task(atr_refresh_loop(cst, token, base_url, epic, atr_holder))
    asyncio.create_task(trend_refresh_loop(cst, token, base_url, epic, trend_holder))

    async with websockets.connect(ws_url) as ws:
        await ws.send(json.dumps({
            "destination":   "marketData.subscribe",
            "correlationId": 1,
            "cst":           cst,
            "securityToken": token,
            "payload":       {"epics": [epic]},
        }))
        logger.info("[WS] marketData feliratkozás → %s", epic)

        # Megjegyzés: nincs explicit OPU/trade subscribe — Capital demo-n
        # mindkettő BAD_REQUEST-tel tér vissza, ezek nem támogatottak. Az OPU
        # automatikusan érkezne (Capital docs szerint), de a demo accountnál
        # sosem érkeznek. Ezért a poll-based close-detection a fallback
        # (`csv_update_terminal` → `_fetch_transactions_and_log`), ami a
        # `size` mezőből szedi le a realised PnL-t.

        asyncio.create_task(ping_loop(ws, cst, token))

        # Ismeretlen destination-ok korlátozott naplózása — Capital néha új
        # destination-okat küld, ne legyen log-spam
        unknown_dest_count: dict[str, int] = {}
        UNKNOWN_LOG_LIMIT = 3

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
                    manager.broadcast(float(bid), float(ask))
            elif dest == "OPU":
                manager.handle_opu(data.get("payload", {}))
            else:
                # Diagnostika: az első néhány ismeretlen üzenet teljes
                # tartalmát naplózzuk, hogy lássuk milyen destination-okat
                # küld a Capital WS (pl. trade/account/dealConfirms)
                n = unknown_dest_count.get(dest, 0)
                if n < UNKNOWN_LOG_LIMIT:
                    logger.warning("[WS] ismeretlen destination=%s | payload=%s",
                                   dest, json.dumps(data)[:400])
                    unknown_dest_count[dest] = n + 1


# ─── CLI ─────────────────────────────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser(description="Live consensus + per-channel trader")
    p.add_argument("--epic", default=EPIC, help="Capital.com epic (default: GOLD)")
    p.add_argument("--live", action="store_true",
                   help="ÉLES számla használata (default: demo)")
    p.add_argument("--no-dry-run", action="store_true",
                   help="Tényleges pozíció nyitás (default: csak logol)")
    return p.parse_args()


def main():
    args = parse_args()
    base_url = ("https://api-capital.backend-capital.com" if args.live
                else "https://demo-api-capital.backend-capital.com")
    ws_url = "wss://api-streaming-capital.backend-capital.com/connect"
    dry_run = not args.no_dry_run

    logger.info("=" * 72)
    logger.info("MODE: %s | DRY-RUN: %s | EPIC: %s",
                "ÉLES" if args.live else "DEMO", dry_run, args.epic)
    logger.info("=" * 72)
    if args.live and not dry_run:
        logger.warning("⚠️  ÉLES számlán fogunk pozíciót nyitni — Ctrl+C 5 mp-en belül a megálláshoz")
        import time as _t; _t.sleep(5)

    # Robusztus reconnect-loop. Minden kivételt naplózunk, és rövid várakozás
    # után újraindítunk — kivéve KeyboardInterrupt, ami szándékos leállítás.
    import time as _t
    import traceback as _tb
    while True:
        try:
            asyncio.run(stream(args.epic, base_url, ws_url, dry_run))
            # asyncio.run normál visszatérése (pl. WS lezárta a stream-loopot)
            # — szándékon kívüli, restart kell
            logger.error("[MAIN] stream() exception nélkül kilépett — 60 mp múlva újra")
            _t.sleep(60)
        except KeyboardInterrupt:
            logger.info("[EXIT] leállítás (Ctrl+C)")
            break
        except websockets.ConnectionClosedError:
            logger.error("[WS] kapcsolat lezárva — 60 mp múlva újraindítás")
            _t.sleep(60)
        except Exception as e:
            logger.error("[MAIN] nem várt kivétel: %s", e)
            logger.error("[MAIN] traceback:\n%s", _tb.format_exc())
            logger.error("[MAIN] 60 mp múlva újraindítás")
            _t.sleep(60)


if __name__ == "__main__":
    main()

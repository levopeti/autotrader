#!/usr/bin/env python3
"""
Kalman GOLD-SILVER pairs trade — DRY-RUN prototype.

Fő funkció:
  - Capital WS csatlakozás GOLD ÉS SILVER 1-perces tick-stream-re
  - Latest mid holder mindkét symbolra
  - 5 percenként (aligned to :00, :05, ..., :55) Kalman update
  - Signal generálás (|z| > K_entry): LOG only, semmilyen tényleges pozíció nyitás
  - Warmup: local parquet-ekből (utolsó 500 5-min bar) OLS init + Kalman fit
  - Kalman state perzisztencia (JSON) — restart-safe

Használat:
  python main_kalman_pairs.py                    # DRY-RUN dev
  python main_kalman_pairs.py --live-warmup      # warmup a REST candles-ből
                                                    (ha nincs local parquet)
Naplók: logs_live/kalman_pairs_YYYYMMDD_HHMMSS.log
State:  state_kalman_pairs.json
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

import aiohttp
import numpy as np
import pandas as pd
import websockets

from live.kalman_pairs import KalmanFilter

ROOT = Path(__file__).resolve().parent
STATE_PATH = ROOT / "state_kalman_pairs.json"
LOG_DIR = ROOT / "logs_live"
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(message)s")
logger = logging.getLogger("kalman_pairs")

with open(ROOT / "keys_urls.json") as f:
    _cfg = json.load(f)
API_KEY, IDENTIFIER, PASSWORD = _cfg["capital_api_key"], _cfg["capital_login"], _cfg["capital_pw"]

BASE_URL = "https://demo-api-capital.backend-capital.com"
WS_URL   = "wss://api-streaming-capital.backend-capital.com/connect"

# ── Kalman + strategy params (from Q/R + exit sweep — validated defaults)
KALMAN_Q_ALPHA = 1e-5
KALMAN_Q_BETA  = 1e-6
KALMAN_R       = 25.0
K_ENTRY        = 2.5
K_EXIT         = 0.3
MAX_HOLD_BARS  = 500        # 500 × 5min = 41.7h — safety timeout
UPDATE_INTERVAL_SEC = 5 * 60   # 5-min aligned Kalman step
STATE_SAVE_INTERVAL_SEC = 60   # state mentés gyakorisága

# ── Instrument config
GOLD_EPIC   = "GOLD"
SILVER_EPIC = "SILVER"
GOLD_PARQUET   = ROOT / "data/tick_data_GOLD.parquet"
SILVER_PARQUET = ROOT / "data/tick_data_SILVER.parquet"

# Warmup: ennyi 5-min bar-t olvasunk be a local parquet-ből
WARMUP_BARS = 2000


@dataclass
class PriceHolder:
    """Tick-buffer: mindkét symbolra tárol egy latest mid + ts párost."""
    gold_mid: Optional[float] = None
    gold_ts:  Optional[datetime] = None
    silver_mid: Optional[float] = None
    silver_ts:  Optional[datetime] = None

    def update(self, epic: str, bid: float, ask: float, ts: datetime) -> None:
        mid = (bid + ask) / 2.0
        if epic == GOLD_EPIC:
            self.gold_mid = mid
            self.gold_ts = ts
        elif epic == SILVER_EPIC:
            self.silver_mid = mid
            self.silver_ts = ts

    def both_fresh(self, max_age_sec: float = 60.0) -> bool:
        """Igaz, ha mindkét symbolnak van friss (max_age_sec-en belüli) mid-je."""
        if self.gold_mid is None or self.silver_mid is None:
            return False
        now = datetime.now(timezone.utc)
        return ((now - self.gold_ts).total_seconds() < max_age_sec
                and (now - self.silver_ts).total_seconds() < max_age_sec)


@dataclass
class SimulatedPosition:
    """DRY-RUN pozíció szimuláció — csak logolunk, nem nyitunk valós pozíciót."""
    side: str            # "long_spread" | "short_spread"
    entry_ts: datetime
    entry_gold: float
    entry_silver: float
    entry_beta: float
    entry_z: float
    bar_count: int = 0

    def current_pnl(self, cur_gold: float, cur_silver: float) -> float:
        """Élő PnL (float $, slippage nélkül)."""
        if self.side == "long_spread":
            return (cur_gold - self.entry_gold) - self.entry_beta * (cur_silver - self.entry_silver)
        else:
            return (self.entry_gold - cur_gold) - self.entry_beta * (self.entry_silver - cur_silver)


# ─────────────────────────────────────────────────────────────
# Capital auth + candle fetch
# ─────────────────────────────────────────────────────────────

async def capital_login(session: aiohttp.ClientSession) -> tuple[str, str]:
    headers = {"X-CAP-API-KEY": API_KEY, "Content-Type": "application/json"}
    payload = {"identifier": IDENTIFIER, "password": PASSWORD, "encryptedPassword": False}
    async with session.post(f"{BASE_URL}/api/v1/session", json=payload, headers=headers) as r:
        r.raise_for_status()
        cst = r.headers.get("CST")
        tok = r.headers.get("X-SECURITY-TOKEN")
    logger.info("[AUTH] bejelentkezés OK")
    return cst, tok


# ─────────────────────────────────────────────────────────────
# Warmup: local parquet-ekből 5-min bar aggregáció + Kalman fit
# ─────────────────────────────────────────────────────────────

def load_warmup_bars(n_bars: int = WARMUP_BARS) -> pd.DataFrame:
    """
    Utolsó n_bars 5-min bar (GOLD + SILVER aligned) local parquet-ekből.
    Fallback: ha kevesebb bar van, azt adja vissza.
    """
    if not GOLD_PARQUET.exists() or not SILVER_PARQUET.exists():
        raise FileNotFoundError(f"Missing parquet: {GOLD_PARQUET} or {SILVER_PARQUET}")

    logger.info("[WARMUP] parquet-ek olvasása...")
    # optimalizálás: csak az utolsó ~n_bars*5min időszakot olvassuk be
    # 5min bar → n_bars=2000 = ~7 nap. Egy full parquet több tíz millió tick.
    # Egyszerűség kedvéért az egészet olvassuk be, aztán resample.
    g = pd.read_parquet(GOLD_PARQUET, columns=["timestamp_utc", "bid", "ask"])
    s = pd.read_parquet(SILVER_PARQUET, columns=["timestamp_utc", "bid", "ask"])
    g["mid"] = (g["bid"] + g["ask"]) / 2.0
    s["mid"] = (s["bid"] + s["ask"]) / 2.0
    g["timestamp_utc"] = pd.to_datetime(g["timestamp_utc"], utc=True)
    s["timestamp_utc"] = pd.to_datetime(s["timestamp_utc"], utc=True)

    g5 = g.set_index("timestamp_utc")["mid"].resample("5min").last().dropna()
    s5 = s.set_index("timestamp_utc")["mid"].resample("5min").last().dropna()
    df = pd.DataFrame({"gold": g5, "silver": s5}).dropna().tail(n_bars)
    logger.info(f"[WARMUP] {len(df)} bar betöltve: {df.index[0]} → {df.index[-1]}")
    return df


def warmup_kalman(kf: KalmanFilter, df: pd.DataFrame) -> None:
    """OLS init a legrégebbi 1000 bar-on, majd Kalman a maradékon."""
    gold = df["gold"].to_numpy()
    silver = df["silver"].to_numpy()
    ols_n = min(1000, len(gold) // 2)
    kf.warmup_ols(gold[:ols_n], silver[:ols_n])
    logger.info(f"[WARMUP] OLS init: alpha={kf.state.alpha:.2f}, beta={kf.state.beta:.4f}")

    # Filter through remaining bars
    for i in range(ols_n, len(gold)):
        kf.update(gold[i], silver[i])
    logger.info(f"[WARMUP] Kalman filtered {len(gold) - ols_n} további bar-on")
    logger.info(f"[WARMUP] Végállapot: {kf.summary}")


# ─────────────────────────────────────────────────────────────
# 5-min aligned Kalman update loop
# ─────────────────────────────────────────────────────────────

async def kalman_loop(kf: KalmanFilter, prices: PriceHolder, log_file: Path):
    """
    Aligned :00, :05, :10, ..., :55 percekre. Warm-up 60s a start után, hogy
    összegyűljön mindkét symbol első tick-je.
    """
    await asyncio.sleep(60)
    logger.info("[KALMAN] loop indul")

    open_pos: Optional[SimulatedPosition] = None
    trades_log: list[dict] = []

    while True:
        # Következő 5-perces határig várunk
        now = datetime.now(timezone.utc)
        next_boundary = (now.replace(second=0, microsecond=0)
                         + timedelta(minutes=(5 - now.minute % 5)))
        wait = (next_boundary - now).total_seconds()
        if wait < 5:
            wait = 5 + (5 - now.minute % 5) * 60
        await asyncio.sleep(wait)

        ts = datetime.now(timezone.utc)
        if not prices.both_fresh(max_age_sec=UPDATE_INTERVAL_SEC * 2):
            logger.warning("[KALMAN] elavult prices: %s", (prices.gold_ts, prices.silver_ts))
            continue

        g_mid, s_mid = prices.gold_mid, prices.silver_mid
        residual, sigma, z = kf.update(g_mid, s_mid, ts=ts.isoformat())

        # Signal logika
        signal = "NONE"
        if open_pos is None:
            if z < -K_ENTRY:
                open_pos = SimulatedPosition("long_spread", ts, g_mid, s_mid, kf.state.beta, z)
                signal = f"ENTRY_LONG (z={z:.2f}, gold={g_mid:.2f}, silver={s_mid:.4f}, beta={kf.state.beta:.4f})"
            elif z > K_ENTRY:
                open_pos = SimulatedPosition("short_spread", ts, g_mid, s_mid, kf.state.beta, z)
                signal = f"ENTRY_SHORT (z={z:.2f}, gold={g_mid:.2f}, silver={s_mid:.4f}, beta={kf.state.beta:.4f})"
        else:
            open_pos.bar_count += 1
            pnl = open_pos.current_pnl(g_mid, s_mid)
            exit_reason = None
            if abs(z) < K_EXIT:
                exit_reason = "MEAN_REVERT"
            elif open_pos.bar_count >= MAX_HOLD_BARS:
                exit_reason = "TIMEOUT"
            if exit_reason:
                trade = {
                    "entry_ts": open_pos.entry_ts.isoformat(),
                    "exit_ts": ts.isoformat(),
                    "hold_bars": open_pos.bar_count,
                    "side": open_pos.side,
                    "entry_z": round(open_pos.entry_z, 3),
                    "exit_z": round(z, 3),
                    "entry_gold": open_pos.entry_gold, "exit_gold": g_mid,
                    "entry_silver": open_pos.entry_silver, "exit_silver": s_mid,
                    "beta": open_pos.entry_beta,
                    "pnl_gross": round(pnl, 2),
                    "exit_reason": exit_reason,
                }
                trades_log.append(trade)
                signal = f"EXIT ({exit_reason}, pnl={pnl:+.2f}, held {open_pos.bar_count} bar)"
                open_pos = None

        # Log entry (JSONL per Kalman step)
        entry = {
            "ts": ts.isoformat(),
            "gold": g_mid, "silver": s_mid,
            "alpha": round(kf.state.alpha, 4),
            "beta": round(kf.state.beta, 4),
            "residual": round(residual, 3),
            "sigma": round(sigma, 3),
            "z": round(z, 3),
            "signal": signal,
            "position_open": open_pos.side if open_pos else None,
            "position_pnl": round(open_pos.current_pnl(g_mid, s_mid), 2) if open_pos else None,
        }
        with log_file.open("a") as f:
            f.write(json.dumps(entry) + "\n")

        if signal != "NONE":
            logger.info(f"[SIGNAL] {ts.strftime('%H:%M:%S')} | {signal}")
        else:
            logger.info(f"[KAL ] {ts.strftime('%H:%M:%S')} | z={z:+.2f} | β={kf.state.beta:.3f}"
                        + (f" | pos={open_pos.side} pnl={open_pos.current_pnl(g_mid, s_mid):+.2f}" if open_pos else ""))


# ─────────────────────────────────────────────────────────────
# State save loop
# ─────────────────────────────────────────────────────────────

async def state_save_loop(kf: KalmanFilter):
    while True:
        await asyncio.sleep(STATE_SAVE_INTERVAL_SEC)
        try:
            kf.save_state(STATE_PATH)
        except Exception as e:
            logger.warning("[STATE] mentés hiba: %s", e)


# ─────────────────────────────────────────────────────────────
# WebSocket subscriber
# ─────────────────────────────────────────────────────────────

async def ws_listener(cst: str, tok: str, prices: PriceHolder):
    """1 WS session, 2 subscription (GOLD + SILVER). Auto-reconnect.
    Watchdog: ha 90s-en belül nem érkezik quote, force reconnect (Capital
    zombie-WS elleni védelem, 2026-07-24 incident: TCP él, quote-ok nem
    érkeznek 11+ óráig)."""
    QUOTE_TIMEOUT_SEC = 90.0     # ha nincs quote ennyi idő alatt, reconnect
    while True:
        try:
            async with websockets.connect(WS_URL, ping_interval=20, ping_timeout=20) as ws:
                for i, epic in enumerate([GOLD_EPIC, SILVER_EPIC], 1):
                    await ws.send(json.dumps({
                        "destination": "marketData.subscribe",
                        "correlationId": i,
                        "cst": cst, "securityToken": tok,
                        "payload": {"epics": [epic]},
                    }))
                    logger.info("[WS] feliratkozás → %s", epic)

                # explicit recv-with-timeout loop (watchdog beépítve)
                while True:
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=QUOTE_TIMEOUT_SEC)
                    except asyncio.TimeoutError:
                        logger.warning("[WS] %.0fs óta nincs quote — force reconnect",
                                        QUOTE_TIMEOUT_SEC)
                        break    # exit inner loop → close ws → outer while reconnect
                    try:
                        data = json.loads(raw)
                    except Exception:
                        continue
                    if data.get("destination") != "quote":
                        continue
                    payload = data.get("payload", {}) or {}
                    epic = payload.get("epic", "")
                    bid = payload.get("bid")
                    ask = payload.get("ofr") or payload.get("ask") or payload.get("offer")
                    if bid is None or ask is None:
                        continue
                    ts = datetime.now(timezone.utc)
                    prices.update(epic, float(bid), float(ask), ts)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.warning("[WS] hiba, 5s múlva újra: %s", e)
            await asyncio.sleep(5)


# ─────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────

async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--force-warmup", action="store_true",
                        help="Kényszerít teljes warmup-ot még ha van state file is")
    args = parser.parse_args()

    log_file = LOG_DIR / f"kalman_pairs_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.log"
    (LOG_DIR / "_kalman_pairs_current.log").write_text(str(log_file))
    logger.info("======================================================================")
    logger.info("MODE: DRY-RUN | KALMAN GOLD-SILVER PAIRS | log: %s", log_file.name)
    logger.info("======================================================================")

    # Kalman filter
    kf = KalmanFilter(Q_alpha=KALMAN_Q_ALPHA, Q_beta=KALMAN_Q_BETA, R=KALMAN_R)
    if not args.force_warmup and kf.load_state(STATE_PATH):
        logger.info("[STATE] betöltve: %s | %s", STATE_PATH, kf.summary)
    else:
        logger.info("[STATE] nincs vagy force_warmup → warmup indul")
        df_warm = load_warmup_bars(WARMUP_BARS)
        warmup_kalman(kf, df_warm)
        kf.save_state(STATE_PATH)
        logger.info("[STATE] warmup-utáni state elmentve → %s", STATE_PATH)

    # Auth
    async with aiohttp.ClientSession() as sess:
        cst, tok = await capital_login(sess)

    prices = PriceHolder()

    # Kick off tasks
    tasks = [
        asyncio.create_task(ws_listener(cst, tok, prices)),
        asyncio.create_task(kalman_loop(kf, prices, log_file)),
        asyncio.create_task(state_save_loop(kf)),
    ]
    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        pass


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("[EXIT] Ctrl-C")

"""
LiveRunner — a Strategy interface-t demo-számlán futtatja.

A backtest engine config-jából érkező Strategy (TrendReversal, RangeScalp, stb.)
ugyanazon az `on_tick(ts, bid, ask) → Decision` interface-en él. A runner:

  - login + WS subscribe + REST candle pull (TF-enként)
  - periodikusan `Strategy.refresh_candles(candles_mtf)` — friss candle history
  - minden tick-en `Strategy.on_tick()` → ha allow_trade=True, REST POST /positions
  - manuális break-even/atr_trail (live/trailing.py)
  - lezárás-detektálás REST poll-on → `Strategy.on_position_closed()`
  - RunLogger ugyanaz mint a backtest (events.jsonl, decisions.csv, trades.csv)
"""
from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Optional

import pandas as pd

from backtest.engine.runner import EngineConfig, _resolve_tp_layers
from backtest.engine.position_sizer import SizingConfig, compute_size
from backtest.runlog.run_logger import RunLogger
from backtest.strategies.base import Decision, Strategy, StrategyContext

from .capital_client import TF_TO_RESOLUTION, CapitalClient
from .live_position import LivePosition, apply_trailing


logger = logging.getLogger("live.runner")


def _utcnow_naive() -> pd.Timestamp:
    """UTC-naive timestamp — backtest searchsorted konzisztencia."""
    return pd.Timestamp(datetime.now(timezone.utc)).tz_convert("UTC").tz_localize(None)


def _to_utc_naive(ts) -> pd.Timestamp:
    """Bármilyen datetime/Timestamp-et UTC-naive Timestamp-re alakít."""
    t = pd.Timestamp(ts)
    if t.tz is not None:
        t = t.tz_convert("UTC").tz_localize(None)
    return t


def _extract_position_deal_id(confirm: dict) -> Optional[str]:
    """
    A Capital confirm payload-jában a `dealId` az order-id, a tényleges
    pozíció-id az `affectedDeals[0].dealId`-ben van. A get_open_positions
    ez utóbbival listázza a pozíciókat.
    """
    affected = confirm.get("affectedDeals") if isinstance(confirm, dict) else None
    if isinstance(affected, list) and affected:
        first = affected[0]
        if isinstance(first, dict) and first.get("dealId"):
            return str(first.get("dealId"))
    return confirm.get("dealId") if isinstance(confirm, dict) else None


@dataclass
class LiveConfig:
    epic: str
    candle_max_points: int = 500
    candle_refresh_seconds: float = 60.0    # ennyi időnként pull-olunk friss candle-okat
    position_poll_seconds: float = 10.0     # nyitott pozíciók REST poll
    dry_run: bool = False                    # ha True, nem nyit valódi pozíciót, csak logol

    # Grace period: a frissen nyitott pozíciót N sec-ig nem zárjuk lokálisan
    # (a Capital REST/positions késleltetve látja az új deal_id-t)
    reconcile_grace_seconds: float = 30.0
    # Csak akkor zárjuk lokálisan, ha M egymás utáni poll-ban hiányzik a pozíció
    reconcile_missing_polls: int = 3

    # Rate limit — safety net a runaway-open ellen (edit-flood bug 2026-07-20)
    open_attempt_window_seconds: float = 60.0
    open_attempt_max_in_window: int = 3

    # /confirms retry — Capital race-window kezelése
    confirm_retry_attempts: int = 5
    confirm_retry_delay_seconds: float = 0.5


class LiveRunner:
    def __init__(
        self,
        strategy: Strategy,
        engine_cfg: EngineConfig,
        live_cfg: LiveConfig,
        client: CapitalClient,
        logger_obj: RunLogger,
    ):
        self.strategy = strategy
        self.engine_cfg = engine_cfg
        self.live_cfg = live_cfg
        self.client = client
        self.log = logger_obj

        self._sizing = SizingConfig(
            mode=engine_cfg.sizing_mode,
            fixed_lot_size=engine_cfg.fixed_lot_size,
            min_order_size=engine_cfg.min_order_size,
            max_order_size=engine_cfg.max_order_size,
            risk_pct=engine_cfg.risk_pct,
            equity=engine_cfg.equity,
        )
        self._tp_layers, self._tp_layer_size_pcts = _resolve_tp_layers(
            engine_cfg.tp_layers, engine_cfg.tp_layer_size_pcts
        )

        self._open_positions: Dict[str, LivePosition] = {}    # deal_id → LivePosition
        self._trade_seq = 0
        self._last_candle_refresh = 0.0
        self._candles_mtf: Dict[str, pd.DataFrame] = {}
        self._recent_open_attempts: List[float] = []          # monotonic ts-ek — rate limit

    # ── főloop ──

    async def run(self) -> None:
        self.client.ensure_login()
        self.client.ensure_account()
        self.epic = self.client.resolve_epic(self.live_cfg.epic)
        self.log.log_text(f"Login + epic resolved: {self.live_cfg.epic} → {self.epic}")

        # Első candle-pull + strategy startup
        await self._refresh_candles(force=True)
        now = _utcnow_naive()
        ctx = StrategyContext(
            epic=self.live_cfg.epic,
            segment_start=now,
            segment_end=now + pd.Timedelta(days=365),
            candles_mtf=self._candles_mtf,
            segment_ticks=None,
        )
        self.strategy.on_segment_start(ctx)
        self.log.log_text(f"Strategy started: {self.strategy.name} | mode: {'DRY-RUN' if self.live_cfg.dry_run else 'LIVE'}")

        await asyncio.gather(
            self._tick_loop(),
            self._candle_refresh_loop(),
            self._position_poll_loop(),
        )

    # ── tick stream ──

    async def _tick_loop(self) -> None:
        async for tick in self.client.stream_ticks(self.epic):
            ts = _to_utc_naive(tick["ts"])
            bid, ask = tick["bid"], tick["ask"]
            # 1) Trailing-update minden nyitott pozícióra
            for pos in list(self._open_positions.values()):
                self._maybe_update_trailing(pos, bid, ask, ts)
            # 2) Stratégia döntés
            try:
                decision = self.strategy.on_tick(ts, bid, ask)
            except Exception as e:
                logger.exception("on_tick hiba: %s", e)
                continue
            if decision is None:
                continue
            self._log_decision_and_maybe_open(decision, ts, bid, ask)

    # ── periodikus candle pull ──

    async def _candle_refresh_loop(self) -> None:
        while True:
            await asyncio.sleep(self.live_cfg.candle_refresh_seconds)
            try:
                await self._refresh_candles(force=False)
            except Exception as e:
                logger.warning("Candle refresh hiba: %s", e)

    async def _refresh_candles(self, force: bool) -> None:
        loop = asyncio.get_event_loop()
        tfs = self.strategy.required_timeframes()
        new_mtf: Dict[str, pd.DataFrame] = {}
        for tf in tfs:
            resolution = TF_TO_RESOLUTION.get(tf, "MINUTE")
            df = await loop.run_in_executor(
                None, self.client.get_prices, self.epic, resolution, self.live_cfg.candle_max_points
            )
            new_mtf[tf] = df
        self._candles_mtf = new_mtf
        self.strategy.refresh_candles(new_mtf)
        self.log.log_text("Candles refreshed: " + ", ".join(f"{tf}={len(df)}" for tf, df in new_mtf.items()))

    # ── pozíció nyitás ──

    def _log_decision_and_maybe_open(self, decision: Decision, ts: pd.Timestamp,
                                      bid: float, ask: float) -> None:
        decision_event_id = self.log.log_decision({
            "ts": ts.isoformat(),
            "epic": self.live_cfg.epic,
            "strategy": self.strategy.name,
            "allow_trade": decision.allow_trade,
            "reason": decision.reason,
            "direction": decision.direction,
            "score": decision.score,
            "size": decision.size,
            "sl_distance": decision.sl_distance,
            "tp_distance": decision.tp_distance,
            "indicators": decision.indicators,
        })
        if not decision.allow_trade or not decision.size:
            return

        # Signal-szintű limit: parent_trade_id alapján csoportosítva
        # (PENDING placeholder is ide számít, így a confirm-hiba nem enged burst-öt)
        open_signal_count = len({p.parent_trade_id or p.trade_id for p in self._open_positions.values()})
        if open_signal_count >= self.engine_cfg.max_open_positions:
            self.log.log_text(
                f"[SIGNAL_SKIP] {decision.direction} score={decision.score} — "
                f"max_open_positions ({self.engine_cfg.max_open_positions}) elérve"
            )
            return

        # Rate limit safety net — csúszó ablak, hard cap
        now_wall = time.monotonic()
        window = self.live_cfg.open_attempt_window_seconds
        self._recent_open_attempts = [t for t in self._recent_open_attempts if now_wall - t < window]
        if len(self._recent_open_attempts) >= self.live_cfg.open_attempt_max_in_window:
            self.log.log_text(
                f"[RATE_LIMIT] {decision.direction} — "
                f"{self.live_cfg.open_attempt_max_in_window} open-attempt/"
                f"{window:.0f}s elérve, skip"
            )
            return
        self._recent_open_attempts.append(now_wall)

        self.log.log_text(
            f"[SIGNAL] {decision.direction} score={decision.score} "
            f"sl_dist={decision.sl_distance} tp_dist={decision.tp_distance} "
            f"layers={self._tp_layers}"
        )

        total_size = compute_size(
            self._sizing,
            strategy_size=float(decision.size or 0.0),
            score=decision.score,
            sl_distance=decision.sl_distance,
        )
        if total_size <= 0:
            return

        sl_dist = float(decision.sl_distance or 0.0)
        tp_dist = float(decision.tp_distance) if decision.tp_distance else None
        entry_price = ask if decision.direction == "BUY" else bid
        if decision.direction == "BUY":
            sl_level = entry_price - sl_dist
        else:
            sl_level = entry_price + sl_dist

        ind = decision.indicators or {}
        atr_at_open = ind.get("atr") or ind.get("atr_ltf")
        try:
            atr_at_open = float(atr_at_open) if atr_at_open is not None else None
        except (TypeError, ValueError):
            atr_at_open = None

        parent_trade_id: Optional[int] = None
        for layer_idx, (layer_tp_pct, layer_size_pct) in enumerate(zip(self._tp_layers, self._tp_layer_size_pcts)):
            layer_size = total_size * layer_size_pct
            if layer_size <= 0:
                continue
            if tp_dist is not None and layer_tp_pct > 0:
                layer_tp_dist = tp_dist * layer_tp_pct
                tp_level = (entry_price + layer_tp_dist) if decision.direction == "BUY" else (entry_price - layer_tp_dist)
            else:
                layer_tp_dist = None
                tp_level = None

            self._trade_seq += 1
            trade_id = self._trade_seq
            if parent_trade_id is None:
                parent_trade_id = trade_id

            if self.live_cfg.dry_run:
                deal_id = f"DRY-{trade_id}"
                deal_ref = deal_id
                entry_actual = entry_price
                confirm: Dict = {}
            else:
                # 1) POST /positions — a create-hiba után biztosan nincs pozíció
                try:
                    resp = self.client.create_position(
                        epic=self.epic,
                        direction=decision.direction,
                        size=layer_size,
                        stop_distance=sl_dist,
                        profit_distance=layer_tp_dist,
                        trailing_stop=False,    # MANUÁLIS trailing — a runner kezeli
                    )
                    deal_ref = resp.get("dealReference")
                except Exception as e:
                    self.log.log_text(f"[OPEN ERROR create] layer={layer_idx}: {e}")
                    logger.exception("create_position hiba: %s", e)
                    continue    # nincs pozíció, próbáljuk a következő layert

                # 2) GET /confirms/{ref} retry-jal — 404 = Capital race, nem "sikertelen"
                try:
                    confirm = self.client.confirm_position_with_retry(
                        deal_ref,
                        attempts=self.live_cfg.confirm_retry_attempts,
                        delay_seconds=self.live_cfg.confirm_retry_delay_seconds,
                    ) if deal_ref else {}
                except Exception as e:
                    # A POST már átment — a pozíció valószínűleg NYITVA van Capital-on,
                    # csak nem tudjuk lekérdezni a confirmation-t. PENDING placeholder-t
                    # rakunk _open_positions-be hogy a max_open továbbra is védjen.
                    self.log.log_text(
                        f"[CONFIRM FAIL] layer={layer_idx} deal_ref={deal_ref}: {e} — "
                        f"PENDING placeholder felvéve, reconcile fogja rendezni"
                    )
                    logger.warning("confirm_position retry kimerült: %s", e)
                    pending_deal_id = f"PENDING-{deal_ref}"
                    placeholder = LivePosition(
                        trade_id=trade_id,
                        deal_id=pending_deal_id,
                        deal_ref=str(deal_ref),
                        decision_event_id=decision_event_id,
                        direction=decision.direction,
                        entry_ts=ts,
                        entry_price=entry_price,    # bid/ask a signal pillanatában
                        size=layer_size,
                        sl=sl_level,
                        tp=tp_level,
                        initial_sl=sl_level,
                        atr_at_open=atr_at_open,
                        open_indicators=dict(ind),
                        parent_trade_id=parent_trade_id,
                        layer_idx=layer_idx,
                        layer_count=len(self._tp_layers),
                        layer_tp_pct=layer_tp_pct,
                        layer_size_pct=layer_size_pct,
                        verified_open=False,
                    )
                    self._open_positions[pending_deal_id] = placeholder
                    self.log.log_event("pending_open", {
                        "ts": ts.isoformat(),
                        "trade_id": trade_id,
                        "parent_trade_id": parent_trade_id,
                        "layer_idx": layer_idx,
                        "decision_event_id": decision_event_id,
                        "deal_id": pending_deal_id,
                        "deal_ref": deal_ref,
                        "direction": decision.direction,
                        "size": layer_size,
                        "reason": str(e),
                    })
                    # NEM continue-lunk le a layer-loopon: az első layer confirm-hibája
                    # után a többi layer nyitása is felesleges (max_open már counter-elve).
                    return

                deal_status = (confirm.get("dealStatus") or "").upper()
                deal_id = _extract_position_deal_id(confirm) or deal_ref
                level = confirm.get("level")
                is_null_uuid = isinstance(deal_id, str) and deal_id.startswith("00000000-0000-0000")
                is_rejected = (
                    deal_status == "REJECTED"
                    or is_null_uuid
                    or level is None
                    or level == 0
                )
                if is_rejected:
                    reason = confirm.get("reason") or deal_status or "unknown"
                    self.log.log_text(
                        f"[REJECTED] {decision.direction} layer={layer_idx} reason={reason} "
                        f"deal_id={deal_id} deal_ref={deal_ref}"
                    )
                    self.log.log_event("rejected", {
                        "ts": ts.isoformat(),
                        "trade_id": trade_id,
                        "parent_trade_id": parent_trade_id,
                        "layer_idx": layer_idx,
                        "decision_event_id": decision_event_id,
                        "deal_id": deal_id,
                        "deal_ref": deal_ref,
                        "direction": decision.direction,
                        "size": layer_size,
                        "requested_sl_distance": sl_dist,
                        "requested_tp_distance": layer_tp_dist,
                        "confirm": confirm,
                    })
                    continue
                entry_actual = float(level)

            pos = LivePosition(
                trade_id=trade_id,
                deal_id=str(deal_id),
                deal_ref=str(deal_ref),
                decision_event_id=decision_event_id,
                direction=decision.direction,
                entry_ts=ts,
                entry_price=entry_actual,
                size=layer_size,
                sl=sl_level,
                tp=tp_level,
                initial_sl=sl_level,
                atr_at_open=atr_at_open,
                open_indicators=dict(ind),
                parent_trade_id=parent_trade_id,
                layer_idx=layer_idx,
                layer_count=len(self._tp_layers),
                layer_tp_pct=layer_tp_pct,
                layer_size_pct=layer_size_pct,
            )
            self._open_positions[pos.deal_id] = pos

            self.log.log_text(
                f"[OPEN] {pos.direction} layer={layer_idx}/{len(self._tp_layers)-1} "
                f"@ {pos.entry_price:.4f} size={pos.size:.3f} "
                f"sl={pos.sl:.4f} tp={pos.tp if pos.tp is None else round(pos.tp, 4)} "
                f"trade_id={pos.trade_id} parent={parent_trade_id} "
                f"{'[DRY]' if self.live_cfg.dry_run else ''}"
            )
            pos.open_event_id = self.log.log_open({
                "ts": ts.isoformat(),
                "epic": self.live_cfg.epic,
                "strategy": self.strategy.name,
                "trade_id": trade_id,
                "parent_trade_id": parent_trade_id,
                "layer_idx": layer_idx,
                "layer_count": len(self._tp_layers),
                "layer_tp_pct": layer_tp_pct,
                "layer_size_pct": layer_size_pct,
                "decision_event_id": decision_event_id,
                "deal_id": pos.deal_id,
                "deal_ref": pos.deal_ref,
                "direction": pos.direction,
                "entry_price": pos.entry_price,
                "size": pos.size,
                "sl": pos.sl,
                "tp": pos.tp,
                "atr_at_open": pos.atr_at_open,
                "dry_run": self.live_cfg.dry_run,
                "score": decision.score,
                "indicators": dict(ind),
                "confirm": confirm,
            })

    # ── trailing: minden tick-en a nyitott pozíciókra ──

    def _maybe_update_trailing(self, pos: LivePosition, bid: float, ask: float,
                               ts: pd.Timestamp) -> None:
        new_sl = apply_trailing(
            pos, bid, ask,
            trailing_mode=self.engine_cfg.trailing_mode,
            trail_atr_mult=self.engine_cfg.trail_atr_mult,
            break_even_trigger_atr_mult=self.engine_cfg.break_even_trigger_atr_mult,
        )
        if new_sl is None:
            return
        # SL elmozdult — REST PUT (kivéve dry-run)
        if not self.live_cfg.dry_run:
            try:
                self.client.update_position(pos.deal_id, stop_level=new_sl)
            except Exception as e:
                logger.warning("SL update hiba (%s): %s", pos.deal_id, e)
                return
        old_sl = pos.sl
        pos.sl = new_sl
        self.log.log_text(
            f"[SL_MOVED] trade_id={pos.trade_id} {pos.direction} {old_sl:.4f} → {new_sl:.4f} "
            f"{'(break-even)' if pos.break_even_done else ''}"
        )
        self.log.log_event("sl_moved", {
            "ts": ts.isoformat(),
            "trade_id": pos.trade_id,
            "deal_id": pos.deal_id,
            "old_sl": old_sl,
            "new_sl": new_sl,
            "bid": bid,
            "ask": ask,
            "break_even_done": pos.break_even_done,
        })

    # ── nyitott pozíciók poll-ja + zárás-detektálás ──

    async def _position_poll_loop(self) -> None:
        while True:
            await asyncio.sleep(self.live_cfg.position_poll_seconds)
            try:
                await self._reconcile_positions()
            except Exception as e:
                logger.warning("Position poll hiba: %s", e)

    async def _reconcile_positions(self) -> None:
        if not self._open_positions:
            return
        if self.live_cfg.dry_run:
            return    # dry-run: a runner maga nem zárhat le, későbbi feature
        loop = asyncio.get_event_loop()
        live = await loop.run_in_executor(None, self.client.get_open_positions)
        live_deal_ids = set()
        live_by_deal_ref: Dict[str, str] = {}    # dealReference → dealId (PENDING adopt-hoz)
        for p in live:
            pos_node = p.get("position", {})
            d = pos_node.get("dealId")
            if d:
                live_deal_ids.add(str(d))
            dref = pos_node.get("dealReference") or p.get("dealReference")
            if dref and d:
                live_by_deal_ref[str(dref)] = str(d)

        now = _utcnow_naive()

        for pos in list(self._open_positions.values()):
            # PENDING placeholder → próbáljuk adoptálni dealReference alapján
            if pos.deal_id.startswith("PENDING-") and pos.deal_ref in live_by_deal_ref:
                real_deal_id = live_by_deal_ref[pos.deal_ref]
                self.log.log_text(
                    f"[ADOPT] PENDING placeholder → valódi dealId "
                    f"trade_id={pos.trade_id} deal_ref={pos.deal_ref} deal_id={real_deal_id}"
                )
                self.log.log_event("pending_adopted", {
                    "ts": now.isoformat(),
                    "trade_id": pos.trade_id,
                    "deal_ref": pos.deal_ref,
                    "old_deal_id": pos.deal_id,
                    "new_deal_id": real_deal_id,
                })
                del self._open_positions[pos.deal_id]
                pos.deal_id = real_deal_id
                pos.verified_open = True
                pos.missing_poll_count = 0
                self._open_positions[real_deal_id] = pos
                continue

            if pos.deal_id in live_deal_ids:
                # Megerősítve a Capital-on
                if not pos.verified_open:
                    pos.verified_open = True
                    self.log.log_text(f"[VERIFIED] trade_id={pos.trade_id} deal_id={pos.deal_id}")
                pos.missing_poll_count = 0
                continue

            # Hiányzik a Capital-tól. Három ok lehet:
            #   1) Még nem szinkronizált (friss nyitás, grace period alatt)
            #   2) Tényleg lezárult (SL/TP/manuál close)
            #   3) PENDING placeholder ami sosem lett valódi (create sikerült de
            #      a pozíció mégsem nyílt meg — Capital-oldali edge case)
            age_sec = (now - pos.entry_ts).total_seconds()
            if not pos.verified_open and age_sec < self.live_cfg.reconcile_grace_seconds:
                # Frissen nyitottuk, várjunk amíg megjelenik
                continue

            pos.missing_poll_count += 1
            if pos.missing_poll_count < self.live_cfg.reconcile_missing_polls:
                self.log.log_text(
                    f"[POLL_MISS] trade_id={pos.trade_id} ({pos.missing_poll_count}/"
                    f"{self.live_cfg.reconcile_missing_polls}) — még nem zárjuk"
                )
                continue

            # PENDING placeholder aki sosem lett verified → csak töröljük, ne "close"-oljunk
            # (nincs mit close-olni, csak state cleanup)
            if pos.deal_id.startswith("PENDING-") and not pos.verified_open:
                self.log.log_text(
                    f"[PENDING GC] trade_id={pos.trade_id} deal_ref={pos.deal_ref} — "
                    f"placeholder eltávolítva ({pos.missing_poll_count} poll-on át hiányzott)"
                )
                self.log.log_event("pending_gc", {
                    "ts": now.isoformat(),
                    "trade_id": pos.trade_id,
                    "deal_ref": pos.deal_ref,
                    "age_sec": age_sec,
                })
                del self._open_positions[pos.deal_id]
                continue

            # M egymást követő poll-on hiányzott + nem grace alatt → lezárult
            await self._close_position(pos)

    async def _close_position(self, pos: LivePosition, exit_price: Optional[float] = None,
                              exit_reason: str = "BROKER_CLOSE") -> None:
        # A Capital tx néhány másodperc múlva válik elérhetővé → retry
        tx = None
        currency = None
        if not self.live_cfg.dry_run:
            tx = await self._fetch_close_transaction(pos.deal_id)
        pnl_set_from_tx = False
        if tx:
            try:
                cl = tx.get("closeLevel")
                if cl is not None:
                    exit_price = float(cl)
            except (TypeError, ValueError):
                pass
            currency = tx.get("currency")
            # Capital `/history/transactions` válaszában a `size` mező a P&L értéke
            # (legacy elnevezés, nem a lot). A `profitAndLoss` ritkán szerepel.
            pnl_raw = tx.get("profitAndLoss")
            if pnl_raw is None:
                pnl_raw = tx.get("size")
            if pnl_raw is not None:
                try:
                    s = str(pnl_raw).replace(",", "")
                    digits = "".join(ch for ch in s if ch in "0123456789.-+")
                    if digits:
                        pos.pnl = float(digits)
                        pnl_set_from_tx = True
                except (TypeError, ValueError):
                    pass

        if exit_price is None:
            exit_price = pos.entry_price  # fallback ha sehonnan nincs adat
        pos.exit_ts = _utcnow_naive()
        pos.exit_price = exit_price
        pos.exit_reason = exit_reason
        if not pnl_set_from_tx:
            # PnL becslés az exit_price-ból (lehet a Capital `level`-ből, vagy entry-fallback)
            if pos.direction == "BUY":
                pos.pnl = (exit_price - pos.entry_price) * pos.size
            else:
                pos.pnl = (pos.entry_price - exit_price) * pos.size

        self.log.log_text(
            f"[CLOSE] trade_id={pos.trade_id} {pos.direction} @ {exit_price:.4f} "
            f"pnl={pos.pnl:+.2f}{(' ' + currency) if currency else ''} "
            f"reason={exit_reason} hold={(pos.exit_ts - pos.entry_ts).total_seconds():.0f}s"
        )
        self.log.log_close({
            "ts": pos.exit_ts.isoformat(),
            "epic": self.live_cfg.epic,
            "strategy": self.strategy.name,
            "trade_id": pos.trade_id,
            "parent_trade_id": pos.parent_trade_id or pos.trade_id,
            "layer_idx": pos.layer_idx,
            "layer_count": pos.layer_count,
            "layer_tp_pct": pos.layer_tp_pct,
            "layer_size_pct": pos.layer_size_pct,
            "decision_event_id": pos.decision_event_id,
            "open_event_id": pos.open_event_id,
            "deal_id": pos.deal_id,
            "deal_ref": pos.deal_ref,
            "direction": pos.direction,
            "entry_ts": pos.entry_ts.isoformat(),
            "entry_price": pos.entry_price,
            "exit_price": exit_price,
            "size": pos.size,
            "initial_sl": pos.initial_sl,
            "final_sl": pos.sl,
            "tp": pos.tp,
            "break_even_done": pos.break_even_done,
            "atr_at_open": pos.atr_at_open,
            "pnl": pos.pnl,
            "pnl_source": "transaction" if pnl_set_from_tx else "estimated",
            "currency": currency,
            "exit_reason": exit_reason,
            "hold_seconds": (pos.exit_ts - pos.entry_ts).total_seconds(),
            "transaction": tx,
        })
        del self._open_positions[pos.deal_id]
        try:
            self.strategy.on_position_closed(pos)
        except Exception:
            pass

    async def _fetch_close_transaction(self, deal_id: str) -> Optional[Dict]:
        """Retry-val próbálja a /history/transactions-t — a Capital néhány sec szinkron."""
        loop = asyncio.get_event_loop()
        for attempt in range(5):
            await asyncio.sleep(2 ** attempt * 0.5)    # 0.5, 1, 2, 4, 8 sec — összesen ~15s
            try:
                tx = await loop.run_in_executor(
                    None, self.client.get_transaction_for_deal, deal_id
                )
                if tx:
                    return tx
            except Exception as e:
                logger.warning("transaction fetch hiba (%d/5): %s", attempt + 1, e)
        return None

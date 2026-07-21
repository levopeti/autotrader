"""
Signal-replay stratégia: a SignalArchive-ból betöltött Telegram-jeleket
visszajátssza tick adaton. Konzisztens a live Position state machine-nel
(WAITING zónába esésig → zóna-belépés → SL/TP).
"""
from __future__ import annotations

import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

# A repository-gyökeret kell importálnunk a `signals` package-hoz
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from signals.archive import SignalArchive  # noqa: E402

from .base import Decision, Strategy, StrategyContext  # noqa: E402


@dataclass
class _SignalRow:
    id: int
    ts: pd.Timestamp
    chat_id: int
    chat_name: str
    direction: str
    entry_low: float
    entry_high: float
    tp_list: List[float]
    sl: float
    state: str = "WAITING"   # WAITING | USED | EXPIRED


class SignalReplay(Strategy):
    name = "signal_replay"

    def __init__(self, params: dict):
        super().__init__(params)
        p = params
        self.signals_dir: str = p["signals_dir"]
        self.channels: Optional[List[int]] = p.get("channels")
        self.signal_timeout_minutes: float = p.get("signal_timeout_minutes", 15.0)
        self.tp_idx: int = int(p.get("tp_idx", 0))    # 0 = legközelebbi, -1 = legtávolabbi
        self.entry_zone_expand: float = float(p.get("entry_zone_expand", 0.0))
        self.candle_tf: str = p.get("candle_tf", "1min")

        self._archive = SignalArchive(self.signals_dir)
        self._signals: List[_SignalRow] = []
        self._next_to_activate: int = 0   # pointer az _signals-ra

    def required_timeframes(self) -> List[str]:
        return [self.candle_tf]

    def on_segment_start(self, ctx: StrategyContext) -> None:
        seg_start = ctx.segment_start
        seg_end = ctx.segment_end
        if seg_start.tz is None:
            seg_start = seg_start.tz_localize("UTC")
        if seg_end.tz is None:
            seg_end = seg_end.tz_localize("UTC")
        df = self._archive.load_range(
            start=seg_start - pd.Timedelta(minutes=self.signal_timeout_minutes),
            end=seg_end,
            channels=self.channels,
            only_valid=True,
        )
        rows: List[_SignalRow] = []
        for i, r in df.iterrows():
            tp_list = r.get("parsed.tp_list") or []
            if not isinstance(tp_list, list) or not tp_list:
                continue
            ts_raw = pd.Timestamp(r["ts_utc"])
            # A runner tz-naive ts-eket ad át (numpy datetime64-ből),
            # ezért UTC-naive-ra konvertálunk
            ts_naive = ts_raw.tz_convert("UTC").tz_localize(None) if ts_raw.tz is not None else ts_raw
            rows.append(_SignalRow(
                id=int(i),
                ts=ts_naive,
                chat_id=int(r["chat_id"]),
                chat_name=str(r.get("chat_name", "")),
                direction=str(r["parsed.direction"]),
                entry_low=float(r["parsed.entry_low"]) - self.entry_zone_expand,
                entry_high=float(r["parsed.entry_high"]) + self.entry_zone_expand,
                tp_list=[float(x) for x in tp_list],
                sl=float(r["parsed.sl"]),
            ))
        # Időrendi sorrend, hogy a `next_to_activate` pointer jól haladjon
        rows.sort(key=lambda s: s.ts)
        self._signals = rows
        self._next_to_activate = 0

    def on_tick(self, ts: pd.Timestamp, bid: float, ask: float) -> Optional[Decision]:
        timeout = pd.Timedelta(minutes=self.signal_timeout_minutes)

        # Aktív (WAITING) signalok közül megnézzük az elsőt amelyik zónába esik.
        # Közben az időkeretükön túliakat EXPIRED-be.
        active: List[_SignalRow] = []
        for s in self._signals[: self._next_to_activate]:
            if s.state != "WAITING":
                continue
            if ts - s.ts > timeout:
                s.state = "EXPIRED"
                continue
            active.append(s)
        # Új signalok aktiválása: amik időben már beérkeztek
        while self._next_to_activate < len(self._signals):
            cand = self._signals[self._next_to_activate]
            if cand.ts > ts:
                break
            self._next_to_activate += 1
            if ts - cand.ts > timeout:
                cand.state = "EXPIRED"
            else:
                active.append(cand)

        if not active:
            return None

        # Mely signalra adunk Decision-t? Az első, amelyik zónában van.
        for s in active:
            trigger = ask if s.direction == "BUY" else bid
            if not (s.entry_low <= trigger <= s.entry_high):
                continue

            # Megjelöljük USED-nek (egy signal max 1 trade-ot szül)
            s.state = "USED"

            entry_price = trigger
            tp_choice = s.tp_list[self.tp_idx] if 0 <= self.tp_idx < len(s.tp_list) else s.tp_list[-1]

            sl_distance = abs(entry_price - s.sl)
            tp_distance = abs(tp_choice - entry_price)

            return Decision(
                ts=ts,
                allow_trade=True,
                reason="ok",
                direction=s.direction,
                score=1.0,
                size=1.0,
                sl_distance=sl_distance,
                tp_distance=tp_distance,
                indicators={
                    "signal_id": s.id,
                    "signal_ts": s.ts.isoformat(),
                    "chat_id": s.chat_id,
                    "chat_name": s.chat_name,
                    "entry_low": s.entry_low,
                    "entry_high": s.entry_high,
                    "tp_chosen": tp_choice,
                    "tp_idx": self.tp_idx,
                    "n_tp": len(s.tp_list),
                    "sl": s.sl,
                },
            )
        return None

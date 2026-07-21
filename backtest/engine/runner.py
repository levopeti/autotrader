from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional

import pandas as pd

from ..data.candle_builder import build_global_mtf
from ..data.gap_detector import Segment, detect_segments
from ..runlog.run_logger import RunLogger
from ..strategies.base import Decision, Strategy, StrategyContext
from .metrics import compute_metrics
from .position import Position, calc_pnl, check_exit
from .position_sizer import SizingConfig, compute_size


@dataclass
class EngineConfig:
    epic: str
    candle_tf: str
    max_gap_factor: float = 2.0
    min_segment_duration: str = "1h"
    max_open_positions: int = 1
    allow_multiple_directions: bool = False
    max_hold_seconds: Optional[float] = None
    slippage: float = 0.0
    commission_per_trade: float = 0.0
    trailing_mode: str = "none"                  # none | break_even | atr_trail
    trail_atr_mult: float = 1.5
    break_even_trigger_atr_mult: float = 1.0

    # Sizing
    sizing_mode: str = "from_strategy"           # from_strategy | fixed_lot | score_scaled | fixed_risk
    fixed_lot_size: float = 1.0
    min_order_size: float = 0.01
    max_order_size: float = 10.0
    risk_pct: float = 0.01
    equity: float = 10_000.0


class BacktestRunner:
    def __init__(self, strategy: Strategy, engine_cfg: EngineConfig, logger: RunLogger):
        self.strategy = strategy
        self.cfg = engine_cfg
        self.logger = logger
        self.positions: List[Position] = []
        self._trade_seq = 0
        self._sizing = SizingConfig(
            mode=engine_cfg.sizing_mode,
            fixed_lot_size=engine_cfg.fixed_lot_size,
            min_order_size=engine_cfg.min_order_size,
            max_order_size=engine_cfg.max_order_size,
            risk_pct=engine_cfg.risk_pct,
            equity=engine_cfg.equity,
        )

    def run(self, ticks: pd.DataFrame) -> Dict:
        segments = detect_segments(
            ticks,
            candle_tf=self.cfg.candle_tf,
            max_gap_factor=self.cfg.max_gap_factor,
            min_segment_duration=self.cfg.min_segment_duration,
        )
        self.logger.log_text(f"Szegmensek száma: {len(segments)}")

        tfs = self.strategy.required_timeframes()
        candles_mtf = build_global_mtf(ticks, tfs)
        self.logger.log_text(
            "Global MTF candles: " + ", ".join(f"{tf}={len(df)}" for tf, df in candles_mtf.items())
        )

        for seg_i, seg in enumerate(segments):
            self._run_segment(seg_i, seg, ticks, candles_mtf)

        metrics = compute_metrics(self.positions)
        self.logger.write_metrics(metrics)
        self.logger.log_text(f"Kész. n_trades={metrics.get('n_trades')} pnl={metrics.get('total_pnl')}")
        return metrics

    def _run_segment(
        self,
        seg_i: int,
        seg: Segment,
        ticks: pd.DataFrame,
        candles_mtf: Dict[str, pd.DataFrame],
    ) -> None:
        self.logger.log_segment({
            "segment_idx": seg_i,
            "start_ts": seg.start_ts.isoformat(),
            "end_ts": seg.end_ts.isoformat(),
            "n_ticks": seg.n_ticks,
            "duration_hours": round(seg.duration.total_seconds() / 3600, 3),
            "candle_counts": {tf: len(df) for tf, df in candles_mtf.items()},
        })

        seg_ticks = ticks.iloc[seg.start_idx:seg.end_idx]
        ctx = StrategyContext(
            epic=self.cfg.epic,
            segment_start=seg.start_ts,
            segment_end=seg.end_ts,
            candles_mtf=candles_mtf,
            segment_ticks=seg_ticks,
        )
        self.strategy.on_segment_start(ctx)

        open_positions: List[Position] = []
        ts_arr = seg_ticks["timestamp_utc"].values
        bid_arr = seg_ticks["bid"].values
        ask_arr = seg_ticks["ask"].values

        for i in range(len(seg_ticks)):
            ts = pd.Timestamp(ts_arr[i])
            bid = float(bid_arr[i])
            ask = float(ask_arr[i])

            still_open: List[Position] = []
            for pos in open_positions:
                exit_info = check_exit(
                    pos, bid, ask, ts,
                    max_hold_seconds=self.cfg.max_hold_seconds,
                    trailing_mode=self.cfg.trailing_mode,
                    trail_atr_mult=self.cfg.trail_atr_mult,
                    break_even_trigger_atr_mult=self.cfg.break_even_trigger_atr_mult,
                )
                if exit_info is None:
                    still_open.append(pos)
                    continue
                self._close_position(pos, exit_info[0], ts, exit_info[1])
            open_positions = still_open

            if len(open_positions) >= self.cfg.max_open_positions:
                continue

            decision = self.strategy.on_tick(ts, bid, ask)
            if decision is None:
                continue

            decision_event_id = self.logger.log_decision({
                "ts": ts.isoformat(),
                "epic": self.cfg.epic,
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

            if not decision.allow_trade or decision.direction is None or not decision.size:
                continue

            if not self.cfg.allow_multiple_directions:
                if any(p.direction == decision.direction for p in open_positions):
                    continue

            self._open_position(decision, decision_event_id, ts, bid, ask, open_positions)

        for pos in open_positions:
            last_ts = pd.Timestamp(ts_arr[-1])
            last_bid = float(bid_arr[-1])
            last_ask = float(ask_arr[-1])
            exit_price = last_bid if pos.direction == "BUY" else last_ask
            self._close_position(pos, exit_price, last_ts, "SEGMENT_END")

        self.strategy.on_segment_end()

    def _open_position(
        self,
        decision: Decision,
        decision_event_id: str,
        ts: pd.Timestamp,
        bid: float,
        ask: float,
        open_positions: List[Position],
    ) -> None:
        direction = decision.direction
        size = compute_size(
            self._sizing,
            strategy_size=float(decision.size or 0.0),
            score=decision.score,
            sl_distance=decision.sl_distance,
        )
        if size <= 0:
            return
        sl_dist = float(decision.sl_distance or 0.0)
        tp_dist = float(decision.tp_distance) if decision.tp_distance is not None else None

        if direction == "BUY":
            entry = ask + self.cfg.slippage
            sl = entry - sl_dist
            tp = (entry + tp_dist) if tp_dist is not None else None
        else:
            entry = bid - self.cfg.slippage
            sl = entry + sl_dist
            tp = (entry - tp_dist) if tp_dist is not None else None

        ind = decision.indicators or {}
        atr_at_open = ind.get("atr") or ind.get("atr_ltf")
        try:
            atr_at_open = float(atr_at_open) if atr_at_open is not None else None
        except (TypeError, ValueError):
            atr_at_open = None

        self._trade_seq += 1
        pos = Position(
            id=self._trade_seq,
            decision_event_id=decision_event_id,
            open_event_id=None,
            direction=direction,
            entry_ts=ts,
            entry_price=entry,
            size=size,
            sl=sl,
            tp=tp,
            score=float(decision.score or 0.0),
            open_indicators=dict(decision.indicators),
            open_note=decision.reason,
            initial_sl=sl,
            atr_at_open=atr_at_open,
        )

        pos.open_event_id = self.logger.log_open({
            "ts": ts.isoformat(),
            "epic": self.cfg.epic,
            "strategy": self.strategy.name,
            "trade_id": pos.id,
            "decision_event_id": decision_event_id,
            "direction": direction,
            "entry_price": entry,
            "size": size,
            "sl": sl,
            "tp": tp,
            "score": pos.score,
            "indicators": pos.open_indicators,
        })

        self.positions.append(pos)
        open_positions.append(pos)

    def _close_position(
        self,
        pos: Position,
        exit_price: float,
        ts: pd.Timestamp,
        reason: str,
    ) -> None:
        pos.exit_ts = ts
        pos.exit_price = exit_price
        pos.exit_reason = reason
        pos.pnl = calc_pnl(pos.direction, pos.entry_price, exit_price, pos.size) - self.cfg.commission_per_trade

        self.logger.log_close({
            "ts": ts.isoformat(),
            "epic": self.cfg.epic,
            "strategy": self.strategy.name,
            "trade_id": pos.id,
            "decision_event_id": pos.decision_event_id,
            "open_event_id": pos.open_event_id,
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
            "exit_reason": reason,
            "hold_seconds": pos.hold_seconds,
            "score": pos.score,
        })
        self.strategy.on_position_closed(pos)
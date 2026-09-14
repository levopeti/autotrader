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
    max_open_positions: int = 1                  # NB: signal-szintű, nem réteg-szintű
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

    # Layered TP (scaled exit): egy signal → N pozíció rétegezett TP-vel,
    # közös SL-lel (induláskor). tp_layers a tp_distance %-os szintjei (pl.
    # [0.5, 0.75, 1.0] → 3 réteg). tp_layer_size_pcts a teljes méret hányada
    # rétegenként (össz = 1.0). Default: 1 réteg = teljes TP (régi viselkedés).
    tp_layers: Optional[List[float]] = None
    tp_layer_size_pcts: Optional[List[float]] = None

    # close_on_opposite: ha True, az on_tick MINDIG fut (nyitott pozíció közben is),
    # és ha ellentétes irányú allow_trade=True decision érkezik, az összes ellentétes
    # nyitott pozíció ZÁRUL az aktuális bid/ask-on (exit_reason=OPPOSITE_SIGNAL).
    # Új pozíciót ugyanezen a ticken nem nyitunk (csak zárunk, nem flip).
    # Default False → régi viselkedés.
    close_on_opposite: bool = False


# Optuna számára: stringből választható preset-ek a tp_layers-hez.
TP_LAYERS_PRESETS = {
    "single":          ([1.0],            [1.0]),
    "two_equal":       ([0.5, 1.0],       [0.5, 0.5]),
    "three_equal":     ([0.5, 0.75, 1.0], [0.33, 0.34, 0.33]),
    "three_weighted":  ([0.5, 0.75, 1.0], [0.5, 0.3, 0.2]),
    "four_layer":      ([0.4, 0.7, 1.0, 1.3], [0.4, 0.3, 0.2, 0.1]),
}


def apply_tp_layers_preset(engine_block: dict) -> None:
    """
    Ha a config-ban `tp_layers_preset` mező van (pl. az Optuna tette be),
    azt feloldja konkrét `tp_layers` + `tp_layer_size_pcts` listákra.
    """
    preset = engine_block.pop("tp_layers_preset", None)
    if preset is None:
        return
    if preset not in TP_LAYERS_PRESETS:
        raise ValueError(f"Ismeretlen tp_layers_preset: {preset} ({list(TP_LAYERS_PRESETS)})")
    layers, sizes = TP_LAYERS_PRESETS[preset]
    engine_block["tp_layers"] = list(layers)
    engine_block["tp_layer_size_pcts"] = list(sizes)


def _resolve_tp_layers(
    tp_layers: Optional[List[float]],
    tp_layer_size_pcts: Optional[List[float]],
) -> tuple[List[float], List[float]]:
    """
    Layer-listák normalizálása.
    Üres/None → egyetlen 1.0 layer (régi single-TP viselkedés).
    Size-pctek nélkül → uniform szétosztás.
    """
    layers = list(tp_layers) if tp_layers else [1.0]
    if tp_layer_size_pcts:
        if len(tp_layer_size_pcts) != len(layers):
            raise ValueError(f"tp_layer_size_pcts hossza ({len(tp_layer_size_pcts)}) "
                             f"!= tp_layers hossza ({len(layers)})")
        sizes = list(tp_layer_size_pcts)
        total = sum(sizes)
        if total <= 0:
            raise ValueError(f"tp_layer_size_pcts összege <= 0: {total}")
        sizes = [s / total for s in sizes]
    else:
        sizes = [1.0 / len(layers)] * len(layers)
    return layers, sizes


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
        self._tp_layers, self._tp_layer_size_pcts = _resolve_tp_layers(
            engine_cfg.tp_layers, engine_cfg.tp_layer_size_pcts
        )
        # Signal-szintű "logikai pozíció" számláló — egy signal akkor is 1, ha N rétegre bontódik.
        # A max_open_positions ezt korlátozza, nem az egyedi réteg-pozíciókat.
        self._open_signal_groups: Dict[int, int] = {}    # trade_id → még nyitott rétegek száma

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

            # Signal-szintű limit: az `open_positions` réteg-pozíciókat tartalmaz, de a
            # max_open_positions logikai signal-okra vonatkozik (egy signal = N réteg).
            # close_on_opposite=True esetén az on_tick-et akkor is meghívjuk, ha
            # max_open elérve — mert a decision alapján zárhatunk ellentétes pozíciót.
            open_signal_count = len({p.parent_trade_id or p.id for p in open_positions})
            if open_signal_count >= self.cfg.max_open_positions and not self.cfg.close_on_opposite:
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

            # close_on_opposite: allow_trade=True + ellentétes irányú nyitott →
            # zárjuk az ellentéteseket az aktuális ticken, és NEM nyitunk újat.
            if self.cfg.close_on_opposite and open_positions:
                opposite = [p for p in open_positions if p.direction != decision.direction]
                if opposite:
                    for pos in opposite:
                        exit_price = bid if pos.direction == "BUY" else ask
                        self._close_position(pos, exit_price, ts, "OPPOSITE_SIGNAL")
                    open_positions = [p for p in open_positions if p.direction == decision.direction]
                    continue

            # A közeli max_open-check (close_on_opposite=True path esetére, hogy
            # zárás nélkül azonos irányú signal ne toljon többet a max_open fölé).
            open_signal_count = len({p.parent_trade_id or p.id for p in open_positions})
            if open_signal_count >= self.cfg.max_open_positions:
                continue

            # allow_multiple_directions: ha False, akkor BUY és SELL nem lehet
            # egyszerre nyitva — vagyis ellentétes irányú nyitott pozíció esetén
            # blokkolunk. Azonos irányt a `max_open_positions` korlátoz.
            if not self.cfg.allow_multiple_directions:
                if any(p.direction != decision.direction for p in open_positions):
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
        total_size = compute_size(
            self._sizing,
            strategy_size=float(decision.size or 0.0),
            score=decision.score,
            sl_distance=decision.sl_distance,
        )
        if total_size <= 0:
            return
        sl_dist = float(decision.sl_distance or 0.0)
        tp_dist = float(decision.tp_distance) if decision.tp_distance is not None else None

        if direction == "BUY":
            entry = ask + self.cfg.slippage
            sl = entry - sl_dist
        else:
            entry = bid - self.cfg.slippage
            sl = entry + sl_dist

        ind = decision.indicators or {}
        atr_at_open = ind.get("atr") or ind.get("atr_ltf")
        try:
            atr_at_open = float(atr_at_open) if atr_at_open is not None else None
        except (TypeError, ValueError):
            atr_at_open = None

        # Multi-TP a stratégiától: abszolút TP-távolságok listája. Ha jelen van,
        # felülírja az engine.tp_layers-t. A layer_tp_pct ekkor "logikai" mező
        # az event-logban (= abszolút_tp_dist / max_tp_dist).
        if decision.tp_distances:
            decision_tp_dists = [float(d) for d in decision.tp_distances if d is not None and d > 0]
        else:
            decision_tp_dists = None

        if decision_tp_dists:
            layers_abs = decision_tp_dists
            n_layers = len(layers_abs)
            # Méret-súlyok: ha az engine_cfg-ben passzol a hossz, használjuk; egyébként uniform
            if len(self._tp_layer_size_pcts) == n_layers:
                layer_sizes_pct = self._tp_layer_size_pcts
            else:
                layer_sizes_pct = [1.0 / n_layers] * n_layers
            max_tp_dist = max(layers_abs)
            layers_pct_for_log = [d / max_tp_dist for d in layers_abs]
        else:
            layers_abs = None
            layer_sizes_pct = self._tp_layer_size_pcts
            layers_pct_for_log = self._tp_layers

        parent_trade_id: Optional[int] = None

        for layer_idx, (layer_tp_pct, layer_size_pct) in enumerate(zip(layers_pct_for_log, layer_sizes_pct)):
            layer_size = total_size * layer_size_pct
            if layer_size <= 0:
                continue
            # Tényleges abszolút TP-távolság
            if layers_abs is not None:
                this_tp_dist = layers_abs[layer_idx]
            elif tp_dist is not None and layer_tp_pct > 0:
                this_tp_dist = tp_dist * layer_tp_pct
            else:
                this_tp_dist = None

            if this_tp_dist is not None:
                tp = (entry + this_tp_dist) if direction == "BUY" else (entry - this_tp_dist)
            else:
                tp = None

            self._trade_seq += 1
            if parent_trade_id is None:
                parent_trade_id = self._trade_seq

            # TP-ladder árak: a Decision-ben favourable-irány távolságok érkeznek
            ladder_trig_price = None
            ladder_dest_price = None
            ld_trig = decision.ladder_trigger_distance
            ld_dest = decision.ladder_dest_distance
            if ld_trig is not None and ld_dest is not None and ld_trig > 0 and ld_dest > 0:
                if direction == "BUY":
                    ladder_trig_price = entry + float(ld_trig)
                    ladder_dest_price = entry + float(ld_dest)
                else:
                    ladder_trig_price = entry - float(ld_trig)
                    ladder_dest_price = entry - float(ld_dest)

            pos = Position(
                id=self._trade_seq,
                decision_event_id=decision_event_id,
                open_event_id=None,
                direction=direction,
                entry_ts=ts,
                entry_price=entry,
                size=layer_size,
                sl=sl,
                tp=tp,
                score=float(decision.score or 0.0),
                open_indicators=dict(decision.indicators),
                open_note=decision.reason,
                initial_sl=sl,
                atr_at_open=atr_at_open,
                parent_trade_id=parent_trade_id,
                layer_idx=layer_idx,
                layer_count=len(layers_pct_for_log),
                layer_tp_pct=layer_tp_pct,
                layer_size_pct=layer_size_pct,
                ladder_trigger_price=ladder_trig_price,
                ladder_dest_price=ladder_dest_price,
                exit_at_ts=decision.exit_at_ts,
            )

            pos.open_event_id = self.logger.log_open({
                "ts": ts.isoformat(),
                "epic": self.cfg.epic,
                "strategy": self.strategy.name,
                "trade_id": pos.id,
                "parent_trade_id": parent_trade_id,
                "layer_idx": layer_idx,
                "layer_count": len(layers_pct_for_log),
                "layer_tp_pct": layer_tp_pct,
                "layer_size_pct": layer_size_pct,
                "decision_event_id": decision_event_id,
                "direction": direction,
                "entry_price": entry,
                "size": layer_size,
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
            "parent_trade_id": pos.parent_trade_id or pos.id,
            "layer_idx": pos.layer_idx,
            "layer_count": pos.layer_count,
            "layer_tp_pct": pos.layer_tp_pct,
            "layer_size_pct": pos.layer_size_pct,
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
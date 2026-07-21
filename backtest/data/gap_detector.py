from __future__ import annotations

from dataclasses import dataclass
from typing import List

import pandas as pd


@dataclass
class Segment:
    start_idx: int          # ticks DataFrame index (inclusive)
    end_idx: int            # ticks DataFrame index (exclusive)
    start_ts: pd.Timestamp
    end_ts: pd.Timestamp

    @property
    def n_ticks(self) -> int:
        return self.end_idx - self.start_idx

    @property
    def duration(self) -> pd.Timedelta:
        return self.end_ts - self.start_ts


def detect_segments(
    ticks: pd.DataFrame,
    candle_tf: str,
    max_gap_factor: float = 2.0,
    min_segment_duration: str | pd.Timedelta = "1h",
) -> List[Segment]:
    """
    Felbontja a tick stream-et folyamatos szegmensekre.
    Két tick között megengedett max. szünet = max_gap_factor × candle_tf.
    """
    if ticks.empty:
        return []

    tf = pd.Timedelta(candle_tf)
    gap_threshold = tf * max_gap_factor
    min_dur = pd.Timedelta(min_segment_duration) if isinstance(min_segment_duration, str) else min_segment_duration

    ts = ticks["timestamp_utc"].values
    diffs = pd.Series(ts).diff()

    break_mask = diffs > gap_threshold
    break_positions = list(break_mask[break_mask].index)

    boundaries = [0] + break_positions + [len(ticks)]
    segments: List[Segment] = []
    for i in range(len(boundaries) - 1):
        a, b = boundaries[i], boundaries[i + 1]
        if b - a < 2:
            continue
        start_ts = pd.Timestamp(ts[a])
        end_ts = pd.Timestamp(ts[b - 1])
        if end_ts - start_ts < min_dur:
            continue
        segments.append(Segment(start_idx=a, end_idx=b, start_ts=start_ts, end_ts=end_ts))

    return segments


def summarize(segments: List[Segment]) -> pd.DataFrame:
    rows = [{
        "start_ts": s.start_ts.isoformat(),
        "end_ts": s.end_ts.isoformat(),
        "n_ticks": s.n_ticks,
        "duration_hours": round(s.duration.total_seconds() / 3600, 3),
    } for s in segments]
    return pd.DataFrame(rows)
from __future__ import annotations

import hashlib
import json
import platform
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

from .candle_builder import build_global_mtf
from .gap_detector import Segment, detect_segments


def compute_data_summary(
    ticks: pd.DataFrame,
    parquet_path: str | Path,
    candle_tf: str,
    timeframes: List[str],
    max_gap_factor: float,
    min_segment_duration: str,
    compute_hash: bool = True,
) -> Dict:
    """
    Visszaad egy reproducálhatósági snapshot-ot az adatról.
    Tartalmazza: fájl-hash, tick range, spread stats, MTF candle counts,
    szegmens-statisztika, futási környezet.
    """
    p = Path(parquet_path)

    file_info = _file_info(p, compute_hash=compute_hash)
    tick_info = _tick_info(ticks)

    segments = detect_segments(
        ticks,
        candle_tf=candle_tf,
        max_gap_factor=max_gap_factor,
        min_segment_duration=min_segment_duration,
    )
    seg_info = _segments_info(segments)
    n_total = tick_info.get("count", 0)
    if n_total > 0:
        seg_info["usable_pct"] = round(100.0 * seg_info["usable_ticks"] / n_total, 2)

    candles_mtf = build_global_mtf(ticks, timeframes)
    candle_counts = {tf: int(len(df)) for tf, df in candles_mtf.items()}

    env = {
        "python_version": sys.version.split()[0],
        "platform": platform.platform(),
        "pandas_version": pd.__version__,
        "numpy_version": np.__version__,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
    }

    return {
        "file": file_info,
        "ticks": tick_info,
        "candle_counts": candle_counts,
        "segments": seg_info,
        "engine_data_cfg": {
            "candle_tf": candle_tf,
            "timeframes": list(timeframes),
            "max_gap_factor": max_gap_factor,
            "min_segment_duration": min_segment_duration,
        },
        "env": env,
    }


def print_data_summary(s: Dict) -> None:
    line = "═" * 72
    print(line)
    print("  ADAT-ÖSSZEFOGLALÓ (reprodukálhatósághoz mentve)")
    print(line)

    f = s["file"]
    print(f"  Parquet      : {f['path']}")
    print(f"               : {f['size_bytes']:,} B  | mtime: {f['mtime_utc']}")
    if f.get("sha256"):
        print(f"  SHA-256      : {f['sha256']}")

    t = s["ticks"]
    print(f"  Tickek       : {t['count']:,} | {t['first_ts']} → {t['last_ts']}")
    print(f"               : {t['duration_days']} nap | átlag tick-távolság {t['median_gap_ms']:.0f} ms")
    print(f"  Spread       : átlag {t['spread_mean']:.4f} | median {t['spread_median']:.4f} | p99 {t['spread_p99']:.4f}")

    print(f"  MTF candles  : " + " | ".join(f"{tf}={n}" for tf, n in s["candle_counts"].items()))

    seg = s["segments"]
    print(f"  Szegmensek   : {seg['count']} db (használható: {seg['usable_ticks']:,} tick = {seg['usable_pct']:.1f}%)")
    if seg["count"]:
        print(f"               : tartam — min {seg['min_duration_h']:.1f}h | max {seg['max_duration_h']:.1f}h | össz {seg['total_duration_h']:.1f}h")

    e = s["env"]
    print(f"  Környezet    : Python {e['python_version']} | pandas {e['pandas_version']} | numpy {e['numpy_version']}")
    print(line)


def save_data_summary(s: Dict, run_dir: str | Path) -> Path:
    path = Path(run_dir) / "data_summary.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(s, f, indent=2, default=str, ensure_ascii=False)
    return path


# ─── helpers ────────────────────────────────────────────────────────────────

def _file_info(p: Path, compute_hash: bool) -> Dict:
    if not p.exists():
        return {"path": str(p), "exists": False}
    stat = p.stat()
    out = {
        "path": str(p.resolve()),
        "exists": True,
        "size_bytes": int(stat.st_size),
        "mtime_utc": datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat(),
        "sha256": None,
    }
    if compute_hash:
        out["sha256"] = _sha256_file(p)
    return out


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _tick_info(ticks: pd.DataFrame) -> Dict:
    if ticks.empty:
        return {"count": 0}
    ts = ticks["timestamp_utc"]
    first, last = ts.min(), ts.max()
    duration = last - first
    diffs = ts.diff().dropna().dt.total_seconds() * 1000  # ms
    spread = ticks["spread"]
    return {
        "count": int(len(ticks)),
        "first_ts": first.isoformat(),
        "last_ts": last.isoformat(),
        "duration_days": round(duration.total_seconds() / 86400, 2),
        "median_gap_ms": float(diffs.median()) if len(diffs) else None,
        "p99_gap_sec": float(diffs.quantile(0.99) / 1000) if len(diffs) else None,
        "max_gap_sec": float(diffs.max() / 1000) if len(diffs) else None,
        "spread_mean": float(spread.mean()),
        "spread_median": float(spread.median()),
        "spread_p99": float(spread.quantile(0.99)),
        "spread_max": float(spread.max()),
        "instruments": sorted({x for x in ticks["instrument"].dropna().unique()}) if "instrument" in ticks.columns else [],
    }


def _segments_info(segments: List[Segment]) -> Dict:
    if not segments:
        return {"count": 0, "usable_ticks": 0, "usable_pct": 0.0, "items": []}
    items = [{
        "idx": i,
        "start_ts": s.start_ts.isoformat(),
        "end_ts": s.end_ts.isoformat(),
        "n_ticks": int(s.n_ticks),
        "duration_h": round(s.duration.total_seconds() / 3600, 3),
    } for i, s in enumerate(segments)]
    usable_ticks = sum(s.n_ticks for s in segments)
    total_dur_h = sum(s.duration.total_seconds() for s in segments) / 3600
    return {
        "count": len(segments),
        "usable_ticks": int(usable_ticks),
        "usable_pct": 0.0,  # backfill caller-szinten (összes tick aránya kell hozzá)
        "min_duration_h": round(min(s.duration.total_seconds() for s in segments) / 3600, 3),
        "max_duration_h": round(max(s.duration.total_seconds() for s in segments) / 3600, 3),
        "total_duration_h": round(total_dur_h, 3),
        "items": items,
    }



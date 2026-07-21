from __future__ import annotations

import csv
import json
import uuid
from dataclasses import asdict, is_dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

import yaml


def make_run_dir(
    base_dir: str | Path,
    strategy: str,
    epic: str,
    mode: str = "backtest",
    suffix: Optional[str] = None,
) -> Path:
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    parts = [ts, mode, strategy, epic]
    if suffix:
        parts.append(suffix)
    name = "_".join(parts)
    path = Path(base_dir) / name
    path.mkdir(parents=True, exist_ok=False)
    return path


class RunLogger:
    """
    Eseményalapú logger.

    Fájlok a run-mappában:
      - config.yaml       : a futás teljes konfigja (snapshot)
      - events.jsonl      : minden event 1 sor JSON-ként
      - decisions.csv     : tick-szintű döntések táblázat
      - trades.csv        : lezárt trade-ek
      - segments.csv      : használt adat-szegmensek
      - metrics.json      : végső metrikák
      - run.log           : human-readable szöveges log
    """

    def __init__(self, run_dir: Path, config: dict, mode: str = "backtest"):
        self.run_dir = Path(run_dir)
        self.run_dir.mkdir(parents=True, exist_ok=True)
        self.mode = mode

        with open(self.run_dir / "config.yaml", "w", encoding="utf-8") as f:
            yaml.safe_dump(config, f, sort_keys=False, allow_unicode=True)

        self._events_f = open(self.run_dir / "events.jsonl", "a", encoding="utf-8", buffering=1)
        self._log_f = open(self.run_dir / "run.log", "a", encoding="utf-8", buffering=1)

        self._decisions_path = self.run_dir / "decisions.csv"
        self._trades_path = self.run_dir / "trades.csv"
        self._segments_path = self.run_dir / "segments.csv"
        self._decisions_writer: Optional[_CsvHandle] = None
        self._trades_writer: Optional[_CsvHandle] = None
        self._segments_writer: Optional[_CsvHandle] = None

    def log_text(self, msg: str) -> None:
        ts = datetime.now(timezone.utc).isoformat()
        self._log_f.write(f"{ts} | {msg}\n")

    def log_event(self, event_type: str, payload: Dict[str, Any]) -> str:
        event_id = uuid.uuid4().hex[:12]
        row = {
            "event_id": event_id,
            "ts_logged": datetime.now(timezone.utc).isoformat(),
            "type": event_type,
            **_to_jsonable(payload),
        }
        self._events_f.write(json.dumps(row, default=str, ensure_ascii=False) + "\n")
        return event_id

    def log_decision(self, decision_row: Dict[str, Any]) -> str:
        event_id = self.log_event("decision", decision_row)
        flat = {"event_id": event_id, **_flatten(decision_row)}
        self._decisions_writer = _append_csv(self._decisions_path, self._decisions_writer, flat)
        return event_id

    def log_open(self, open_row: Dict[str, Any]) -> str:
        return self.log_event("open", open_row)

    def log_close(self, close_row: Dict[str, Any]) -> str:
        event_id = self.log_event("close", close_row)
        self._trades_writer = _append_csv(self._trades_path, self._trades_writer, _flatten(close_row))
        return event_id

    def log_segment(self, segment_row: Dict[str, Any]) -> None:
        self.log_event("segment", segment_row)
        self._segments_writer = _append_csv(self._segments_path, self._segments_writer, _flatten(segment_row))

    def write_metrics(self, metrics: Dict[str, Any]) -> None:
        with open(self.run_dir / "metrics.json", "w", encoding="utf-8") as f:
            json.dump(_to_jsonable(metrics), f, indent=2, default=str, ensure_ascii=False)

    def close(self) -> None:
        for fh in (self._events_f, self._log_f):
            try:
                if fh is not None:
                    fh.close()
            except Exception:
                pass
        for w in (self._decisions_writer, self._trades_writer, self._segments_writer):
            if w is not None:
                w.close()

    def __enter__(self) -> "RunLogger":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()


def _to_jsonable(obj: Any) -> Any:
    if is_dataclass(obj):
        return _to_jsonable(asdict(obj))
    if isinstance(obj, dict):
        return {k: _to_jsonable(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_to_jsonable(v) for v in obj]
    try:
        import pandas as pd
        if isinstance(obj, pd.Timestamp):
            return obj.isoformat()
    except ImportError:
        pass
    return obj


def _flatten(row: Dict[str, Any], prefix: str = "") -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for k, v in row.items():
        key = f"{prefix}{k}"
        if isinstance(v, dict):
            out.update(_flatten(v, prefix=f"{key}."))
        elif is_dataclass(v):
            out.update(_flatten(asdict(v), prefix=f"{key}."))
        else:
            out[key] = v
    return out


class _CsvHandle:
    """
    DictWriter wrapper. Az első sorra rögzül az oszlop-szett — a későbbi
    sorokban hiányzó kulcsok üresen, extra kulcsok eldobva (extrasaction='ignore').
    """

    def __init__(self, path: Path, fieldnames):
        self.path = path
        self.fieldnames = list(fieldnames)
        self.f = open(path, "w", encoding="utf-8", newline="", buffering=1)
        self.writer = csv.DictWriter(self.f, fieldnames=self.fieldnames, extrasaction="ignore")
        self.writer.writeheader()

    def write(self, row: Dict[str, Any]) -> None:
        self.writer.writerow(row)

    def close(self) -> None:
        try:
            self.f.close()
        except Exception:
            pass


def _append_csv(path: Path, handle: Optional[_CsvHandle], row: Dict[str, Any]) -> _CsvHandle:
    if handle is None:
        handle = _CsvHandle(path, fieldnames=row.keys())
    handle.write(row)
    return handle
"""
Signal archívum — append-only jsonl, daily rotáció.

Minden Telegram-üzenetből 1 rekord, akkor is, ha a parser nem találta jelnek
(parser_valid=False). Így a parser-fejlesztéshez utólag is van adat.

Storage:
  data/signals/YYYY-MM-DD.jsonl  — egy nap egy fájl, append-only

Olvasáskor (backtest):
  load_range(start, end, channels)  → pandas DataFrame
"""
from __future__ import annotations

import json
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import pandas as pd

from .schema import ParsedSignal


SCHEMA_VERSION = 1


class SignalArchive:
    def __init__(self, base_dir: str | Path = "./data/signals"):
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(parents=True, exist_ok=True)

    # ── írás ──

    def append(
        self,
        raw_text: str,
        parsed: ParsedSignal,
        chat_id: int,
        chat_name: str,
        message_id: int,
        edited: bool = False,
        parent_message_id: Optional[int] = None,
        ts: Optional[datetime] = None,
    ) -> Dict:
        ts = ts or datetime.now(timezone.utc)
        record = {
            "schema_version": SCHEMA_VERSION,
            "ts_utc": ts.isoformat(),
            "chat_id": int(chat_id),
            "chat_name": chat_name,
            "message_id": int(message_id),
            "edited": bool(edited),
            "parent_message_id": int(parent_message_id) if parent_message_id is not None else None,
            "raw_text": raw_text,
            "parsed": asdict(parsed),
        }
        path = self._path_for(ts)
        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
        return record

    def _path_for(self, ts: datetime) -> Path:
        return self.base_dir / f"{ts.strftime('%Y-%m-%d')}.jsonl"

    # ── olvasás ──

    def load_range(
        self,
        start: pd.Timestamp,
        end: pd.Timestamp,
        channels: Optional[Iterable[int]] = None,
        only_valid: bool = True,
    ) -> pd.DataFrame:
        """
        A [start, end] intervallumba eső signalok flatten-elt DataFrame-je.
        - channels: ha megadva, csak ezekből a chat_id-kból
        - only_valid: csak a parser-által érvényesnek tekintett jelek
        """
        start = pd.to_datetime(start, utc=True)
        end = pd.to_datetime(end, utc=True)
        days = pd.date_range(start.normalize(), end.normalize(), freq="D")
        records: List[Dict] = []
        for d in days:
            path = self.base_dir / f"{d.strftime('%Y-%m-%d')}.jsonl"
            if not path.exists():
                continue
            with open(path, "r", encoding="utf-8") as f:
                for line in f:
                    try:
                        rec = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    records.append(rec)

        if not records:
            return pd.DataFrame()

        df = pd.json_normalize(records, sep=".")
        df["ts_utc"] = pd.to_datetime(df["ts_utc"], utc=True)
        df = df[(df["ts_utc"] >= start) & (df["ts_utc"] <= end)]
        if channels is not None:
            ch = set(int(c) for c in channels)
            df = df[df["chat_id"].isin(ch)]
        if only_valid:
            df = df[df["parsed.valid"] == True]  # noqa: E712
        return df.reset_index(drop=True)

    def stats(self) -> Dict:
        """Gyors áttekintés: hány fájl, hány rekord, hány érvényes jel."""
        files = sorted(self.base_dir.glob("*.jsonl"))
        total, valid = 0, 0
        for p in files:
            with open(p, "r", encoding="utf-8") as f:
                for line in f:
                    total += 1
                    try:
                        rec = json.loads(line)
                        if rec.get("parsed", {}).get("valid"):
                            valid += 1
                    except json.JSONDecodeError:
                        continue
        return {
            "files": len(files),
            "total_records": total,
            "valid_signals": valid,
            "first_day": files[0].stem if files else None,
            "last_day": files[-1].stem if files else None,
        }

from __future__ import annotations

from typing import Any, Dict


class NoopLogger:
    """
    Interface-kompatibilis a RunLogger-rel, de mindenhol no-op.
    Optimize trial-okban használjuk, hogy ne keletkezzen run-mappa
    minden próbáláshoz — gyors, fájlrendszer-mentes futtatás.
    """

    def log_text(self, msg: str) -> None:
        pass

    def log_event(self, event_type: str, payload: Dict[str, Any]) -> str:
        return ""

    def log_decision(self, decision_row: Dict[str, Any]) -> str:
        return ""

    def log_open(self, open_row: Dict[str, Any]) -> str:
        return ""

    def log_close(self, close_row: Dict[str, Any]) -> str:
        return ""

    def log_segment(self, segment_row: Dict[str, Any]) -> None:
        pass

    def write_metrics(self, metrics: Dict[str, Any]) -> None:
        pass

    def close(self) -> None:
        pass

    def __enter__(self) -> "NoopLogger":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        pass
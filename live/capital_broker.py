"""
CapitalBroker — a BrokerClient protocol Capital.com implementációja.

A már meglévő `CapitalClient`-et wrap-eli, és a nyers Capital-payload-okat
NORMALIZÁLT `OpenPosition` / `Confirm` / `ClosingTx` objektumokra alakítja.
Így a runner soha nem lát Capital-specifikus mezőt (`dealId`, `stopLevel` stb.),
csak a normalizált nézetet.

Új broker (pl. eToro) hozzáadása: azonos interface, saját normalizáló-függvények.
"""
from __future__ import annotations
from typing import List, Optional

import pandas as pd

from .broker import BrokerClient, ClosingTx, Confirm, OpenPosition
from .capital_client import CapitalClient, TF_TO_RESOLUTION as _CAP_TF_MAP

TF_TO_RESOLUTION = _CAP_TF_MAP


def _null_uuid(s: Optional[str]) -> bool:
    return isinstance(s, str) and s.startswith("00000000-0000-0000")


class CapitalBroker(BrokerClient):
    def __init__(self, demo: bool = True, account_id: Optional[str] = None):
        self._c = CapitalClient(demo=demo, account_id=account_id)

    # ── Session / account ──
    def ensure_login(self) -> None: self._c.ensure_login()
    def ensure_account(self) -> None: self._c.ensure_account()
    def resolve_epic(self, symbol: str) -> str: return self._c.resolve_epic(symbol)
    def get_balance(self, account_id: Optional[str] = None):
        return self._c.get_balance(account_id)

    # ── Piaci adat ──
    def get_prices(self, epic: str, resolution: str, max_points: int) -> pd.DataFrame:
        return self._c.get_prices(epic, resolution, max_points)

    def stream_ticks(self, epic: str):
        return self._c.stream_ticks(epic)

    # ── Pozíciók (normalizálva) ──
    def get_open_positions(self) -> List[OpenPosition]:
        raw = self._c.get_open_positions()
        out: List[OpenPosition] = []
        for p in raw:
            pos = p.get("position", {}) or {}
            mk = p.get("market", {}) or {}
            deal_id = pos.get("dealId")
            if not deal_id:
                continue
            out.append(OpenPosition(
                deal_id=str(deal_id),
                deal_reference=pos.get("dealReference") or p.get("dealReference"),
                epic=mk.get("epic") or "",
                direction=pos.get("direction") or "",
                entry_price=float(pos.get("level")) if pos.get("level") is not None else 0.0,
                size=float(pos.get("size")) if pos.get("size") is not None else 0.0,
                stop_level=float(pos["stopLevel"]) if pos.get("stopLevel") is not None else None,
                profit_level=float(pos["profitLevel"]) if pos.get("profitLevel") is not None else None,
                created_utc=pos.get("createdDateUTC") or pos.get("createdDate"),
                upl=float(pos["upl"]) if pos.get("upl") is not None else None,
                currency=pos.get("currency") or mk.get("instrumentCurrency"),
                raw=p,
            ))
        return out

    def create_position(self, epic: str, direction: str, size: float,
                        stop_distance: float, profit_distance: Optional[float] = None,
                        trailing_stop: bool = False) -> dict:
        return self._c.create_position(epic, direction, size,
                                        stop_distance, profit_distance, trailing_stop)

    def confirm_position_with_retry(self, deal_reference: str,
                                     attempts: int = 5,
                                     delay_seconds: float = 0.5) -> Confirm:
        raw = self._c.confirm_position_with_retry(deal_reference, attempts, delay_seconds)
        # Capital confirm: dealStatus + affectedDeals[0].dealId + level
        deal_id: Optional[str] = None
        affected = raw.get("affectedDeals")
        if isinstance(affected, list) and affected:
            first = affected[0]
            if isinstance(first, dict) and first.get("dealId"):
                deal_id = str(first["dealId"])
        if deal_id is None:
            deal_id = str(raw.get("dealId")) if raw.get("dealId") else None

        status = (raw.get("dealStatus") or "").upper()
        level = raw.get("level")
        rejected = (
            status == "REJECTED"
            or _null_uuid(deal_id)
            or level is None or level == 0
        )
        return Confirm(
            accepted=not rejected,
            deal_id=deal_id,
            deal_reference=deal_reference,
            level=float(level) if level is not None else None,
            status=status or ("REJECTED" if rejected else "OPEN"),
            reason=raw.get("reason") or "",
            raw=raw,
        )

    def update_position(self, deal_id: str,
                        stop_level: Optional[float] = None,
                        profit_level: Optional[float] = None) -> dict:
        return self._c.update_position(deal_id, stop_level=stop_level, profit_level=profit_level)

    def close_position(self, deal_id: str) -> dict:
        return self._c.close_position(deal_id)

    def get_transaction_for_deal(self, deal_id: str,
                                  last_period_sec: int = 86400) -> Optional[ClosingTx]:
        tx = self._c.get_transaction_for_deal(deal_id, last_period_sec)
        if not tx:
            return None
        # Capital-specifika: PnL a `size` mezőben (legacy), profitAndLoss ritkán
        pnl_raw = tx.get("profitAndLoss")
        if pnl_raw is None:
            pnl_raw = tx.get("size")
        pnl_val: Optional[float] = None
        if pnl_raw is not None:
            try:
                s = str(pnl_raw).replace(",", "")
                digits = "".join(ch for ch in s if ch in "0123456789.-+")
                if digits:
                    pnl_val = float(digits)
            except (TypeError, ValueError):
                pass
        cl = tx.get("closeLevel")
        try:
            cl = float(cl) if cl is not None else None
        except (TypeError, ValueError):
            cl = None
        return ClosingTx(pnl=pnl_val, close_level=cl,
                         currency=tx.get("currency"), raw=tx)

import asyncio
import aiohttp
import logging
from datetime import datetime, timezone

from signal_utils.csv_logger import CsvLogger
from signal_utils.position import PositionState, Position, PositionConfig

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

MAX_OPEN_POSITIONS = 8
SENDER_MAX_OPEN_POSITIONS = 4
POSITION_TIMEOUT_SEC = 15 * 60
CSV_FILE = "positions.csv"
CS_WRITE_DELAY = 10  # sec


class PositionManager:
    def __init__(self, base_url: str, cst: str, token: str):
        self.base_url = base_url
        self.cst = cst
        self.token = token
        self.max_open = MAX_OPEN_POSITIONS
        self.sender_max_open = SENDER_MAX_OPEN_POSITIONS
        self._positions: list[Position] = []
        self._csv = CsvLogger(CSV_FILE)

    # ── Limit ellenőrzés ──────────────────────────────────────────────────────

    def open_count(self, sender=None) -> int:
        return sum(
            1 for p in self._positions
            if p.state in (PositionState.OPEN, PositionState.OPENING)
            and (sender is None or p.config.chat_id == sender)
        )

    def can_open(self, sender) -> bool:
        return (
                self.open_count() < self.max_open
                and self.open_count(sender) < self.sender_max_open
        )

    # ── Pozíció hozzáadása ────────────────────────────────────────────────────

    def add(self, config: PositionConfig) -> Position:
        pos = Position(config, self.base_url, self.cst, self.token, manager=self)
        pos.start()
        self._positions.append(pos)
        self._csv.append_once(pos)  # ← 1. CSV írás: WAITING sor
        logger.info("[MGR] ➕ %s | Zone %.2f–%.2f | TP:%.2f SL:%.2f",
                    pos, config.zone_low, config.zone_high, config.tp, config.sl)
        return pos

    # ── Árbroadcast ───────────────────────────────────────────────────────────

    def broadcast(self, bid: float, ask: float) -> None:
        for pos in self._positions:
            pos.on_price(bid, ask)

    # ── OPU kezelés ───────────────────────────────────────────────────────────

    def handle_opu(self, payload: dict) -> None:
        deal_id = payload.get("dealId")
        for pos in self._positions:
            if pos.deal_id == deal_id:
                pos.apply_opu(payload)
                # apply_opu belsejében indul a _fetch_transactions_and_log task
                break

    # ── CSV frissítés terminális állapotban ───────────────────────────────────

    def csv_update_terminal(self, pos: Position) -> None:
        """
        2. CSV írás: a WAITING-kor létrehozott sort frissíti registered_at alapján.
        Ha valamiért nem találja, appendeli.
        """
        if not pos.is_terminal():
            return

        updated = self._csv.update_row_by(
            "registered_at",
            pos.registered_at.isoformat(),
            pos.to_csv_row(),
        )
        if not updated:
            logger.warning("[MGR] Sor nem található, appendelés: %s", pos)
            self._csv.append_once(pos)

    # ── REST poll ─────────────────────────────────────────────────────────────

    async def poll_and_log(self) -> None:
        # Timeout ellenőrzés — de a _monitor_loop is kezeli, ez csak fallback
        for pos in self._positions:
            if pos.state == PositionState.WAITING:
                elapsed = (datetime.now(timezone.utc) - pos.registered_at).total_seconds()
                if elapsed >= POSITION_TIMEOUT_SEC:
                    pos.state = PositionState.EXPIRED
                    pos.expired_at = datetime.now(timezone.utc)
                    pos.realised_pnl = 0.0
                    logger.info("[MGR] ⏰ Poll során lejárt: %s", pos)
                    self.csv_update_terminal(pos)

        open_positions = [p for p in self._positions if p.state == PositionState.OPEN]
        if open_positions:
            await self._fetch_rest_positions(open_positions)

    async def _fetch_rest_positions(self, targets: list[Position]) -> None:
        headers = {"CST": self.cst, "X-SECURITY-TOKEN": self.token}
        try:
            async with aiohttp.ClientSession() as s:
                async with s.get(
                        f"{self.base_url}/api/v1/positions", headers=headers
                ) as r:
                    data = await r.json()
                    api_map = {
                        p["position"]["dealId"]: p
                        for p in data.get("positions", [])
                        if "position" in p
                    }
                    for pos in targets:
                        if pos.deal_id in api_map:
                            pos.apply_rest_data(api_map[pos.deal_id])
                        elif pos.state == PositionState.OPEN:
                            # REST-ből eltűnt → OPU valószínűleg elmaradt
                            pos.state = PositionState.FILLED
                            pos.close_reason = "CLOSED_EXTERNALLY"
                            pos.closed_at = datetime.now(timezone.utc)
                            logger.warning("[MGR] REST-ből eltűnt: %s", pos)
                            # Transactions-szal próbáljuk begyűjteni az adatokat
                            asyncio.create_task(pos._fetch_transactions_and_log())
        except Exception as e:
            logger.error("[MGR] REST poll hiba: %s", e)

    # ── Indításkori CSV backfill ──────────────────────────────────────────────

    # ── Indításkori CSV backfill ──────────────────────────────────────────────

    async def startup_backfill_csv(self) -> None:
        """
        Indításkor egyszer végigmegy a CSV-n.
        FILLED soroknál ahol hiányzik open_level / close_level / realised_pnl,
        elsőként a /confirms/{dealReference} végpontot hívja,
        fallback esetén a transactions végpontot használja.
        """
        rows = self._csv.read_all()
        if not rows:
            logger.info("[STARTUP] CSV üres, nincs mit javítani")
            return

        incomplete = [
            r for r in rows
            if (r.get("state") or "").upper() == "FILLED"
               and r.get("deal_id", "").strip()
               and (
                       not r.get("open_level", "").strip()
                       or not r.get("close_level", "").strip()
                       or not r.get("realised_pnl", "").strip()
               )
        ]

        if not incomplete:
            logger.info("[STARTUP] Nincs hiányos FILLED sor")
            return

        logger.info("[STARTUP] %d hiányos sor javítása...", len(incomplete))

        for row in incomplete:
            deal_id = row["deal_id"]
            deal_reference = row.get("deal_reference", "").strip()

            updates = {}

            # 1. Elsődleges forrás: /confirms/{dealReference}
            if deal_reference:
                updates = await self._fetch_confirms_for_row(deal_reference)
                if updates:
                    logger.info("[STARTUP] ✅ Confirms forrás: deal_reference=%s", deal_reference)

            # # 2. Fallback: /history/transactions ha a confirms nem hozott eredményt
            # if not updates:
            #     logger.info("[STARTUP] Confirms üres, transactions fallback: deal_id=%s", deal_id)
            #     updates = await self._fetch_transactions_for_row(deal_id)

            if not updates:
                logger.warning("[STARTUP] Nem érkezett adat deal_id=%s", deal_id)
                continue

            # Csak a ténylegesen hiányzó mezőket írjuk felül
            filtered = {
                k: v for k, v in updates.items()
                if not row.get(k, "").strip() and v not in (None, "")
            }

            if not filtered:
                logger.info("[STARTUP] Nincs újabb adat deal_id=%s", deal_id)
                continue

            ok = self._csv.update_row_by("deal_id", deal_id, filtered)
            if ok:
                logger.info("[STARTUP] ✅ Javítva deal_id=%s: %s",
                            deal_id, list(filtered.keys()))
            else:
                logger.warning("[STARTUP] Sor nem található deal_id=%s", deal_id)

    async def _fetch_confirms_for_row(self, deal_reference: str) -> dict:
        """
        /confirms/{dealReference} végpont lekérése.

        Visszatérési mezők:
            - open_level  ← az API nem adja vissza közvetlenül, nincs confirms-ban
            - close_level ← confirms 'level' mezője (a végrehajtási árfolyam)
            - realised_pnl ← confirms 'profit' mezője
            - currency     ← confirms 'currency' mezője (ha elérhető)

        Megjegyzés: a /confirms végpont a lezárási árfolyamot ('level') adja vissza,
        de nyitási árfolyamot nem — azt a transactions végpont tölti ki.
        """
        headers = {"CST": self.cst, "X-SECURITY-TOKEN": self.token}
        url = f"{self.base_url}/api/v1/confirms/{deal_reference}"

        for attempt in range(5):
            try:
                async with aiohttp.ClientSession() as s:
                    async with s.get(url, headers=headers) as r:
                        if r.status == 404:
                            logger.info("[STARTUP] Confirms 404 deal_reference=%s", deal_reference)
                            return {}

                        if r.status != 200:
                            logger.warning(
                                "[STARTUP] Confirms HTTP %d, %d. kísérlet, deal_reference=%s",
                                r.status, attempt + 1, deal_reference
                            )
                            await asyncio.sleep(2 ** attempt)
                            continue

                        data = await r.json()

                        # REJECTED deal esetén nincs értelmes árfolyam
                        if data.get("dealStatus") == "REJECTED" or data.get("level", 0) == 0:
                            logger.info(
                                "[STARTUP] Confirms REJECTED/level=0, skip: deal_reference=%s",
                                deal_reference
                            )
                            return {}

                        open_level = data.get("level")
                        close_level = data.get("profitLevel")
                        pnl = close_level - open_level if data.get("direction") == "BUY" else open_level - close_level

                        return {
                            k: v for k, v in {
                                "close_level": close_level,
                                "realised_pnl": round(pnl, 2),
                                "currency": data.get("currency"),
                                "deal_status": data.get("dealStatus"),
                            }.items() if v is not None
                        }

            except Exception as e:
                logger.warning(
                    "[STARTUP] Confirms hiba (%d. kísérlet) deal_reference=%s: %s",
                    attempt + 1, deal_reference, e
                )
                await asyncio.sleep(2)

        return {}

    @property
    def positions(self) -> list[Position]:
        return list(self._positions)

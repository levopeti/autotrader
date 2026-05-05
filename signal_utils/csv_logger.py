import logging
import csv
import os
import tempfile
import shutil

from datetime import datetime, timezone
from signal_utils.position import Position

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)


CSV_FIELDS = [
    "log_time",          # Mikor lett a CSV sor írva
    "deal_id",
    "deal_reference",
    "epic",
    "direction",
    "size",
    "state",
    "deal_status",
    # Konfig
    "zone_low",
    "zone_high",
    "tp_configured",
    "sl_configured",
    # Élő adatok a REST-ből
    "open_level",        # Tényleges nyitóár
    "current_level",     # Jelenlegi ár (REST poll)
    "limit_level",       # TP szint az API-ban
    "stop_level",        # SL szint az API-ban
    "profit_loss",       # Aktuális P&L
    "realised_pnl",     # Végleges P&L dollárban (zárásnál confirm-ből)
    "currency",
    "created_date",      # Mikor nyílt a pozíció
    "close_level",       # Záróár (ha zárva)
    "close_reason",      # LIMIT / STOP / MANUAL / EXPIRED / stb.
    # Időbélyegek
    "registered_at",     # Mikor adták hozzá a managerhez
    "opened_at",         # Mikor küldte el az API-nak
    "closed_at",         # Mikor zárult
    "expired_at",        # Mikor járt le (timeout)
    "last_poll_at",      # Utolsó REST frissítés
    "raw_text",
    "send_date",
    "edited",
    "chat_id",
    "chat_name",
    "tp_idx",
    # Hiba
    "error_msg",
]

class CsvLogger:
    """
    - append_once()    : új sor hozzáfűzése (signal beérkezéskor)
    - update_row_by()  : tetszőleges mező alapján keresi és frissíti a sort
    - read_all()       : teljes CSV beolvasása
    """

    def __init__(self, path: str):
        self.path     = path
        self._written: set[str] = set()
        self._ensure_header()

    def _ensure_header(self) -> None:
        if not os.path.exists(self.path):
            with open(self.path, "w", newline="", encoding="utf-8") as f:
                csv.DictWriter(f, fieldnames=CSV_FIELDS).writeheader()
            logger.info("[CSV] Fájl létrehozva: %s", self.path)

    # ── Új sor hozzáfűzése ────────────────────────────────────────────────────

    def append_once(self, pos: "Position") -> None:
        """
        Signal beérkezésekor hívódik — ekkor még nincs deal_ref,
        ezért Python object id-t használunk egyedi kulcsként.
        """
        key = str(id(pos))
        if key in self._written:
            return
        with open(self.path, "a", newline="", encoding="utf-8") as f:
            csv.DictWriter(f, fieldnames=CSV_FIELDS).writerow(pos.to_csv_row())
        self._written.add(key)
        logger.info("[CSV] ✍️  Létrehozva (%s): %s", pos.state.value, pos)

    # ── Meglévő sor frissítése ────────────────────────────────────────────────

    def update_row_by(self, key_field: str, key_value: str, updates: dict) -> bool:
        """
        Beolvassa az egész CSV-t, megkeresi a key_field == key_value sort,
        frissíti az összes mezőt az updates dict-ből (üres értéket nem ír felül),
        majd atomikusan visszaírja.
        Visszatér True-val ha talált és frissített sort.

        Használat:
            update_row_by("registered_at", pos.registered_at.isoformat(), pos.to_csv_row())
            update_row_by("deal_id", deal_id, {"open_level": 2310.5, ...})
        """
        if not os.path.exists(self.path):
            return False

        rows  = []
        found = False

        with open(self.path, "r", newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                if not found and row.get(key_field) == key_value:
                    for k, v in updates.items():
                        if k in CSV_FIELDS and v not in (None, ""):
                            row[k] = v
                    row["log_time"] = datetime.now(timezone.utc).isoformat()
                    found = True
                rows.append(row)

        if not found:
            return False

        self._atomic_write(rows)
        logger.info("[CSV] 🔄 Frissítve (%s=%s)", key_field, key_value)
        return True

    # ── Atomikus visszaírás ───────────────────────────────────────────────────

    def _atomic_write(self, rows: list[dict]) -> None:
        """Temp fájlba ír, majd rename — félbeszakadáskor sem sérül a CSV."""
        dir_            = os.path.dirname(os.path.abspath(self.path))
        tmp_fd, tmp_path = tempfile.mkstemp(dir=dir_, suffix=".tmp")
        try:
            with os.fdopen(tmp_fd, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
                writer.writeheader()
                for row in rows:
                    writer.writerow({k: row.get(k, "") for k in CSV_FIELDS})
            shutil.move(tmp_path, self.path)
        except Exception:
            os.unlink(tmp_path)
            raise

    # ── Teljes CSV beolvasása ─────────────────────────────────────────────────

    def read_all(self) -> list[dict]:
        if not os.path.exists(self.path):
            return []
        with open(self.path, "r", newline="", encoding="utf-8") as f:
            return list(csv.DictReader(f))

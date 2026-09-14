"""
Parser dispatch — chat_id alapján csatorna-specifikus parser, fallback a generic-re.

A Telethon `event.chat.id` egy supergroup-nál a "-100" prefix nélküli pozitív
integer (pl. -1002001216034 → 2001216034). A SignalArchive ezt menti.
A dispatch ezekre matchelt, és a `negate_supergroup_id`-vel mindkét formát kezeli.
"""
from __future__ import annotations

from typing import Callable, Dict, Optional

from ..schema import ParsedSignal
from .channels import ann as ann_parser
from .channels import traderz as traderz_parser
from .channels import vip_signals as vip_signals_parser
from .regex_parser import parse as generic_parse


# chat_id → channel-specifikus parser. A chat_id a Telethon "pozitív" formája.
# Ha a config-ban "-1002001216034" szerepel, a Telethon event.chat.id 2001216034.
CHANNEL_PARSERS: Dict[int, Callable[[str], ParsedSignal]] = {
    2001216034: vip_signals_parser.parse,   # VIP SIGNALS ROOM
    3496306840: traderz_parser.parse,       # Traderz Gold VIP
    1086101437: ann_parser.parse,           # ANN Zerofloat
}


def _normalize_chat_id(chat_id: Optional[int]) -> Optional[int]:
    """A Telegram -100xxxxxxxxxxxxx supergroup id-t a Telethon pozitív formára."""
    if chat_id is None:
        return None
    cid = int(chat_id)
    if cid < 0:
        # -100xxxxxxxxxxxxx → xxxxxxxxxxxxx
        s = str(abs(cid))
        if s.startswith("100"):
            try:
                return int(s[3:])
            except ValueError:
                return cid
        return abs(cid)
    return cid


def parse(text: str, chat_id: Optional[int] = None) -> ParsedSignal:
    """
    Csatorna-specifikus parser fut először (ha van), és a generic parser
    egyszerű fallback (ha a specifikus invalid-ot adott VAGY chat_id nincs).
    """
    cid = _normalize_chat_id(chat_id)
    if cid is not None and cid in CHANNEL_PARSERS:
        specific = CHANNEL_PARSERS[cid](text)
        if specific.valid:
            return specific
        # Specifikus failed — próbáljuk a generic-et fallback-ként.
        # Ha a generic is invalid, a specifikus reason-t adjuk vissza (specifikus
        # tudja jobban, miért nem signal a saját csatornáján).
        fallback = generic_parse(text)
        if fallback.valid:
            fallback.parser = f"{specific.parser}+fallback_regex"
            return fallback
        return specific
    return generic_parse(text)

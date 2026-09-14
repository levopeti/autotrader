"""
ANN Zerofloat — chat_id: 1086101437

Tipikus minta:
  XAUUSD SELL NOW
  @ 4535  - 4540
  SL🛑4545
  TP✅4525
  #FollowRapatAn ❗️❗️❗️

Az "@" jel zóna-jelölő, emoji a SL/TP után közvetlenül a számhoz tapad.
Általában 1 TP. Egyszerű formátum.
"""
from __future__ import annotations

import re

from ..regex_parser import _EMOJI_RE, _check_direction_consistency, _in_range
from ...schema import ParsedSignal


_DIRECTION_RE = re.compile(r"\b(XAU(?:USD)?|GOLD)\s+(BUY|SELL|LONG|SHORT)\b", re.IGNORECASE)
_ENTRY_ZONE_RE = re.compile(
    r"@\s*(\d+(?:\.\d+)?)\s*[-/]\s*(\d+(?:\.\d+)?)",
    re.IGNORECASE,
)
_ENTRY_SINGLE_RE = re.compile(r"@\s*(\d+(?:\.\d+)?)", re.IGNORECASE)
# Az SL/TP után közvetlenül szám (emoji eltávolítása UTÁN)
_SL_RE = re.compile(r"SL\s*[:\-]?\s*(\d+(?:\.\d+)?)", re.IGNORECASE)
_TP_RE = re.compile(r"TP\d*\s*[:\-]?\s*(\d+(?:\.\d+)?)", re.IGNORECASE)


def parse(text: str) -> ParsedSignal:
    parser_name = "ann"
    if not text or not isinstance(text, str):
        return ParsedSignal.invalid("empty_input", parser=parser_name)

    clean = _EMOJI_RE.sub("", text).strip()

    m = _DIRECTION_RE.search(clean)
    if not m:
        return ParsedSignal.invalid("not_a_signal", parser=parser_name)
    dword = m.group(2).upper()
    direction = "BUY" if dword in ("BUY", "LONG") else "SELL"

    # Entry zone "@ X - Y" vagy "@ X"
    em = _ENTRY_ZONE_RE.search(clean)
    if em:
        e1, e2 = float(em.group(1)), float(em.group(2))
        entry_low, entry_high = min(e1, e2), max(e1, e2)
    else:
        ms = _ENTRY_SINGLE_RE.search(clean)
        if not ms:
            return ParsedSignal.invalid("missing_entry", parser=parser_name)
        ep = float(ms.group(1))
        entry_low, entry_high = ep - 1.0, ep + 1.0

    sl_match = _SL_RE.search(clean)
    if not sl_match:
        return ParsedSignal.invalid("missing_or_invalid_sl", parser=parser_name)
    sl = float(sl_match.group(1))
    if not _in_range(str(sl)):
        return ParsedSignal.invalid("missing_or_invalid_sl", parser=parser_name)

    tp_list = [float(x) for x in _TP_RE.findall(clean) if _in_range(x)]
    if not tp_list:
        return ParsedSignal.invalid("missing_tp", parser=parser_name)

    tp_list = sorted(set(tp_list))
    ok, why = _check_direction_consistency(direction, entry_low, entry_high, sl, tp_list)
    if not ok:
        return ParsedSignal.invalid(why, parser=parser_name)

    return ParsedSignal(
        valid=True, reason="ok", direction=direction,
        entry_low=entry_low, entry_high=entry_high,
        tp_list=tp_list, sl=sl, parser=parser_name,
    )

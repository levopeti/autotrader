"""
VIP SIGNALS ROOM (LIFETIME) — chat_id: 2001216034

Tipikus minta:
  #XAUUSD BUY 4515-4512
  TP 4518
  TP 4521
  ...
  SL 4503

Egyszerű space-elválasztott, hashtag-stílusú. Több TP, egy SL.
"""
from __future__ import annotations

import re

from ..regex_parser import _check_direction_consistency, _in_range
from ...schema import ParsedSignal


_HEADER_RE = re.compile(
    r"#?\s*(XAU(?:USD)?|GOLD)\s+(BUY|SELL|LONG|SHORT)\s+(\d+(?:\.\d+)?)\s*[-/]\s*(\d+(?:\.\d+)?)",
    re.IGNORECASE,
)
_HEADER_SINGLE_RE = re.compile(
    r"#?\s*(XAU(?:USD)?|GOLD)\s+(BUY|SELL|LONG|SHORT)\s+(\d+(?:\.\d+)?)",
    re.IGNORECASE,
)
_TP_LINE = re.compile(r"\bTP\d*\s*:?\s*(\d+(?:\.\d+)?)", re.IGNORECASE)
_SL_LINE = re.compile(r"\bSL\s*:?\s*(\d+(?:\.\d+)?)", re.IGNORECASE)


def parse(text: str) -> ParsedSignal:
    parser_name = "vip_signals"
    if not text or not isinstance(text, str):
        return ParsedSignal.invalid("empty_input", parser=parser_name)

    # Header (direction + entry zone)
    m = _HEADER_RE.search(text)
    if m:
        direction_word = m.group(2).upper()
        e1, e2 = float(m.group(3)), float(m.group(4))
        entry_low, entry_high = min(e1, e2), max(e1, e2)
    else:
        m2 = _HEADER_SINGLE_RE.search(text)
        if not m2:
            return ParsedSignal.invalid("not_a_signal", parser=parser_name)
        direction_word = m2.group(2).upper()
        ep = float(m2.group(3))
        entry_low, entry_high = ep - 1.0, ep + 1.0

    direction = "BUY" if direction_word in ("BUY", "LONG") else "SELL"

    # TP-k és SL
    tp_list = [float(x) for x in _TP_LINE.findall(text) if _in_range(x)]
    sl_match = _SL_LINE.search(text)
    if not sl_match:
        return ParsedSignal.invalid("missing_or_invalid_sl", parser=parser_name)
    sl = float(sl_match.group(1))
    if not _in_range(str(sl)):
        return ParsedSignal.invalid("missing_or_invalid_sl", parser=parser_name)
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

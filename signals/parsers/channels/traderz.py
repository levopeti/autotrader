"""
Traderz Gold VIP — chat_id: 3496306840

Tipikus minta:
  📈 XAUUSD SELL NOW
  Entry: 4530-35
  🛑 SL: 4545
  🎯 TP1: 4525
  🎯 TP2: 4520
  ...
  🛡 Always use stop-loss & proper money management

Emoji-gazdag, "Entry: X-YY" rövidített (last 2 digits) forma is gyakori.
"""
from __future__ import annotations

import re

from ..regex_parser import _EMOJI_RE, _check_direction_consistency, _expand_shorthand, _in_range
from ...schema import ParsedSignal


_DIRECTION_RE = re.compile(r"\b(XAU(?:USD)?|GOLD)\s+(BUY|SELL|LONG|SHORT)\b", re.IGNORECASE)
_ENTRY_LINE_RE = re.compile(
    r"Entry\s*:?\s*(\d+(?:\.\d+)?)\s*[-/]\s*(\d+(?:\.\d+)?)",
    re.IGNORECASE,
)
_ENTRY_SINGLE_RE = re.compile(r"Entry\s*:?\s*(\d+(?:\.\d+)?)", re.IGNORECASE)
_SL_RE = re.compile(r"SL\s*:?\s*(\d+(?:\.\d+)?)", re.IGNORECASE)
_TP_RE = re.compile(r"TP\d*\s*:?\s*(\d+(?:\.\d+)?)", re.IGNORECASE)


def parse(text: str) -> ParsedSignal:
    parser_name = "traderz"
    if not text or not isinstance(text, str):
        return ParsedSignal.invalid("empty_input", parser=parser_name)

    clean = _EMOJI_RE.sub("", text).strip()

    m = _DIRECTION_RE.search(clean)
    if not m:
        return ParsedSignal.invalid("not_a_signal", parser=parser_name)
    dword = m.group(2).upper()
    direction = "BUY" if dword in ("BUY", "LONG") else "SELL"

    # Entry zone (rövidített kibontással)
    em = _ENTRY_LINE_RE.search(clean)
    if em:
        raw1, raw2 = em.group(1), em.group(2)
        # Ha a 2. rövidebb (pl. 4530-35), kibontjuk
        if len(raw1) > len(raw2) and len(raw2) >= 2 and _in_range(raw1):
            expanded = _expand_shorthand(raw2, raw1)
            if expanded is not None:
                e1, e2 = float(raw1), float(expanded)
            else:
                e1, e2 = float(raw1), float(raw2)
        elif len(raw2) > len(raw1) and len(raw1) >= 2 and _in_range(raw2):
            expanded = _expand_shorthand(raw1, raw2)
            if expanded is not None:
                e1, e2 = float(expanded), float(raw2)
            else:
                e1, e2 = float(raw1), float(raw2)
        else:
            e1, e2 = float(raw1), float(raw2)
        entry_low, entry_high = min(e1, e2), max(e1, e2)
    else:
        m2 = _ENTRY_SINGLE_RE.search(clean)
        if not m2:
            return ParsedSignal.invalid("missing_entry", parser=parser_name)
        ep = float(m2.group(1))
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

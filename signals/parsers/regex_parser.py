"""
Robosztus regex-alapú signal parser.

Az új parser:
- Nem feltételez fix számjegy-hosszt (a régi 4-hosszú lock a XAU 10000+ felett elbukna).
- A rövidített számokat (pl. "4695-05") a hosszú számhoz **közelebbi** kibontásra
  hozza (régi heurisztika hibázott pl. SELL zóna felső szélén).
- Nincs assert; minden hibából érvényes "invalid" választ ad reason-nel.
- Edit-üzenet és teszt-corpus alapján regressziós tesztelhető (signals/test_corpus.py).
"""
from __future__ import annotations

import re
from typing import List, Optional, Tuple

from ..schema import ParsedSignal


# Reasonable XAUUSD price-range (laza, hogy a többi instrumentum is beférhessen)
PRICE_MIN = 100.0
PRICE_MAX = 100_000.0

# Emoji-eltávolító (egyszerű unicode range; az "emoji" pip csomag nélkül is megy)
_EMOJI_RE = re.compile(
    "[\U0001F300-\U0001FAFF\U00002600-\U000027BF\U0001F600-\U0001F64F"
    "\U0001F680-\U0001F6FF\U0001F900-\U0001F9FF\U0001FA70-\U0001FAFF"
    "\U00002B00-\U00002BFF✀-➿‍️]+",
    flags=re.UNICODE,
)

_ASSET_RE = re.compile(r"(?<![A-Za-z])(XAU(?:USD)?|GOLD)(?![A-Za-z])", re.IGNORECASE)
_BUY_RE = re.compile(r"(?<![A-Za-z])(BUY|LONG)(?![A-Za-z])", re.IGNORECASE)
_SELL_RE = re.compile(r"(?<![A-Za-z])(SELL|SHORT)(?![A-Za-z])", re.IGNORECASE)
_SL_RE = re.compile(r"(?<![A-Za-z])(SL|STOP\s*LOSS|STOP)(?![A-Za-z])", re.IGNORECASE)
_TP_RE = re.compile(r"(?<![A-Za-z])(TP\d*|TAKE\s*PROFIT|TARGET|PROFIT)(?![A-Za-z])", re.IGNORECASE)
_ENTRY_RE = re.compile(r"(?<![A-Za-z])(ENTRY|ENTRIES|ENTER|ENTRY\s*POINT)(?![A-Za-z])", re.IGNORECASE)
_NUMBER_RE = re.compile(r"\d+(?:\.\d+)?")


class RegexSignalParser:
    name = "regex"

    def parse(self, text: str) -> ParsedSignal:
        if not text or not isinstance(text, str):
            return ParsedSignal.invalid("empty_input", parser=self.name)

        clean = _EMOJI_RE.sub("", text).strip()

        if not _ASSET_RE.search(clean):
            return ParsedSignal.invalid("not_a_signal", parser=self.name)

        direction = self._detect_direction(clean)
        if direction is None:
            return ParsedSignal.invalid("no_direction", parser=self.name)

        lines = [ln.strip() for ln in clean.splitlines() if ln.strip()]
        sl: Optional[float] = None
        tp_list: List[float] = []
        entry_numbers: List[Tuple[str, float]] = []  # (raw_str, value)

        for ln in lines:
            tagged = self._classify_line(ln)
            nums_raw = _NUMBER_RE.findall(ln)
            nums_in_range = [n for n in nums_raw if _in_range(n)]

            if tagged == "sl":
                if nums_in_range:
                    sl = float(nums_in_range[0])
            elif tagged == "tp":
                if nums_in_range:
                    tp_list.append(float(nums_in_range[0]))
            elif tagged == "entry":
                for n in nums_raw:
                    entry_numbers.append((n, _safe_float(n)))
            else:
                # A "direction" sora vagy szabad szám-sor — itt is lehet entry zóna,
                # pl. "Gold sell now 4626-4629" vagy "XAUUSD BUY NOW @4549-4544"
                if _BUY_RE.search(ln) or _SELL_RE.search(ln) or ln.startswith("@"):
                    for n in nums_raw:
                        entry_numbers.append((n, _safe_float(n)))

        # ── Számok kibontása rövidített formából ──
        entries = self._resolve_entries(entry_numbers)

        # ── Validáció ──
        if sl is None:
            return ParsedSignal.invalid("missing_or_invalid_sl", parser=self.name)
        if not tp_list:
            return ParsedSignal.invalid("missing_tp", parser=self.name)
        if not entries:
            return ParsedSignal.invalid("missing_entry", parser=self.name)

        if len(entries) == 1:
            entry_low = entries[0] - 1.0
            entry_high = entries[0] + 1.0
        else:
            entry_low = min(entries)
            entry_high = max(entries)

        tp_list = sorted(set(tp_list))

        ok, why = _check_direction_consistency(direction, entry_low, entry_high, sl, tp_list)
        if not ok:
            return ParsedSignal.invalid(why, parser=self.name)

        return ParsedSignal(
            valid=True,
            reason="ok",
            direction=direction,
            entry_low=entry_low,
            entry_high=entry_high,
            tp_list=tp_list,
            sl=sl,
            parser=self.name,
        )

    # ── helpers ──────────────────────────────────────────────────────────────

    @staticmethod
    def _detect_direction(text: str) -> Optional[str]:
        has_buy = bool(_BUY_RE.search(text))
        has_sell = bool(_SELL_RE.search(text))
        if has_buy and not has_sell:
            return "BUY"
        if has_sell and not has_buy:
            return "SELL"
        return None

    @staticmethod
    def _classify_line(line: str) -> str:
        # A sorrend kritikus: ha a sorban "Stop Loss" van, az SL — még akkor is, ha
        # esetleg "TP" is szerepel. Ezért előbb SL.
        if _SL_RE.search(line):
            return "sl"
        if _TP_RE.search(line):
            return "tp"
        if _ENTRY_RE.search(line):
            return "entry"
        return "other"

    @staticmethod
    def _resolve_entries(numbers: List[Tuple[str, float]]) -> List[float]:
        """
        Két szám esetén kezeli a rövidített formát ("4660-70" → 4660, 4670).
        A "közelebbi" opciót választja a hosszú számhoz mérve.
        Egyetlen szám esetén a (raw, val) listából kiveszi az első in-range értéket.
        """
        if not numbers:
            return []

        # Distinct preserve order
        seen = set()
        uniq = []
        for raw, val in numbers:
            if raw not in seen:
                seen.add(raw)
                uniq.append((raw, val))

        in_range_pairs = [(r, v) for r, v in uniq if _in_range(r)]
        if len(in_range_pairs) >= 2:
            vs = [v for _, v in in_range_pairs[:2]]
            return [min(vs), max(vs)]

        if len(in_range_pairs) == 1 and len(uniq) >= 2:
            long_raw, long_val = in_range_pairs[0]
            short_raw, _ = next(((r, v) for r, v in uniq if r != long_raw), (None, None))
            # Rövidített ár-kibontás: a short legalább 2-jegyű (1-jegyű = sorszám pl. "Entry 1")
            if short_raw is not None and 2 <= len(short_raw) < len(long_raw):
                expanded = _expand_shorthand(short_raw, long_raw)
                if expanded is not None:
                    return [min(long_val, expanded), max(long_val, expanded)]
            return [long_val]

        if len(in_range_pairs) == 1:
            return [in_range_pairs[0][1]]

        return []


# ── Module-level convenience function ───────────────────────────────────────

_DEFAULT_PARSER = RegexSignalParser()


def parse(text: str) -> ParsedSignal:
    return _DEFAULT_PARSER.parse(text)


# ── helpers ──────────────────────────────────────────────────────────────────

def _safe_float(s: str) -> float:
    try:
        return float(s)
    except (TypeError, ValueError):
        return float("nan")


def _in_range(raw: str) -> bool:
    try:
        v = float(raw)
    except (TypeError, ValueError):
        return False
    return PRICE_MIN <= v <= PRICE_MAX


def _expand_shorthand(short: str, long: str) -> Optional[float]:
    """
    "4695" + "05" → (4605, 4705). A long-hoz közelebbit visszaadjuk.
    """
    if len(short) >= len(long):
        return None
    try:
        long_val = int(long)
        prefix = long[:-len(short)]
        a = int(prefix + short)
        b = int(str(int(prefix) + 1) + short)
        # közelebbi a hosszú számhoz
        return float(a) if abs(a - long_val) <= abs(b - long_val) else float(b)
    except (TypeError, ValueError):
        return None


def _check_direction_consistency(
    direction: str,
    entry_low: float,
    entry_high: float,
    sl: float,
    tp_list: List[float],
) -> Tuple[bool, str]:
    if direction == "BUY":
        if sl >= entry_low:
            return False, "direction_levels_inconsistent"
        if any(tp <= entry_high for tp in tp_list):
            return False, "direction_levels_inconsistent"
        if any(tp <= sl for tp in tp_list):
            return False, "direction_levels_inconsistent"
    else:  # SELL
        if sl <= entry_high:
            return False, "direction_levels_inconsistent"
        if any(tp >= entry_low for tp in tp_list):
            return False, "direction_levels_inconsistent"
        if any(tp >= sl for tp in tp_list):
            return False, "direction_levels_inconsistent"
    return True, "ok"

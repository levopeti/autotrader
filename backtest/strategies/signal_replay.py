"""
Signal-replay stratégia: a SignalArchive-ból betöltött Telegram-jeleket
visszajátssza tick adaton. Konzisztens a live Position state machine-nel
(WAITING zónába esésig → zóna-belépés → SL/TP).
"""
from __future__ import annotations

import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

# A repository-gyökeret kell importálnunk a `signals` package-hoz
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from signals.archive import SignalArchive  # noqa: E402

from ..indicators.candle_indicators import (adx as _adx_fn, atr as _atr_fn,   # noqa: E402
                                              ema as _ema_fn)
from .base import Decision, Strategy, StrategyContext  # noqa: E402


def _tf_td64(tf: str) -> np.timedelta64:
    return np.timedelta64(int(pd.Timedelta(tf).total_seconds()), "s")


@dataclass
class _SignalRow:
    id: int
    ts: pd.Timestamp
    chat_id: int
    chat_name: str
    direction: str
    entry_low: float
    entry_high: float
    tp_list: List[float]
    sl: float
    state: str = "WAITING"   # WAITING | USED | EXPIRED


class SignalReplay(Strategy):
    name = "signal_replay"

    def __init__(self, params: dict):
        super().__init__(params)
        p = params
        self.signals_dir: str = p["signals_dir"]
        self.channels: Optional[List[int]] = p.get("channels")
        self.signal_timeout_minutes: float = p.get("signal_timeout_minutes", 15.0)
        self.tp_idx: int = int(p.get("tp_idx", 0))    # 0 = legközelebbi, -1 = legtávolabbi (csak single TP módban)
        # tp_strategy:
        #   "first"  → csak az első TP (tp_idx-szel finomhangolható)
        #   "last"   → legtávolabbi TP (tp_idx ignored, -1-et használ)
        #   "multi"  → minden parsed TP külön réteg-pozíció közös SL-lel
        self.tp_strategy: str = p.get("tp_strategy", "first")
        if self.tp_strategy not in ("first", "last", "multi"):
            raise ValueError(f"tp_strategy: {self.tp_strategy} (first|last|multi)")
        self.entry_zone_expand: float = float(p.get("entry_zone_expand", 0.0))
        self.candle_tf: str = p.get("candle_tf", "1min")

        # ── Új: signal SL/TP távolság-multiplikátorok (1.0 = változatlan)
        self.sl_mult: float = float(p.get("sl_mult", 1.0))
        self.tp_mult: float = float(p.get("tp_mult", 1.0))

        # ── Új: multi módban TP-részhalmaz választás (mindhárom None = összes TP)
        # tp_multi_first_n:  csak a legközelebbi N TP-t használjuk rétegként
        # tp_multi_last_n:   csak a legtávolabbi N TP-t (a distance-sorted lista végéről)
        # tp_multi_indices:  TETSZŐLEGES indexek listája a distance-sorted TP-listából
        #                    (pl. [1, 2] → 2. és 3. legközelebbi); ha jelen van,
        #                    a first_n/last_n-t felülírja
        self.tp_multi_first_n: Optional[int] = p.get("tp_multi_first_n")
        self.tp_multi_last_n: Optional[int] = p.get("tp_multi_last_n")
        self.tp_multi_indices: Optional[List[int]] = p.get("tp_multi_indices")

        # ── Új: TP-ladder. Ha mindkét index meg van adva, a Decision-be
        # belekerül a ladder_trigger_distance / ladder_dest_distance.
        # A trade engine ezt csak akkor használja, ha engine.trailing_mode == "tp_ladder".
        # Mindkettő 0-based index a `tps_by_dist` listába (distance-ascending).
        # Pl. ladder_trigger_idx=2, ladder_dest_idx=0 → amint a piac TP3-at eléri,
        # az SL TP1-re ugrik. A target TP-vel KÜLÖN állítható (rendszerint a
        # tp_idx-szel; multi módban tp_multi_indices-vel).
        self.ladder_trigger_idx: Optional[int] = p.get("ladder_trigger_idx")
        self.ladder_dest_idx: Optional[int] = p.get("ladder_dest_idx")

        # ── Új: ATR-alapú SL/TP override. Ha True, a jel SL/TP-jét IGNORÁLJUK,
        #      csak az entry zónát használjuk; SL/TP = ATR × mult.
        self.use_atr_levels: bool = bool(p.get("use_atr_levels", False))
        # compute_atr: ha True (vagy use_atr_levels), számítjuk az ATR-t és
        # bekerül az indicators["atr"] mezőbe. A runner az atr_at_open-t innen
        # veszi, amire az engine trailing logikának szüksége van — így a
        # signal-szintű SL/TP érintetlen marad, de trailingelhet.
        self.compute_atr: bool = bool(p.get("compute_atr", False))
        self.atr_tf: str = p.get("atr_tf", "5min")
        self.atr_period: int = int(p.get("atr_period", 14))
        self.sl_atr_mult: float = float(p.get("sl_atr_mult", 1.5))
        self.tp_atr_mult: float = float(p.get("tp_atr_mult", 2.0))

        # ── Új: trend-filter. Ha aktív, csak azok a signalok mehetnek tovább,
        # amelyek iránya egyezik a megadott TF EMA-jának (fast vs slow) trendjével.
        # BUY: ema_fast > ema_slow kell; SELL: ema_fast < ema_slow.
        # `trend_filter_mode = "neutral_drop"`: ha bizonytalan (kettő közel),
        # eldobjuk; "neutral_allow": átengedjük.
        self.trend_filter_enabled: bool = bool(p.get("trend_filter_enabled", False))
        self.trend_filter_tf: str = p.get("trend_filter_tf", "1h")
        self.trend_filter_ema_fast: int = int(p.get("trend_filter_ema_fast", 9))
        self.trend_filter_ema_slow: int = int(p.get("trend_filter_ema_slow", 21))
        self.trend_filter_mode: str = p.get("trend_filter_mode", "neutral_drop")

        # ── Új: "csak-ranging" filter (counter-trend csatornáknak, pl. ANN)
        # Ha aktív, csak akkor engedélyezünk trade-et, ha az ADX a megadott
        # TF-en a küszöb ALATT van (= nincs erős trend).
        self.require_ranging: bool = bool(p.get("require_ranging", False))
        self.ranging_adx_tf: str = p.get("ranging_adx_tf", "4h")
        self.ranging_adx_period: int = int(p.get("ranging_adx_period", 14))
        self.ranging_adx_threshold: float = float(p.get("ranging_adx_threshold", 30.0))

        # ── Új: cross-channel consensus szűrés. Ha aktív, csak azok a signalok
        #      maradnak, amelyekhez egy másik csatornán is érkezett azonos
        #      irányú signal a megadott idő- és ár-toleranciával.
        # consensus_channels: melyik csatornák VEHETNEK RÉSZT a consensus-ben.
        #   None → minden csatorna. (Független a trade-szűrő `channels` mezőtől.)
        self.consensus_required: bool = bool(p.get("consensus_required", False))
        self.consensus_window_min: float = float(p.get("consensus_window_min", 60.0))
        self.consensus_price_tol: float = float(p.get("consensus_price_tol", 5.0))
        self.consensus_min_channels: int = int(p.get("consensus_min_channels", 2))
        self.consensus_channels: Optional[List[int]] = p.get("consensus_channels")

        # ── Új: news avoidance filter.
        # news_blocked_hours: list of [start_h, end_h] UTC intervallumok
        #   pl. [[11, 15]] → 11:00-14:59 UTC minden nap blokk (US CPI/PPI/Retail window)
        # news_blocked_weekdays: opcionális szűkítés hétköznapokra (0=hétfő..6=vasárnap)
        # news_events_file: opcionális JSON abszolút timestamp list, pl:
        #   [{"ts": "2026-07-15T12:30:00Z", "desc": "US CPI"}, ...]
        # news_event_buffer_min: az abszolút events körüli ±buffer perc
        self.news_blocked_hours: List[List[int]] = p.get("news_blocked_hours") or []
        self.news_blocked_weekdays: Optional[List[int]] = p.get("news_blocked_weekdays")
        self.news_events_file: Optional[str] = p.get("news_events_file")
        self.news_event_buffer_min: float = float(p.get("news_event_buffer_min", 30.0))
        # Előszámolt abszolút event intervallumok (UTC): list of (start_ts, end_ts)
        self._news_event_windows: List[tuple] = self._load_news_events(self.news_events_file)

        self._archive = SignalArchive(self.signals_dir)
        self._signals: List[_SignalRow] = []
        self._next_to_activate: int = 0   # pointer az _signals-ra
        self._atr_arr: Optional[np.ndarray] = None
        self._atr_close_ts: Optional[np.ndarray] = None
        # Trend-filter állapota
        self._trend_fast_arr: Optional[np.ndarray] = None
        self._trend_slow_arr: Optional[np.ndarray] = None
        self._trend_close_ts: Optional[np.ndarray] = None
        # ADX-alapú "csak ranging" filter állapot
        self._ranging_adx_arr: Optional[np.ndarray] = None
        self._ranging_adx_close_ts: Optional[np.ndarray] = None

    def required_timeframes(self) -> List[str]:
        tfs = [self.candle_tf]
        if (self.use_atr_levels or self.compute_atr) and self.atr_tf not in tfs:
            tfs.append(self.atr_tf)
        if self.trend_filter_enabled and self.trend_filter_tf not in tfs:
            tfs.append(self.trend_filter_tf)
        if self.require_ranging and self.ranging_adx_tf not in tfs:
            tfs.append(self.ranging_adx_tf)
        return tfs

    def on_segment_start(self, ctx: StrategyContext) -> None:
        seg_start = ctx.segment_start
        seg_end = ctx.segment_end
        if seg_start.tz is None:
            seg_start = seg_start.tz_localize("UTC")
        if seg_end.tz is None:
            seg_end = seg_end.tz_localize("UTC")
        # Consensus szűréshez ÖSSZES csatornát be kell tölteni; a channel-szűrés
        # később jön. (Egyébként csak a megadott channels-eket.)
        load_channels = None if self.consensus_required else self.channels
        df = self._archive.load_range(
            start=seg_start - pd.Timedelta(minutes=self.signal_timeout_minutes),
            end=seg_end,
            channels=load_channels,
            only_valid=True,
        )
        rows: List[_SignalRow] = []
        for i, r in df.iterrows():
            tp_list = r.get("parsed.tp_list") or []
            if not isinstance(tp_list, list) or not tp_list:
                continue
            ts_raw = pd.Timestamp(r["ts_utc"])
            # A runner tz-naive ts-eket ad át (numpy datetime64-ből),
            # ezért UTC-naive-ra konvertálunk
            ts_naive = ts_raw.tz_convert("UTC").tz_localize(None) if ts_raw.tz is not None else ts_raw
            rows.append(_SignalRow(
                id=int(i),
                ts=ts_naive,
                chat_id=int(r["chat_id"]),
                chat_name=str(r.get("chat_name", "")),
                direction=str(r["parsed.direction"]),
                entry_low=float(r["parsed.entry_low"]) - self.entry_zone_expand,
                entry_high=float(r["parsed.entry_high"]) + self.entry_zone_expand,
                tp_list=[float(x) for x in tp_list],
                sl=float(r["parsed.sl"]),
            ))
        # Időrendi sorrend, hogy a `next_to_activate` pointer jól haladjon
        rows.sort(key=lambda s: s.ts)

        if self.consensus_required:
            # Csak a consensus-listán szereplő csatornák jeleit használjuk a
            # detektáláshoz; a többi átmegy "neutrális" módban (nem participál).
            if self.consensus_channels is not None:
                cons_set = set(int(c) for c in self.consensus_channels)
                cons_rows = [s for s in rows if s.chat_id in cons_set]
            else:
                cons_rows = rows
            kept = self._filter_consensus(cons_rows)
            kept_ids = {(s.chat_id, s.ts) for s in kept}
            # Csak azokat tartjuk meg az eredeti rows-ból, amelyek a consensus-mintát
            # alkotó signalok. (Egyértelmű azonosítás: (chat_id, ts) párral.)
            rows = [s for s in rows if (s.chat_id, s.ts) in kept_ids]

        # Channel post-filter (csak ha consensus mód aktív, mert csak ott töltöttünk be mindent)
        if self.consensus_required and self.channels is not None:
            allowed = set(int(c) for c in self.channels)
            rows = [s for s in rows if s.chat_id in allowed]

        self._signals = rows
        self._next_to_activate = 0

        # ATR előszámítás (ha kell — vagy override-hoz, vagy trailinghez)
        if self.use_atr_levels or self.compute_atr:
            atr_df = ctx.candles_mtf[self.atr_tf].copy()
            atr_series = _atr_fn(atr_df["high"], atr_df["low"], atr_df["close"], self.atr_period)
            self._atr_arr = atr_series.to_numpy(dtype=float)
            # A tick csak már lezárt gyertyát olvashat — searchsorted-hoz a zárás time-ot használjuk
            tf_td = _tf_td64(self.atr_tf)
            self._atr_close_ts = atr_df["timestamp"].values + tf_td
        else:
            self._atr_arr = None
            self._atr_close_ts = None

        # require_ranging: ADX előszámítás
        if self.require_ranging:
            adx_df = ctx.candles_mtf[self.ranging_adx_tf].copy()
            adx_series, _, _ = _adx_fn(adx_df["high"], adx_df["low"], adx_df["close"], self.ranging_adx_period)
            self._ranging_adx_arr = adx_series.to_numpy(dtype=float)
            tf_td_rng = _tf_td64(self.ranging_adx_tf)
            self._ranging_adx_close_ts = adx_df["timestamp"].values + tf_td_rng
        else:
            self._ranging_adx_arr = None
            self._ranging_adx_close_ts = None

        # Trend-filter EMA előszámítás
        if self.trend_filter_enabled:
            tdf = ctx.candles_mtf[self.trend_filter_tf].copy()
            self._trend_fast_arr = _ema_fn(tdf["close"], self.trend_filter_ema_fast).to_numpy(dtype=float)
            self._trend_slow_arr = _ema_fn(tdf["close"], self.trend_filter_ema_slow).to_numpy(dtype=float)
            tf_td_tr = _tf_td64(self.trend_filter_tf)
            self._trend_close_ts = tdf["timestamp"].values + tf_td_tr
        else:
            self._trend_fast_arr = None
            self._trend_slow_arr = None
            self._trend_close_ts = None

    def _filter_consensus(self, rows: List[_SignalRow]) -> List[_SignalRow]:
        """
        Csak azokat a signalokat tartja meg, amelyekhez van legalább
        `consensus_min_channels` különböző csat. azonos irányú signalja
        consensus_window_min perces ablakban, entry-mid abs különbség <=
        consensus_price_tol $.

        Csoport-épitéshez union-find: minden olyan (i, j) párt egyesítünk
        amelyik a fenti feltételeket teljesíti.
        """
        n = len(rows)
        if n == 0 or self.consensus_min_channels < 2:
            return rows
        parent = list(range(n))

        def find(x: int) -> int:
            while parent[x] != x:
                parent[x] = parent[parent[x]]
                x = parent[x]
            return x

        def union(a: int, b: int) -> None:
            ra, rb = find(a), find(b)
            if ra != rb:
                parent[ra] = rb

        win = pd.Timedelta(minutes=self.consensus_window_min)
        for i in range(n):
            for j in range(i + 1, n):
                if rows[j].ts - rows[i].ts > win:
                    break
                if rows[j].direction != rows[i].direction:
                    continue
                ei = (rows[i].entry_low + rows[i].entry_high) / 2.0
                ej = (rows[j].entry_low + rows[j].entry_high) / 2.0
                if abs(ei - ej) > self.consensus_price_tol:
                    continue
                union(i, j)

        # Csoportonként megnézzük hány csatornán van
        from collections import defaultdict
        groups: dict[int, list[int]] = defaultdict(list)
        for i in range(n):
            groups[find(i)].append(i)
        keep_ids: set[int] = set()
        for root, members in groups.items():
            channels_in_group = {rows[m].chat_id for m in members}
            if len(channels_in_group) >= self.consensus_min_channels:
                keep_ids.update(members)
        return [rows[i] for i in sorted(keep_ids)]

    def _load_news_events(self, path: Optional[str]) -> List[tuple]:
        """Beolvassa a news events JSON-t és (start_ts, end_ts) intervallumokká
        alakítja a `news_event_buffer_min` alapján."""
        if not path:
            return []
        import json as _json
        try:
            data = _json.loads(Path(path).read_text())
        except Exception:
            return []
        buf = pd.Timedelta(minutes=self.news_event_buffer_min)
        wins = []
        for ev in data:
            try:
                ts = pd.Timestamp(ev["ts"]).tz_convert("UTC").tz_localize(None)
            except Exception:
                try:
                    ts = pd.Timestamp(ev["ts"])
                    if ts.tz is not None:
                        ts = ts.tz_convert("UTC").tz_localize(None)
                except Exception:
                    continue
            wins.append((ts - buf, ts + buf))
        return wins

    def _in_news_window(self, ts: pd.Timestamp) -> bool:
        """True, ha az adott timestamp valamely news-blokk ablakba esik."""
        # 1) Ismétlődő óra-blokkok (opt. weekday-szűrővel)
        if self.news_blocked_hours:
            if self.news_blocked_weekdays is not None:
                if ts.weekday() not in self.news_blocked_weekdays:
                    goto_events = True
                else:
                    goto_events = False
            else:
                goto_events = False
            if not goto_events:
                h = ts.hour
                for rng in self.news_blocked_hours:
                    start_h, end_h = int(rng[0]), int(rng[1])
                    if start_h <= h < end_h:
                        return True
        # 2) Abszolút events (JSON-ból)
        if self._news_event_windows:
            for start_ts, end_ts in self._news_event_windows:
                if start_ts <= ts < end_ts:
                    return True
        return False

    def _trend_direction(self, ts: pd.Timestamp) -> Optional[str]:
        """Visszaadja 'BUY' (uptrend) / 'SELL' (downtrend) / None (nincs adat)
        a megadott időpontra a trend-filter EMA-i alapján."""
        if (self._trend_fast_arr is None or self._trend_slow_arr is None
                or self._trend_close_ts is None):
            return None
        i = int(np.searchsorted(self._trend_close_ts, ts.to_datetime64(), side="right") - 1)
        if i < 0 or i >= len(self._trend_fast_arr):
            return None
        f = float(self._trend_fast_arr[i])
        s = float(self._trend_slow_arr[i])
        if not (np.isfinite(f) and np.isfinite(s)):
            return None
        if f > s:
            return "BUY"
        if f < s:
            return "SELL"
        return None  # egyenlő → bizonytalan

    def _current_ranging_adx(self, ts: pd.Timestamp) -> Optional[float]:
        """Az aktuális ADX érték a ranging_adx_tf-en (searchsorted a lezárt gyertyák close_ts-jén)."""
        if self._ranging_adx_arr is None or self._ranging_adx_close_ts is None:
            return None
        i = int(np.searchsorted(self._ranging_adx_close_ts, ts.to_datetime64(), side="right") - 1)
        if i < 0 or i >= len(self._ranging_adx_arr):
            return None
        v = float(self._ranging_adx_arr[i])
        return v if np.isfinite(v) else None

    def _current_atr(self, ts: pd.Timestamp) -> Optional[float]:
        if self._atr_arr is None or self._atr_close_ts is None:
            return None
        i = int(np.searchsorted(self._atr_close_ts, ts.to_datetime64(), side="right") - 1)
        if i < 0 or i >= len(self._atr_arr):
            return None
        v = float(self._atr_arr[i])
        return v if np.isfinite(v) and v > 0 else None

    def on_tick(self, ts: pd.Timestamp, bid: float, ask: float) -> Optional[Decision]:
        timeout = pd.Timedelta(minutes=self.signal_timeout_minutes)

        # Aktív (WAITING) signalok közül megnézzük az elsőt amelyik zónába esik.
        # Közben az időkeretükön túliakat EXPIRED-be.
        active: List[_SignalRow] = []
        for s in self._signals[: self._next_to_activate]:
            if s.state != "WAITING":
                continue
            if ts - s.ts > timeout:
                s.state = "EXPIRED"
                continue
            active.append(s)
        # Új signalok aktiválása: amik időben már beérkeztek
        while self._next_to_activate < len(self._signals):
            cand = self._signals[self._next_to_activate]
            if cand.ts > ts:
                break
            self._next_to_activate += 1
            if ts - cand.ts > timeout:
                cand.state = "EXPIRED"
            else:
                active.append(cand)

        if not active:
            return None

        # News avoidance: ne trade-eljünk news-blokk ablakban
        # (a signalok WAITING-ben maradnak, ha a window után is aktívak triggerelhetnek)
        if self._in_news_window(ts):
            return None

        # Mely signalra adunk Decision-t? Az első, amelyik zónában van.
        for s in active:
            trigger = ask if s.direction == "BUY" else bid
            if not (s.entry_low <= trigger <= s.entry_high):
                continue

            # Trend-filter: csak akkor lép be a strategia, ha a megadott TF
            # EMA-trendje azonos irányú a signallal. Bizonytalanságot
            # mode szerint kezelünk.
            if self.trend_filter_enabled:
                trend = self._trend_direction(ts)
                if trend is None:
                    if self.trend_filter_mode == "neutral_drop":
                        s.state = "USED"  # ne próbálkozzunk vele többet
                        continue
                elif trend != s.direction:
                    s.state = "USED"  # counter-trend → eldobjuk
                    continue

            # require_ranging: csak akkor engedjük, ha ADX a küszöb ALATT van
            if self.require_ranging:
                adx_v = self._current_ranging_adx(ts)
                if adx_v is None or adx_v >= self.ranging_adx_threshold:
                    s.state = "USED"  # trend jelen van → eldobjuk
                    continue

            # Megjelöljük USED-nek (egy signal max 1 trade-ot szül)
            s.state = "USED"

            entry_price = trigger

            # ── SL/TP távolságok meghatározása ────────────────────────────
            tp_distances: Optional[List[float]] = None
            atr_v: Optional[float] = self._current_atr(ts)
            mode = "atr" if self.use_atr_levels else "signal"

            if self.use_atr_levels:
                if atr_v is None:
                    # ATR még warm-up alatt — átugorjuk a jelet
                    continue
                sl_distance = atr_v * self.sl_atr_mult
                tp_distance = atr_v * self.tp_atr_mult
                tp_chosen = (entry_price + tp_distance) if s.direction == "BUY" else (entry_price - tp_distance)
                tp_idx_used = "atr"

                if self.tp_strategy == "multi":
                    # ATR-alapú multi-réteg: a tp_atr_mult-ot a rétegekre osztjuk lineárisan
                    # (default: csak 1 réteg, mert tp_atr_mult egyetlen szám)
                    # Ha valaki tényleg több réteget akar ATR módban, megadhatja:
                    n_layers = int(self.params.get("atr_n_layers", 1))
                    if n_layers > 1:
                        step = self.tp_atr_mult / n_layers
                        tp_distances = [atr_v * step * (k + 1) for k in range(n_layers)]
                        tp_distance = tp_distances[-1]
                        tp_chosen = (entry_price + tp_distance) if s.direction == "BUY" else (entry_price - tp_distance)
            else:
                # ── Eredeti: signal-alapú SL/TP, mostantól sl_mult/tp_mult skálával
                sl_distance = abs(entry_price - s.sl) * self.sl_mult

                if s.direction == "BUY":
                    valid_tps = [tp for tp in s.tp_list if tp > entry_price]
                else:
                    valid_tps = [tp for tp in s.tp_list if tp < entry_price]
                if not valid_tps:
                    continue
                tps_by_dist = sorted(valid_tps, key=lambda tp: abs(tp - entry_price))

                if self.tp_strategy == "multi":
                    subset = tps_by_dist
                    if self.tp_multi_indices is not None:
                        # csak az érvényes indexek (a többit némán eldobjuk)
                        subset = [tps_by_dist[i] for i in self.tp_multi_indices
                                  if 0 <= int(i) < len(tps_by_dist)]
                    elif self.tp_multi_first_n is not None:
                        subset = subset[: int(self.tp_multi_first_n)]
                    elif self.tp_multi_last_n is not None:
                        subset = subset[-int(self.tp_multi_last_n):]
                    if not subset:
                        continue
                    tp_distances = [abs(tp - entry_price) * self.tp_mult for tp in subset]
                    tp_distance = tp_distances[-1]
                    tp_chosen = (entry_price + tp_distance) if s.direction == "BUY" else (entry_price - tp_distance)
                    tp_idx_used = "multi"
                elif self.tp_strategy == "last":
                    tp_choice = tps_by_dist[-1]
                    tp_distance = abs(tp_choice - entry_price) * self.tp_mult
                    tp_chosen = (entry_price + tp_distance) if s.direction == "BUY" else (entry_price - tp_distance)
                    tp_idx_used = len(tps_by_dist) - 1
                else:  # "first"
                    idx = self.tp_idx if -len(tps_by_dist) <= self.tp_idx < len(tps_by_dist) else 0
                    tp_choice = tps_by_dist[idx]
                    tp_distance = abs(tp_choice - entry_price) * self.tp_mult
                    tp_chosen = (entry_price + tp_distance) if s.direction == "BUY" else (entry_price - tp_distance)
                    tp_idx_used = idx

            # TP-ladder távolságok — ha a strategia params megadta, és a
            # signal-alapú módban vagyunk (signal SL/TP listája rendelkezésre áll)
            ladder_trigger_distance: Optional[float] = None
            ladder_dest_distance: Optional[float] = None
            if (not self.use_atr_levels
                    and self.ladder_trigger_idx is not None
                    and self.ladder_dest_idx is not None
                    and 0 <= int(self.ladder_trigger_idx) < len(tps_by_dist)
                    and 0 <= int(self.ladder_dest_idx) < len(tps_by_dist)):
                lt_tp = tps_by_dist[int(self.ladder_trigger_idx)]
                ld_tp = tps_by_dist[int(self.ladder_dest_idx)]
                ladder_trigger_distance = abs(lt_tp - entry_price)
                ladder_dest_distance    = abs(ld_tp - entry_price)

            return Decision(
                ts=ts,
                allow_trade=True,
                reason="ok",
                direction=s.direction,
                score=1.0,
                size=1.0,
                sl_distance=sl_distance,
                tp_distance=tp_distance,
                tp_distances=tp_distances,
                ladder_trigger_distance=ladder_trigger_distance,
                ladder_dest_distance=ladder_dest_distance,
                indicators={
                    "signal_id": s.id,
                    "signal_ts": s.ts.isoformat(),
                    "chat_id": s.chat_id,
                    "chat_name": s.chat_name,
                    "entry_low": s.entry_low,
                    "entry_high": s.entry_high,
                    "tp_chosen": tp_chosen,
                    "tp_idx": tp_idx_used,
                    "tp_strategy": self.tp_strategy,
                    "n_tp": len(s.tp_list),
                    "sl": s.sl,
                    "level_mode": mode,
                    # "atr" kulcs alatt — a runner ezt olvassa az atr_at_open-hez,
                    # ami a trailing logikának kell (break_even / atr_trail)
                    "atr": atr_v if atr_v is not None else float("nan"),
                    "atr_v": atr_v if atr_v is not None else float("nan"),
                    "sl_mult": self.sl_mult,
                    "tp_mult": self.tp_mult,
                },
            )
        return None

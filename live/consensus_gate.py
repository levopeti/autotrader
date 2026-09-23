"""
Consensus + per-channel recipe gate a live signal-flow-ra.

A gate az alábbi szabályokat alkalmazza minden bejövő ZMQ signal-üzenetre:

  1) Per-channel TP-szint szűrés (csak a csatorna-specifikus tp_idx engedett):
       ANN     → tp_idx = 0   (a csatorna 1-TP-s, ezt soha nem szabad váltani)
       VIP     → tp_idx = 1   (a 2. legközelebbi TP)
       Traderz → tp_idx = 2   (a 3. legközelebbi TP)
  2) ANN-jelek SL/TP-jét felülírjuk ATR-alapúra (sl = 2×ATR, tp = 3×ATR a
     zóna közepétől). A signal SL-je live-ban túl szűk, ezért nem trade-elhető.
  3) Cross-channel consensus detektálás: ha egy ANN VAGY Traderz jelhez 60
     percen belül azonos irányú, ±5$ közelségű "tükör" érkezik a MÁSIK
     csatornán (ANN↔Traderz only — VIP NEM számít a consensushoz), akkor:
       - megemeli a méretet `consensus_size_mult`-szal (default 2.0×)
       - logba kerül (high-confidence)

A backtest 76+13 futás alapján (ld. `logs_bulk/perch_metrics.csv` és
`consensus_novip_metrics.csv`, 39 nap GOLD): ANN+Traderz consensus PF
1.94–3.22 a non-VIP módban; VIP "egyetértést" eldobjuk.

A gate stateful (sliding window-t tart az utolsó N percre), thread-safe NEM,
csak async kontextusban használandó.
"""
from __future__ import annotations

import logging
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable, Deque, List, Optional

import numpy as np
import pandas as pd

logger = logging.getLogger("live.consensus_gate")


# ── Csatorna-azonosítók (egyezzen a parser/ZMQ message-ben küldött chat_id-vel)
CH_ANN     = 1086101437
CH_VIP     = 2001216034
CH_TRADERZ = 3496306840


@dataclass
class RecipeRules:
    """Csatornánkénti recept-leíró. Kibővíthető további szabályokkal.

    A gate-ben minden `*_tp_idx` mező 0-based DISTANCE-RANK index — a backtest
    szemszögéből az entry-től való távolság szerinti (legközelebbi=0). A parser
    viszont PRICE-ASCENDING sorrendet küld (parsed.tp_list[0] = legkisebb ár),
    ami BUY-on egybeesik a distance-szal, SELL-en INVERZE. A gate az
    `expected_tp_count` mező alapján számolja a helyes konverziót:
        BUY:  distance_rank = price_rank
        SELL: distance_rank = (N - 1) - price_rank
    """
    tp_idx_only: Optional[int] = None          # csak ezt a tp_idx-et fogadjuk (0-based DISTANCE)
    expected_tp_count: int = 1                 # mennyi TP-t küld a csatorna jelenként (Traderz=5, VIP=5-6, ANN=1)
    use_atr_levels: bool = False               # ATR-alapú SL/TP override
    atr_sl_mult: float = 2.0
    atr_tp_mult: float = 3.0
    # TP-ladder: 0-based DISTANCE-rank indexek a backtest-receptből
    ladder_trigger_tp_idx: Optional[int] = None
    ladder_dest_tp_idx: Optional[int] = None
    # Trend-filter: ha True, a signalt csak akkor fogadjuk el, ha az iránya
    # egyezik a `trend_provider` által visszaadott trend-iránnyal. None →
    # nincs trend-filter (ANN-en pl. KIFEJEZETTEN ne legyen, mert ott
    # counter-trend reverzió a működő mechanizmus).
    trend_filter: bool = False
    # Per-channel news_blocked_hours override — ha megadva (nem None), akkor
    # a GateConfig.news_blocked_hours GLOBÁLIS érték helyett ez alkalmazódik
    # csak erre a csatornára. Így pl. ANN-nek szűrhetünk 08-16 UTC-t, de
    # VIP+Traderz-en globálisan kikapcsolva maradhat a filter.
    # None → a globális GateConfig.news_blocked_hours érvényes
    # [] → nincs news filter (override, felülírja a globálist)
    # [[8,16]] → 08-15 UTC block csak ezen a csatornán
    news_blocked_hours: Optional[List[List[int]]] = None


# A backtest eredménye alapján.
# NB: VIP-et szándékosan KIHAGYJUK (lásd memory/per_channel_signal_recipes.md).
# Backtest: VIP tp_idx=1 PF 1.07, +16$/39 nap GOLD; reális slippage/spread mellett
# valószínűleg breakeven vagy negatív. Az ANN+Traderz consensus már a backtest
# kétharmadát adja sokkal jobb risk-adjusted módon — VIP felvétele csak komplexitást
# növel kezdéskor. Ha élesben az ANN+Traderz setup beigazolódik, VIP utólag
# visszakapcsolható egy új sweep után.
PER_CHANNEL_RULES: dict[int, RecipeRules] = {
    # ── DEPLOY 2026-07-29: "combo_2_shared_tp2" — WF validated best-of-best.
    # Backtest 68 nap: +$144/hó IS, +$162/hó OOS (12-ablak WF, 83% pos, worst -$6).
    # 2026-09-09 sweep (120d): tp=2 tr=4h news=off → VIP +$144, Traderz +$140.
    # Uniform tp_idx=2 (3. legközelebbi TP), közös 4h trend filter, news filter off.
    #
    # VIP: tp_idx=2 = 3. legközelebbi (VIP általában 6 TP-t küld).
    # news_blocked_hours=[[0,4]]: 2026-09-23 live audit szerint 90 nap alatt
    # a 0-3 UTC blokk 182 tr WR 24% -$601 volt, ami ≈ a teljes 90d vesztés
    # (-$585). Regime-független Ázsia-hajnal fake breakout — kizárva.
    CH_VIP:     RecipeRules(tp_idx_only=2, expected_tp_count=6,
                            trend_filter=True,
                            news_blocked_hours=[[0, 4]]),
    # Traderz: tp_idx=2 = 3. legközelebbi (Traderz 5 TP-t küld).
    # LADDER KIKAPCSOLVA — a combo_2 uniform tp_idx=2 esetén nem kell.
    # news_blocked_hours=[[0,4]]: ld. VIP magyarázat fent.
    CH_TRADERZ: RecipeRules(tp_idx_only=2, expected_tp_count=5,
                            trend_filter=True,
                            news_blocked_hours=[[0, 4]]),
    # ── 2026-09-09 ANN VISSZAKAPCSOLVA külön regime-robusztus recepttel.
    # Sweep 120d: tp=0, trend=off, news=[[8,16]], ATR-override → +$117 total,
    # 57% WR, PF 1.96, minden regime pozitív (BULL+$20 / BEAR+$27 / SIDE+$71).
    # WF 4 ablak: 3/4 OOS pozitív. Ez az EGYETLEN regime-robusztus ANN config.
    #
    # Kulcs-paraméterek:
    #   - tp_idx=0 (ANN 1-TP-s csatorna, mindegy hogyan indexeljük)
    #   - trend_filter=OFF (ANN counter-trend reverzió, trend filter árt)
    #   - news_blocked_hours=[[8,16]] UTC (per-channel override; VIP+Traderz-nél OFF marad)
    #   - use_atr_levels=True: az ANN saját SL-je élesben túl szűk → 2×ATR SL, 3×ATR TP
    CH_ANN:     RecipeRules(tp_idx_only=0, expected_tp_count=1,
                            trend_filter=False,       # ANN nem szereti trend filter-t
                            news_blocked_hours=[[8, 16]],   # per-channel news block
                            use_atr_levels=True,
                            atr_sl_mult=2.0, atr_tp_mult=3.0),
}

# Mely csatornák vesznek részt a consensus-detektálásban
# combo_2 setup: consensus_required=False, tehát ez már nem használt,
# de tartsuk fenn a szemantikát: VIP+Traderz mindkettő aktív, mindkettő számít.
CONSENSUS_CHANNELS: set[int] = {CH_VIP, CH_TRADERZ}


@dataclass
class GateConfig:
    consensus_window_min: float = 60.0
    consensus_price_tol: float = 5.0
    consensus_size_mult: float = 2.0
    # Az eredeti signal méret (1.0) ennyivel szorzódik, ha consensus-jelnek
    # minősül. Ha nincs consensus, a méret változatlan (1.0×).
    history_keep_min: float = 180.0    # cache-ben tartott múlt (>= window)
    # News avoidance: ne fogadjunk el signalt a megadott UTC óra-intervallumokban
    # pl. [[8, 16]] → 08:00-15:59 UTC block ("US news window").
    # Walk-forward validated 2026-07-24: signal_replay OOS +$62 vs -$140 (∆+$202).
    # Ha üres/None → nincs filter (backward compat).
    news_blocked_hours: List[List[int]] = field(default_factory=list)
    # Opcionális weekday-szűrés: pl. [0,1,2,3,4] = csak hétköznap. None = mindenkor.
    news_blocked_weekdays: Optional[List[int]] = None


@dataclass
class _SignalMemo:
    """A consensus-detektorban tárolt rövid memorandum egy signalról."""
    ts: pd.Timestamp
    chat_id: int
    direction: str            # "BUY" | "SELL"
    entry_mid: float
    message_id: Optional[int] = None


@dataclass
class GateOutput:
    """A gate után létrejövő, módosított signal-paraméterek."""
    accept: bool
    reason: str
    # Csak ha accept=True:
    direction: str = ""
    zone_low: float = 0.0
    zone_high: float = 0.0
    tp: float = 0.0
    sl: float = 0.0
    size_mult: float = 1.0
    is_consensus: bool = False
    consensus_partner: Optional[str] = None   # a párt-csatorna neve (logoláshoz)
    # TP-ladder árszintek (csak ha a recept aktiválta és sikerült feloldani
    # a korábbi ZMQ-üzenetekből). Élesben a Position OPEN állapotban
    # ezekkel a szintekkel mozgatja az SL-t REST-en keresztül.
    ladder_trigger_price: Optional[float] = None
    ladder_dest_price: Optional[float] = None
    # A TÉNYLEGESEN kereskedett target raw tp_idx (1-based, price-ascending
    # a parser konvenciója szerint). A CSV-be ezt kell írni — NEM az aktuális
    # bejövő ZMQ-üzenet tp_idx-ét, mert a gate re-emit + cancel-replace loop
    # miatt az utolsó bejövő raw_tp_idx (általában a legmagasabb price-rank)
    # felülírná a target-et. Számítás:
    #   BUY:  target_raw = rule.tp_idx_only + 1
    #   SELL: target_raw = rule.expected_tp_count - rule.tp_idx_only
    target_raw_tp_idx: Optional[int] = None


class ConsensusGate:
    """
    A gate egy hosszú életű objektum: minden bejövő signal `process(...)` hívást
    eredményez, és a gate egy GateOutput-tal felel — accept=True esetén a hívó
    a transformált paramétereket továbbküldi a position queue-ra.

    Az `atr_provider` callable adja meg az aktuális ATR ($) értéket egy
    timestamp-re. Ha None, az ATR-override szabályok blokkolják az adott
    signalt (nincs SL/TP, nem trade-elünk). A provider-t a runner adja át;
    eredeti felhasználás: 5-min candle pull-ból frissített ATR(14).
    """

    def __init__(
        self,
        cfg: Optional[GateConfig] = None,
        atr_provider: Optional[Callable[[pd.Timestamp], Optional[float]]] = None,
        trend_provider: Optional[Callable[[], Optional[str]]] = None,
        rules: Optional[dict[int, RecipeRules]] = None,
        consensus_channels: Optional[set[int]] = None,
    ):
        self.cfg = cfg or GateConfig()
        self.atr_provider = atr_provider
        # trend_provider: visszaad "BUY"/"SELL"/None — a runner adja át, és
        # belül egy hosszabb-TF EMA9 vs EMA21 alapján számítja periodikusan
        self.trend_provider = trend_provider
        self.rules = dict(rules) if rules is not None else dict(PER_CHANNEL_RULES)
        self.cons_channels = set(consensus_channels) if consensus_channels is not None else set(CONSENSUS_CHANNELS)
        self._history: Deque[_SignalMemo] = deque()
        # Per-(chat_id, msg_id) tp_idx → tp_price memo a ladder feloldásához.
        # A parser tp_idx-enként külön ZMQ-üzenetet küld; a ladder konfigja
        # több TP árszintet igényel (pl. TP3 trigger + TP1 dest), és ezeket
        # csak akkor tudjuk feloldani, ha minden megfelelő tp_idx-et láttunk
        # ugyanazzal a message_id-vel.
        # Kulcs: (chat_id, message_id) → {parser_tp_idx (1-based): tp_price}.
        self._tp_price_memo: dict[tuple[int, int], dict[int, float]] = {}
        self._memo_ts: dict[tuple[int, int], pd.Timestamp] = {}
        # Az utoljára emit-elt memo-tartalom hash-e — így nem emit-elünk
        # duplán ugyanarra a (chat_id, msg_id)-re, és edit esetén észleljük
        # a változást → re-emit.
        self._last_emit_hash: dict[tuple[int, int], tuple] = {}
        # Egy emit-re visszatartott eredeti üzenet-context (a target message
        # paraméterei), hogy késleltetett emit-nél is az igazi target tp/sl-jét
        # használjuk, ne az aktuális (utolsó beérkező) üzenetét.
        self._pending_target: dict[tuple[int, int], dict] = {}

    # ── Belső segédfüggvények ───────────────────────────────────────────────

    def _now(self) -> pd.Timestamp:
        return pd.Timestamp(datetime.now(timezone.utc)).tz_convert("UTC").tz_localize(None)

    def _prune(self, now: pd.Timestamp) -> None:
        keep = pd.Timedelta(minutes=self.cfg.history_keep_min)
        while self._history and (now - self._history[0].ts) > keep:
            self._history.popleft()

    def _prune_memo(self, now: pd.Timestamp) -> None:
        # 5 percnél régebbi memo-bejegyzéseket eldobjuk (egy signal összes
        # tp_idx-üzenete néhány másodpercen belül megérkezik a parsertől)
        keep = pd.Timedelta(minutes=5)
        stale_keys = [k for k, v_ts in self._memo_ts.items() if (now - v_ts) > keep]
        for k in stale_keys:
            self._tp_price_memo.pop(k, None)
            self._memo_ts.pop(k, None)

    def _find_consensus_partner(
        self,
        candidate: _SignalMemo,
    ) -> Optional[_SignalMemo]:
        """Visszaadja a partner-jelet, ha van; None ha nincs."""
        if candidate.chat_id not in self.cons_channels:
            return None
        win = pd.Timedelta(minutes=self.cfg.consensus_window_min)
        tol = self.cfg.consensus_price_tol
        for prev in reversed(self._history):
            if candidate.ts - prev.ts > win:
                break
            if prev.chat_id == candidate.chat_id:
                continue
            if prev.chat_id not in self.cons_channels:
                continue
            if prev.direction != candidate.direction:
                continue
            if abs(prev.entry_mid - candidate.entry_mid) > tol:
                continue
            return prev
        return None

    # ── Fő API ───────────────────────────────────────────────────────────────

    def process(
        self,
        chat_id: int,
        chat_name: str,
        direction: str,
        zone_low: float,
        zone_high: float,
        tp: float,
        sl: float,
        tp_idx: int,
        message_id: Optional[int] = None,
        raw_tp_idx: Optional[int] = None,
        ts: Optional[pd.Timestamp] = None,
    ) -> GateOutput:
        """
        raw_tp_idx: a parser eredeti 1-based tp_idx-e (price-ascending sorrend).
        tp_idx: legacy compat (ignorált, ha raw_tp_idx adott).
        A gate a direction + rule.expected_tp_count alapján számolja a DISTANCE-
        rank indexet, ami a backtest receptünkkel kompatibilis (BUY-on egyezik
        a price-rank-kel, SELL-en a fordítottja).
        """
        ts = ts or self._now()
        entry_mid = (zone_low + zone_high) / 2.0

        # TP-ár memo feltöltése: MINDEN bejövő üzenetet rögzítünk az adott
        # (chat_id, message_id) alatt, függetlenül attól, hogy elfogadjuk-e
        # a trade-et — a ladder árszintekhez kell.
        raw_idx = raw_tp_idx if raw_tp_idx is not None else (tp_idx + 1)
        if message_id is not None:
            key = (int(chat_id), int(message_id))
            self._tp_price_memo.setdefault(key, {})[int(raw_idx)] = float(tp)
            self._memo_ts[key] = ts
            self._prune_memo(ts)

        # 1) Per-channel recept-lookup
        rule = self.rules.get(chat_id)
        if rule is None:
            return GateOutput(False, reason=f"unknown_channel({chat_id})")

        # 1.5) raw_tp_idx (1-based, PRICE-ascending) → DISTANCE-rank (0-based)
        # konverzió direction-függő, recept expected_tp_count alapján.
        n_tp = int(rule.expected_tp_count)
        price_rank_0based = int(raw_idx) - 1
        if direction.upper() == "BUY":
            distance_idx = price_rank_0based
        else:  # SELL: árszint-sorrend inverze a távolság-sorrendnek
            distance_idx = (n_tp - 1) - price_rank_0based

        # 1.6) Trend-filter (csak akkor, ha a recept kéri ÉS van trend_provider)
        if rule.trend_filter and self.trend_provider is not None:
            trend = self.trend_provider()
            if trend is None:
                # Bizonytalan trend → eldobjuk (konzervatív)
                return GateOutput(False, reason="trend_filter(unknown)")
            if trend != direction.upper():
                return GateOutput(False, reason=f"trend_filter({trend}!={direction})")

        # 1.7) News avoidance filter (UTC óra-blokk, opcionális weekday-szűrő).
        # Precedencia: rule.news_blocked_hours (per-channel override) > cfg.news_blocked_hours (globális).
        # Walk-forward validated 2026-07-24: [[8,16]] → OOS +$62 vs -$140 (∆+$202).
        # 2026-09-09 sweep: ANN-en news=[[8,16]] KELL (+$27 vs -$111 avg), VIP+Traderz-en OFF kell.
        active_news_blocks = (rule.news_blocked_hours
                              if rule.news_blocked_hours is not None
                              else self.cfg.news_blocked_hours)
        if active_news_blocks:
            wd_ok = True
            if self.cfg.news_blocked_weekdays is not None:
                wd_ok = ts.weekday() in self.cfg.news_blocked_weekdays
            if wd_ok:
                h = ts.hour
                for rng in active_news_blocks:
                    if int(rng[0]) <= h < int(rng[1]):
                        return GateOutput(False, reason=f"news_window({h}h in {rng})")

        # 2) Target-azonosítás: ha ez a (chat_id, msg_id) target-üzenete
        # (= a recept tp_idx-ének megfelelő distance-indexű), eltároljuk a
        # context-jét egy "pending"-ben. Az emit-et akkor tüzeljük, amikor a
        # memo a ladder-hez szükséges minden TP-t tartalmazza.
        is_target = (rule.tp_idx_only is not None and distance_idx == rule.tp_idx_only)
        memo_key  = (int(chat_id), int(message_id)) if message_id is not None else None
        if is_target and memo_key is not None:
            self._pending_target[memo_key] = {
                "direction": direction,
                "zone_low": zone_low, "zone_high": zone_high,
                "tp": tp, "sl": sl,
                "chat_id": chat_id, "chat_name": chat_name,
                "ts": ts,
            }

        # Megnézzük, hogy a memo elég teljes-e a ladder feloldásához.
        # Ha nincs ladder a receptben, "elég" már egy bejegyzés is.
        tp_memo = self._tp_price_memo.get(memo_key, {}) if memo_key else {}
        ladder_active = (
            rule.ladder_trigger_tp_idx is not None
            and rule.ladder_dest_tp_idx is not None
            and not rule.use_atr_levels
        )

        def _dist_to_raw(d: int) -> int:
            if direction.upper() == "BUY":
                return d + 1
            return n_tp - d

        ready_to_emit = True
        if ladder_active and memo_key is not None:
            trig_raw = _dist_to_raw(int(rule.ladder_trigger_tp_idx))
            dest_raw = _dist_to_raw(int(rule.ladder_dest_tp_idx))
            ready_to_emit = (trig_raw in tp_memo) and (dest_raw in tp_memo)

        # Nincs target context (még) erre a msg_id-re → drop az aktuális üzenetet
        if memo_key is None or memo_key not in self._pending_target:
            return GateOutput(False, reason=f"tp_idx_filter({distance_idx}!={rule.tp_idx_only})")

        # Egy adott (chat_id, msg_id) emit-jét csak akkor ismételjük, ha a
        # memo-tartalom változott (edit-detekció).
        hash_now = tuple(sorted(tp_memo.items()))
        if not ready_to_emit:
            return GateOutput(False, reason=f"awaiting_memo({len(tp_memo)})")
        if self._last_emit_hash.get(memo_key) == hash_now:
            return GateOutput(False, reason="already_emitted")
        self._last_emit_hash[memo_key] = hash_now

        # Innentől az emit-építéshez a pending TARGET context-jét használjuk,
        # nem az aktuális (utolsó beérkező) üzenetét.
        pt = self._pending_target[memo_key]
        direction = pt["direction"]
        zone_low  = pt["zone_low"]; zone_high = pt["zone_high"]
        tp        = pt["tp"];       sl        = pt["sl"]
        chat_name = pt["chat_name"]
        ts        = pt["ts"]
        entry_mid = (zone_low + zone_high) / 2.0

        # 3) ATR-override SL/TP-re, ha a recept ezt mondja (ANN)
        if rule.use_atr_levels:
            atr = self.atr_provider(ts) if self.atr_provider else None
            if atr is None or atr <= 0 or not np.isfinite(atr):
                return GateOutput(False, reason="atr_not_ready")
            sl_dist = atr * rule.atr_sl_mult
            tp_dist = atr * rule.atr_tp_mult
            if direction == "BUY":
                new_sl = entry_mid - sl_dist
                new_tp = entry_mid + tp_dist
            else:
                new_sl = entry_mid + sl_dist
                new_tp = entry_mid - tp_dist
            sl, tp = new_sl, new_tp
            logger.info(
                "[GATE] ATR-override (%s) | atr=%.3f | SL=%.2f TP=%.2f",
                chat_name, atr, sl, tp,
            )

        # 4) Consensus-detektálás
        self._prune(ts)
        memo = _SignalMemo(ts=ts, chat_id=chat_id, direction=direction,
                           entry_mid=entry_mid, message_id=message_id)
        partner = self._find_consensus_partner(memo)
        is_cons = partner is not None
        # Mindenképp memóziumra rakjuk (a jövőbeli signalok consensus-megfeleltetéséhez)
        self._history.append(memo)

        size_mult = self.cfg.consensus_size_mult if is_cons else 1.0
        partner_name = None
        if is_cons:
            partner_name = "ANN" if partner.chat_id == CH_ANN else "Traderz"
            logger.warning(
                "[GATE] ✅ CONSENSUS (%s + %s) | %s @ %.2f | size×%.2f",
                chat_name, partner_name, direction, entry_mid, size_mult,
            )

        # ── TP-ladder árszintek feloldása (most már a memo TELJES) ──────────
        ladder_trigger_price: Optional[float] = None
        ladder_dest_price: Optional[float] = None
        if ladder_active:
            trig_raw = _dist_to_raw(int(rule.ladder_trigger_tp_idx))
            dest_raw = _dist_to_raw(int(rule.ladder_dest_tp_idx))
            ladder_trigger_price = tp_memo[trig_raw]
            ladder_dest_price    = tp_memo[dest_raw]
            logger.info(
                "[GATE] LADDER feloldva (%s %s) | trigger TP%d(d=%d) @ %.2f → "
                "dest TP%d(d=%d) @ %.2f",
                chat_name, direction, trig_raw, rule.ladder_trigger_tp_idx,
                ladder_trigger_price, dest_raw, rule.ladder_dest_tp_idx,
                ladder_dest_price,
            )

        # Target raw tp_idx (1-based, price-ascending) — a CSV-hez és a
        # downstream logikához. Direction-függő konverzió a distance-rank-ból:
        #   BUY:  raw = distance + 1
        #   SELL: raw = n_tp - distance
        # (a rule.tp_idx_only garantáltan nem None, mert idáig eljutottunk)
        if direction.upper() == "BUY":
            target_raw_tp_idx = int(rule.tp_idx_only) + 1
        else:
            target_raw_tp_idx = int(rule.expected_tp_count) - int(rule.tp_idx_only)

        return GateOutput(
            accept=True,
            reason="ok",
            direction=direction,
            zone_low=zone_low, zone_high=zone_high,
            tp=tp, sl=sl,
            size_mult=size_mult,
            is_consensus=is_cons,
            consensus_partner=partner_name,
            ladder_trigger_price=ladder_trigger_price,
            ladder_dest_price=ladder_dest_price,
            target_raw_tp_idx=target_raw_tp_idx,
        )

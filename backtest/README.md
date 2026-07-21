# backtest/

Közös backtest- és (későbbi) optimalizáló framework két stratégiához:
- **`trend_reversal`** — EMA9/21 + RSI + ATR + tick imbalance scalper (a `capital_multi_instrument_bot_v2.py` logikája).
- **`range_scalp`** — ADX + BB squeeze + rolling S/R mean-reversion (a `range_scalping/core/engine.py` logikája).

Mindkettő ugyanazon az adat- és logger-rétegen fut, közös motorral. A live bot **későbbi iterációban** áll át ugyanezekre a `strategies/` modulokra.

## Struktúra

```
backtest/
├── data/
│   ├── tick_store.py        # parquet betöltés, oszlop-normalizálás
│   ├── gap_detector.py      # használható szegmensek (adaptív gap-küszöb)
│   └── candle_builder.py    # tick → MTF candle (csak szegmenseken belül)
├── indicators/
│   └── candle_indicators.py # EMA, RSI, ATR, ADX, BB, range S/R
├── strategies/
│   ├── base.py              # Strategy ABC + Decision/Signal típusok
│   ├── trend_reversal.py
│   └── range_scalp.py
├── engine/
│   ├── position.py          # Position dataclass + SL/TP/timeout exit
│   ├── metrics.py
│   └── runner.py            # szegmens-iterátor backtest motor
├── runlog/
│   └── run_logger.py        # közös Logger (backtest + future live)
├── configs/
│   ├── trend_reversal.yaml
│   └── range_scalp.yaml
└── run_backtest.py          # CLI belépési pont
```

## Futtatás

```bash
# Trend/reversal scalper a meglévő GOLD parquet-en
python -m backtest.run_backtest --config backtest/configs/trend_reversal.yaml

# Range scalper ugyanazon
python -m backtest.run_backtest --config backtest/configs/range_scalp.yaml

# Override más parquet-tel / epic-kel
python -m backtest.run_backtest \
  --config backtest/configs/trend_reversal.yaml \
  --data data/tick_data_GOLD.parquet \
  --epic GOLD
```

Az alapértelmezett tick-forrás a config-ban van (`data.tick_parquet`). A `tick_logger.py` által írt
`data/tick_data_<EPIC>.parquet` fájl a természetes input.

## Adat-szegmensek (gap kezelés)

A motor a tick stream-et **folyamatos szegmensekre** bontja a `gap_detector`-ral:
- A két szomszédos tick közötti `gap > max_gap_factor × candle_tf` → új szegmens kezdődik.
- A `min_segment_duration`-nál rövidebb szegmenseket eldobja (a stratégia warmup-jához nincs elég candle).
- Szegmens végén minden nyitott pozíció `SEGMENT_END` reason-nel kényszer-zárul.

Így a backtest csak olyan időablakokon fut, amelyek tényleg folyamatos adatot tartalmaznak —
éjszakai zárások, hétvégék, kollektor-leállások nem okoznak hibás eredményt.

## Run-mappa kimenet

Minden futás a `runs/<timestamp>_backtest_<strategy>_<epic>/` mappába kerül:

```
runs/20260520_103011_backtest_trend_reversal_GOLD/
├── config.yaml      # az adott futás config-jának snapshot-ja (ami történt, az reproducible)
├── events.jsonl     # minden event 1 sor JSON-ként (decision/open/close/segment)
├── decisions.csv    # tick-szintű döntések (allow_trade, indicator snapshot, reason)
├── trades.csv       # lezárt trade-ek (entry/exit, pnl, hold, exit_reason)
├── segments.csv     # mely tick-szegmenseken futott
├── metrics.json     # összesített metrikák
└── run.log          # human-readable szöveges log
```

A három mező, ami **összeköti** az eseményeket:
- `decision_event_id` — minden Decision-höz egyedi event_id; az `open` és `close` is hivatkozik rá.
- `open_event_id` — a `close` ezen keresztül kötődik a nyitáshoz.
- `trade_id` — sorszám a futáson belül.

Így utólag minden `close` mellé vissza lehet keresni a teljes indikátor-snapshot-ot, ami alapján
a `decision` allow_trade-et adott — pont ahhoz, hogy a "miért nyíltunk meg / miért zárt vesztesen"
kérdést elemezni lehessen.

## Stratégia interface

```python
class Strategy(ABC):
    name: str

    def required_timeframes(self) -> List[str]: ...
    def on_segment_start(self, ctx: StrategyContext) -> None: ...
    def on_tick(self, ts, bid, ask) -> Optional[Decision]: ...
    def on_segment_end(self) -> None: ...
```

- `on_segment_start` — itt számolja elő a stratégia vektorizáltan a candle indikátorokat
  az adott szegmensre. Tick-szinten csak lookup van.
- `on_tick` — `Decision` (érdemleges esemény) vagy `None` (nem logolt) a kimenete.
  Ha `allow_trade=True`, a motor a Decision-ből Signal-t épít és nyit pozíciót.

Új stratégia hozzáadása: implementáld a Strategy-t, regisztráld a `run_backtest.py`
`STRATEGY_REGISTRY`-ben, írj hozzá YAML config-ot.

## Live bot integráció (későbbi)

A `Strategy.on_tick(...)` interface azonos szignatúrával fog futni a live bot oldalon is.
A `RunLogger` ugyanaz a két oldalon, ezért az events.jsonl + decisions.csv + trades.csv séma
megegyezik backtestben és élesben → utólag bármilyen elemzést ugyanazon a formátumon lehet írni.
A `capital_multi_instrument_bot_v2.py` átállítása következő iteráció.

## Konfig — kulcs paraméterek

### `engine` (közös)

| paraméter | mit jelent |
|-----------|-----------|
| `candle_tf` | a gap-detektor és a min-segment vonatkoztatása erre megy (a fő LTF) |
| `max_gap_factor` | tick-hézag küszöb = `max_gap_factor × candle_tf` |
| `min_segment_duration` | az ennél rövidebb szegmenseket eldobja |
| `max_open_positions` | egyidejűleg nyitható pozíciók |
| `allow_multiple_directions` | BUY és SELL egyszerre lehet-e |
| `max_hold_seconds` | TIMEOUT exit, `null` = nincs |
| `slippage` | $ csúszás belépéskor |

### `strategy_params` — `trend_reversal`

A `configs/trend_reversal.yaml` minden mezője a `TrendReversal.__init__`-ben olvasott
név egy az egyben (EMA-k, RSI küszöbök, tick buffer, score threshold stb.).

### `strategy_params` — `range_scalp`

A `configs/range_scalp.yaml` mezői a `RangeScalp.__init__`-ben olvasottak (ADX/BB beállítás,
range_lookback, entry_buffer, tick velocity / spread z-küszöbök, SL/TP $-ban).
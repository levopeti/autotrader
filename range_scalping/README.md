# XAUUSD Range Scalper — Backtesting Framework

Tick-szintű, eseményvezérelt backtesting és Walk-Forward optimalizálási framework
arany (XAUUSD) mean-reversion scalping stratégiához.

## Telepítés

```bash
pip install -r requirements.txt
```

## Gyors indítás

```bash
# 1. Szintetikus adaton (azonnal futtatható)
python run_backtest.py --synthetic

# 2. Saját parquet adatokon
python run_backtest.py --data_dir ./data --sl 5.0 --tp 8.0

# 3. Walk-Forward optimalizálás
python run_optimize.py --data_dir ./data --n_trials 100 --n_splits 5

# 4. Szintetikus adaton optimalizálás (teszt)
python run_optimize.py --synthetic --n_trials 50 --n_splits 3
```

## Projekt struktúra

```
gold_scalper_backtest/
├── core/
│   ├── data_loader.py      — parquet betöltés, resample, szintetikus generátor
│   ├── indicators.py       — ADX, BB, ATR, RSI, tick velocity, spread z-score
│   └── engine.py           — tick-szintű backtesting motor + metrikák
├── optimization/
│   └── optimizer.py        — Optuna objective, WalkForwardOptimizer, GridSearch
├── reports/
│   └── reporter.py         — HTML riport + konzolos összefoglaló
├── utils/
│   ├── news_filter.py      — makroeseményszűrő (ForexFactory CSV)
│   └── position_sizer.py   — dinamikus lot-méretező
├── run_backtest.py          — egyszerű backtest futtatás
├── run_optimize.py          — Walk-Forward + Optuna optimalizálás
└── requirements.txt
```

## Parquet séma

Elvárt oszlopok (rugalmas auto-detektálás):
- `timestamp` / `time` / `datetime`  →  datetime64 UTC
- `bid`, `ask`  VAGY  `price` / `close`
- `volume`  (opcionális)

## Optimalizálható paraméterek

| Paraméter          | Tartomány     | Leírás                        |
|--------------------|---------------|-------------------------------|
| sl_dollars         | 2.0 – 20.0    | Stop Loss ($)                 |
| tp_dollars         | 3.0 – 30.0    | Take Profit ($)               |
| adx_threshold      | 15.0 – 40.0   | ADX oldalazás küszöb          |
| bb_period          | 10 – 50       | Bollinger Band periódus       |
| bb_std             | 1.5 – 3.5     | Bollinger Band szórás-szorzó  |
| bb_squeeze_pct     | 0.10 – 0.60   | Squeeze érzékenység           |
| range_lookback     | 20 – 120      | Support/Resistance lookback   |
| entry_buffer       | 0.05 – 2.00   | Belépési buffer ($)           |
| tick_vel_max       | 0.5 – 15.0    | Max tick velocity             |
| spread_z_max       | 1.5 – 5.0     | Max spread z-score            |
| max_trade_dur_min  | 20 – 300      | Max tartási idő (perc)        |

## --metric opciók

- `sharpe_ratio`   — kockázattal súlyozott hozam (ajánlott)
- `sortino_ratio`  — csak negatív volatilitást bünteti
- `profit_factor`  — bruttó nyereség / bruttó veszteség
- `total_pnl`      — összes dollár nyereség
- `expectancy`     — trade-enkénti várható nyereség ($)
- `calmar_ratio`   — hozam / max drawdown

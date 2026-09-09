#!/usr/bin/env python3
"""
Batch 3 — szakirodalomból leszűrt jelöltek a 2 éves Dukascopy 1-min adaton.

1) TSMOM napi (Moskowitz/Ooi/Pedersen): sign(múlt N-nap hozam), pozíció-flip
   jelváltáskor, napi kötésű. N ∈ {20, 60, 120}. Long/short szimmetrikus.
2) Ázsia-range → London breakout: 00-07 UTC range, kitörés + buffer belépő
   07-13 UTC között, SL = range túloldal, exit 20:00 UTC (vagy SL).
   Max 1 trade/nap/irány-first-touch.

Költség: bid/ask spread a datából + extra slippage {0.10, 0.30} $/láb.
Értékelés: PnL/hó + féléves bontás (robusztusság).
"""
from __future__ import annotations
import numpy as np
import pandas as pd
from pathlib import Path

ROOT = Path(__file__).resolve().parent

print("Loading 2y Dukascopy XAU 1-min...")
df = pd.read_parquet(ROOT / 'data/dukascopy_1min_XAU_USD.parquet', columns=['bid', 'ask'])
df.index = pd.to_datetime(df.index, utc=True)
df['mid'] = (df.bid + df.ask) / 2
df = df.sort_index()
months = (df.index.max() - df.index.min()).days / 30
print(f"  {len(df):,} bar, {df.index.min().date()} → {df.index.max().date()} ({months:.0f} hó)")

halves = pd.date_range(df.index.min().normalize(), df.index.max(), freq='6MS')


def half_report(trades: pd.DataFrame) -> str:
    if len(trades) == 0:
        return "0 trade"
    t = trades.copy()
    t['half'] = pd.cut(t.entry_ts, bins=list(halves) + [df.index.max()])
    hp = t.groupby('half', observed=True).pnl.sum()
    pos = int((hp > 0).sum())
    return f"{pos}/{len(hp)} félév+  [" + ", ".join(f"{v:+.0f}" for v in hp) + "]"


def summarize(tag, trades, slip):
    if len(trades) == 0:
        print(f"  {tag}: 0 trade"); return None
    tr = trades.copy()
    tr['pnl'] = tr.pnl_gross - 2 * slip     # entry+exit láb slippage
    wins = tr[tr.pnl > 0]
    pf = wins.pnl.sum() / max(1e-9, -tr[tr.pnl < 0].pnl.sum())
    eq = tr.pnl.cumsum()
    dd = (eq - eq.cummax()).min()
    row = dict(tag=tag, slip=slip, n=len(tr), per_mo=round(len(tr)/months, 1),
               pnl=round(tr.pnl.sum(), 2), pnl_mo=round(tr.pnl.sum()/months, 2),
               wr=round(len(wins)/len(tr)*100), pf=round(pf, 2), dd=round(dd, 2),
               halves=half_report(tr))
    print(f"  {tag} slip{slip}: n={row['n']} ({row['per_mo']}/hó) "
          f"PnL=${row['pnl']:+9.2f} (${row['pnl_mo']:+7.2f}/hó) WR={row['wr']}% "
          f"PF={row['pf']} DD={row['dd']} | {row['halves']}")
    return row


results = []

# ═════════════════ 1) TSMOM napi ═════════════════
print("\n=== TSMOM napi (long/short, jelváltásos flip) ===")
daily = df['mid'].resample('1D').last().dropna()
daily_open_ask = df['ask'].resample('1D').first().dropna()
daily_open_bid = df['bid'].resample('1D').first().dropna()

for N in [20, 60, 120]:
    sig = np.sign(daily.pct_change(N)).shift(1)  # előző napi jel, ma reggeli belépő
    sig = sig.reindex(daily.index)
    trades = []
    pos = 0; entry_px = 0.0; entry_ts = None
    for day, s in sig.items():
        if np.isnan(s) or s == 0 or day not in daily_open_ask.index:
            continue
        if pos == 0:
            pos = int(s)
            entry_px = daily_open_ask[day] if pos > 0 else daily_open_bid[day]
            entry_ts = day
        elif int(s) != pos:
            exit_px = daily_open_bid[day] if pos > 0 else daily_open_ask[day]
            pnl = (exit_px - entry_px) * pos
            trades.append(dict(entry_ts=entry_ts, exit_ts=day, pnl_gross=pnl, side=pos))
            pos = int(s)
            entry_px = daily_open_ask[day] if pos > 0 else daily_open_bid[day]
            entry_ts = day
    tr = pd.DataFrame(trades)
    for slip in [0.10, 0.30]:
        r = summarize(f"TSMOM_N{N}", tr, slip)
        if r: results.append(r)

# ═════════════════ 2) Ázsia-range → London breakout ═════════════════
print("\n=== Ázsia-range (00-07 UTC) → London breakout ===")
d = df.copy()
d['date'] = d.index.date
d['hour'] = d.index.hour

# napi ATR proxy (előző 14 nap daily range átlaga)
drange = (df['mid'].resample('1D').max() - df['mid'].resample('1D').min()).dropna()
atr_d = drange.rolling(14).mean().shift(1)  # előző napokból

for buf_frac, trend_flt in [(0.05, False), (0.15, False), (0.05, True), (0.15, True)]:
    # trend filter: napi 20-nap EMA irányába csak
    ema20 = daily.ewm(span=20, adjust=False).mean()
    trend_dir = np.sign(daily - ema20).shift(1)  # tegnapi állapot
    trades = []
    for day, g in d.groupby('date'):
        day_ts = pd.Timestamp(day, tz='UTC')
        if day_ts not in atr_d.index or np.isnan(atr_d.get(day_ts, np.nan)):
            continue
        atr = atr_d[day_ts]
        asia = g[(g.hour >= 0) & (g.hour < 7)]
        sess = g[(g.hour >= 7) & (g.hour < 20)]
        if len(asia) < 60 or len(sess) < 60:
            continue
        hi, lo = asia['mid'].max(), asia['mid'].min()
        rng = hi - lo
        if rng <= 0 or rng > 1.2 * atr:   # túl széles Asia-range → skip (nem konszolidáció)
            continue
        buf = buf_frac * atr
        up_lvl, dn_lvl = hi + buf, lo - buf
        tdir = trend_dir.get(day_ts, 0) if trend_flt else 0
        pos = 0; entry_px = 0.0; entry_t = None; sl = 0.0
        exit_px = None; exit_t = None
        for t, row in sess.iterrows():
            if pos == 0:
                if row.ask >= up_lvl and (not trend_flt or tdir > 0):
                    pos, entry_px, entry_t, sl = 1, row.ask, t, lo
                elif row.bid <= dn_lvl and (not trend_flt or tdir < 0):
                    pos, entry_px, entry_t, sl = -1, row.bid, t, hi
            else:
                if pos > 0 and row.bid <= sl:
                    exit_px, exit_t = row.bid, t; break
                if pos < 0 and row.ask >= sl:
                    exit_px, exit_t = row.ask, t; break
        if pos != 0 and exit_px is None:   # session-close exit
            last = sess.iloc[-1]
            exit_px = last.bid if pos > 0 else last.ask
            exit_t = sess.index[-1]
        if pos != 0:
            pnl = (exit_px - entry_px) * pos
            trades.append(dict(entry_ts=entry_t, exit_ts=exit_t, pnl_gross=pnl, side=pos))
    tr = pd.DataFrame(trades)
    tag = f"LOB_buf{buf_frac}_{'trend' if trend_flt else 'notrend'}"
    for slip in [0.10, 0.30]:
        r = summarize(tag, tr, slip)
        if r: results.append(r)

out = pd.DataFrame(results)
out.to_csv(ROOT / 'algo_batch3_summary.csv', index=False)
print("\nMentve: algo_batch3_summary.csv")

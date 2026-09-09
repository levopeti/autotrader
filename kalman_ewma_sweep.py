#!/usr/bin/env python3
"""
Kalman EWMA-R paraméter-sweep 2 éves Dukascopy 1-min adaton (GOLD-SILVER).

Grid: EWMA_alpha {0.0002, 0.0005, 0.001, 0.002} × K_entry {1.5, 1.75, 2.0, 2.5}
K_exit=0.3 fix, max_hold=41h (élő setup-pal egyező).

Optimalizáció: Kalman egyszer fut alpha-nként, a K-küszöbök a z-sorozaton
gyors szimulációk.

Értékelés: NEM top-1 PnL, hanem:
  - féléves bontás (4 db 6-hónapos szakasz) → hány szakasz pozitív
  - max DD, PF, trade-frekvencia (élő-realitás: min ~4 trade/hó kell)
Kimenet: kalman_ewma_sweep.csv + top configok féléves bontása stdout-ra
"""
from __future__ import annotations
import numpy as np
import pandas as pd
from pathlib import Path

ROOT = Path(__file__).resolve().parent
SLIP = 0.10  # per láb; 4 láb-esemény/trade → 4×SLIP levonás (élő konvenció)

print("Loading Dukascopy 2y data...")
g = pd.read_parquet(ROOT / 'data/dukascopy_1min_XAU_USD.parquet', columns=['mid'])
s = pd.read_parquet(ROOT / 'data/dukascopy_1min_XAG_USD.parquet', columns=['mid'])
both = pd.DataFrame({'g': g['mid'], 'x': s['mid']}).dropna()
gold = both['g'].values; silver = both['x'].values; ts_arr = both.index
N = len(gold)
print(f"  {N:,} közös 1-min bar, {ts_arr.min()} → {ts_arr.max()}")

WARMUP = 500
W_SIM = 1000  # sim start index
K_EXIT = 0.3
MAX_HOLD = 41 * 60  # bar


def run_kalman_ewma(alpha_ewma: float):
    Xw = np.column_stack([np.ones(WARMUP), silver[:WARMUP]])
    b_init, *_ = np.linalg.lstsq(Xw, gold[:WARMUP], rcond=None)
    R = float(np.var(gold[:WARMUP] - Xw @ b_init))
    Q = np.diag([R * 1e-6, R * 1e-9])
    a = np.zeros(N); b = np.zeros(N)
    resid = np.zeros(N); sig = np.zeros(N)
    a[0], b[0] = b_init
    P = np.eye(2)
    for t in range(1, N):
        P = P + Q
        H = np.array([1.0, silver[t]])
        r_v = gold[t] - H @ np.array([a[t-1], b[t-1]])
        R = (1 - alpha_ewma) * R + alpha_ewma * (r_v ** 2)
        S_v = float(H @ P @ H.T + R)
        K = P @ H.T / S_v
        a[t] = a[t-1] + K[0] * r_v
        b[t] = b[t-1] + K[1] * r_v
        P = (np.eye(2) - np.outer(K, H)) @ P
        resid[t] = r_v; sig[t] = np.sqrt(S_v)
    return b, resid, sig


def sim(z, beta, K_ent):
    trades = []
    pos = None
    for i in range(W_SIM, N):
        zi = z[i]
        if pos is None:
            if zi < -K_ent:
                pos = ('long', i, gold[i], silver[i], beta[i])
            elif zi > K_ent:
                pos = ('short', i, gold[i], silver[i], beta[i])
        else:
            side, ei, eg, es, eb = pos
            if abs(zi) < K_EXIT or (i - ei) >= MAX_HOLD:
                if side == 'long':
                    pnl = (gold[i] - eg) - eb * (silver[i] - es)
                else:
                    pnl = (eg - gold[i]) - eb * (es - silver[i])
                pnl -= 4 * SLIP
                trades.append((ts_arr[ei], ts_arr[i], pnl, i - ei, side))
                pos = None
    return pd.DataFrame(trades, columns=['entry_ts', 'exit_ts', 'pnl', 'hold_bars', 'side'])


ALPHAS = [0.0002, 0.0005, 0.001, 0.002]
K_ENTRIES = [1.5, 1.75, 2.0, 2.5]

# Féléves szakaszok
period_edges = pd.date_range(ts_arr.min().normalize(), ts_arr.max(), freq='6MS')
rows = []
trade_store = {}
for alpha in ALPHAS:
    print(f"\nKalman fut: alpha={alpha} ...", flush=True)
    beta, resid, sig = run_kalman_ewma(alpha)
    z = np.divide(resid, sig, out=np.zeros_like(resid), where=sig > 0)
    for K_ent in K_ENTRIES:
        tr = sim(z, beta, K_ent)
        tag = f"a{alpha}_K{K_ent}"
        trade_store[tag] = tr
        if len(tr) == 0:
            rows.append({'alpha': alpha, 'K': K_ent, 'n': 0, 'pnl': 0, 'wr': 0,
                         'pf': 0, 'dd': 0, 'tr_per_mo': 0, 'pos_halves': 0, 'n_halves': 0})
            continue
        months = (ts_arr.max() - ts_arr.min()).days / 30
        wins = tr[tr.pnl > 0]; losses = tr[tr.pnl < 0]
        pf = wins.pnl.sum() / max(1e-9, -losses.pnl.sum())
        eq = tr.pnl.cumsum()
        dd = (eq - eq.cummax()).min()
        # féléves bontás
        tr2 = tr.copy()
        tr2['half'] = pd.cut(tr2.entry_ts, bins=list(period_edges) + [ts_arr.max()])
        half_pnl = tr2.groupby('half', observed=True).pnl.sum()
        pos_halves = int((half_pnl > 0).sum()); n_halves = len(half_pnl)
        rows.append({
            'alpha': alpha, 'K': K_ent, 'n': len(tr),
            'pnl': round(tr.pnl.sum(), 2),
            'pnl_per_mo': round(tr.pnl.sum() / months, 2),
            'wr': round(len(wins) / len(tr) * 100, 0),
            'pf': round(pf, 2), 'dd': round(dd, 2),
            'tr_per_mo': round(len(tr) / months, 1),
            'pos_halves': pos_halves, 'n_halves': n_halves,
        })
        r = rows[-1]
        print(f"  K={K_ent}: n={r['n']:>4} ({r['tr_per_mo']}/hó) PnL=${r['pnl']:+9.2f} "
              f"(${r['pnl_per_mo']:+.2f}/hó) WR={r['wr']:.0f}% PF={r['pf']} "
              f"DD={r['dd']} félévek+: {pos_halves}/{n_halves}", flush=True)

df = pd.DataFrame(rows)
df.to_csv(ROOT / 'kalman_ewma_sweep.csv', index=False)
print("\n=== TELJES GRID (pnl_per_mo szerint) ===")
print(df.sort_values('pnl_per_mo', ascending=False).to_string(index=False))

# Robusztus jelöltek: minden félév pozitív ÉS DD > -150 ÉS >= 3 trade/hó
rob = df[(df.pos_halves == df.n_halves) & (df.dd > -150) & (df.tr_per_mo >= 3)]
print(f"\n=== ROBUSZTUS jelöltek (minden félév+, DD>-150, >=3 trade/hó): {len(rob)} ===")
if len(rob):
    print(rob.sort_values('pnl_per_mo', ascending=False).to_string(index=False))
    best_tag = f"a{rob.iloc[0]['alpha']}_K{rob.iloc[0]['K']}"
    tr = trade_store[best_tag]
    tr['month'] = tr.entry_ts.dt.to_period('M')
    print(f"\n=== LEGJOBB robusztus ({best_tag}) havi bontás ===")
    print(tr.groupby('month').agg(n=('pnl', 'count'), pnl=('pnl', 'sum')).round(2).to_string())

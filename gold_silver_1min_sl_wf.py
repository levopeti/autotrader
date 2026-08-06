#!/usr/bin/env python3
"""
Kalman 1-min TF walk-forward — HARD DOLLAR STOP-LOSS variánsok.

Egy Kalman fut, aztán minden (config × sl_dollar) párra újraszimuláljuk a
trade-eket a Kalman residuál/sigma-sorozat felett. SL a live-ben használt
`current_pnl` képlettel egyezik (slip-free intra-hold, slip csak exit-kor).
"""
import numpy as np
import pandas as pd
from pathlib import Path

SLIP = 0.10

gold = pd.read_parquet('data/tick_data_GOLD.parquet', columns=['timestamp_utc','bid','ask'])
silver = pd.read_parquet('data/tick_data_SILVER.parquet', columns=['timestamp_utc','bid','ask'])
for df in [gold, silver]:
    df['mid'] = (df['bid'] + df['ask']) / 2
    df['timestamp_utc'] = pd.to_datetime(df['timestamp_utc'], utc=True)

g1 = gold.set_index('timestamp_utc')['mid'].resample('1min').last().dropna()
s1 = silver.set_index('timestamp_utc')['mid'].resample('1min').last().dropna()
both = pd.DataFrame({'g': g1, 'x': s1}).dropna()
g = both['g'].values; x = both['x'].values; ts_arr = both.index
N = len(g)

# Kalman fut egyszer (fix R, ugyanaz mint gold_silver_1min_wf.py)
warmup = 500
Xw = np.column_stack([np.ones(warmup), x[:warmup]])
b_init, *_ = np.linalg.lstsq(Xw, g[:warmup], rcond=None)
R = float(np.var(g[:warmup] - Xw @ b_init))
Q = np.diag([R*1e-6, R*1e-9])

alpha = np.zeros(N); bx = np.zeros(N)
resid = np.zeros(N); sig = np.zeros(N)
alpha[0], bx[0] = b_init
P = np.eye(2)
for t in range(1, N):
    P = P + Q
    H = np.array([1.0, x[t]])
    r_v = g[t] - H @ np.array([alpha[t-1], bx[t-1]])
    S_v = float(H @ P @ H.T + R); K = P @ H.T / S_v
    alpha[t] = alpha[t-1] + K[0] * r_v
    bx[t] = bx[t-1] + K[1] * r_v
    P = (np.eye(2) - np.outer(K, H)) @ P
    resid[t] = r_v; sig[t] = np.sqrt(S_v)

w = 1000
z_arr = resid[w:] / sig[w:]
sim_start = ts_arr[w]
print(f"1-min bars: {N:,}, R auto-cal: {R:.2f}, sim indul: {sim_start}\n")


def sim(K_ent, K_exit, N_confirm, sl_dollar=None, max_h_bars=int(41*60)):
    """sl_dollar=None → nincs SL; egyébként ha unrealized_pnl < -sl_dollar → STOP_LOSS."""
    trades = []; pos = None
    streak_dir = None; streak_count = 0
    for i in range(w, N):
        zi = z_arr[i - w]
        current_dir = 'long' if zi < -K_ent else 'short' if zi > K_ent else None
        if current_dir is None:
            streak_dir = None; streak_count = 0
        elif current_dir == streak_dir:
            streak_count += 1
        else:
            streak_dir = current_dir; streak_count = 1

        if pos is None:
            if streak_count >= N_confirm:
                pos = (streak_dir, i, g[i], x[i], bx[i], ts_arr[i])
                streak_count = 0
        else:
            side, ei, eg, ex_, ebx, ets = pos
            # slip-free intra-hold PnL (live current_pnl-lel egyező képlet)
            if side == 'long':
                cur_pnl = (g[i] - eg) - ebx * (x[i] - ex_)
            else:
                cur_pnl = (eg - g[i]) - ebx * (ex_ - x[i])

            exit_reason = None
            if sl_dollar is not None and cur_pnl < -sl_dollar:
                exit_reason = 'SL'
            elif abs(zi) < K_exit:
                exit_reason = 'MR'
            elif (i - ei) >= max_h_bars:
                exit_reason = 'TO'

            if exit_reason:
                pnl_gross = cur_pnl
                pnl = pnl_gross - 4 * SLIP  # 2 leg × 2 side slip on exit accounting
                trades.append({
                    'entry_ts': ets, 'exit_ts': ts_arr[i],
                    'pnl': pnl, 'hold_min': i - ei,
                    'side': side, 'exit_reason': exit_reason,
                })
                pos = None; streak_dir = None; streak_count = 0
    return pd.DataFrame(trades)


# WF ablakok
windows = []
cur = sim_start
if cur.tz is None:
    cur = cur.tz_localize('UTC')
end = ts_arr[-1]
while cur + pd.Timedelta(days=10) <= end:
    windows.append((cur, cur + pd.Timedelta(days=10)))
    cur += pd.Timedelta(days=5)
print(f"WF ablakok: {len(windows)} (10-nap × 5-nap step)\n")


# A KÉT ÉLES CONFIG: 1min live (K2.0/x0.5/N1), 5min base rokon (K2.5/x0.3/N1)
CONFIGS = [
    ("1min_live__K2.0_x0.5_N1", 2.0, 0.5, 1),
    ("base_ish__K2.5_x0.3_N1",  2.5, 0.3, 1),
]

SL_LEVELS = [None, 100, 75, 50, 30]  # None = nincs SL (baseline)

print("=" * 130)
print(f"{'config':<28}{'SL':>6}{'trades':>8}{'pos_w%':>8}"
      f"{'sum_PnL':>10}{'per_mo':>10}{'worst_w':>10}{'best_w':>10}"
      f"{'max_DD':>10}{'PF':>8}{'wr%':>6}{'SL/MR/TO':>16}")
print("-" * 130)

all_results = {}
for name, K_ent, K_exit, N_c in CONFIGS:
    for sl in SL_LEVELS:
        tr = sim(K_ent, K_exit, N_c, sl_dollar=sl)
        if len(tr) == 0:
            continue
        tr['entry_ts'] = pd.to_datetime(tr['entry_ts'], utc=True)
        # Per-window bucket
        win_pnls, win_dds = [], []
        for a, b_ in windows:
            sub = tr[(tr.entry_ts >= a) & (tr.entry_ts < b_)]
            if len(sub) == 0:
                win_pnls.append(0); win_dds.append(0); continue
            eq = sub.pnl.cumsum()
            win_pnls.append(sub.pnl.sum())
            win_dds.append((eq - eq.cummax()).min())
        # Aggregate
        gp = tr[tr.pnl > 0].pnl.sum()
        gl = -tr[tr.pnl < 0].pnl.sum()
        pf = gp / gl if gl > 1e-6 else float('inf')
        wr = (tr.pnl > 0).mean() * 100
        pos_w = sum(1 for p in win_pnls if p > 0) / len(win_pnls) * 100
        exit_counts = tr.exit_reason.value_counts().to_dict()
        exit_str = f"{exit_counts.get('SL',0)}/{exit_counts.get('MR',0)}/{exit_counts.get('TO',0)}"
        sl_str = f"${sl}" if sl else "none"
        pf_s = f"{pf:.2f}" if pf != float('inf') else "inf"
        per_mo = sum(win_pnls) / len(win_pnls) * 3  # 10-nap ablak * 3 ≈ hó

        print(f"  {name:<26}{sl_str:>6}{len(tr):>8}{pos_w:>7.0f}%"
              f"{sum(win_pnls):>+10.2f}{per_mo:>+10.2f}"
              f"{min(win_pnls):>+10.2f}{max(win_pnls):>+10.2f}"
              f"{min(win_dds):>+10.2f}{pf_s:>8}{wr:>5.0f}%{exit_str:>16}")
        all_results[(name, sl)] = {
            'trades': tr, 'win_pnls': win_pnls, 'win_dds': win_dds,
            'pf': pf, 'wr': wr, 'pos_w': pos_w, 'per_mo': per_mo,
        }
    print()

# Fókusz: SL benefit-cost a live 1min configon
print("\n=== FÓKUSZ: 1min_live config — SL benefit vs cost ===\n")
base = all_results.get(('1min_live__K2.0_x0.5_N1', None))
if base:
    b_pnl = sum(base['win_pnls']); b_dd = min(base['win_dds'])
    print(f"Baseline (no SL): PnL {b_pnl:+.2f}  worst_window {min(base['win_pnls']):+.2f}  "
          f"max_DD {b_dd:+.2f}  PF {base['pf']:.2f}\n")
    for sl in [100, 75, 50, 30]:
        r = all_results.get(('1min_live__K2.0_x0.5_N1', sl))
        if not r: continue
        pnl = sum(r['win_pnls']); dd = min(r['win_dds'])
        pnl_diff = pnl - b_pnl
        dd_diff = dd - b_dd  # kisebb negatív = jobb
        pnl_diff_pct = (pnl_diff / b_pnl * 100) if b_pnl != 0 else 0
        dd_impr_pct = (1 - dd/b_dd) * 100 if b_dd != 0 else 0
        print(f"SL=${sl}: PnL {pnl:+.2f} ({pnl_diff:+.2f}, {pnl_diff_pct:+.0f}%)  "
              f"worst_w {min(r['win_pnls']):+.2f}  max_DD {dd:+.2f} ({dd_impr_pct:+.0f}% jobb)  "
              f"PF {r['pf']:.2f}")

print("\n=== FÓKUSZ: base_ish config — SL benefit vs cost ===\n")
base = all_results.get(('base_ish__K2.5_x0.3_N1', None))
if base:
    b_pnl = sum(base['win_pnls']); b_dd = min(base['win_dds'])
    print(f"Baseline (no SL): PnL {b_pnl:+.2f}  worst_window {min(base['win_pnls']):+.2f}  "
          f"max_DD {b_dd:+.2f}  PF {base['pf']:.2f}\n")
    for sl in [100, 75, 50, 30]:
        r = all_results.get(('base_ish__K2.5_x0.3_N1', sl))
        if not r: continue
        pnl = sum(r['win_pnls']); dd = min(r['win_dds'])
        pnl_diff = pnl - b_pnl
        dd_diff = dd - b_dd
        pnl_diff_pct = (pnl_diff / b_pnl * 100) if b_pnl != 0 else 0
        dd_impr_pct = (1 - dd/b_dd) * 100 if b_dd != 0 else 0
        print(f"SL=${sl}: PnL {pnl:+.2f} ({pnl_diff:+.2f}, {pnl_diff_pct:+.0f}%)  "
              f"worst_w {min(r['win_pnls']):+.2f}  max_DD {dd:+.2f} ({dd_impr_pct:+.0f}% jobb)  "
              f"PF {r['pf']:.2f}")

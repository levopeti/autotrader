#!/usr/bin/env python3
"""
EWMA-R vs fix-R összehasonlítás egyazon 2-éves adaton.
Trade frequency, PnL, MAE eloszlás, DD.
"""
import numpy as np
import pandas as pd

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


def run_kalman(fix_R=None, ewma_alpha=None):
    """fix_R megadva → fix R; ewma_alpha megadva → EWMA-R adaptive."""
    warmup = 500
    Xw = np.column_stack([np.ones(warmup), x[:warmup]])
    b_init, *_ = np.linalg.lstsq(Xw, g[:warmup], rcond=None)
    R0 = float(np.var(g[:warmup] - Xw @ b_init))
    Q = np.diag([R0*1e-6, R0*1e-9])

    R = R0
    R_arr = np.zeros(N); R_arr[0] = R0
    alpha = np.zeros(N); bx = np.zeros(N)
    resid = np.zeros(N); sig = np.zeros(N)
    alpha[0], bx[0] = b_init
    P = np.eye(2)
    for t in range(1, N):
        P = P + Q
        H = np.array([1.0, x[t]])
        r_v = g[t] - H @ np.array([alpha[t-1], bx[t-1]])
        # EWMA update BEFORE using R (like the live logic: use current R for this step's z)
        if ewma_alpha is not None:
            R = (1 - ewma_alpha) * R + ewma_alpha * (r_v ** 2)
        R_arr[t] = R
        S_v = float(H @ P @ H.T + R); K = P @ H.T / S_v
        alpha[t] = alpha[t-1] + K[0] * r_v
        bx[t] = bx[t-1] + K[1] * r_v
        P = (np.eye(2) - np.outer(K, H)) @ P
        resid[t] = r_v; sig[t] = np.sqrt(S_v)
    return alpha, bx, resid, sig, R_arr


def sim(z_arr, ts_arr_sim, bx_sim, g_sim, x_sim, w_start,
        K_ent, K_exit, N_confirm, max_h_bars=int(41*60)):
    trades = []; pos = None
    streak_dir = None; streak_count = 0
    mae = None
    for i in range(w_start, len(g_sim)):
        zi = z_arr[i - w_start]
        current_dir = 'long' if zi < -K_ent else 'short' if zi > K_ent else None
        if current_dir is None:
            streak_dir = None; streak_count = 0
        elif current_dir == streak_dir:
            streak_count += 1
        else:
            streak_dir = current_dir; streak_count = 1

        if pos is None:
            if streak_count >= N_confirm:
                pos = (streak_dir, i, g_sim[i], x_sim[i], bx_sim[i], ts_arr_sim[i])
                streak_count = 0
                mae = 0.0
        else:
            side, ei, eg, ex_, ebx, ets = pos
            if side == 'long':
                cur = (g_sim[i] - eg) - ebx * (x_sim[i] - ex_)
            else:
                cur = (eg - g_sim[i]) - ebx * (ex_ - x_sim[i])
            mae = min(mae, cur)
            if abs(zi) < K_exit or (i - ei) >= max_h_bars:
                pnl = cur - 4 * SLIP
                trades.append({
                    'entry_ts': ets, 'exit_ts': ts_arr_sim[i],
                    'pnl': pnl, 'hold_min': i - ei, 'side': side, 'mae': mae,
                })
                pos = None; streak_dir = None; streak_count = 0
    return pd.DataFrame(trades)


# ── FIX R (live 1min config: K=2.0, x=0.5, N=1) ──
alpha_f, bx_f, resid_f, sig_f, R_f = run_kalman(fix_R=True)
z_fix = resid_f[1000:] / sig_f[1000:]
tr_fix = sim(z_fix, ts_arr[1000:], bx_f[1000:], g[1000:], x[1000:], 0,
             K_ent=2.0, K_exit=0.5, N_confirm=1)
tr_fix['entry_ts'] = pd.to_datetime(tr_fix['entry_ts'], utc=True)

# ── EWMA (live ewma config: α=0.0005, K=3.0, x=0.3, N=1) ──
alpha_e, bx_e, resid_e, sig_e, R_e = run_kalman(ewma_alpha=0.0005)
z_ewma = resid_e[1000:] / sig_e[1000:]
tr_ewma = sim(z_ewma, ts_arr[1000:], bx_e[1000:], g[1000:], x[1000:], 0,
              K_ent=3.0, K_exit=0.3, N_confirm=1)
tr_ewma['entry_ts'] = pd.to_datetime(tr_ewma['entry_ts'], utc=True)


def summarize(tr, label):
    if len(tr) == 0:
        print(f"===== {label}: 0 trade =====\n")
        return
    total_pnl = tr.pnl.sum()
    wins = tr[tr.pnl > 0]; losses = tr[tr.pnl < 0]
    pf = wins.pnl.sum() / -losses.pnl.sum() if len(losses) else float('inf')
    wr = len(wins) / len(tr) * 100
    days = (tr.exit_ts.max() - tr.entry_ts.min()).total_seconds() / 86400
    per_mo = total_pnl / (days / 30)
    trades_per_mo = len(tr) / (days / 30)
    # Equity curve DD
    eq = tr.sort_values('entry_ts').pnl.cumsum().values
    dd_curve = eq - np.maximum.accumulate(eq)
    max_dd = dd_curve.min()
    # Avg hold
    avg_hold_h = tr.hold_min.mean() / 60

    print(f"===== {label} =====")
    print(f"  Trade szám:       {len(tr):>7}   ({trades_per_mo:.1f} trade/hó)")
    print(f"  Össz PnL:         {total_pnl:>+7.2f}$  ({per_mo:+.2f}$/hó)")
    print(f"  Win rate:         {wr:>7.0f}%   PF: {pf:.2f}")
    print(f"  Max realizált DD: {max_dd:>+7.2f}$")
    print(f"  Átlag hold:       {avg_hold_h:>7.1f}h")
    print(f"  Worst MAE:        {tr.mae.min():>+7.2f}$")
    print(f"  Winners MAE 95%:  {np.percentile(wins.mae, 5):>+7.2f}$  100%: {wins.mae.min():+.2f}$")
    print()


summarize(tr_fix,  "fix-R  (1min live: K=2.0, x=0.5, N=1)")
summarize(tr_ewma, "EWMA-R (live: α=0.0005, K=3.0, x=0.3, N=1)")

# Havi bontás egymás mellett
print("\n===== HAVI BONTÁS =====\n")
for tr, label in [(tr_fix, 'fix-R'), (tr_ewma, 'EWMA')]:
    tr['month'] = tr.entry_ts.dt.to_period('M')
    m = tr.groupby('month').agg(n=('pnl','count'), pnl=('pnl','sum'), worst_mae=('mae','min')).round(2)
    print(f"--- {label} ---")
    print(m.to_string())
    print()

# EWMA + z=2 (agresszívebb ent) mint kompromisszum?
print("\n===== EWMA-R agresszívebb küszöbökkel (K=2.0-2.5) =====\n")
for K in [2.0, 2.25, 2.5]:
    tr_e2 = sim(z_ewma, ts_arr[1000:], bx_e[1000:], g[1000:], x[1000:], 0,
                K_ent=K, K_exit=0.3, N_confirm=1)
    tr_e2['entry_ts'] = pd.to_datetime(tr_e2['entry_ts'], utc=True)
    summarize(tr_e2, f"EWMA-R (K={K}, x=0.3, N=1)")

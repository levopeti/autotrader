#!/usr/bin/env python3
"""
MAE (max adverse excursion) analízis — mennyire mennek mélyre nyitva a
trade-ek a jelenlegi (SL nélküli) 1min live configon.
Egy SL épp azokat a trade-eket vágná le, ahol a MAE < -SL.
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


def sim_with_mae(K_ent, K_exit, N_confirm, max_h_bars=int(41*60)):
    trades = []; pos = None
    streak_dir = None; streak_count = 0
    mae = mfe = None
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
                mae = 0.0; mfe = 0.0
        else:
            side, ei, eg, ex_, ebx, ets = pos
            if side == 'long':
                cur = (g[i] - eg) - ebx * (x[i] - ex_)
            else:
                cur = (eg - g[i]) - ebx * (ex_ - x[i])
            mae = min(mae, cur)
            mfe = max(mfe, cur)

            if abs(zi) < K_exit or (i - ei) >= max_h_bars:
                pnl = cur - 4 * SLIP
                trades.append({
                    'entry_ts': ets, 'exit_ts': ts_arr[i],
                    'pnl': pnl, 'hold_min': i - ei,
                    'side': side, 'mae': mae, 'mfe': mfe,
                })
                pos = None; streak_dir = None; streak_count = 0
    return pd.DataFrame(trades)


for name, K_ent, K_exit, N_c in [
    ("1min_live__K2.0_x0.5_N1", 2.0, 0.5, 1),
    ("base_ish__K2.5_x0.3_N1",  2.5, 0.3, 1),
]:
    tr = sim_with_mae(K_ent, K_exit, N_c)
    print(f"\n===== {name}  ({len(tr)} trade) =====")
    tr['is_win'] = tr.pnl > 0
    print(f"Baseline: sum PnL {tr.pnl.sum():+.2f}, PF {tr[tr.pnl>0].pnl.sum()/-tr[tr.pnl<0].pnl.sum():.2f}, "
          f"WR {(tr.pnl>0).mean()*100:.0f}%")
    print()
    print("MAE eloszlás (mennyire ment mélyre nyitva):")
    for pct in [50, 75, 90, 95, 99, 100]:
        v = np.percentile(tr.mae, 100-pct)
        print(f"  {pct:>3}%-os trade legalább {v:+7.2f}$-ig ment lefelé")
    print(f"  Legmélyebb: {tr.mae.min():+.2f}$ (worst MAE)")
    print()
    print("Nyertes trade-ek MAE eloszlása (ezekben rejlik a fő PnL, ezt vágná le az SL):")
    wins = tr[tr.is_win]
    for pct in [50, 75, 90, 95, 99, 100]:
        v = np.percentile(wins.mae, 100-pct)
        print(f"  {pct:>3}%-os nyertes trade legalább {v:+7.2f}$-ig ment lefelé nyitva")
    print()
    print("SL simuláció — MI TÖRTÉNIK ha adott SL érvényes lett volna?")
    for sl in [30, 50, 75, 100, 150]:
        stopped = tr[tr.mae < -sl].copy()
        # Ezek a trade-ek a live PnL helyett -sl-t adnak (+slip)
        actual_pnl_stopped = stopped.pnl.sum()
        sl_pnl_stopped = -sl * len(stopped) - 4*SLIP*len(stopped)
        diff = sl_pnl_stopped - actual_pnl_stopped
        new_total = tr.pnl.sum() - actual_pnl_stopped + sl_pnl_stopped
        # Hány nyertes esne áldozatul
        wins_stopped = stopped.is_win.sum()
        loses_stopped = (~stopped.is_win).sum()
        wins_saved = (~stopped.is_win).sum()  # ezek amúgy is bukott, most kevesebbet buknak
        # Mennyit spórolt / vesztett dollárban
        lose_savings = -stopped[~stopped.is_win].pnl.sum() - sl*loses_stopped
        win_losses = stopped[stopped.is_win].pnl.sum() - (-sl*wins_stopped)
        print(f"  SL=${sl:>3}: {len(stopped):>3} trade stopolt "
              f"({wins_stopped}W→SL, {loses_stopped}L→SL), "
              f"nettó impact ${diff:+.2f}, új sum PnL ${new_total:+.2f}")

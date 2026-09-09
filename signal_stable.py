#!/usr/bin/env python3
"""
Konzervatív config-választás: min-PnL minden regime-ben pozitív alapon.

Módszer:
  1. Csatornánként megnézzük minden config regime-bontását
  2. Csak azokat tartjuk meg, amelyek MINDEN regime-ben pozitívak
  3. Ezek közül a "worst regime PnL" legmagasabb-t választjuk (max-min)
  4. Aztán walk-forward-on nézzük, hogy ez a config stabilabb-e mint a
     re-optimalizált top-1

Kimenet: stable_configs.csv + stable_wf.csv
"""
from __future__ import annotations
import pandas as pd
from pathlib import Path

ROOT = Path(__file__).resolve().parent

trades = pd.read_csv('signal_sweep_trades.csv')
regime = pd.read_csv('regime_daily.csv', index_col=0, parse_dates=True)
regime.index = pd.to_datetime(regime.index, utc=True)
regime['day'] = regime.index.date
regime_map = dict(zip(regime.day, regime.regime))

trades['entry_ts'] = pd.to_datetime(trades['entry_ts'], errors='coerce')
if trades['entry_ts'].dt.tz is None:
    trades['entry_ts'] = trades['entry_ts'].dt.tz_localize('UTC')
trades['day'] = trades['entry_ts'].dt.date
trades['regime'] = trades['day'].map(regime_map).fillna('unknown')
trades['is_win'] = trades['pnl'] > 0

# ──────────────────────────────────────────────────────
# 1. Konzervatív config-választás csatornánként
# ──────────────────────────────────────────────────────
print("=" * 100)
print("KONZERVATÍV CONFIG-VÁLASZTÁS  |  minden regime-ben pozitív, max-min PnL")
print("=" * 100)

stable_choice = {}
for ch in sorted(trades._channel.unique()):
    ch_tr = trades[trades._channel == ch]
    piv = ch_tr.groupby(['_config','regime']).pnl.sum().unstack(fill_value=0)
    # Filter: minden regime > 0
    regimes = [r for r in ['BULL_RALLY','BEAR_RALLY','SIDEWAYS'] if r in piv.columns]
    for r in regimes:
        piv[r] = piv[r].fillna(0)
    # Minden regime pozitív?
    piv['all_positive'] = piv[regimes].apply(lambda row: (row > 0).all(), axis=1)
    piv['min_regime'] = piv[regimes].min(axis=1)
    piv['sum_regime'] = piv[regimes].sum(axis=1)
    stable = piv[piv['all_positive']]
    if len(stable) == 0:
        print(f"\n{ch}: NINCS regime-robusztus config")
        continue
    # Max-min: legmagasabb worst-regime PnL (Kelly-jellegű konzervatív választás)
    best_by_maxmin = stable.sort_values('min_regime', ascending=False).iloc[0]
    best_cfg_maxmin = best_by_maxmin.name
    # Legmagasabb sum a robusztusok között
    best_by_sum = stable.sort_values('sum_regime', ascending=False).iloc[0]
    best_cfg_sum = best_by_sum.name
    print(f"\n{ch}  |  {len(stable)} regime-robusztus config a {len(piv)}-ből")
    print(f"  MAX-MIN választás (worst-regime legjobb): {best_cfg_maxmin}")
    print(f"    BULL={best_by_maxmin.get('BULL_RALLY',0):+.2f}, "
          f"BEAR={best_by_maxmin.get('BEAR_RALLY',0):+.2f}, "
          f"SIDE={best_by_maxmin.get('SIDEWAYS',0):+.2f}, "
          f"sum={best_by_maxmin['sum_regime']:+.2f}, min={best_by_maxmin['min_regime']:+.2f}")
    print(f"  SUM választás (össz legjobb, még robusztus): {best_cfg_sum}")
    print(f"    BULL={best_by_sum.get('BULL_RALLY',0):+.2f}, "
          f"BEAR={best_by_sum.get('BEAR_RALLY',0):+.2f}, "
          f"SIDE={best_by_sum.get('SIDEWAYS',0):+.2f}, "
          f"sum={best_by_sum['sum_regime']:+.2f}, min={best_by_sum['min_regime']:+.2f}")
    # Összes robusztus config listája
    print(f"  Mind a {len(stable)} regime-robusztus config:")
    print(stable[regimes + ['sum_regime','min_regime']].sort_values('min_regime', ascending=False).round(2).to_string())
    stable_choice[ch] = {'maxmin': best_cfg_maxmin, 'sum': best_cfg_sum}

# ──────────────────────────────────────────────────────
# 2. WF a stable configokon (nincs re-optimalizáció)
# ──────────────────────────────────────────────────────
print("\n\n" + "=" * 100)
print("WALK-FORWARD  |  fix stable config, nincs re-opt (4 ablak, 45d IS + 20d OOS)")
print("=" * 100)

# Ugyanaz az ablak-osztás mint signal_wf.py-ban
data_start = trades.entry_ts.min()
data_end = trades.entry_ts.max()
IS_DAYS, OOS_DAYS, STEP_DAYS = 45, 20, 15
windows = []
cur = data_start.normalize()
while True:
    is_end = cur + pd.Timedelta(days=IS_DAYS)
    oos_end = is_end + pd.Timedelta(days=OOS_DAYS)
    if oos_end > data_end: break
    windows.append((cur, is_end, oos_end))
    cur = cur + pd.Timedelta(days=STEP_DAYS)

rows = []
for choice_name in ['maxmin', 'sum']:
    print(f"\n─── {choice_name.upper()} választás (nincs re-opt) ─────────────────────")
    for i, (is_start, is_end, oos_end) in enumerate(windows, 1):
        for ch in sorted(stable_choice.keys()):
            cfg = stable_choice[ch][choice_name]
            ch_tr = trades[trades._channel == ch]
            oos_tr = ch_tr[(ch_tr._config == cfg) &
                           (ch_tr.entry_ts >= is_end) & (ch_tr.entry_ts < oos_end)]
            oos_pnl = oos_tr.pnl.sum()
            oos_n = len(oos_tr)
            oos_wr = (oos_tr.pnl > 0).mean() * 100 if oos_n > 0 else 0
            rows.append({
                'choice': choice_name, 'window': i, 'channel': ch,
                'cfg': cfg, 'oos_start': is_end.date(), 'oos_end': oos_end.date(),
                'oos_n': oos_n, 'oos_pnl': round(oos_pnl, 2),
                'oos_wr': round(oos_wr, 0),
            })
            print(f"  W{i} {ch}: OOS ${oos_pnl:+7.2f} ({oos_n:>3}tr, {oos_wr:.0f}% WR)")

df = pd.DataFrame(rows)
df.to_csv(ROOT / 'stable_wf.csv', index=False)

print("\n" + "=" * 100)
print("STABLE-CONFIG WF ÖSSZEGZÉS (nincs re-opt)")
print("=" * 100)
for choice in ['maxmin', 'sum']:
    print(f"\n{choice.upper()} választás:")
    sub = df[df.choice == choice]
    for ch in sorted(sub.channel.unique()):
        chs = sub[sub.channel == ch]
        oos_total = chs.oos_pnl.sum()
        oos_pos = (chs.oos_pnl > 0).sum()
        # 120d total (nincs re-opt)
        cfg = stable_choice[ch][choice]
        all_ch_tr = trades[(trades._channel == ch) & (trades._config == cfg)]
        total_pnl = all_ch_tr.pnl.sum()
        total_n = len(all_ch_tr)
        total_wr = (all_ch_tr.pnl > 0).mean() * 100
        print(f"  {ch:<10} → {cfg}")
        print(f"       OOS windows: ${oos_total:+.2f} ({oos_pos}/{len(chs)} pos)")
        print(f"       Teljes 120d (stabil cfg): ${total_pnl:+.2f}, {total_n} trade, {total_wr:.0f}% WR")

# Végső "portfolió" — mindhárom stable config együtt
print("\n" + "=" * 100)
print("PORTFOLIÓ — mindhárom stable config együtt (120d, nincs re-opt)")
print("=" * 100)
for choice in ['maxmin', 'sum']:
    print(f"\n{choice.upper()} választás:")
    tot_pnl = 0; tot_n = 0
    for ch in sorted(stable_choice.keys()):
        cfg = stable_choice[ch][choice]
        all_ch_tr = trades[(trades._channel == ch) & (trades._config == cfg)]
        tot_pnl += all_ch_tr.pnl.sum()
        tot_n += len(all_ch_tr)
    print(f"  Portfolio 120d: ${tot_pnl:+.2f}, {tot_n} trade → ~${tot_pnl/4:+.2f}/hó")

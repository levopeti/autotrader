#!/usr/bin/env python3
"""
Walk-forward validáció a signal_sweep_trades.csv-n.

Módszer:
  - Több IS/OOS split: az IS-en kiválasztjuk a top-1 configot (PnL szerint),
    aztán ugyanezzel a configgal megnézzük a következő OOS ablakot.
  - 30-napos OOS-ok, csúsztatott IS ablak.

Kimenet:
  - wf_summary.csv: config-választás + IS PnL + OOS PnL per ablak/csatorna
  - wf_stability.csv: mennyire konzisztensen ugyanaz a top-1 config
"""
from __future__ import annotations
import pandas as pd
from pathlib import Path

ROOT = Path(__file__).resolve().parent

trades = pd.read_csv('signal_sweep_trades.csv')
trades['entry_ts'] = pd.to_datetime(trades['entry_ts'], errors='coerce')
if trades['entry_ts'].dt.tz is None:
    trades['entry_ts'] = trades['entry_ts'].dt.tz_localize('UTC')
trades['day'] = trades['entry_ts'].dt.normalize()

# Csatorna szint: a _channel oszlop van
channels = sorted(trades._channel.unique())
print(f"Csatornák: {channels}")

# Adat span
data_start = trades.entry_ts.min()
data_end = trades.entry_ts.max()
print(f"Adat: {data_start.date()} → {data_end.date()}  "
      f"({(data_end-data_start).days} nap)")

# Walk-forward ablakok (45d IS + 20d OOS, 15d step)
IS_DAYS = 45
OOS_DAYS = 20
STEP_DAYS = 15

windows = []
cur = data_start.normalize()
while True:
    is_end = cur + pd.Timedelta(days=IS_DAYS)
    oos_end = is_end + pd.Timedelta(days=OOS_DAYS)
    if oos_end > data_end:
        break
    windows.append((cur, is_end, oos_end))
    cur = cur + pd.Timedelta(days=STEP_DAYS)
print(f"\nWF ablakok ({IS_DAYS}d IS + {OOS_DAYS}d OOS, step {STEP_DAYS}d): {len(windows)}\n")

# Minden ablakhoz: kiválasztjuk a top-1 configot IS-en, mérjük OOS-en
rows = []
config_choice_by_channel = {ch: [] for ch in channels}

for i, (is_start, is_end, oos_end) in enumerate(windows, 1):
    print(f"─── Ablak {i}: IS {is_start.date()}..{is_end.date()} → OOS {is_end.date()}..{oos_end.date()} ───")
    for ch in channels:
        ch_tr = trades[trades._channel == ch]
        is_tr = ch_tr[(ch_tr.entry_ts >= is_start) & (ch_tr.entry_ts < is_end)]
        oos_tr = ch_tr[(ch_tr.entry_ts >= is_end) & (ch_tr.entry_ts < oos_end)]
        # Config-onkénti IS PnL
        is_by_cfg = is_tr.groupby('_config').pnl.agg(['sum','count'])
        is_by_cfg = is_by_cfg[is_by_cfg['count'] >= 3]  # min 3 trade
        if len(is_by_cfg) == 0:
            print(f"  {ch}: IS nincs elég trade, skip")
            continue
        best_cfg = is_by_cfg['sum'].idxmax()
        is_best_pnl = is_by_cfg.loc[best_cfg, 'sum']
        is_n = int(is_by_cfg.loc[best_cfg, 'count'])
        # OOS a KIVÁLASZTOTT configon
        oos_sub = oos_tr[oos_tr._config == best_cfg]
        oos_pnl = oos_sub.pnl.sum()
        oos_n = len(oos_sub)
        oos_wr = (oos_sub.pnl > 0).mean() * 100 if oos_n > 0 else 0
        rows.append({
            'window': i, 'is_start': is_start.date(), 'oos_start': is_end.date(),
            'oos_end': oos_end.date(), 'channel': ch, 'best_cfg': best_cfg,
            'is_n': is_n, 'is_pnl': round(is_best_pnl, 2),
            'oos_n': oos_n, 'oos_pnl': round(oos_pnl, 2),
            'oos_wr': round(oos_wr, 0),
        })
        config_choice_by_channel[ch].append(best_cfg)
        print(f"  {ch}: BEST_IS={best_cfg[:35]:<35}  IS ${is_best_pnl:+7.2f} ({is_n:>3}tr)  →  OOS ${oos_pnl:+7.2f} ({oos_n:>3}tr, {oos_wr:.0f}% WR)")

df = pd.DataFrame(rows)
df.to_csv(ROOT / 'wf_summary.csv', index=False)

print("\n" + "=" * 100)
print("WF ÖSSZEGZÉS — csatornánként")
print("=" * 100)
for ch in channels:
    sub = df[df.channel == ch]
    if len(sub) == 0: continue
    is_sum = sub.is_pnl.sum()
    oos_sum = sub.oos_pnl.sum()
    oos_pos = (sub.oos_pnl > 0).sum()
    degradation = (1 - oos_sum/is_sum) * 100 if is_sum != 0 else 0
    print(f"\n{ch}:")
    print(f"  IS össz PnL:  ${is_sum:+.2f}")
    print(f"  OOS össz PnL: ${oos_sum:+.2f}  ({oos_pos}/{len(sub)} ablak pozitív)")
    print(f"  Degradáció:   {degradation:.0f}% (mennyivel rosszabb OOS)")
    # Config-válasz stabilitás
    configs = config_choice_by_channel[ch]
    if configs:
        top_choice = pd.Series(configs).value_counts()
        print(f"  Config-választás konzisztencia: {len(top_choice)} különböző config")
        print(f"    leggyakoribb: {top_choice.index[0]} ({top_choice.iloc[0]}/{len(configs)} ablakban)")

# Csatornánként a leggyakrabban választott config OOS-en összesítve
print("\n" + "=" * 100)
print("HA A LEGGYAKRABBAN VÁLASZTOTT CONFIG-OT HASZNÁLNÁNK VÉGIG (nincs re-optimalizáció)")
print("=" * 100)
for ch in channels:
    configs = config_choice_by_channel[ch]
    if not configs: continue
    top = pd.Series(configs).value_counts().index[0]
    all_ch_tr = trades[(trades._channel == ch) & (trades._config == top)]
    total_pnl = all_ch_tr.pnl.sum()
    total_n = len(all_ch_tr)
    total_wr = (all_ch_tr.pnl > 0).mean() * 100
    print(f"{ch:<10} → {top}")
    print(f"   ÖSSZ (120d) PnL ${total_pnl:+.2f}, {total_n} trade, {total_wr:.0f}% WR")

#!/usr/bin/env python3
"""
Donchian plató + walk-forward elemzés a plateau-grid trade-logjaiból.

1) Plató-tábla: period × trail PnL a teljes 2 éven (slip 0.30) — tű-hegy vs plató
2) Walk-forward RE-OPTIMALIZÁLÁSSAL: 6 hó IS → 2 hó OOS, 2 hó lépés.
   Minden ablakban az IS-legjobb (period, trail) megy OOS-ra.
3) Fix center (p20, tr2.0) OOS ugyanezeken az ablakokon — összevetés.
"""
from __future__ import annotations
import re
import numpy as np
import pandas as pd
from pathlib import Path

ROOT = Path(__file__).resolve().parent

# trade-logok betöltése a plateau.log-ból
runs = {}
for line in open(ROOT / 'plateau.log'):
    m = re.match(r"(\S+): n=(\S+) pnl=(\S+) pf=(\S+) dd=(\S+) trades=(\S+)", line.strip())
    if not m: continue
    tag = m.group(1)
    runs[tag] = dict(n=m.group(2), pnl=float(m.group(3)), pf=float(m.group(4)),
                     dd=float(m.group(5)), trades_csv=m.group(6))

print(f"{len(runs)} futás betöltve\n")

# ── 1) Plató-tábla (period × trail)
grid = {}
for tag, r in runs.items():
    m = re.match(r"p(\d+)_tr([\d.]+)$", tag)
    if m:
        grid[(int(m.group(1)), float(m.group(2)))] = r['pnl']
periods = sorted({k[0] for k in grid})
trails = sorted({k[1] for k in grid})
print("=== PLATÓ: period × trail — 2y PnL (slip 0.30) ===")
hdr = "period  " + "".join(f"trail={t:<10}" for t in trails)
print(hdr)
for p in periods:
    row = f"p={p:<5} "
    for t in trails:
        v = grid.get((p, t))
        row += f"{v:>+10.0f}  " if v is not None else f"{'—':>10}  "
    print(row)

# egydimenziós extrák
print("\n=== Center-szomszédok (p20, trail2.0 mellett) ===")
for tag in ['p20_tr2.0_sl1.5', 'p20_tr2.0_sl2.5', 'p20_tr2.0_tr1h', 'p20_tr2.0_notrend']:
    if tag in runs:
        r = runs[tag]
        print(f"  {tag:<22} PnL={r['pnl']:>+9.2f}  PF={r['pf']:.2f}  DD={r['dd']:.0f}")

# ── 2) WF a period×trail rácson
trades_by_cfg = {}
for tag, r in runs.items():
    if not re.match(r"p(\d+)_tr([\d.]+)$", tag): continue
    tp = Path(r['trades_csv'])
    if not tp.exists(): continue
    tr = pd.read_csv(tp, parse_dates=['entry_ts'])
    tr['entry_ts'] = pd.to_datetime(tr.entry_ts, utc=True)
    trades_by_cfg[tag] = tr

start = min(t.entry_ts.min() for t in trades_by_cfg.values()).normalize()
end = max(t.entry_ts.max() for t in trades_by_cfg.values())
IS_M, OOS_M, STEP_M = 6, 2, 2

windows = []
cur = start
while True:
    is_end = cur + pd.DateOffset(months=IS_M)
    oos_end = is_end + pd.DateOffset(months=OOS_M)
    if oos_end > end: break
    windows.append((cur, is_end, oos_end))
    cur += pd.DateOffset(months=STEP_M)

print(f"\n=== WALK-FORWARD (6hó IS → 2hó OOS, {len(windows)} ablak, re-opt a 15-ös rácson) ===")
reopt_oos, fixed_oos = [], []
for i, (a, b, c) in enumerate(windows, 1):
    is_pnls = {}
    for tag, tr in trades_by_cfg.items():
        sub = tr[(tr.entry_ts >= a) & (tr.entry_ts < b)]
        if len(sub) >= 5:
            is_pnls[tag] = sub.pnl.sum()
    if not is_pnls: continue
    best = max(is_pnls, key=is_pnls.get)
    oos_best = trades_by_cfg[best]
    oos_pnl = oos_best[(oos_best.entry_ts >= b) & (oos_best.entry_ts < c)].pnl.sum()
    fx = trades_by_cfg.get('p20_tr2.0')
    fx_pnl = fx[(fx.entry_ts >= b) & (fx.entry_ts < c)].pnl.sum()
    reopt_oos.append(oos_pnl); fixed_oos.append(fx_pnl)
    print(f"  W{i:>2} OOS {b.date()}→{c.date()} | IS-best={best:<12} "
          f"(IS {is_pnls[best]:+7.0f}) → OOS {oos_pnl:+8.2f} | fix p20_tr2.0 OOS: {fx_pnl:+8.2f}")

print(f"\nRE-OPT OOS összesen:  {sum(reopt_oos):+.2f}  ({sum(1 for x in reopt_oos if x>0)}/{len(reopt_oos)} ablak pozitív)")
print(f"FIX p20_tr2.0 OOS:    {sum(fixed_oos):+.2f}  ({sum(1 for x in fixed_oos if x>0)}/{len(fixed_oos)} ablak pozitív)")
plateau_all = [v for v in grid.values()]
print(f"\nPlató-statisztika: {sum(1 for v in plateau_all if v>0)}/{len(plateau_all)} rács-config pozitív a 2 éven")

#!/usr/bin/env python3
"""
Stratégia-attribúció (egyszerűsített):

1. range_scalp: a runner-oldali trades.csv-ben van PnL — közvetlen igazság
2. broker összes tx PnL - range_scalp = consensus (+ egyéb manual/zombie)
3. Bónusz: OPEN activity dealReference-jei alapján megjelöljük az RS tx-eket,
   így napi/heti bontás pontos mindkettőre
"""
from __future__ import annotations
import json
import requests
import pandas as pd
from datetime import datetime, timezone, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent
cfg = json.loads((ROOT / "keys_urls.json").read_text())
base = "https://demo-api-capital.backend-capital.com"
s = requests.Session()
s.headers.update({"X-CAP-API-KEY": cfg["capital_api_key"], "Content-Type": "application/json"})
r = s.post(f"{base}/api/v1/session",
           json={"identifier": cfg["capital_login"],
                 "password": cfg["capital_pw"], "encryptedPassword": False})
r.raise_for_status()
s.headers.update({"CST": r.headers["CST"], "X-SECURITY-TOKEN": r.headers["X-SECURITY-TOKEN"]})

# 30-day activity + tx
all_act = []; all_tx = []
now = datetime.now(timezone.utc)
for db in range(0, 31):
    to_dt = now - timedelta(days=db)
    from_dt = to_dt - timedelta(days=1)
    fmt = lambda dt: dt.strftime('%Y-%m-%dT%H:%M:%S')
    all_act.extend(s.get(
        f"{base}/api/v1/history/activity?from={fmt(from_dt)}&to={fmt(to_dt)}&detailed=true",
        timeout=30).json().get("activities", []))
    all_tx.extend(s.get(
        f"{base}/api/v1/history/transactions?from={fmt(from_dt)}&to={fmt(to_dt)}",
        timeout=30).json().get("transactions", []))

# Dedup
def dedup(items, key_fn):
    seen = set(); out = []
    for x in items:
        k = key_fn(x)
        if k not in seen:
            seen.add(k); out.append(x)
    return out

acts = dedup(all_act, lambda a: (a.get('dealId'), a.get('dateUTC')))
txs = dedup(all_tx, lambda t: (t.get('dealId'), t.get('dateUtc')))
tx_trades = [t for t in txs if t.get('transactionType') == 'TRADE']

# TX-dataframe
tx_df = pd.DataFrame(tx_trades)
tx_df['pnl'] = pd.to_numeric(tx_df['size'], errors='coerce')
tx_df['dateUtc'] = pd.to_datetime(tx_df['dateUtc'], utc=True)
tx_df = tx_df.sort_values('dateUtc').reset_index(drop=True)

# =============================================================================
# 1. range_scalp — runner-oldali igazság a trades.csv-ből
# =============================================================================
rs = pd.read_csv('runs/20260803_145837_live_range_scalp_GOLD_live/trades.csv')
rs['entry_ts'] = pd.to_datetime(rs['entry_ts'], utc=True)
rs['exit_ts_calc'] = rs['entry_ts'] + pd.to_timedelta(rs['hold_seconds'], unit='s')
rs_refs = set(rs['deal_ref'].dropna())

# =============================================================================
# 2. Broker tx → RS tx-eket megjelöljük timestamp+price+size alapján
#    (RS trades.csv-ben van transaction.dealId — használjuk ezt közvetlenül!)
# =============================================================================
rs_broker_dealids = set(rs['transaction.dealId'].dropna())
tx_df['is_rs'] = tx_df['dealId'].isin(rs_broker_dealids)
tx_df['strategy'] = tx_df['is_rs'].map({True: 'range_scalp', False: 'consensus/other'})

# =============================================================================
# 3. STATISZTIKÁK
# =============================================================================
print("=" * 78)
print(f"30-napi broker teljesítmény (2026-08-{(datetime.now(timezone.utc)-timedelta(days=30)).day:02d} → most)")
print("=" * 78)

print(f"\nBroker tx TRADE típusú: {len(tx_df)} db")
print(f"  Össz PnL: ${tx_df.pnl.sum():+.2f}")
print(f"  WR: {(tx_df.pnl>0).mean()*100:.0f}%, PF: {tx_df[tx_df.pnl>0].pnl.sum() / max(1e-6, -tx_df[tx_df.pnl<0].pnl.sum()):.2f}")

rs_from_broker = tx_df[tx_df.is_rs]
cons_from_broker = tx_df[~tx_df.is_rs]

print(f"\n=== STRATÉGIA-BONTÁS ===")
print(f"\nrange_scalp (trades.csv → dealId match a brókerrel):")
print(f"  Broker-side tx-ek: {len(rs_from_broker)} (RS trades.csv: {len(rs)})")
print(f"  PnL broker-oldal:  ${rs_from_broker.pnl.sum():+.2f}")
print(f"  PnL trades.csv:    ${rs.pnl.sum():+.2f}  (különbség: ${(rs_from_broker.pnl.sum() - rs.pnl.sum()):+.2f})")
print(f"  WR: {(rs_from_broker.pnl>0).mean()*100:.0f}%, PF: "
      f"{rs_from_broker[rs_from_broker.pnl>0].pnl.sum() / max(1e-6, -rs_from_broker[rs_from_broker.pnl<0].pnl.sum()):.2f}")

print(f"\nconsensus/other (minden ami nem RS):")
print(f"  Broker-side tx-ek: {len(cons_from_broker)}")
print(f"  PnL:               ${cons_from_broker.pnl.sum():+.2f}")
print(f"  WR: {(cons_from_broker.pnl>0).mean()*100:.0f}%, PF: "
      f"{cons_from_broker[cons_from_broker.pnl>0].pnl.sum() / max(1e-6, -cons_from_broker[cons_from_broker.pnl<0].pnl.sum()):.2f}")
print(f"  Best: ${cons_from_broker.pnl.max():+.2f}, Worst: ${cons_from_broker.pnl.min():+.2f}")

# Heti bontás stratégiánként
tx_df['week'] = tx_df.dateUtc.dt.to_period('W-SUN')
tx_df['day'] = tx_df.dateUtc.dt.date

print(f"\n=== HETI BONTÁS (broker-oldal, PnL$ + trade-szám) ===")
weekly = tx_df.groupby(['week','strategy']).agg(
    n=('pnl','count'), pnl=('pnl', 'sum')).round(2)
print(weekly.unstack(fill_value=0).to_string())

print(f"\n=== NAPI BONTÁS (utolsó 14 nap) ===")
daily = tx_df.groupby(['day','strategy']).agg(
    n=('pnl','count'), pnl=('pnl','sum')).round(2).unstack(fill_value=0)
print(daily.tail(14).to_string())

# =============================================================================
# 4. Consensus mélyítés: positions.csv-ből csatorna-információ
# =============================================================================
pos = pd.read_csv('positions.csv', low_memory=False)
pos['send_date'] = pd.to_datetime(pos['send_date'], errors='coerce', utc=True)
pos['opened_at'] = pd.to_datetime(pos['opened_at'], errors='coerce', utc=True)

# Csak a consensus-hez tartozó tx-ek időbélyege alapján csatorna-nyomozás
# tx.dateUtc közelében (±5 min) melyik channel send-elt jelet?
cutoff = tx_df.dateUtc.min()
recent_pos = pos[(pos['send_date'].notna()) & (pos['send_date'] >= cutoff)]
print(f"\n=== CONSENSUS csatorna-becslés (positions.csv send_date + tx close_time közelítéssel) ===")
print(f"  positions.csv send_date entries (30 nap): {len(recent_pos)}")
print(f"  Chat_name eloszlás:")
print(recent_pos.groupby(['chat_name','direction']).size().to_string())

# Save
tx_df.to_csv('capital_tx_30d_attributed.csv', index=False)
print(f"\nMentve: capital_tx_30d_attributed.csv")

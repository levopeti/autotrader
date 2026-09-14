#!/usr/bin/env python3
"""
Csatornánkénti PnL 30 napra.

Logika:
1. TX list (dealId, dateUtc, pnl) — a broker igazsága
2. Minden tx-hez matching close activity keresés (dealId szerint)
3. Close activity → openPrice + direction (inverted = orig_dir) → matching positions.csv sor keresés
   (chat_name, opened_at) → csatorna-attribúció
4. range_scalp trades.csv.transaction.dealId előre matcheli az RS tx-eket

Kimenet: PnL / csatorna, WR, PF, heti bontás
"""
from __future__ import annotations
import json, requests, pandas as pd
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

# --- Fetch 30d activity + tx ---
all_act = []; all_tx = []
now = datetime.now(timezone.utc)
for db in range(0, 31):
    to_dt = now - timedelta(days=db)
    from_dt = to_dt - timedelta(days=1)
    fmt = lambda dt: dt.strftime('%Y-%m-%dT%H:%M:%S')
    all_act.extend(s.get(f"{base}/api/v1/history/activity?from={fmt(from_dt)}&to={fmt(to_dt)}&detailed=true",
                         timeout=30).json().get("activities", []))
    all_tx.extend(s.get(f"{base}/api/v1/history/transactions?from={fmt(from_dt)}&to={fmt(to_dt)}",
                        timeout=30).json().get("transactions", []))

# Dedup
acts = list({(a.get('dealId'), a.get('dateUTC')): a for a in all_act}.values())
txs = list({(t.get('dealId'), t.get('dateUtc')): t for t in all_tx}.values())
tx_trades = [t for t in txs if t.get('transactionType') == 'TRADE']

# --- close activity dict by dealId (first match wins) ---
close_by_dealid = {}
for a in acts:
    if a.get('type') == 'POSITION' and a.get('status') == 'ACCEPTED' \
       and a.get('source') in ('TP','SL','USER','MARKET','LIMIT','TRAILING_STOP'):
        did = a['dealId']
        if did not in close_by_dealid:
            close_by_dealid[did] = a

# --- positions.csv ---
pos = pd.read_csv('positions.csv', low_memory=False)
pos['opened_at'] = pd.to_datetime(pos['opened_at'], errors='coerce', utc=True)
pos['open_level'] = pd.to_numeric(pos['open_level'], errors='coerce')
pos_filled = pos[(pos['state']=='FILLED') & pos['opened_at'].notna() &
                 pos['open_level'].notna()].copy().reset_index(drop=True)
# Consumption tracking: minden positions.csv sort max 1× használunk
pos_used = [False] * len(pos_filled)

# --- range_scalp dealIds ---
rs = pd.read_csv('runs/20260803_145837_live_range_scalp_GOLD_live/trades.csv')
rs_dealids = set(rs['transaction.dealId'].dropna())

# --- attribute each tx (unique per tx, no duplication) ---
tx_trades_sorted = sorted(tx_trades, key=lambda t: t.get('dateUtc', ''))

rows = []
for t in tx_trades_sorted:
    did = t['dealId']
    tx_dt = pd.to_datetime(t['dateUtc'], utc=True)
    pnl = float(t.get('size', 0)) if t.get('size') else 0
    row = {'dealId': did, 'tx_dt': tx_dt, 'pnl': pnl, 'note': t.get('note')}

    # 1) RS check
    if did in rs_dealids:
        row['channel'] = 'range_scalp'
        rows.append(row); continue

    # 2) close activity lookup
    close = close_by_dealid.get(did)
    if not close:
        row['channel'] = 'no_close_activity'
        rows.append(row); continue
    d = close.get('details') or {}
    close_dir = d.get('direction')
    open_price = d.get('openPrice')
    close_level = d.get('level')
    src = close.get('source')
    row.update({'source': src, 'close_dir': close_dir, 'openPrice': open_price, 'closeLevel': close_level})
    if not close_dir or not open_price:
        row['channel'] = 'no_openprice'
        rows.append(row); continue

    orig_dir = 'BUY' if close_dir == 'SELL' else 'SELL'

    # 3) positions.csv match (consume: 1 tx <-> 1 pos row)
    best_idx = None; best_dt_diff = pd.Timedelta(days=999)
    for idx in range(len(pos_filled)):
        if pos_used[idx]:
            continue
        p = pos_filled.iloc[idx]
        if p['direction'] != orig_dir:
            continue
        if abs(p['open_level'] - open_price) > 3.0:
            continue
        if p['opened_at'] > tx_dt:  # opened must be before closed
            continue
        dt_diff = tx_dt - p['opened_at']
        if dt_diff > pd.Timedelta(days=3):
            continue
        if dt_diff < best_dt_diff:
            best_dt_diff = dt_diff
            best_idx = idx
    if best_idx is None:
        row['channel'] = 'no_pos_match'
        rows.append(row); continue
    pos_used[best_idx] = True
    row['channel'] = pos_filled.iloc[best_idx]['chat_name']
    row['tp_idx'] = pos_filled.iloc[best_idx].get('tp_idx')
    row['hold_h'] = (tx_dt - pos_filled.iloc[best_idx]['opened_at']).total_seconds() / 3600
    rows.append(row)

df = pd.DataFrame(rows)
print(f"=== ATTRIBUCIÓ: {len(df)} tx ===")
print(df['channel'].value_counts().to_string())

print(f"\n=== CSATORNÁNKÉNTI PnL 30 nap ===")
agg = df.groupby('channel').agg(
    n=('pnl', 'count'), pnl=('pnl', 'sum'),
    wins=('pnl', lambda s: (s>0).sum()), losses=('pnl', lambda s: (s<0).sum()),
    best=('pnl', 'max'), worst=('pnl', 'min'),
).round(2)
agg['wr%'] = (agg.wins / agg.n * 100).round(0)
agg['avg'] = (agg.pnl / agg.n).round(2)
agg['pf'] = df.groupby('channel').apply(
    lambda g: (g[g.pnl>0].pnl.sum() / max(1e-6, -g[g.pnl<0].pnl.sum())) if len(g[g.pnl<0])>0 else 999.99,
    include_groups=False).round(2)
print(agg.sort_values('pnl', ascending=False).to_string())

# csatorna × irány
if 'openPrice' in df.columns:
    df['orig_dir'] = df['close_dir'].map({'BUY':'SELL','SELL':'BUY'})
    known = df[df.channel.isin(['VIP SIGNALS ROOM (LIFETIME)🚀', 'Traderz Gold VIP', 'range_scalp'])]
    if len(known) > 0:
        print(f"\n=== KNOWN CSATORNÁK × irány ===")
        print(known.groupby(['channel','orig_dir']).agg(n=('pnl','count'), pnl=('pnl','sum')).round(2).to_string())

# tp_idx bontás
if 'tp_idx' in df.columns:
    tp_known = df[df.tp_idx.notna() & df.channel.isin(['VIP SIGNALS ROOM (LIFETIME)🚀', 'Traderz Gold VIP'])]
    if len(tp_known) > 0:
        print(f"\n=== KNOWN CSATORNÁK × tp_idx ===")
        print(tp_known.groupby(['channel','tp_idx']).agg(n=('pnl','count'), pnl=('pnl','sum')).round(2).to_string())

# Heti
df['week'] = df.tx_dt.dt.to_period('W-SUN')
print(f"\n=== HETI × CSATORNA (PnL$) ===")
weekly = df.groupby(['week','channel']).pnl.sum().unstack(fill_value=0).round(2)
print(weekly.to_string())

df.to_csv('channel_pnl_30d.csv', index=False)
print(f"\nMentve: channel_pnl_30d.csv")

#!/usr/bin/env python3
"""
positions.csv backfill: a `realised_pnl=NaN` sorokat utólag kitölti a Capital
`/history/transactions` válasza alapján. 30-napi from-to loopot használ (napi
24h ablakokban).

Match: opening dealId prefix (utolsó karakter nélküli) → closing tranzakció.
PnL: a tranzakció `size` mezője (string), float-ra konvertálva.

Emellett kiírja a valódi 30-napi teljesítményt is (broker-oldali).
"""
from __future__ import annotations

import json
import sys
import requests
import pandas as pd
from pathlib import Path
from datetime import datetime, timezone, timedelta

ROOT = Path(__file__).resolve().parent
CSV = ROOT / "positions.csv"

cfg = json.loads((ROOT / "keys_urls.json").read_text())
base = "https://demo-api-capital.backend-capital.com"

s = requests.Session()
s.headers.update({"X-CAP-API-KEY": cfg["capital_api_key"], "Content-Type": "application/json"})
r = s.post(f"{base}/api/v1/session",
           json={"identifier": cfg["capital_login"],
                 "password": cfg["capital_pw"], "encryptedPassword": False})
r.raise_for_status()
s.headers.update({"CST": r.headers["CST"], "X-SECURITY-TOKEN": r.headers["X-SECURITY-TOKEN"]})

# 30-napi tx napi ablakokban
all_tx = []
now = datetime.now(timezone.utc)
for days_back in range(0, 31):
    to_dt = now - timedelta(days=days_back)
    from_dt = to_dt - timedelta(days=1)
    url = (f"{base}/api/v1/history/transactions"
           f"?from={from_dt.strftime('%Y-%m-%dT%H:%M:%S')}"
           f"&to={to_dt.strftime('%Y-%m-%dT%H:%M:%S')}")
    r = s.get(url, timeout=30).json()
    all_tx.extend(r.get("transactions", []))

# Dedup by dealId + dateUtc
seen = set(); unique_tx = []
for t in all_tx:
    key = (t.get('dealId'), t.get('dateUtc'))
    if key in seen: continue
    seen.add(key); unique_tx.append(t)

trades = [t for t in unique_tx if t.get('transactionType') == 'TRADE']
print(f"Capital tranzakciók 30 napra: {len(unique_tx)} tx, ebből TRADE: {len(trades)}")

# closing dealId prefix → tx lookup
prefix_to_tx = {}
for t in trades:
    did = str(t.get("dealId", ""))
    if not did: continue
    prefix = did[:-1]
    prev = prefix_to_tx.get(prefix)
    if (prev is None) or (t.get("dateUtc", "") > prev.get("dateUtc", "")):
        prefix_to_tx[prefix] = t
print(f"DealId-prefixenkénti closing tranzakciók: {len(prefix_to_tx)}")

# positions.csv backfill
df = pd.read_csv(CSV, on_bad_lines="skip")
filled_mask = df["state"] == "FILLED"
filled = df[filled_mask].copy()
print(f"\npositions.csv FILLED sorok: {len(filled)}, "
      f"realised_pnl=NaN: {filled['realised_pnl'].isna().sum()}")

updated = 0
for idx, row in filled.iterrows():
    if pd.notna(row["realised_pnl"]) and row["realised_pnl"] != "":
        continue
    deal_id = str(row.get("deal_id", "")).strip()
    if not deal_id or deal_id == "nan":
        continue
    prefix = deal_id[:-1]
    t = prefix_to_tx.get(prefix)
    if not t:
        continue
    try:
        pnl = float(t.get("size"))
    except (TypeError, ValueError):
        continue
    df.at[idx, "realised_pnl"] = pnl
    df.at[idx, "currency"] = t.get("currency") or row.get("currency", "")
    updated += 1

print(f"positions.csv frissítve: {updated} sor")

if updated > 0:
    backup = CSV.with_suffix(".csv.bak")
    print(f"Backup: {backup}")
    Path(CSV).rename(backup)
    df.to_csv(CSV, index=False)
    print(f"Mentve: {CSV}")

# Broker-oldali teljes 30-napi kép
tx_df = pd.DataFrame(trades)
tx_df['pnl'] = pd.to_numeric(tx_df['size'], errors='coerce')
tx_df['dateUtc'] = pd.to_datetime(tx_df['dateUtc'], utc=True)
tx_df = tx_df.sort_values('dateUtc').reset_index(drop=True)

print(f"\n=== BROKER 30-napi PnL (Capital tx-ekből) ===")
print(f"Trades: {len(tx_df)}, Sum: ${tx_df.pnl.sum():+.2f}, "
      f"WR: {(tx_df.pnl>0).mean()*100:.0f}%, PF: "
      f"{tx_df[tx_df.pnl>0].pnl.sum() / max(1e-6, -tx_df[tx_df.pnl<0].pnl.sum()):.2f}")

# Save tx dump for attribution script
tx_df.to_csv(ROOT / "capital_tx_30d.csv", index=False)
print(f"Mentve: capital_tx_30d.csv")

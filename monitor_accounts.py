#!/usr/bin/env python3
"""
Fiókonkénti PnL-monitor.

Két Capital demo account-ot külön lekérdez:
  - SignalTrader (320258870701535518) → consensus/signal_replay
  - AutoTrader   (315743940946252958) → range_scalp

Output: balance, aktuális nyitott pozíciók, N napi PnL napi/heti bontásban.

Használat:
  python monitor_accounts.py              # default 30 nap
  python monitor_accounts.py --days 7     # csak 7 nap
  python monitor_accounts.py --account SignalTrader
"""
from __future__ import annotations
import argparse
import json
import sys
import time
import requests
import pandas as pd
from datetime import datetime, timezone, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent
CFG = json.loads((ROOT / "keys_urls.json").read_text())
BASE = "https://demo-api-capital.backend-capital.com"

ACCOUNTS = {
    "SignalTrader": "320258870701535518",  # USD, consensus/signal_replay
    "AutoTrader":   "315743940946252958",  # EUR, range_scalp
}


def login_with_retry(max_attempts: int = 5, wait: int = 45) -> requests.Session:
    """Login, 429 esetén várunk + újra."""
    for attempt in range(1, max_attempts + 1):
        s = requests.Session()
        s.headers.update({"X-CAP-API-KEY": CFG["capital_api_key"], "Content-Type": "application/json"})
        r = s.post(f"{BASE}/api/v1/session",
                   json={"identifier": CFG["capital_login"], "password": CFG["capital_pw"], "encryptedPassword": False},
                   timeout=30)
        if r.status_code == 429:
            print(f"  [rate-limit 429, wait {wait}s, attempt {attempt}/{max_attempts}]", file=sys.stderr)
            time.sleep(wait); continue
        r.raise_for_status()
        s.headers.update({"CST": r.headers["CST"], "X-SECURITY-TOKEN": r.headers["X-SECURITY-TOKEN"]})
        s._current_account = str(r.json().get("currentAccountId"))
        return s
    raise RuntimeError(f"Login failed after {max_attempts} attempts")


def switch_account(s: requests.Session, account_id: str) -> None:
    if s._current_account == str(account_id):
        return
    r = s.put(f"{BASE}/api/v1/session", json={"accountId": str(account_id)}, timeout=30)
    if r.status_code == 429:
        time.sleep(30)
        r = s.put(f"{BASE}/api/v1/session", json={"accountId": str(account_id)}, timeout=30)
    r.raise_for_status()
    s._current_account = str(account_id)


def fetch_account_data(name: str, account_id: str, days: int, session: requests.Session) -> dict:
    switch_account(session, account_id)
    # Balance + open positions
    acc = session.get(f"{BASE}/api/v1/accounts", timeout=30).json()
    my_acc = next((a for a in acc.get("accounts", []) if str(a.get("accountId")) == str(account_id)), {})
    b = my_acc.get("balance", {})

    pos_r = session.get(f"{BASE}/api/v1/positions", timeout=30).json()
    open_positions = pos_r.get("positions", [])

    # Tx history N napra napi ablakokban
    all_tx = []
    now = datetime.now(timezone.utc)
    for db in range(0, days + 1):
        to_dt = now - timedelta(days=db)
        from_dt = to_dt - timedelta(days=1)
        fmt = lambda dt: dt.strftime('%Y-%m-%dT%H:%M:%S')
        try:
            r = session.get(f"{BASE}/api/v1/history/transactions?from={fmt(from_dt)}&to={fmt(to_dt)}", timeout=30)
            all_tx.extend(r.json().get("transactions", []))
        except Exception as e:
            print(f"  [warn: tx fetch fail for {from_dt.date()}: {e}]", file=sys.stderr)
    # Dedup + filter TRADE
    seen = set(); uniq = []
    for t in all_tx:
        k = (t.get('dealId'), t.get('dateUtc'))
        if k not in seen:
            seen.add(k); uniq.append(t)
    trades = [t for t in uniq if t.get('transactionType') == 'TRADE']

    return {
        'name': name, 'account_id': account_id,
        'balance': b.get('balance'), 'currency': my_acc.get('currency'),
        'available': b.get('available'), 'deposit': b.get('deposit'),
        'profitLoss_open': b.get('profitLoss'),
        'open_positions': open_positions,
        'trades': trades,
    }


def summarize(data: dict, days: int) -> None:
    name = data['name']
    print(f"\n{'='*80}")
    print(f"  {name}  ({data['account_id']})  {data['currency']}")
    print(f"{'='*80}")
    print(f"  Balance:   {data['balance']:.2f} {data['currency']}   Available: {data['available']:.2f}   "
          f"Deposit alap: {data['deposit']:.2f}")
    print(f"  Nyitott pozíciók (broker-oldal): {len(data['open_positions'])}")
    if data['open_positions']:
        for p in data['open_positions'][:5]:
            m = p.get('market', {}); pd_ = p.get('position', {})
            print(f"    {m.get('epic')} {pd_.get('direction')} size={pd_.get('size')} "
                  f"open={pd_.get('level')} SL={pd_.get('stopLevel')} TP={pd_.get('profitLevel')} upl={pd_.get('upl'):+.2f}")
    # profitLoss_open sometimes None
    if data.get('profitLoss_open') is not None:
        print(f"  Nyitott pozíciók összes UPL: {data['profitLoss_open']:.2f} {data['currency']}")

    trades = data['trades']
    if not trades:
        print(f"\n  Nincs zárt trade az utolsó {days} napban.")
        return
    df = pd.DataFrame(trades)
    df['pnl'] = pd.to_numeric(df['size'], errors='coerce')
    df['dt'] = pd.to_datetime(df['dateUtc'], utc=True)
    df = df.sort_values('dt').reset_index(drop=True)

    total = df.pnl.sum()
    wins = (df.pnl > 0).sum(); losses = (df.pnl < 0).sum()
    wr = wins / len(df) * 100
    gp = df[df.pnl > 0].pnl.sum(); gl = -df[df.pnl < 0].pnl.sum()
    pf = gp / max(1e-6, gl)
    print(f"\n  --- {days} napi zárt trade ---")
    print(f"  Trade: {len(df)} ({wins}W/{losses}L, WR {wr:.0f}%)  Sum: {total:+.2f}  "
          f"PF: {pf:.2f}  Best: {df.pnl.max():+.2f}  Worst: {df.pnl.min():+.2f}")
    print(f"  Avg/trade: {total/len(df):+.2f}  ~{total/days*30:+.2f}/hó szinten")

    df['day'] = df.dt.dt.date
    daily = df.groupby('day').agg(n=('pnl', 'count'), pnl=('pnl', 'sum'),
                                   wins=('pnl', lambda s: (s>0).sum())).round(2)
    daily['wr'] = (daily.wins / daily.n * 100).round(0)
    print(f"\n  --- Napi bontás (utolsó {min(days, len(daily))} nap) ---")
    print(daily.tail(days).to_string())

    if days >= 7:
        df['week'] = df.dt.dt.to_period('W-SUN')
        weekly = df.groupby('week').agg(n=('pnl','count'), pnl=('pnl','sum'),
                                         wins=('pnl', lambda s: (s>0).sum())).round(2)
        weekly['wr'] = (weekly.wins / weekly.n * 100).round(0)
        print(f"\n  --- Heti bontás ---")
        print(weekly.to_string())


def main():
    p = argparse.ArgumentParser(description="Fiókonkénti PnL monitor")
    p.add_argument("--days", type=int, default=30)
    p.add_argument("--account", default=None, choices=[*ACCOUNTS.keys(), None],
                   help="csak egy fiók (default: mindkettő)")
    args = p.parse_args()

    print(f"Capital account monitor  |  {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}  |  utolsó {args.days} nap")
    session = login_with_retry()

    picks = [(n, i) for n, i in ACCOUNTS.items() if args.account is None or n == args.account]
    for name, aid in picks:
        try:
            data = fetch_account_data(name, aid, args.days, session)
            summarize(data, args.days)
        except Exception as e:
            print(f"\n[ERROR] {name} lekérdezés fail: {e}", file=sys.stderr)


if __name__ == "__main__":
    main()

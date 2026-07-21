from __future__ import annotations

from typing import Dict, List

import numpy as np
import pandas as pd

from .position import Position


def compute_metrics(positions: List[Position]) -> Dict[str, float | int]:
    closed = [p for p in positions if not p.is_open]
    if not closed:
        return {"n_trades": 0, "total_pnl": 0.0}

    pnl = pd.Series([p.pnl for p in closed], dtype=float)
    n = len(pnl)
    n_win = int((pnl > 0).sum())
    n_loss = int((pnl <= 0).sum())
    wr = n_win / n if n else 0.0

    gross_profit = float(pnl[pnl > 0].sum())
    gross_loss = float(pnl[pnl < 0].sum())
    pf = (gross_profit / abs(gross_loss)) if gross_loss < 0 else float("inf") if gross_profit > 0 else 0.0

    avg_win = float(pnl[pnl > 0].mean()) if n_win else 0.0
    avg_loss = float(pnl[pnl <= 0].mean()) if n_loss else 0.0
    expect = wr * avg_win + (1 - wr) * avg_loss

    closes = pd.DataFrame({
        "ts": [p.exit_ts for p in closed],
        "pnl": [p.pnl for p in closed],
    })
    closes["ts"] = pd.to_datetime(closes["ts"], utc=True)
    daily = closes.groupby(closes["ts"].dt.date)["pnl"].sum()

    if daily.std() > 0:
        sharpe = float(daily.mean() / daily.std() * np.sqrt(252))
    else:
        sharpe = 0.0
    neg = daily[daily < 0]
    sortino = float(daily.mean() / neg.std() * np.sqrt(252)) if len(neg) > 1 and neg.std() > 0 else 0.0

    equity = pnl.cumsum()
    peak = equity.cummax()
    dd = (equity - peak)
    mdd_abs = float(dd.min()) if len(dd) else 0.0

    exit_counts = pd.Series([p.exit_reason for p in closed]).value_counts().to_dict()
    hold_secs = pd.Series([p.hold_seconds for p in closed if p.hold_seconds is not None])

    return {
        "n_trades": int(n),
        "n_win": n_win,
        "n_loss": n_loss,
        "win_rate": round(wr, 4),
        "total_pnl": round(float(pnl.sum()), 4),
        "gross_profit": round(gross_profit, 4),
        "gross_loss": round(gross_loss, 4),
        "profit_factor": round(pf, 4) if np.isfinite(pf) else None,
        "avg_win": round(avg_win, 4),
        "avg_loss": round(avg_loss, 4),
        "expectancy": round(expect, 4),
        "sharpe_ratio": round(sharpe, 4),
        "sortino_ratio": round(sortino, 4),
        "max_drawdown_abs": round(mdd_abs, 4),
        "avg_hold_sec": round(float(hold_secs.mean()), 1) if len(hold_secs) else None,
        "exit_counts": {str(k): int(v) for k, v in exit_counts.items()},
    }
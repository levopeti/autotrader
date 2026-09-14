#!/usr/bin/env python3
"""Csatornánkénti kiértékelés a per-channel sweep eredményéből."""
from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent
CSV  = ROOT / "logs_bulk" / "perch_metrics.csv"
OUT  = ROOT / "logs_bulk" / "perch_report.md"

BLOCK_TITLES = {
    "A": "A) TP single-level scan",
    "B": "B) Multi-TP subset scan",
    "C": "C) Timeout scan",
    "D": "D) SL multiplier scan",
    "E": "E) ATR-based SL/TP grid",
}

CHANNEL_TITLES = {
    "ann":     "ANN Zerofloat",
    "vip":     "VIP SIGNALS ROOM",
    "traderz": "Traderz Gold VIP",
}

DISPLAY_COLS = ["experiment", "n_trades", "win_rate", "total_pnl",
                "profit_factor", "expectancy", "max_dd_abs",
                "exit_TP", "exit_SL", "exit_TIMEOUT"]


def fmt_table(df: pd.DataFrame, cols=DISPLAY_COLS) -> str:
    if df.empty:
        return "*(nincs adat)*"
    d = df[cols].copy()
    for c in ("win_rate",):
        d[c] = d[c].apply(lambda x: f"{x:.3f}" if pd.notna(x) else "")
    for c in ("total_pnl", "profit_factor", "expectancy", "max_dd_abs"):
        d[c] = d[c].apply(lambda x: f"{x:+.2f}" if pd.notna(x) else "")
    for c in ("n_trades", "exit_TP", "exit_SL", "exit_TIMEOUT"):
        d[c] = d[c].apply(lambda x: f"{int(x)}" if pd.notna(x) else "")
    return d.to_string(index=False)


def main():
    df = pd.read_csv(CSV)
    out_lines: list[str] = []

    def emit(s=""):
        out_lines.append(s)

    emit("# Per-channel signal_replay sweep — eredmény")
    emit()
    emit(f"Forrás: `{CSV.relative_to(ROOT)}` ({len(df)} futás)")
    emit()

    # ── Globális rangsor: top-10 minden csatornán át, PnL szerint
    emit("## Top-10 az összes futás közül (PnL szerint)")
    emit("```")
    top = df.sort_values("total_pnl", ascending=False).head(10)
    emit(top[["channel", "experiment", "n_trades", "win_rate", "total_pnl",
              "profit_factor", "expectancy", "max_dd_abs"]].to_string(index=False))
    emit("```")
    emit()

    # ── Csatornánkénti, blokkonkénti bontás
    for ch_key, ch_name in CHANNEL_TITLES.items():
        ch_df = df[df["channel"] == ch_key].copy()
        if ch_df.empty:
            continue
        emit(f"## {ch_name} ({ch_key})")
        emit()
        emit(f"Összes futás csatornán: {len(ch_df)}")
        emit()

        # Csatorna baseline-ja (A_tp0)
        base = ch_df[ch_df["experiment"] == "A_tp0"]
        if not base.empty:
            r = base.iloc[0]
            emit(f"**Baseline (tp_idx=0, defaults):** n={int(r['n_trades'])}, "
                 f"win={r['win_rate']:.3f}, PnL={r['total_pnl']:+.2f}$, "
                 f"PF={r['profit_factor']:.3f}")
            emit()

        for blk in ("A", "B", "C", "D", "E"):
            sub = ch_df[ch_df["block"] == blk].copy()
            if sub.empty:
                continue
            sub = sub.sort_values("total_pnl", ascending=False)
            emit(f"### {BLOCK_TITLES[blk]}  —  top by PnL")
            emit("```")
            emit(fmt_table(sub))
            emit("```")
            emit()

        # Csatornán belüli abszolút legjobb
        best = ch_df.sort_values("total_pnl", ascending=False).iloc[0]
        emit(f"**Csatorna best:** `{best['experiment']}` — "
             f"PnL **{best['total_pnl']:+.2f}$**, PF {best['profit_factor']:.3f}, "
             f"win {best['win_rate']:.3f}, n={int(best['n_trades'])}")
        emit(f"  Override: `{best['overrides']}`")
        emit(f"  Run-dir: `{best['run_dir']}`")
        emit()

    OUT.write_text("\n".join(out_lines))
    print(f"Riport mentve: {OUT}\n")
    print("\n".join(out_lines))


if __name__ == "__main__":
    main()

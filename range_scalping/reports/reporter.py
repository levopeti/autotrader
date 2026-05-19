"""
Reporter
========
Backtest eredmény HTML riport + konzolos összefoglaló.
"""

from __future__ import annotations

import json
import numpy as np
import pandas as pd
from pathlib import Path
from typing import Optional


# ── KONZOLOS ÖSSZEFOGLALÓ ─────────────────────────────────────────────────────

def print_metrics(metrics: dict, title: str = "BACKTEST EREDMÉNY"):
    w = 56
    print(f"\n{'═'*w}")
    print(f"  {title}")
    print(f"{'═'*w}")
    rows = [
        ("Összes PnL",        f"${metrics.get('total_pnl', 0):>12,.2f}"),
        ("Trades száma",       f"{metrics.get('n_trades', 0):>13,}"),
        ("Win Rate",           f"{metrics.get('win_rate', 0):>12.1%}"),
        ("Profit Factor",      f"{metrics.get('profit_factor', 0):>13.4f}"),
        ("Átlag nyereség",     f"${metrics.get('avg_win', 0):>12,.2f}"),
        ("Átlag veszteség",    f"${metrics.get('avg_loss', 0):>12,.2f}"),
        ("Expectancy/trade",   f"${metrics.get('expectancy', 0):>12,.2f}"),
        ("Sharpe Ratio",       f"{metrics.get('sharpe_ratio', 0):>13.4f}"),
        ("Sortino Ratio",      f"{metrics.get('sortino_ratio', 0):>13.4f}"),
        ("Calmar Ratio",       f"{metrics.get('calmar_ratio', 0):>13.4f}"),
        ("Max Drawdown",       f"{metrics.get('max_drawdown_pct', 0):>12.2f}%"),
        ("TP találatok",       f"{metrics.get('n_tp', 0):>13,}"),
        ("SL találatok",       f"{metrics.get('n_sl', 0):>13,}"),
        ("Timeout zárások",    f"{metrics.get('n_timeout', 0):>13,}"),
        ("Átlag tartás",       f"{metrics.get('avg_duration_min', 0):>11.1f} perc"),
    ]
    for label, val in rows:
        print(f"  {label:<22} {val}")
    print(f"{'═'*w}\n")


# ── HTML RIPORT ───────────────────────────────────────────────────────────────

def generate_html_report(
    result,
    config,
    output_path: str = "backtest_report.html",
    wf_df: Optional[pd.DataFrame] = None,
) -> str:

    from core.engine import BacktestResult

    m      = result.metrics
    trades = result.trades
    eq     = result.equity_curve

    if not trades:
        print("⚠️  Nincs trade az eredményben.")
        return ""

    df = result.trades_df()
    df["exit_time"]  = pd.to_datetime(df["exit_time"])
    df["entry_time"] = pd.to_datetime(df["entry_time"])
    df["date"]       = df["exit_time"].dt.date

    # Napi PnL
    daily      = df.groupby("date")["pnl"].sum().reset_index()
    daily.columns = ["date", "pnl"]
    daily["cum_pnl"] = daily["pnl"].cumsum()

    # Havi bontás
    df["month"] = df["exit_time"].dt.to_period("M").astype(str)
    monthly = df.groupby("month").agg(
        pnl=("pnl", "sum"),
        trades=("pnl", "count"),
        wins=("pnl", lambda x: (x > 0).sum()),
    ).reset_index()
    monthly["win_rate"] = monthly["wins"] / monthly["trades"]

    # Kilépési okok
    exit_c = df["exit_reason"].value_counts()

    # Walk-Forward tábla
    wf_rows_html = ""
    if wf_df is not None and not wf_df.empty:
        for _, r in wf_df.iterrows():
            score = f"{r['oos_score']:.4f}" if pd.notna(r.get("oos_score")) else "N/A"
            clr   = "#4caf82" if (r.get("oos_pnl") or 0) > 0 else "#e05a5a"
            wf_rows_html += f"""<tr>
                <td>Fold {r['fold']}</td>
                <td>{r['is_start']}→{r['is_end']}</td>
                <td>{r['oos_start']}→{r['oos_end']}</td>
                <td>{r['is_score']:.4f}</td>
                <td style="color:{clr}">{score}</td>
                <td style="color:{clr}">${(r.get('oos_pnl') or 0):,.0f}</td>
                <td>{int(r.get('oos_trades') or 0)}</td>
                <td>{(r.get('oos_wr') or 0):.1%}</td>
                <td>{(r.get('oos_pf') or 0):.3f}</td>
                <td>{(r.get('oos_dd') or 0):.1f}%</td>
            </tr>"""

    wf_section = f"""
    <section class="card">
      <h2>Walk-Forward Validáció</h2>
      <div class="table-wrap">
      <table><thead><tr>
        <th>Fold</th><th>IS Periódus</th><th>OOS Periódus</th>
        <th>IS Score</th><th>OOS Score</th><th>OOS PnL</th>
        <th>Trades</th><th>Win%</th><th>PF</th><th>MDD</th>
      </tr></thead><tbody>{wf_rows_html}</tbody></table>
      </div>
    </section>""" if wf_rows_html else ""

    # Config tábla
    from dataclasses import asdict
    cfg_rows = "".join(
        f"<tr><td>{k}</td><td>{v}</td></tr>"
        for k, v in asdict(config).items()
    )

    # Monthly rows
    month_rows = ""
    for _, r in monthly.iterrows():
        clr = "#4caf82" if r["pnl"] > 0 else "#e05a5a"
        month_rows += (
            f"<tr><td>{r['month']}</td>"
            f"<td style='color:{clr}'>${r['pnl']:,.2f}</td>"
            f"<td>{r['trades']}</td>"
            f"<td>{r['win_rate']:.1%}</td></tr>"
        )

    # JSON adatok a chartshoz
    dates_json   = json.dumps([str(d) for d in daily["date"]])
    cum_pnl_json = json.dumps([round(v, 2) for v in daily["cum_pnl"]])
    day_pnl_json = json.dumps([round(v, 2) for v in daily["pnl"]])
    exit_labels  = json.dumps(exit_c.index.tolist())
    exit_vals    = json.dumps(exit_c.values.tolist())

    def kpi_color(val, positive_good=True):
        if positive_good:
            return "pos" if val > 0 else "neg"
        return "neg" if val > 0 else "pos"

    html = f"""<!DOCTYPE html>
<html lang="hu">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>XAUUSD Scalper Backtest Report</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>
:root{{
  --bg:#0c0e10;--s1:#131518;--s2:#1a1d21;--s3:#202428;
  --border:#262a2f;--text:#dde1e7;--muted:#7a818c;--faint:#4a5060;
  --gold:#f0b429;--green:#43c97e;--red:#e05252;--blue:#4d9eff;--purple:#a37af0;
}}
*{{box-sizing:border-box;margin:0;padding:0}}
body{{background:var(--bg);color:var(--text);font-family:'Inter',system-ui,sans-serif;font-size:14px;line-height:1.6}}
a{{color:var(--gold)}}
.header{{background:var(--s1);border-bottom:1px solid var(--border);padding:18px 32px;display:flex;align-items:center;gap:20px}}
.header-icon{{width:36px;height:36px;background:var(--gold);border-radius:8px;display:grid;place-items:center;font-size:18px;flex-shrink:0}}
.header h1{{font-size:18px;font-weight:700;color:var(--gold);letter-spacing:-.01em}}
.header .sub{{color:var(--muted);font-size:12px;margin-top:2px}}
.wrap{{max-width:1400px;margin:0 auto;padding:24px 32px;display:flex;flex-direction:column;gap:20px}}
.kpis{{display:grid;grid-template-columns:repeat(auto-fill,minmax(170px,1fr));gap:10px}}
.kpi{{background:var(--s1);border:1px solid var(--border);border-radius:8px;padding:14px 16px}}
.kpi .lbl{{color:var(--muted);font-size:11px;text-transform:uppercase;letter-spacing:.06em;margin-bottom:4px}}
.kpi .val{{font-size:22px;font-weight:700;letter-spacing:-.02em}}
.pos{{color:var(--green)}} .neg{{color:var(--red)}} .neu{{color:var(--gold)}}
.card{{background:var(--s1);border:1px solid var(--border);border-radius:8px;padding:20px}}
.card h2{{font-size:13px;font-weight:600;color:var(--muted);text-transform:uppercase;letter-spacing:.06em;margin-bottom:16px;padding-bottom:10px;border-bottom:1px solid var(--border)}}
.charts2{{display:grid;grid-template-columns:1fr 1fr;gap:20px}}
canvas{{max-height:260px}}
.table-wrap{{overflow-x:auto}}
table{{width:100%;border-collapse:collapse;font-size:13px}}
th{{text-align:left;padding:8px 12px;background:var(--s2);color:var(--muted);font-size:11px;text-transform:uppercase;letter-spacing:.04em;border-bottom:1px solid var(--border);white-space:nowrap}}
td{{padding:8px 12px;border-bottom:1px solid var(--border)}}
tr:last-child td{{border-bottom:none}}
tr:hover td{{background:var(--s2)}}
.badge{{display:inline-block;padding:2px 7px;border-radius:4px;font-size:11px;font-weight:600}}
.badge.pos{{background:rgba(67,201,126,.15);color:var(--green)}}
.badge.neg{{background:rgba(224,82,82,.15);color:var(--red)}}
@media(max-width:768px){{.charts2{{grid-template-columns:1fr}}.wrap{{padding:16px}}}}
</style>
</head>
<body>

<div class="header">
  <div class="header-icon">⚡</div>
  <div>
    <h1>XAUUSD Range Scalper — Backtest Riport</h1>
    <div class="sub">Tick-szintű szimuláció · Mean-Reversion Scalping</div>
  </div>
</div>

<div class="wrap">

<!-- KPI sor -->
<div class="kpis">
  <div class="kpi"><div class="lbl">Összes PnL</div><div class="val {kpi_color(m.get('total_pnl',0))}">${m.get('total_pnl',0):,.2f}</div></div>
  <div class="kpi"><div class="lbl">Trades</div><div class="val neu">{m.get('n_trades',0):,}</div></div>
  <div class="kpi"><div class="lbl">Win Rate</div><div class="val {kpi_color(m.get('win_rate',0)-.5)}">{m.get('win_rate',0):.1%}</div></div>
  <div class="kpi"><div class="lbl">Profit Factor</div><div class="val {kpi_color(m.get('profit_factor',0)-1)}">{m.get('profit_factor',0):.3f}</div></div>
  <div class="kpi"><div class="lbl">Sharpe</div><div class="val {kpi_color(m.get('sharpe_ratio',0))}">{m.get('sharpe_ratio',0):.3f}</div></div>
  <div class="kpi"><div class="lbl">Sortino</div><div class="val {kpi_color(m.get('sortino_ratio',0))}">{m.get('sortino_ratio',0):.3f}</div></div>
  <div class="kpi"><div class="lbl">Max DD</div><div class="val neg">{m.get('max_drawdown_pct',0):.2f}%</div></div>
  <div class="kpi"><div class="lbl">Expectancy</div><div class="val {kpi_color(m.get('expectancy',0))}">${m.get('expectancy',0):.2f}</div></div>
  <div class="kpi"><div class="lbl">Avg Win</div><div class="val pos">${m.get('avg_win',0):.2f}</div></div>
  <div class="kpi"><div class="lbl">Avg Loss</div><div class="val neg">${m.get('avg_loss',0):.2f}</div></div>
  <div class="kpi"><div class="lbl">Calmar</div><div class="val {kpi_color(m.get('calmar_ratio',0))}">{m.get('calmar_ratio',0):.3f}</div></div>
  <div class="kpi"><div class="lbl">Avg Tartás</div><div class="val neu">{m.get('avg_duration_min',0):.0f} perc</div></div>
</div>

<!-- Equity görbe -->
<div class="card">
  <h2>Kumulatív PnL Görbe (napi)</h2>
  <canvas id="eqChart"></canvas>
</div>

<!-- 2 col charts -->
<div class="charts2">
  <div class="card"><h2>Napi PnL Hisztogram</h2><canvas id="dailyBar"></canvas></div>
  <div class="card"><h2>Kilépési okok</h2><canvas id="exitPie"></canvas></div>
</div>

<!-- Havi bontás -->
<section class="card">
  <h2>Havi Bontás</h2>
  <table>
    <thead><tr><th>Hónap</th><th>PnL</th><th>Trades</th><th>Win Rate</th></tr></thead>
    <tbody>{month_rows}</tbody>
  </table>
</section>

{wf_section}

<!-- Konfiguráció -->
<section class="card">
  <h2>Bot Konfiguráció</h2>
  <div class="table-wrap">
  <table><thead><tr><th>Paraméter</th><th>Érték</th></tr></thead>
  <tbody>{cfg_rows}</tbody></table>
  </div>
</section>

</div><!-- /wrap -->

<script>
const G='#43c97e',R='#e05252',GOLD='#f0b429',B='#4d9eff',MUTED='#7a818c',GRID='#262a2f';
Chart.defaults.color=MUTED; Chart.defaults.borderColor=GRID;

new Chart(document.getElementById('eqChart'),{{
  type:'line',
  data:{{labels:{dates_json},datasets:[{{label:'Cum PnL ($)',data:{cum_pnl_json},
    borderColor:GOLD,backgroundColor:'rgba(240,180,41,0.07)',borderWidth:2,fill:true,tension:.35,pointRadius:0}}]}},
  options:{{responsive:true,maintainAspectRatio:true,
    plugins:{{legend:{{display:false}}}},
    scales:{{x:{{ticks:{{maxTicksLimit:12,maxRotation:0}}}},y:{{grid:{{color:GRID}}}}}}
  }}
}});

const dpv={day_pnl_json};
new Chart(document.getElementById('dailyBar'),{{
  type:'bar',
  data:{{labels:{dates_json},datasets:[{{label:'Napi PnL',data:dpv,
    backgroundColor:dpv.map(v=>v>=0?'rgba(67,201,126,.65)':'rgba(224,82,82,.65)'),borderRadius:2}}]}},
  options:{{responsive:true,maintainAspectRatio:true,
    plugins:{{legend:{{display:false}}}},
    scales:{{x:{{ticks:{{maxTicksLimit:10,maxRotation:0}}}},y:{{grid:{{color:GRID}}}}}}
  }}
}});

new Chart(document.getElementById('exitPie'),{{
  type:'doughnut',
  data:{{labels:{exit_labels},datasets:[{{data:{exit_vals},
    backgroundColor:[G,R,B,'#a37af0'],borderWidth:0,hoverOffset:6}}]}},
  options:{{responsive:true,maintainAspectRatio:true,
    plugins:{{legend:{{position:'right',labels:{{padding:16,font:{{size:12}}}}}}}}
  }}
}});
</script>
</body>
</html>"""

    Path(output_path).write_text(html, encoding="utf-8")
    print(f"✓ HTML riport mentve → {output_path}")
    return output_path

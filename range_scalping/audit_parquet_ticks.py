#!/usr/bin/env python3
import argparse
import json
from pathlib import Path
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go


def detect_time_col(df):
    cols = [c.lower().strip() for c in df.columns]
    mapping = dict(zip(cols, df.columns))
    for c in ["timestamp", "time", "datetime", "date", "ts"]:
        if c in mapping:
            return mapping[c]
    return None


def detect_price_cols(df):
    cols = {c.lower().strip(): c for c in df.columns}
    bid = cols.get("bid")
    ask = cols.get("ask")
    price = cols.get("price") or cols.get("close") or cols.get("last")
    return bid, ask, price


def audit_file(path: Path):
    out = {"file": str(path), "status": "ok", "issues": []}
    try:
        df = pd.read_parquet(path)
        df.rename(columns={"timestamp_utc": "timestamp"}, inplace=True)
    except Exception as e:
        out["status"] = "read_error"
        out["issues"].append(f"read_error: {e}")
        return out

    out["rows"] = int(len(df))
    out["columns"] = list(df.columns)

    if df.empty:
        out["status"] = "empty"
        out["issues"].append("empty_file")
        return out

    tcol = detect_time_col(df)
    out["time_col"] = tcol
    if tcol is None:
        out["status"] = "schema_error"
        out["issues"].append("missing_time_column")
        return out

    ts = pd.to_datetime(df[tcol], utc=True, errors="coerce")
    out["null_timestamps"] = int(ts.isna().sum())
    ts_valid = ts.dropna().sort_values()

    if ts_valid.empty:
        out["status"] = "schema_error"
        out["issues"].append("all_timestamps_invalid")
        return out

    out["start"] = ts_valid.iloc[0].isoformat()
    out["end"] = ts_valid.iloc[-1].isoformat()
    out["duration_hours"] = round((ts_valid.iloc[-1] - ts_valid.iloc[0]).total_seconds() / 3600, 2)
    out["duplicates"] = int(ts_valid.duplicated().sum())

    diffs = ts_valid.diff().dropna()
    if len(diffs):
        out["median_gap_ms"] = float(diffs.median().total_seconds() * 1000)
        out["max_gap_sec"] = float(diffs.max().total_seconds())
        out["p99_gap_sec"] = float(diffs.quantile(0.99).total_seconds())
    else:
        out["median_gap_ms"] = None
        out["max_gap_sec"] = None
        out["p99_gap_sec"] = None

    bid, ask, price = detect_price_cols(df)
    out["bid_col"], out["ask_col"], out["price_col"] = bid, ask, price

    if bid and ask:
        b = pd.to_numeric(df[bid], errors="coerce")
        a = pd.to_numeric(df[ask], errors="coerce")
        spread = a - b
        out["negative_spread_rows"] = int((spread < 0).sum())
        out["zero_spread_rows"] = int((spread == 0).sum())
        out["spread_mean"] = float(spread.mean()) if len(spread) else None
        out["spread_p99"] = float(spread.quantile(0.99)) if len(spread) else None
        if (spread < 0).sum() > 0:
            out["issues"].append("negative_spread_present")
    elif not price:
        out["issues"].append("missing_bid_ask_or_price")

    if out["duplicates"] > 0:
        out["issues"].append("duplicate_timestamps")
    if out["null_timestamps"] > 0:
        out["issues"].append("null_timestamps")
    if out.get("max_gap_sec") and out["max_gap_sec"] > 60:
        out["issues"].append("large_time_gap_over_60s")

    usable = True
    reason = []
    if out["status"] != "ok":
        usable = False
        reason.append(out["status"])
    if "missing_time_column" in out["issues"] or "all_timestamps_invalid" in out["issues"]:
        usable = False
        reason.append("invalid_time_axis")
    if "missing_bid_ask_or_price" in out["issues"]:
        usable = False
        reason.append("missing_price_columns")

    out["usable_for_backtest"] = usable
    out["usable_reason"] = ", ".join(reason) if reason else ("usable_with_caveats" if out["issues"] else "usable")
    return out


def build_report(data_dir: str, pattern: str, output_dir: str):
    data_dir = Path(data_dir)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    files = sorted(data_dir.glob(pattern))
    audits = [audit_file(f) for f in files]
    df = pd.DataFrame(audits)
    df.to_csv(output_dir / "audit_summary.csv", index=False)

    if df.empty:
        (output_dir / "audit_report.md").write_text("# Audit report\n\nNo files found.\n", encoding="utf-8")
        return

    cov_rows = []
    for a in audits:
        if a.get("start") and a.get("end"):
            cov_rows.append({
                "file": Path(a["file"]).name,
                "start": pd.to_datetime(a["start"]),
                "end": pd.to_datetime(a["end"]),
                "rows": a.get("rows", 0),
            })
    cov = pd.DataFrame(cov_rows)

    if not cov.empty:
        fig = go.Figure()
        for _, r in cov.iterrows():
            fig.add_trace(go.Scatter(
                x=[r["start"], r["end"]],
                y=[r["file"], r["file"]],
                mode="lines",
                line=dict(width=10),
                hovertemplate="%{y}<br>%{x}<extra></extra>",
            ))
        fig.update_layout(title="File coverage")
        fig.update_xaxes(title_text="Time")
        fig.update_yaxes(title_text="File")
        fig.write_html(output_dir / "coverage.html")

    tmp = df.copy()
    tmp["file_name"] = tmp["file"].map(lambda x: Path(x).name)

    fig = px.bar(tmp, x="file_name", y="rows", title="Rows per file", hover_data=["usable_for_backtest"])
    fig.write_html(output_dir / "rows_per_file.html")

    if "max_gap_sec" in df.columns:
        fig = px.bar(tmp, x="file_name", y="max_gap_sec", title="Max gap by file", hover_data=["usable_for_backtest"])
        fig.write_html(output_dir / "max_gap.html")

    issues = []
    for _, r in df.iterrows():
        if r.get("issues") not in [None, "", "[]"]:
            issues.append(f"- {Path(r['file']).name}: {r.get('issues')}")

    summary_cols = [
        c for c in [
            "file", "rows", "time_col", "start", "end", "duration_hours",
            "duplicates", "null_timestamps", "max_gap_sec",
            "usable_for_backtest", "usable_reason"
        ] if c in df.columns
    ]

    issue_lines = issues if issues else ["- No major issues detected."]
    lines = [
        "# Tick data audit report",
        "",
        f"- Files found: {len(df)}",
        f"- Usable for backtest: {int(df['usable_for_backtest'].fillna(False).sum())}/{len(df)}",
        "",
        "## Per-file summary",
        "",
        df[summary_cols].to_markdown(index=False),
        "",
        "## Issues",
        "",
        *issue_lines,
        "",
        "## Notes",
        "",
        "- `max_gap_sec > 60` usually means missing data or market/session gaps.",
        "- Negative spread means bid/ask corruption or wrong column mapping.",
        "- Duplicate timestamps should usually be deduplicated before backtesting.",
    ]
    (output_dir / "audit_report.md").write_text("\n".join(lines), encoding="utf-8")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--data_dir", required=True)
    p.add_argument("--pattern", default="*.parquet")
    p.add_argument("--output_dir", default="output/data_audit")
    args = p.parse_args()
    build_report(args.data_dir, args.pattern, args.output_dir)
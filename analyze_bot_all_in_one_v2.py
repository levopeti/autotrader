import os
import math
import argparse
import pandas as pd
import numpy as np
from pathlib import Path

TRADE_JOURNAL_CSV = os.getenv("TRADE_JOURNAL_CSV", "trade_journal.csv")
BOT_STATE_CSV = os.getenv("BOT_STATE_CSV", "bot_state.csv")
POSITION_TRACKER_CSV = os.getenv("POSITION_TRACKER_CSV", "position_tracker.csv")
CLOSED_TRADES_CSV = os.getenv("CLOSED_TRADES_CSV", "closed_trades.csv")
OUTPUT_PREFIX = os.getenv("OUTPUT_PREFIX", "bot_diagnostics_v2")


def load_csv(path):
    p = Path(path)
    if not p.exists():
        return pd.DataFrame()
    df = pd.read_csv(p)
    if df.empty:
        return df
    for col in ["timestamp_utc", "open_time_utc", "close_time_utc"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], utc=True, errors="coerce")
    return df


def parse_note_value(note, key):
    if pd.isna(note):
        return None
    parts = str(note).split(";")
    for part in parts:
        if part.startswith(f"{key}="):
            return part.split("=", 1)[1]
    return None


def prepare_state(df):
    if df.empty:
        return df
    df = df.copy()
    df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], utc=True, errors="coerce")
    for col in ["price", "atr", "spread", "tick_imbalance", "daily_trades", "daily_pnl_est"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    df["spread_atr_ratio"] = df["spread"] / df["atr"]
    df["ht_tick_aligned"] = (
        ((df["direction"] == "BUY") & (df["tick_bias"] == "BUY")) |
        ((df["direction"] == "SELL") & (df["tick_bias"] == "SELL"))
    )
    df["potential_entry"] = (
        df["direction"].isin(["BUY", "SELL"]) &
        df["ht_tick_aligned"] &
        (df["spread_atr_ratio"] <= 0.20)
    )
    df["hour"] = df["timestamp_utc"].dt.hour
    return df


def prepare_trades(df):
    if df.empty:
        return df
    df = df.copy()
    if "timestamp_utc" in df.columns:
        df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], utc=True, errors="coerce")
    for col in ["price", "atr", "spread", "size"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def enrich_open_trades(trades):
    if trades.empty or "event" not in trades.columns:
        return pd.DataFrame()
    opens = trades[trades["event"] == "OPEN"].copy()
    if opens.empty:
        return opens
    key_map = {
        "stopDistance": "stop_distance",
        "spread_atr": "spread_atr",
        "DRY_RUN": "dry_run",
        "trailingStop": "trailing_stop",
        "dealReference": "deal_reference",
        "dealId": "deal_id",
        "score": "score",
        "sizeMultiplier": "size_multiplier",
        "finalSize": "final_size",
        "withinHours": "within_hours",
    }
    for raw_key, col in key_map.items():
        opens[col] = opens["note"].apply(lambda x, kk=raw_key: parse_note_value(x, kk)) if "note" in opens.columns else None
    for col in ["stop_distance", "spread_atr", "score", "size_multiplier", "final_size"]:
        if col in opens.columns:
            opens[col] = pd.to_numeric(opens[col], errors="coerce")
    return opens


def nearest_state_snapshot(open_time, states, seconds=20):
    if states.empty:
        return None
    tmp = states.copy()
    tmp["dt_diff"] = (tmp["timestamp_utc"] - open_time).abs().dt.total_seconds()
    tmp = tmp[tmp["dt_diff"] <= seconds].sort_values("dt_diff")
    if tmp.empty:
        return None
    return tmp.iloc[0]


def evaluate_open_trades(opens, states):
    rows = []
    if opens.empty:
        return pd.DataFrame()
    for _, tr in opens.iterrows():
        snap = nearest_state_snapshot(tr["timestamp_utc"], states)
        tick_bias = snap["tick_bias"] if snap is not None and "tick_bias" in snap else None
        tick_imbalance = snap["tick_imbalance"] if snap is not None and "tick_imbalance" in snap else None
        action = snap["action"] if snap is not None and "action" in snap else None
        bias = snap["bias"] if snap is not None and "bias" in snap else None
        atr = float(tr["atr"]) if "atr" in tr and not pd.isna(tr["atr"]) else (float(snap["atr"]) if snap is not None and "atr" in snap else math.nan)
        spread = float(tr["spread"]) if "spread" in tr and not pd.isna(tr["spread"]) else (float(snap["spread"]) if snap is not None and "spread" in snap else math.nan)
        stop_distance = tr["stop_distance"] if "stop_distance" in tr else math.nan
        stop_atr_mult_est = stop_distance / atr if pd.notna(stop_distance) and pd.notna(atr) and atr != 0 else math.nan
        spread_stop_ratio = spread / stop_distance if pd.notna(stop_distance) and stop_distance != 0 and pd.notna(spread) else math.nan
        rows.append({
            "open_time_utc": tr["timestamp_utc"],
            "hour": tr["timestamp_utc"].hour if pd.notna(tr["timestamp_utc"]) else np.nan,
            "direction": tr.get("direction"),
            "price": tr.get("price"),
            "size": tr.get("size"),
            "action": action,
            "bias": bias,
            "tick_bias": tick_bias,
            "tick_imbalance": tick_imbalance,
            "atr": atr,
            "spread": spread,
            "stop_distance": stop_distance,
            "stop_atr_mult_est": stop_atr_mult_est,
            "spread_atr": tr.get("spread_atr"),
            "spread_stop_ratio": spread_stop_ratio,
            "dry_run": tr.get("dry_run"),
            "deal_reference": tr.get("deal_reference"),
            "deal_id": tr.get("deal_id"),
            "score": tr.get("score"),
            "size_multiplier": tr.get("size_multiplier"),
            "final_size": tr.get("final_size"),
            "within_hours": tr.get("within_hours"),
        })
    return pd.DataFrame(rows)


def validate_pipeline(tracker, closed, journal):
    issues = []
    if tracker.empty:
        issues.append({"severity": "WARN", "check": "tracker_exists", "detail": "position_tracker.csv is empty or missing"})
    if closed.empty:
        issues.append({"severity": "WARN", "check": "closed_exists", "detail": "closed_trades.csv is empty or missing"})
    if not tracker.empty and "deal_id" in tracker.columns:
        dup_tracker = tracker[tracker.duplicated(subset=["deal_id"], keep=False)]
        if not dup_tracker.empty:
            issues.append({"severity": "ERROR", "check": "duplicate_tracker_deal_id", "detail": f"Duplicate deal_id rows in tracker: {dup_tracker['deal_id'].astype(str).nunique()} ids"})
        if "status" in tracker.columns:
            issues.append({"severity": "INFO", "check": "tracker_open_count", "detail": f"OPEN tracker rows: {len(tracker[tracker['status'].astype(str) == 'OPEN'])}"})
            issues.append({"severity": "INFO", "check": "tracker_closed_count", "detail": f"CLOSED tracker rows: {len(tracker[tracker['status'].astype(str) == 'CLOSED'])}"})
    if not closed.empty:
        for col in ["entry_price", "exit_price", "size", "pnl", "hold_minutes"]:
            if col in closed.columns:
                closed[col] = pd.to_numeric(closed[col], errors="coerce")
        if "deal_id" in closed.columns:
            dup_closed = closed[closed.duplicated(subset=["deal_id"], keep=False)]
            if not dup_closed.empty:
                issues.append({"severity": "ERROR", "check": "duplicate_closed_deal_id", "detail": f"Duplicate deal_id rows in closed_trades: {dup_closed['deal_id'].astype(str).nunique()} ids"})
        if "hold_minutes" in closed.columns:
            neg_hold = closed[closed["hold_minutes"] < 0]
            if not neg_hold.empty:
                issues.append({"severity": "ERROR", "check": "negative_hold_time", "detail": f"Rows with negative hold time: {len(neg_hold)}"})
        if set(["direction", "entry_price", "exit_price", "size", "pnl"]).issubset(closed.columns):
            recalc = []
            for _, r in closed.iterrows():
                if pd.isna(r["entry_price"]) or pd.isna(r["exit_price"]) or pd.isna(r["size"]) or pd.isna(r["pnl"]):
                    recalc.append(np.nan)
                elif str(r["direction"]).upper() == "BUY":
                    recalc.append((r["exit_price"] - r["entry_price"]) * r["size"])
                elif str(r["direction"]).upper() == "SELL":
                    recalc.append((r["entry_price"] - r["exit_price"]) * r["size"])
                else:
                    recalc.append(np.nan)
            closed["pnl_recalc"] = recalc
            comparable = closed.dropna(subset=["pnl", "pnl_recalc"])
            if not comparable.empty:
                diff_rows = comparable[(comparable["pnl"] - comparable["pnl_recalc"]).abs() > 1e-6]
                issues.append({"severity": "INFO", "check": "pnl_recalc_comparable", "detail": f"Rows comparable for pnl recalculation: {len(comparable)}"})
                issues.append({"severity": "INFO", "check": "pnl_recalc_diff_rows", "detail": f"Rows where logged pnl != simple recalculated pnl: {len(diff_rows)}"})
    if not tracker.empty and not closed.empty and "deal_id" in tracker.columns and "deal_id" in closed.columns:
        tracker_ids = set(tracker["deal_id"].dropna().astype(str))
        closed_ids = set(closed["deal_id"].dropna().astype(str))
        if "status" in tracker.columns:
            missing_in_closed = tracker[(tracker["status"].astype(str) == "CLOSED") & (~tracker["deal_id"].astype(str).isin(closed_ids))]
            if not missing_in_closed.empty:
                issues.append({"severity": "WARN", "check": "closed_tracker_missing_in_closed_csv", "detail": f"Tracker CLOSED rows missing from closed_trades.csv: {len(missing_in_closed)}"})
        orphan_closed = closed[~closed["deal_id"].astype(str).isin(tracker_ids)]
        if not orphan_closed.empty:
            issues.append({"severity": "WARN", "check": "orphan_closed_rows", "detail": f"closed_trades rows with no tracker match: {len(orphan_closed)}"})
    if not journal.empty and "event" in journal.columns:
        open_events = journal[journal["event"].astype(str) == "OPEN"]
        issues.append({"severity": "INFO", "check": "journal_open_events", "detail": f"OPEN events in trade_journal: {len(open_events)}"})
        if not closed.empty:
            issues.append({"severity": "INFO", "check": "closed_rows_count", "detail": f"Rows in closed_trades.csv: {len(closed)}"})
    return pd.DataFrame(issues)


def make_diagnosis_summary(state, open_analysis, issues, closed):
    rows = []
    if not state.empty:
        align_ratio = float(state["ht_tick_aligned"].mean())
        pot_ratio = float(state["potential_entry"].mean())
        spread_ratio = float(state["spread_atr_ratio"].mean())
        rows.append({"topic": "alignment_ratio", "value": round(align_ratio, 4), "diagnosis": "low" if align_ratio < 0.2 else "medium" if align_ratio < 0.5 else "high", "suggestion": "Lazíts a tick szűrőn vagy növeld a decision intervallumot." if align_ratio < 0.2 else "Az alignment arány rendben van."})
        rows.append({"topic": "potential_entry_ratio", "value": round(pot_ratio, 4), "diagnosis": "low" if pot_ratio < 0.05 else "medium" if pot_ratio < 0.2 else "high", "suggestion": "Túl kevés setup; érdemes lazítani a combined filteren." if pot_ratio < 0.05 else "Túl sok setup; érdemes szigorítani a reversal vagy spread szűrőt." if pot_ratio > 0.2 else "A setup arány első ránézésre egészséges."})
        rows.append({"topic": "avg_spread_atr_ratio", "value": round(spread_ratio, 4), "diagnosis": "high" if spread_ratio > 0.2 else "ok", "suggestion": "A spread/ATR magas; szigorúbb spread filter vagy jobb session lehet indokolt." if spread_ratio > 0.2 else "A spread/ATR átlag kezelhető."})
    if not open_analysis.empty:
        avg_stop_mult = float(open_analysis["stop_atr_mult_est"].mean()) if "stop_atr_mult_est" in open_analysis else np.nan
        avg_spread_stop = float(open_analysis["spread_stop_ratio"].mean()) if "spread_stop_ratio" in open_analysis else np.nan
        if pd.notna(avg_stop_mult):
            rows.append({"topic": "avg_stop_atr_mult", "value": round(avg_stop_mult, 4), "diagnosis": "tight" if avg_stop_mult < 1.0 else "medium" if avg_stop_mult <= 1.5 else "wide", "suggestion": "A trailing stop túl szűk lehet." if avg_stop_mult < 1.0 else "A trailing stop tartomány rendben van." if avg_stop_mult <= 1.5 else "A trailing stop elég tág; ellenőrizd a profitvédelem lassulását."})
        if pd.notna(avg_spread_stop):
            rows.append({"topic": "avg_spread_stop_ratio", "value": round(avg_spread_stop, 4), "diagnosis": "high" if avg_spread_stop > 0.25 else "medium" if avg_spread_stop > 0.10 else "low", "suggestion": "A spread túl nagy a stophoz képest; lehet túl szoros a trade struktúra." if avg_spread_stop > 0.25 else "A spread érzékelhető, de még kezelhető." if avg_spread_stop > 0.10 else "A spread alacsony a stophoz képest."})
        if "size_multiplier" in open_analysis.columns and open_analysis["size_multiplier"].notna().any():
            mult_mean = float(open_analysis["size_multiplier"].mean())
            rows.append({"topic": "avg_size_multiplier", "value": round(mult_mean, 4), "diagnosis": "conservative" if mult_mean < 1.0 else "balanced" if mult_mean <= 1.25 else "aggressive", "suggestion": "A sizing inkább konzervatív." if mult_mean < 1.0 else "A sizing kiegyensúlyozott." if mult_mean <= 1.25 else "A sizing elég agresszív; figyeld a drawdownt."})
    if not issues.empty:
        err_cnt = int((issues["severity"] == "ERROR").sum())
        warn_cnt = int((issues["severity"] == "WARN").sum())
        rows.append({"topic": "pipeline_errors", "value": err_cnt, "diagnosis": "bad" if err_cnt > 0 else "ok", "suggestion": "Javítsd a closed trade pipeline hibáit a live elemzés előtt." if err_cnt > 0 else "Nincs kritikus pipeline hiba."})
        rows.append({"topic": "pipeline_warnings", "value": warn_cnt, "diagnosis": "warn" if warn_cnt > 0 else "ok", "suggestion": "Ellenőrizd a hiányzó vagy árván maradt sorokat." if warn_cnt > 0 else "Nincs jelentős pipeline warning."})
    if not closed.empty and "pnl" in closed.columns:
        closed = closed.copy()
        closed["pnl"] = pd.to_numeric(closed["pnl"], errors="coerce")
        rows.append({"topic": "closed_trade_count", "value": int(closed["pnl"].notna().sum()), "diagnosis": "low_sample" if len(closed) < 20 else "usable", "suggestion": "Még kevés a lezárt trade a megbízható session-döntéshez." if len(closed) < 20 else "Már van használható minta a time-of-day elemzéshez."})
        rows.append({"topic": "closed_total_pnl", "value": round(float(closed["pnl"].sum()), 4), "diagnosis": "positive" if float(closed["pnl"].sum()) > 0 else "negative" if float(closed["pnl"].sum()) < 0 else "flat", "suggestion": "A rendszer eddig pozitív lezárt PnL-t mutat." if float(closed["pnl"].sum()) > 0 else "A rendszer eddig negatív lezárt PnL-t mutat; session és signal filter felülvizsgálat indokolt." if float(closed["pnl"].sum()) < 0 else "A lezárt PnL egyelőre lapos."})
    return pd.DataFrame(rows)


def print_section(title):
    print("\n" + "=" * 80)
    print(title)
    print("=" * 80)


def print_state_report(state, trades):
    if state.empty:
        print_section("STATE REPORT")
        print("bot_state.csv missing or empty")
        return
    print_section("ALAP STATS")
    print(f"Rows in bot_state.csv: {len(state)}")
    print(f"Time range: {state['timestamp_utc'].min()} -> {state['timestamp_utc'].max()}")
    print_section("ACTION COUNTS")
    print(state["action"].value_counts(dropna=False))
    print_section("BIAS COUNTS")
    print(state["bias"].value_counts(dropna=False))
    print_section("TICK BIAS COUNTS")
    print(state["tick_bias"].value_counts(dropna=False))
    print_section("HT-TICK ALIGNMENT")
    print(state["ht_tick_aligned"].value_counts(dropna=False))
    print("\nAlignment ratio:")
    print(state["ht_tick_aligned"].value_counts(normalize=True).round(4))
    print_section("POTENTIAL ENTRIES")
    print(state["potential_entry"].value_counts(dropna=False))
    print("\nPotential entry ratio:")
    print(state["potential_entry"].value_counts(normalize=True).round(4))
    print_section("AVG SPREAD/ATR RATIO BY ACTION")
    print(state.groupby("action")["spread_atr_ratio"].mean().sort_values().round(4))
    print_section("AVG TICK IMBALANCE BY DIRECTION")
    print(state.groupby("direction")["tick_imbalance"].mean().round(4))
    print_section("POTENTIAL ENTRIES BY HOUR UTC")
    print(state.loc[state["potential_entry"]].groupby("hour").size().sort_values(ascending=False))
    print_section("TREND VOLT, DE TICK NEM ERŐSÍTETTE MEG")
    stalled = state[state["direction"].isin(["BUY", "SELL"]) & (~state["ht_tick_aligned"])][[
        "timestamp_utc", "action", "bias", "direction", "tick_bias", "tick_imbalance", "atr", "spread", "spread_atr_ratio"
    ]]
    print(stalled.tail(20).to_string(index=False))
    print_section("ALIGNMENT + JÓ SPREAD + AKCIÓ")
    good = state[state["potential_entry"]][[
        "timestamp_utc", "action", "bias", "direction", "tick_bias", "tick_imbalance", "atr", "spread", "spread_atr_ratio"
    ]]
    print(good.tail(20).to_string(index=False))
    if not trades.empty:
        print_section("TRADE JOURNAL ÖSSZEFOGLALÓ")
        if "event" in trades.columns:
            print(trades["event"].value_counts(dropna=False))
        print(f"Rows in trade_journal.csv: {len(trades)}")
        opens = trades[trades["event"] == "OPEN"].copy() if "event" in trades.columns else pd.DataFrame()
        if not opens.empty and "timestamp_utc" in opens.columns:
            opens["hour"] = opens["timestamp_utc"].dt.hour
            print("\nOPEN trades by hour UTC:")
            print(opens.groupby("hour").size().sort_values(ascending=False))


def print_open_trade_report(df):
    print_section("OPEN TRADE ANALYSIS")
    if df.empty:
        print("No OPEN trades found.")
        return
    print(f"Rows: {len(df)}")
    print(f"Time range: {df['open_time_utc'].min()} -> {df['open_time_utc'].max()}")
    print()
    print("AVERAGES")
    print(df[["atr", "spread", "stop_distance", "stop_atr_mult_est", "spread_atr", "spread_stop_ratio"]].mean(numeric_only=True).round(4))
    print()
    print("BY DIRECTION")
    print(df.groupby("direction")[["stop_distance", "stop_atr_mult_est", "spread_atr", "spread_stop_ratio"]].mean(numeric_only=True).round(4))
    print()
    cols = [c for c in ["hour", "direction", "action", "tick_bias", "tick_imbalance", "stop_distance", "stop_atr_mult_est", "score", "size_multiplier", "size"] if c in df.columns]
    print("TICK ALIGNMENT SNAPSHOT")
    print(df[cols].to_string(index=False))
    if "score" in df.columns:
        print()
        print("SCORE BY HOUR")
        print(df.groupby("hour")[["score", "size_multiplier", "size"]].mean(numeric_only=True).round(4))


def print_validation_report(issues, tracker, closed):
    print_section("CLOSED TRADE VALIDATION")
    print(f"Tracker rows: {len(tracker)}")
    print(f"Closed rows: {len(closed)}")
    print()
    if issues.empty:
        print("No issues found.")
        return
    for severity in ["ERROR", "WARN", "INFO"]:
        subset = issues[issues["severity"] == severity]
        if subset.empty:
            continue
        print(severity)
        print(subset[["check", "detail"]].to_string(index=False))
        print()


def print_diagnosis(summary):
    print_section("DIAGNOSIS SUMMARY")
    if summary.empty:
        print("No diagnosis available.")
        return
    print(summary[["topic", "value", "diagnosis", "suggestion"]].to_string(index=False))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bot-state-csv", default=BOT_STATE_CSV)
    parser.add_argument("--trade-journal-csv", default=TRADE_JOURNAL_CSV)
    parser.add_argument("--position-tracker-csv", default=POSITION_TRACKER_CSV)
    parser.add_argument("--closed-trades-csv", default=CLOSED_TRADES_CSV)
    parser.add_argument("--output-prefix", default=OUTPUT_PREFIX)
    args = parser.parse_args()

    state = prepare_state(load_csv(args.bot_state_csv))
    trades = prepare_trades(load_csv(args.trade_journal_csv))
    tracker = load_csv(args.position_tracker_csv)
    closed = load_csv(args.closed_trades_csv)
    opens = enrich_open_trades(trades)
    open_analysis = evaluate_open_trades(opens, state)
    issues = validate_pipeline(tracker, closed, trades)
    summary = make_diagnosis_summary(state, open_analysis, issues, closed)

    print_state_report(state, trades)
    print_open_trade_report(open_analysis)
    print_validation_report(issues, tracker, closed)
    print_diagnosis(summary)

    base = args.output_prefix
    if not state.empty:
        state.to_csv(f"{base}_state_prepared.csv", index=False)
    if not open_analysis.empty:
        open_analysis.to_csv(f"{base}_open_trade_analysis.csv", index=False)
    if not issues.empty:
        issues.to_csv(f"{base}_validation.csv", index=False)
    if not summary.empty:
        summary.to_csv(f"{base}_diagnosis_summary.csv", index=False)


if __name__ == "__main__":
    main()

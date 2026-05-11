import os
import math
import argparse
import pandas as pd
import numpy as np
from pathlib import Path

TRADE_JOURNAL_CSV = os.getenv("TRADE_JOURNAL_CSV", "./logs/trade_journal_multi.csv")
BOT_STATE_CSV = os.getenv("BOT_STATE_CSV", "./logs/bot_state_multi.csv")
POSITION_TRACKER_CSV = os.getenv("POSITION_TRACKER_CSV", "./logs/position_tracker_multi.csv")
CLOSED_TRADES_CSV = os.getenv("CLOSED_TRADES_CSV", "./logs/closed_trades_multi.csv")
OUTPUT_PREFIX = os.getenv("OUTPUT_PREFIX", "./logs/bot_diagnostics_multi_v2")


def load_csv(path):
    p = Path(path)
    if not p.exists():
        return pd.DataFrame()
    df = pd.read_csv(p, dtype=str, keep_default_na=False)
    if df.empty:
        return df
    for col in ["timestamp_utc", "open_time_utc", "close_time_utc"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], utc=True, errors="coerce")
    return df


def parse_note_value(note, key):
    if pd.isna(note) or note is None:
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
    if "timestamp_utc" in df.columns:
        df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], utc=True, errors="coerce")
    for col in ["price", "atr", "spread", "tick_imbalance", "score", "size"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    if "allow_trade" in df.columns:
        df["allow_trade"] = df["allow_trade"].astype(str).str.lower().map({"true": True, "false": False})
    df["spread_atr_ratio"] = df["spread"] / df["atr"] if set(["spread", "atr"]).issubset(df.columns) else np.nan
    df["ht_tick_aligned"] = (
        ((df.get("direction") == "BUY") & (df.get("tick_bias") == "BUY")) |
        ((df.get("direction") == "SELL") & (df.get("tick_bias") == "SELL"))
    )
    max_spread = 0.20
    df["potential_entry"] = (
        df.get("direction").isin(["BUY", "SELL"]) &
        df["ht_tick_aligned"] &
        (df["spread_atr_ratio"] <= max_spread)
    )
    if "timestamp_utc" in df.columns:
        df["hour"] = df["timestamp_utc"].dt.hour
    if "instrument" not in df.columns:
        df["instrument"] = "UNKNOWN"
    return df


def prepare_trades(df):
    if df.empty:
        return df
    df = df.copy()
    if "timestamp_utc" in df.columns:
        df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], utc=True, errors="coerce")
    for col in ["price", "atr", "spread", "size", "score"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    if "instrument" not in df.columns:
        df["instrument"] = "UNKNOWN"
    return df


def prepare_tracker(df):
    if df.empty:
        return df
    df = df.copy()
    for col in ["open_time_utc", "close_time_utc"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], utc=True, errors="coerce")
    for col in ["entry_price", "exit_price", "size", "atr", "stop_distance", "score"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    if "instrument" not in df.columns:
        df["instrument"] = "UNKNOWN"
    return df


def prepare_closed(df):
    if df.empty:
        return df
    df = df.copy()
    for col in ["open_time_utc", "close_time_utc"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], utc=True, errors="coerce")
    for col in ["entry_price", "exit_price", "size", "pnl", "hold_minutes"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    if "instrument" not in df.columns:
        df["instrument"] = "UNKNOWN"
    return df


def enrich_open_trades(trades):
    if trades.empty or "event" not in trades.columns:
        return pd.DataFrame()
    opens = trades[trades["event"].astype(str) == "OPEN"].copy()
    if opens.empty:
        return opens
    key_map = {
        "stopDistance": "stop_distance_old",
        "spread_atr": "spread_atr_old",
        "DRY_RUN": "dry_run_old",
        "trailingStop": "trailing_stop_old",
        "dealReference": "deal_reference_old",
        "dealId": "deal_id_old",
        "score": "score_from_note",
        "sizeMultiplier": "size_multiplier",
        "finalSize": "final_size",
        "withinHours": "within_hours",
        "spread_ratio": "spread_ratio_note",
        "stop_distance": "stop_distance",
        "deal_reference": "deal_reference",
    }
    if "note" in opens.columns:
        for raw_key, col in key_map.items():
            opens[col] = opens["note"].apply(lambda x, kk=raw_key: parse_note_value(x, kk))
    for col in ["stop_distance", "stop_distance_old", "spread_atr_old", "score_from_note", "size_multiplier", "final_size", "spread_ratio_note"]:
        if col in opens.columns:
            opens[col] = pd.to_numeric(opens[col], errors="coerce")
    if "deal_reference" not in opens.columns and "deal_reference_old" in opens.columns:
        opens["deal_reference"] = opens["deal_reference_old"]
    return opens


def nearest_state_snapshot(open_time, states, instrument=None, seconds=20):
    if states.empty or pd.isna(open_time):
        return None
    tmp = states.copy()
    if instrument is not None and "instrument" in tmp.columns:
        tmp = tmp[tmp["instrument"] == instrument].copy()
    if tmp.empty:
        return None
    tmp["dt_diff"] = (tmp["timestamp_utc"] - open_time).abs().dt.total_seconds()
    tmp = tmp[tmp["dt_diff"] <= seconds].sort_values("dt_diff")
    if tmp.empty:
        return None
    return tmp.iloc[0]


def evaluate_open_trades(opens, states, tracker=None):
    rows = []
    if opens.empty:
        return pd.DataFrame()
    tracker_map = {}
    if tracker is not None and not tracker.empty and "deal_reference" in tracker.columns:
        tracker_map = tracker.set_index(tracker["deal_reference"].astype(str), drop=False).to_dict("index")
    for _, tr in opens.iterrows():
        instrument = tr.get("instrument")
        snap = nearest_state_snapshot(tr.get("timestamp_utc"), states, instrument=instrument)
        tick_bias = snap["tick_bias"] if snap is not None and "tick_bias" in snap else None
        tick_imbalance = snap["tick_imbalance"] if snap is not None and "tick_imbalance" in snap else None
        action = snap["action"] if snap is not None and "action" in snap else None
        bias = snap["bias"] if snap is not None and "bias" in snap else None
        atr = float(tr["atr"]) if "atr" in tr and not pd.isna(tr["atr"]) else (float(snap["atr"]) if snap is not None and "atr" in snap and pd.notna(snap["atr"]) else math.nan)
        spread = float(tr["spread"]) if "spread" in tr and not pd.isna(tr["spread"]) else (float(snap["spread"]) if snap is not None and "spread" in snap and pd.notna(snap["spread"]) else math.nan)
        deal_reference = tr.get("deal_reference")
        tracker_row = tracker_map.get(str(deal_reference)) if deal_reference is not None else None
        stop_distance = tr.get("stop_distance")
        if pd.isna(stop_distance) and tracker_row is not None:
            stop_distance = pd.to_numeric(tracker_row.get("stop_distance"), errors="coerce")
        score = tr.get("score")
        if pd.isna(score):
            score = tr.get("score_from_note")
        if pd.isna(score) and tracker_row is not None:
            score = pd.to_numeric(tracker_row.get("score"), errors="coerce")
        stop_atr_mult_est = stop_distance / atr if pd.notna(stop_distance) and pd.notna(atr) and atr != 0 else math.nan
        spread_atr = tr.get("spread_ratio_note") if "spread_ratio_note" in tr and pd.notna(tr.get("spread_ratio_note")) else (spread / atr if pd.notna(spread) and pd.notna(atr) and atr != 0 else math.nan)
        spread_stop_ratio = spread / stop_distance if pd.notna(stop_distance) and stop_distance != 0 and pd.notna(spread) else math.nan
        rows.append({
            "instrument": instrument,
            "open_time_utc": tr.get("timestamp_utc"),
            "hour": tr.get("timestamp_utc").hour if pd.notna(tr.get("timestamp_utc")) else np.nan,
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
            "spread_atr": spread_atr,
            "spread_stop_ratio": spread_stop_ratio,
            "deal_reference": deal_reference,
            "deal_id": tr.get("deal_id") if "deal_id" in tr else (tracker_row.get("deal_id") if tracker_row else None),
            "score": score,
            "size_multiplier": tr.get("size_multiplier"),
            "final_size": tr.get("final_size"),
            "within_hours": tr.get("within_hours"),
            "tracker_status": tracker_row.get("status") if tracker_row else None,
        })
    return pd.DataFrame(rows)


def validate_pipeline(tracker, closed, journal):
    issues = []
    if tracker.empty:
        issues.append({"severity": "WARN", "check": "tracker_exists", "detail": "position_tracker csv is empty or missing"})
    if closed.empty:
        issues.append({"severity": "WARN", "check": "closed_exists", "detail": "closed_trades csv is empty or missing"})
    if not tracker.empty and "deal_reference" in tracker.columns:
        dup_ref = tracker[tracker.duplicated(subset=["deal_reference"], keep=False)]
        if not dup_ref.empty:
            issues.append({"severity": "ERROR", "check": "duplicate_tracker_deal_reference", "detail": f"Duplicate deal_reference rows in tracker: {dup_ref['deal_reference'].astype(str).nunique()} ids"})
        if "deal_id" in tracker.columns:
            dup_tracker = tracker[tracker["deal_id"].notna() & tracker.duplicated(subset=["deal_id"], keep=False)]
            if not dup_tracker.empty:
                issues.append({"severity": "ERROR", "check": "duplicate_tracker_deal_id", "detail": f"Duplicate deal_id rows in tracker: {dup_tracker['deal_id'].astype(str).nunique()} ids"})
        if "status" in tracker.columns:
            issues.append({"severity": "INFO", "check": "tracker_open_count", "detail": f"OPEN tracker rows: {len(tracker[tracker['status'].astype(str) == 'OPEN'])}"})
            issues.append({"severity": "INFO", "check": "tracker_closed_count", "detail": f"CLOSED tracker rows: {len(tracker[tracker['status'].astype(str) == 'CLOSED'])}"})
    if not closed.empty:
        if "deal_reference" in closed.columns:
            dup_closed_ref = closed[closed["deal_reference"].notna() & closed.duplicated(subset=["deal_reference"], keep=False)]
            if not dup_closed_ref.empty:
                issues.append({"severity": "ERROR", "check": "duplicate_closed_deal_reference", "detail": f"Duplicate deal_reference rows in closed_trades: {dup_closed_ref['deal_reference'].astype(str).nunique()} ids"})
        if "deal_id" in closed.columns:
            dup_closed = closed[closed["deal_id"].notna() & closed.duplicated(subset=["deal_id"], keep=False)]
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
            closed = closed.copy()
            closed["pnl_recalc"] = recalc
            comparable = closed.dropna(subset=["pnl", "pnl_recalc"])
            if not comparable.empty:
                diff_rows = comparable[(comparable["pnl"] - comparable["pnl_recalc"]).abs() > 1e-6]
                issues.append({"severity": "INFO", "check": "pnl_recalc_comparable", "detail": f"Rows comparable for pnl recalculation: {len(comparable)}"})
                issues.append({"severity": "INFO", "check": "pnl_recalc_diff_rows", "detail": f"Rows where logged pnl != simple recalculated pnl: {len(diff_rows)}"})
    if not tracker.empty and not closed.empty:
        tracker_refs = set(tracker["deal_reference"].dropna().astype(str)) if "deal_reference" in tracker.columns else set()
        closed_refs = set(closed["deal_reference"].dropna().astype(str)) if "deal_reference" in closed.columns else set()
        if "status" in tracker.columns and "deal_reference" in tracker.columns:
            missing_in_closed = tracker[(tracker["status"].astype(str) == "CLOSED") & (~tracker["deal_reference"].astype(str).isin(closed_refs))]
            if not missing_in_closed.empty:
                issues.append({"severity": "WARN", "check": "closed_tracker_missing_in_closed_csv", "detail": f"Tracker CLOSED rows missing from closed_trades: {len(missing_in_closed)}"})
        if "deal_reference" in closed.columns:
            orphan_closed = closed[~closed["deal_reference"].astype(str).isin(tracker_refs)]
            if not orphan_closed.empty:
                issues.append({"severity": "WARN", "check": "orphan_closed_rows", "detail": f"closed_trades rows with no tracker match: {len(orphan_closed)}"})
    if not journal.empty and "event" in journal.columns:
        open_events = journal[journal["event"].astype(str) == "OPEN"]
        issues.append({"severity": "INFO", "check": "journal_open_events", "detail": f"OPEN events in trade_journal: {len(open_events)}"})
        if not closed.empty:
            issues.append({"severity": "INFO", "check": "closed_rows_count", "detail": f"Rows in closed_trades: {len(closed)}"})
    return pd.DataFrame(issues)


def make_diagnosis_summary(state, open_analysis, issues, closed):
    rows = []
    if not state.empty:
        align_ratio = float(state["ht_tick_aligned"].mean()) if "ht_tick_aligned" in state.columns else np.nan
        pot_ratio = float(state["potential_entry"].mean()) if "potential_entry" in state.columns else np.nan
        spread_ratio = float(state["spread_atr_ratio"].mean()) if "spread_atr_ratio" in state.columns else np.nan
        if pd.notna(align_ratio):
            rows.append({"topic": "alignment_ratio", "value": round(align_ratio, 4), "diagnosis": "low" if align_ratio < 0.2 else "medium" if align_ratio < 0.5 else "high", "suggestion": "Lazíts a tick szűrőn vagy növeld a decision intervallumot." if align_ratio < 0.2 else "Az alignment arány rendben van."})
        if pd.notna(pot_ratio):
            rows.append({"topic": "potential_entry_ratio", "value": round(pot_ratio, 4), "diagnosis": "low" if pot_ratio < 0.05 else "medium" if pot_ratio < 0.2 else "high", "suggestion": "Túl kevés setup; érdemes lazítani a combined filteren." if pot_ratio < 0.05 else "Túl sok setup; érdemes szigorítani a spread vagy signal filtert." if pot_ratio > 0.2 else "A setup arány első ránézésre egészséges."})
        if pd.notna(spread_ratio):
            rows.append({"topic": "avg_spread_atr_ratio", "value": round(spread_ratio, 4), "diagnosis": "high" if spread_ratio > 0.2 else "ok", "suggestion": "A spread/ATR magas; szigorúbb spread filter vagy jobb session lehet indokolt." if spread_ratio > 0.2 else "A spread/ATR átlag kezelhető."})
        if "allow_trade" in state.columns and state["allow_trade"].notna().any():
            allow_ratio = float(state["allow_trade"].fillna(False).mean())
            rows.append({"topic": "allow_trade_ratio", "value": round(allow_ratio, 4), "diagnosis": "low" if allow_ratio < 0.03 else "medium" if allow_ratio < 0.15 else "high", "suggestion": "Kevés engedélyezett trade; nézd meg a reason mezőt és a score thresholdot." if allow_ratio < 0.03 else "Az engedélyezett trade arány rendben van."})
    if not open_analysis.empty:
        avg_stop_mult = float(open_analysis["stop_atr_mult_est"].mean()) if "stop_atr_mult_est" in open_analysis.columns else np.nan
        avg_spread_stop = float(open_analysis["spread_stop_ratio"].mean()) if "spread_stop_ratio" in open_analysis.columns else np.nan
        avg_score = float(open_analysis["score"].mean()) if "score" in open_analysis.columns and open_analysis["score"].notna().any() else np.nan
        if pd.notna(avg_stop_mult):
            rows.append({"topic": "avg_stop_atr_mult", "value": round(avg_stop_mult, 4), "diagnosis": "tight" if avg_stop_mult < 1.0 else "medium" if avg_stop_mult <= 1.8 else "wide", "suggestion": "A stop túl szűk lehet." if avg_stop_mult < 1.0 else "A stop tartomány rendben van." if avg_stop_mult <= 1.8 else "A stop elég tág; ellenőrizd a profitvédelem lassulását."})
        if pd.notna(avg_spread_stop):
            rows.append({"topic": "avg_spread_stop_ratio", "value": round(avg_spread_stop, 4), "diagnosis": "high" if avg_spread_stop > 0.25 else "medium" if avg_spread_stop > 0.10 else "low", "suggestion": "A spread túl nagy a stophoz képest." if avg_spread_stop > 0.25 else "A spread kezelhető a stophoz mérve."})
        if pd.notna(avg_score):
            rows.append({"topic": "avg_trade_score", "value": round(avg_score, 4), "diagnosis": "weak" if avg_score < 0.4 else "ok" if avg_score < 0.7 else "strong", "suggestion": "A belépési score átlag alacsony; érdemes finomítani a signal-t vagy a score thresholdot." if avg_score < 0.4 else "A score szint vállalható."})
    if not issues.empty:
        err_cnt = int((issues["severity"] == "ERROR").sum())
        warn_cnt = int((issues["severity"] == "WARN").sum())
        rows.append({"topic": "pipeline_errors", "value": err_cnt, "diagnosis": "bad" if err_cnt > 0 else "ok", "suggestion": "Javítsd a closed trade pipeline hibáit a live elemzés előtt." if err_cnt > 0 else "Nincs kritikus pipeline hiba."})
        rows.append({"topic": "pipeline_warnings", "value": warn_cnt, "diagnosis": "warn" if warn_cnt > 0 else "ok", "suggestion": "Ellenőrizd a hiányzó vagy árván maradt sorokat." if warn_cnt > 0 else "Nincs jelentős pipeline warning."})
    if not closed.empty and "pnl" in closed.columns:
        rows.append({"topic": "closed_trade_count", "value": int(closed["pnl"].notna().sum()), "diagnosis": "low_sample" if len(closed) < 20 else "usable", "suggestion": "Még kevés a lezárt trade a megbízható következtetéshez." if len(closed) < 20 else "Már van használható minta a time-of-day elemzéshez."})
        total_pnl = float(closed["pnl"].sum()) if closed["pnl"].notna().any() else 0.0
        rows.append({"topic": "closed_total_pnl", "value": round(total_pnl, 4), "diagnosis": "positive" if total_pnl > 0 else "negative" if total_pnl < 0 else "flat", "suggestion": "A rendszer eddig pozitív lezárt PnL-t mutat." if total_pnl > 0 else "A rendszer eddig negatív lezárt PnL-t mutat; filter felülvizsgálat indokolt." if total_pnl < 0 else "A lezárt PnL egyelőre lapos."})
    return pd.DataFrame(rows)


def print_section(title):
    print("\n" + "=" * 80)
    print(title)
    print("=" * 80)


def print_state_report(state, trades):
    if state.empty:
        print_section("STATE REPORT")
        print("bot_state csv missing or empty")
        return
    print_section("ALAP STATS")
    print(f"Rows in bot_state: {len(state)}")
    print(f"Time range: {state['timestamp_utc'].min()} -> {state['timestamp_utc'].max()}")
    if "instrument" in state.columns:
        print("\nRows by instrument:")
        print(state.groupby("instrument").size().sort_values(ascending=False))
    print_section("ACTION COUNTS")
    if "action" in state.columns:
        print(state["action"].value_counts(dropna=False))
    print_section("TICK BIAS COUNTS")
    if "tick_bias" in state.columns:
        print(state["tick_bias"].value_counts(dropna=False))
    print_section("HT-TICK ALIGNMENT")
    print(state["ht_tick_aligned"].value_counts(dropna=False))
    print("\nAlignment ratio:")
    print(state["ht_tick_aligned"].value_counts(normalize=True).round(4))
    print_section("ALLOW_TRADE")
    if "allow_trade" in state.columns:
        print(state["allow_trade"].value_counts(dropna=False))
    if "reason" in state.columns:
        print("\nTop reasons:")
        print(state["reason"].astype(str).value_counts().head(20))
    print_section("POTENTIAL ENTRIES")
    print(state["potential_entry"].value_counts(dropna=False))
    print("\nPotential entry ratio:")
    print(state["potential_entry"].value_counts(normalize=True).round(4))
    print_section("AVG SPREAD/ATR RATIO BY INSTRUMENT")
    if set(["instrument", "spread_atr_ratio"]).issubset(state.columns):
        print(state.groupby("instrument")["spread_atr_ratio"].mean().sort_values().round(4))
    print_section("AVG SCORE BY INSTRUMENT")
    if set(["instrument", "score"]).issubset(state.columns):
        print(state.groupby("instrument")["score"].mean().sort_values(ascending=False).round(4))
    print_section("POTENTIAL ENTRIES BY HOUR UTC")
    if "hour" in state.columns:
        print(state.loc[state["potential_entry"]].groupby("hour").size().sort_values(ascending=False))
    if not trades.empty:
        print_section("TRADE JOURNAL ÖSSZEFOGLALÓ")
        if "event" in trades.columns:
            print(trades["event"].value_counts(dropna=False))
        print(f"Rows in trade_journal: {len(trades)}")
        if "instrument" in trades.columns:
            print("\nTrade journal rows by instrument:")
            print(trades.groupby("instrument").size().sort_values(ascending=False))


def print_open_trade_report(df):
    print_section("OPEN TRADE ANALYSIS")
    if df.empty:
        print("No OPEN trades found.")
        return
    print(f"Rows: {len(df)}")
    print(f"Time range: {df['open_time_utc'].min()} -> {df['open_time_utc'].max()}")
    if "instrument" in df.columns:
        print("\nRows by instrument:")
        print(df.groupby("instrument").size().sort_values(ascending=False))
    print("\nAVERAGES")
    cols = [c for c in ["atr", "spread", "stop_distance", "stop_atr_mult_est", "spread_atr", "spread_stop_ratio", "score", "size"] if c in df.columns]
    if cols:
        print(df[cols].mean(numeric_only=True).round(4))
    print("\nBY INSTRUMENT")
    grp_cols = [c for c in ["stop_distance", "stop_atr_mult_est", "spread_atr", "spread_stop_ratio", "score", "size"] if c in df.columns]
    if grp_cols and "instrument" in df.columns:
        print(df.groupby("instrument")[grp_cols].mean(numeric_only=True).round(4))
    print("\nSNAPSHOT")
    cols = [c for c in ["instrument", "hour", "direction", "action", "tick_bias", "tick_imbalance", "stop_distance", "stop_atr_mult_est", "score", "size"] if c in df.columns]
    print(df[cols].to_string(index=False))


def print_validation_report(issues, tracker, closed):
    print_section("CLOSED TRADE VALIDATION")
    print(f"Tracker rows: {len(tracker)}")
    print(f"Closed rows: {len(closed)}")
    if not tracker.empty and "instrument" in tracker.columns:
        print("\nTracker rows by instrument:")
        print(tracker.groupby("instrument").size().sort_values(ascending=False))
    if not closed.empty and "instrument" in closed.columns:
        print("\nClosed rows by instrument:")
        print(closed.groupby("instrument").size().sort_values(ascending=False))
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


def export_outputs(base, state, open_analysis, issues, summary, tracker, closed):
    if not state.empty:
        state.to_csv(f"{base}_state_prepared.csv", index=False)
    if not open_analysis.empty:
        open_analysis.to_csv(f"{base}_open_trade_analysis.csv", index=False)
    if not issues.empty:
        issues.to_csv(f"{base}_validation.csv", index=False)
    if not summary.empty:
        summary.to_csv(f"{base}_diagnosis_summary.csv", index=False)
    if not state.empty and "instrument" in state.columns:
        for instrument, sub in state.groupby("instrument"):
            safe = str(instrument).replace("/", "_")
            sub.to_csv(f"{base}_state_{safe}.csv", index=False)
    if not open_analysis.empty and "instrument" in open_analysis.columns:
        for instrument, sub in open_analysis.groupby("instrument"):
            safe = str(instrument).replace("/", "_")
            sub.to_csv(f"{base}_open_analysis_{safe}.csv", index=False)
    if not closed.empty and "instrument" in closed.columns:
        for instrument, sub in closed.groupby("instrument"):
            safe = str(instrument).replace("/", "_")
            sub.to_csv(f"{base}_closed_{safe}.csv", index=False)
    if not tracker.empty and "instrument" in tracker.columns:
        for instrument, sub in tracker.groupby("instrument"):
            safe = str(instrument).replace("/", "_")
            sub.to_csv(f"{base}_tracker_{safe}.csv", index=False)


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
    tracker = prepare_tracker(load_csv(args.position_tracker_csv))
    closed = prepare_closed(load_csv(args.closed_trades_csv))
    opens = enrich_open_trades(trades)
    open_analysis = evaluate_open_trades(opens, state, tracker=tracker)
    issues = validate_pipeline(tracker, closed, trades)
    summary = make_diagnosis_summary(state, open_analysis, issues, closed)

    print_state_report(state, trades)
    print_open_trade_report(open_analysis)
    print_validation_report(issues, tracker, closed)
    print_diagnosis(summary)

    export_outputs(args.output_prefix, state, open_analysis, issues, summary, tracker, closed)


if __name__ == "__main__":
    main()

import os
import argparse
import pandas as pd
import numpy as np

POSITION_TRACKER_CSV = os.getenv("POSITION_TRACKER_CSV", "position_tracker.csv")
CLOSED_TRADES_CSV = os.getenv("CLOSED_TRADES_CSV", "closed_trades.csv")
TRADE_JOURNAL_CSV = os.getenv("TRADE_JOURNAL_CSV", "trade_journal.csv")
OUTPUT_CSV = os.getenv("OUTPUT_CSV", "closed_trade_validation.csv")


def load_csv(path):
    if not os.path.exists(path):
        return pd.DataFrame()
    df = pd.read_csv(path)
    if df.empty:
        return df
    for col in ["timestamp_utc", "open_time_utc", "close_time_utc"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], utc=True, errors="coerce")
    return df


def validate(tracker, closed, journal):
    issues = []

    if tracker.empty:
        issues.append({"severity": "WARN", "check": "tracker_exists", "detail": "position_tracker.csv is empty or missing"})
    if closed.empty:
        issues.append({"severity": "WARN", "check": "closed_exists", "detail": "closed_trades.csv is empty or missing"})

    if not tracker.empty:
        dup_tracker = tracker[tracker.duplicated(subset=["deal_id"], keep=False)] if "deal_id" in tracker.columns else pd.DataFrame()
        if not dup_tracker.empty:
            issues.append({"severity": "ERROR", "check": "duplicate_tracker_deal_id", "detail": f"Duplicate deal_id rows in tracker: {dup_tracker['deal_id'].astype(str).nunique()} ids"})

        if "status" in tracker.columns and "deal_id" in tracker.columns:
            open_tracker = tracker[tracker["status"].astype(str) == "OPEN"]
            closed_tracker = tracker[tracker["status"].astype(str) == "CLOSED"]
            issues.append({"severity": "INFO", "check": "tracker_open_count", "detail": f"OPEN tracker rows: {len(open_tracker)}"})
            issues.append({"severity": "INFO", "check": "tracker_closed_count", "detail": f"CLOSED tracker rows: {len(closed_tracker)}"})

    if not closed.empty:
        dup_closed = closed[closed.duplicated(subset=["deal_id"], keep=False)] if "deal_id" in closed.columns else pd.DataFrame()
        if not dup_closed.empty:
            issues.append({"severity": "ERROR", "check": "duplicate_closed_deal_id", "detail": f"Duplicate deal_id rows in closed_trades: {dup_closed['deal_id'].astype(str).nunique()} ids"})

        for col in ["entry_price", "exit_price", "size", "pnl", "hold_minutes"]:
            if col in closed.columns:
                closed[col] = pd.to_numeric(closed[col], errors="coerce")

        missing_core = closed[
            closed[[c for c in ["open_time_utc", "close_time_utc", "direction", "size", "entry_price", "exit_price", "pnl", "deal_id"] if c in closed.columns]].isna().any(axis=1)
        ] if set(["open_time_utc", "close_time_utc", "direction", "size", "entry_price", "exit_price", "pnl", "deal_id"]).intersection(closed.columns) else pd.DataFrame()
        if not missing_core.empty:
            issues.append({"severity": "WARN", "check": "closed_missing_core_fields", "detail": f"Rows with missing core fields: {len(missing_core)}"})

        if "hold_minutes" in closed.columns:
            neg_hold = closed[closed["hold_minutes"] < 0]
            if not neg_hold.empty:
                issues.append({"severity": "ERROR", "check": "negative_hold_time", "detail": f"Rows with negative hold time: {len(neg_hold)}"})

        if "pnl" in closed.columns:
            zero_pnl = closed[np.isclose(closed["pnl"].fillna(0), 0.0)]
            issues.append({"severity": "INFO", "check": "zero_pnl_rows", "detail": f"Rows with pnl ~ 0: {len(zero_pnl)}"})

        if set(["direction", "entry_price", "exit_price", "size", "pnl"]).issubset(closed.columns):
            est = []
            for _, r in closed.iterrows():
                if pd.isna(r["entry_price"]) or pd.isna(r["exit_price"]) or pd.isna(r["size"]) or pd.isna(r["pnl"]):
                    est.append(np.nan)
                    continue
                if str(r["direction"]).upper() == "BUY":
                    est.append((r["exit_price"] - r["entry_price"]) * r["size"])
                elif str(r["direction"]).upper() == "SELL":
                    est.append((r["entry_price"] - r["exit_price"]) * r["size"])
                else:
                    est.append(np.nan)
            closed["pnl_recalc"] = est
            comparable = closed.dropna(subset=["pnl", "pnl_recalc"])
            if not comparable.empty:
                comparable["pnl_diff"] = comparable["pnl"] - comparable["pnl_recalc"]
                large_diff = comparable[comparable["pnl_diff"].abs() > 1e-6]
                issues.append({"severity": "INFO", "check": "pnl_recalc_comparable", "detail": f"Rows comparable for pnl recalculation: {len(comparable)}"})
                issues.append({"severity": "INFO", "check": "pnl_recalc_diff_rows", "detail": f"Rows where logged pnl != simple recalculated pnl: {len(large_diff)}"})

    if not tracker.empty and not closed.empty and "deal_id" in tracker.columns and "deal_id" in closed.columns:
        tracker_ids = set(tracker["deal_id"].dropna().astype(str))
        closed_ids = set(closed["deal_id"].dropna().astype(str))
        missing_in_closed = tracker[(tracker["status"].astype(str) == "CLOSED") & (~tracker["deal_id"].astype(str).isin(closed_ids))] if "status" in tracker.columns else pd.DataFrame()
        orphan_closed = closed[~closed["deal_id"].astype(str).isin(tracker_ids)]
        if not missing_in_closed.empty:
            issues.append({"severity": "WARN", "check": "closed_tracker_missing_in_closed_csv", "detail": f"Tracker CLOSED rows missing from closed_trades.csv: {len(missing_in_closed)}"})
        if not orphan_closed.empty:
            issues.append({"severity": "WARN", "check": "orphan_closed_rows", "detail": f"closed_trades rows with no tracker match: {len(orphan_closed)}"})

    if not journal.empty:
        open_events = journal[journal["event"].astype(str) == "OPEN"] if "event" in journal.columns else pd.DataFrame()
        issues.append({"severity": "INFO", "check": "journal_open_events", "detail": f"OPEN events in trade_journal: {len(open_events)}"})
        if not closed.empty:
            issues.append({"severity": "INFO", "check": "closed_rows_count", "detail": f"Rows in closed_trades.csv: {len(closed)}"})

    return pd.DataFrame(issues)


def print_report(issues, tracker, closed):
    print("=" * 80)
    print("CLOSED TRADE VALIDATION")
    print("=" * 80)
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


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--position-tracker-csv", default=POSITION_TRACKER_CSV)
    parser.add_argument("--closed-trades-csv", default=CLOSED_TRADES_CSV)
    parser.add_argument("--trade-journal-csv", default=TRADE_JOURNAL_CSV)
    parser.add_argument("--output-csv", default=OUTPUT_CSV)
    args = parser.parse_args()

    tracker = load_csv(args.position_tracker_csv)
    closed = load_csv(args.closed_trades_csv)
    journal = load_csv(args.trade_journal_csv)

    issues = validate(tracker, closed, journal)
    issues.to_csv(args.output_csv, index=False)
    print_report(issues, tracker, closed)


if __name__ == "__main__":
    main()

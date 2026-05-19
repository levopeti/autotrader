import os
import ast
import math
import argparse
import pandas as pd

TRADE_JOURNAL_CSV = os.getenv("TRADE_JOURNAL_CSV", "trade_journal.csv")
BOT_STATE_CSV = os.getenv("BOT_STATE_CSV", "bot_state.csv")
OUTPUT_CSV = os.getenv("OUTPUT_CSV", "trade_analysis.csv")


def parse_note_value(note, key):
    if pd.isna(note):
        return None
    parts = str(note).split(";")
    for part in parts:
        if part.startswith(f"{key}="):
            return part.split("=", 1)[1]
    return None


def load_data(trade_csv, state_csv):
    trades = pd.read_csv(trade_csv) if os.path.exists(trade_csv) else pd.DataFrame()
    states = pd.read_csv(state_csv) if os.path.exists(state_csv) else pd.DataFrame()

    if not trades.empty:
        trades["timestamp_utc"] = pd.to_datetime(trades["timestamp_utc"], utc=True, errors="coerce")
    if not states.empty:
        states["timestamp_utc"] = pd.to_datetime(states["timestamp_utc"], utc=True, errors="coerce")

    return trades, states


def enrich_trades(trades):
    if trades.empty:
        return trades
    opens = trades[trades["event"] == "OPEN"].copy()
    opens["stop_distance"] = opens["note"].apply(lambda x: parse_note_value(x, "stopDistance"))
    opens["spread_atr"] = opens["note"].apply(lambda x: parse_note_value(x, "spread_atr"))
    opens["dry_run"] = opens["note"].apply(lambda x: parse_note_value(x, "DRY_RUN"))
    opens["trailing_stop"] = opens["note"].apply(lambda x: parse_note_value(x, "trailingStop"))
    opens["deal_reference"] = opens["note"].apply(lambda x: parse_note_value(x, "dealReference"))
    opens["stop_distance"] = pd.to_numeric(opens["stop_distance"], errors="coerce")
    opens["spread_atr"] = pd.to_numeric(opens["spread_atr"], errors="coerce")
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
    for _, tr in opens.iterrows():
        snap = nearest_state_snapshot(tr["timestamp_utc"], states)
        tick_bias = snap["tick_bias"] if snap is not None and "tick_bias" in snap else None
        tick_imbalance = snap["tick_imbalance"] if snap is not None and "tick_imbalance" in snap else None
        action = snap["action"] if snap is not None and "action" in snap else None
        bias = snap["bias"] if snap is not None and "bias" in snap else None
        atr = float(tr["atr"]) if not pd.isna(tr["atr"]) else (float(snap["atr"]) if snap is not None and "atr" in snap else math.nan)
        spread = float(tr["spread"]) if not pd.isna(tr["spread"]) else (float(snap["spread"]) if snap is not None and "spread" in snap else math.nan)
        stop_distance = tr["stop_distance"]

        stop_atr_mult_est = stop_distance / atr if pd.notna(stop_distance) and pd.notna(atr) and atr != 0 else math.nan
        spread_stop_ratio = spread / stop_distance if pd.notna(stop_distance) and stop_distance != 0 and pd.notna(spread) else math.nan

        rows.append({
            "open_time_utc": tr["timestamp_utc"],
            "direction": tr["direction"],
            "price": tr["price"],
            "size": tr["size"],
            "action": action,
            "bias": bias,
            "tick_bias": tick_bias,
            "tick_imbalance": tick_imbalance,
            "atr": atr,
            "spread": spread,
            "stop_distance": stop_distance,
            "stop_atr_mult_est": stop_atr_mult_est,
            "spread_atr": tr["spread_atr"],
            "spread_stop_ratio": spread_stop_ratio,
            "dry_run": tr["dry_run"],
            "trailing_stop": tr["trailing_stop"],
            "deal_reference": tr["deal_reference"],
        })
    return pd.DataFrame(rows)


def print_report(df):
    if df.empty:
        print("No OPEN trades found.")
        return

    print("=" * 80)
    print("OPEN TRADE ANALYSIS")
    print("=" * 80)
    print(f"Rows: {len(df)}")
    print(f"Time range: {df['open_time_utc'].min()} -> {df['open_time_utc'].max()}")
    print()

    print("AVERAGES")
    print(df[["atr", "spread", "stop_distance", "stop_atr_mult_est", "spread_atr", "spread_stop_ratio"]].mean(numeric_only=True).round(4))
    print()

    print("BY DIRECTION")
    print(df.groupby("direction")[["stop_distance", "stop_atr_mult_est", "spread_atr", "spread_stop_ratio"]].mean(numeric_only=True).round(4))
    print()

    print("TICK ALIGNMENT SNAPSHOT")
    print(df[["direction", "action", "tick_bias", "tick_imbalance", "stop_distance", "stop_atr_mult_est"]].to_string(index=False))
    print()

    print("HEURISTICS")
    avg_stop_mult = df["stop_atr_mult_est"].mean()
    avg_spread_stop = df["spread_stop_ratio"].mean()

    if pd.notna(avg_stop_mult):
        if avg_stop_mult < 1.0:
            print("- A stop valószínűleg szűk: átlagos stopDistance < 1 ATR.")
        elif avg_stop_mult <= 1.5:
            print("- A stop közepes: kb. 1.0-1.5 ATR tartomány.")
        else:
            print("- A stop tágabb: 1.5 ATR fölötti átlagos stopDistance.")

    if pd.notna(avg_spread_stop):
        if avg_spread_stop > 0.25:
            print("- A spread a stophoz képest magas; lehet, hogy túl szoros a trailing stop.")
        elif avg_spread_stop > 0.10:
            print("- A spread érezhető a stophoz képest, de még kezelhető.")
        else:
            print("- A spread alacsony a stopDistance-hez képest.")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--trade-csv", default=TRADE_JOURNAL_CSV)
    parser.add_argument("--state-csv", default=BOT_STATE_CSV)
    parser.add_argument("--output-csv", default=OUTPUT_CSV)
    args = parser.parse_args()

    trades, states = load_data(args.trade_csv, args.state_csv)
    opens = enrich_trades(trades)
    analysis = evaluate_open_trades(opens, states)
    analysis.to_csv(args.output_csv, index=False)
    print_report(analysis)


if __name__ == "__main__":
    main()
import pandas as pd
from pathlib import Path

BOT_STATE_CSV = "bot_state.csv"
TRADE_JOURNAL_CSV = "trade_journal.csv"


def load_csv(path):
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Missing file: {path}")
    return pd.read_csv(p)


def prepare_state(df):
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
    df = df.copy()
    if "timestamp_utc" in df.columns:
        df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], utc=True, errors="coerce")
    for col in ["price", "atr", "spread", "size"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def print_section(title):
    print("\n" + "=" * 80)
    print(title)
    print("=" * 80)


def main():
    state = prepare_state(load_csv(BOT_STATE_CSV))
    trades = prepare_trades(load_csv(TRADE_JOURNAL_CSV)) if Path(TRADE_JOURNAL_CSV).exists() else pd.DataFrame()

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
    stalled = state[
        state["direction"].isin(["BUY", "SELL"]) &
        (~state["ht_tick_aligned"])
    ][[
        "timestamp_utc", "action", "bias", "direction",
        "tick_bias", "tick_imbalance", "atr", "spread", "spread_atr_ratio"
    ]]
    print(stalled.tail(20).to_string(index=False))

    print_section("ALIGNMENT + JÓ SPREAD + AKCIÓ")
    good = state[state["potential_entry"]][[
        "timestamp_utc", "action", "bias", "direction",
        "tick_bias", "tick_imbalance", "atr", "spread", "spread_atr_ratio"
    ]]
    print(good.tail(20).to_string(index=False))

    if not trades.empty:
        print_section("TRADE JOURNAL ÖSSZEFOGLALÓ")
        if "event" in trades.columns:
            print(trades["event"].value_counts(dropna=False))
        print(f"Rows in trade_journal.csv: {len(trades)}")

        opens = trades[trades.get("event", pd.Series(dtype=str)) == "OPEN"].copy() if "event" in trades.columns else pd.DataFrame()
        if not opens.empty and "timestamp_utc" in opens.columns:
            opens["hour"] = opens["timestamp_utc"].dt.hour
            print("\nOPEN trades by hour UTC:")
            print(opens.groupby("hour").size().sort_values(ascending=False))

        if "note" in trades.columns:
            pnl_rows = trades[trades["note"].astype(str).str.startswith("PNL_EST=", na=False)].copy()
            if not pnl_rows.empty:
                pnl_rows["pnl_est"] = pnl_rows["note"].str.replace("PNL_EST=", "", regex=False).astype(float)
                print("\nEstimated closed PnL stats:")
                print(pnl_rows["pnl_est"].describe().round(4))

    print_section("HASZNOS KÖVETKEZTETÉSEK")
    pot_ratio = state["potential_entry"].mean()
    align_ratio = state["ht_tick_aligned"].mean()
    avg_spread_ratio = state["spread_atr_ratio"].mean()

    if align_ratio < 0.2:
        print("- A tick megerősítés valószínűleg szigorú, mert ritka a HT-tick összhang.")
    elif align_ratio > 0.5:
        print("- A tick megerősítés elég gyakran együtt mozog a HT bias-szal.")
    else:
        print("- A HT és tick megerősítés közepes gyakorisággal esik egybe.")

    if avg_spread_ratio > 0.2:
        print("- Az átlagos spread/ATR arány magas, lehet hogy a spread filter túl sok setupot blokkol.")
    else:
        print("- Az átlagos spread/ATR arány még kezelhető tartományban van.")

    if pot_ratio < 0.05:
        print("- Kevés a potenciális belépő; lehet, hogy túl konzervatív a kombinált filterezés.")
    elif pot_ratio > 0.2:
        print("- Sok a potenciális belépő; lehet, hogy érdemes szigorítani valamelyik szűrőt.")
    else:
        print("- A potenciális belépők aránya első ránézésre életszerű.")


if __name__ == "__main__":
    main()

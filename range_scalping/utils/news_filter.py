"""
News Filter
===========
Makrogazdasági híresemények kizárása a backtest és live trading során.
Forrás: kézzel megadott lista, vagy ForexFactory CSV export.

Használat:
  nf = NewsFilter("news_calendar.csv")
  clean_ticks = nf.filter_ticks(ticks, buffer_minutes=30)
"""

import pandas as pd
from pathlib import Path
from typing import Optional


GOLD_RELEVANT_EVENTS = [
    "CPI", "Core CPI", "NFP", "Non-Farm",
    "FOMC", "Fed", "Interest Rate", "Powell",
    "PPI", "GDP", "Unemployment", "PCE",
    "Retail Sales", "ISM", "PMI",
    "Inflation", "Treasury",
]


class NewsFilter:

    def __init__(self, calendar_path: Optional[str] = None):
        self.events: pd.DataFrame = pd.DataFrame()
        if calendar_path and Path(calendar_path).exists():
            self._load_calendar(calendar_path)

    def _load_calendar(self, path: str):
        df = pd.read_csv(path)
        df.columns = df.columns.str.lower().str.strip()

        ts_col = next((c for c in df.columns if "date" in c or "time" in c), None)
        if ts_col:
            df["event_time"] = pd.to_datetime(df[ts_col], utc=True, errors="coerce")

        if "impact" in df.columns:
            df = df[df["impact"].str.lower().str.contains("high", na=False)]

        self.events = df.dropna(subset=["event_time"])
        print(f"✓ News calendar betöltve: {len(self.events)} high-impact esemény")

    def add_manual_event(self, timestamp: str, name: str = "Manual"):
        """Kézzel hozzáad egy eseményt (UTC timestamp string)."""
        new = pd.DataFrame([{
            "event_time": pd.Timestamp(timestamp, tz="UTC"),
            "name": name,
        }])
        self.events = pd.concat([self.events, new], ignore_index=True)

    def get_blackout_mask(
        self,
        timestamps: pd.Series,
        buffer_minutes: int = 30,
    ) -> pd.Series:
        """True ahol a tick hír-zónában van (buffer_minutes előtt/után)."""
        mask = pd.Series(False, index=timestamps.index)
        if self.events.empty:
            return mask
        buf = pd.Timedelta(minutes=buffer_minutes)
        for _, ev in self.events.iterrows():
            t    = ev["event_time"]
            zone = (timestamps >= t - buf) & (timestamps <= t + buf)
            mask |= zone
        return mask

    def filter_ticks(
        self,
        ticks: pd.DataFrame,
        buffer_minutes: int = 30,
    ) -> pd.DataFrame:
        """Hír-zóna tickek eltávolítása a DataFrame-ből."""
        mask    = self.get_blackout_mask(ticks["timestamp"], buffer_minutes)
        removed = mask.sum()
        if removed > 0:
            print(f"  News filter: {removed:,} tick eltávolítva ({removed/len(ticks)*100:.1f}%)")
        return ticks[~mask].reset_index(drop=True)

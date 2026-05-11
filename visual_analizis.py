import pandas as pd
from pathlib import Path
import json
import plotly.express as px
import plotly.io as pio

state_path = Path('./logs/bot_state_multi.csv')
trade_path = Path('./logs/trade_journal_multi.csv')
tracker_path = Path('./logs/position_tracker_multi.csv')
closed_path = Path('./logs/closed_trades_multi.csv')

out = Path('output')
out.mkdir(exist_ok=True)

if state_path.exists():
    state = pd.read_csv(state_path, dtype=str, keep_default_na=False)
else:
    state = pd.DataFrame()
if trade_path.exists():
    trades = pd.read_csv(trade_path, dtype=str, keep_default_na=False)
else:
    trades = pd.DataFrame()
if tracker_path.exists():
    tracker = pd.read_csv(tracker_path, dtype=str, keep_default_na=False)
else:
    tracker = pd.DataFrame()
if closed_path.exists():
    closed = pd.read_csv(closed_path, dtype=str, keep_default_na=False)
else:
    closed = pd.DataFrame()

for df in [state, trades, tracker, closed]:
    for col in ['timestamp_utc', 'open_time_utc', 'close_time_utc']:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], utc=True, errors='coerce')

for col in ['score','atr','spread','tick_imbalance','size']:
    if col in state.columns:
        state[col] = pd.to_numeric(state[col], errors='coerce')
for col in ['score','price','atr','spread','size']:
    if col in trades.columns:
        trades[col] = pd.to_numeric(trades[col], errors='coerce')
for col in ['entry_price','exit_price','size','pnl','hold_minutes']:
    if col in closed.columns:
        closed[col] = pd.to_numeric(closed[col], errors='coerce')

if not state.empty:
    state['allow_trade'] = state.get('allow_trade', '').astype(str).str.lower().map({'true': True, 'false': False})
    state['spread_atr_ratio'] = state['spread'] / state['atr']
    state['hour'] = state['timestamp_utc'].dt.hour

charts = []

if not state.empty and 'instrument' in state.columns:
    score_df = state.groupby('instrument', dropna=False)['score'].mean().reset_index().dropna()
    if not score_df.empty:
        fig = px.bar(score_df, x='instrument', y='score')
        fig.update_layout(title={"text": "Átlag score instrumentenként (aktuális minta)<br><span style='font-size: 18px; font-weight: normal;'>Forrás: bot_state_multi.csv | magasabb érték erősebb setupot jelez</span>"}, legend=dict(orientation='h', yanchor='bottom', y=1.05, xanchor='center', x=0.5))
        fig.update_xaxes(title_text='Instr.')
        fig.update_yaxes(title_text='Avg score')
        fig.update_traces(cliponaxis=False)
        fn = out / 'avg_score_by_instrument.png'
        fig.write_image(str(fn))
        with open(str(fn)+'.meta.json','w') as f:
            json.dump({"caption":"Átlag score instrumentenként","description":"Oszlopdiagram az átlagos belépési score-ról instrumentenként a bot state adatok alapján."}, f)
        charts.append(fn)

    spread_df = state.groupby('instrument', dropna=False)['spread_atr_ratio'].mean().reset_index().dropna()
    if not spread_df.empty:
        fig = px.bar(spread_df, x='instrument', y='spread_atr_ratio')
        fig.update_layout(title={"text": "Átlag spread/ATR instrumentenként (aktuális minta)<br><span style='font-size: 18px; font-weight: normal;'>Forrás: bot_state_multi.csv | alacsonyabb érték kedvezőbb költségterhelést jelez</span>"}, legend=dict(orientation='h', yanchor='bottom', y=1.05, xanchor='center', x=0.5))
        fig.update_xaxes(title_text='Instr.')
        fig.update_yaxes(title_text='Spread/ATR')
        fig.update_traces(cliponaxis=False)
        fn = out / 'avg_spread_atr_by_instrument.png'
        fig.write_image(str(fn))
        with open(str(fn)+'.meta.json','w') as f:
            json.dump({"caption":"Átlag spread/ATR instrumentenként","description":"Oszlopdiagram az átlagos spread per ATR arányról instrumentenként."}, f)
        charts.append(fn)

    allow_df = state.dropna(subset=['allow_trade']).groupby('instrument', dropna=False)['allow_trade'].mean().reset_index()
    if not allow_df.empty:
        fig = px.bar(allow_df, x='instrument', y='allow_trade')
        fig.update_layout(title={"text": "Allow-trade arány instrumentenként (aktuális minta)<br><span style='font-size: 18px; font-weight: normal;'>Forrás: bot_state_multi.csv | megmutatja, milyen gyakran ment át a setup a szűrőkön</span>"}, legend=dict(orientation='h', yanchor='bottom', y=1.05, xanchor='center', x=0.5))
        fig.update_xaxes(title_text='Instr.')
        fig.update_yaxes(title_text='Allow rate')
        fig.update_traces(cliponaxis=False)
        fn = out / 'allow_trade_rate_by_instrument.png'
        fig.write_image(str(fn))
        with open(str(fn)+'.meta.json','w') as f:
            json.dump({"caption":"Allow-trade arány instrumentenként","description":"Oszlopdiagram arról, milyen arányban engedélyezett a trade instrumentenként."}, f)
        charts.append(fn)

if not state.empty and {'hour','instrument','allow_trade'}.issubset(state.columns):
    heat = state.dropna(subset=['hour','allow_trade']).groupby(['hour','instrument'])['allow_trade'].mean().reset_index()
    if not heat.empty:
        fig = px.density_heatmap(heat, x='hour', y='instrument', z='allow_trade', histfunc='avg')
        fig.update_layout(title={"text": "Allow-trade hőtérkép óránként (aktuális minta)<br><span style='font-size: 18px; font-weight: normal;'>Forrás: bot_state_multi.csv | segít megtalálni az instrumentenként jobb időablakokat</span>"})
        fig.update_xaxes(title_text='Óra UTC')
        fig.update_yaxes(title_text='Instr.')
        fn = out / 'allow_trade_heatmap.png'
        fig.write_image(str(fn))
        with open(str(fn)+'.meta.json','w') as f:
            json.dump({"caption":"Allow-trade hőtérkép óránként","description":"Hőtérkép az allow_trade arányról óránként és instrumentenként."}, f)
        charts.append(fn)

print('\n'.join(str(x) for x in charts))
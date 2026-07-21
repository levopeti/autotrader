"""
Ismert minta-üzenetek a parserek regressziós teszteléséhez.

Minden `valid` rekord (msg, expected) párt tartalmaz — az új parser ki kell
nyerje az `expected` mezőket. Az `invalid` rekordok olyan üzenetek, amiket
NEM szabad jellé alakítani.
"""
from __future__ import annotations

VALID = [
    (
        "XAUUSD BUY NOW❗\n@ 4433- 4428\n\nSL🛑4423\nTP✅4443",
        {"direction": "BUY", "entry_low": 4428, "entry_high": 4433, "tp_list": [4443], "sl": 4423},
    ),
    (
        "XAUUSD BUY NOW❗️\n@ 4549 - 4544\n\nSL🛑4539\nTP✅4559",
        {"direction": "BUY", "entry_low": 4544, "entry_high": 4549, "tp_list": [4559], "sl": 4539},
    ),
    (
        "📉 XAU/USD BUY NOW\n\n✨ Entries:\nEntry 1: @4462\nEntry 2: @4459 (Recovery Zone)\n\n"
        "🚨 Stop Loss (SL): @4449\n(Strictly follow)\n\n🎯 Take Profit Targets:\n\n"
        "✅ TP 1: @4467\n✅ TP 2: @4472\n✅ TP 3: @4477\n✅ TP 4: @4482",
        {"direction": "BUY", "entry_low": 4459, "entry_high": 4462,
         "tp_list": [4467, 4472, 4477, 4482], "sl": 4449},
    ),
    (
        "📉 XAU/USD SELL NOW\n\n✨ Entries:\nEntry 1: @4543\nEntry 2: @4547 (Recovery Zone)\n\n"
        "🚨 Stop Loss (SL): @4557\n\n🎯 Take Profit Targets:\n\n"
        "✅ TP 1: @4538\n✅ TP 2: @4533\n✅ TP 3: @4528\n✅ TP 4: Open",
        {"direction": "SELL", "entry_low": 4543, "entry_high": 4547,
         "tp_list": [4528, 4533, 4538], "sl": 4557},
    ),
    (
        "📉 XAU/USD SELL NOW\n\n✨ Entries:\nEntry 1: @4543\n\n🚨 Stop Loss (SL): @4557\n\n"
        "🎯 Take Profit Targets:\n✅ TP 1: @4538\n✅ TP 2: @4533\n✅ TP 3: @4528\n✅ TP 4: @4523",
        {"direction": "SELL", "entry_low": 4542, "entry_high": 4544,
         "tp_list": [4523, 4528, 4533, 4538], "sl": 4557},
    ),
    (
        "🚨 SIGNAL ALERT 🚨\n\n🌐 XAUUSD\n\n📊 Trade Details:📈 BUY\n\n"
        "⚪️ Entry Point: 4530\n🔴 Stop Loss (SL): 4520\n\n"
        "🟢 Take Profit 1 (TP1): 4535\n🟢 Take Profit 2 (TP2): 4540\n🟢 Take Profit 3 (TP3): 4545",
        {"direction": "BUY", "entry_low": 4529, "entry_high": 4531,
         "tp_list": [4535, 4540, 4545], "sl": 4520},
    ),
    (
        "Gold sell now 4626-4629\n\nSL:4633\n\nTP:4624\nTP:4622\nTP:4620\nTP:open",
        {"direction": "SELL", "entry_low": 4626, "entry_high": 4629,
         "tp_list": [4620, 4622, 4624], "sl": 4633},
    ),
    (
        "Gold sell now 4629-4626\n\nSL:4633\n\nTP:4624\nTP:4622\nTP:4620\nTP:open",
        {"direction": "SELL", "entry_low": 4626, "entry_high": 4629,
         "tp_list": [4620, 4622, 4624], "sl": 4633},
    ),
    (
        "📈 XAUUSD SELL NOW\nEntry: 4660-70 KIS RISK\n🛑 SL: 4680\n"
        "🎯 TP1: 4655\n🎯 TP2: 4650\n🎯 TP3: 4645\n🎯 TP4: 4640\n🎯 TP5: 4630",
        {"direction": "SELL", "entry_low": 4660, "entry_high": 4670,
         "tp_list": [4630, 4640, 4645, 4650, 4655], "sl": 4680},
    ),
    (
        "📈 XAUUSD SELL NOW\nEntry: 4695-05 KIS RISK\n🛑 SL: 4710\n"
        "🎯 TP1: 4690\n🎯 TP2: 4685",
        {"direction": "SELL", "entry_low": 4695, "entry_high": 4705,
         "tp_list": [4685, 4690], "sl": 4710},
    ),
    (
        "XAUUSD Signal:\n📈 Entry Direction: (SELL)\n✅ Entry Point: (4652/4655)\n"
        "✅TP1  :4647\n✅TP2 :4642\n✅TP3 :4637\n✅TP4 :4632\n ❌Stop Loss: 4665",
        {"direction": "SELL", "entry_low": 4652, "entry_high": 4655,
         "tp_list": [4632, 4637, 4642, 4647], "sl": 4665},
    ),
]


# Ezeknél NEM szabad valid signal-t visszaadni.
INVALID = [
    (
        # SL túl rövid (44) — abszolút price-range alatt
        "XAUUSD BUY NOW❗\n@ 4433- 4428\n\nSL🛑44\nTP✅44",
        "missing_or_invalid_sl",
    ),
    (
        # BUY direction-on a SL > entries → értelmetlen
        "Gold buy now 4626-4629\n\nSL:4633\n\nTP:4624",
        "direction_levels_inconsistent",
    ),
    (
        # SELL-en SL < entries → értelmetlen
        "Gold sell now 4626-4629\n\nSL:4623\n\nTP:4624",
        "direction_levels_inconsistent",
    ),
    (
        # Nem signal: TP-hit jelentés
        "TP3 HIT!\nSELL GOLD @ 4686\n\nTarget 3 Reached: 4671",
        "not_a_signal",
    ),
    (
        # XAU/GOLD/BUY/SELL nem szerepel
        "Forex update: market is volatile today.",
        "not_a_signal",
    ),
]

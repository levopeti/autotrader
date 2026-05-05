from pprint import pprint

import emoji
import re


def signal_parser(signal: str):
    signal_dict = dict()
    signal = emoji.replace_emoji(signal, replace='')
    error_msg = "not a signal"
    if ("XAU" in signal.upper() or "GOLD" in signal.upper()) and ("SELL" in signal.upper() or "BUY" in signal.upper()):
        direction = None
        entries = list()
        tp_list = list()
        sl_list = list()
        lines = [l for l in signal.split("\n") if len(l) > 0]

        for line in lines:
            if "BUY" in line.upper():
                if direction is not None and direction != "BUY":
                    print("Error, direction: {}".format(line))
                    return {}, "Error, direction: {}".format(line)
                direction = "BUY"

            if "SELL" in line.upper():
                if direction is not None and direction != "SELL":
                    print("Error, direction: {}".format(line))
                    return {}, "Error, direction: {}".format(line)
                direction = "SELL"

            numbers = [x for x in re.findall(r'\d+', line) if len(x) > 1]
            if len(numbers) >= 3:
                print("Error, direction: {}".format(line))
                return {}, "Error, direction: {}".format(line)

            if len(numbers) == 2 or "ENTRY" in line.upper():
                for n in numbers:
                    entries.append(n)

                if len(entries) == 2 and len(min(entries, key=lambda x: int(x))) < 4:
                    min_n = min(entries, key=lambda x: int(x))
                    max_n = max(entries, key=lambda x: int(x))
                    min_n = max_n[:-len(min_n)] + min_n
                    entries = [min_n, max_n]


            if "TP" in line.upper():
                assert len(numbers) in [0, 1], (numbers, line)
                if len(numbers) == 1 and len(numbers[0]) == 4:
                    tp_list.append(numbers[0])
            if "SL" in line.upper() or "STOP LOSS" in line.upper():
                assert len(numbers) in [0, 1], (numbers, line)
                if len(numbers) == 1 and len(numbers[0]) == 4:
                    sl_list.append(numbers[0])

        signal_dict = {
            "direction": direction,
            "entries": sorted([int(x) for x in entries]),
            "tp_list": sorted([int(x) for x in tp_list]),
            "sl_list": sorted([int(x) for x in sl_list])
        }

        if len(signal_dict["sl_list"]) != 1:
            print("Error, sl_list: {}".format(signal_dict["sl_list"]))
            return {}, "Error, sl_list: {}".format(signal_dict["sl_list"])

        if len(signal_dict["entries"]) == 1:
            signal_dict["entries"] = [signal_dict["entries"] - 1, signal_dict["entries"] + 1]

        if len(signal_dict["entries"]) != 2:
            print("Error, entries: ", signal_dict["entries"])
            return {}, "Error, entries: {}".format(signal_dict["entries"])

        # check signal
        signal_ok = True
        error_msg = "ok"
        if signal_dict["direction"] == "BUY":
            for e in signal_dict["entries"]:
                if signal_dict["sl_list"][0] > e:
                    signal_ok = False
                    break

            for tp in signal_dict["tp_list"]:
                if signal_dict["sl_list"][0] > tp:
                    signal_ok = False

            for tp in signal_dict["tp_list"]:
                if max(signal_dict["entries"]) > tp:
                    signal_ok = False
        else:  # SELL
            for e in signal_dict["entries"]:
                if signal_dict["sl_list"][0] < e:
                    signal_ok = False

            for tp in signal_dict["tp_list"]:
                if signal_dict["sl_list"][0] < tp:
                    signal_ok = False

            for tp in signal_dict["tp_list"]:
                if min(signal_dict["entries"]) < tp:
                    signal_ok = False

        if not signal_ok:
            signal_dict = dict()
            error_msg = "entry, tp, sl error"
    return signal_dict, error_msg

if __name__ == '__main__':
    good_signal = [
        "XAUUSD BUY NOW❗\n@ 4433- 4428\n\nSL🛑4423\nTP✅4443\n\n#FollowRapatAn ❗️❗️❗️❗️❗️ ❗️",
        "XAUUSD BUY NOW❗️\n@ 4549 - 4544\n\nSL🛑4539\nTP✅4559\n\n#FollowRapatAn ❗️❗️❗️❗️❗️ ❗️",
        "📉 XAU/USD BUY NOW\n\n✨ Entries:\nEntry 1: @4462\nEntry 2: @4459 (Recovery Zone)\n\n🚨 Stop Loss (SL): @4449\n(Strictly follow)\n\n🎯 Take Profit Targets:\n\n✅ TP 1: @4467\n✅ TP 2: @4472\n✅ TP 3: @4477\n✅ TP 4: @4482\n💡 Pro Tip: Always use proper money management to protect your capita",
        "📉 XAU/USD SELL NOW\n\n✨ Entries:\nEntry 1: @4543\nEntry 2: @4547 (Recovery Zone)\n\n🚨 Stop Loss (SL): @4557\n(Strictly follow)\n\n🎯 Take Profit Targets:\n\n✅ TP 1: @4538\n✅ TP 2: @4533\n✅ TP 3: @4528\n✅ TP 4: Open\n\n💡 Pro Tip: Always use proper money management to protect your capital",
        "📉 XAU/USD SELL NOW\n\n✨ Entries:\nEntry 1: @4543\n\n🚨 Stop Loss (SL): @4557\n(Strictly follow)\n\n🎯 Take Profit Targets:\n\n✅ TP 1: @4538\n✅ TP 2: @4533\n✅ TP 3: @4528\n✅ TP 4: @4523\n\n💡 Pro Tip: Always use proper money management to protect your capital",
        "🚨 SIGNAL ALERT 🚨\n\n🌐 XAUUSD\n\n📊 Trade Details:📈 BUY\n\n⚪️ Entry Point: 4530\n🔴 Stop Loss (SL): 4520\n\n🟢 Take Profit 1 (TP1): 4535\n🟢 Take Profit 2 (TP2): 4540\n🟢 Take Profit 3 (TP3): 4545",
        "Gold sell now 4626-4629\n\nSL:4633\n\nTP:4624\nTP:4622\nTP:4620\nTP:open",
        "Gold sell now 4629-4626\n\nSL:4633\n\nTP:4624\nTP:4622\nTP:4620\nTP:open",
        "📈 XAUUSD SELL NOW\nEntry: 4660-70 KIS RISK\n🛑 SL: 4680\n🎯 TP1: 4655\n🎯 TP2: 4650\n🎯 TP3: 4645\n🎯 TP4: 4640\n🎯 TP5: 4630\n🛡 Always use stop-loss & proper money management",
        "📈 XAUUSD SELL NOW\nEntry: 4695-05 KIS RISK\n🛑 SL: 4680\n🎯 TP1: 4655\n🎯 TP2: 4650\n🎯 TP3: 4645\n🎯 TP4: 4640\n🎯 TP5: 4630\n🛡 Always use stop-loss & proper money management",
        "XAUUSD Signal:\n📈 Entry Direction: (SELL)\n✅ Entry Point: (4652/4655)\n✅TP1  :4647\n✅TP2 :4642\n✅TP3 :4637\n✅TP4 :4632\n ❌Stop Loss: 4665\nHigh Risk Setup – Gold's current trading volume remains relatively low, but a price structure has formed. Please manage your risk carefully.",
    ]

    wrong_signal = [
        "XAUUSD BUY NOW❗\n@ 4433- 4428\n\nSL🛑44\nTP✅44\n\n#FollowRapatAn ❗️❗️❗️❗️❗️ ❗️",
        "XAUUSD BUY NOW❗\n@ 4433- 44\n\nSL🛑44\nTP✅4443\nTP4445\n#FollowRapatAn ❗️❗️❗️❗️❗️ ❗️",
        "Gold sell now 4626-4629\n\nSL:4623\n\nTP:4624\nTP:4622\nTP:4620\nTP:open",
        "TP3 HIT! \nSELL GOLD @ 4686\n\nTarget 3 Reached: 4671\n\nProfit on TP3: +150 Pips\n\nGold followed our Free Channel Signal perfectly — moving exactly as predicted!\n\nStay tuned for next targets — more profits ahead!",
    ]

    for s in wrong_signal:
        pprint(signal_parser(s))



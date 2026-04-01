import json
from datetime import datetime
from pprint import pprint

import zmq
from time import sleep
from openai import OpenAI

from capital_api import new_session, create_position, get_prices

client = OpenAI(
    api_key="",
    base_url="https://api.perplexity.ai"
)

SIGNAL_SCHEMA = {
    "type": "object",
    "properties": {
        "valid": {"type": "boolean"},
        "direction": {"type": "string"},
        "entry": {
            "type": ["object", "null"],
            "properties": {
                "min": {"type": "number"},
                "max": {"type": "number"}
            },
            "required": ["min", "max"],
            "additionalProperties": False
        },
        "sl": {"type": ["number", "null"]},
        "tp": {
            "type": ["array", "null"],
            "items": {"type": "number"}
        }
    },
    "required": ["valid", "entry", "sl", "tp"],
    "additionalProperties": False
}

SYSTEM_PROMPT = """
Te egy XAUUSD trade signal parser vagy.
A feladatod: egy szabad szöveges üzenetből kinyerni a trade adatokat.

Csak akkor tekintsd érvényesnek az üzenetet, ha egyértelműen tartalmazza:
- instrumentum: XAUUSD vagy GOLD
- BUY vagy SELL
- belépési zóna vagy entry range
- stop loss (SL)
- legalább 1 take profit (TP)

Első információ általában a direction (sell/buy).
Második információ a belépési pont (egy vagy 2 szám)
Harmadik az SL, ezt mindig SL-lel jelölik és végül egy vagy több TP.

Kimeneti szabályok:
- Ha az üzenet helyes és egyértelmű, adj vissza JSON-t ebben a formában:
  {
    "valid": true,
    "direction": null,
    "entry": null,
    "sl": null,
    "entry": {"min": ..., "max": ...},
    "sl": ...,
    "tp": [...]
  }

- Ha az üzenet hibás, hiányos, nem XAUUSD-re vonatkozik, vagy nem értelmezhető egyértelműen, akkor:
  {
    "valid": false,
    "direction": null,
    "entry": null,
    "sl": null,
    "tp": null
  }

Szabályok:
- Ha csak egyetlen belépési ár van, akkor entry.min = entry.max.
- A tp és sl megfelel a buy vagy sell iránynak
- A tp lista minden eleme szám legyen.
- Ne találj ki hiányzó értékeket.
- Csak valid JSON-t adj vissza.
"""


def parse_xauusd_signal(message: str):
    response = client.chat.completions.create(
        model="sonar-pro",
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": message}
        ],
        temperature=0,
        extra_body={
            "response_format": {
                "type": "json_schema",
                "json_schema": {
                    "name": "xauusd_signal_parser",
                    "schema": SIGNAL_SCHEMA
                }
            }
        }
    )

    content = response.choices[0].message.content.strip()
    data = json.loads(content)

    if not data.get("valid"):
        return None

    return {
        "direction": data["direction"],
        "entry": {
            "min": data["entry"]["min"],
            "max": data["entry"]["max"]
        },
        "sl": data["sl"],
        "tp": data["tp"]
    }


if __name__ == "__main__":
    import emoji
    # m = "XAUUSD BUY NOW❗\n@ 4433- 4428\n\nSL🛑4423\nTP✅4443\n\n#FollowRapatAn ❗️❗️❗️❗️❗️ ❗️"
    # m = "XAUUSD BUY NOW❗\n@ 4433- 4428\n\nSL🛑44\nTP✅44\n\n#FollowRapatAn ❗️❗️❗️❗️❗️ ❗️"
    m = "XAUUSD BUY NOW❗\n@ 4433- 44\n\nSL🛑44\nTP✅4443\nTP4445\n#FollowRapatAn ❗️❗️❗️❗️❗️ ❗️"
    m = "📉 XAU/USD BUY NOW\n\n✨ Entries:\nEntry 1: @4462\nEntry 2: @4459 (Recovery Zone)\n\n🚨 Stop Loss (SL): @4449\n(Strictly follow)\n\n🎯 Take Profit Targets:\n\n✅ TP 1: @4467\n✅ TP 2: @4472\n✅ TP 3: @4477\n✅ TP 4: @4482\n💡 Pro Tip: Always use proper money management to protect your capita"
    signal = "📉 XAU/USD SELL NOW\n\n✨ Entries:\nEntry 1: @4543\nEntry 2: @4547 (Recovery Zone)\n\n🚨 Stop Loss (SL): @4557\n(Strictly follow)\n\n🎯 Take Profit Targets:\n\n✅ TP 1: @4538\n✅ TP 2: @4533\n✅ TP 3: @4528\n✅ TP 4: Open\n\n💡 Pro Tip: Always use proper money management to protect your capital"
    # signal = "📉 XAU/USD SELL NOW\n\n✨ Entries:\nEntry 1: @4543\n\n🚨 Stop Loss (SL): @4557\n(Strictly follow)\n\n🎯 Take Profit Targets:\n\n✅ TP 1: @4538\n✅ TP 2: @4533\n✅ TP 3: @4528\n✅ TP 4: @4523\n\n💡 Pro Tip: Always use proper money management to protect your capital"
    signal = "🚨 SIGNAL ALERT 🚨\n\n🌐 XAUUSD\n\n📊 Trade Details:📈 BUY\n\n⚪️ Entry Point: 4530\n🔴 Stop Loss (SL): 4520\n\n🟢 Take Profit 1 (TP1): 4535\n🟢 Take Profit 2 (TP2): 4540\n🟢 Take Profit 3 (TP3): 4545"
    # signal = "XAUUSD BUY NOW❗️\n@ 4549 - 4544\n\nSL🛑4539\nTP✅4559\n\n#FollowRapatAn ❗️❗️❗️❗️❗️ ❗️"
    signal = emoji.replace_emoji(signal, replace='')

    print(signal.upper())
    print(("XAU" in signal.upper() or "GOLD" in signal.upper()) and (
                "SELL" in signal.upper() or "BUY" in signal.upper()))
    pos_dict = parse_xauusd_signal("signal")
    # pos_dict = {'direction': 'BUY', 'entry': {'min': 4560, 'max': 4566}, 'sl': 4550, 'tp': [4569, 4568, 4567]}
    print(pos_dict)
    exit()

    context = zmq.Context()
    socket = context.socket(zmq.PUSH)
    socket.connect("tcp://localhost:5555")
    socket.send_pyobj({"raw_text": signal,
                       "position_dict": pos_dict,
                       "chat_id": None,
                       "send_date": datetime.now().strftime('%y:%m:%d:%H:%M:%S')
                       })

    # while True:
    #     context = zmq.Context()
    #     socket = context.socket(zmq.PULL)
    #     socket.connect("tcp://localhost:5555")
    #     signal = socket.recv_pyobj()["text"]
    #
    #     print(signal)
    #     if ("xau" in signal.capitalize() or "gold" in signal.capitalize()) and ("sell" in signal.capitalize() or "buy" in signal.capitalize()):
    #         position_dict = parse_xauusd_signal(m)
    #         print(position_dict)
    #     print("\n" + "*" * 10 + "\n")

import json
import zmq
from datetime import datetime, timezone
from telethon import TelegramClient, events
from signal_parser import signal_parser
from pprint import pprint

with open('keys_urls.json', 'r') as f:
    config = json.load(f)

API_ID = config["telegram_api_id"]
API_HASH = config["telegram_api_hash"]

# A csatornák listája (username vagy numerikus ID)
traderz_gold_wip = -1003496306840
technical_pips_vip = -1002001216034
gold_trader_mo = "@gold_Trader_mo_gtmofx_Official"

ann_zerofloat = "@livetradeann"
gold_signal_vip = "@GoldSignalsVipOriginal"
mychal_fx = "@CHEMPION_HUB"
CHANNELS = [
    # traderz_gold_wip,
    # ann_zerofloat,
    technical_pips_vip,
    # gold_trader_mo,
    # gold_signal_vip,
    # mychal_fx
]

client = TelegramClient('session_neve', API_ID, API_HASH)

context = zmq.Context()
socket = context.socket(zmq.PUSH)
socket.connect("tcp://localhost:5555")

tp_idx_map = {
    1: 3,
    2: 2
}
entry_zone_expand = 1


def log_print(message, logfile="app.log"):
    pprint(message)
    with open(logfile, "a", encoding="utf-8") as f:
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        f.write(f"{now} - {message}\n")


def send_position(event, edited):
    signal_dict, error_msg = signal_parser(event.raw_text)

    if len(signal_dict) == 0:
        log_print("wrong message\n", "telegram.log")
        log_print(error_msg)
    else:
        log_print("message ok\n", "telegram.log")
        # log_print(event.raw_text)
        for tp_idx, tp in enumerate(signal_dict["tp_list"]):
            position_dict = {
                "epic": "GOLD",
                "direction": signal_dict["direction"],
                "size": 1.0 * tp_idx_map.get(tp_idx + 1, 1),
                "zone_low": min(signal_dict["entries"]) - entry_zone_expand,
                "zone_high": max(signal_dict["entries"]) + entry_zone_expand,
                "tp": tp,
                "sl": signal_dict["sl_list"][0],
                "tp_idx": tp_idx + 1,
                "raw_text": event.raw_text,
                "send_date": datetime.now(timezone.utc).strftime('%y:%m:%d:%H:%M:%S'),
                "edited": edited,
                "chat_id": event.chat.id,
                "chat_name": event.chat.title,
            }
            socket.send_pyobj(position_dict)

    # if ("xau" in event.raw_text.capitalize() or "gold" in event.raw_text.capitalize()) and (
    #         "sell" in event.raw_text.capitalize() or "buy" in event.raw_text.capitalize()):
    #     position_dict = parse_xauusd_signal(event.raw_text)
    #     if position_dict is not None:
    #         socket.send_pyobj({"raw_text": event.raw_text,
    #                            "position_dict": position_dict,
    #                            "chat_id": event.chat.id,
    #                            "send_date": datetime.now().strftime('%y:%m:%d:%H:%M:%S'),
    #                            "edited": edited
    #                            })
    #         log_print("message ok\n", "telegram.log")
    #     else:
    #         log_print("message parse error\n", "telegram.log")
    # else:
    #     log_print("wrong message\n", "telegram.log")


@client.on(events.NewMessage(chats=CHANNELS))
async def on_new_message(event):
    chat = await event.get_chat()
    log_print(f"[NEW][{chat.title}] id={event.id}\n\n{event.raw_text}", "telegram.log")
    log_print("\n" + "*" * 10, "telegram.log")
    send_position(event, edited=False)


@client.on(events.MessageEdited(chats=CHANNELS))
async def on_edited_message(event):
    chat = await event.get_chat()
    log_print(f"[EDIT][{chat.title}] id={event.id}\n\n{event.raw_text}", "telegram.log")
    log_print("\n" + "*" * 10, "telegram.log")
    send_position(event, edited=True)


if __name__ == '__main__':
    client.start()
    try:
        client.run_until_disconnected()
    except KeyboardInterrupt:
        socket.close()

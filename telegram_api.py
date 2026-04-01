import zmq
from datetime import datetime
from telethon import TelegramClient, events

from backtrader_zmq import log_print
from perplexity_api import parse_xauusd_signal

API_ID = -1
API_HASH = ''

# A csatornák listája (username vagy numerikus ID)
traderz_gold_wip = -3496306840
ann_zerofloat = "@livetradeann"
technical_pips = "@Technicalpipshuk50"
gold_trader_mo = "@gold_Trader_mo_gtmofx_Official"
gold_signal_vip = "@Gold_Signal_Vip_Official"
mychal_fx = "@CHEMPION_HUB"
CHANNELS = [traderz_gold_wip, ann_zerofloat, technical_pips, gold_trader_mo, gold_signal_vip, mychal_fx]

client = TelegramClient('session_neve', API_ID, API_HASH)

context = zmq.Context()
socket = context.socket(zmq.PUSH)
socket.connect("tcp://localhost:5555")


def send_position(event):
    if ("xau" in event.raw_text.capitalize() or "gold" in event.raw_text.capitalize()) and (
            "sell" in event.raw_text.capitalize() or "buy" in event.raw_text.capitalize()):
        position_dict = parse_xauusd_signal(event.raw_text)
        if position_dict is not None:
            socket.send_pyobj({"raw_text": event.raw_text,
                               "position_dict": position_dict,
                               "chat_id": event.chat.id,
                               "send_date": datetime.now().strftime('%y:%m:%d:%H:%M:%S')
                               })
            log_print("message ok\n", "telegram.log")
        else:
            log_print("message parse error\n", "telegram.log")
    else:
        log_print("wrong message\n", "telegram.log")


@client.on(events.NewMessage(chats=CHANNELS))
async def on_new_message(event):
    chat = await event.get_chat()
    log_print(f"[NEW][{chat.title}] id={event.id}\n\n{event.raw_text}", "telegram.log")
    log_print("\n" + "*" * 10, "telegram.log")
    send_position(event)


@client.on(events.MessageEdited(chats=CHANNELS))
async def on_edited_message(event):
    chat = await event.get_chat()
    log_print(f"[EDIT][{chat.title}] id={event.id}\n\n{event.raw_text}", "telegram.log")
    log_print("\n" + "*" * 10, "telegram.log")
    send_position(event)


if __name__ == '__main__':
    client.start()
    try:
        client.run_until_disconnected()
    except KeyboardInterrupt:
        socket.close()

"""
Telegram listener — feliratkozik csatornákra, beolvas új és szerkesztett üzeneteket,
archiválja MINDET (parsolt + nyers) a SignalArchive-ban, és a parser-által érvényes
jeleket ZMQ-n keresztül továbbküldi a position_manager-nek.
"""
import sys
from datetime import datetime, timezone
from pathlib import Path
from pprint import pprint

import yaml
import zmq
from telethon import TelegramClient, events

# A repository-gyökeret importálnunk kell a `signals` package-hoz
ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from signals.archive import SignalArchive
from signals.parsers import parse
from utils.keys import load_keys


config = load_keys()
API_ID = config["telegram_api_id"]
API_HASH = config["telegram_api_hash"]

# ── Csatornák — a signals/channels.yaml-ból ──
def _load_channels() -> list:
    cfg_path = Path(__file__).resolve().parent / "channels.yaml"
    with open(cfg_path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}
    out = []
    for entry in raw.get("channels", []):
        if not entry.get("enabled", False):
            continue
        out.append(entry["chat"])
    return out


CHANNELS = _load_channels()
print(f"[INIT] Telegram csatornák: {CHANNELS}")

# ── ZMQ + Telethon kliens ──
client = TelegramClient(str(ROOT / "session_neve"), API_ID, API_HASH)

context = zmq.Context()
socket = context.socket(zmq.PUSH)
socket.connect("tcp://localhost:5555")

# ── Pozíció méretezés TP-szám szerint ──
# TP1-en a legnagyobb pozíció (3 lot), TP2-n 2 lot, TP3-tól 1 lot.
# Ha valaki később konfigfile-ba akarja venni: ez ide jön.
TP_IDX_SIZE_MAP = {
    1: 3.0,
    2: 2.0,
}
DEFAULT_TP_SIZE = 1.0
ENTRY_ZONE_EXPAND = 1.0

# ── SignalArchive ──
archive = SignalArchive(ROOT / "data" / "signals")


def log_print(message, logfile=str(ROOT / "telegram.log")):
    pprint(message)
    with open(logfile, "a", encoding="utf-8") as f:
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        f.write(f"{now} - {message}\n")


def _send_position(event, edited: bool) -> None:
    raw = event.raw_text
    parsed = parse(raw, chat_id=event.chat.id)

    # MINDEN üzenet archiválódik — a parser-fejlesztéshez utólag is van adat
    archive.append(
        raw_text=raw,
        parsed=parsed,
        chat_id=event.chat.id,
        chat_name=getattr(event.chat, "title", "") or "",
        message_id=event.id,
        edited=edited,
        parent_message_id=event.id if edited else None,
    )

    if not parsed.valid:
        log_print(f"[PARSE] dropped: {parsed.reason}")
        return

    log_print(f"[PARSE] ok: {parsed.direction} {parsed.entry_low}-{parsed.entry_high} "
              f"SL={parsed.sl} TPs={parsed.tp_list}")

    # Minden TP-re külön ZMQ üzenet (a position_manager külön pozíciókként kezeli)
    for tp_idx, tp in enumerate(parsed.tp_list):
        size = TP_IDX_SIZE_MAP.get(tp_idx + 1, DEFAULT_TP_SIZE)
        position_dict = {
            "epic": "GOLD",
            "direction": parsed.direction,
            "size": float(size),
            "zone_low": parsed.entry_low - ENTRY_ZONE_EXPAND,
            "zone_high": parsed.entry_high + ENTRY_ZONE_EXPAND,
            "tp": float(tp),
            "sl": float(parsed.sl),
            "tp_idx": tp_idx + 1,
            "raw_text": raw,
            "send_date": datetime.now(timezone.utc).strftime("%y:%m:%d:%H:%M:%S"),
            "edited": edited,
            "chat_id": event.chat.id,
            "chat_name": getattr(event.chat, "title", "") or "",
            "message_id": event.id,
        }
        socket.send_pyobj(position_dict)


@client.on(events.NewMessage(chats=CHANNELS))
async def on_new_message(event):
    chat = await event.get_chat()
    log_print(f"[NEW][{chat.title}] id={event.id}\n\n{event.raw_text}")
    _send_position(event, edited=False)


@client.on(events.MessageEdited(chats=CHANNELS))
async def on_edited_message(event):
    chat = await event.get_chat()
    log_print(f"[EDIT][{chat.title}] id={event.id}\n\n{event.raw_text}")
    _send_position(event, edited=True)


if __name__ == "__main__":
    client.start()
    try:
        client.run_until_disconnected()
    except KeyboardInterrupt:
        socket.close()

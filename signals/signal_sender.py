"""
Fejlesztői teszt-utility: szimulál egy Telegram-üzenetet, parse-olja az új
parser-rel, és ZMQ-n keresztül továbbküldi a position_manager-nek.

Használat:
    python -m signal.signal_sender    # példa-jel küldése
"""
import sys
from datetime import datetime, timezone
from pathlib import Path

import zmq

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from signals.parsers import parse


TP_IDX_SIZE_MAP = {
    1: 3.0,
    2: 2.0,
}
DEFAULT_TP_SIZE = 1.0
ENTRY_ZONE_EXPAND = 1.0


def send_signal(raw_signal: str) -> None:
    context = zmq.Context()
    socket = context.socket(zmq.PUSH)
    socket.connect("tcp://localhost:5555")

    parsed = parse(raw_signal)
    if not parsed.valid:
        print(f"[SENDER] parse failed: {parsed.reason}")
        socket.close()
        return

    print(f"[SENDER] sending {parsed.direction} | {parsed.entry_low}-{parsed.entry_high} "
          f"| SL={parsed.sl} | TPs={parsed.tp_list}")

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
            "raw_text": raw_signal,
            "send_date": datetime.now(timezone.utc).strftime("%y:%m:%d:%H:%M:%S"),
            "edited": False,
            "chat_id": 0,
            "chat_name": "test",
            "message_id": int(datetime.now(timezone.utc).timestamp()),
        }
        socket.send_pyobj(position_dict)
    socket.close()


if __name__ == "__main__":
    actual_price = 4684
    signal = (
        f"#XAUUSD BUY {actual_price - 5}-{actual_price + 5}\n"
        f"TP {actual_price + 15}\n"
        f"TP {actual_price + 20}\n"
        f"TP {actual_price + 25}\n"
        f"TP {actual_price + 30}\n"
        f"\nSL {actual_price - 15}\n"
    )
    send_signal(signal)

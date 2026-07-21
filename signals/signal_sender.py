import zmq
import json
import time
from datetime import datetime, timezone
from signal_parser import signal_parser

tp_idx_map = {
    1: 3,
    2: 2
}
entry_zone_expand = 1


def send_signal(raw_signal):
    context = zmq.Context()
    socket = context.socket(zmq.PUSH)
    socket.connect("tcp://localhost:5555")

    signal_dict, error_msg = signal_parser(raw_signal)

    if len(signal_dict) == 0:
        print("wrong message\n", "telegram.log")
        print(error_msg)
    else:
        print("message ok\n", "telegram.log")
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
                "raw_text": raw_signal,
                "send_date": datetime.now(timezone.utc).strftime('%y:%m:%d:%H:%M:%S'),
                "edited": False,
                "chat_id": 000,
                "chat_name": "test",
            }
            socket.send_pyobj(position_dict)
    socket.close()


# Példa jelek
if __name__ == '__main__':
    actual_price = 4684
    signal = "#XAUUSD BUY{}-{}\nTP {}\nTP {}\nTP {}\nTP {}\nTP {}\nTP {}\n\nSL {}\n".format(actual_price - 5,
                                                                                            actual_price + 5,
                                                                                            actual_price + 15,
                                                                                            actual_price + 20,
                                                                                            actual_price + 25,
                                                                                            actual_price + 30,
                                                                                            actual_price + 35,
                                                                                            actual_price + 40,
                                                                                            actual_price - 15)
    send_signal(signal)

# def next(self):
#         if self.position:
#             return
#
#         # ZMQ jel ellenőrzése
#         new_signal = self.zmq_receiver.get_signal()
#
#         if new_signal is not None:
#             self.signals.append(new_signal)
#
#         for signal in self.signals:
#             if signal["sent_date"] + timedelta(minutes=10) < datetime.now():
#                 print("signal is expired with sent date: {}".format(signal["sent_date"]))
#                 self.signals.remove(signal)
#                 continue
#
#             current_price = self.datas[0].close[0]
#             direction = signal['direction'].upper()
#             lot_size = signal['lot_size']
#             sl_price = signal['sl']
#             tp_price = signal['tp']
#             entry_low = signal['entry_low']
#             entry_high = signal['entry_high']
#
#             self.log(f"🎯 JEL: {direction} {lot_size}lot | Entry: [{entry_low:.1f}-{entry_high:.1f}]")
#
#             # Entry tartomány ellenőrzése
#             if entry_low <= current_price <= entry_high:
#                 size = lot_size * 100000  # Forex lot -> units
#
#                 if direction == 'BUY':
#                     self.order = self.buy(size=size)
#                     self.sell(exectype=bt.Order.Stop, price=sl_price, size=size)
#                     self.sell(exectype=bt.Order.Limit, price=tp_price, size=size)
#                     self.log(f"🚀 LONG NYITVA @ {current_price:.5f} | SL:{sl_price:.1f} TP:{tp_price:.1f}")
#
#                 elif direction == 'SELL':
#                     self.order = self.sell(size=size)
#                     self.buy(exectype=bt.Order.Stop, price=sl_price, size=size)
#                     self.buy(exectype=bt.Order.Limit, price=tp_price, size=size)
#                     self.log(f"🔻 SHORT NYITVA @ {current_price:.5f} | SL:{sl_price:.1f} TP:{tp_price:.1f}")
#             else:
#                 self.log(f"⏳ Entry kívül: {current_price:.1f} vs [{entry_low:.1f}-{entry_high:.1f}]")

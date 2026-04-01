import zmq
import json
import time
from datetime import datetime

def send_signal(direction, lot_size, sl, tp, entry_low, entry_high):
    context = zmq.Context()
    socket = context.socket(zmq.PUSH)
    socket.bind("tcp://localhost:5556")
    
    signal = {
        'direction': direction,  # 'BUY' vagy 'SELL'
        'lot_size': lot_size,    # pl. 0.1, 0.5, 1.0
        'sl': sl,      # pl. 30, 50
        'tp': tp,      # pl. 60, 100
        'entry_low': entry_low,  # pl. 2500.50
        'entry_high': entry_high, # pl. 2501.00
        'sent_date': datetime.now().strftime('%y:%m:%d:%H:%M:%S')
    }
    
    # socket.send_json({"mama": datetime.now().strftime('%H:%M:%S')})
    socket.send_pyobj(signal)
    print(f"📤 Jel elküldve: {signal}")
    socket.close()

# Példa jelek
if __name__ == '__main__':
    send_signal('BUY', 0.01, 5140, 5150, 5143, 5146)





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

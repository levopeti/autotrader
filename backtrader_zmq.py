import zmq
import threading
import time
from pprint import pprint
from datetime import datetime, timedelta

from capital_api import get_prices, create_position

EXPIRATION_TIME = timedelta(minutes=10)


def log_print(message, logfile="app.log"):
    pprint(message)
    with open(logfile, "a", encoding="utf-8") as f:
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        f.write(f"{now} - {message}\n")


class ZMQSignalReceiver:
    def __init__(self):
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.PULL)
        self.socket.bind("tcp://localhost:5555")
        # self.socket.subscribe("")
        # self.socket.setsockopt_string(zmq.SUBSCRIBE, "")
        self.pending_signals = []
        log_print("ZMQ Server: tcp://localhost:5555")

    def receive_signals(self):
        """
        {"raw_text": event.raw_text,
         "position_dict": position_dict,
         "chat_id": event.chat.id,
         "send_date": datetime.now().strftime('%y:%m:%d:%H:%M:%S')
        }
        """
        while True:
            try:
                signal_dict = self.socket.recv_pyobj()
                signal_dict["rec_date"] = datetime.now()
                signal_dict["send_date"] = datetime.strptime(signal_dict["send_date"], '%y:%m:%d:%H:%M:%S')
                # signal = json.loads(message)
                self.pending_signals.append(signal_dict)
                log_print("New signal:")
                log_print(signal_dict["position_dict"])
            except zmq.Again:
                time.sleep(0.01)

    def get_signal(self):
        if self.pending_signals:
            return self.pending_signals.pop(0)
        return None


class Signal:
    def __init__(self, signal_dict):
        self.signal_dict = signal_dict
        self.position_dict = self.signal_dict["position_dict"]
        self.valid_position = self.check_position()

        if not self.valid_position:
            log_print(("invalid position", self.signal_dict))

        self.expired = False
        self.done = False

        self._max_entry_diff = 1

    def check_position(self):
        _position_dict = self.signal_dict["position_dict"]
        if _position_dict["entry"]["min"] == _position_dict["entry"]["max"]:
            _position_dict["entry"]["min"] -= 1.5
            _position_dict["entry"]["max"] += 1.5

        if _position_dict["entry"]["min"] > _position_dict["entry"]["max"]:
            log_print("min > max")
            return False

        if _position_dict["direction"] == "BUY":
            if _position_dict["sl"] >= _position_dict["entry"]["min"]:
                log_print("sl > min")
                return False

            if min(_position_dict["tp"]) <= _position_dict["entry"]["max"]:
                log_print("tp < max")
                return False

        if _position_dict["direction"] == "SELL":
            if _position_dict["sl"] <= _position_dict["entry"]["max"]:
                log_print("sl < max")
                return False

            if max(_position_dict["tp"]) >= _position_dict["entry"]["min"]:
                log_print("tp > min")
                return False
        return True

    def is_expired(self):
        if self.signal_dict["send_date"] + EXPIRATION_TIME < datetime.now():
            log_print(("option expired ({})".format(self.signal_dict["send_date"]), self.signal_dict))

            self.expired = True

    @staticmethod
    def current_price():
        try:
            price = get_prices()["prices"][-1]
        except KeyError:
            breakpoint()
        return price

    def try_activate(self):
        price_dict = self.current_price()
        key = "ask" if self.position_dict["direction"] == "BUY" else "bid"
        high = price_dict["highPrice"][key]
        low = price_dict["lowPrice"][key]
        close = price_dict["closePrice"][key]

        entry_min = self.position_dict["entry"]["min"]
        entry_max = self.position_dict["entry"]["max"]

        if max(0, min(entry_max, high) - max(entry_min, low)) != 0:
            # price moved into the range
            if self.position_dict["direction"] == "BUY" and entry_min < close + self._max_entry_diff or \
                    self.position_dict["direction"] == "SELL" and entry_max > close - self._max_entry_diff:

                for tp in self.position_dict["tp"]:
                    position = {
                        "epic": "GOLD",
                        "direction": self.position_dict["direction"],
                        "size": 1,
                        "guaranteedStop": False,
                        "stopLevel": self.position_dict["sl"],
                        "profitLevel": tp
                    }
                    log_print(create_position(position))

                    self.done = True
                    log_print("{} NOW {}-{}, SL: {}, TP: {}, PRICE: {}".format(self.position_dict["direction"], entry_max,
                                                                               entry_min,
                                                                               self.position_dict["sl"],
                                                                               tp, close))
            else:
                log_print(
                    ("Price moved too much. close: {}, entry min: {}, entry max: {}".format(close, entry_min, entry_max),
                    self.signal_dict))


class LiveTrader:
    def __init__(self):
        self.signals = list()
        self.done_signals = list()
        self.signal_receiver = ZMQSignalReceiver()

        zmq_thread = threading.Thread(target=self.signal_receiver.receive_signals, daemon=True)
        zmq_thread.start()

    def receive_signals(self):
        """ZMQ thread"""
        signal = self.signal_receiver.get_signal()
        while signal is not None:
            self.signals.append(Signal(signal))
            signal = self.signal_receiver.get_signal()

    def iterate_signals(self):
        for signal in self.signals:
            if not signal.done:
                signal.try_activate()

            signal.is_expired()

            if signal.expired or signal.done:
                self.done_signals.append(signal)
                self.signals.remove(signal)

    def run(self):
        try:
            while True:
                self.receive_signals()
                self.iterate_signals()
                time.sleep(10)
        except KeyboardInterrupt:
            log_print("STOP")


if __name__ == '__main__':
    log_print("START")
    trader = LiveTrader()
    trader.run()

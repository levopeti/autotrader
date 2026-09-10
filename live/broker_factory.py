"""Broker-factory: névből → BrokerClient. A run_live.py `--broker` flag ezt hívja."""
from __future__ import annotations
from typing import Optional

from .broker import BrokerClient


def make_broker(name: str, demo: bool = True,
                account_id: Optional[str] = None) -> BrokerClient:
    n = (name or "capital").lower()
    if n == "capital":
        from .capital_broker import CapitalBroker
        return CapitalBroker(demo=demo, account_id=account_id)
    if n == "etoro":
        from .etoro_broker import EtoroBroker
        return EtoroBroker(demo=demo, account_id=account_id)
    raise ValueError(f"Ismeretlen broker: {name!r} (választható: capital, etoro)")


def tf_resolution_map(broker: BrokerClient) -> dict:
    """A stratégia candle_tf-jét broker-specifikus resolution-re fordítja."""
    from .capital_broker import TF_TO_RESOLUTION as CAP
    from .etoro_broker import TF_TO_RESOLUTION as ETO
    from .capital_broker import CapitalBroker
    from .etoro_broker import EtoroBroker
    if isinstance(broker, CapitalBroker):
        return CAP
    if isinstance(broker, EtoroBroker):
        return ETO
    return CAP  # sensible default

"""
Position Sizer
==============
Dinamikus lot-méret számítás kockázatkezelési szabályok alapján.

Módszerek:
  - fixed_lot:    Fix lot méret
  - fixed_risk:   Fix % kockázat / trade
  - volatility:   ATR-alapú adaptív méretezés
"""

import numpy as np
from typing import Literal


class PositionSizer:

    def __init__(
        self,
        equity:    float = 100_000.0,
        risk_pct:  float = 0.01,
        max_lot:   float = 5.0,
        min_lot:   float = 0.01,
        method: Literal["fixed_lot", "fixed_risk", "volatility"] = "fixed_risk",
    ):
        self.equity   = equity
        self.risk_pct = risk_pct
        self.max_lot  = max_lot
        self.min_lot  = min_lot
        self.method   = method

    def calculate(
        self,
        sl_dollars: float,
        atr:        float = 1.0,
        fixed_lot:  float = 1.0,
    ) -> float:
        """
        sl_dollars : SL távolság $-ban
        atr        : jelenlegi ATR (volatility method-hoz)
        fixed_lot  : fix_lot method-hoz
        """
        if self.method == "fixed_lot":
            lot = fixed_lot

        elif self.method == "fixed_risk":
            # lot = (equity * risk_pct) / sl_dollars
            lot = (self.equity * self.risk_pct) / (sl_dollars + 1e-10)

        elif self.method == "volatility":
            base = (self.equity * self.risk_pct) / (sl_dollars + 1e-10)
            norm = atr / (sl_dollars + 1e-10)
            lot  = base / max(norm, 0.5)

        else:
            lot = fixed_lot

        return float(np.clip(round(lot, 2), self.min_lot, self.max_lot))

    def update_equity(self, new_equity: float):
        """Tőke frissítése compound lot-számításhoz."""
        self.equity = new_equity

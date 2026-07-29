"""
2D Kalman filter GOLD-SILVER pairs trade-hez.

State: [alpha, beta] — dinamikus lineáris regresszió (gold = alpha + beta * silver + noise)
Observation: gold_t (silver_t H-mátrixban van)

Használat:
    kf = KalmanFilter(Q_alpha=1e-5, Q_beta=1e-6, R=25.0)
    kf.load_state(...) or kf.warmup_ols(gold_arr, silver_arr)
    for each (gold, silver) tick:
        z, sigma = kf.update(gold, silver)
        if abs(z) > K_entry: ... signal
    kf.save_state(path)
"""
from __future__ import annotations
import json
from dataclasses import dataclass, asdict, field
from pathlib import Path
from typing import Optional, Tuple, List

import numpy as np


@dataclass
class KalmanState:
    """Perzisztálható állapot."""
    alpha: float = 0.0
    beta: float = 1.0
    P: List[List[float]] = field(default_factory=lambda: [[1.0, 0.0], [0.0, 1.0]])
    last_ts: Optional[str] = None
    tick_count: int = 0
    warmed_up: bool = False
    R: Optional[float] = None    # EWMA módban itt tároljuk a jelenlegi R-t (restart-safe)


class KalmanFilter:
    """
    2D Kalman: gold_t = alpha_t + beta_t * silver_t + v_t
    - state = [alpha, beta]
    - observation model: H_t = [1, silver_t]
    - process noise: Q = diag(Q_alpha, Q_beta) — állapotok random walk
    - obs noise: R (skalár, gold-mérés varianciája)
    """

    def __init__(self, Q_alpha: float = 1e-5, Q_beta: float = 1e-6, R: float = 25.0,
                 ewma_alpha: float = 0.0):
        """
        ewma_alpha > 0 esetén az R paraméter minden update után EWMA-ban
        adaptálódik: R_new = (1-α)·R_old + α·residual². Ez a "regime-adaptív"
        Kalman variánsa — a fix R rossz a regime-váltásoknál (backtest bizonyíték
        2 éves adaton: fix R DD=-$1231, EWMA α=0.0005 DD=-$108, 11× jobb).
        """
        self.Q_alpha = Q_alpha
        self.Q_beta = Q_beta
        self.R = R                  # jelenlegi R (EWMA módban változik)
        self.ewma_alpha = ewma_alpha
        self._Q = np.array([[Q_alpha, 0.0], [0.0, Q_beta]])
        self.state = KalmanState()

    def warmup_ols(self, gold: np.ndarray, silver: np.ndarray) -> None:
        """Egyszeri OLS fit a warmup adatokra. Az utolsó bar-tól használható."""
        assert len(gold) == len(silver) and len(gold) > 10
        X = np.vstack([np.ones(len(silver)), silver]).T
        b, *_ = np.linalg.lstsq(X, gold, rcond=None)
        self.state.alpha = float(b[0])
        self.state.beta = float(b[1])
        # P kis kezdő szórással, hogy Kalman ne "zuhanjon" azonnal
        self.state.P = [[1.0, 0.0], [0.0, 1.0]]
        self.state.tick_count = len(gold)
        self.state.warmed_up = True

    def update(self, gold: float, silver: float, ts: Optional[str] = None
                ) -> Tuple[float, float, float]:
        """
        1 lépés update. Visszaad: (residual, sigma, z_score).
        residual = gold - (alpha + beta*silver)  ($)
        sigma = sqrt(P_pred + R)                 (residual std)
        z = residual / sigma
        """
        if not self.state.warmed_up:
            # Első tick — csak eltárol, nem generál signal-t
            self.state.alpha = gold - self.state.beta * silver
            self.state.warmed_up = True
            self.state.tick_count += 1
            return (0.0, 1.0, 0.0)

        P = np.array(self.state.P)
        # Predict — Q arányos az aktuális R-hez ha EWMA mód (regime-adaptív)
        if self.ewma_alpha > 0:
            self._Q = np.array([[self.R * 1e-6, 0.0], [0.0, self.R * 1e-9]])
        P = P + self._Q
        # Update
        H = np.array([1.0, silver])
        y_pred = H @ np.array([self.state.alpha, self.state.beta])
        residual = gold - y_pred
        S_var = float(H @ P @ H.T + self.R)
        sigma = np.sqrt(S_var)
        K = P @ H.T / S_var
        self.state.alpha += float(K[0]) * residual
        self.state.beta += float(K[1]) * residual
        P = (np.eye(2) - np.outer(K, H)) @ P
        self.state.P = P.tolist()
        self.state.last_ts = ts
        self.state.tick_count += 1
        # EWMA R adaptáció (regime-detektor beépített)
        if self.ewma_alpha > 0:
            self.R = (1 - self.ewma_alpha) * self.R + self.ewma_alpha * residual * residual
        z = residual / sigma if sigma > 0 else 0.0
        return (float(residual), float(sigma), float(z))

    def save_state(self, path: Path) -> None:
        # EWMA módban a jelenlegi R-t is elmentjük restart-safe módon
        if self.ewma_alpha > 0:
            self.state.R = self.R
        path.write_text(json.dumps(asdict(self.state), indent=2))

    def load_state(self, path: Path) -> bool:
        if not path.exists():
            return False
        try:
            d = json.loads(path.read_text())
            self.state = KalmanState(**d)
            # EWMA módban visszaállítjuk a mentett R-t
            if self.ewma_alpha > 0 and self.state.R is not None:
                self.R = self.state.R
            return True
        except Exception:
            return False

    @property
    def summary(self) -> dict:
        return {
            "alpha": round(self.state.alpha, 4),
            "beta": round(self.state.beta, 4),
            "P00": round(self.state.P[0][0], 6),
            "P11": round(self.state.P[1][1], 6),
            "tick_count": self.state.tick_count,
            "last_ts": self.state.last_ts,
            "warmed_up": self.state.warmed_up,
        }

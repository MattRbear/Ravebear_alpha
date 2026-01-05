#!/usr/bin/env python3
"""
VWAP Feature Computation
=========================
Computes Volume Weighted Average Price features.
"""

import math
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional
from collections import defaultdict

from feeds.okx_trades import Trade


@dataclass
class VWAPAccumulator:
    """Accumulator for VWAP calculation."""
    sum_pv: float = 0.0  # Sum of price * volume
    sum_v: float = 0.0   # Sum of volume
    sum_pv2: float = 0.0  # Sum of price^2 * volume (for variance)
    
    def add(self, price: float, volume: float) -> None:
        """Add a trade to the accumulator."""
        self.sum_pv += price * volume
        self.sum_v += volume
        self.sum_pv2 += (price ** 2) * volume
    
    @property
    def vwap(self) -> float:
        """Calculate VWAP."""
        if self.sum_v <= 0:
            return 0.0
        return self.sum_pv / self.sum_v
    
    @property
    def variance(self) -> float:
        """Calculate variance of prices weighted by volume."""
        if self.sum_v <= 0:
            return 0.0
        mean = self.vwap
        # E[X^2] - E[X]^2
        e_x2 = self.sum_pv2 / self.sum_v
        return max(0.0, e_x2 - mean ** 2)
    
    @property
    def stdev(self) -> float:
        """Calculate standard deviation."""
        return math.sqrt(self.variance)


@dataclass
class VWAPState:
    """Per-symbol VWAP state."""
    global_acc: VWAPAccumulator = field(default_factory=VWAPAccumulator)
    session_accs: Dict[str, VWAPAccumulator] = field(default_factory=dict)
    price_history: List[float] = field(default_factory=list)


# Global state dictionary keyed by symbol
STATE: Dict[str, VWAPState] = defaultdict(VWAPState)


def compute_vwap_features(
    trades: List[Trade],
    now: datetime,
    symbol: str,
    session_label: str,
    last_price: float
) -> Dict:
    """
    Compute VWAP features for a symbol.
    
    Args:
        trades: List of recent trades
        now: Current timestamp
        symbol: Trading pair symbol
        session_label: Current trading session (asia/london/ny)
        last_price: Current/last price
    
    Returns:
        Dict of VWAP features
    """
    state = STATE[symbol]
    
    # Add trades to accumulators
    for trade in trades:
        state.global_acc.add(trade.price, trade.size)
        
        if session_label not in state.session_accs:
            state.session_accs[session_label] = VWAPAccumulator()
        state.session_accs[session_label].add(trade.price, trade.size)
        
        state.price_history.append(trade.price)
    
    # Keep history bounded
    max_history = 10000
    if len(state.price_history) > max_history:
        state.price_history = state.price_history[-max_history:]
    
    # Calculate distances
    global_vwap = state.global_acc.vwap
    session_acc = state.session_accs.get(session_label)
    session_vwap = session_acc.vwap if session_acc else global_vwap
    
    # Distance as percentage
    if global_vwap > 0:
        global_vwap_distance = (last_price - global_vwap) / global_vwap
    else:
        global_vwap_distance = 0.0
    
    if session_vwap > 0:
        session_vwap_distance = (last_price - session_vwap) / session_vwap
    else:
        session_vwap_distance = 0.0
    
    # Calculate standard deviation bands
    stdev = state.global_acc.stdev
    
    # Band flags
    vwap_band_flag_1sd = False
    vwap_band_flag_2sd = False
    
    if stdev > 0:
        z_score = (last_price - global_vwap) / stdev
        vwap_band_flag_1sd = abs(z_score) >= 1.0
        vwap_band_flag_2sd = abs(z_score) >= 2.0
    
    # Mean reversion score
    # Scale: 0-100 based on z-score magnitude
    # Sign indicates direction: negative = price above VWAP, positive = price below VWAP
    # 3 sigma deviation = 100 score
    if stdev > 0:
        z_score = (last_price - global_vwap) / stdev
        # Normalize: |z| / 3 * 100, capped at 100
        magnitude = min(100.0, abs(z_score) / 3.0 * 100.0)
        # Sign: negative if price > VWAP (mean reversion would push down)
        # Positive if price < VWAP (mean reversion would push up)
        vwap_mean_reversion_score = magnitude if z_score < 0 else -magnitude
    else:
        vwap_mean_reversion_score = 0.0
    
    return {
        "session_vwap_distance": round(session_vwap_distance, 6),
        "global_vwap_distance": round(global_vwap_distance, 6),
        "vwap_band_flag_1sd": vwap_band_flag_1sd,
        "vwap_band_flag_2sd": vwap_band_flag_2sd,
        "vwap_mean_reversion_score": round(vwap_mean_reversion_score, 4),
    }

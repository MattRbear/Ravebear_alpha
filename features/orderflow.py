#!/usr/bin/env python3
"""
Order Flow Feature Computation
==============================
Computes order flow features including CVD, delta, and trade frequency.
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional
from collections import deque
import statistics

from feeds.okx_trades import Trade
from utils.aggregation import Candle


@dataclass
class OrderFlowState:
    """Per-symbol order flow state."""
    cvd: float = 0.0
    cvd_history: List[float] = field(default_factory=list)
    trade_count_history: List[int] = field(default_factory=list)
    delta_history: List[float] = field(default_factory=list)


# Global state dictionary keyed by symbol
STATE: Dict[str, OrderFlowState] = {}


def _get_state(symbol: str) -> OrderFlowState:
    """Get or create state for a symbol."""
    if symbol not in STATE:
        STATE[symbol] = OrderFlowState()
    return STATE[symbol]


def reset_state(symbol: Optional[str] = None) -> None:
    """Reset state for a symbol or all symbols."""
    global STATE
    if symbol is None:
        STATE.clear()
    elif symbol in STATE:
        del STATE[symbol]


def compute_orderflow_features(candle: Candle, trades: List[Trade]) -> Dict:
    """
    Compute order flow features for a candle and its trades.
    
    Args:
        candle: The aggregated candle
        trades: List of trades in the candle period
    
    Returns:
        Dict of order flow features
    """
    symbol = candle.symbol
    state = _get_state(symbol)
    
    # Compute delta (buy - sell volume)
    buy_volume = sum(t.size for t in trades if t.side == "buy")
    sell_volume = sum(t.size for t in trades if t.side == "sell")
    delta = buy_volume - sell_volume
    
    # Update CVD (cumulative volume delta)
    state.cvd += delta
    state.cvd_history.append(state.cvd)
    state.delta_history.append(delta)
    state.trade_count_history.append(len(trades))
    
    # Keep history bounded (last 100 candles)
    max_history = 100
    if len(state.cvd_history) > max_history:
        state.cvd_history = state.cvd_history[-max_history:]
    if len(state.delta_history) > max_history:
        state.delta_history = state.delta_history[-max_history:]
    if len(state.trade_count_history) > max_history:
        state.trade_count_history = state.trade_count_history[-max_history:]
    
    # CVD Slope (over last 10 candles)
    cvd_slope_10 = 0.0
    if len(state.cvd_history) >= 2:
        recent = state.cvd_history[-10:] if len(state.cvd_history) >= 10 else state.cvd_history
        if len(recent) >= 2:
            # Simple linear regression slope
            n = len(recent)
            x_mean = (n - 1) / 2
            y_mean = sum(recent) / n
            numerator = sum((i - x_mean) * (y - y_mean) for i, y in enumerate(recent))
            denominator = sum((i - x_mean) ** 2 for i in range(n))
            if denominator > 0:
                cvd_slope_10 = numerator / denominator
    
    # Delta at wick (current delta)
    delta_at_wick = delta
    
    # Delta previous pivot (previous candle delta)
    delta_prev_pivot = 0.0
    if len(state.delta_history) >= 2:
        delta_prev_pivot = state.delta_history[-2]
    
    # Delta divergence: price direction doesn't match delta direction
    price_change = candle.close - candle.open
    delta_divergence_flag = (
        (price_change > 0 and delta < 0) or
        (price_change < 0 and delta > 0)
    )
    
    # Absorption detection: high volume with small price change
    total_volume = buy_volume + sell_volume
    price_range = candle.high - candle.low
    absorption_flag = False
    if total_volume > 0 and price_range > 0:
        volume_to_range_ratio = total_volume / price_range
        # Flag if volume is high relative to price movement
        if len(state.trade_count_history) >= 20:
            avg_volume = sum(state.trade_count_history[-20:]) / 20
            if len(trades) > avg_volume * 2 and abs(price_change) / price_range < 0.3:
                absorption_flag = True
    
    # Exhaustion detection: declining delta with same price direction
    exhaustion_flag = False
    if len(state.delta_history) >= 3:
        recent_deltas = state.delta_history[-3:]
        # Check if deltas are declining in magnitude
        if all(recent_deltas[i] * price_change > 0 for i in range(len(recent_deltas))):
            # Same direction as price
            if abs(recent_deltas[-1]) < abs(recent_deltas[-2]) < abs(recent_deltas[-3]):
                exhaustion_flag = True
    
    # Trade frequency spike (z-score)
    trade_frequency_spike = 0.0
    if len(state.trade_count_history) >= 20:
        recent_counts = state.trade_count_history[-20:]
        mean = statistics.mean(recent_counts)
        stdev = statistics.stdev(recent_counts) if len(recent_counts) > 1 else 1.0
        if stdev > 0:
            trade_frequency_spike = (len(trades) - mean) / stdev
    
    # Bid/ask refresh rate (simplified: based on trade frequency)
    bid_ask_refresh_rate = len(trades) / 60.0 if len(trades) > 0 else 0.0
    
    # Iceberg detection: many trades at same price with consistent size
    iceberg_flag = False
    if len(trades) >= 5:
        # Group trades by price
        price_counts: Dict[float, int] = {}
        for t in trades:
            price_counts[t.price] = price_counts.get(t.price, 0) + 1
        # If any price has many trades, might be iceberg
        max_trades_at_price = max(price_counts.values()) if price_counts else 0
        if max_trades_at_price >= 5:
            iceberg_flag = True
    
    return {
        "delta_at_wick": round(delta_at_wick, 6),
        "delta_prev_pivot": round(delta_prev_pivot, 6),
        "delta_divergence_flag": delta_divergence_flag,
        "cvd_slope_10": round(cvd_slope_10, 6),
        "absorption_flag": absorption_flag,
        "exhaustion_flag": exhaustion_flag,
        "trade_frequency_spike": round(trade_frequency_spike, 4),
        "bid_ask_refresh_rate": round(bid_ask_refresh_rate, 4),
        "iceberg_flag": iceberg_flag,
    }

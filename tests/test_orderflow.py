# file: tests/test_orderflow.py
"""
Tests for orderflow feature computation.
Includes verification of per-symbol state isolation.
"""
import pytest
from datetime import datetime, timezone
from typing import List
from features import orderflow
from feeds.okx_trades import Trade
from utils.aggregation import Candle


def create_candle(symbol: str = "BTC-USDT") -> Candle:
    """Helper to create test candles."""
    return Candle(
        start_ts=datetime.now(timezone.utc),
        end_ts=datetime.now(timezone.utc),
        symbol=symbol,
        open=100.0,
        high=100.0,
        low=100.0,
        close=100.0,
        volume=0.0, 
        buy_volume=0.0,
        sell_volume=0.0,
        trades=[]
    )


def test_cvd_accumulates():
    """Test that CVD accumulates correctly for a symbol."""
    orderflow.reset_state()
    
    trades = [
        Trade(datetime.now(timezone.utc), "BTC-USDT", 100.0, 1.0, "buy"),
        Trade(datetime.now(timezone.utc), "BTC-USDT", 100.0, 2.0, "buy"), 
        Trade(datetime.now(timezone.utc), "BTC-USDT", 100.0, 1.0, "sell")
    ]  # net +2
    
    c = create_candle("BTC-USDT")
    orderflow.compute_orderflow_features(c, trades)
    
    state = orderflow._get_state("BTC-USDT")
    assert state.cvd == 2.0
    
    trades2 = [Trade(datetime.now(timezone.utc), "BTC-USDT", 100.0, 1.0, "sell")]  # net -1
    orderflow.compute_orderflow_features(c, trades2)
    assert state.cvd == 1.0


def test_cvd_slope_positive():
    """Test that CVD slope is positive when CVD is increasing."""
    orderflow.reset_state()
    c = create_candle("BTC-USDT")
    
    # 5 iterations of +1 CVD each time
    trades = [Trade(datetime.now(timezone.utc), "BTC-USDT", 100.0, 1.0, "buy")]
    
    res = {}
    for _ in range(5):
        res = orderflow.compute_orderflow_features(c, trades)
        
    # CVD history: 1, 2, 3, 4, 5
    # Slope should be positive
    assert res["cvd_slope_10"] > 0.0


def test_trade_frequency_spike():
    """Test that trade frequency spike detects unusual volume."""
    orderflow.reset_state()
    c = create_candle("BTC-USDT")
    
    # Fill history with low trade counts
    low_trades = [Trade(datetime.now(timezone.utc), "BTC-USDT", 100.0, 1.0, "buy")]
    for _ in range(25):
        orderflow.compute_orderflow_features(c, low_trades)
        
    # High trade count
    high_trades = [Trade(datetime.now(timezone.utc), "BTC-USDT", 100.0, 1.0, "buy")] * 100
    res = orderflow.compute_orderflow_features(c, high_trades)
    
    # Should be positive spike
    assert res["trade_frequency_spike"] > 0.0


def test_trade_frequency_spike_zero_history():
    """Test that spike is 0 when not enough history."""
    orderflow.reset_state()
    c = create_candle("BTC-USDT")
    trades = [Trade(datetime.now(timezone.utc), "BTC-USDT", 100.0, 1.0, "buy")]
    
    res = orderflow.compute_orderflow_features(c, trades)
    assert res["trade_frequency_spike"] == 0.0


def test_symbol_isolation():
    """
    CRITICAL TEST: Verify that CVD for BTC does not affect ETH.
    
    This was bug #24 - global STATE mixed all symbols together.
    """
    orderflow.reset_state()
    
    # Process BTC trades: +10 CVD
    btc_candle = create_candle("BTC-USDT")
    btc_trades = [Trade(datetime.now(timezone.utc), "BTC-USDT", 100.0, 10.0, "buy")]
    orderflow.compute_orderflow_features(btc_candle, btc_trades)
    
    # Process ETH trades: +5 CVD
    eth_candle = create_candle("ETH-USDT")
    eth_trades = [Trade(datetime.now(timezone.utc), "ETH-USDT", 100.0, 5.0, "buy")]
    orderflow.compute_orderflow_features(eth_candle, eth_trades)
    
    # Process SOL trades: -3 CVD
    sol_candle = create_candle("SOL-USDT")
    sol_trades = [Trade(datetime.now(timezone.utc), "SOL-USDT", 100.0, 3.0, "sell")]
    orderflow.compute_orderflow_features(sol_candle, sol_trades)
    
    # Verify each symbol has INDEPENDENT state
    btc_state = orderflow._get_state("BTC-USDT")
    eth_state = orderflow._get_state("ETH-USDT")
    sol_state = orderflow._get_state("SOL-USDT")
    
    assert btc_state.cvd == 10.0, f"BTC CVD should be 10.0, got {btc_state.cvd}"
    assert eth_state.cvd == 5.0, f"ETH CVD should be 5.0, got {eth_state.cvd}"
    assert sol_state.cvd == -3.0, f"SOL CVD should be -3.0, got {sol_state.cvd}"
    
    # Verify they are different objects
    assert btc_state is not eth_state
    assert eth_state is not sol_state


def test_symbol_isolation_history():
    """Verify CVD history is also isolated per symbol."""
    orderflow.reset_state()
    
    # Build up BTC history
    btc_candle = create_candle("BTC-USDT")
    for i in range(5):
        trades = [Trade(datetime.now(timezone.utc), "BTC-USDT", 100.0, 1.0, "buy")]
        orderflow.compute_orderflow_features(btc_candle, trades)
    
    # ETH has no history yet
    eth_candle = create_candle("ETH-USDT")
    eth_trades = [Trade(datetime.now(timezone.utc), "ETH-USDT", 100.0, 1.0, "buy")]
    result = orderflow.compute_orderflow_features(eth_candle, eth_trades)
    
    # ETH should have slope 0 (only 1 data point)
    # BTC should have positive slope (5 increasing points)
    btc_state = orderflow._get_state("BTC-USDT")
    eth_state = orderflow._get_state("ETH-USDT")
    
    assert len(btc_state.cvd_history) == 5
    assert len(eth_state.cvd_history) == 1
    
    # ETH cvd_slope should be 0 (not enough history)
    assert result["cvd_slope_10"] == 0.0


def test_reset_single_symbol():
    """Test that reset_state can clear a single symbol."""
    orderflow.reset_state()
    
    # Add data for both symbols
    btc_candle = create_candle("BTC-USDT")
    eth_candle = create_candle("ETH-USDT")
    trades = [Trade(datetime.now(timezone.utc), "X", 100.0, 1.0, "buy")]
    
    orderflow.compute_orderflow_features(btc_candle, trades)
    orderflow.compute_orderflow_features(eth_candle, trades)
    
    # Reset only BTC
    orderflow.reset_state("BTC-USDT")
    
    # BTC should be fresh, ETH should persist
    assert "BTC-USDT" not in orderflow.STATE
    assert "ETH-USDT" in orderflow.STATE
    assert orderflow._get_state("ETH-USDT").cvd == 1.0

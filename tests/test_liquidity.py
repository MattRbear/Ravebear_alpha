# file: tests/test_liquidity.py
import pytest
from datetime import datetime
from features import liquidity
from feeds.okx_orderbook import OrderBookSnapshot

def create_book(bids, asks):
    return OrderBookSnapshot(
        ts=datetime.now(),
        symbol="BTC",
        best_bid=bids[0][0],
        best_ask=asks[0][0],
        bids=bids,
        asks=asks
    )

def test_l1_l5_depths_spread():
    bids = [(100.0, 1.0), (99.0, 1.0)]
    asks = [(102.0, 2.0), (103.0, 2.0)]
    bk = create_book(bids, asks)
    
    res = liquidity.compute_liquidity_features(bk)
    assert res["spread"] == 2.0 # 102 - 100
    assert res["l1_depth_bid"] == 1.0
    assert res["l1_depth_ask"] == 2.0
    assert res["l5_depth_bid"] == 2.0
    assert res["l5_depth_ask"] == 4.0

def test_depth_imbalance():
    # Symmetric
    bids = [(100.0, 10.0)]
    asks = [(101.0, 10.0)]
    bk = create_book(bids, asks)
    assert liquidity.compute_liquidity_features(bk)["depth_imbalance"] == 0.0
    
    # Asymmetric
    bids2 = [(100.0, 30.0)] # total 30
    asks2 = [(101.0, 10.0)] # total 10
    bk2 = create_book(bids2, asks2)
    # (30 - 10) / 40 = 0.5
    assert liquidity.compute_liquidity_features(bk2)["depth_imbalance"] == 0.5

def test_void_flag():
    # Regular gaps
    bids = [(100.0, 1.0), (99.0, 1.0)] # Gap 1
    # Ask has huge gap
    # 101, then 120
    asks = [(101.0, 1.0), (120.0, 1.0)] # Gap 19
    # Min gap = 1, Max gap = 19. 19 >= 5*1.
    bk = create_book(bids, asks)
    assert liquidity.compute_liquidity_features(bk)["liquidity_void_flag"] is True

def test_stacked_imbalance():
    # 3x ratio
    bids = [(100.0, 30.0)]
    asks = [(101.0, 10.0)]
    bk = create_book(bids, asks)
    assert liquidity.compute_liquidity_features(bk)["stacked_imbalance_nearby"] is True
    
    # < 3 bit > 1/3
    asks2 = [(101.0, 15.0)] # 30/15 = 2.0
    bk2 = create_book(bids, asks2)
    assert liquidity.compute_liquidity_features(bk2)["stacked_imbalance_nearby"] is False

def test_no_orderbook():
    res = liquidity.compute_liquidity_features(None)
    assert res["spread"] == 0.0
    assert res["liquidity_void_flag"] is False

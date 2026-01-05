# file: tests/test_derivatives.py
import pytest
from datetime import datetime, timedelta
from features.derivatives import (
    STATE,
    register_oi_snapshot,
    register_funding_snapshot,
    register_liquidation_event,
    compute_derivatives_features
)
from feeds.coinalyze_derivs import OIDeltaSnapshot, FundingSnapshot, LiquidationEvent

def reset_state():
    STATE.clear()

def test_oi_change_basic():
    reset_state()
    symbol = "BTC"
    base = datetime.now()
    
    # 1. 100 -> 110 (+10%)
    s1 = OIDeltaSnapshot(base, symbol, 100.0, 100.0, 0.0)
    s2 = OIDeltaSnapshot(base + timedelta(seconds=60), symbol, 100.0, 110.0, 10.0)
    register_oi_snapshot(s1)
    register_oi_snapshot(s2)
    
    feats = compute_derivatives_features(symbol, base + timedelta(seconds=60))
    # Start=100 (from s1 open), End=110 (from s2 close). Actually implementation sorts by ts.
    # Start snapshot s1 (ts=base), End snapshot s2 (ts=base+60).
    # s1.oi_open = 100. s2.oi_close = 110. ((110 - 100) / 100) = 0.1
    assert feats["oi_change_pct"] == pytest.approx(0.1)
    assert feats["oi_direction"] == "inc"

def test_liquidation_density_flag():
    reset_state()
    symbol = "BTC"
    base = datetime.now()
    # 2 events sum to > 1.0
    l1 = LiquidationEvent(base, symbol, "long", 0.6, 100.0)
    l2 = LiquidationEvent(base, symbol, "short", 0.5, 99.0)
    register_liquidation_event(l1)
    register_liquidation_event(l2)
    
    feats = compute_derivatives_features(symbol, base)
    assert feats["liquidation_density"] > 0
    assert feats["oi_liquidation_flag"] is True

def test_funding_distance():
    reset_state()
    symbol = "BTC"
    base = datetime.now()
    next_ts = base + timedelta(minutes=60)
    
    snap = FundingSnapshot(base, symbol, 0.01, 0.01, next_ts)
    register_funding_snapshot(snap)
    
    feats = compute_derivatives_features(symbol, base)
    # 60 mins away
    assert feats["funding_distance_to_timestamp"] == pytest.approx(60.0, abs=0.1)

def test_defaults():
    reset_state()
    feats = compute_derivatives_features("BTC", datetime.now())
    assert feats["oi_change_pct"] == 0.0
    assert feats["oi_direction"] == "inc"
    assert feats["liquidation_density"] == 0.0

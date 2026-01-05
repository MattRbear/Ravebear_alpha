# file: tests/test_vwap.py
import pytest
from datetime import datetime
from features import vwap
from feeds.okx_trades import Trade

def test_vwap_constant_price():
    vwap.STATE.clear()
    trades = [
        Trade(datetime.now(), "BTC", 100.0, 1.0, "buy"),
        Trade(datetime.now(), "BTC", 100.0, 2.0, "sell")
    ]
    # Price always 100
    res = vwap.compute_vwap_features(trades, datetime.now(), "BTC", "sess", 100.0)
    
    # No deviation
    assert res["session_vwap_distance"] == 0.0
    assert res["global_vwap_distance"] == 0.0
    assert res["vwap_band_flag_1sd"] is False

def test_vwap_distances():
    vwap.STATE.clear()
    # 1 trade at 100, size 10
    trades = [Trade(datetime.now(), "BTC", 100.0, 10.0, "buy")]
    vwap.compute_vwap_features(trades, datetime.now(), "BTC", "sess", 100.0)
    
    # VWAP is 100.
    # Next call, empty trades, last_price=105.
    res = vwap.compute_vwap_features([], datetime.now(), "BTC", "sess", 105.0)
    
    # Dist = (105 - 100)/100 = 0.05
    assert abs(res["session_vwap_distance"] - 0.05) < 1e-9

def test_reversion_score():
    vwap.STATE.clear()
    # Create variance: 1 trade at 90, 1 at 110. VWAP=100. Sigma=10.
    trades = [
        Trade(datetime.now(), "BTC", 90.0, 1.0, "buy"),
        Trade(datetime.now(), "BTC", 110.0, 1.0, "buy")
    ]
    vwap.compute_vwap_features(trades, datetime.now(), "BTC", "sess", 110.0)
    
    # Price=120. Z = (120-100)/10 = 2.0.
    # Score magnitude = 2.0/3.0 * 100 = 66.67
    # Price > VWAP so score is negative (mean reversion would push down)
    res = vwap.compute_vwap_features([], datetime.now(), "BTC", "sess", 120.0)
    assert -70 < res["vwap_mean_reversion_score"] < -60, f"Expected score around -66.7, got {res['vwap_mean_reversion_score']}"
    
    # Price=80. Z = (80-100)/10 = -2.0.
    # Score magnitude = 2.0/3.0 * 100 = 66.67
    # Price < VWAP so score is positive (mean reversion would push up)
    res2 = vwap.compute_vwap_features([], datetime.now(), "BTC", "sess", 80.0)
    assert 60 < res2["vwap_mean_reversion_score"] < 70, f"Expected score around 66.7, got {res2['vwap_mean_reversion_score']}"

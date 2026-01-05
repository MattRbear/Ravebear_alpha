# tests/test_session.py
import pytest
from datetime import datetime, timezone
from features.session import compute_session_features

def test_asia_session():
    """Test Asia session detection (00:00-08:00 UTC)"""
    ts = datetime(2025, 12, 27, 3, 30, 0, tzinfo=timezone.utc)  # 03:30 UTC
    result = compute_session_features(ts)
    
    assert result["session_label"] == "asia"
    assert result["hour_of_day"] == 3
    assert result["minutes_into_session"] == 3 * 60 + 30  # 210 minutes
    assert result["minutes_until_session_close"] == 480 - 210 - 1  # 269

def test_london_session():
    """Test London session detection (08:00-16:00 UTC)"""
    ts = datetime(2025, 12, 27, 12, 0, 0, tzinfo=timezone.utc)  # 12:00 UTC
    result = compute_session_features(ts)
    
    assert result["session_label"] == "london"
    assert result["hour_of_day"] == 12
    assert result["minutes_into_session"] == 4 * 60  # 240 minutes

def test_ny_session():
    """Test NY session detection (16:00-24:00 UTC)"""
    ts = datetime(2025, 12, 27, 20, 15, 0, tzinfo=timezone.utc)  # 20:15 UTC
    result = compute_session_features(ts)
    
    assert result["session_label"] == "ny"
    assert result["hour_of_day"] == 20
    assert result["minutes_into_session"] == 4 * 60 + 15  # 255 minutes

def test_weekend_flag():
    """Test weekend detection"""
    # Saturday
    ts_sat = datetime(2025, 12, 27, 12, 0, 0, tzinfo=timezone.utc)
    result = compute_session_features(ts_sat)
    assert result["weekend_flag"] == True
    assert result["day_of_week"] == 5  # Saturday
    
    # Wednesday
    ts_wed = datetime(2025, 12, 24, 12, 0, 0, tzinfo=timezone.utc)
    result = compute_session_features(ts_wed)
    assert result["weekend_flag"] == False
    assert result["day_of_week"] == 2  # Wednesday

def test_cme_proximity_friday_before():
    """Test CME proximity before Friday close"""
    # Friday 18:00 UTC = 3 hours before 21:00
    ts = datetime(2025, 12, 26, 18, 0, 0, tzinfo=timezone.utc)
    result = compute_session_features(ts)
    assert result["cme_close_proximity"] == 3 * 60  # 180 minutes

def test_cme_proximity_friday_after():
    """Test CME proximity after Friday close"""
    # Friday 22:00 UTC = past 21:00
    ts = datetime(2025, 12, 26, 22, 0, 0, tzinfo=timezone.utc)
    result = compute_session_features(ts)
    assert result["cme_close_proximity"] == 0.0

def test_cme_proximity_weekend():
    """Test CME proximity on weekend"""
    # Saturday
    ts = datetime(2025, 12, 27, 12, 0, 0, tzinfo=timezone.utc)
    result = compute_session_features(ts)
    assert result["cme_close_proximity"] == 0.0

def test_naive_datetime_handling():
    """Test that naive datetime gets handled"""
    ts = datetime(2025, 12, 27, 12, 0, 0)  # No timezone
    result = compute_session_features(ts)
    assert result["session_label"] == "london"  # Should still work

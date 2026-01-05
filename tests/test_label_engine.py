# tests/test_label_engine.py
import pytest
from datetime import datetime, timedelta, timezone
from scripts.label_engine import (
    CandleBar, CandleSeries, compute_labels_for_event, 
    LOOKAHEAD_4H, TOUCH_BPS
)

# Helper to make bars
def make_bars(start_ts, count, price=100.0) -> list[CandleBar]:
    bars = []
    for i in range(count):
        t = start_ts + timedelta(minutes=i)
        bars.append(CandleBar(
            start_ts=t,
            end_ts=t + timedelta(minutes=1),
            open=price, high=price, low=price, close=price, volume=100
        ))
    return bars

def test_compute_labels_simple_upper_untouched():
    # Wick at 100. Upper wick. Future highs at 99.0.
    start = datetime(2025, 1, 1, 10, 0, tzinfo=timezone.utc)
    series = CandleSeries("BTC", make_bars(start, LOOKAHEAD_4H, price=99.0))
    
    labels = compute_labels_for_event(
        {"event_ts": start.isoformat()}, 
        100.0, 
        "upper", 
        series
    )
    
    assert labels["untouched_30m"] is True
    assert labels["untouched_1h"] is True
    assert labels["untouched_4h"] is True
    assert labels["hold_duration"] == 240.0
    assert labels["mae"] == 0.0 # No adverse move
    # MFE: wick(100) - low(99) = 1.0. 1/100 = 0.01
    assert labels["mfe"] == pytest.approx(0.01)

def test_compute_labels_upper_touched_immediately():
    start = datetime(2025, 1, 1, 10, 0, tzinfo=timezone.utc)
    bars = make_bars(start, LOOKAHEAD_4H, price=99.0)
    # Be touched at minute 5 (index 5, but enumerate starts at 1, so index 4 in list)
    # index 4 is 5th bar. 10:00, 10:01, 10:02, 10:03, 10:04.
    # If 10:04 bar hits the level.
    bars[4].high = 100.0 
    
    series = CandleSeries("BTC", bars)
    
    labels = compute_labels_for_event(
        {"event_ts": start.isoformat()},
        100.0,
        "upper",
        series
    )
    
    assert labels["untouched_30m"] is False
    assert labels["hold_duration"] == 5.0
    
    # MAE check: High 100.0 matches entry 100.0 -> MAE 0?
    # Actually if high goes to 101:
    bars[4].high = 101.0
    labels = compute_labels_for_event({"event_ts": start.isoformat()}, 100.0, "upper", series)
    # MAE = (101 - 100)/100 = 0.01
    assert labels["mae"] == 0.01

def test_compute_labels_lower_wick():
    # Lower wick at 100. Market rallies to 110.
    start = datetime(2025, 1, 1, 10, 0, tzinfo=timezone.utc)
    bars = make_bars(start, LOOKAHEAD_4H, price=105.0)
    bars[-1].close = 110.0 # Close at end
    
    series = CandleSeries("BTC", bars)
    labels = compute_labels_for_event({"event_ts": start.isoformat()}, 100.0, "lower", series)
    
    assert labels["untouched_4h"] is True
    # MFE = (110 - 100)/100 = 0.1? No, max high is 105 (except last close? wait, bars have high/low)
    # The helper `make_bars` sets OHLC to `price`.
    # So highs are 105. Last bar close 110? Update last bar high too.
    # Wait, `bars[-1].close = 110`. Its high is still 105 in `make_bars`.
    # Let's clean up logic.
    bars[-1].high = 110.0
    
    labels = compute_labels_for_event({"event_ts": start.isoformat()}, 100.0, "lower", series)
    
    # MFE = (110 - 100)/100 = 0.1
    assert labels["mfe"] == pytest.approx(0.1)
    # Dist moved = (110 - 100)/100 = 0.1
    assert labels["distance_moved"] == pytest.approx(0.1)

def test_insufficient_data():
    start = datetime(2025, 1, 1, 10, 0, tzinfo=timezone.utc)
    # Only 100 bars
    bars = make_bars(start, 100, price=100.0)
    series = CandleSeries("BTC", bars)
    
    labels = compute_labels_for_event({"event_ts": start.isoformat()}, 100.0, "upper", series)
    assert labels == {}

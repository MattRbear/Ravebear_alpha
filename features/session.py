#!/usr/bin/env python3
"""
Session Feature Computation
============================
Computes trading session and timing features.
"""

from datetime import datetime, timezone, timedelta
from typing import Dict


# Trading session definitions (UTC hours)
SESSIONS = {
    "asia": (0, 8),      # 00:00-08:00 UTC
    "london": (8, 16),   # 08:00-16:00 UTC
    "ny": (16, 24),      # 16:00-24:00 UTC
}

# CME Bitcoin futures close time on Friday (21:00 UTC)
CME_CLOSE_HOUR = 21


def compute_session_features(ts: datetime) -> Dict:
    """
    Compute session and timing features for a given timestamp.
    
    Args:
        ts: Timestamp (datetime, will be converted to UTC if naive)
    
    Returns:
        Dict of session features
    """
    # Ensure UTC timezone
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    else:
        ts = ts.astimezone(timezone.utc)
    
    hour = ts.hour
    minute = ts.minute
    day_of_week = ts.weekday()  # 0=Monday, 6=Sunday
    
    # Determine session
    session_label = "unknown"
    minutes_into_session = 0
    minutes_until_session_close = 0
    
    for session_name, (start_hour, end_hour) in SESSIONS.items():
        if start_hour <= hour < end_hour:
            session_label = session_name
            
            # Calculate minutes into session
            minutes_into_session = (hour - start_hour) * 60 + minute
            
            # Calculate minutes until session close
            session_duration = (end_hour - start_hour) * 60
            minutes_until_session_close = session_duration - minutes_into_session - 1
            break
    
    # Weekend flag (Saturday=5, Sunday=6)
    weekend_flag = day_of_week >= 5
    
    # CME close proximity (Friday only, before close)
    cme_close_proximity = 0.0
    if day_of_week == 4:  # Friday
        if hour < CME_CLOSE_HOUR:
            # Minutes until CME close
            cme_close_proximity = (CME_CLOSE_HOUR - hour) * 60 - minute
        # After CME close or on other days, proximity is 0
    
    return {
        "session_label": session_label,
        "minutes_into_session": minutes_into_session,
        "minutes_until_session_close": minutes_until_session_close,
        "hour_of_day": hour,
        "day_of_week": day_of_week,
        "weekend_flag": weekend_flag,
        "cme_close_proximity": cme_close_proximity,
    }

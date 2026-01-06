#!/usr/bin/env python3
"""
Derivatives Feature Computation
================================
Computes derivatives features from OI, funding, and liquidation data.
"""

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List

from feeds.coinalyze_derivs import (
    OIDeltaSnapshot,
    FundingSnapshot,
    LiquidationEvent
)


@dataclass
class DerivativesState:
    """Per-symbol derivatives state."""
    oi_snapshots: List[OIDeltaSnapshot] = field(default_factory=list)
    funding_snapshots: List[FundingSnapshot] = field(default_factory=list)
    liquidation_events: List[LiquidationEvent] = field(default_factory=list)


# Global state dictionary keyed by symbol
STATE: Dict[str, DerivativesState] = {}


def _get_state(symbol: str) -> DerivativesState:
    """Get or create state for a symbol."""
    if symbol not in STATE:
        STATE[symbol] = DerivativesState()
    return STATE[symbol]


def register_oi_snapshot(snapshot: OIDeltaSnapshot) -> None:
    """Register an OI snapshot."""
    state = _get_state(snapshot.symbol)
    state.oi_snapshots.append(snapshot)

    # Keep last 100 snapshots
    if len(state.oi_snapshots) > 100:
        state.oi_snapshots = state.oi_snapshots[-100:]


def register_funding_snapshot(snapshot: FundingSnapshot) -> None:
    """Register a funding snapshot."""
    state = _get_state(snapshot.symbol)
    state.funding_snapshots.append(snapshot)

    # Keep last 100 snapshots
    if len(state.funding_snapshots) > 100:
        state.funding_snapshots = state.funding_snapshots[-100:]


def register_liquidation_event(event: LiquidationEvent) -> None:
    """Register a liquidation event."""
    state = _get_state(event.symbol)
    state.liquidation_events.append(event)

    # Keep last 1000 events
    if len(state.liquidation_events) > 1000:
        state.liquidation_events = state.liquidation_events[-1000:]


def compute_derivatives_features(
    symbol: str,
    wick_ts: datetime,
    lookback_minutes: int = 15
) -> Dict:
    """
    Compute derivatives features for a symbol at a given timestamp.
    
    Args:
        symbol: Trading pair symbol
        wick_ts: Timestamp of the wick event
        lookback_minutes: How far back to look for data
    
    Returns:
        Dict of derivatives features
    """
    state = _get_state(symbol)

    # Default values
    result = {
        "oi_change_pct": 0.0,
        "oi_direction": "inc",
        "oi_liquidation_flag": False,
        "liquidation_density": 0.0,
        "funding_rate_now": 0.0,
        "funding_rate_next": 0.0,
        "funding_distance_to_timestamp": 0.0,
    }

    # Calculate OI change
    if len(state.oi_snapshots) >= 2:
        # Sort by timestamp
        sorted_oi = sorted(state.oi_snapshots, key=lambda x: x.ts)

        # Get first and last in the lookback window
        cutoff = wick_ts - timedelta(minutes=lookback_minutes)
        relevant = [s for s in sorted_oi if s.ts >= cutoff]

        if len(relevant) >= 2:
            start_oi = relevant[0].oi_open
            end_oi = relevant[-1].oi_close

            if start_oi > 0:
                result["oi_change_pct"] = (end_oi - start_oi) / start_oi
                result["oi_direction"] = "inc" if end_oi > start_oi else "dec"
        elif len(relevant) == 1:
            snap = relevant[0]
            if snap.oi_open > 0:
                oi_change = (snap.oi_close - snap.oi_open) / snap.oi_open
                result["oi_change_pct"] = oi_change
                oi_dir = "inc" if snap.oi_close > snap.oi_open else "dec"
                result["oi_direction"] = oi_dir

    # Calculate liquidation density
    if state.liquidation_events:
        cutoff = wick_ts - timedelta(minutes=lookback_minutes)
        recent_liqs = [e for e in state.liquidation_events if e.ts >= cutoff]

        total_liq_volume = sum(e.volume for e in recent_liqs)
        result["liquidation_density"] = total_liq_volume

        # Flag if significant liquidations occurred
        result["oi_liquidation_flag"] = total_liq_volume > 1.0

    # Get latest funding snapshot
    if state.funding_snapshots:
        # Get most recent
        latest_funding = max(state.funding_snapshots, key=lambda x: x.ts)

        result["funding_rate_now"] = latest_funding.funding_rate_now
        result["funding_rate_next"] = latest_funding.funding_rate_next

        # Calculate distance to next funding timestamp
        if latest_funding.next_funding_ts > wick_ts:
            delta = latest_funding.next_funding_ts - wick_ts
            result["funding_distance_to_timestamp"] = delta.total_seconds() / 60.0

    return result

#!/usr/bin/env python3
"""
Wick Geometry Feature Computation
=================================
Computes geometric features of price wicks.
"""

from typing import Dict, Literal
from utils.aggregation import Candle


def compute_wick_geometry(candle: Candle, wick_side: Literal["upper", "lower"]) -> Dict:
    """
    Compute geometric features for a wick.
    
    Args:
        candle: The candle containing the wick
        wick_side: "upper" or "lower"
    
    Returns:
        Dict of wick geometry features
    """
    range_size = candle.high - candle.low

    # Avoid division by zero
    if range_size <= 0:
        return {
            "wick_size_pct": 0.0,
            "body_size_pct": 0.0,
            "wick_to_body_ratio": 0.0,
            "protrusion_pct": 0.0,
            "rejection_velocity": 0.0,
            "displacement_idx": 0.0,
            "finished_auction": False,
            "unfinished_business": False,
            "zero_print_flag": False,
            "imbalance_trap_score": 0.0,
        }

    body_top = max(candle.open, candle.close)
    body_bottom = min(candle.open, candle.close)
    body_size = body_top - body_bottom

    if wick_side == "upper":
        wick_size = candle.high - body_top
    else:
        wick_size = body_bottom - candle.low

    wick_size_pct = wick_size / range_size
    body_size_pct = body_size / range_size

    # Avoid division by zero for ratio
    if body_size > 0.00000001:
        wick_to_body_ratio = wick_size / body_size
    else:
        # Doji candle - consider wick as significant
        wick_to_body_ratio = wick_size * 100 if wick_size > 0 else 0.0

    # Protrusion: how far the wick extends beyond normal range
    # Simplified: wick_size as percentage of body
    protrusion_pct = wick_size_pct

    # Rejection velocity: price change per second during the wick
    # Calculate duration of the candle
    duration_seconds = (candle.end_ts - candle.start_ts).total_seconds()
    if duration_seconds > 0:
        rejection_velocity = wick_size / duration_seconds
    else:
        rejection_velocity = 0.0

    # Displacement index: measure of how aggressive the reversal was
    # Higher displacement = stronger rejection
    displacement_idx = wick_size_pct * wick_to_body_ratio

    # Imbalance trap score: likelihood of trapped traders
    # Based on volume imbalance during the wick
    buy_vol = candle.buy_volume
    sell_vol = candle.sell_volume
    total_vol = buy_vol + sell_vol

    if total_vol > 0:
        if wick_side == "upper":
            # Upper wick: buyers likely trapped at high
            # Score higher if more buy volume at the top
            imbalance = buy_vol / total_vol
        else:
            # Lower wick: sellers likely trapped at low
            imbalance = sell_vol / total_vol

        # Scale to 0-100
        imbalance_trap_score = imbalance * 100 * wick_to_body_ratio
        imbalance_trap_score = min(100.0, imbalance_trap_score)
    else:
        imbalance_trap_score = 0.0

    # Finished auction: strong rejection with high volume
    finished_auction = (
        wick_to_body_ratio >= 2.0 and
        total_vol > 0 and
        wick_size_pct >= 0.3
    )

    # Unfinished business: moderate rejection, likely to revisit
    unfinished_business = (
        1.0 <= wick_to_body_ratio < 2.0 and
        wick_size_pct >= 0.2
    )

    # Zero print flag: extreme low volume in the wick area
    # Simplified: flag if very small volume relative to size
    zero_print_flag = (
        total_vol < 0.001 and wick_size_pct > 0.1
    )

    return {
        "wick_size_pct": round(wick_size_pct, 6),
        "body_size_pct": round(body_size_pct, 6),
        "wick_to_body_ratio": round(wick_to_body_ratio, 4),
        "protrusion_pct": round(protrusion_pct, 6),
        "rejection_velocity": round(rejection_velocity, 6),
        "displacement_idx": round(displacement_idx, 6),
        "finished_auction": finished_auction,
        "unfinished_business": unfinished_business,
        "zero_print_flag": zero_print_flag,
        "imbalance_trap_score": round(imbalance_trap_score, 2),
    }

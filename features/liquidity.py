#!/usr/bin/env python3
"""
Liquidity Feature Computation
==============================
Computes liquidity features from orderbook data.
"""

from typing import Dict, List, Optional, Tuple
from feeds.okx_orderbook import OrderBookSnapshot


def compute_liquidity_features(orderbook: Optional[OrderBookSnapshot]) -> Dict:
    """
    Compute liquidity features from an orderbook snapshot.
    
    Args:
        orderbook: OrderBookSnapshot or None
    
    Returns:
        Dict of liquidity features
    """
    # Handle missing orderbook
    if orderbook is None:
        return {
            "spread": 0.0,
            "l1_depth_bid": 0.0,
            "l1_depth_ask": 0.0,
            "l5_depth_bid": 0.0,
            "l5_depth_ask": 0.0,
            "depth_imbalance": 0.0,
            "liquidity_void_flag": False,
            "stacked_imbalance_nearby": False,
        }
    
    bids = orderbook.bids
    asks = orderbook.asks
    
    # Spread
    spread = 0.0
    if bids and asks:
        spread = asks[0][0] - bids[0][0]
    
    # L1 Depth (best bid/ask size)
    l1_depth_bid = bids[0][1] if bids else 0.0
    l1_depth_ask = asks[0][1] if asks else 0.0
    
    # L5 Depth (sum of top 5 levels)
    l5_depth_bid = sum(size for _, size in bids[:5])
    l5_depth_ask = sum(size for _, size in asks[:5])
    
    # Depth imbalance: (bid - ask) / (bid + ask)
    total_depth = l5_depth_bid + l5_depth_ask
    if total_depth > 0:
        depth_imbalance = (l5_depth_bid - l5_depth_ask) / total_depth
    else:
        depth_imbalance = 0.0
    
    # Liquidity void detection
    # Check for abnormal gaps between price levels
    liquidity_void_flag = _detect_liquidity_void(bids, asks)
    
    # Stacked imbalance detection
    # Check if one side has significantly more depth
    stacked_imbalance_nearby = _detect_stacked_imbalance(l5_depth_bid, l5_depth_ask)
    
    return {
        "spread": round(spread, 6),
        "l1_depth_bid": round(l1_depth_bid, 6),
        "l1_depth_ask": round(l1_depth_ask, 6),
        "l5_depth_bid": round(l5_depth_bid, 6),
        "l5_depth_ask": round(l5_depth_ask, 6),
        "depth_imbalance": round(depth_imbalance, 6),
        "liquidity_void_flag": liquidity_void_flag,
        "stacked_imbalance_nearby": stacked_imbalance_nearby,
    }


def _detect_liquidity_void(
    bids: List[Tuple[float, float]], 
    asks: List[Tuple[float, float]]
) -> bool:
    """
    Detect if there's a liquidity void (abnormal gap between levels).
    
    A void is detected if any gap is >= 5x the minimum gap.
    """
    gaps = []
    
    # Calculate bid gaps (bids should be sorted descending by price)
    for i in range(len(bids) - 1):
        gap = bids[i][0] - bids[i + 1][0]
        if gap > 0:
            gaps.append(gap)
    
    # Calculate ask gaps (asks should be sorted ascending by price)
    for i in range(len(asks) - 1):
        gap = asks[i + 1][0] - asks[i][0]
        if gap > 0:
            gaps.append(gap)
    
    if len(gaps) < 2:
        return False
    
    min_gap = min(gaps)
    max_gap = max(gaps)
    
    # Void if max gap is 5x larger than min gap
    if min_gap > 0 and max_gap >= 5 * min_gap:
        return True
    
    return False


def _detect_stacked_imbalance(bid_depth: float, ask_depth: float) -> bool:
    """
    Detect if there's a stacked imbalance (one side has 3x+ more depth).
    """
    if bid_depth <= 0 and ask_depth <= 0:
        return False
    
    if bid_depth <= 0:
        return ask_depth > 0
    
    if ask_depth <= 0:
        return bid_depth > 0
    
    ratio = max(bid_depth / ask_depth, ask_depth / bid_depth)
    return ratio >= 3.0

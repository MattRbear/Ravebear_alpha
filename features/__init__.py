#!/usr/bin/env python3
"""
Features Module
===============
Core data models for wick events and feature extraction.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, Optional
from pydantic import BaseModel, Field


class WickFeatures(BaseModel):
    """
    Complete feature set for a wick event.
    All features default to sensible values to handle missing data gracefully.
    """
    
    # Wick Geometry
    wick_size_pct: float = 0.0
    body_size_pct: float = 0.0
    wick_to_body_ratio: float = 0.0
    protrusion_pct: float = 0.0
    rejection_velocity: float = 0.0
    displacement_idx: float = 0.0
    finished_auction: bool = False
    unfinished_business: bool = False
    zero_print_flag: bool = False
    imbalance_trap_score: float = 0.0
    
    # Order Flow
    delta_at_wick: float = 0.0
    delta_prev_pivot: float = 0.0
    delta_divergence_flag: bool = False
    cvd_slope_10: float = 0.0
    absorption_flag: bool = False
    exhaustion_flag: bool = False
    trade_frequency_spike: float = 0.0
    bid_ask_refresh_rate: float = 0.0
    iceberg_flag: bool = False
    
    # Liquidity
    spread: float = 0.0
    l1_depth_bid: float = 0.0
    l1_depth_ask: float = 0.0
    l5_depth_bid: float = 0.0
    l5_depth_ask: float = 0.0
    depth_imbalance: float = 0.0
    liquidity_void_flag: bool = False
    stacked_imbalance_nearby: bool = False
    
    # Derivatives
    oi_change_pct: float = 0.0
    oi_direction: str = "inc"
    oi_liquidation_flag: bool = False
    liquidation_density: float = 0.0
    funding_rate_now: float = 0.0
    funding_rate_next: float = 0.0
    funding_distance_to_timestamp: float = 0.0
    
    # VWAP
    session_vwap_distance: float = 0.0
    global_vwap_distance: float = 0.0
    vwap_band_flag_1sd: bool = False
    vwap_band_flag_2sd: bool = False
    vwap_mean_reversion_score: float = 0.0
    
    # Regime
    hurst_exponent: float = 0.5
    adx_14: float = 0.0
    atr_14: float = 0.0
    trend_strength: float = 0.0
    btc_d: float = 0.0
    usdt_d: float = 0.0
    eth_btc_trend: float = 0.0
    rolling_beta_btc_30: float = 0.0
    rolling_beta_btc_90: float = 0.0
    correlation_drift: float = 0.0
    
    # Session
    session_label: str = "unknown"
    minutes_into_session: int = 0
    minutes_until_session_close: int = 0
    hour_of_day: int = 0
    day_of_week: int = 0
    weekend_flag: bool = False
    cme_close_proximity: float = 0.0
    
    # Market Profile
    fresh_sd_zone_flag: bool = False
    sd_zone_penetration_pct: float = 0.0
    poc_distance: float = 0.0
    vah_distance: float = 0.0
    val_distance: float = 0.0
    value_rejection_flag: bool = False
    
    # Labels (populated by label_engine)
    untouched_30m: Optional[bool] = None
    untouched_1h: Optional[bool] = None
    untouched_4h: Optional[bool] = None
    hold_duration: Optional[float] = None
    mfe: Optional[float] = None
    mae: Optional[float] = None
    distance_moved: Optional[float] = None

    class Config:
        extra = "allow"  # Allow extra fields for forward compatibility


class WickEvent(BaseModel):
    """
    A detected wick event with all associated features.
    """
    ts: datetime
    symbol: str
    timeframe: str = "1m"
    wick_side: str  # "upper" or "lower"
    wick_high: float
    wick_low: float
    features: WickFeatures = Field(default_factory=WickFeatures)
    
    class Config:
        extra = "allow"  # Allow extra fields like 'orderbook'


__all__ = [
    "WickFeatures",
    "WickEvent",
]

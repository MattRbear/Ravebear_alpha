#!/usr/bin/env python3
"""
ALPHA Void & Wall Detector
==========================
Computes actual void bands and stacked walls from orderbook data.

Author: Flint for RaveBear
"""

import numpy as np
from dataclasses import dataclass, field
from typing import List, Dict, Tuple, Optional
from collections import deque


@dataclass
class VoidBand:
    """A detected liquidity void band."""
    start_price: float
    end_price: float
    width_bps: float
    cum_depth: float  # Total depth in the band (should be low)
    direction: str    # "above" or "below"
    
    def __str__(self):
        dir_arrow = "↑" if self.direction == "above" else "↓"
        return f"VOID{dir_arrow} {self.start_price:,.2f} → {self.end_price:,.2f} ({self.width_bps:.0f}bps) depth=${self.cum_depth/1000:.1f}k"


@dataclass
class StackedWall:
    """A detected liquidity wall."""
    price: float
    size: float          # In base currency
    notional: float      # In quote currency (USD)
    distance_bps: float  # Distance from reference price
    side: str            # "bid" or "ask"
    
    def __str__(self):
        side_label = "ASK" if self.side == "ask" else "BID"
        sign = "+" if self.side == "ask" else "-"
        return f"WALL({side_label}) {self.price:,.2f} size=${self.notional/1000:.1f}k dist={sign}{abs(self.distance_bps):.0f}bps"


@dataclass
class OrderbookSnapshot:
    """Raw orderbook data for analysis."""
    symbol: str
    timestamp: str
    mid_price: float
    bids: List[Tuple[float, float]]  # [(price, size), ...] sorted desc by price
    asks: List[Tuple[float, float]]  # [(price, size), ...] sorted asc by price
    
    def to_dict(self) -> Dict:
        return {
            'symbol': self.symbol,
            'timestamp': self.timestamp,
            'mid_price': self.mid_price,
            'bids': self.bids[:20],  # Store top 20 levels
            'asks': self.asks[:20],
        }
    
    @classmethod
    def from_dict(cls, d: Dict) -> 'OrderbookSnapshot':
        return cls(
            symbol=d['symbol'],
            timestamp=d['timestamp'],
            mid_price=d['mid_price'],
            bids=[(p, s) for p, s in d.get('bids', [])],
            asks=[(p, s) for p, s in d.get('asks', [])],
        )


class VoidWallDetector:
    """
    Detects void bands and stacked walls from orderbook data.
    
    Void = band of price levels with abnormally low depth
    Stacked = price level(s) with abnormally high depth
    """
    
    def __init__(
        self,
        band_width_bps: float = 10.0,      # Width of each scan band in bps
        void_percentile: float = 10.0,      # Bottom X% depth = void
        stack_percentile: float = 90.0,     # Top X% size = stack
        max_bands_to_scan: int = 20,        # How far to scan from price
        history_size: int = 100,            # Rolling history for calibration
    ):
        self.band_width_bps = band_width_bps
        self.void_percentile = void_percentile
        self.stack_percentile = stack_percentile
        self.max_bands_to_scan = max_bands_to_scan
        
        # Rolling history for dynamic thresholds per symbol
        self.depth_history: Dict[str, deque] = {}
        self.wall_history: Dict[str, deque] = {}
        self.history_size = history_size
    
    def _ensure_history(self, symbol: str):
        if symbol not in self.depth_history:
            self.depth_history[symbol] = deque(maxlen=self.history_size)
        if symbol not in self.wall_history:
            self.wall_history[symbol] = deque(maxlen=self.history_size)
    
    def _get_void_threshold(self, symbol: str) -> float:
        """Get dynamic void threshold based on recent history."""
        self._ensure_history(symbol)
        if len(self.depth_history[symbol]) < 10:
            # Default threshold if not enough history
            return 50000  # $50k notional
        
        depths = list(self.depth_history[symbol])
        return np.percentile(depths, self.void_percentile)
    
    def _get_stack_threshold(self, symbol: str) -> float:
        """Get dynamic stack threshold based on recent history."""
        self._ensure_history(symbol)
        if len(self.wall_history[symbol]) < 10:
            # Default threshold
            return 500000  # $500k notional
        
        walls = list(self.wall_history[symbol])
        return np.percentile(walls, self.stack_percentile)
    
    def _price_to_bps(self, price: float, ref_price: float) -> float:
        """Convert price difference to basis points."""
        if ref_price == 0:
            return 0
        return ((price - ref_price) / ref_price) * 10000
    
    def _bps_to_price(self, bps: float, ref_price: float) -> float:
        """Convert basis points to price."""
        return ref_price * (1 + bps / 10000)
    
    def _compute_band_depth(
        self, 
        levels: List[Tuple[float, float]], 
        band_start: float, 
        band_end: float,
        ref_price: float
    ) -> float:
        """Compute total notional depth in a price band."""
        total = 0.0
        for price, size in levels:
            if min(band_start, band_end) <= price <= max(band_start, band_end):
                total += size * price  # Notional in USD
        return total
    
    def detect_void_bands(
        self, 
        ob: OrderbookSnapshot,
        direction: str = "both"  # "above", "below", or "both"
    ) -> List[VoidBand]:
        """
        Detect void bands above and/or below current price.
        
        Returns merged void bands sorted by distance from price.
        """
        voids = []
        ref_price = ob.mid_price
        void_threshold = self._get_void_threshold(ob.symbol)
        
        # Scan above (asks)
        if direction in ("above", "both"):
            above_voids = self._scan_for_voids(
                ob.asks, 
                ref_price, 
                void_threshold,
                direction="above"
            )
            voids.extend(above_voids)
        
        # Scan below (bids)
        if direction in ("below", "both"):
            below_voids = self._scan_for_voids(
                ob.bids, 
                ref_price, 
                void_threshold,
                direction="below"
            )
            voids.extend(below_voids)
        
        return voids
    
    def _scan_for_voids(
        self,
        levels: List[Tuple[float, float]],
        ref_price: float,
        threshold: float,
        direction: str
    ) -> List[VoidBand]:
        """Scan orderbook levels for void bands."""
        voids = []
        band_depths = []
        
        # Generate bands to scan
        for i in range(self.max_bands_to_scan):
            if direction == "above":
                band_start = self._bps_to_price(i * self.band_width_bps, ref_price)
                band_end = self._bps_to_price((i + 1) * self.band_width_bps, ref_price)
            else:
                band_start = self._bps_to_price(-i * self.band_width_bps, ref_price)
                band_end = self._bps_to_price(-(i + 1) * self.band_width_bps, ref_price)
            
            depth = self._compute_band_depth(levels, band_start, band_end, ref_price)
            band_depths.append((band_start, band_end, depth))
            
            # Update history for calibration
            self._ensure_history("_global_")  # Use global for band calibration
            self.depth_history["_global_"].append(depth)
        
        # Find void bands (depth below threshold)
        void_bands = []
        for band_start, band_end, depth in band_depths:
            if depth < threshold:
                void_bands.append((band_start, band_end, depth))
        
        # Merge adjacent void bands
        merged = self._merge_adjacent_bands(void_bands, ref_price, direction)
        
        return merged
    
    def _merge_adjacent_bands(
        self,
        bands: List[Tuple[float, float, float]],
        ref_price: float,
        direction: str
    ) -> List[VoidBand]:
        """Merge adjacent void bands into continuous regions."""
        if not bands:
            return []
        
        # Sort by start price
        sorted_bands = sorted(bands, key=lambda x: x[0], reverse=(direction == "below"))
        
        merged = []
        current_start = sorted_bands[0][0]
        current_end = sorted_bands[0][1]
        current_depth = sorted_bands[0][2]
        
        for i in range(1, len(sorted_bands)):
            band_start, band_end, depth = sorted_bands[i]
            
            # Check if adjacent (within 1 band width)
            gap = abs(band_start - current_end)
            threshold = abs(self._bps_to_price(self.band_width_bps * 1.5, ref_price) - ref_price)
            
            if gap <= threshold:
                # Merge
                current_end = band_end
                current_depth += depth
            else:
                # Save current and start new
                width_bps = abs(self._price_to_bps(current_end, ref_price) - 
                               self._price_to_bps(current_start, ref_price))
                merged.append(VoidBand(
                    start_price=min(current_start, current_end),
                    end_price=max(current_start, current_end),
                    width_bps=width_bps,
                    cum_depth=current_depth,
                    direction=direction
                ))
                current_start = band_start
                current_end = band_end
                current_depth = depth
        
        # Don't forget the last one
        width_bps = abs(self._price_to_bps(current_end, ref_price) - 
                       self._price_to_bps(current_start, ref_price))
        merged.append(VoidBand(
            start_price=min(current_start, current_end),
            end_price=max(current_start, current_end),
            width_bps=width_bps,
            cum_depth=current_depth,
            direction=direction
        ))
        
        return merged
    
    def detect_stacked_walls(
        self,
        ob: OrderbookSnapshot,
        top_n: int = 3
    ) -> Tuple[List[StackedWall], List[StackedWall]]:
        """
        Detect stacked walls (large resting orders) on both sides.
        
        Returns (bid_walls, ask_walls) sorted by size descending.
        """
        stack_threshold = self._get_stack_threshold(ob.symbol)
        ref_price = ob.mid_price
        
        bid_walls = []
        ask_walls = []
        
        # Scan bids
        for price, size in ob.bids:
            notional = price * size
            
            # Update history
            self._ensure_history(ob.symbol)
            self.wall_history[ob.symbol].append(notional)
            
            if notional >= stack_threshold:
                dist_bps = self._price_to_bps(price, ref_price)
                bid_walls.append(StackedWall(
                    price=price,
                    size=size,
                    notional=notional,
                    distance_bps=dist_bps,
                    side="bid"
                ))
        
        # Scan asks
        for price, size in ob.asks:
            notional = price * size
            
            self.wall_history[ob.symbol].append(notional)
            
            if notional >= stack_threshold:
                dist_bps = self._price_to_bps(price, ref_price)
                ask_walls.append(StackedWall(
                    price=price,
                    size=size,
                    notional=notional,
                    distance_bps=dist_bps,
                    side="ask"
                ))
        
        # Sort by notional descending and take top N
        bid_walls = sorted(bid_walls, key=lambda w: w.notional, reverse=True)[:top_n]
        ask_walls = sorted(ask_walls, key=lambda w: w.notional, reverse=True)[:top_n]
        
        return bid_walls, ask_walls
    
    def analyze(self, ob: OrderbookSnapshot) -> Dict:
        """
        Full analysis: voids + walls.
        
        Returns a dict ready for dashboard display.
        """
        voids = self.detect_void_bands(ob)
        bid_walls, ask_walls = self.detect_stacked_walls(ob)
        
        # Separate voids by direction
        void_above = [v for v in voids if v.direction == "above"]
        void_below = [v for v in voids if v.direction == "below"]
        
        # Get nearest/most significant
        nearest_void_above = void_above[0] if void_above else None
        nearest_void_below = void_below[0] if void_below else None
        
        return {
            'void_above': nearest_void_above,
            'void_below': nearest_void_below,
            'all_voids_above': void_above,
            'all_voids_below': void_below,
            'bid_walls': bid_walls,
            'ask_walls': ask_walls,
            'has_void': len(voids) > 0,
            'has_stack': len(bid_walls) > 0 or len(ask_walls) > 0,
        }


# ==================== FORMATTING FOR DASHBOARD ====================

def format_void_line(void: Optional[VoidBand], symbol: str) -> str:
    """Format a void band for terminal display."""
    if void is None:
        return "---"
    
    # Format prices based on symbol
    if 'BTC' in symbol:
        return f"{void.start_price:,.0f}→{void.end_price:,.0f} ({void.width_bps:.0f}bps) ${void.cum_depth/1000:.0f}k"
    else:
        return f"{void.start_price:,.2f}→{void.end_price:,.2f} ({void.width_bps:.0f}bps) ${void.cum_depth/1000:.0f}k"


def format_wall_line(wall: Optional[StackedWall], symbol: str) -> str:
    """Format a wall for terminal display."""
    if wall is None:
        return "---"
    
    sign = "+" if wall.side == "ask" else "-"
    if 'BTC' in symbol:
        return f"{wall.price:,.0f} ${wall.notional/1000:.0f}k ({sign}{abs(wall.distance_bps):.0f}bps)"
    else:
        return f"{wall.price:,.2f} ${wall.notional/1000:.0f}k ({sign}{abs(wall.distance_bps):.0f}bps)"


# ==================== TEST / DEMO ====================

if __name__ == "__main__":
    # Demo with synthetic orderbook
    print("Testing VoidWallDetector...")
    
    # Create synthetic orderbook (BTC around $91,300)
    mid = 91300
    
    # Bids: some levels with gaps (voids)
    bids = [
        (91290, 0.5),   # $45k
        (91280, 0.3),   # $27k
        (91270, 0.1),   # $9k - thin
        (91260, 0.05),  # $4.5k - very thin (VOID)
        (91250, 0.02),  # $1.8k - very thin (VOID)
        (91240, 0.8),   # $73k
        (91200, 2.5),   # $228k - WALL
        (91150, 1.2),   # $109k
        (91100, 0.6),   # $55k
    ]
    
    # Asks: some levels with walls
    asks = [
        (91310, 0.4),   # $36k
        (91320, 0.2),   # $18k
        (91330, 0.05),  # $4.5k - thin (VOID)
        (91340, 0.03),  # $2.7k - thin (VOID)
        (91350, 0.02),  # $1.8k - thin (VOID)
        (91400, 1.8),   # $164k - WALL
        (91450, 2.2),   # $201k - WALL
        (91500, 0.5),   # $46k
    ]
    
    ob = OrderbookSnapshot(
        symbol="BTC-USDT",
        timestamp="2026-01-04T15:00:00Z",
        mid_price=mid,
        bids=bids,
        asks=asks,
    )
    
    detector = VoidWallDetector(
        band_width_bps=5.0,
        void_percentile=20.0,
        stack_percentile=80.0,
    )
    
    # Pre-seed some history for calibration
    for _ in range(50):
        detector.depth_history["_global_"].append(50000)
        detector.wall_history["BTC-USDT"].append(100000)
    
    result = detector.analyze(ob)
    
    print("\n" + "="*60)
    print("VOID BANDS:")
    print("="*60)
    
    if result['void_above']:
        print(f"  VOID↑ {format_void_line(result['void_above'], 'BTC-USDT')}")
    else:
        print("  VOID↑ ---")
    
    if result['void_below']:
        print(f"  VOID↓ {format_void_line(result['void_below'], 'BTC-USDT')}")
    else:
        print("  VOID↓ ---")
    
    print("\n" + "="*60)
    print("STACKED WALLS:")
    print("="*60)
    
    print("  ASK WALLS:")
    for wall in result['ask_walls']:
        print(f"    {format_wall_line(wall, 'BTC-USDT')}")
    
    print("  BID WALLS:")
    for wall in result['bid_walls']:
        print(f"    {format_wall_line(wall, 'BTC-USDT')}")
    
    print("\n✓ Test complete")

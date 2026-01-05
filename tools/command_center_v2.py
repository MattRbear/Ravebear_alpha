#!/usr/bin/env python3
"""
ALPHA COMMAND CENTER v2.1
=========================
Professional-grade wick intelligence dashboard.
Now with actual VOID bands and STACKED walls with price levels.

Author: Flint for RaveBear
"""

import os
import sys
import json
import time
import glob
import asyncio
import msvcrt
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, field

# Add project root to path for imports
SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent
sys.path.insert(0, str(PROJECT_ROOT))

from analysis.void_wall_detector import VoidWallDetector, OrderbookSnapshot, VoidBand, StackedWall, format_void_line, format_wall_line

# ==================== PATHS ====================
DATA_DIR = PROJECT_ROOT / "data"
STATUS_FILE = DATA_DIR / "engine_status.json"
ORDERBOOK_CACHE = DATA_DIR / "orderbook_cache.json"

# ==================== ANSI COLORS ====================
RESET = "\033[0m"
BOLD = "\033[1m"
DIM = "\033[2m"
UNDERLINE = "\033[4m"

BLACK = "\033[30m"
WHITE = "\033[97m"
GRAY = "\033[90m"

BTC_COLOR = "\033[96m"
ETH_COLOR = "\033[95m"
SOL_COLOR = "\033[93m"

GREEN = "\033[92m"
YELLOW = "\033[93m"
RED = "\033[91m"
PURPLE = "\033[35m"
CYAN = "\033[36m"

BG_RED = "\033[41m"
BG_YELLOW = "\033[43m"
BG_GREEN = "\033[42m"

SYMBOL_COLORS = {
    "BTC-USDT": BTC_COLOR,
    "ETH-USDT": ETH_COLOR,
    "SOL-USDT": SOL_COLOR,
}


@dataclass
class WickData:
    ts: str
    symbol: str
    timeframe: str
    wick_side: str
    wick_high: float
    wick_low: float
    features: Dict
    raw: Dict
    orderbook: Optional[Dict] = None
    
    # Computed
    wb_ratio: float = 0.0
    is_doji: bool = False
    magnet_score: float = 0.0
    confidence: float = 0.0
    trap_mode: str = "NONE"
    timing_class: str = "NORMAL"
    attack_window: int = 0
    attention_score: float = 0.0
    integrity: float = 1.0
    missing_flags: List[str] = None
    market_state: str = ""
    market_state_conf: float = 0.0
    market_state_evidence: List[str] = None
    
    # Void/Wall data
    void_above: Optional[VoidBand] = None
    void_below: Optional[VoidBand] = None
    bid_walls: List[StackedWall] = None
    ask_walls: List[StackedWall] = None

    def __post_init__(self):
        if self.missing_flags is None:
            self.missing_flags = []
        if self.market_state_evidence is None:
            self.market_state_evidence = []
        if self.bid_walls is None:
            self.bid_walls = []
        if self.ask_walls is None:
            self.ask_walls = []


# ==================== GLOBALS ====================
void_detector = VoidWallDetector(
    band_width_bps=10.0,
    void_percentile=15.0,
    stack_percentile=85.0,
)


def clear():
    os.system('cls' if os.name == 'nt' else 'clear')


def get_latest_jsonl() -> Optional[Path]:
    pattern = str(DATA_DIR / "wick_events_*.jsonl")
    files = glob.glob(pattern)
    return Path(max(files, key=os.path.getmtime)) if files else None


def load_status() -> Dict:
    try:
        with open(STATUS_FILE, 'r') as f:
            return json.load(f)
    except:
        return {}


def load_orderbook_cache() -> Dict[str, OrderbookSnapshot]:
    """Load cached orderbook snapshots."""
    cache = {}
    try:
        if ORDERBOOK_CACHE.exists():
            with open(ORDERBOOK_CACHE, 'r') as f:
                data = json.load(f)
                for symbol, ob_data in data.items():
                    cache[symbol] = OrderbookSnapshot.from_dict(ob_data)
    except:
        pass
    return cache


def load_recent_wicks(n: int = 100) -> List[WickData]:
    jsonl_path = get_latest_jsonl()
    if not jsonl_path:
        return []
    
    wicks = []
    try:
        with open(jsonl_path, 'r') as f:
            lines = f.readlines()
            for line in lines[-n:]:
                try:
                    raw = json.loads(line.strip())
                    wick = WickData(
                        ts=raw.get('ts', ''),
                        symbol=raw.get('symbol', 'UNKNOWN'),
                        timeframe=raw.get('timeframe', '1m'),
                        wick_side=raw.get('wick_side', 'unknown'),
                        wick_high=raw.get('wick_high', 0),
                        wick_low=raw.get('wick_low', 0),
                        features=raw.get('features', {}),
                        raw=raw,
                        orderbook=raw.get('orderbook'),
                    )
                    process_wick(wick)
                    wicks.append(wick)
                except:
                    pass
    except:
        pass
    
    return wicks


def process_wick(w: WickData):
    """Compute all derived fields."""
    f = w.features
    
    w.wb_ratio = min(f.get('wick_to_body_ratio', 0), 999)
    w.is_doji = w.wb_ratio >= 50 or f.get('body_size_pct', 1) < 0.05
    
    # Missing flags
    w.missing_flags = []
    if f.get('l5_depth_bid', 0) == 0 and f.get('l5_depth_ask', 0) == 0:
        w.missing_flags.append("NO_DEPTH")
    if f.get('oi_change_pct') is None:
        w.missing_flags.append("NO_OI")
    if f.get('funding_rate_now') is None or f.get('funding_rate_now', 0) == 0:
        w.missing_flags.append("NO_FUND")
    
    total_fields = 5
    w.integrity = (total_fields - len(w.missing_flags)) / total_fields
    
    w.magnet_score = compute_magnet_score(w)
    w.confidence = compute_confidence(w)
    w.trap_mode = detect_trap_mode(w)
    w.timing_class, w.attack_window = classify_timing(w)
    
    void_bonus = 0.15 if f.get('liquidity_void_flag', False) else 0
    stacked_bonus = 0.15 if f.get('stacked_imbalance_nearby', False) else 0
    w.attention_score = w.magnet_score * (w.confidence / 100) * w.integrity * (1 + void_bonus + stacked_bonus)
    
    w.market_state, w.market_state_conf, w.market_state_evidence = detect_market_state(w)
    
    # Process orderbook for void/wall detection
    if w.orderbook:
        try:
            ob = OrderbookSnapshot.from_dict(w.orderbook)
            result = void_detector.analyze(ob)
            w.void_above = result['void_above']
            w.void_below = result['void_below']
            w.bid_walls = result['bid_walls']
            w.ask_walls = result['ask_walls']
        except:
            pass


def compute_magnet_score(w: WickData) -> float:
    f = w.features
    score = 0
    
    if w.wb_ratio >= 2:
        score += 15
    elif w.wb_ratio >= 1:
        score += 8
    
    vwap_score = f.get('vwap_mean_reversion_score', 0)
    if vwap_score >= 70:
        score += 20
    elif vwap_score >= 40:
        score += 10
    
    depth_total = f.get('l5_depth_bid', 0) + f.get('l5_depth_ask', 0)
    if depth_total > 0:
        score += min(15, depth_total / 10)
    
    rej_vel = f.get('rejection_velocity', 0)
    if rej_vel > 0.1:
        score += 15
    elif rej_vel > 0.05:
        score += 8
    
    if f.get('liquidity_void_flag', False):
        score += 10
    if f.get('stacked_imbalance_nearby', False):
        score += 10
    
    oi_change = abs(f.get('oi_change_pct', 0) * 100)
    if oi_change > 0.05:
        score += 5
    
    return min(100, score)


def compute_confidence(w: WickData) -> float:
    conf = 50
    f = w.features
    conf += w.integrity * 20
    delta = abs(f.get('delta_at_wick', 0))
    if delta > 50:
        conf += 15
    elif delta > 10:
        conf += 8
    imbal = abs(f.get('depth_imbalance', 0))
    if imbal > 0.5:
        conf += 10
    return min(100, conf)


def detect_trap_mode(w: WickData) -> str:
    f = w.features
    delta = f.get('delta_at_wick', 0)
    
    if w.wb_ratio >= 3:
        if w.wick_side == 'lower' and delta < -20:
            return "HARD_TRAP"
        elif w.wick_side == 'upper' and delta > 20:
            return "HARD_TRAP"
        elif abs(delta) > 10:
            return "SOFT_TRAP"
    
    if f.get('oi_liquidation_flag', False):
        return "LIQ_REVERSE"
    
    return "NO_TRAP"


def classify_timing(w: WickData) -> Tuple[str, int]:
    f = w.features
    mins_left = f.get('minutes_until_session_close', 999)
    mins_into = f.get('minutes_into_session', 0)
    
    attack_window = 300
    
    if mins_left < 30:
        return "SESS_END", mins_left * 60
    elif mins_into < 30:
        return "SESS_OPEN", 180
    elif f.get('vwap_mean_reversion_score', 0) > 80:
        return "EXTENDED", 120
    
    return "NORMAL", attack_window


def detect_market_state(w: WickData) -> Tuple[str, float, List[str]]:
    f = w.features
    
    delta = f.get('delta_at_wick', 0)
    rej_vel = f.get('rejection_velocity', 0)
    depth_total = f.get('l5_depth_bid', 0) + f.get('l5_depth_ask', 0)
    depth_imbal = f.get('depth_imbalance', 0)
    void = f.get('liquidity_void_flag', False)
    cvd_slope = f.get('cvd_slope_10', 0)
    
    if abs(delta) > 30 and w.wb_ratio >= 1.5 and rej_vel < 0.05:
        conf = min(0.95, 0.5 + abs(delta) / 100)
        return "ABSORPTION", conf, ["delta", "rej_vel", "wb_ratio"]
    
    if depth_total < 5 and void:
        return "VACUUM", 0.85, ["l5_depth", "void", "imbal"]
    
    if f.get('exhaustion_flag', False) or (cvd_slope * delta < 0 and abs(cvd_slope) > 20):
        return "EXHAUSTION", 0.75, ["cvd_slope", "delta", "exhaust"]
    
    if rej_vel > 0.2 and void:
        return "BREAKOUT", 0.80, ["rej_vel", "void"]
    
    if depth_imbal > 0.3 and w.wb_ratio < 0.5:
        return "ACCUM", 0.65, ["imbal", "wb_ratio"]
    
    if depth_imbal < -0.3 and w.wb_ratio < 0.5:
        return "DISTRIB", 0.65, ["imbal", "wb_ratio"]
    
    return "NEUTRAL", 0.5, []


def format_price(price: float, symbol: str) -> str:
    return f"${price:,.0f}" if 'BTC' in symbol else f"${price:,.2f}"


def health_color(status: str) -> str:
    return GREEN if status == "OK" else YELLOW if status in ("SLOW", "STALE") else RED


def feed_status(age: int) -> Tuple[str, str]:
    if age < 30:
        return "OK", GREEN
    elif age < 120:
        return "SLOW", YELLOW
    return "DEAD", RED


def confidence_color(conf: float) -> str:
    return GREEN if conf >= 75 else YELLOW if conf >= 50 else RED


def magnet_color(score: float) -> str:
    return GREEN if score >= 70 else YELLOW if score >= 50 else RED


def trap_color(mode: str) -> str:
    return GRAY if mode == "NO_TRAP" else YELLOW if mode == "SOFT_TRAP" else RED


# ==================== RENDER FUNCTIONS ====================

def render_health_strip(status: Dict, jsonl_path: Optional[Path], wicks: List[WickData]):
    running = status.get('running', False)
    uptime = status.get('uptime_seconds', 0)
    uptime_str = time.strftime("%H:%M:%S", time.gmtime(uptime))
    feed_ages = status.get('feed_age', {})
    last_error = status.get('last_alert_error', '')
    
    engine_ok = running
    feeds_ok = all(feed_ages.get(f, 999) < 60 for f in ['trades', 'orderbook', 'derivs'])
    
    if engine_ok and feeds_ok:
        strip_bg, strip_status = BG_GREEN, "HEALTHY"
    elif engine_ok:
        strip_bg, strip_status = BG_YELLOW, "DEGRADED"
    else:
        strip_bg, strip_status = BG_RED, "CRITICAL"
    
    if jsonl_path:
        file_size = jsonl_path.stat().st_size / 1024
        file_mtime = datetime.fromtimestamp(jsonl_path.stat().st_mtime).strftime("%H:%M:%S")
        file_name = jsonl_path.name[-25:]
    else:
        file_size, file_mtime, file_name = 0, "N/A", "NO FILE"
    
    integrity_pct = wicks[-1].integrity * 100 if wicks else 0
    
    print(f"{strip_bg}{BLACK}{BOLD} {strip_status} {RESET}", end="")
    
    eng_color = GREEN if running else RED
    eng_status = "RUN" if running else "STOP"
    print(f" ENG:{eng_color}{eng_status}{RESET}({uptime_str})", end="")
    
    print(f" │", end="")
    for feed_name in ['trades', 'orderbook', 'derivs', 'macro']:
        age = feed_ages.get(feed_name, 999)
        st, col = feed_status(age)
        print(f" {feed_name[:3]}:{col}{st}{RESET}", end="")
    
    print(f" │ {GRAY}{file_name}{RESET}({file_mtime})", end="")
    
    int_color = GREEN if integrity_pct >= 80 else YELLOW if integrity_pct >= 50 else RED
    print(f" │ INT:{int_color}{integrity_pct:.0f}%{RESET}")
    
    if last_error and last_error != "None":
        print(f"  {RED}⚠ {last_error[:70]}{RESET}")
    
    print(f"{GRAY}{'─'*120}{RESET}")


def render_ticker_card(symbol: str, wicks: List[WickData], ob_cache: Dict):
    color = SYMBOL_COLORS.get(symbol, WHITE)
    symbol_wicks = [w for w in wicks if w.symbol == symbol]
    
    if not symbol_wicks:
        print(f"  {color}┌{'─'*58}┐{RESET}")
        print(f"  {color}│{RESET} {BOLD}{symbol:<12}{RESET}      {GRAY}NO DATA{RESET}                         {color}│{RESET}")
        print(f"  {color}└{'─'*58}┘{RESET}")
        return
    
    w = symbol_wicks[-1]
    f = w.features
    
    # Analyze live orderbook if available
    if symbol in ob_cache:
        try:
            result = void_detector.analyze(ob_cache[symbol])
            w.void_above = result['void_above']
            w.void_below = result['void_below']
            w.bid_walls = result['bid_walls']
            w.ask_walls = result['ask_walls']
        except:
            pass
    
    try:
        t_obj = datetime.fromisoformat(w.ts.replace('Z', '+00:00'))
        time_str = t_obj.strftime("%H:%M:%S")
    except:
        time_str = "??:??:??"
    
    side = "▲UP" if w.wick_side == 'upper' else "▼DN"
    side_color = RED if w.wick_side == 'upper' else GREEN
    price = w.wick_high if w.wick_side == 'upper' else w.wick_low
    price_str = format_price(price, symbol)
    
    wb_str = f"{w.wb_ratio:.1f}" if w.wb_ratio < 100 else "999"
    doji_flag = f" {PURPLE}DOJI{RESET}" if w.is_doji else ""
    
    vwap_score = f.get('vwap_mean_reversion_score', 0)
    vwap_dist = f.get('session_vwap_distance', 0) * 100
    vwap_color = RED if vwap_score >= 70 else YELLOW if vwap_score >= 40 else GREEN
    
    # Card render
    print(f"  {color}┌{'─'*58}┐{RESET}")
    print(f"  {color}│{RESET} {BOLD}{symbol}{RESET} @ {time_str}  {side_color}{side}{RESET} {price_str:<14}       {color}│{RESET}")
    print(f"  {color}│{RESET} W:B:{BOLD}{wb_str:>5}{RESET}{doji_flag:<8} Mag:{magnet_color(w.magnet_score)}{w.magnet_score:>4.0f}{RESET} Cnf:{confidence_color(w.confidence)}{w.confidence:>3.0f}{RESET}%        {color}│{RESET}")
    print(f"  {color}│{RESET} Trap:{trap_color(w.trap_mode)}{w.trap_mode:<11}{RESET} Time:{w.timing_class:<9} Atk:{w.attack_window:>4}s  {color}│{RESET}")
    print(f"  {color}│{RESET} VWAP:{vwap_color}{vwap_score:>4.0f}{RESET}({vwap_dist:>+.2f}%)  State:{BOLD}{w.market_state:<10}{RESET}      {color}│{RESET}")
    
    # VOID BANDS LINE
    void_up_str = format_void_line(w.void_above, symbol) if w.void_above else "---"
    void_dn_str = format_void_line(w.void_below, symbol) if w.void_below else "---"
    print(f"  {color}│{RESET} {CYAN}VOID↑{RESET} {void_up_str:<50}{color}│{RESET}")
    print(f"  {color}│{RESET} {CYAN}VOID↓{RESET} {void_dn_str:<50}{color}│{RESET}")
    
    # WALL LINES
    ask_wall = w.ask_walls[0] if w.ask_walls else None
    bid_wall = w.bid_walls[0] if w.bid_walls else None
    wall_up_str = format_wall_line(ask_wall, symbol) if ask_wall else "---"
    wall_dn_str = format_wall_line(bid_wall, symbol) if bid_wall else "---"
    print(f"  {color}│{RESET} {YELLOW}WALL↑{RESET} {wall_up_str:<50}{color}│{RESET}")
    print(f"  {color}│{RESET} {YELLOW}WALL↓{RESET} {wall_dn_str:<50}{color}│{RESET}")
    
    # Missing data flags
    missing_str = " ".join([f"{RED}{m}{RESET}" for m in w.missing_flags[:3]]) if w.missing_flags else f"{GREEN}DATA OK{RESET}"
    print(f"  {color}│{RESET} {missing_str:<56}{color}│{RESET}")
    
    print(f"  {color}└{'─'*58}┘{RESET}")


def render_attention_feed(wicks: List[WickData], selected_idx: int = -1):
    sorted_wicks = sorted(wicks, key=lambda w: w.attention_score, reverse=True)[:20]
    
    print(f"\n{BOLD}  ATTENTION FEED (Ranked by Score){RESET}")
    print(f"  {GRAY}{'TIME':<9} {'SYM':<10} {'SD':<3} {'PRICE':<10} {'W:B':<5} {'ATN':<5} {'MAG':<4} {'CNF':<4} {'TRAP':<10} {'VWAP':<4} {'FLAGS':<20}{RESET}")
    print(f"  {GRAY}{'─'*105}{RESET}")
    
    for i, w in enumerate(sorted_wicks):
        color = SYMBOL_COLORS.get(w.symbol, WHITE)
        
        if i == selected_idx:
            row_start, row_end = f"{BG_GREEN}{BLACK}", RESET
        else:
            row_start, row_end = "", ""
        
        try:
            t_obj = datetime.fromisoformat(w.ts.replace('Z', '+00:00'))
            time_str = t_obj.strftime("%H:%M:%S")
        except:
            time_str = "??:??:??"
        
        side = "UP" if w.wick_side == 'upper' else "DN"
        price = w.wick_high if w.wick_side == 'upper' else w.wick_low
        price_str = format_price(price, w.symbol)
        
        wb_str = f"{w.wb_ratio:.1f}" if w.wb_ratio < 100 else "999"
        
        flags = []
        if w.void_above or w.void_below or w.features.get('liquidity_void_flag'):
            flags.append(f"{CYAN}V{RESET}")
        if w.bid_walls or w.ask_walls or w.features.get('stacked_imbalance_nearby'):
            flags.append(f"{YELLOW}S{RESET}")
        if w.is_doji:
            flags.append(f"{PURPLE}D{RESET}")
        for mf in w.missing_flags[:1]:
            flags.append(f"{RED}!{RESET}")
        flags_str = "".join(flags) if flags else "-"
        
        vwap_score = w.features.get('vwap_mean_reversion_score', 0)
        
        sym_short = w.symbol.replace("-USDT", "")
        print(f"  {row_start}{color}{time_str:<9} {sym_short:<10} {side:<3} {price_str:<10} {wb_str:<5} {w.attention_score:<5.1f} {w.magnet_score:<4.0f} {w.confidence:<4.0f} {w.trap_mode:<10} {vwap_score:<4.0f} {flags_str:<20}{row_end}{RESET}")


def render_drilldown(w: WickData):
    f = w.features
    color = SYMBOL_COLORS.get(w.symbol, WHITE)
    
    print(f"\n{color}{'═'*80}{RESET}")
    print(f"{BOLD}  DRILLDOWN: {w.symbol} @ {w.ts}{RESET}")
    print(f"{color}{'═'*80}{RESET}")
    
    # Score panel
    print(f"\n  {UNDERLINE}SCORES{RESET}")
    print(f"  Magnet:{magnet_color(w.magnet_score)}{w.magnet_score:.0f}{RESET} | Conf:{confidence_color(w.confidence)}{w.confidence:.0f}%{RESET} | Trap:{trap_color(w.trap_mode)}{w.trap_mode}{RESET} | Time:{w.timing_class} | Atk:{w.attack_window}s")
    
    # Top drivers
    print(f"\n  {UNDERLINE}SCORE DRIVERS{RESET}")
    drivers = []
    if w.wb_ratio >= 2: drivers.append(("W:B≥2", "+15"))
    if f.get('vwap_mean_reversion_score', 0) >= 70: drivers.append(("VWAP Ext", "+20"))
    if f.get('liquidity_void_flag'): drivers.append(("Void", "+10"))
    if f.get('stacked_imbalance_nearby'): drivers.append(("Stacked", "+10"))
    if f.get('rejection_velocity', 0) > 0.1: drivers.append(("High Rej", "+15"))
    for name, pts in drivers[:4]:
        print(f"    {GREEN}•{RESET} {name}: {pts}")
    
    # VOID BANDS (Full detail)
    print(f"\n  {UNDERLINE}VOID BANDS{RESET}")
    if w.void_above:
        print(f"    {CYAN}↑ ABOVE:{RESET} {w.void_above}")
    else:
        print(f"    {GRAY}↑ ABOVE: No void detected{RESET}")
    if w.void_below:
        print(f"    {CYAN}↓ BELOW:{RESET} {w.void_below}")
    else:
        print(f"    {GRAY}↓ BELOW: No void detected{RESET}")
    
    # STACKED WALLS (Full detail)
    print(f"\n  {UNDERLINE}STACKED WALLS{RESET}")
    print(f"    {YELLOW}ASK WALLS (resistance):{RESET}")
    if w.ask_walls:
        for wall in w.ask_walls[:3]:
            print(f"      {wall}")
    else:
        print(f"      {GRAY}No significant walls{RESET}")
    
    print(f"    {YELLOW}BID WALLS (support):{RESET}")
    if w.bid_walls:
        for wall in w.bid_walls[:3]:
            print(f"      {wall}")
    else:
        print(f"      {GRAY}No significant walls{RESET}")
    
    # Feature clusters
    print(f"\n  {UNDERLINE}GEOMETRY{RESET}")
    print(f"    W:B:{w.wb_ratio:.2f} | RejVel:{f.get('rejection_velocity', 0):.4f} | Body:{f.get('body_size_pct', 0):.2f}")
    
    print(f"\n  {UNDERLINE}ORDERFLOW{RESET}")
    delta = f.get('delta_at_wick', 0)
    delta_color = GREEN if delta > 0 else RED if delta < 0 else WHITE
    print(f"    Delta:{delta_color}{delta:+.1f}{RESET} | CVD:{f.get('cvd_slope_10', 0):.2f} | Absorb:{f.get('absorption_flag', False)} | Exhaust:{f.get('exhaustion_flag', False)}")
    
    print(f"\n  {UNDERLINE}LIQUIDITY{RESET}")
    l5_total = f.get('l5_depth_bid', 0) + f.get('l5_depth_ask', 0)
    print(f"    L5:{l5_total:.2f} | Imbal:{f.get('depth_imbalance', 0):+.2f} | Void:{f.get('liquidity_void_flag', False)} | Stack:{f.get('stacked_imbalance_nearby', False)}")
    
    print(f"\n  {UNDERLINE}DERIVS{RESET}")
    print(f"    OI:{f.get('oi_change_pct', 0)*100:+.3f}% | Fund:{f.get('funding_rate_now', 0)*100:.4f}% | LiqDens:{f.get('liquidation_density', 0):.2f}")
    
    print(f"\n  {UNDERLINE}SESSION{RESET}")
    print(f"    {f.get('session_label', 'N/A').upper()} | VWAPdist:{f.get('session_vwap_distance', 0)*100:+.3f}% | VWAPscore:{f.get('vwap_mean_reversion_score', 0):.0f} | MinsIn:{f.get('minutes_into_session', 0)}")
    
    if w.market_state:
        print(f"\n  {UNDERLINE}MARKET STATE{RESET}")
        print(f"    {BOLD}{w.market_state}{RESET}({w.market_state_conf:.2f}) — {', '.join(w.market_state_evidence)}")
    
    if w.missing_flags:
        print(f"\n  {RED}⚠ MISSING: {', '.join(w.missing_flags)}{RESET}")
    
    print(f"\n  {GRAY}[J] Full JSON | [ESC] Back{RESET}")


def render_json_view(w: WickData):
    print(f"\n{GRAY}{'─'*80}{RESET}")
    print(f"{BOLD}  RAW JSON{RESET}")
    print(f"{GRAY}{'─'*80}{RESET}")
    print(json.dumps(w.raw, indent=2)[:3000])
    print(f"\n  {GRAY}[ESC] Back{RESET}")


# ==================== MAIN ====================

def main():
    selected_row = -1
    show_drilldown = False
    show_json = False
    selected_wick = None
    
    while True:
        clear()
        
        status = load_status()
        wicks = load_recent_wicks(100)
        jsonl_path = get_latest_jsonl()
        ob_cache = load_orderbook_cache()
        
        now = datetime.now().strftime('%H:%M:%S')
        
        # Header
        print(f"{BOLD}{WHITE}╔{'═'*118}╗{RESET}")
        print(f"{BOLD}{WHITE}║  ALPHA v2.1  │  {now}  │  {BTC_COLOR}■BTC{RESET} {ETH_COLOR}■ETH{RESET} {SOL_COLOR}■SOL{RESET}  │  [↑↓]Select [ENTER]Drill [Q]Quit                        {WHITE}║{RESET}")
        print(f"{BOLD}{WHITE}╚{'═'*118}╝{RESET}")
        
        render_health_strip(status, jsonl_path, wicks)
        
        if show_json and selected_wick:
            render_json_view(selected_wick)
        elif show_drilldown and selected_wick:
            render_drilldown(selected_wick)
        else:
            print(f"\n{BOLD}  NOW CARDS{RESET}")
            for sym in ['BTC-USDT', 'ETH-USDT', 'SOL-USDT']:
                render_ticker_card(sym, wicks, ob_cache)
            
            render_attention_feed(wicks, selected_row)
        
        print(f"\n{GRAY}  Data: {jsonl_path.name if jsonl_path else 'N/A'} | Refresh: 2s{RESET}")
        
        # Input
        start_time = time.time()
        while time.time() - start_time < 2:
            if msvcrt.kbhit():
                key = msvcrt.getch()
                
                if key in (b'q', b'Q'):
                    print(f"\n{YELLOW}Exiting...{RESET}")
                    return
                
                elif key == b'\x1b':
                    show_drilldown = show_json = False
                    selected_wick = None
                
                elif key in (b'j', b'J'):
                    if show_drilldown and selected_wick:
                        show_json = True
                
                elif key == b'\r':
                    if selected_row >= 0:
                        sorted_wicks = sorted(wicks, key=lambda w: w.attention_score, reverse=True)[:20]
                        if selected_row < len(sorted_wicks):
                            selected_wick = sorted_wicks[selected_row]
                            show_drilldown = True
                            show_json = False
                
                elif key in (b'H', b'\x00', b'\xe0'):
                    key2 = msvcrt.getch()
                    if key2 == b'H':
                        selected_row = max(0, selected_row - 1)
                    elif key2 == b'P':
                        sorted_wicks = sorted(wicks, key=lambda w: w.attention_score, reverse=True)[:20]
                        selected_row = min(len(sorted_wicks) - 1, selected_row + 1)
                
                break
            
            time.sleep(0.05)


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print(f"\n\n{YELLOW}Exiting...{RESET}")

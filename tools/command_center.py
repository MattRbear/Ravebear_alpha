#!/usr/bin/env python3
"""
ALPHA COMMAND CENTER - Full Feature Dashboard (Color Coded)
============================================================
BTC = CYAN  |  ETH = MAGENTA  |  SOL = YELLOW

Author: Flint for RaveBear
"""

import os
import sys
import json
import time
import glob
from datetime import datetime, timezone
from pathlib import Path

# Project paths
SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent
DATA_DIR = PROJECT_ROOT / "data"
STATUS_FILE = DATA_DIR / "engine_status.json"

# ==================== ANSI COLORS ====================
RESET = "\033[0m"
BOLD = "\033[1m"
DIM = "\033[2m"

# Symbol Colors (Bright versions for visibility)
BTC_COLOR = "\033[96m"   # Bright Cyan
ETH_COLOR = "\033[95m"   # Bright Magenta  
SOL_COLOR = "\033[93m"   # Bright Yellow

# Status Colors
GREEN = "\033[92m"
RED = "\033[91m"
YELLOW = "\033[93m"
WHITE = "\033[97m"
GRAY = "\033[90m"

# Symbol color map
SYMBOL_COLORS = {
    "BTC-USDT": BTC_COLOR,
    "ETH-USDT": ETH_COLOR,
    "SOL-USDT": SOL_COLOR,
}

def get_color(symbol):
    return SYMBOL_COLORS.get(symbol, WHITE)

def clear():
    os.system('cls' if os.name == 'nt' else 'clear')

def get_latest_jsonl():
    """Get the most recent wick events file."""
    pattern = str(DATA_DIR / "wick_events_*.jsonl")
    files = glob.glob(pattern)
    if not files:
        return None
    return max(files, key=os.path.getmtime)

def load_recent_wicks(n=50):
    """Load the N most recent wick events."""
    jsonl_path = get_latest_jsonl()
    if not jsonl_path:
        return []
    
    wicks = []
    try:
        with open(jsonl_path, 'r') as f:
            lines = f.readlines()
            for line in lines[-n:]:
                try:
                    wicks.append(json.loads(line.strip()))
                except:
                    pass
    except:
        pass
    
    return wicks

def load_status():
    """Load engine status."""
    try:
        with open(STATUS_FILE, 'r') as f:
            return json.load(f)
    except:
        return {}

def format_side(side):
    """Format wick side."""
    if side == "upper":
        return "^UP^"
    else:
        return "vDNv"

def feed_health_str(age):
    """Color-coded feed health."""
    if age < 30:
        return f"{GREEN}OK({age}s){RESET}"
    elif age < 120:
        return f"{YELLOW}SLOW({age}s){RESET}"
    else:
        return f"{RED}STALE({age}s){RESET}"

def display():
    """Main display loop."""
    while True:
        clear()
        
        status = load_status()
        wicks = load_recent_wicks(50)
        
        now = datetime.now().strftime('%H:%M:%S')
        
        # ==================== HEADER ====================
        print(f"{BOLD}{WHITE}{'='*100}{RESET}")
        print(f"{BOLD}{WHITE}  ALPHA COMMAND CENTER  |  {now}  |  {BTC_COLOR}■ BTC{RESET}  {ETH_COLOR}■ ETH{RESET}  {SOL_COLOR}■ SOL{RESET}  |  Ctrl+C to exit")
        print(f"{BOLD}{WHITE}{'='*100}{RESET}")
        
        # ==================== FEED HEALTH ====================
        feed_ages = status.get('feed_age', {})
        running = status.get('running', False)
        uptime = status.get('uptime_seconds', 0)
        uptime_str = time.strftime("%H:%M:%S", time.gmtime(uptime))
        wicks_total = status.get('wicks_detected', 0)
        
        status_str = f"{GREEN}RUNNING{RESET}" if running else f"{RED}STOPPED{RESET}"
        
        feeds_str = ""
        for feed in ['trades', 'orderbook', 'derivs', 'macro', 'whale']:
            age = feed_ages.get(feed, 999)
            feeds_str += f"{feed.upper()}:{feed_health_str(age)}  "
        
        print(f"\n  {status_str} | Uptime: {uptime_str} | Wicks: {wicks_total}")
        print(f"  {feeds_str}")
        
        # ==================== GROUP WICKS BY SYMBOL ====================
        by_symbol = {"BTC-USDT": [], "ETH-USDT": [], "SOL-USDT": []}
        for w in wicks:
            sym = w.get('symbol', 'UNKNOWN')
            if sym in by_symbol:
                by_symbol[sym].append(w)
        
        # ==================== SYMBOL PANELS (Side by Side Style) ====================
        print(f"\n{WHITE}{'─'*100}{RESET}")
        print(f"{BOLD}  LATEST BY SYMBOL{RESET}")
        print(f"{WHITE}{'─'*100}{RESET}")
        
        for symbol in ['BTC-USDT', 'ETH-USDT', 'SOL-USDT']:
            color = get_color(symbol)
            symbol_wicks = by_symbol.get(symbol, [])
            
            if not symbol_wicks:
                print(f"\n  {color}{BOLD}{symbol}{RESET} {GRAY}-- No data --{RESET}")
                continue
            
            latest = symbol_wicks[-1]
            f = latest.get('features', {})
            
            # Extract key data
            side = format_side(latest.get('wick_side', '?'))
            wick_high = latest.get('wick_high', 0)
            wick_low = latest.get('wick_low', 0)
            
            wick_ratio = f.get('wick_to_body_ratio', 0)
            rejection_vel = f.get('rejection_velocity', 0)
            delta = f.get('delta_at_wick', 0)
            cvd_slope = f.get('cvd_slope_10', 0)
            depth_imbal = f.get('depth_imbalance', 0)
            oi_change = f.get('oi_change_pct', 0) * 100
            funding = f.get('funding_rate_now', 0) * 100
            vwap_dist = f.get('session_vwap_distance', 0) * 100
            vwap_score = f.get('vwap_mean_reversion_score', 0)
            session = f.get('session_label', 'none')
            liq_void = f.get('liquidity_void_flag', False)
            stacked = f.get('stacked_imbalance_nearby', False)
            
            # Timestamp
            ts = latest.get('ts', '')
            try:
                t_obj = datetime.fromisoformat(ts.replace('Z', '+00:00'))
                time_str = t_obj.strftime("%H:%M:%S")
            except:
                time_str = "?"
            
            # Format price
            if 'BTC' in symbol:
                price_high = f"${wick_high:,.0f}"
                price_low = f"${wick_low:,.0f}"
            else:
                price_high = f"${wick_high:,.2f}"
                price_low = f"${wick_low:,.2f}"
            
            # Flags
            void_str = f"{GREEN}YES{RESET}" if liq_void else f"{GRAY}no{RESET}"
            stack_str = f"{GREEN}YES{RESET}" if stacked else f"{GRAY}no{RESET}"
            
            # VWAP score color
            if vwap_score >= 70:
                vwap_color = RED
            elif vwap_score >= 40:
                vwap_color = YELLOW
            else:
                vwap_color = GREEN
            
            # Delta color
            delta_color = GREEN if delta > 0 else RED if delta < 0 else WHITE
            
            print(f"\n  {color}{BOLD}━━━ {symbol} @ {time_str} ━━━{RESET}")
            print(f"  {color}{side}{RESET}  High: {color}{price_high}{RESET}  |  Low: {color}{price_low}{RESET}")
            print(f"  {color}W:B Ratio:{RESET} {wick_ratio:<6.2f}  {color}Reject Vel:{RESET} {rejection_vel:.4f}")
            print(f"  {color}Delta:{RESET} {delta_color}{delta:<+8.1f}{RESET}  {color}CVD Slope:{RESET} {cvd_slope:<+8.2f}  {color}Depth:{RESET} {depth_imbal:<+.2f}")
            print(f"  {color}OI:{RESET} {oi_change:<+.3f}%  {color}Funding:{RESET} {funding:.4f}%  {color}Session:{RESET} {session.upper()}")
            print(f"  {color}VWAP Dist:{RESET} {vwap_dist:<+.3f}%  {color}VWAP Score:{RESET} {vwap_color}{vwap_score:<.1f}{RESET}  {color}Void:{RESET} {void_str}  {color}Stacked:{RESET} {stack_str}")
        
        # ==================== WICK HISTORY TABLE ====================
        print(f"\n{WHITE}{'─'*100}{RESET}")
        print(f"{BOLD}  RECENT WICK HISTORY (Last 20){RESET}")
        print(f"{WHITE}{'─'*100}{RESET}")
        print(f"  {GRAY}{'TIME':<10} {'SYMBOL':<12} {'SIDE':<6} {'PRICE':<12} {'W:B':<8} {'DELTA':<10} {'OI%':<10} {'VWAP':<8}{RESET}")
        print(f"  {GRAY}{'─'*80}{RESET}")
        
        for w in wicks[-20:]:
            f = w.get('features', {})
            sym = w.get('symbol', '?')
            color = get_color(sym)
            
            ts = w.get('ts', '')
            try:
                t_obj = datetime.fromisoformat(ts.replace('Z', '+00:00'))
                time_str = t_obj.strftime("%H:%M:%S")
            except:
                time_str = "?"
            
            side = "UP" if w.get('wick_side') == 'upper' else "DN"
            price = w.get('wick_high', 0) if side == "UP" else w.get('wick_low', 0)
            wb_ratio = f.get('wick_to_body_ratio', 0)
            delta = f.get('delta_at_wick', 0)
            oi_pct = f.get('oi_change_pct', 0) * 100
            vwap_s = f.get('vwap_mean_reversion_score', 0)
            
            # Format price
            if 'BTC' in sym:
                price_str = f"${price:,.0f}"
            else:
                price_str = f"${price:,.2f}"
            
            # Highlight high ratio wicks
            if wb_ratio >= 1.5:
                ratio_str = f"{BOLD}{wb_ratio:<8.2f}{RESET}"
            else:
                ratio_str = f"{wb_ratio:<8.2f}"
            
            print(f"  {color}{time_str:<10} {sym:<12} {side:<6} {price_str:<12} {ratio_str} {delta:<+10.1f} {oi_pct:<+10.3f} {vwap_s:<8.1f}{RESET}")
        
        # ==================== HIGH VALUE WICKS ====================
        high_ratio_wicks = [w for w in wicks if w.get('features', {}).get('wick_to_body_ratio', 0) >= 1.5]
        
        print(f"\n{WHITE}{'─'*100}{RESET}")
        print(f"{BOLD}  HIGH VALUE WICKS (W:B >= 1.5): {len(high_ratio_wicks)} found{RESET}")
        print(f"{WHITE}{'─'*100}{RESET}")
        
        if high_ratio_wicks:
            for w in high_ratio_wicks[-8:]:
                f = w.get('features', {})
                sym = w.get('symbol', '?')
                color = get_color(sym)
                
                side = "UP" if w.get('wick_side') == 'upper' else "DN"
                price = w.get('wick_high', 0) if side == "UP" else w.get('wick_low', 0)
                wb_ratio = f.get('wick_to_body_ratio', 0)
                liq_void = f.get('liquidity_void_flag', False)
                stacked = f.get('stacked_imbalance_nearby', False)
                
                # Format price
                if 'BTC' in sym:
                    price_str = f"${price:,.0f}"
                else:
                    price_str = f"${price:,.2f}"
                
                # Flags
                flags = []
                if liq_void:
                    flags.append(f"{RED}LIQ_VOID{RESET}")
                if stacked:
                    flags.append(f"{YELLOW}STACKED{RESET}")
                flags_str = " ".join(flags) if flags else f"{GRAY}--{RESET}"
                
                print(f"  {color}{sym} {side} @ {price_str:<12}{RESET}  |  Ratio: {BOLD}{wb_ratio:<6.2f}{RESET}  |  {flags_str}")
        else:
            print(f"  {GRAY}No high-value wicks in recent data{RESET}")
        
        # ==================== FOOTER ====================
        print(f"\n{WHITE}{'='*100}{RESET}")
        data_file = get_latest_jsonl()
        if data_file:
            print(f"  {GRAY}Data: {data_file}{RESET}")
        print(f"{WHITE}{'='*100}{RESET}")
        
        time.sleep(2)


if __name__ == '__main__':
    try:
        display()
    except KeyboardInterrupt:
        print(f"\n\n{YELLOW}Exiting...{RESET}")

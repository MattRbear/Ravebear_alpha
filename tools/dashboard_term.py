#!/usr/bin/env python3
"""
ALPHA Terminal Dashboard
"""
import sys
import json
import time
import os
import shutil
from datetime import datetime

# ANSI Colors
C_RESET = "\033[0m"
C_RED = "\033[31m"
C_GREEN = "\033[32m"
C_YELLOW = "\033[33m"
C_CYAN = "\033[36m"
C_MAGENTA = "\033[35m"
C_WHITE = "\033[37m"
C_BOLD = "\033[1m"

# Determine absolute path to data/engine_status.json
# It is located in ../data/engine_status.json relative to this script
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
STATUS_FILE = os.path.join(PROJECT_ROOT, "data", "engine_status.json")

def clear_screen():
    os.system('cls' if os.name == 'nt' else 'clear')

def get_color_for_age(age_sec, thresh_warn=60, thresh_crit=300):
    if age_sec > thresh_crit: return C_RED
    if age_sec > thresh_warn: return C_YELLOW
    return C_GREEN

def get_color_for_score(score):
    if score >= 60: return C_GREEN
    if score >= 40: return C_YELLOW
    return C_RED

def draw_dashboard():
    while True:
        try:
            with open(STATUS_FILE, 'r') as f:
                data = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            print(f"Waiting for engine status at: {STATUS_FILE}")
            time.sleep(1)
            continue

        clear_screen()
        
        ts = data.get("timestamp", "N/A")
        uptime = data.get("uptime_seconds", 0)
        uptime_str = time.strftime("%H:%M:%S", time.gmtime(uptime))
        
        print(f"{C_BOLD}{C_CYAN}╔══════════════════════════════════════════════════════════════════════════════╗{C_RESET}")
        print(f"{C_BOLD}{C_CYAN}║ ALPHA WICK ENGINE - SYSTEM MONITOR                                           ║{C_RESET}")
        print(f"{C_BOLD}{C_CYAN}╚══════════════════════════════════════════════════════════════════════════════╝{C_RESET}")
        
        # ZONE 1: ENGINE CORE
        running = data.get("running", False)
        status_col = C_GREEN if running else C_RED
        status_txt = "RUNNING" if running else "STOPPED"
        wicks = data.get("wicks_detected", 0)
        alerts = data.get("alerts_sent", 0)
        
        print(f"\n{C_WHITE}--- [ ENGINE CORE ] ---")
        print(f"Status: {status_col}{status_txt}{C_RESET} | Uptime: {uptime_str} | Last Update: {ts}")
        print(f"Wicks Detected: {C_BOLD}{wicks}{C_RESET} | Alerts Sent: {C_BOLD}{alerts}{C_RESET}")

        # ZONE 2: ALERT PIPELINE
        enabled = data.get("discord_enabled", False)
        min_ratio = data.get("wick_min_ratio", 0.0)
        last_err = data.get("last_alert_error", "None")
        webhooks = data.get("webhooks_configured", [])
        
        en_col = C_GREEN if enabled else C_YELLOW
        err_col = C_GREEN if last_err == "None" else C_RED
        
        print(f"\n{C_WHITE}--- [ ALERT PIPELINE ] ---")
        print(f"Discord Enabled: {en_col}{enabled}{C_RESET} | Min Ratio: {min_ratio}")
        print(f"Webhooks: {len(webhooks)} configured | Last Error: {err_col}{last_err}{C_RESET}")

        # ZONE 3: FEED HEALTH
        ages = data.get("feed_age", {})
        print(f"\n{C_WHITE}--- [ FEED HEALTH (Latency) ] ---{C_RESET}")
        
        line = ""
        for feed, age in ages.items():
            col = get_color_for_age(age)
            line += f"{feed.upper()}: {col}{age}s{C_RESET}  "
        print(line)

        # ZONE 4: SYMBOL SNAPSHOTS
        snapshots = data.get("symbol_snapshots", {})
        print(f"\n{C_WHITE}--- [ SYMBOL SNAPSHOTS ] ---{C_RESET}")
        print(f"{ 'SYMBOL':<12} {'LAST WICK':<10} {'SCORE':<10} {'UPDATED'}")
        print("-" * 60)
        
        for sym, snap in snapshots.items():
            side = snap.get("last_wick_side", "-").upper()
            score = snap.get("last_score", 0)
            score_col = get_color_for_score(score)
            
            # Parse TS just to show time part
            last_ts = snap.get("last_candle_ts", "")
            try:
                t_obj = datetime.fromisoformat(last_ts)
                time_str = t_obj.strftime("%H:%M:%S")
            except:
                time_str = last_ts

            print(f"{sym:<12} {side:<10} {score_col}{score:<10.1f}{C_RESET} {time_str}")

        print(f"\n{C_CYAN}Press Ctrl+C to exit dashboard.{C_RESET}")
        time.sleep(1)

if __name__ == "__main__":
    try:
        draw_dashboard()
    except KeyboardInterrupt:
        print("\nExiting.")

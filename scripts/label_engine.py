# scripts/label_engine.py
import argparse
import asyncio
import json
import logging
import math
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import httpx
from dateutil import parser as date_parser

# Setup basic logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S"
)
logger = logging.getLogger("label_engine")

# --- Constants ---
LOOKAHEAD_30M = 30
LOOKAHEAD_1H = 60
LOOKAHEAD_4H = 240
TOUCH_BPS = 2.0  # 0.02%
MIN_BARS_4H = LOOKAHEAD_4H

# --- Models ---

@dataclass
class CandleBar:
    start_ts: datetime
    end_ts: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float

class CandleSeries:
    def __init__(self, symbol: str, bars: List[CandleBar]):
        self.symbol = symbol
        # Ensure bars are sorted by start_ts
        self.bars = sorted(bars, key=lambda b: b.start_ts)

    def slice_between(self, start_ts: datetime, end_ts: datetime) -> List[CandleBar]:
        """Returns bars that overlap with [start_ts, end_ts]."""
        # Overlap means: bar_start <= req_end AND bar_end >= req_start.
        # But commonly we just want bars where start_ts >= req_start and start_ts < req_end
        # or similar. The spec says "overlap (start_ts >= start_ts_event and end_ts <= end_ts_event)".
        # Wait, spec says: "returns bars whose [start_ts, end_ts] overlap (start_ts >= start_ts_event and end_ts <= end_ts_event)"
        # Actually standard definition for "future candles" from event_ts is usually:
        # candle.start_ts >= event_ts.
        # Let's align with spec: "slice_between(event_ts, event_ts + 4h)"
        # And "returns bars whose [start_ts, end_ts] overlap"
        # Since these are 1m candles, we likely want every candle that starts inside the window.
        
        result = []
        for b in self.bars:
            # We want future bars starting from event_ts up to event_ts + 4h
            # The wick event happens AT event_ts. 
            # If event_ts is 10:00, the candle 10:00-10:01 is technically "now" or "just finished"?
            # Main collector uses `closed_candle.end_ts` as event_ts.
            # So if event_ts is 10:01 (close of 10:00 bar), the NEXT bar is 10:01-10:02.
            # So we want bars where start_ts >= start_ts argument.
            if b.start_ts >= start_ts and b.end_ts <= end_ts:
                result.append(b)
        return result

# --- Data Source ---

class OkxRestClient:
    TYPE_CASTS = {
        "ts": int, "o": float, "h": float, "l": float, "c": float, "vol": float
    }

    def __init__(self):
        self.base_url = "https://www.okx.com"
        self.client = httpx.AsyncClient(timeout=30.0)

    async def close(self):
        await self.client.aclose()

    async def fetch_1m_candles(self, symbol: str, start_ts: datetime, end_ts: datetime) -> List[CandleBar]:
        """
        Fetch 1m candles for the given range using OKX API.
        Handles pagination/batching if needed (OKX usually returns 100-300 limit).
        OKX API: GET /api/v5/market/history-candles (for older) or candles (for recent).
        We'll use history-candles to be safe for offline labeling of past data.
        """
        bars = []
        # OKX timestamps are milliseconds
        end_ms = int(end_ts.timestamp() * 1000)
        start_ms = int(start_ts.timestamp() * 1000)
        
        # Pagination loop: query reverse from end_ts down to start_ts
        current_after = end_ms
        
        # Safety break
        limit = 100
        while True:
            params = {
                "instId": symbol,
                "bar": "1m",
                "after": str(current_after + 60000), # OKX 'after' is exclusive, older data. Wait.
                # OKX pagination: 'after' asks for records OLDER than this id.
                # 'before' asks for records NEWER than this id.
                # If we scan backwards:
                # pass after = end_ts.
                # But we want to include end_ts? 
                # Let's use `after` descending.
            }
            # OKX: "If you try to retrieve data more than 1 month ago, use /history-candles"
            # We will try history-candles first.
            url = f"{self.base_url}/api/v5/market/history-candles"
            
            # Adjustment: start with the latest time we need, go backwards
            # current_after is the timestamp we want to start looking BEFORE.
            # initial current_after should be end_ms + small buffer to include end_ms?
            # Or just pass 'after' as end_ms to get candles strictly before end_ms?
            # Spec says "slice_between(start, end)".
            
            # Simplified logic:
            # We want candles >= start_ms.
            # We request from end_ms backwards.
            # Page loop.
            pass

            # Actually, let's just implement a robust fetcher.
            # For this strictly defined task, I'll assume we iterate moving 'after' pointer.
            if len(bars) > 0:
                # update pointer to the oldest bar start_ts
                oldest = bars[-1].start_ts
                current_after = int(oldest.timestamp() * 1000)
            else:
                 # First request, start from end_ts
                 current_after = end_ms + 60000 # To include end_ts bar if exists? 
                 # Candle timestamps are open times.
                 
            # Note: OKX API `after` returns candles with ts < after.
            try:
                resp = await self.client.get(url, params={
                    "instId": symbol,
                    "bar": "1m",
                    "after": str(current_after),
                    "limit": "100"
                })
                resp.raise_for_status()
                data = resp.json()
                if data["code"] != "0":
                    logger.error(f"OKX API error: {data}")
                    break
                
                raw_candles = data["data"]
                if not raw_candles:
                    break
                
                # Parse
                batch = []
                for r in raw_candles:
                    # r: [ts, o, h, l, c, vol, volCcy, volCcyQuote, confirm]
                    ts = int(r[0])
                    if ts < start_ms:
                        continue # Too old
                        
                    dt_start = datetime.fromtimestamp(ts / 1000.0, tz=timezone.utc)
                    # 1m candle end is +60s
                    dt_end = dt_start + timedelta(seconds=60)
                    
                    b = CandleBar(
                        start_ts=dt_start,
                        end_ts=dt_end,
                        open=float(r[1]),
                        high=float(r[2]),
                        low=float(r[3]),
                        close=float(r[4]),
                        volume=float(r[5])
                    )
                    batch.append(b)
                
                if not batch:
                    # We got data but all were older than start_ms
                    break
                
                bars.extend(batch)
                
                # If we got fewer than limit, we are likely done (but history endpoint is specific).
                # Main stop condition: the last candle in raw_candles is older than start_ms?
                last_ts = int(raw_candles[-1][0])
                if last_ts < start_ms:
                    break
                
                # Sleep to respect rate limits
                await asyncio.sleep(0.1)
                
            except Exception as e:
                logger.error(f"Fetch error: {e}")
                await asyncio.sleep(1)
                break
                
        # Dedupe and sort
        # Sometimes pagination overlaps
        unique = {b.start_ts: b for b in bars}
        sorted_bars = sorted(unique.values(), key=lambda x: x.start_ts)
        return sorted_bars

# --- Logic ---

def compute_labels_for_event(
    ev_meta: dict, 
    wick_price: float, 
    wick_side: str, 
    series: CandleSeries
) -> Dict[str, Any]:
    
    # 1. Parse Event TS
    try:
        # Support ISO strings like "2025-12-05T22:04:00Z"
        # or main_collector which uses isoformat()
        ts_val = ev_meta.get("event_ts") or ev_meta.get("ts")
        if isinstance(ts_val, str):
            event_ts = date_parser.parse(ts_val)
        elif isinstance(ts_val, (int, float)):
             event_ts = datetime.fromtimestamp(ts_val, tz=timezone.utc)
        else:
            return {}
            
        # Ensure UTC
        if event_ts.tzinfo is None:
            event_ts = event_ts.replace(tzinfo=timezone.utc)
            
    except Exception:
        return {}

    # 2. Slice future candles
    # slice_between(event_ts, event_ts + 4h)
    target_end = event_ts + timedelta(minutes=LOOKAHEAD_4H)
    
    # We want candles that start >= event_ts (the next 4 hours)
    # If the wick happened at 10:00:00 (candle close), the next candle opens at 10:00:00?
    # No, if 10:00 candle closes, it covers 09:59-10:00 or 10:00-10:01? 
    # Usually "candle H" is 10:00 open. 
    # If event_ts is close time, we want candles starting from event_ts?
    # Let's assume strict "future": candles starting >= event_ts.
    future_bars = series.slice_between(event_ts, target_end)
    
    if len(future_bars) < MIN_BARS_4H:
        # Check if we are asking too much (e.g. data ends).
        # Spec: If len(candles_future) < MIN_BARS_4H: SKIP
        return {}
        
    tol_abs = wick_price * (TOUCH_BPS / 10000.0)
    
    # --- Untouched Flags ---
    
    def check_touched(bars_subset: List[CandleBar]) -> bool:
        for c in bars_subset:
            if wick_side == "upper":
                if c.high >= wick_price - tol_abs:
                    return True
            else: # lower
                if c.low <= wick_price + tol_abs:
                    return True
        return False

    untouched_30m = not check_touched(future_bars[:LOOKAHEAD_30M])
    untouched_1h = not check_touched(future_bars[:LOOKAHEAD_1H])
    untouched_4h = not check_touched(future_bars[:LOOKAHEAD_4H])
    
    # --- Hold Duration ---
    # Time until first touch, capped at 4h (240.0)
    hold_duration = float(LOOKAHEAD_4H)
    for i, c in enumerate(future_bars, start=1):
        is_touch = False
        if wick_side == "upper":
            if c.high >= wick_price - tol_abs:
                is_touch = True
        else:
            if c.low <= wick_price + tol_abs:
                is_touch = True
        
        if is_touch:
            hold_duration = float(i)
            break

    # --- MAE / MFE ---
    # max adverse/favorable excursion %
    
    max_adv = 0.0
    max_fav = 0.0
    
    for c in future_bars:
        if wick_side == "upper":
            # Short logic
            adv = max(0.0, c.high - wick_price)
            fav = max(0.0, wick_price - c.low)
        else:
            # Long logic
            adv = max(0.0, wick_price - c.low)
            fav = max(0.0, c.high - wick_price)
            
        if adv > max_adv: max_adv = adv
        if fav > max_fav: max_fav = fav
        
    mae = max_adv / wick_price if wick_price > 0 else 0.0
    mfe = max_fav / wick_price if wick_price > 0 else 0.0
    
    # --- Distance Moved ---
    # At 4h horizon (last bar)
    last_bar = future_bars[-1]
    close_end = last_bar.close
    distance_moved = 0.0
    if wick_price > 0:
        distance_moved = (close_end - wick_price) / wick_price
        
    return {
        "untouched_30m": untouched_30m,
        "untouched_1h": untouched_1h,
        "untouched_4h": untouched_4h,
        "hold_duration": hold_duration,
        "mae": mae,
        "mfe": mfe,
        "distance_moved": distance_moved
    }

# --- IO ---

def load_events_from_paths(paths: List[str]) -> List[Tuple[Path, Dict]]:
    results = []
    for item in paths:
        p = Path(item)
        files = []
        if any(ch in item for ch in "*?[]"):
            files.extend(Path().glob(item))
        elif p.is_dir():
            files.extend(sorted(p.glob("*.jsonl")))
        elif p.is_file():
            files.append(p)
            
        for fpath in files:
            if not fpath.is_file(): continue
            try:
                with fpath.open("r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if not line: continue
                        try:
                            obj = json.loads(line)
                            results.append((fpath, obj))
                        except json.JSONDecodeError:
                            continue
            except Exception as e:
                logger.warning(f"Error reading {fpath}: {e}")
    return results

# --- Main Pipeline ---

async def run_pipeline(input_paths: List[str], out_dir: str):
    # 1. Load Events
    all_events = load_events_from_paths(input_paths)
    if not all_events:
        logger.warning("No events found.")
        return
    logger.info(f"Loaded {len(all_events)} events.")

    # 2. Group by Symbol
    by_symbol = {}
    for src, obj in all_events:
        # Resolve meta
        clean = obj.get("event", obj)
        sym = clean.get("symbol")
        if not sym: continue
        
        # Parse ts to find range
        ts_val = clean.get("event_ts") or clean.get("ts")
        if not ts_val: continue
        
        if isinstance(ts_val, str):
            ts = date_parser.parse(ts_val)
        else:
            ts = datetime.fromtimestamp(ts_val, tz=timezone.utc)
        if ts.tzinfo is None: ts = ts.replace(tzinfo=timezone.utc)
            
        if sym not in by_symbol:
            by_symbol[sym] = []
        by_symbol[sym].append(ts)

    # 3. Fetch Candles & Build Series
    series_map = {}
    client = OkxRestClient()
    
    try:
        for sym, times in by_symbol.items():
            if not times: continue
            min_ts = min(times)
            max_ts = max(times)
            # extend max by 4h
            fetch_end = max_ts + timedelta(minutes=LOOKAHEAD_4H + 60) # buffer
            
            logger.info(f"Fetching candles for {sym}: {min_ts} -> {fetch_end}")
            bars = await client.fetch_1m_candles(sym, min_ts, fetch_end)
            logger.info(f"Fetched {len(bars)} bars for {sym}")
            
            if bars:
                series_map[sym] = CandleSeries(sym, bars)
    finally:
        await client.close()

    # 4. Label & Write
    # Group output by source file to mirror structure
    # out_dir / filename_labeled.jsonl
    
    Path(out_dir).mkdir(parents=True, exist_ok=True)
    
    # Sort events by source path for sequential writing
    all_events.sort(key=lambda x: str(x[0]))
    
    current_path = None
    writer = None
    handle = None
    
    stats = {"labeled": 0, "skipped": 0}
    
    for src_path, raw_obj in all_events:
        # Switch file if needed
        if src_path != current_path:
            if handle: handle.close()
            
            current_path = src_path
            fname = src_path.stem + "_labeled.jsonl"
            out_path = Path(out_dir) / fname
            handle = out_path.open("w", encoding="utf-8")
            logger.info(f"Writing to {out_path}")
            
        # Extract fields
        ev = raw_obj.get("event", raw_obj)
        sym = ev.get("symbol")
        side = ev.get("wick_side")
        
        # wick_price logic
        w_high = ev.get("wick_high")
        w_low = ev.get("wick_low")
        w_price = w_high if side == "upper" else w_low
        
        if not sym or not side or w_price is None or sym not in series_map:
            stats["skipped"] += 1
            handle.write(json.dumps(raw_obj) + "\n")
            continue
            
        # Compute
        labels = compute_labels_for_event(ev, float(w_price), side, series_map[sym])
        
        if not labels:
            stats["skipped"] += 1
            handle.write(json.dumps(raw_obj) + "\n")
        else:
            stats["labeled"] += 1
            if "features" not in raw_obj:
                raw_obj["features"] = {}
            raw_obj["features"].update(labels)
            handle.write(json.dumps(raw_obj) + "\n")
            
    if handle:
        handle.close()
        
    logger.info(f"Done. Labeled: {stats['labeled']}, Skipped: {stats['skipped']}")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("paths", nargs="+", help="Input files")
    parser.add_argument("--out-dir", default="data_labeled", help="Output directory")
    args = parser.parse_args()
    
    asyncio.run(run_pipeline(args.paths, args.out_dir))

if __name__ == "__main__":
    main()

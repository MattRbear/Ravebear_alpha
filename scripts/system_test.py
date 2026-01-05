# scripts/system_test.py
import asyncio
import logging
import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
import main_collector
import scripts.label_engine as label_engine
import scripts.feature_report as feature_report

# Setup log
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("system_test")

async def test_collector_startup():
    logger.info("--- Testing Collector Startup (10s run) ---")
    # Wrap main in a task
    task = asyncio.create_task(main_collector.main())
    
    # Let it run for 10s
    await asyncio.sleep(10)
    
    # Cancel
    logger.info("Stopping collector...")
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        logger.info("Collector stopped cleanly.")
    except Exception as e:
        logger.error(f"Collector crashed: {e}")
        
    # Check if data dir exists
    if os.path.exists("data"):
        logger.info("Data directory verified.")
    else:
        logger.error("Data directory missing!")

def generate_dummy_data():
    logger.info("--- Generating Dummy Wick Events ---")
    # Create valid JSONL for label engine
    # Need 'wick_side', 'wick_high', etc.
    # Use a recent timestamp that OKX has data for (e.g. yesterday)
    
    # Use UTC for timestamps
    base_ts = datetime.now(timezone.utc) - timedelta(hours=24)
    
    events = []
    # Event 1: BTC Upper Wick
    events.append({
        "ts": base_ts.isoformat(),
        "symbol": "BTC-USDT",
        "timeframe": "1m",
        "wick_side": "upper",
        "wick_high": 95000.0,
        "wick_low": 94000.0,
        "features": {
            "wick_size_pct": 0.005
        }
    })
    
    # Event 2: ETH Lower Wick (5 hours ago)
    ts2 = datetime.now(timezone.utc) - timedelta(hours=5)
    events.append({
        "ts": ts2.isoformat(),
        "symbol": "ETH-USDT",
        "timeframe": "1m",
        "wick_side": "lower",
        "wick_high": 3500.0,
        "wick_low": 3400.0,
        "features": {
            "wick_size_pct": 0.005
        }
    })
    
    # Ensure data dir
    Path("data").mkdir(exist_ok=True)
    fpath = Path("data/system_test_input.jsonl")
    
    with fpath.open("w", encoding="utf-8") as f:
        for ev in events:
            f.write(json.dumps(ev) + "\n")
            
    logger.info(f"Written {fpath}")
    return str(fpath)

async def run_label_engine_pipeline(input_path: str):
    logger.info("--- Running Label Engine ---")
    out_dir = "data_labeled"
    await label_engine.run_pipeline([input_path], out_dir)
    
    # Check output
    outfile = Path(out_dir) / "system_test_input_labeled.jsonl"
    if outfile.exists():
        logger.info(f"Label engine produced: {outfile}")
        return str(outfile)
    else:
        logger.error("Label engine failed to produce output.")
        return None

def run_report(input_path: str):
    logger.info("--- Running Feature Report ---")
    # Can't use main() easily because it parses sys.argv
    # Use internal functions
    meta = feature_report.load_schema(Path("schema/schema_v1.json"))
    paths = feature_report.iter_jsonl_paths([input_path])
    stats = feature_report.analyze_files(paths, meta)
    feature_report.print_report(stats)

async def main():
    await test_collector_startup()
    
    # Fake tokens might fail on OKX if they don't exist, but BTC/ETH exist.
    # BUT, the `label_engine` uses `okx_rest_client` which hits REAL API.
    # Does "BTC-USDT" work on OKX Candles?
    # instId for OKX is "BTC-USDT" for spot, or "BTC-USDT-SWAP".
    # User's code (`feeds/okx_trades.py`) uses `BTC-USDT`. I assume Spot.
    
    input_file = generate_dummy_data()
    labeled_file = await run_label_engine_pipeline(input_file)
    
    if labeled_file:
        run_report(labeled_file)

if __name__ == "__main__":
    asyncio.run(main())

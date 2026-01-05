#!/usr/bin/env python3
"""
SMOKE COLLECT - 60 second data collection test
===============================================
Connects to OKX public WebSocket and counts messages for 60 seconds.
Uses ONLY public data (no secrets required).

Usage:
    python scripts/smoke_collect.py
"""

import asyncio
import json
import logging
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

import websockets

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-7s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("smoke_collect")

# Configuration
WS_URL = "wss://ws.okx.com:8443/ws/v5/public"
SYMBOLS = ["BTC-USDT", "ETH-USDT", "SOL-USDT"]
RUN_DURATION_SECS = 60
OUTPUT_DIR = Path("_smoke_out")


class SmokeCollector:
    """Minimal WebSocket data collector for smoke testing."""

    def __init__(self):
        self.message_counts: Dict[str, int] = defaultdict(int)
        self.error_counts: Dict[str, int] = defaultdict(int)
        self.first_message_times: Dict[str, float] = {}
        self.last_message_times: Dict[str, float] = {}
        self.sample_messages: Dict[str, list] = defaultdict(list)
        self.start_time: float = 0
        self.running: bool = False

    async def run(self):
        """Run the smoke test for RUN_DURATION_SECS."""
        self.start_time = time.time()
        self.running = True

        logger.info("=" * 60)
        logger.info("  SMOKE COLLECT - Starting")
        logger.info("=" * 60)
        logger.info(f"WebSocket URL: {WS_URL}")
        logger.info(f"Symbols: {SYMBOLS}")
        logger.info(f"Duration: {RUN_DURATION_SECS} seconds")
        logger.info("=" * 60)

        # Ensure output directory
        OUTPUT_DIR.mkdir(exist_ok=True)

        try:
            # Connect to WebSocket
            logger.info("Connecting to WebSocket...")
            async with websockets.connect(WS_URL) as ws:
                logger.info("Connected!")

                # Subscribe to trades and orderbook
                await self._subscribe(ws, "trades")
                await self._subscribe(ws, "books5")

                # Receive messages until timeout
                deadline = self.start_time + RUN_DURATION_SECS

                while time.time() < deadline and self.running:
                    try:
                        # Wait for message with timeout
                        remaining = deadline - time.time()
                        if remaining <= 0:
                            break

                        msg = await asyncio.wait_for(
                            ws.recv(),
                            timeout=min(remaining, 5.0)
                        )
                        self._process_message(msg)

                    except asyncio.TimeoutError:
                        # No message received, continue
                        continue
                    except websockets.exceptions.ConnectionClosed as e:
                        logger.error(f"Connection closed: {e}")
                        self.error_counts["connection_closed"] += 1
                        break

        except Exception as e:
            logger.error(f"Connection error: {e}")
            self.error_counts["connection_error"] += 1

        # Generate report
        self._generate_report()

    async def _subscribe(self, ws, channel: str):
        """Subscribe to a channel for all symbols."""
        args = [{"channel": channel, "instId": sym} for sym in SYMBOLS]
        msg = json.dumps({"op": "subscribe", "args": args})
        await ws.send(msg)
        logger.info(f"Subscribed to {channel} for {len(SYMBOLS)} symbols")

    def _process_message(self, raw_msg: str):
        """Process a raw WebSocket message."""
        now = time.time()

        try:
            msg = json.loads(raw_msg)

            # Handle subscription responses
            if "event" in msg:
                event = msg["event"]
                if event == "subscribe":
                    channel = msg.get("arg", {}).get("channel", "unknown")
                    self.message_counts[f"sub_{channel}"] += 1
                elif event == "error":
                    logger.warning(f"Subscription error: {msg}")
                    self.error_counts["subscription_error"] += 1
                return

            # Handle data messages
            if "data" in msg:
                arg = msg.get("arg", {})
                channel = arg.get("channel", "unknown")
                inst_id = arg.get("instId", "unknown")
                key = f"{channel}:{inst_id}"

                self.message_counts[key] += 1

                if key not in self.first_message_times:
                    self.first_message_times[key] = now
                self.last_message_times[key] = now

                # Keep sample messages (first 3)
                if len(self.sample_messages[key]) < 3:
                    sample = msg["data"][0] if msg["data"] else msg
                    self.sample_messages[key].append(sample)

        except json.JSONDecodeError:
            self.error_counts["json_decode"] += 1
        except Exception as e:
            self.error_counts["parse_error"] += 1
            logger.debug(f"Parse error: {e}")

    def _generate_report(self):
        """Generate and display the smoke test report."""
        elapsed = time.time() - self.start_time

        logger.info("")
        logger.info("=" * 60)
        logger.info("  SMOKE COLLECT - SUMMARY")
        logger.info("=" * 60)
        logger.info(f"Duration: {elapsed:.1f} seconds")
        logger.info("")

        # Message counts by channel
        logger.info("MESSAGE COUNTS BY CHANNEL:")
        logger.info("-" * 40)

        total_messages = 0
        for key, count in sorted(self.message_counts.items()):
            if not key.startswith("sub_"):
                total_messages += count
                rate = count / elapsed if elapsed > 0 else 0
                logger.info(f"  {key}: {count} msgs ({rate:.1f}/sec)")

        logger.info("-" * 40)
        logger.info(f"  TOTAL: {total_messages} messages")
        logger.info("")

        # Subscription confirmations
        subs = {k: v for k, v in self.message_counts.items() if k.startswith("sub_")}
        if subs:
            logger.info("SUBSCRIPTIONS CONFIRMED:")
            for key, count in subs.items():
                logger.info(f"  {key}: {count}")
            logger.info("")

        # Error counts
        if self.error_counts:
            logger.info("ERRORS:")
            for key, count in self.error_counts.items():
                logger.info(f"  {key}: {count}")
            logger.info("")

        # Status
        if total_messages > 0 and not self.error_counts.get("connection_error"):
            logger.info("STATUS: ✅ PASS - Received data from OKX WebSocket")
        elif total_messages > 0:
            logger.info("STATUS: ⚠️ PARTIAL - Received data but had errors")
        else:
            logger.info("STATUS: ❌ FAIL - No data received")

        logger.info("=" * 60)

        # Write report to file
        has_conn_error = self.error_counts.get("connection_error")
        status = "pass" if total_messages > 0 and not has_conn_error else "fail"
        report = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "duration_secs": elapsed,
            "total_messages": total_messages,
            "message_counts": dict(self.message_counts),
            "error_counts": dict(self.error_counts),
            "sample_messages": {k: v for k, v in self.sample_messages.items()},
            "status": status,
        }

        report_file = OUTPUT_DIR / f"smoke_report_{int(time.time())}.json"
        with open(report_file, "w") as f:
            json.dump(report, f, indent=2, default=str)

        logger.info(f"Report written to: {report_file}")


async def main():
    """Main entry point."""
    collector = SmokeCollector()
    await collector.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Interrupted by user")

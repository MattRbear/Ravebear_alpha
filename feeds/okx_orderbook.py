# file: feeds/okx_orderbook.py
import asyncio
import json
import logging
import random
import websockets
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import AsyncIterator, List, Optional, Tuple

logger = logging.getLogger("feeds.okx_orderbook")

# Reconnect configuration
INITIAL_BACKOFF_SECS = 1.0
MAX_BACKOFF_SECS = 60.0
BACKOFF_MULTIPLIER = 2.0
JITTER_FACTOR = 0.1
MAX_RECONNECT_ATTEMPTS = 10


@dataclass
class OrderBookSnapshot:
    ts: datetime
    symbol: str
    best_bid: float
    best_ask: float
    bids: List[Tuple[float, float]]  # list of (price, size)
    asks: List[Tuple[float, float]]  # list of (price, size)


class OkxOrderBookStream:
    def __init__(self, url: str, symbols: List[str]):
        self.url = url
        self.symbols = symbols
        self.running = False
        self._connection: Optional[websockets.WebSocketClientProtocol] = None
        self._reconnect_attempts = 0
        self._current_backoff = INITIAL_BACKOFF_SECS

    async def connect(self):
        """Establish WebSocket connection and subscribe to books5 channel."""
        logger.info(f"Connecting to {self.url}...")
        self._connection = await websockets.connect(
            self.url,
            ping_interval=20,
            ping_timeout=10,
            close_timeout=5,
        )
        self.running = True
        self._reconnect_attempts = 0
        self._current_backoff = INITIAL_BACKOFF_SECS
        
        # Subscribe to 5-level depth
        args = [{"channel": "books5", "instId": sym} for sym in self.symbols]
        msg = {
            "op": "subscribe",
            "args": args
        }
        await self._connection.send(json.dumps(msg))
        logger.info(f"Subscribed to books5 for {self.symbols}")

    async def _reconnect(self) -> bool:
        """
        Attempt to reconnect with exponential backoff and jitter.
        
        Returns:
            True if reconnection succeeded, False if max attempts reached.
        """
        if self._reconnect_attempts >= MAX_RECONNECT_ATTEMPTS:
            logger.error(f"Max reconnect attempts ({MAX_RECONNECT_ATTEMPTS}) reached")
            return False
        
        self._reconnect_attempts += 1
        
        # Add jitter to prevent thundering herd
        jitter = random.uniform(-JITTER_FACTOR, JITTER_FACTOR) * self._current_backoff
        wait_time = self._current_backoff + jitter
        
        logger.warning(
            f"Reconnecting in {wait_time:.1f}s "
            f"(attempt {self._reconnect_attempts}/{MAX_RECONNECT_ATTEMPTS})"
        )
        
        await asyncio.sleep(wait_time)
        
        try:
            await self.connect()
            logger.info("Reconnection successful")
            return True
        except Exception as e:
            logger.error(f"Reconnection failed: {e}")
            # Increase backoff for next attempt
            self._current_backoff = min(
                self._current_backoff * BACKOFF_MULTIPLIER,
                MAX_BACKOFF_SECS
            )
            return await self._reconnect()  # Recursive retry

    async def stream(self) -> AsyncIterator[OrderBookSnapshot]:
        """Yield OrderBookSnapshot objects from the stream."""
        if not self.running:
            await self.connect()

        while self.running:
            try:
                async for raw_msg in self._connection:
                    if not self.running: 
                        break
                    
                    msg = json.loads(raw_msg)
                    
                    if "event" in msg:
                        continue

                    if "data" in msg:
                        for item in msg["data"]:
                            # OKX format:
                            # bids/asks: [["price", "size", "num_orders", "deprecated"], ...]
                            try:
                                ts_ms = int(item["ts"])
                                bids_raw = item.get("bids", [])
                                asks_raw = item.get("asks", [])
                                
                                bids = []
                                for entry in bids_raw:
                                    price = float(entry[0])
                                    size = float(entry[1])
                                    # Validate: skip invalid entries
                                    if price <= 0 or size < 0:
                                        logger.warning(f"Invalid bid: price={price}, size={size}")
                                        continue
                                    bids.append((price, size))
                                
                                asks = []
                                for entry in asks_raw:
                                    price = float(entry[0])
                                    size = float(entry[1])
                                    # Validate: skip invalid entries
                                    if price <= 0 or size < 0:
                                        logger.warning(f"Invalid ask: price={price}, size={size}")
                                        continue
                                    asks.append((price, size))
                                
                                if not bids or not asks:
                                    continue

                                snapshot = OrderBookSnapshot(
                                    ts=datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc),
                                    symbol=item["instId"],
                                    best_bid=bids[0][0],
                                    best_ask=asks[0][0],
                                    bids=bids,
                                    asks=asks
                                )
                                yield snapshot
                            
                            except KeyError as e:
                                logger.error(f"Missing field in orderbook data: {e} | Item: {item}")
                            except (ValueError, TypeError) as e:
                                logger.error(f"Error parsing orderbook: {e} | Item: {item}")
            
            except websockets.exceptions.ConnectionClosed as e:
                logger.warning(f"Connection closed: {e}")
                if self.running:
                    if not await self._reconnect():
                        self.running = False
                        raise RuntimeError("Failed to reconnect after max attempts")
                else:
                    break
            except Exception as e:
                logger.error(f"Stream error: {e}")
                if self.running:
                    if not await self._reconnect():
                        self.running = False
                        raise
                else:
                    break

    async def close(self):
        self.running = False
        if self._connection:
            await self._connection.close()
            logger.info("Orderbook stream closed.")

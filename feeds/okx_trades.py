# file: feeds/okx_trades.py
import asyncio
import json
import logging
import random
import websockets
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import AsyncIterator, List, Literal, Optional

logger = logging.getLogger("feeds.okx_trades")

# Reconnect configuration
INITIAL_BACKOFF_SECS = 1.0
MAX_BACKOFF_SECS = 60.0
BACKOFF_MULTIPLIER = 2.0
JITTER_FACTOR = 0.1
MAX_RECONNECT_ATTEMPTS = 10


@dataclass
class Trade:
    ts: datetime
    symbol: str
    price: float
    size: float
    side: Literal["buy", "sell"]


class OkxTradeStream:
    def __init__(self, url: str, symbols: List[str]):
        self.url = url
        self.symbols = symbols
        self.running = False
        self._connection: Optional[websockets.WebSocketClientProtocol] = None
        self._reconnect_attempts = 0
        self._current_backoff = INITIAL_BACKOFF_SECS

    async def connect(self):
        """Establish WebSocket connection and subscribe to trades."""
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
        
        # Subscribe
        args = [{"channel": "trades", "instId": sym} for sym in self.symbols]
        msg = {
            "op": "subscribe",
            "args": args
        }
        await self._connection.send(json.dumps(msg))
        logger.info(f"Subscribed to trades for {self.symbols}")

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

    async def stream(self) -> AsyncIterator[Trade]:
        """Yield parsed Trade objects from the stream."""
        if not self.running:
            await self.connect()

        while self.running:
            try:
                async for raw_msg in self._connection:
                    if not self.running: 
                        break
                    
                    msg = json.loads(raw_msg)
                    
                    # Handle initial subscription response or errors
                    if "event" in msg:
                        if msg["event"] == "subscribe":
                            logger.debug(f"Subscription confirmation: {msg}")
                        elif msg["event"] == "error":
                            logger.error(f"Subscription error: {msg}")
                        continue

                    # Parse trade updates
                    if "data" in msg:
                        for item in msg["data"]:
                            # OKX format: {'instId': 'BTC-USDT', 'px': '99000.5', 'sz': '0.01', 'side': 'buy', 'ts': '169...'}
                            try:
                                ts_ms = int(item["ts"])
                                price = float(item["px"])
                                size = float(item["sz"])
                                
                                # Validate data integrity
                                if price <= 0:
                                    logger.warning(f"Invalid price {price} for {item['instId']}")
                                    continue
                                if size <= 0:
                                    logger.warning(f"Invalid size {size} for {item['instId']}")
                                    continue
                                if item["side"] not in ("buy", "sell"):
                                    logger.warning(f"Invalid side {item['side']} for {item['instId']}")
                                    continue
                                
                                yield Trade(
                                    ts=datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc),
                                    symbol=item["instId"],
                                    price=price,
                                    size=size,
                                    side=item["side"]
                                )
                            except KeyError as e:
                                logger.error(f"Missing field in trade data: {e} | Item: {item}")
                            except (ValueError, TypeError) as e:
                                logger.error(f"Error parsing trade: {e} | Item: {item}")
            
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
            logger.info("Trade stream closed.")

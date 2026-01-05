# file: feeds/okx_orderbook.py
import asyncio
import json
import logging
import websockets
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import AsyncIterator, List, Tuple

logger = logging.getLogger("feeds.okx_orderbook")

@dataclass
class OrderBookSnapshot:
    ts: datetime
    symbol: str
    best_bid: float
    best_ask: float
    bids: List[Tuple[float, float]] # list of (price, size)
    asks: List[Tuple[float, float]] # list of (price, size)

class OkxOrderBookStream:
    def __init__(self, url: str, symbols: List[str]):
        self.url = url
        self.symbols = symbols
        self.running = False
        self._connection = None

    async def connect(self):
        """Establish WebSocket connection and subscribe to books5 channel."""
        logger.info(f"Connecting to {self.url}...")
        self._connection = await websockets.connect(self.url)
        self.running = True
        
        # Subscribe to 5-level depth
        args = [{"channel": "books5", "instId": sym} for sym in self.symbols]
        msg = {
            "op": "subscribe",
            "args": args
        }
        await self._connection.send(json.dumps(msg))
        logger.info(f"Subscribed to books5 for {self.symbols}")

    async def stream(self) -> AsyncIterator[OrderBookSnapshot]:
        """Yield OrderBookSnapshot objects from the stream."""
        if not self.running:
            await self.connect()

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
                            
                            bids = [(float(p), float(s)) for p, s, *_ in bids_raw]
                            asks = [(float(p), float(s)) for p, s, *_ in asks_raw]
                            
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
                        
                        except Exception as e:
                            logger.error(f"Error parsing orderbook: {e} | Item: {item}")
        
        except websockets.exceptions.ConnectionClosed as e:
            logger.warning(f"Connection closed: {e}")
            self.running = False
        except Exception as e:
            logger.error(f"Stream error: {e}")
            self.running = False
            raise

    async def close(self):
        self.running = False
        if self._connection:
            await self._connection.close()
            logger.info("Orderbook stream closed.")

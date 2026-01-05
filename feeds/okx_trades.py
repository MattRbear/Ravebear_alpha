# file: feeds/okx_trades.py
import asyncio
import json
import logging
import websockets
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import AsyncIterator, List, Literal

logger = logging.getLogger("feeds.okx_trades")

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
        self._connection = None

    async def connect(self):
        """Establish WebSocket connection and subscribe to trades."""
        logger.info(f"Connecting to {self.url}...")
        self._connection = await websockets.connect(self.url)
        self.running = True
        
        # Subscribe
        args = [{"channel": "trades", "instId": sym} for sym in self.symbols]
        msg = {
            "op": "subscribe",
            "args": args
        }
        await self._connection.send(json.dumps(msg))
        logger.info(f"Subscribed to trades for {self.symbols}")

    async def stream(self) -> AsyncIterator[Trade]:
        """Yield parsed Trade objects from the stream."""
        if not self.running:
            await self.connect()

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
                            yield Trade(
                                ts=datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc),
                                symbol=item["instId"],
                                price=float(item["px"]),
                                size=float(item["sz"]),
                                side=item["side"] # "buy" or "sell"
                            )
                        except Exception as e:
                            logger.error(f"Error parsing trade: {e} | Item: {item}")
        
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
            logger.info("Trade stream closed.")

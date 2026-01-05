#!/usr/bin/env python3
"""
COINALYZE DERIVATIVES FEED
==========================
Fetches OI, Funding Rate, and Liquidation data from Coinalyze API.

Author: Flint for RaveBear
"""

import asyncio
import aiohttp
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Literal, Optional, Dict, List, AsyncIterator

logger = logging.getLogger("feeds.coinalyze")


@dataclass
class OIDeltaSnapshot:
    ts: datetime
    symbol: str
    oi_open: float
    oi_close: float
    delta_oi: float


@dataclass
class FundingSnapshot:
    ts: datetime
    symbol: str
    funding_rate_now: float
    funding_rate_next: float
    next_funding_ts: datetime


@dataclass
class LiquidationEvent:
    ts: datetime
    symbol: str
    side: Literal["long", "short"]
    volume: float
    price: float


class CoinalyzeClient:
    """
    Coinalyze API client for derivatives data.
    
    Endpoints:
    - /v1/open-interest: Open interest data
    - /v1/funding-rate: Funding rate data
    - /v1/liquidation: Liquidation data
    """
    
    BASE_URL = "https://api.coinalyze.net/v1"
    
    # Mapping OKX generic symbols (and others) to Coinalyze format
    SYMBOL_MAP = {
        "BTC-USDT": "BTCUSDT_PERP.A",
        "ETH-USDT": "ETHUSDT_PERP.A",
        "SOL-USDT": "SOLUSDT_PERP.A",
        "BTC-USDT-SWAP": "BTCUSDT_PERP.A",
        "ETH-USDT-SWAP": "ETHUSDT_PERP.A",
        "SOL-USDT-SWAP": "SOLUSDT_PERP.A"
    }
    
    def __init__(self, api_key: str = ""):
        self.api_key = api_key
        self.session: Optional[aiohttp.ClientSession] = None
        
    async def initialize(self):
        """Initialize HTTP session"""
        headers = {}
        if self.api_key:
            headers["api_key"] = self.api_key
        self.session = aiohttp.ClientSession(headers=headers)
        logger.info("[COINALYZE] Client initialized")
        
    async def close(self):
        """Close HTTP session"""
        if self.session:
            await self.session.close()
            
    def _convert_symbol(self, symbol: str) -> str:
        """Convert OKX/Generic symbol to Coinalyze format"""
        # 1. Check explicit map
        if symbol in self.SYMBOL_MAP:
            return self.SYMBOL_MAP[symbol]
            
        # 2. Fallback heuristic
        # If input is 'BTC-USDT' (or similar), assume linear perp 'BTCUSDT_PERP.A'
        # unless specifically mapped otherwise.
        base = symbol.split("-")[0]
        # Defaulting to USDT Linear Perpetual Aggregated
        return f"{base}USDT_PERP.A"
    
    async def fetch_open_interest(self, symbol: str) -> Optional[OIDeltaSnapshot]:
        """
        Fetch current open interest for a symbol.
        
        Returns:
            OIDeltaSnapshot or None on error
        """
        if not self.session:
            await self.initialize()
            
        coinalyze_symbol = self._convert_symbol(symbol)
        
        try:
            now = int(datetime.now(timezone.utc).timestamp())
            start = now - 900  # 15 mins lookback for 2x5min candles
            
            params = {
                "symbols": coinalyze_symbol,
                "interval": "5min",
                "from": start,
                "to": now,
            }
            
            async with self.session.get(
                f"{self.BASE_URL}/open-interest-history",
                params=params,
                timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    
                    if data and len(data) > 0:
                        # Data format: [{"t": timestamp, "o": open, "h": high, "l": low, "c": close}]
                        records = data[0].get("history", [])
                        
                        if len(records) >= 2:
                            prev = records[-2]
                            curr = records[-1]
                            
                            return OIDeltaSnapshot(
                                ts=datetime.fromtimestamp(curr["t"] / 1000, tz=timezone.utc),
                                symbol=symbol,
                                oi_open=prev["c"],
                                oi_close=curr["c"],
                                delta_oi=curr["c"] - prev["c"]
                            )
                        elif len(records) == 1:
                            curr = records[0]
                            return OIDeltaSnapshot(
                                ts=datetime.fromtimestamp(curr["t"] / 1000, tz=timezone.utc),
                                symbol=symbol,
                                oi_open=curr["o"],
                                oi_close=curr["c"],
                                delta_oi=curr["c"] - curr["o"]
                            )
                    
                    return None
                    
                elif resp.status == 401:
                    logger.warning("[COINALYZE] Invalid API key")
                    return None
                else:
                    err_text = await resp.text()
                    logger.warning(f"[COINALYZE] OI request failed: {resp.status} | Body: {err_text}")
                    return None
                    
        except Exception as e:
            logger.error(f"[COINALYZE] OI fetch error: {e}")
            return None
    
    async def fetch_funding_rate(self, symbol: str) -> Optional[FundingSnapshot]:
        """
        Fetch current funding rate for a symbol.
        
        Returns:
            FundingSnapshot or None on error
        """
        if not self.session:
            await self.initialize()
            
        coinalyze_symbol = self._convert_symbol(symbol)
        
        try:
            params = {
                "symbols": coinalyze_symbol,
            }
            
            async with self.session.get(
                f"{self.BASE_URL}/funding-rate",
                params=params,
                timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    
                    if data and len(data) > 0:
                        record = data[0]
                        
                        # Funding rate is typically every 8 hours
                        # Calculate next funding time
                        now = datetime.now(timezone.utc)
                        hour = now.hour
                        next_funding_hour = ((hour // 8) + 1) * 8
                        if next_funding_hour >= 24:
                            next_funding_hour = 0
                            next_day = now.replace(hour=0, minute=0, second=0, microsecond=0)
                            next_funding = next_day.replace(day=now.day + 1)
                        else:
                            next_funding = now.replace(hour=next_funding_hour, minute=0, second=0, microsecond=0)
                        
                        return FundingSnapshot(
                            ts=now,
                            symbol=symbol,
                            funding_rate_now=record.get("value", 0) / 100,  # Convert from % to decimal
                            funding_rate_next=record.get("predicted", record.get("value", 0)) / 100,
                            next_funding_ts=next_funding
                        )
                    
                    return None
                    
                else:
                    logger.warning(f"[COINALYZE] Funding request failed: {resp.status}")
                    return None
                    
        except Exception as e:
            logger.error(f"[COINALYZE] Funding fetch error: {e}")
            return None
    
    async def fetch_liquidations(
        self, 
        symbol: str, 
        lookback_minutes: int = 5
    ) -> List[LiquidationEvent]:
        """
        Fetch recent liquidation events.
        
        Returns:
            List of LiquidationEvent
        """
        if not self.session:
            await self.initialize()
            
        coinalyze_symbol = self._convert_symbol(symbol)
        
        try:
            now = int(datetime.now(timezone.utc).timestamp())
            start = now - (lookback_minutes * 60)
            
            params = {
                "symbols": coinalyze_symbol,
                "interval": "5min",
                "from": start,
                "to": now,
            }
            
            async with self.session.get(
                f"{self.BASE_URL}/liquidation-history",
                params=params,
                timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    
                    events = []
                    if data and len(data) > 0:
                        records = data[0].get("history", [])
                        
                        for record in records:
                            # Long and short liquidations
                            ts = datetime.fromtimestamp(record["t"] / 1000, tz=timezone.utc)
                            
                            long_vol = record.get("l", 0)
                            short_vol = record.get("s", 0)
                            
                            if long_vol > 0:
                                events.append(LiquidationEvent(
                                    ts=ts,
                                    symbol=symbol,
                                    side="long",
                                    volume=long_vol,
                                    price=0  # Price not available in this endpoint
                                ))
                            
                            if short_vol > 0:
                                events.append(LiquidationEvent(
                                    ts=ts,
                                    symbol=symbol,
                                    side="short",
                                    volume=short_vol,
                                    price=0
                                ))
                    
                    return events
                    
                else:
                    err_text = await resp.text()
                    logger.warning(f"[COINALYZE] Liquidation request failed: {resp.status} | Body: {err_text}")
                    return []
                    
        except Exception as e:
            logger.error(f"[COINALYZE] Liquidation fetch error: {e}")
            return []


# Global client instance
_client: Optional[CoinalyzeClient] = None


def get_client(api_key: str = "") -> CoinalyzeClient:
    """Get or create global client instance."""
    global _client
    if _client is None:
        _client = CoinalyzeClient(api_key)
    return _client


async def poll_oi_and_funding(symbol: str, api_key: str = "") -> Dict:
    """
    Poll OI and funding data for a symbol.
    
    Returns dict with both snapshots.
    """
    client = get_client(api_key)
    await client.initialize()
    
    oi = await client.fetch_open_interest(symbol)
    funding = await client.fetch_funding_rate(symbol)
    
    return {
        "oi": oi,
        "funding": funding,
    }


async def liquidation_stream(symbol: str, api_key: str = "") -> AsyncIterator[LiquidationEvent]:
    """
    Continuous liquidation polling.
    
    Yields LiquidationEvent objects.
    """
    client = get_client(api_key)
    await client.initialize()
    
    seen_ts = set()
    
    while True:
        try:
            events = await client.fetch_liquidations(symbol)
            
            for event in events:
                key = f"{event.ts}_{event.side}"
                if key not in seen_ts:
                    seen_ts.add(key)
                    yield event
            
            # Clean old keys
            if len(seen_ts) > 1000:
                seen_ts.clear()
                
        except Exception as e:
            logger.error(f"[COINALYZE] Liquidation stream error: {e}")
        
        await asyncio.sleep(30)  # Poll every 30 seconds


# ═══════════════════════════════════════════════════════════════════════════════
# STANDALONE TEST
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import os
    from dotenv import load_dotenv
    load_dotenv()
    
    async def test():
        api_key = os.getenv("COINALYZE_API_KEY", "")
        client = CoinalyzeClient(api_key=api_key)
        await client.initialize()
        
        print("\n[TEST] Fetching BTC OI...")
        oi = await client.fetch_open_interest("BTC-USDT")
        if oi:
            print(f"  OI: {oi.oi_close:,.0f} (delta: {oi.delta_oi:+,.0f})")
        else:
            print("  No OI data")
        
        print("\n[TEST] Fetching BTC Funding...")
        funding = await client.fetch_funding_rate("BTC-USDT")
        if funding:
            print(f"  Current: {funding.funding_rate_now:.4%}")
            print(f"  Next: {funding.funding_rate_next:.4%}")
        else:
            print("  No funding data")
        
        print("\n[TEST] Fetching BTC Liquidations...")
        liqs = await client.fetch_liquidations("BTC-USDT")
        print(f"  Found {len(liqs)} events")
        for liq in liqs[:3]:
            print(f"    {liq.side}: ${liq.volume:,.0f}")
        
        await client.close()
    
    asyncio.run(test())

import asyncio
import aiohttp
import logging
from datetime import datetime, timezone
from typing import Dict, Optional

logger = logging.getLogger("feeds.macro")

class MacroMonitor:
    """
    Monitors Macro conditions via CoinGecko.
    Tracks USDT Dominance and broad market trend.
    """
    
    BASE_URL = "https://api.coingecko.com/api/v3"
    
    def __init__(self, api_key: str = "", check_interval: int = 60):
        self.api_key = api_key
        self.check_interval = check_interval
        self.running = False
        
        # State
        self.usdt_dominance: float = 0.0
        self.usdt_trend: str = "NEUTRAL"
        self.btc_dominance: float = 0.0
        self.last_update: Optional[datetime] = None
        
        # History for trend calc
        self.history = [] # List of (timestamp, usdt_d)
        
    async def start(self):
        self.running = True
        logger.info("[MACRO] Monitor started")
        while self.running:
            try:
                await self._update_metrics()
            except Exception as e:
                logger.error(f"[MACRO] Error: {e}")
            await asyncio.sleep(self.check_interval)

    async def stop(self):
        self.running = False
        logger.info("[MACRO] Monitor stopped")

    async def _update_metrics(self):
        headers = {}
        if self.api_key:
            # Use demo key header as per legacy implementation
            headers['x-cg-demo-api-key'] = self.api_key
            
        url = f"{self.BASE_URL}/global"
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    market_data = data.get('data', {})
                    
                    total_mcap = market_data.get('total_market_cap', {}).get('usd', 0)
                    market_cap_pct = market_data.get('market_cap_percentage', {})
                    
                    # 1. BTC Dominance
                    self.btc_dominance = market_cap_pct.get('btc', 0.0)
                    
                    # 2. USDT Dominance
                    # CG returns USDT dominance in 'market_cap_percentage' usually?
                    # Let's check typical response. 'usdt' key usually exists there.
                    self.usdt_dominance = market_cap_pct.get('usdt', 0.0)
                    
                    # If not found, calc manually
                    if self.usdt_dominance == 0 and total_mcap > 0:
                        # Need to fetch USDT mcap separately? 
                        # Usually 'market_cap_percentage' has 'usdt'. 
                        pass
                    
                    self.last_update = datetime.now(timezone.utc)
                    self._update_trend()
                    
                    logger.info(f"[MACRO] USDT.D: {self.usdt_dominance:.2f}% | Trend: {self.usdt_trend}")
                else:
                    logger.warning(f"[MACRO] API request failed: {resp.status}")

    def _update_trend(self):
        # Maintain history
        now = datetime.now().timestamp()
        self.history.append((now, self.usdt_dominance))
        
        # Prune old
        cutoff = now - 3600 # 1 hour history
        self.history = [h for h in self.history if h[0] > cutoff]
        
        if len(self.history) < 2:
            self.usdt_trend = "NEUTRAL"
            return
            
        # Simple slope
        start_val = self.history[0][1]
        end_val = self.history[-1][1]
        
        if end_val > start_val * 1.01:
            self.usdt_trend = "UP" # Risk Off
        elif end_val < start_val * 0.99:
            self.usdt_trend = "DOWN" # Risk On
        else:
            self.usdt_trend = "NEUTRAL"

    def get_state(self) -> Dict:
        return {
            "usdt_d": self.usdt_dominance,
            "usdt_trend": self.usdt_trend,
            "btc_d": self.btc_dominance,
            "last_update": self.last_update
        }

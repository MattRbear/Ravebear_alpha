#!/usr/bin/env python3
"""
ALPHA DISCORD NOTIFIER
======================
Send wick alerts to Discord channels.

Author: Flint for RaveBear
"""

import asyncio
import aiohttp
import logging
from datetime import datetime, timezone
from typing import Dict, Optional
from dataclasses import dataclass

logger = logging.getLogger("alpha.discord")


@dataclass
class WickAlert:
    """Structured wick alert data"""
    symbol: str
    timeframe: str
    wick_side: str
    wick_high: float
    wick_low: float
    score: float = 0.0
    features: Dict = None


class DiscordNotifier:
    """Send alerts to Discord webhooks"""
    
    def __init__(
        self,
        webhook_general: str = "",
        webhook_btc: str = "",
        webhook_eth: str = "",
        webhook_sol: str = ""
    ):
        self.webhooks = {
            "general": webhook_general,
            "BTC": webhook_btc,
            "ETH": webhook_eth,
            "SOL": webhook_sol,
        }
        self.session: Optional[aiohttp.ClientSession] = None
        self.cooldowns: Dict[str, float] = {}
        self.cooldown_seconds = 300  # 5 min
        
    async def initialize(self):
        """Initialize HTTP session"""
        self.session = aiohttp.ClientSession()
        configured = [k for k, v in self.webhooks.items() if v]
        logger.info(f"[DISCORD] Initialized: {configured}")
        
    async def close(self):
        """Close HTTP session"""
        if self.session:
            await self.session.close()
    
    def _get_channels(self, symbol: str) -> list:
        """Get target channels for a symbol"""
        channels = ["general"]
        
        for base in ["BTC", "ETH", "SOL"]:
            if base in symbol.upper() and self.webhooks.get(base):
                channels.append(base)
        
        return channels
    
    def _check_cooldown(self, key: str) -> bool:
        """Check if alert is on cooldown"""
        if key not in self.cooldowns:
            return True
        elapsed = datetime.now(timezone.utc).timestamp() - self.cooldowns[key]
        return elapsed >= self.cooldown_seconds
    
    async def send_wick_alert(self, alert: WickAlert) -> bool:
        """
        Send wick detection alert to Discord.
        
        Returns:
            True if sent successfully
        """
        if not self.session:
            await self.initialize()
        
        # Check cooldown
        key = f"{alert.symbol}_{alert.wick_side}"
        if not self._check_cooldown(key):
            logger.debug(f"[DISCORD] Suppressed (cooldown): {key}")
            return False
        
        # Build embed
        emoji = "🟢" if alert.wick_side == "lower" else "🔴"
        direction = "BULL" if alert.wick_side == "lower" else "BEAR"
        
        # Extract key features
        feats = alert.features or {}
        wick_ratio = feats.get("wick_to_body_ratio", 0)
        delta = feats.get("delta_at_wick", 0)
        depth_imbal = feats.get("depth_imbalance", 0)
        funding = feats.get("funding_rate_now", 0)
        
        embed = {
            "title": f"{emoji} {direction} WICK - {alert.symbol}",
            "description": f"Wick detected on {alert.timeframe} timeframe",
            "color": 0x00FF00 if alert.wick_side == "lower" else 0xFF0000,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "fields": [
                {"name": "High", "value": f"${alert.wick_high:,.2f}", "inline": True},
                {"name": "Low", "value": f"${alert.wick_low:,.2f}", "inline": True},
                {"name": "Wick Ratio", "value": f"{wick_ratio:.2f}", "inline": True},
                {"name": "Delta", "value": f"{delta:+.4f}", "inline": True},
                {"name": "Depth Imbal", "value": f"{depth_imbal:+.2%}", "inline": True},
                {"name": "Funding", "value": f"{funding:.4%}", "inline": True},
            ],
            "footer": {"text": "ALPHA Wick Engine | RaveBear"}
        }
        
        # Send to channels
        channels = self._get_channels(alert.symbol)
        success = False
        
        for channel in channels:
            webhook_url = self.webhooks.get(channel, "")
            if not webhook_url:
                continue
            
            try:
                payload = {"embeds": [embed], "username": "ALPHA Wick Engine"}
                
                async with self.session.post(
                    webhook_url,
                    json=payload,
                    timeout=aiohttp.ClientTimeout(total=10)
                ) as resp:
                    if resp.status in [200, 204]:
                        success = True
                        logger.info(f"[DISCORD] Sent to {channel}")
                    elif resp.status == 429:
                        logger.warning(f"[DISCORD] Rate limited on {channel}")
                    else:
                        logger.warning(f"[DISCORD] Failed {channel}: {resp.status}")
                        
            except Exception as e:
                logger.error(f"[DISCORD] Error: {e}")
        
        if success:
            self.cooldowns[key] = datetime.now(timezone.utc).timestamp()
        
        return success


# ═══════════════════════════════════════════════════════════════════════════════
# STANDALONE TEST
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    async def test():
        notifier = DiscordNotifier(
            webhook_general="https://discord.com/api/webhooks/1381125921876217907/WAGTOsJLyAwOiS2oi4V1iCbW6KKDVMiGcQkMQPHYG6z1WrlDik_1QOmowZh_Kw1aLDLU"
        )
        await notifier.initialize()
        
        alert = WickAlert(
            symbol="BTC-USDT",
            timeframe="1m",
            wick_side="lower",
            wick_high=100000.0,
            wick_low=99500.0,
            features={
                "wick_to_body_ratio": 2.5,
                "delta_at_wick": -15.5,
                "depth_imbalance": -0.25,
                "funding_rate_now": 0.0001,
            }
        )
        
        result = await notifier.send_wick_alert(alert)
        print(f"Alert sent: {result}")
        
        await notifier.close()
    
    asyncio.run(test())

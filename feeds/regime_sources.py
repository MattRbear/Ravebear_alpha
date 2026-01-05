# file: feeds/regime_sources.py
import logging

logger = logging.getLogger("feeds.regime")

async def poll_market_dominance():
    """
    STUB ONLY
    Fetch BTC.D, USDT.D, TOTAL2 from external source (e.g. TradingView or specialized API).
    Returns dict with dominance values.
    """
    logger.debug("poll_market_dominance stub called")
    pass

async def poll_eth_btc_ratio():
    """
    STUB ONLY
    Fetch ETH/BTC price and trend data.
    """
    logger.debug("poll_eth_btc_ratio stub called")
    pass

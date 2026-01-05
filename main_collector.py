#!/usr/bin/env python3
"""
ALPHA WICK ENGINE COLLECTOR
===========================
Real-time wick detection with full feature extraction.

Features:
- OKX WebSocket (trades + orderbook)
- Coinalyze (OI, Funding, Liquidations)
- CoinGecko (Dominance)
- Whale Alert (Large Transactions)
- Discord alerts
- JSONL archival

Author: Flint for RaveBear
"""

import asyncio
import signal
import logging
import os
import json
import time
from datetime import datetime, timezone

from config import load_settings
from utils.logging import setup_logger
from feeds.okx_trades import OkxTradeStream
from feeds.okx_orderbook import OkxOrderBookStream
from feeds.coinalyze_derivs import CoinalyzeClient
from feeds.whale_alert import WhaleAlertClient
from feeds.coingecko_macro import MacroMonitor
from analysis.scorer import WickScorer
from features.derivatives import register_oi_snapshot, register_funding_snapshot, register_liquidation_event
from feeds.discord_notifier import DiscordNotifier, WickAlert
from utils.aggregation import CandleAggregator
from detectors.wick_detector import detect_wick_events
from features import WickEvent, WickFeatures
from features.wick_geometry import compute_wick_geometry
from features.orderflow import compute_orderflow_features
from features.liquidity import compute_liquidity_features
from features.vwap import compute_vwap_features
from features.derivatives import compute_derivatives_features
from features.session import compute_session_features
from storage.jsonl_writer import JsonlWriter

# Setup global logger
logger = setup_logger("main_collector", "INFO")


class AlphaWickEngine:
    """
    Main engine coordinating all data feeds and processing.
    """
    
    def __init__(self):
        self.settings = load_settings()
        self.running = False
        
        # Data feeds
        self.trade_stream = None
        self.orderbook_stream = None
        self.coinalyze_client = None
        self.whale_client = None
        self.macro_monitor = None
        self.discord_notifier = None
        
        # Analysis
        self.scorer = None
        
        # Processing
        self.aggregators = {}  # Per-symbol aggregators
        self.latest_orderbook = {}  # Per-symbol orderbook
        
        # Storage
        self.writer = None
        
        # Stats
        self.wicks_detected = 0
        self.alerts_sent = 0
        self.last_alert_error = "None"
        
        # Health Tracking
        self.start_time = time.time()
        self.last_feed_activity = {
            "trades": 0.0,
            "orderbook": 0.0,
            "derivs": 0.0,
            "macro": 0.0,
            "whale": 0.0
        }
        self.symbol_snapshots = {} # symbol -> {last_candle_ts, last_wick_side, last_score}
        
    async def initialize(self):
        """Initialize all components"""
        logger.info("=" * 60)
        logger.info("  ALPHA WICK ENGINE - INITIALIZING")
        logger.info("=" * 60)
        
        # Trade stream
        self.trade_stream = OkxTradeStream(
            url=self.settings.okx.base_url_ws_public,
            symbols=self.settings.okx.symbols
        )
        logger.info(f"[INIT] Trade stream: {self.settings.okx.symbols}")
        
        # Orderbook stream
        self.orderbook_stream = OkxOrderBookStream(
            url=self.settings.okx.base_url_ws_public,
            symbols=self.settings.okx.symbols
        )
        logger.info("[INIT] Orderbook stream ready")
        
        # Coinalyze client
        self.coinalyze_client = CoinalyzeClient(
            api_key=self.settings.coinalyze.api_key
        )
        await self.coinalyze_client.initialize()
        logger.info("[INIT] Coinalyze client ready")
        
        # Whale Alert Client
        whale_key = os.getenv("WHALE_ALERT_KEY")
        if whale_key:
            self.whale_client = WhaleAlertClient(api_key=whale_key)
            logger.info("[INIT] Whale Alert client ready")
        else:
            logger.warning("[INIT] WHALE_ALERT_KEY not found - Whale monitoring disabled")

        # Macro Monitor
        self.macro_monitor = MacroMonitor(
            api_key=self.settings.coingecko.api_key if self.settings.coingecko else ""
        )
        logger.info("[INIT] Macro monitor ready")
        
        # Wick Scorer
        self.scorer = WickScorer(self.settings.model_dump())
        logger.info("[INIT] Wick Scorer ready")

        # Discord notifier
        if self.settings.discord:
            self.discord_notifier = DiscordNotifier(
                webhook_general=self.settings.discord.webhook_general,
                webhook_btc=self.settings.discord.webhook_btc,
                webhook_eth=self.settings.discord.webhook_eth,
                webhook_sol=self.settings.discord.webhook_sol,
            )
            await self.discord_notifier.initialize()
            logger.info("[INIT] Discord notifier ready")
        
        # Candle aggregators (one per symbol)
        for symbol in self.settings.okx.symbols:
            self.aggregators[symbol] = CandleAggregator(
                timeframe_secs=self.settings.engine.candle_timeframe_secs
            )
        logger.info(f"[INIT] Aggregators: {len(self.aggregators)}")
        
        # Storage writer
        self.writer = JsonlWriter(
            output_dir=self.settings.storage.output_dir,
            file_rotation_mb=self.settings.storage.file_rotation_mb
        )
        logger.info("[INIT] Storage writer ready")
        
        logger.info("=" * 60)
        logger.info("  ALL COMPONENTS INITIALIZED")
        logger.info("=" * 60)
    
    async def run(self):
        """Main run loop"""
        await self.initialize()
        self.running = True
        
        # Create background tasks
        tasks = [
            asyncio.create_task(self._process_trades()),
            asyncio.create_task(self._process_orderbooks()),
            asyncio.create_task(self._poll_derivatives()),
            asyncio.create_task(self.macro_monitor.start()),
            asyncio.create_task(self._log_stats()),
            asyncio.create_task(self._update_status_file()),
        ]
        
        if self.whale_client:
            tasks.append(asyncio.create_task(self.whale_client.start()))
        
        logger.info("\n[START] Engine running...")
        
        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            logger.info("[STOP] Shutdown requested")
        finally:
            await self.shutdown()
    
    async def _update_status_file(self):
        """Write engine status to JSON every 5 seconds"""
        # Ensure absolute path to avoid CWD confusion
        base_dir = os.path.dirname(os.path.abspath(__file__))
        status_file = os.path.join(base_dir, self.settings.storage.output_dir, "engine_status.json")
        os.makedirs(os.path.dirname(status_file), exist_ok=True)
        
        while self.running:
            try:
                # Update polling feeds activity
                if self.macro_monitor:
                    self.last_feed_activity["macro"] = self.macro_monitor.last_update.timestamp() if self.macro_monitor.last_update else 0
                
                # Whale client doesn't expose easy timestamp, we assume alive if running
                if self.whale_client and self.whale_client.running:
                     self.last_feed_activity["whale"] = time.time()

                now = time.time()
                uptime = now - self.start_time
                
                status = {
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "uptime_seconds": int(uptime),
                    "running": self.running,
                    "wicks_detected": self.wicks_detected,
                    "alerts_sent": self.alerts_sent,
                    "usdt_dominance": self.macro_monitor.usdt_dominance if self.macro_monitor else 0.0,
                    
                    # Alert Pipeline
                    "discord_enabled": self.discord_notifier is not None,
                    "webhooks_configured": list(self.discord_notifier.webhooks.keys()) if self.discord_notifier else [],
                    "wick_min_ratio": self.settings.engine.wick_min_ratio,
                    "last_alert_error": self.last_alert_error,
                    
                    # Feed Health (Age in seconds)
                    "feed_age": {
                        "trades": int(now - self.last_feed_activity["trades"]),
                        "orderbook": int(now - self.last_feed_activity["orderbook"]),
                        "derivs": int(now - self.last_feed_activity["derivs"]),
                        "macro": int(now - self.last_feed_activity["macro"]),
                        "whale": int(now - self.last_feed_activity["whale"])
                    },
                    
                    # Symbol Snapshots
                    "symbol_snapshots": self.symbol_snapshots
                }
                
                with open(status_file, "w") as f:
                    json.dump(status, f, indent=2)
                    
            except Exception as e:
                logger.error(f"[STATUS] Error writing status: {e}")
            
            await asyncio.sleep(5)

    async def _process_trades(self):
        """Process incoming trades and detect wicks"""
        async for trade in self.trade_stream.stream():
            self.last_feed_activity["trades"] = time.time()
            if not self.running:
                break
            
            try:
                symbol = trade.symbol
                aggregator = self.aggregators.get(symbol)
                
                if not aggregator:
                    continue
                
                # Aggregate into candle
                closed_candle = aggregator.process_trade(trade)
                
                if closed_candle:
                    logger.info(
                        f"[CANDLE] {symbol} | O:{closed_candle.open:.2f} "
                        f"H:{closed_candle.high:.2f} L:{closed_candle.low:.2f} "
                        f"C:{closed_candle.close:.2f} V:{closed_candle.volume:.4f}"
                    )
                    
                    # Detect wicks
                    wick_events = detect_wick_events(
                        closed_candle, 
                        wick_min_ratio=0.05 # Detect ALL wicks for data/dashboard (filter for alerts later)
                    )
                    
                    for event_meta in wick_events:
                        await self._process_wick(closed_candle, event_meta)
                        
            except Exception as e:
                logger.error(f"[TRADE] Error: {e}", exc_info=True)
    
    async def _process_wick(self, candle, event_meta):
        """Process detected wick and compute features"""
        wick_side = event_meta["side"]
        symbol = candle.symbol
        
        logger.info(f"[WICK] {symbol} | {wick_side.upper()} wick detected")
        self.wicks_detected += 1
        
        # Get latest orderbook
        orderbook = self.latest_orderbook.get(symbol)
        
        # Compute all features
        geo_feats = compute_wick_geometry(candle, wick_side)
        of_feats = compute_orderflow_features(candle, candle.trades)
        liq_feats = compute_liquidity_features(orderbook)
        session_feats = compute_session_features(candle.end_ts)
        
        # Get session label for VWAP
        session_label = session_feats.get("session_label", "none")
        
        vwap_feats = compute_vwap_features(
            trades=candle.trades,
            now=candle.end_ts,
            symbol=symbol,
            session_label=session_label,
            last_price=candle.close
        )
        
        deriv_feats = compute_derivatives_features(
            symbol=symbol,
            wick_ts=candle.end_ts
        )
        
        # Combine features
        all_features = {
            **geo_feats,
            **of_feats,
            **liq_feats,
            **session_feats,
            **vwap_feats,
            **deriv_feats,
        }
        
        # Add Macro State
        if self.macro_monitor:
            macro_state = self.macro_monitor.get_state()
            all_features['usdt_d'] = macro_state.get('usdt_d', 0.0)
            all_features['btc_d'] = macro_state.get('btc_d', 0.0)
            
        feats_model = WickFeatures(**all_features)
        
        # Create event
        wick_event = WickEvent(
            ts=candle.end_ts,
            symbol=symbol,
            timeframe="1m",
            wick_side=wick_side,
            wick_high=candle.high,
            wick_low=candle.low,
            features=feats_model
        )
        
        # SCORE THE WICK
        score_result = self.scorer.score_wick(wick_event)
        
        # Check Whale Activity
        recent_whales = []
        if self.whale_client:
            recent_whales = self.whale_client.get_recent_whales(symbol)
        
        # Log Score
        magnet_score = score_result.get('wick_magnet_score', 0)
        confidence = score_result.get('confidence', 0)
        whale_icon = "🐳" if recent_whales else ""
        logger.info(f"[SCORE] {magnet_score:.1f}/100 | Conf: {confidence}% {whale_icon}")
        
        # Update Snapshot
        self.symbol_snapshots[symbol] = {
            "last_candle_ts": datetime.fromtimestamp(candle.end_ts.timestamp()).isoformat(),
            "last_wick_side": wick_side,
            "last_score": magnet_score
        }

        # Write to storage with orderbook snapshot for void/wall detection
        event_dict = {
            'ts': wick_event.ts.isoformat() if hasattr(wick_event.ts, 'isoformat') else str(wick_event.ts),
            'symbol': wick_event.symbol,
            'timeframe': wick_event.timeframe,
            'wick_side': wick_event.wick_side,
            'wick_high': wick_event.wick_high,
            'wick_low': wick_event.wick_low,
            'features': wick_event.features.model_dump() if hasattr(wick_event.features, 'model_dump') else dict(wick_event.features),
        }
        
        # Embed raw orderbook for dashboard void/wall detection
        if orderbook:
            event_dict['orderbook'] = {
                'symbol': orderbook.symbol,
                'timestamp': orderbook.ts.isoformat() if hasattr(orderbook.ts, 'isoformat') else str(orderbook.ts),
                'mid_price': (orderbook.best_bid + orderbook.best_ask) / 2,
                'bids': [(p, s) for p, s in orderbook.bids[:20]],
                'asks': [(p, s) for p, s in orderbook.asks[:20]],
            }
        
        await self.writer.write_event_dict(event_dict)
        
        # Send Discord alert (for significant wicks)
        wick_ratio = geo_feats.get("wick_to_body_ratio", 0)
        min_ratio = self.settings.engine.wick_min_ratio
        discord_enabled = self.discord_notifier is not None
        
        # DEBUG ALERT LOGIC
        # Log every detected wick so user sees system is ALIVE
        status_icon = "🔔" if (wick_ratio >= min_ratio and discord_enabled) else "📝"
        logger.info(f"{status_icon} [WICK] {symbol} {wick_side.upper()} | Ratio: {wick_ratio:.2f} (Min: {min_ratio}) | Score: {magnet_score:.1f}")

        if wick_ratio >= min_ratio and discord_enabled:
            
            # Enrich alert data
            alert_data = all_features.copy()
            alert_data.update(score_result)
            if recent_whales:
                alert_data['whale_alert'] = True
                alert_data['whale_txs'] = len(recent_whales)
            
            alert = WickAlert(
                symbol=symbol,
                timeframe="1m",
                wick_side=wick_side,
                wick_high=candle.high,
                wick_low=candle.low,
                features=alert_data
            )
            
            sent = await self.discord_notifier.send_wick_alert(alert)
            if sent:
                self.alerts_sent += 1
                logger.info(f"[ALERT SENT] {symbol} {wick_side} wick")
            else:
                self.last_alert_error = "Send Failed (Cooldown or API Error)"
                logger.warning(f"[ALERT FAIL] {symbol} not sent")
        elif not discord_enabled:
             self.last_alert_error = "Discord Not Configured"
        elif wick_ratio < min_ratio:
             # Not an error, just filtered
             pass
    
    async def _process_orderbooks(self):
        """Process orderbook updates"""
        async for ob in self.orderbook_stream.stream():
            self.last_feed_activity["orderbook"] = time.time()
            if not self.running:
                break
            
            self.latest_orderbook[ob.symbol] = ob
    
    async def _poll_derivatives(self):
        """Poll Coinalyze for derivatives data"""
        while self.running:
            self.last_feed_activity["derivs"] = time.time()
            try:
                for symbol in self.settings.okx.symbols:
                    # Fetch OI
                    oi = await self.coinalyze_client.fetch_open_interest(symbol)
                    if oi:
                        register_oi_snapshot(oi)
                    
                    # Fetch funding
                    funding = await self.coinalyze_client.fetch_funding_rate(symbol)
                    if funding:
                        register_funding_snapshot(funding)
                    
                    # Fetch liquidations
                    liqs = await self.coinalyze_client.fetch_liquidations(symbol)
                    for liq in liqs:
                        register_liquidation_event(liq)
                    
                    await asyncio.sleep(1)  # Small delay between symbols
                    
            except Exception as e:
                logger.error(f"[DERIV] Poll error: {e}")
            
            await asyncio.sleep(30)  # Poll every 30 seconds
    
    async def _log_stats(self):
        """Log stats every 5 minutes"""
        while self.running:
            await asyncio.sleep(300)
            
            logger.info("=" * 60)
            logger.info("  ALPHA ENGINE STATS")
            logger.info("=" * 60)
            logger.info(f"  Wicks Detected: {self.wicks_detected}")
            logger.info(f"  Alerts Sent: {self.alerts_sent}")
            logger.info(f"  Symbols: {self.settings.okx.symbols}")
            if self.macro_monitor:
                logger.info(f"  USDT.D: {self.macro_monitor.usdt_dominance:.2f}%")
            logger.info("=" * 60)
    
    async def shutdown(self):
        """Clean shutdown"""
        logger.info("[SHUTDOWN] Cleaning up...")
        self.running = False
        
        if self.trade_stream:
            await self.trade_stream.close()
        
        if self.orderbook_stream:
            await self.orderbook_stream.close()
        
        if self.coinalyze_client:
            await self.coinalyze_client.close()
            
        if self.whale_client:
            await self.whale_client.stop()
            
        if self.macro_monitor:
            await self.macro_monitor.stop()
        
        if self.discord_notifier:
            await self.discord_notifier.close()
        
        logger.info("[SHUTDOWN] Complete")


async def main():
    """Main entry point"""
    engine = AlphaWickEngine()
    
    # Handle signals
    loop = asyncio.get_running_loop()
    
    def signal_handler():
        logger.info("[SIGNAL] Shutdown requested")
        asyncio.create_task(engine.shutdown())
    
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, signal_handler)
        except NotImplementedError:
            pass  # Windows
    
    await engine.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass

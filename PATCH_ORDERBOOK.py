# ALPHA ORDERBOOK PATCH
# =====================
# Apply these 2 changes to enable void/wall detection in dashboard

# ================================================
# CHANGE 1: main_collector.py (around line 368-372)
# ================================================
# FIND THIS:
"""
        # Write to storage (include score in labels if possible, or just features)
        # Assuming we can't easily modify WickEvent labels field type on the fly if it's strict,
        # but we can try setting it if WickLabels supports it, or just rely on the stored features.
        # For now, we write as is.
        await self.writer.write_event(wick_event)
"""

# REPLACE WITH THIS:
"""
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
"""

# ================================================
# CHANGE 2: storage/jsonl_writer.py
# ================================================
# ADD THIS METHOD to the JsonlWriter class:
"""
    async def write_event_dict(self, event_dict: dict):
        '''Write a raw dict to JSONL (for events with embedded orderbook).'''
        await self._ensure_file()
        line = json.dumps(event_dict, default=str) + "\n"
        self._current_file.write(line)
        self._current_file.flush()
        self._current_bytes += len(line.encode('utf-8'))
"""

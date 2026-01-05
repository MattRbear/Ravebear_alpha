# file: utils/aggregation.py
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional, List
from feeds.okx_trades import Trade
import logging

logger = logging.getLogger("utils.aggregation")

@dataclass
class Candle:
    start_ts: datetime
    end_ts: datetime
    symbol: str
    open: float
    high: float
    low: float
    close: float
    volume: float
    buy_volume: float
    sell_volume: float
    trades: List[Trade] # Keep trades for detailed analysis if needed

class CandleAggregator:
    def __init__(self, timeframe_secs: int = 60):
        self.timeframe_secs = timeframe_secs
        self.current_bucket_start: Optional[datetime] = None
        self.current_candle: Optional[Candle] = None
        self.closed_candles: List[Candle] = []

    def process_trade(self, trade: Trade) -> Optional[Candle]:
        """
        Ingest a trade. If it belongs to a new bucket, close and return the old candle.
        Otherwise, update the current candle.
        """
        # Determine bucket start time (floor to timeframe)
        ts_timestamp = trade.ts.timestamp()
        bucket_start_ts = datetime.fromtimestamp(
            (ts_timestamp // self.timeframe_secs) * self.timeframe_secs,
            tz=timezone.utc
        )

        closed_candle = None

        # Check if we moved to a new bucket
        if self.current_bucket_start is None:
            self._init_new_candle(bucket_start_ts, trade)
        
        elif bucket_start_ts > self.current_bucket_start:
            # Close current candle
            self.current_candle.end_ts = self.current_bucket_start + timedelta(seconds=self.timeframe_secs)
            closed_candle = self.current_candle
            self.closed_candles.append(closed_candle)
            
            # Start new candle
            self._init_new_candle(bucket_start_ts, trade)

        else:
            # Update current candle
            self._update_candle(trade)

        return closed_candle

    def _init_new_candle(self, start_ts: datetime, trade: Trade):
        self.current_bucket_start = start_ts
        self.current_candle = Candle(
            start_ts=start_ts,
            end_ts=start_ts + timedelta(seconds=self.timeframe_secs), # Provisional
            symbol=trade.symbol,
            open=trade.price,
            high=trade.price,
            low=trade.price,
            close=trade.price,
            volume=trade.size,
            buy_volume=trade.size if trade.side == "buy" else 0.0,
            sell_volume=trade.size if trade.side == "sell" else 0.0,
            trades=[trade]
        )

    def _update_candle(self, trade: Trade):
        c = self.current_candle
        c.high = max(c.high, trade.price)
        c.low = min(c.low, trade.price)
        c.close = trade.price
        c.volume += trade.size
        if trade.side == "buy":
            c.buy_volume += trade.size
        else:
            c.sell_volume += trade.size
        c.trades.append(trade)

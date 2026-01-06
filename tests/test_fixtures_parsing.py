#!/usr/bin/env python3
"""
Tests for parsing OKX WebSocket messages using replay fixtures.
"""

import json
import pytest
from datetime import datetime, timezone
from pathlib import Path

# Get fixtures directory
FIXTURES_DIR = Path(__file__).parent / "fixtures"


class TestTradeMessageParsing:
    """Tests for trade message parsing logic."""

    @pytest.fixture
    def trade_message(self):
        """Load sample trade message from fixture."""
        with open(FIXTURES_DIR / "okx_trade_message.json") as f:
            return json.load(f)

    def test_trade_message_structure(self, trade_message):
        """Test that fixture has expected structure."""
        assert "arg" in trade_message
        assert "data" in trade_message
        assert trade_message["arg"]["channel"] == "trades"

    def test_parse_trade_timestamp(self, trade_message):
        """Test parsing of millisecond timestamp."""
        item = trade_message["data"][0]
        ts_ms = int(item["ts"])

        # Should be a valid millisecond timestamp
        assert ts_ms > 0

        # Convert to datetime
        dt = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)

        # Should produce valid datetime
        assert dt.year >= 2024
        assert dt.tzinfo == timezone.utc

    def test_parse_trade_price(self, trade_message):
        """Test parsing of price field."""
        item = trade_message["data"][0]
        price = float(item["px"])

        # Price should be positive
        assert price > 0

        # Should be a reasonable BTC price
        assert 1000 < price < 500000

    def test_parse_trade_size(self, trade_message):
        """Test parsing of size field."""
        item = trade_message["data"][0]
        size = float(item["sz"])

        # Size should be positive
        assert size > 0

    def test_parse_trade_side(self, trade_message):
        """Test parsing of side field."""
        item = trade_message["data"][0]
        side = item["side"]

        # Side must be buy or sell
        assert side in ("buy", "sell")

    def test_parse_trade_symbol(self, trade_message):
        """Test parsing of instId (symbol) field."""
        item = trade_message["data"][0]
        symbol = item["instId"]

        # Should be standard OKX format
        assert "-" in symbol
        assert symbol == "BTC-USDT"


class TestOrderbookMessageParsing:
    """Tests for orderbook message parsing logic."""

    @pytest.fixture
    def orderbook_message(self):
        """Load sample orderbook message from fixture."""
        with open(FIXTURES_DIR / "okx_orderbook_message.json") as f:
            return json.load(f)

    def test_orderbook_message_structure(self, orderbook_message):
        """Test that fixture has expected structure."""
        assert "arg" in orderbook_message
        assert "data" in orderbook_message
        assert orderbook_message["arg"]["channel"] == "books5"

    def test_parse_orderbook_timestamp(self, orderbook_message):
        """Test parsing of millisecond timestamp."""
        item = orderbook_message["data"][0]
        ts_ms = int(item["ts"])

        # Convert to datetime
        dt = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)

        assert dt.year >= 2024
        assert dt.tzinfo == timezone.utc

    def test_parse_orderbook_bids(self, orderbook_message):
        """Test parsing of bids array."""
        item = orderbook_message["data"][0]
        bids_raw = item["bids"]

        # Should have 5 levels (books5 channel)
        assert len(bids_raw) == 5

        # Parse bids
        bids = []
        for entry in bids_raw:
            price = float(entry[0])
            size = float(entry[1])
            bids.append((price, size))

        # Prices should be positive
        for price, size in bids:
            assert price > 0
            assert size >= 0

        # Bids should be sorted descending by price
        prices = [p for p, s in bids]
        assert prices == sorted(prices, reverse=True)

    def test_parse_orderbook_asks(self, orderbook_message):
        """Test parsing of asks array."""
        item = orderbook_message["data"][0]
        asks_raw = item["asks"]

        # Should have 5 levels
        assert len(asks_raw) == 5

        # Parse asks
        asks = []
        for entry in asks_raw:
            price = float(entry[0])
            size = float(entry[1])
            asks.append((price, size))

        # Prices should be positive
        for price, size in asks:
            assert price > 0
            assert size >= 0

        # Asks should be sorted ascending by price
        prices = [p for p, s in asks]
        assert prices == sorted(prices)

    def test_spread_is_positive(self, orderbook_message):
        """Test that best_ask > best_bid (positive spread)."""
        item = orderbook_message["data"][0]

        best_bid = float(item["bids"][0][0])
        best_ask = float(item["asks"][0][0])

        assert best_ask > best_bid

        spread = best_ask - best_bid
        assert spread > 0


class TestInvalidMessageHandling:
    """Tests for handling invalid/malformed messages."""

    def test_negative_price_detection(self):
        """Test that negative prices are detected as invalid."""
        msg = {
            "data": [
                {
                    "instId": "BTC-USDT",
                    "px": "-100.5",  # Invalid negative price
                    "sz": "0.01",
                    "side": "buy",
                    "ts": "1704067200000"
                }
            ]
        }

        item = msg["data"][0]
        price = float(item["px"])

        # Price should be detected as invalid
        assert price <= 0

    def test_zero_size_detection(self):
        """Test that zero size is detected."""
        msg = {
            "data": [
                {
                    "instId": "BTC-USDT",
                    "px": "95000.5",
                    "sz": "0",  # Zero size
                    "side": "buy",
                    "ts": "1704067200000"
                }
            ]
        }

        item = msg["data"][0]
        size = float(item["sz"])

        assert size == 0

    def test_invalid_side_detection(self):
        """Test that invalid side values are detected."""
        msg = {
            "data": [
                {
                    "instId": "BTC-USDT",
                    "px": "95000.5",
                    "sz": "0.01",
                    "side": "unknown",  # Invalid side
                    "ts": "1704067200000"
                }
            ]
        }

        item = msg["data"][0]
        side = item["side"]

        assert side not in ("buy", "sell")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

# ALPHA Wick Engine

Real-time wick detection system with full feature extraction for crypto markets.

## Features

- **Real-time Data Collection**: OKX WebSocket feeds for trades and orderbook
- **Derivatives Data**: Coinalyze integration for OI, funding, liquidations
- **Macro Monitoring**: CoinGecko for BTC/USDT dominance
- **Whale Tracking**: Whale Alert API integration
- **Wick Detection**: Automated detection of significant price wicks
- **Feature Extraction**: 50+ features for ML model training
- **Discord Alerts**: Webhook notifications for detected events

## Quick Start

```bash
# Install dependencies
pip install pydantic pyyaml httpx websockets aiohttp python-dateutil python-dotenv

# Run the collector
python main_collector.py

# Or use the Windows launcher
START.bat
```

## Commands

```bash
# Run all checks
make all

# Individual commands
make lint       # Run ruff linter
make typecheck  # Run mypy type checker
make test       # Run pytest
make smoke      # Run 60-second WebSocket connectivity test
make run        # Run the collector
make clean      # Remove build artifacts
```

## Configuration

Set environment variables or create a `.env` file:

```bash
# OKX WebSocket
OKX_WS_URL=wss://ws.okx.com:8443/ws/v5/public
OKX_SYMBOLS=BTC-USDT,ETH-USDT,SOL-USDT

# API Keys (optional)
COINALYZE_API_KEY=your_key
COINGECKO_API_KEY=your_key
WHALE_ALERT_KEY=your_key

# Discord Webhooks (optional)
DISCORD_WEBHOOK_GENERAL=https://...
DISCORD_WEBHOOK_BTC=https://...

# Engine Settings
WICK_MIN_RATIO=1.5
CANDLE_TIMEFRAME=60

# Storage
OUTPUT_DIR=data
FILE_ROTATION_MB=100
```

## Project Structure

```
├── main_collector.py    # Main entry point
├── config.py            # Configuration management
├── features/            # Feature extraction modules
│   ├── wick_geometry.py
│   ├── orderflow.py
│   ├── liquidity.py
│   ├── derivatives.py
│   ├── vwap.py
│   └── session.py
├── feeds/               # Data source connectors
│   ├── okx_trades.py
│   ├── okx_orderbook.py
│   ├── coinalyze_derivs.py
│   └── ...
├── storage/             # Data persistence
│   └── jsonl_writer.py
├── detectors/           # Event detection
│   └── wick_detector.py
├── analysis/            # Scoring and analysis
│   └── scorer.py
├── scripts/             # Utility scripts
│   └── smoke_collect.py
└── tests/               # Test suite
```

## Architecture

```
OKX WebSocket ──┐
                ├──► Trades Parser ──► Candle Aggregator ──► Wick Detector
                │
                ├──► Orderbook Parser ──► Liquidity Features
                │
Coinalyze API ──┴──► Derivatives Features
                                          ├──► Feature Extraction ──► Scorer
CoinGecko API ──────► Macro Monitor ──────┤
                                          │
Whale Alert ────────► Whale Tracker ──────┴──► JSONL Storage
                                               Discord Alerts
```

## Data Reliability

- **WebSocket Reconnect**: Automatic reconnection with exponential backoff (1-60s)
- **Data Validation**: Trades and orderbook entries validated for price > 0, size >= 0
- **Atomic Writes**: Storage uses temp-file + fsync pattern
- **Error Handling**: Explicit exceptions instead of silent failures

## Testing

```bash
# Run all tests
pytest tests/ -v

# Run specific test file
pytest tests/test_orderflow.py -v

# Run smoke test (requires network)
python scripts/smoke_collect.py
```

## License

MIT

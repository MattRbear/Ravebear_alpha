#!/usr/bin/env python3
"""
Configuration Module
====================
Loads and validates configuration settings for the Wick Engine.
"""

import os
from typing import List, Optional

from pydantic import BaseModel, Field
from dotenv import load_dotenv


# Load .env file if present
load_dotenv()


class OkxSettings(BaseModel):
    """OKX exchange settings."""
    base_url_ws_public: str = "wss://ws.okx.com:8443/ws/v5/public"
    base_url_rest: str = "https://www.okx.com"
    symbols: List[str] = Field(default=["BTC-USDT", "ETH-USDT", "SOL-USDT"])


class CoinalyzeSettings(BaseModel):
    """Coinalyze API settings."""
    api_key: str = Field(default="")


class CoinGeckoSettings(BaseModel):
    """CoinGecko API settings."""
    api_key: str = Field(default="")


class DiscordSettings(BaseModel):
    """Discord webhook settings."""
    webhook_general: str = Field(default="")
    webhook_btc: str = Field(default="")
    webhook_eth: str = Field(default="")
    webhook_sol: str = Field(default="")


class EngineSettings(BaseModel):
    """Engine processing settings."""
    candle_timeframe_secs: int = Field(default=60)
    wick_min_ratio: float = Field(default=1.5)


class StorageSettings(BaseModel):
    """Storage settings."""
    output_dir: str = Field(default="data")
    file_rotation_mb: int = Field(default=100)


class Settings(BaseModel):
    """Complete application settings."""
    okx: OkxSettings = Field(default_factory=OkxSettings)
    coinalyze: CoinalyzeSettings = Field(default_factory=CoinalyzeSettings)
    coingecko: Optional[CoinGeckoSettings] = Field(default=None)
    discord: Optional[DiscordSettings] = Field(default=None)
    engine: EngineSettings = Field(default_factory=EngineSettings)
    storage: StorageSettings = Field(default_factory=StorageSettings)


def load_settings() -> Settings:
    """
    Load settings from environment variables and defaults.
    
    Environment variables:
        OKX_WS_URL: WebSocket URL for OKX public feeds
        OKX_SYMBOLS: Comma-separated list of symbols (e.g., "BTC-USDT,ETH-USDT")
        COINALYZE_API_KEY: Coinalyze API key
        COINGECKO_API_KEY: CoinGecko API key
        DISCORD_WEBHOOK_GENERAL: Discord webhook for general alerts
        DISCORD_WEBHOOK_BTC: Discord webhook for BTC alerts
        DISCORD_WEBHOOK_ETH: Discord webhook for ETH alerts
        DISCORD_WEBHOOK_SOL: Discord webhook for SOL alerts
        WICK_MIN_RATIO: Minimum wick-to-body ratio to trigger alerts
        CANDLE_TIMEFRAME: Candle aggregation timeframe in seconds
        OUTPUT_DIR: Directory for output files
        FILE_ROTATION_MB: File rotation size in MB
    
    Returns:
        Settings object with all configuration
    """
    # Build OKX settings
    okx = OkxSettings(
        base_url_ws_public=os.getenv(
            "OKX_WS_URL",
            "wss://ws.okx.com:8443/ws/v5/public"
        ),
        symbols=os.getenv("OKX_SYMBOLS", "BTC-USDT,ETH-USDT,SOL-USDT").split(","),
    )

    # Build Coinalyze settings
    coinalyze = CoinalyzeSettings(
        api_key=os.getenv("COINALYZE_API_KEY", ""),
    )

    # Build CoinGecko settings (optional)
    coingecko_key = os.getenv("COINGECKO_API_KEY", "")
    coingecko = CoinGeckoSettings(api_key=coingecko_key) if coingecko_key else None

    # Build Discord settings (optional)
    discord_general = os.getenv("DISCORD_WEBHOOK_GENERAL", "")
    discord_btc = os.getenv("DISCORD_WEBHOOK_BTC", "")
    discord_eth = os.getenv("DISCORD_WEBHOOK_ETH", "")
    discord_sol = os.getenv("DISCORD_WEBHOOK_SOL", "")

    discord = None
    if discord_general or discord_btc or discord_eth or discord_sol:
        discord = DiscordSettings(
            webhook_general=discord_general,
            webhook_btc=discord_btc,
            webhook_eth=discord_eth,
            webhook_sol=discord_sol,
        )

    # Build Engine settings
    engine = EngineSettings(
        candle_timeframe_secs=int(os.getenv("CANDLE_TIMEFRAME", "60")),
        wick_min_ratio=float(os.getenv("WICK_MIN_RATIO", "1.5")),
    )

    # Build Storage settings
    storage = StorageSettings(
        output_dir=os.getenv("OUTPUT_DIR", "data"),
        file_rotation_mb=int(os.getenv("FILE_ROTATION_MB", "100")),
    )

    return Settings(
        okx=okx,
        coinalyze=coinalyze,
        coingecko=coingecko,
        discord=discord,
        engine=engine,
        storage=storage,
    )

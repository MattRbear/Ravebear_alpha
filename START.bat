@echo off
REM ═══════════════════════════════════════════════════════════════════════════════
REM ALPHA WICK ENGINE - LAUNCHER
REM ═══════════════════════════════════════════════════════════════════════════════

echo.
echo ============================================================
echo   ALPHA WICK ENGINE
echo   Real-time wick detection with full feature extraction
echo ============================================================
echo.

cd /d "%~dp0"

REM Check Python
python --version >nul 2>&1
if errorlevel 1 (
    echo [ERROR] Python not found!
    pause
    exit /b 1
)

echo [INFO] Starting ALPHA Wick Engine...
echo [INFO] Symbols: BTC-USDT, ETH-USDT, SOL-USDT
echo [INFO] Data output: data/
echo.
echo Press Ctrl+C to stop
echo.

python main_collector.py

pause

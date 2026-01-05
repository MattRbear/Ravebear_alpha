@echo off
REM Build a standalone exe for Wick Engine v2 using PyInstaller.

REM Change to project root (one level up from scripts folder)
cd /d "%~dp0.."

REM Ensure PyInstaller is installed
python -m pip install --upgrade pyinstaller

REM Optional: create a "dist" directory if it doesn't exist
if not exist "dist" mkdir dist

REM Build the executable
pyinstaller ^
  --onefile ^
  --name wick_engine_collector ^
  --distpath dist ^
  run_collector.py

echo.
echo Build complete.
echo Executable should be at: %CD%\dist\wick_engine_collector.exe
pause

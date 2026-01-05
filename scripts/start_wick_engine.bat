@echo off
REM Start the Wick Engine v2 collector from the built exe.

REM Resolve project root (one level up from scripts folder)
cd /d "%~dp0.."

SET EXE_PATH=dist\wick_engine_collector.exe

IF NOT EXIST "%EXE_PATH%" (
    echo Executable not found at %EXE_PATH%.
    echo Run scripts\build_exe.bat first.
    pause
    exit /b 1
)

echo Starting Wick Engine v2 collector...
echo (Close this window or press Ctrl+C to stop.)

"%EXE_PATH%"

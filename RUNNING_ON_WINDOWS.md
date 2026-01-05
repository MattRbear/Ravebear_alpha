# Running Wick Engine v2 on Windows

## How to Build the Executable
1. Open the initialized project folder.
2. Run `scripts\build_exe.bat`.
   - This will install PyInstaller (if missing) and build the `.exe`.
   - The output will be in `dist\wick_engine_collector.exe`.

## How to Start the Collector
1. Run `scripts\start_wick_engine.bat`.
   - This launches the collector using the built executable.
   - You can create a Desktop Shortcut to this batch file for easy access.

## Notes
- To stop the collector, press `Ctrl+C` or close the terminal window.
- The collector logs to the console and writes data to the `data/` directory relative to where the script is run.

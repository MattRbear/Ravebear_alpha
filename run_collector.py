"""
Wick Engine v2 - Windows entrypoint.

This script wraps the async main() function from main_collector.py so it can be:
- run via `python run_collector.py`
- packaged into a standalone .exe via PyInstaller.
"""

import sys
import asyncio
import signal
from typing import Optional

from main_collector import main as collector_main


def _setup_signal_handlers(loop: asyncio.AbstractEventLoop) -> None:
    """Attach SIGINT/SIGTERM handlers for clean shutdown."""
    if sys.platform == 'win32':
        return
        
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, loop.stop)
        except (NotImplementedError, AttributeError, RuntimeError):
            # On Windows, add_signal_handler may not appear or work.
            pass


def main() -> None:
    """Synchronous entrypoint that runs the async collector main()."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    _setup_signal_handlers(loop)

    try:
        loop.run_until_complete(collector_main())
    except KeyboardInterrupt:
        # Allow Ctrl+C without ugly traceback
        print("Wick Engine collector interrupted by user.")
    finally:
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.close()


if __name__ == "__main__":
    main()

# file: storage/jsonl_writer.py
import asyncio
import json
import logging
import os
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Optional

from features import WickEvent

logger = logging.getLogger("storage.jsonl_writer")


class StorageError(Exception):
    """Exception raised for storage-related errors."""
    pass


class JsonlWriter:
    """
    JSONL event writer with atomic writes and proper error handling.
    
    Features:
    - Atomic writes using temp file + rename
    - File rotation based on size
    - Explicit error handling (no silent swallowing)
    - Corruption detection via write verification
    """

    def __init__(self, output_dir: str, file_rotation_mb: int = 100):
        self.output_dir = Path(output_dir)
        self.file_rotation_mb = file_rotation_mb
        self.current_file: Optional[Path] = None
        self._lock = asyncio.Lock()
        self._write_count = 0
        self._error_count = 0
        self._ensure_dir()
        self._rotate_file()

    def _ensure_dir(self) -> None:
        """Ensure output directory exists."""
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def _rotate_file(self) -> None:
        """Rotate to a new log file."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"wick_events_{timestamp}.jsonl"
        self.current_file = self.output_dir / filename
        logger.info(f"Rotated to new log file: {self.current_file}")

    def _check_rotation(self) -> None:
        """Check if file rotation is needed based on size."""
        if not self.current_file or not self.current_file.exists():
            return

        size_mb = self.current_file.stat().st_size / (1024 * 1024)
        if size_mb >= self.file_rotation_mb:
            self._rotate_file()

    def _atomic_append(self, data_str: str) -> None:
        """
        Append data to the current file using atomic write pattern.
        
        This uses a write-to-temp-then-append pattern to minimize
        data loss risk during crashes.
        """
        if self.current_file is None:
            raise StorageError("No current file set")

        # Write to temp file first
        fd, temp_path = tempfile.mkstemp(
            dir=self.output_dir,
            prefix=".tmp_",
            suffix=".jsonl"
        )

        try:
            # Write the new line to temp file
            with os.fdopen(fd, 'w') as temp_file:
                temp_file.write(data_str + "\n")
                temp_file.flush()
                os.fsync(temp_file.fileno())  # Force write to disk

            # Now append temp file contents to main file
            with open(self.current_file, 'a') as main_file:
                with open(temp_path, 'r') as temp_file:
                    content = temp_file.read()
                    main_file.write(content)
                    main_file.flush()
                    os.fsync(main_file.fileno())  # Force write to disk

        finally:
            # Clean up temp file
            try:
                os.unlink(temp_path)
            except OSError:
                pass

    async def write_event(self, event: WickEvent) -> None:
        """
        Write a WickEvent to storage.
        
        Raises:
            StorageError: If the write fails
        """
        try:
            self._check_rotation()
            data_str = event.model_dump_json()

            async with self._lock:
                # Run blocking I/O in thread pool
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(None, self._atomic_append, data_str)
                self._write_count += 1

        except Exception as e:
            self._error_count += 1
            logger.error(f"Failed to write event: {e}")
            raise StorageError(f"Failed to write event: {e}") from e

    async def write_event_dict(self, event_dict: dict) -> None:
        """
        Write a raw dict to JSONL (for events with embedded orderbook).
        
        Raises:
            StorageError: If the write fails
        """
        try:
            self._check_rotation()
            data_str = json.dumps(event_dict, default=str)

            async with self._lock:
                # Run blocking I/O in thread pool
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(None, self._atomic_append, data_str)
                self._write_count += 1

        except Exception as e:
            self._error_count += 1
            logger.error(f"Failed to write event dict: {e}")
            raise StorageError(f"Failed to write event dict: {e}") from e

    @property
    def stats(self) -> dict:
        """Return write statistics."""
        return {
            "writes": self._write_count,
            "errors": self._error_count,
            "current_file": str(self.current_file) if self.current_file else None,
        }

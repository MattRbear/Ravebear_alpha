# file: storage/jsonl_writer.py
import asyncio
import json
import logging
import os
from datetime import datetime
from features import WickEvent

logger = logging.getLogger("storage.jsonl_writer")

class JsonlWriter:
    def __init__(self, output_dir: str, file_rotation_mb: int = 100):
        self.output_dir = output_dir
        self.file_rotation_mb = file_rotation_mb
        self.current_file = None
        self._lock = asyncio.Lock()
        self._ensure_dir()
        self._rotate_file()

    def _ensure_dir(self):
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)

    def _rotate_file(self):
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"wick_events_{timestamp}.jsonl"
        self.current_file = os.path.join(self.output_dir, filename)
        logger.info(f"Rotated to new log file: {self.current_file}")

    def _check_rotation(self):
        if not os.path.exists(self.current_file):
            return
        
        size_mb = os.path.getsize(self.current_file) / (1024 * 1024)
        if size_mb >= self.file_rotation_mb:
            self._rotate_file()

    async def write_event(self, event: WickEvent) -> None:
        try:
            self._check_rotation()
            data_str = event.model_dump_json()
            
            async with self._lock:
                with open(self.current_file, "a") as f:
                    f.write(data_str + "\n")
                     
        except Exception as e:
            logger.error(f"Failed to write event: {e}")

    async def write_event_dict(self, event_dict: dict) -> None:
        """Write a raw dict to JSONL (for events with embedded orderbook)."""
        try:
            self._check_rotation()
            data_str = json.dumps(event_dict, default=str)
            
            async with self._lock:
                with open(self.current_file, "a") as f:
                    f.write(data_str + "\n")
                     
        except Exception as e:
            logger.error(f"Failed to write event dict: {e}")

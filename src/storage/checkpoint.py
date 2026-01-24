"""
Checkpoint management for incremental loads and resume capability.

Tracks:
- High watermarks for incremental extraction
- Offset/cursor positions for pagination resume
- Seen hashes/IDs for deduplication

Performance optimizations:
- LRU cache with configurable max size to bound memory usage
- Automatic eviction of least recently used checkpoints
"""

from __future__ import annotations

import json
import logging
from collections import OrderedDict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import UUID

from ..schemas.metadata import Checkpoint

logger = logging.getLogger(__name__)

# Default maximum number of checkpoints to keep in memory
DEFAULT_MAX_CACHE_SIZE = 100


class CheckpointManager:
    """
    Manages checkpoints for incremental loading and resume.

    Checkpoints are persisted to JSON files and loaded at
    scraper startup to enable:
    - Resuming after interruption
    - Incremental loads using watermarks
    - Deduplication via seen hashes

    Performance: Uses LRU cache with configurable max size to
    prevent unbounded memory growth in long-running sessions.
    """

    def __init__(self, checkpoint_path: Path, max_cache_size: int = DEFAULT_MAX_CACHE_SIZE):
        """
        Initialize the checkpoint manager.

        Args:
            checkpoint_path: Directory for checkpoint files
            max_cache_size: Maximum number of checkpoints to keep in memory (default: 100)
        """
        self._path = checkpoint_path
        self._path.mkdir(parents=True, exist_ok=True)
        # Use OrderedDict for LRU behavior
        self._checkpoints: OrderedDict[str, Checkpoint] = OrderedDict()
        self._max_cache_size = max_cache_size
        self._logger = logging.getLogger(__name__)

    def _evict_if_needed(self) -> None:
        """Evict least recently used checkpoints if cache exceeds max size."""
        while len(self._checkpoints) > self._max_cache_size:
            # Remove oldest (first) item
            evicted_key, _ = self._checkpoints.popitem(last=False)
            self._logger.debug(f"Evicted checkpoint from cache: {evicted_key}")

    def _touch(self, source_name: str) -> None:
        """Move a checkpoint to the end (most recently used)."""
        if source_name in self._checkpoints:
            self._checkpoints.move_to_end(source_name)

    def _checkpoint_file(self, source_name: str) -> Path:
        """Get the checkpoint file path for a source."""
        return self._path / f"{source_name}.json"

    def load(self, source_name: str) -> Checkpoint | None:
        """
        Load checkpoint for a source.

        Args:
            source_name: Source identifier

        Returns:
            Checkpoint if exists, None otherwise
        """
        # Check cache first (and mark as recently used)
        if source_name in self._checkpoints:
            self._touch(source_name)
            return self._checkpoints[source_name]

        filepath = self._checkpoint_file(source_name)
        if not filepath.exists():
            return None

        try:
            with open(filepath) as f:
                data = json.load(f)

            # Convert JSON to Checkpoint
            # Handle sets stored as lists
            if "seen_hashes" in data:
                data["seen_hashes"] = set(data["seen_hashes"])
            if "seen_ids" in data:
                data["seen_ids"] = set(data["seen_ids"])

            # Parse datetimes - use single lookup instead of double
            for field in ["last_run_completed_at", "high_watermark", "updated_at"]:
                value = data.get(field)
                if value:
                    data[field] = datetime.fromisoformat(value)

            # Parse UUIDs - use single lookup instead of double
            for field in ["checkpoint_id", "last_successful_run_id"]:
                value = data.get(field)
                if value:
                    data[field] = UUID(value)

            checkpoint = Checkpoint(**data)
            self._checkpoints[source_name] = checkpoint
            self._evict_if_needed()
            self._logger.info(f"Loaded checkpoint for {source_name}")
            return checkpoint

        except Exception as e:
            self._logger.error(f"Failed to load checkpoint for {source_name}: {e}")
            return None

    def save(self, checkpoint: Checkpoint) -> None:
        """
        Save a checkpoint.

        Args:
            checkpoint: Checkpoint to save
        """
        filepath = self._checkpoint_file(checkpoint.source_name)

        # Convert to JSON-serializable format
        data = {
            "source_name": checkpoint.source_name,
            "checkpoint_id": str(checkpoint.checkpoint_id),
            "last_successful_run_id": str(checkpoint.last_successful_run_id) if checkpoint.last_successful_run_id else None,
            "last_run_completed_at": checkpoint.last_run_completed_at.isoformat() if checkpoint.last_run_completed_at else None,
            "high_watermark": checkpoint.high_watermark.isoformat() if checkpoint.high_watermark else None,
            "last_offset": checkpoint.last_offset,
            "last_page": checkpoint.last_page,
            "last_cursor": checkpoint.last_cursor,
            "seen_hashes": list(checkpoint.seen_hashes)[-10000:],  # Keep last 10K
            "seen_ids": list(checkpoint.seen_ids)[-10000:],
            "updated_at": checkpoint.updated_at.isoformat(),
        }

        with open(filepath, "w") as f:
            json.dump(data, f, indent=2)

        self._checkpoints[checkpoint.source_name] = checkpoint
        self._touch(checkpoint.source_name)
        self._evict_if_needed()
        self._logger.debug(f"Saved checkpoint for {checkpoint.source_name}")

    def create_or_load(self, source_name: str) -> Checkpoint:
        """
        Get existing checkpoint or create a new one.

        Args:
            source_name: Source identifier

        Returns:
            Checkpoint (existing or new)
        """
        checkpoint = self.load(source_name)
        if checkpoint is None:
            checkpoint = Checkpoint(source_name=source_name)
            self._checkpoints[source_name] = checkpoint
            self._evict_if_needed()
        return checkpoint

    def update_after_run(
        self,
        source_name: str,
        run_id: UUID,
        *,
        high_watermark: datetime | None = None,
        last_offset: int | None = None,
        last_page: int | None = None,
        last_cursor: str | None = None,
    ) -> Checkpoint:
        """
        Update checkpoint after a successful run.

        Args:
            source_name: Source identifier
            run_id: Completed run ID
            high_watermark: New high watermark
            last_offset: Last offset processed
            last_page: Last page processed
            last_cursor: Last cursor position

        Returns:
            Updated checkpoint
        """
        checkpoint = self.create_or_load(source_name)

        checkpoint.last_successful_run_id = run_id
        checkpoint.last_run_completed_at = datetime.now(timezone.utc)

        if high_watermark:
            checkpoint.update_watermark(high_watermark)
        if last_offset is not None:
            checkpoint.last_offset = last_offset
        if last_page is not None:
            checkpoint.last_page = last_page
        if last_cursor is not None:
            checkpoint.last_cursor = last_cursor

        self.save(checkpoint)
        return checkpoint

    def clear(self, source_name: str) -> None:
        """
        Clear checkpoint for a source (for full reload).

        Args:
            source_name: Source identifier
        """
        filepath = self._checkpoint_file(source_name)
        if filepath.exists():
            filepath.unlink()

        if source_name in self._checkpoints:
            del self._checkpoints[source_name]

        self._logger.info(f"Cleared checkpoint for {source_name}")

    def list_checkpoints(self) -> list[str]:
        """List all sources with checkpoints."""
        return [
            f.stem for f in self._path.glob("*.json")
        ]

    def get_summary(self) -> dict[str, dict[str, Any]]:
        """Get summary of all checkpoints."""
        summary = {}
        for source_name in self.list_checkpoints():
            checkpoint = self.load(source_name)
            if checkpoint:
                summary[source_name] = {
                    "last_run": checkpoint.last_run_completed_at.isoformat() if checkpoint.last_run_completed_at else None,
                    "high_watermark": checkpoint.high_watermark.isoformat() if checkpoint.high_watermark else None,
                    "seen_records": len(checkpoint.seen_hashes),
                    "last_offset": checkpoint.last_offset,
                }
        return summary

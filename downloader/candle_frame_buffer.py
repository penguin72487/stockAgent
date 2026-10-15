"""Bounded Python and columnar staging for long-running candle downloads.

Provider pages contain Python lists of strings.  Keeping years of those pages
alive per symbol makes worker count multiply Python-object overhead.  Convert
small batches to the provider's existing columnar schema while requests run.
Spill large histories to private temporary IPC files; final materialization is
still required by the canonical consumer's DataFrame API. Non-overlapping,
sorted batches need neither a global hash deduplication nor a global sort.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable
from typing import Any
from pathlib import Path
import tempfile

import polars as pl


class CandleFrameBuffer:
    def __init__(
        self,
        normalize: Callable[[list[list[Any]]], pl.DataFrame],
        *,
        max_pending_rows: int = 50_000,
        max_resident_bytes: int = 64 << 20,
        scratch_dir: str | Path | None = None,
    ) -> None:
        if max_pending_rows < 1:
            raise ValueError("max_pending_rows must be positive")
        if max_resident_bytes < 1:
            raise ValueError("max_resident_bytes must be positive")
        self._normalize = normalize
        self._max_pending_rows = max_pending_rows
        self._pending: list[list[Any]] = []
        self._frames: list[pl.DataFrame] = []
        self._max_resident_bytes = max_resident_bytes
        self._scratch_dir = scratch_dir
        self._temporary: tempfile.TemporaryDirectory | None = None
        self._spilled: list[Path] = []
        self._ranges: list[tuple[Any, Any] | None] = []
        self.resident_columnar_bytes = 0
        self.peak_staging_bytes = 0
        self.spilled_parts = 0
        self.used_disjoint_fast_path = False

    def extend(self, rows: Iterable[list[Any]]) -> None:
        try:
            for row in rows:
                self._pending.append(row)
                if len(self._pending) >= self._max_pending_rows:
                    self._flush()
        except Exception:
            self.close()
            raise

    def _flush(self) -> None:
        if not self._pending:
            return
        frame = self._normalize(self._pending)
        self._pending = []
        if not frame.is_empty():
            self._frames.append(frame)
            dates = frame["date"]
            self._ranges.append(
                (dates[0], dates[-1])
                if dates.null_count() == 0 and dates.is_sorted()
                and dates.n_unique() == frame.height else None
            )
            self.resident_columnar_bytes += frame.estimated_size()
            self.peak_staging_bytes = max(self.peak_staging_bytes, self.resident_columnar_bytes)
            if self.resident_columnar_bytes >= self._max_resident_bytes:
                self._spill()

    def _spill(self) -> None:
        if self._temporary is None:
            self._temporary = tempfile.TemporaryDirectory(
                prefix="stockagent-candles-", dir=self._scratch_dir,
            )
        for frame in self._frames:
            path = Path(self._temporary.name) / f"{len(self._spilled):08d}.arrow"
            # No mmap survives cleanup; source receipts/cursors are unaffected.
            frame.write_ipc(path, compression="uncompressed")
            self._spilled.append(path)
            self.spilled_parts += 1
        self._frames.clear()
        self.resident_columnar_bytes = 0

    def close(self) -> None:
        self._frames.clear()
        self._spilled.clear()
        self._ranges.clear()
        self._pending.clear()
        self.resident_columnar_bytes = 0
        if self._temporary is not None:
            self._temporary.cleanup()
            self._temporary = None

    def __del__(self) -> None:
        # Also reclaim private scratch if HTTP/normalization aborts before finish.
        if getattr(self, "_temporary", None) is not None:
            self.close()

    def finish(self) -> pl.DataFrame:
        try:
            self._flush()
            parts: list[Path | pl.DataFrame] = [*self._spilled, *self._frames]
            if not parts:
                return self._normalize([])
            order = list(range(len(parts)))
            if len(parts) > 1 and all(bounds is not None for bounds in self._ranges):
                order.sort(key=lambda index: self._ranges[index][0])
                self.used_disjoint_fast_path = all(
                    self._ranges[left][1] < self._ranges[right][0]
                    for left, right in zip(order, order[1:])
                )
            else:
                self.used_disjoint_fast_path = False
            if not self.used_disjoint_fast_path:
                order = list(range(len(parts)))
            frames = [pl.read_ipc(parts[index], memory_map=False, rechunk=False)
                      if isinstance(parts[index], Path) else parts[index] for index in order]
            if len(frames) == 1:
                return frames[0]
            frame = pl.concat(frames, how="vertical", rechunk=False)
            if self.used_disjoint_fast_path:
                return frame
            # Overlaps retain arrival order: later source pages win, including
            # corrections at page/spill boundaries and head + tail requests.
            return frame.unique(subset=["date"], keep="last", maintain_order=True).sort("date")
        finally:
            self.close()

"""Bounded Python-row staging for long-running candle downloads.

Provider pages contain Python lists of strings.  Keeping years of those pages
alive per symbol makes worker count multiply Python-object overhead.  Convert
small batches to the provider's existing columnar schema while requests run;
only the final, compact columnar frames remain resident.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable
from typing import Any

import polars as pl


class CandleFrameBuffer:
    def __init__(
        self,
        normalize: Callable[[list[list[Any]]], pl.DataFrame],
        *,
        max_pending_rows: int = 50_000,
    ) -> None:
        if max_pending_rows < 1:
            raise ValueError("max_pending_rows must be positive")
        self._normalize = normalize
        self._max_pending_rows = max_pending_rows
        self._pending: list[list[Any]] = []
        self._frames: list[pl.DataFrame] = []

    def extend(self, rows: Iterable[list[Any]]) -> None:
        for row in rows:
            self._pending.append(row)
            if len(self._pending) >= self._max_pending_rows:
                self._flush()

    def _flush(self) -> None:
        if not self._pending:
            return
        frame = self._normalize(self._pending)
        self._pending = []
        if not frame.is_empty():
            self._frames.append(frame)

    def finish(self) -> pl.DataFrame:
        self._flush()
        frames, self._frames = self._frames, []
        if not frames:
            return self._normalize([])
        if len(frames) == 1:
            return frames[0]
        # Each provider normalizer deduplicates within a batch.  A page or
        # batch boundary can still repeat a minute; later pages win, as in the
        # original whole-symbol normalization.
        return (
            pl.concat(frames, how="vertical", rechunk=False)
            .unique(subset=["date"], keep="last", maintain_order=True)
            .sort("date")
        )

"""Coherent live reads of the canonical mutable Taiwan public source tree.

The producer already owns an exclusive ``tw-public-refresh.lock`` across its
dependent builds. Live readers must share that lock, not interpret an in-flight
reference/entitlement pair as a completed generation. No receipt check is relaxed.
"""

from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

try:
    import fcntl
except ImportError:  # pragma: no cover - canonical mutable owner is POSIX
    fcntl = None


class TWPublicSourceUpdatePending(RuntimeError):
    """A real exclusive producer, not a stale progress file, owns the inputs."""

    def __init__(self, root: Path, lock_path: Path):
        self.source_root = root
        self.lock_path = lock_path
        self.details = {
            "reason": "canonical_tw_public_source_update_in_progress",
            "source_root": str(root),
            "source_lock": str(lock_path),
            "producer_lock_held": True,
            "retryable": True,
        }
        super().__init__(
            "台股官方來源正在更新相依資料，等待一致版本完成；"
            "尚未執行模型，不是資料已驗收。"
        )


@contextmanager
def live_tw_public_source_read_guard(parquet_root: str | Path) -> Iterator[None]:
    """Hold one nonblocking shared lease through panel/cache source reads.

    Only registered mutable TW trees participate. Unmanaged fixtures, immutable
    releases and other markets keep their existing readers. No provider call,
    file hashing, sleep or global inference mutex is added to this check.
    """

    root = Path(parquet_root).resolve().parent
    lock_path = root.parent / ".locks" / "tw-public-refresh.lock"
    is_tw_root = any(
        (root / name).exists()
        for name in (
            "tw_corporate_action_reference.parquet",
            "twse_daily_ohlcv.parquet",
        )
    )
    if not is_tw_root or not lock_path.parent.is_dir():
        yield
        return
    if fcntl is None:
        raise RuntimeError("canonical mutable TW sources require POSIX read/write locking")
    with lock_path.open("a+") as handle:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_SH | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise TWPublicSourceUpdatePending(root, lock_path) from exc
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)

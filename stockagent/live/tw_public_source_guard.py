"""Coherent live reads of the canonical mutable Taiwan public source tree.

The producer already owns an exclusive ``tw-public-refresh.lock`` across its
dependent builds. Live readers must share that lock, not interpret an in-flight
reference/entitlement pair as a completed generation. No receipt check is relaxed.
"""

from __future__ import annotations

from contextlib import contextmanager
from concurrent.futures import Future
import json
import os
from pathlib import Path
import threading
import time
from typing import Any, Callable, Iterator

from stockagent.data_sync.completed_forward_capacity import CapacityDeferred

try:
    import fcntl
except ImportError:  # pragma: no cover - canonical mutable owner is POSIX
    fcntl = None


class _ReadGeneration:
    """Immutable scan results shared only while overlapping OS readers exist."""

    def __init__(self):
        self.readers = 0
        self.values: dict[Any, Future] = {}
        self.lock = threading.Lock()

    def get(self, key: Any, build: Callable[[], Any]) -> Any:
        with _READ_GENERATIONS_LOCK:
            if not self.readers:
                raise RuntimeError("source read generation used after its OS leases ended")
        with self.lock:
            future = self.values.get(key)
            first = future is None
            if first:
                future = self.values[key] = Future()
        if first:
            try:
                future.set_result(build())
            except BaseException as exc:
                future.set_exception(exc)
        return future.result()


_READ_GENERATIONS: dict[tuple, _ReadGeneration] = {}
_READ_GENERATIONS_LOCK = threading.Lock()


class TWPublicSourceUpdatePending(RuntimeError):
    """A real exclusive producer, not a stale progress file, owns the inputs."""

    def __init__(self, root: Path, lock_path: Path, *, observed_in_child: bool = False):
        self.source_root = root
        self.lock_path = lock_path
        self.details = {"source_root": str(root), "source_lock": str(lock_path),
                        "retryable": True}
        if observed_in_child:
            # The writer may have released the lock after the child exited.
            # Report the observation, not an unverified current lock state.
            self.details.update({
                "reason": "canonical_tw_public_source_update_observed_in_child",
                "producer_lock_held_at_child_observation": True,
                "current_lock_state": "unverified",
            })
            message = "正式歷史子程序觀察到官方來源寫入鎖；等待後重試，尚未驗收。"
        else:
            self.details.update({
                "reason": "canonical_tw_public_source_update_in_progress",
                "producer_lock_held": True,
            })
            message = "台股官方來源正在更新相依資料，等待一致版本完成；尚未執行模型，不是資料已驗收。"
        super().__init__(message)


_PENDING_PIPE_ENV = "STOCKAGENT_TW_SOURCE_PENDING_PIPE"
_PENDING_TOKEN_ENV = "STOCKAGENT_TW_SOURCE_PENDING_TOKEN"
_PENDING_RECEIPT_LIMIT = 4096


def report_maintenance_pending_to_parent(exc: TWPublicSourceUpdatePending | CapacityDeferred) -> bool:
    """Report a *typed* child failure through the parent's private pipe.

    Normal CLI failures retain their original traceback/exit status. A failed
    or oversized report is ignored so it can never hide the original error.
    """

    try:
        fd = int(os.environ.get(_PENDING_PIPE_ENV, ""))
        token = os.environ.get(_PENDING_TOKEN_ENV, "")
        if fd < 3 or len(token) != 32 or any(c not in "0123456789abcdef" for c in token):
            return False
        record = {"version": 1, "token": token, "observed_at_ns": time.time_ns()}
        if isinstance(exc, TWPublicSourceUpdatePending):
            record.update(kind="tw_public_source_update_pending", source_root=str(exc.source_root),
                          source_lock=str(exc.lock_path))
        elif isinstance(exc, CapacityDeferred):
            # Scalar capacity diagnostics only, never exception text, account
            # credentials or unbounded child output. The original error remains.
            fields = {key: value for key, value in exc.details.items()
                      if key not in {"reason", "contract", "retryable", "allocation_admitted"}
                      and type(value) in (str, int, bool, type(None))}
            record.update(kind="capacity_deferred", reason=exc.details["reason"], fields=fields)
        else:
            return False
        payload = json.dumps(record, separators=(",", ":"), allow_nan=False).encode("utf-8")
        if len(payload) > _PENDING_RECEIPT_LIMIT:
            return False
        return os.write(fd, payload) == len(payload)
    except (OSError, TypeError, ValueError, OverflowError):
        return False


def read_maintenance_pending_from_child(fd: int, token: str) -> TWPublicSourceUpdatePending | CapacityDeferred | None:
    """Accept only a bounded complete receipt from the current invocation."""

    try:
        os.set_blocking(fd, False)
        payload = os.read(fd, _PENDING_RECEIPT_LIMIT + 1)
        if not payload or len(payload) > _PENDING_RECEIPT_LIMIT:
            return None
        record = json.loads(payload)
        if (not isinstance(record, dict) or type(record.get("version")) is not int
                or record["version"] != 1 or record.get("token") != token
                or type(record.get("observed_at_ns")) is not int or record["observed_at_ns"] <= 0):
            return None
        if record.get("kind") == "capacity_deferred":
            if set(record) != {"version", "kind", "token", "reason", "fields", "observed_at_ns"}:
                return None
            reason, fields = record["reason"], record["fields"]
            if (not isinstance(reason, str) or not reason or len(reason) > 160
                    or any(c not in "abcdefghijklmnopqrstuvwxyz0123456789_" for c in reason)
                    or not isinstance(fields, dict) or len(fields) > 24
                    or any(not isinstance(key, str) or len(key) > 120
                           or key in {"reason", "contract", "retryable", "allocation_admitted"}
                           or type(value) not in (str, int, bool, type(None))
                           for key, value in fields.items())):
                return None
            return CapacityDeferred(reason, **fields, observed_in_child=True,
                                    child_observed_at_ns=record["observed_at_ns"],
                                    current_capacity_state="unverified")
        if set(record) != {"version", "kind", "token", "source_root", "source_lock", "observed_at_ns"}:
            return None
        root, lock = record["source_root"], record["source_lock"]
        if (type(record["version"]) is not int or record["version"] != 1
                or record["kind"] != "tw_public_source_update_pending"
                or record["token"] != token or type(record["observed_at_ns"]) is not int
                or record["observed_at_ns"] <= 0 or not isinstance(root, str)
                or not isinstance(lock, str) or not Path(root).is_absolute()
                or Path(lock) != Path(root).parent / ".locks" / "tw-public-refresh.lock"):
            return None
        result = TWPublicSourceUpdatePending(Path(root), Path(lock), observed_in_child=True)
        result.details["child_observed_at_ns"] = record["observed_at_ns"]
        return result
    except (BlockingIOError, OSError, UnicodeDecodeError, ValueError, TypeError):
        return None


def report_source_pending_to_parent(exc: TWPublicSourceUpdatePending) -> bool:
    """Compatibility entrypoint for source-only child callers."""
    return report_maintenance_pending_to_parent(exc)


def read_source_pending_from_child(fd: int, token: str) -> TWPublicSourceUpdatePending | None:
    """Source-only compatibility reader; never mislabel capacity as source."""
    pending = read_maintenance_pending_from_child(fd, token)
    return pending if isinstance(pending, TWPublicSourceUpdatePending) else None


@contextmanager
def live_tw_public_source_read_guard(
    parquet_root: str | Path, *, share_generation: bool = False,
) -> Iterator[_ReadGeneration | None]:
    """Hold one nonblocking shared lease through panel/cache source reads.

    Only registered mutable TW trees participate. Unmanaged fixtures, immutable
    releases and other markets keep their existing readers. No provider call,
    file hashing, sleep or global inference mutex is added to this check.
    Opt-in consumers may coalesce immutable member scans across overlapping
    readers of this exact OS lock. The last reader discards every value before
    unlocking: this is a read transaction, never a directory-mtime/TTL cache.
    Outer operational/benchmark leases do not extend this reuse by default.
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
        yield None
        return
    if fcntl is None:
        raise RuntimeError("canonical mutable TW sources require POSIX read/write locking")
    with lock_path.open("a+") as handle:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_SH | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise TWPublicSourceUpdatePending(root, lock_path) from exc
        generation = None
        key = None
        if share_generation:
            identity = os.fstat(handle.fileno())
            key = (os.getpid(), str(root), identity.st_dev, identity.st_ino)
            with _READ_GENERATIONS_LOCK:
                generation = _READ_GENERATIONS.get(key)
                if generation is None:
                    generation = _READ_GENERATIONS[key] = _ReadGeneration()
                generation.readers += 1
        try:
            yield generation
        finally:
            if generation is not None:
                with _READ_GENERATIONS_LOCK:
                    generation.readers -= 1
                    if generation.readers == 0:
                        del _READ_GENERATIONS[key]
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)

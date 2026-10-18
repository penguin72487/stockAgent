"""Build a bounded storage inventory for Shioaji-backed local datasets.

The scanner is intentionally separate from the public HTTP process.  A full
inventory currently touches hundreds of thousands of files, so a systemd timer
refreshes one compact snapshot while dashboard requests remain cheap and
read-only.
"""

from __future__ import annotations

from bisect import bisect_right
from dataclasses import dataclass
from datetime import UTC, datetime, time as daytime, timedelta
import json
import hashlib
import math
import os
from pathlib import Path
import shutil
import stat
import subprocess
import tempfile
import time
from typing import Any, Final, Iterable
from zoneinfo import ZoneInfo

from downloader.artifact_io import atomic_write_bytes
from stockagent.live.shioaji_storage_epoch import history_storage_observation


STORAGE_SCHEMA_VERSION: Final[int] = 1
GROWTH_WINDOW_DAYS: Final[int] = 30
MAX_DAILY_TOTALS: Final[int] = 90
TAIPEI: Final[ZoneInfo] = ZoneInfo("Asia/Taipei")
INVENTORY_CACHE_CONTRACT = "shioaji_storage_epoch_daily_aggregates_v1"
INVENTORY_RECHECK_SECONDS = 24 * 3600


def _inventory_controls(root: Path, observation: dict) -> dict:
    controls = {"producer": observation}
    for name in (".", "download.lock", "manifest.json", "receipts", "ticks"):
        path = root / name
        try:
            info = path.lstat()
        except FileNotFoundError:
            controls[name] = None
        else:
            if stat.S_ISLNK(info.st_mode):
                raise ValueError("redirected inventory control")
            controls[name] = [info.st_dev, info.st_ino, info.st_size,
                              info.st_mtime_ns, info.st_ctime_ns, info.st_mode]
    return controls


def _inventory_digest(value: dict) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, allow_nan=False).encode()).hexdigest()


def _read_inventory_cache(path: Path, *, controls: dict, now: datetime) -> dict | None:
    """Bounded private metadata, never pickle or a followed filesystem link."""
    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        with os.fdopen(descriptor, 'rb') as stream:
            info = os.fstat(stream.fileno())
            if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid()
                    or info.st_mode & 0o077 or info.st_nlink != 1):
                return None
            raw = stream.read(8 * 1024 * 1024 + 1)
        if len(raw) > 8 * 1024 * 1024:
            return None
        cached = json.loads(raw)
        checksum = cached.pop('sha256')
        if (cached['contract'] != INVENTORY_CACHE_CONTRACT or cached['controls'] != controls
                or checksum != _inventory_digest(cached)
                or not 0 <= now.timestamp() - cached['audited_at_unix'] < INVENTORY_RECHECK_SECONDS
                or not isinstance(cached['groups'], list)):
            return None
        for size, mtime, count in cached['groups']:
            if (type(size) is not int or size < 0 or type(count) is not int or count < 1
                    or type(mtime) not in (int, float) or not math.isfinite(mtime)):
                return None
        return cached
    except (OSError, ValueError, TypeError, KeyError, AttributeError, UnicodeError, RecursionError):
        return None


def _cached_history_metadata(root: Path, *, cache_dir: Path, now: datetime,
                             force: bool, stats: dict) -> Iterable[tuple[int, float, int]]:
    """Reuse only an enrolled producer epoch; cache contains no source rows.

    The source protocol fences canonical in-place repairs as well as atomic
    replacements. Out-of-protocol operator edits require --force-full-scan;
    a bounded daily reconciliation also rejects stale/bad cache metadata.
    This cache must never be used for data-quality or training acceptance.
    """
    controls = None
    path = cache_dir / (hashlib.sha256(str(root.absolute()).encode()).hexdigest() + '.json')
    with history_storage_observation(root) as observation:
        if observation is not None:
            try:
                controls = _inventory_controls(root, observation)
            except (OSError, ValueError):
                pass
        cached = _read_inventory_cache(path, controls=controls, now=now) if controls and not force else None
        if cached is not None:
            stats['reused_epoch_roots'] += 1
            stats['avoided_file_stats'] += sum(row[2] for row in cached['groups'])
            stats['oldest_full_audit_unix'] = min(stats['oldest_full_audit_unix'], cached['audited_at_unix'])
            yield from cached['groups']
            return
    # Expensive find/stat work never holds a reader lease against acquisition.
    if controls is None:
        stats['legacy_or_busy_roots'] += 1
        for size, mtime in _iter_file_metadata(root):
            yield size, mtime, 1
        return
    groups = {}
    for size, mtime in _iter_file_metadata(root):
        day = datetime.fromtimestamp(mtime, TAIPEI).date().isoformat()
        row = groups.setdefault(day, [0, mtime, 0])
        row[0] += max(0, size)
        row[1] = max(row[1], mtime)
        row[2] += 1
    stats['scanned_epoch_roots'] += 1
    with history_storage_observation(root) as after:
        try:
            stable = after is not None and controls == _inventory_controls(root, after)
        except (OSError, ValueError):
            stable = False
        if stable:
            cached = {'contract': INVENTORY_CACHE_CONTRACT, 'controls': controls,
                      'audited_at_unix': now.timestamp(), 'groups': list(groups.values())}
            encoded = {**cached, 'sha256': _inventory_digest(cached)}
            try:
                atomic_write_bytes(path, json.dumps(encoded, allow_nan=False).encode(),
                                   mode=0o600, durable=False)
            except OSError:
                stats['cache_write_errors'] += 1
        else:
            # Preserve original best-effort census semantics, never cache an
            # unstable observation as a reusable producer generation.
            stats['changed_during_scan_roots'] += 1
    yield from groups.values()


def _counted_metadata(root: Path, *, cache_dir: Path | None, now: datetime,
                      force: bool, stats: dict) -> Iterable[tuple[int, float, int]]:
    if cache_dir is None or root.is_symlink() or not root.is_dir():
        for size, mtime in _iter_file_metadata(root):
            yield size, mtime, 1
        return
    with os.scandir(root) as entries:
        for entry in entries:
            path = Path(entry.path)
            if entry.is_dir(follow_symlinks=False):
                yield from _cached_history_metadata(path, cache_dir=cache_dir, now=now,
                                                    force=force, stats=stats)
            else:
                for size, mtime in _iter_file_metadata(path):
                    yield size, mtime, 1


@dataclass(frozen=True)
class StorageDatasetSpec:
    """One non-overlapping physical storage group."""

    dataset_id: str
    title: str
    storage_class: str
    quota_class: str
    roots: tuple[Path, ...]
    description: str


def default_storage_datasets(repo_root: Path) -> tuple[StorageDatasetSpec, ...]:
    root = Path(repo_root)
    return (
        StorageDatasetSpec(
            "futures_history",
            "全期貨歷史 Tick",
            "source",
            "historical",
            (
                root / "data_tw_futures/shioaji_history",
                root / "data_tw_index_futures/shioaji_history",
            ),
            "R1/R2 全期貨回補與既有 TXFR1 receipt-backed Tick。",
        ),
        StorageDatasetSpec(
            "stock_minute",
            "全市場股票 1 分 K 原始資料",
            "source",
            "historical",
            (root / "data_tw_minute/shioaji_1m",),
            "逐檔 29 日切片的 Shioaji KBar 原始回補。",
        ),
        StorageDatasetSpec(
            "historical_market_data",
            "週選／月選／實際月份期貨／指數歷史",
            "source",
            "historical",
            (root / "data_tw_shioaji_history",),
            "Contract V2 聯集、逐合約 1 分 K、歷史 Tick 與雜湊 receipt。",
        ),
        StorageDatasetSpec(
            "stock_daily",
            "台股日 K 混合來源",
            "source",
            "historical",
            (root / "data_tw_public/shioaji",),
            "由分鐘 K 棒物化的日 K 與下載進度證據。",
        ),
        StorageDatasetSpec(
            "fop_stream",
            "期貨／選擇權即時 Tick + 五檔",
            "source",
            "realtime",
            (root / "data_tw_index_derivatives_ticks/shioaji_fop_captures",),
            "期貨、微台與選擇權即時事件及一秒簿快照。",
        ),
        StorageDatasetSpec(
            "top200_stream",
            "Top-200 股票即時 Tick + 五檔",
            "source",
            "realtime",
            (root / "data_tw_microstructure/captures",),
            "Top-200 股票逐筆、五檔事件及一秒簿快照。",
        ),
        StorageDatasetSpec(
            "minute_research",
            "股票分鐘研究資料",
            "derived",
            "none",
            (root / "data_tw_minute/research_dataset",),
            "由已落盤分鐘 K 棒產生的因果研究面板。",
        ),
        StorageDatasetSpec(
            "hft_dataset",
            "Top-200 HFT 衍生資料",
            "derived",
            "none",
            (root / "data_tw_microstructure/hft_dataset",),
            "由即時 Tick／五檔重建的特徵與標籤。",
        ),
        StorageDatasetSpec(
            "contract_catalog",
            "Contract V2 合約目錄",
            "reference",
            "none",
            (root / "data_tw_futures/shioaji_contracts",),
            "商品根、R1/R2 連續合約與 target code 目錄。",
        ),
        StorageDatasetSpec(
            "operations",
            "策略狀態、稽核與下載收據",
            "operations",
            "none",
            (
                root / "artifacts/data_capture/shioaji_taifex_bidask",
                root / "artifacts/data_capture/shioaji_top200",
                root / "artifacts/data_repair/shioaji_full",
                root / "artifacts/data_repair/shioaji_futures_history",
                root / "artifacts/data_repair/shioaji_minute_full",
                root / "artifacts/data_repair/shioaji_historical_market_data",
                root / "artifacts/data_repair/shioaji_tx_history",
                root / "artifacts/live/shioaji_taifex_volatility_simulation",
                root / "artifacts/live/shioaji_traffic",
                root / "artifacts/live/tw_day_trade_simulation/state.json",
            ),
            "公開狀態、策略產物、稽核結果與 immutable receipt。",
        ),
    )


def _iter_files(root: Path) -> Iterable[os.stat_result]:
    """Yield lstat results without following symlinks."""

    try:
        if root.is_symlink():
            return
        if root.is_file():
            yield root.stat()
            return
        if not root.is_dir():
            return
    except OSError:
        return

    pending = [os.fspath(root)]
    while pending:
        current = pending.pop()
        try:
            with os.scandir(current) as entries:
                for entry in entries:
                    try:
                        if entry.is_symlink():
                            continue
                        if entry.is_dir(follow_symlinks=False):
                            pending.append(entry.path)
                        elif entry.is_file(follow_symlinks=False):
                            yield entry.stat(follow_symlinks=False)
                    except OSError:
                        continue
        except OSError:
            continue


def _iter_file_metadata(root: Path) -> Iterable[tuple[int, float]]:
    """Stream file metadata without materializing paths or following symlinks.

    GNU find performs the directory walk in native code.  The Python walker is
    retained for hosts without it; a failed native scan is *not* treated as a
    successful partial inventory.
    """

    try:
        if root.is_symlink() or not (root.is_file() or root.is_dir()):
            return
    except OSError:
        return
    # The timer runs as root; never resolve an executable from a writable
    # environment PATH supplied by a shell or Conda activation.
    find_binary = shutil.which("find", path="/usr/bin:/bin")
    if find_binary is None:
        for stat_result in _iter_files(root):
            yield int(stat_result.st_size), float(stat_result.st_mtime)
        return

    with tempfile.TemporaryFile(mode="w+t", encoding="utf-8") as errors:
        process = subprocess.Popen(
            [
                find_binary,
                os.fspath(root.absolute()),
                "-ignore_readdir_race",
                "-type",
                "f",
                "-printf",
                "%s %T@\n",
            ],
            stdout=subprocess.PIPE,
            stderr=errors,
            text=True,
            encoding="ascii",
        )
        try:
            assert process.stdout is not None
            for line in process.stdout:
                try:
                    size_text, mtime_text = line.split()
                    yield int(size_text), float(mtime_text)
                except ValueError as exc:
                    raise RuntimeError("Invalid storage scan metadata") from exc
            if process.wait() != 0:
                errors.seek(0)
                detail = errors.read(512).strip()
                raise RuntimeError(f"Storage scan failed: {detail or process.returncode}")
        finally:
            if process.stdout is not None:
                process.stdout.close()
            if process.poll() is None:
                process.kill()
                process.wait()


def _scan_dataset(
    spec: StorageDatasetSpec,
    *,
    now: datetime,
    inventory_cache: Path | None = None,
    force_full_scan: bool = False,
) -> dict[str, Any]:
    local_now = now.astimezone(TAIPEI)
    today = local_now.date()
    first_day = today - timedelta(days=GROWTH_WINDOW_DAYS)
    days = [first_day + timedelta(days=offset) for offset in range(GROWTH_WINDOW_DAYS)]
    # Compare numeric mtimes against exact local-midnight boundaries. This
    # preserves the prior timezone/day semantics without creating millions of
    # datetime/date objects for files outside (or inside) the 30-day window.
    bounds = [
        datetime.combine(day, daytime.min, tzinfo=TAIPEI).timestamp()
        for day in (*days, today)
    ]
    daily_bytes = [0] * GROWTH_WINDOW_DAYS
    total_bytes = 0
    file_count = 0
    latest_mtime = 0.0
    cache_stats = {'reused_epoch_roots': 0, 'scanned_epoch_roots': 0,
                   'legacy_or_busy_roots': 0, 'avoided_file_stats': 0,
                   'changed_during_scan_roots': 0,
                   'cache_write_errors': 0, 'oldest_full_audit_unix': now.timestamp()}
    selected_cache = inventory_cache if spec.dataset_id == 'futures_history' else None
    for root in spec.roots:
        for file_size, file_mtime, count in _counted_metadata(
                root, cache_dir=selected_cache, now=now, force=force_full_scan, stats=cache_stats):
            size = max(0, file_size)
            total_bytes += size
            file_count += count
            mtime = file_mtime
            latest_mtime = max(latest_mtime, mtime)
            if bounds[0] <= mtime < bounds[-1]:
                daily_bytes[bisect_right(bounds, mtime) - 1] += size
    recent_bytes = sum(daily_bytes)
    active_days = sum(value > 0 for value in daily_bytes)
    result = {
        "id": spec.dataset_id,
        "title": spec.title,
        "storage_class": spec.storage_class,
        "quota_class": spec.quota_class,
        "description": spec.description,
        "bytes": total_bytes,
        "files": file_count,
        "latest_changed_at_utc": (
            datetime.fromtimestamp(latest_mtime, tz=UTC)
            .isoformat()
            .replace("+00:00", "Z")
            if latest_mtime
            else None
        ),
        "growth_window_days": GROWTH_WINDOW_DAYS,
        "growth_window_bytes": recent_bytes,
        "average_daily_growth_bytes": recent_bytes / GROWTH_WINDOW_DAYS,
        "average_active_day_growth_bytes": (
            recent_bytes / active_days if active_days else 0.0
        ),
        "active_growth_days": active_days,
        "growth_source": "file_mtime_estimate",
        "daily_growth": [
            {"date": day.isoformat(), "bytes": value}
            for day, value in zip(days, daily_bytes)
        ],
    }
    if selected_cache is not None:
        result['inventory_reuse'] = {**cache_stats, 'contract': INVENTORY_CACHE_CONTRACT,
                                     'full_recheck_seconds': INVENTORY_RECHECK_SECONDS,
                                     'not_data_integrity_acceptance': True}
    return result


def _previous_daily_totals(previous: dict[str, Any] | None) -> list[dict[str, Any]]:
    rows = (previous or {}).get("daily_totals")
    if not isinstance(rows, list):
        return []
    output: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict) or not isinstance(row.get("date"), str):
            continue
        output.append(
            {
                "date": row["date"],
                "bytes": max(0, int(row.get("bytes") or 0)),
            }
        )
    return output[-MAX_DAILY_TOTALS:]


def build_shioaji_storage_snapshot(
    repo_root: Path,
    *,
    now: datetime | None = None,
    previous: dict[str, Any] | None = None,
    specs: tuple[StorageDatasetSpec, ...] | None = None,
    inventory_cache: Path | None = None,
    force_full_scan: bool = False,
) -> dict[str, Any]:
    observed = now or datetime.now(UTC)
    if observed.tzinfo is None:
        observed = observed.replace(tzinfo=UTC)
    observed = observed.astimezone(UTC)
    started = time.monotonic()
    datasets = []
    for spec in specs or default_storage_datasets(Path(repo_root)):
        dataset_started = time.monotonic()
        row = _scan_dataset(spec, now=observed, inventory_cache=inventory_cache,
                            force_full_scan=force_full_scan)
        row["scan_seconds"] = round(time.monotonic() - dataset_started, 3)
        datasets.append(row)
    total_bytes = sum(int(item["bytes"]) for item in datasets)
    recent_daily: dict[str, int] = {}
    for item in datasets:
        for row in item["daily_growth"]:
            recent_daily[row["date"]] = recent_daily.get(row["date"], 0) + int(
                row["bytes"]
            )
    growth_window_bytes = sum(recent_daily.values())
    average_daily_growth = growth_window_bytes / GROWTH_WINDOW_DAYS
    disk = shutil.disk_usage(Path(repo_root))
    today = observed.astimezone(TAIPEI).date().isoformat()
    historical_totals = [
        row for row in _previous_daily_totals(previous) if row["date"] != today
    ]
    historical_totals.append({"date": today, "bytes": total_bytes})
    historical_totals = historical_totals[-MAX_DAILY_TOTALS:]
    completed_totals = sorted(
        (row for row in historical_totals if row["date"] < today),
        key=lambda row: row["date"],
    )
    observed_average_net_growth: float | None = None
    observed_growth_days = 0
    if len(completed_totals) >= 2:
        first = completed_totals[0]
        last = completed_totals[-1]
        elapsed = (
            datetime.fromisoformat(last["date"]) - datetime.fromisoformat(first["date"])
        ).days
        if elapsed > 0:
            observed_growth_days = elapsed
            observed_average_net_growth = (
                int(last["bytes"]) - int(first["bytes"])
            ) / elapsed
    use_observed_growth = observed_growth_days >= 7
    capacity_growth_estimate = (
        max(0.0, float(observed_average_net_growth or 0.0))
        if use_observed_growth
        else average_daily_growth
    )
    return {
        "schema_version": STORAGE_SCHEMA_VERSION,
        "generated_at_utc": observed.isoformat().replace("+00:00", "Z"),
        "status": "ready",
        "scan_seconds": round(time.monotonic() - started, 3),
        "summary": {
            "datasets": len(datasets),
            "files": sum(int(item["files"]) for item in datasets),
            "total_bytes": total_bytes,
            "source_bytes": sum(
                int(item["bytes"])
                for item in datasets
                if item["storage_class"] in {"source", "reference"}
            ),
            "derived_bytes": sum(
                int(item["bytes"])
                for item in datasets
                if item["storage_class"] == "derived"
            ),
            "operations_bytes": sum(
                int(item["bytes"])
                for item in datasets
                if item["storage_class"] == "operations"
            ),
            "growth_window_days": GROWTH_WINDOW_DAYS,
            "growth_window_bytes": growth_window_bytes,
            "average_daily_growth_bytes": average_daily_growth,
            "growth_source": "file_mtime_estimate",
            "observed_average_daily_net_growth_bytes": observed_average_net_growth,
            "observed_growth_days": observed_growth_days,
            "capacity_growth_estimate_bytes": capacity_growth_estimate,
            "capacity_growth_source": (
                "daily_total_net_growth"
                if use_observed_growth
                else "file_mtime_estimate"
            ),
            "disk_total_bytes": disk.total,
            "disk_used_bytes": disk.used,
            "disk_free_bytes": disk.free,
            "disk_used_ratio": disk.used / disk.total if disk.total else None,
            "estimated_days_remaining": (
                disk.free / capacity_growth_estimate
                if capacity_growth_estimate > 0
                else None
            ),
        },
        "daily_growth": [
            {"date": date, "bytes": value}
            for date, value in sorted(recent_daily.items())
        ],
        "daily_totals": historical_totals,
        "datasets": sorted(datasets, key=lambda item: int(item["bytes"]), reverse=True),
        "definitions": {
            "total_bytes": (
                f"{len(datasets)} 個互不重疊資料群組的實體檔案大小加總；"
                "不跟隨 symlink。"
            ),
            "average_daily_growth": (
                "最近 30 個完整台北曆日內，依檔案最後修改日歸屬的變動檔案完整大小除以 30；"
                "這是寫入活動量，不是淨容量增加量，大量一次性回補或重寫會使數值偏高。"
            ),
            "observed_average_daily_net_growth": (
                "以每日總容量快照首尾差除以曆日；至少累積 7 個完整日後，才取代 mtime 寫入活動量做容量估計。"
            ),
            "estimated_days_remaining": "目前可用磁碟空間除以選定的保守成長估計，僅為容量規劃估計。",
        },
    }


def write_shioaji_storage_snapshot(
    repo_root: Path,
    output_path: Path,
    *,
    now: datetime | None = None,
    force_full_scan: bool = False,
) -> dict[str, Any]:
    selected = Path(output_path)
    selected.parent.mkdir(parents=True, exist_ok=True)
    inventory_cache = selected.parent / '.inventory-epochs'
    inventory_cache.mkdir(mode=0o700, exist_ok=True)
    info = inventory_cache.lstat()
    if (inventory_cache.resolve() != inventory_cache.absolute() or info.st_uid != os.geteuid()
            or info.st_mode & 0o077):
        raise ValueError('inventory cache must be a private unredirected directory')
    try:
        previous = json.loads(selected.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        previous = None
    payload = build_shioaji_storage_snapshot(
        Path(repo_root),
        now=now,
        previous=previous if isinstance(previous, dict) else None,
        inventory_cache=inventory_cache,
        force_full_scan=force_full_scan,
    )
    selected.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(
        prefix=f".{selected.name}.", dir=selected.parent
    )
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, selected)
    finally:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass
    return payload


__all__ = [
    "GROWTH_WINDOW_DAYS",
    "STORAGE_SCHEMA_VERSION",
    "StorageDatasetSpec",
    "build_shioaji_storage_snapshot",
    "default_storage_datasets",
    "write_shioaji_storage_snapshot",
]

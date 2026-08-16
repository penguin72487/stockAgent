"""Build a bounded storage inventory for Shioaji-backed local datasets.

The scanner is intentionally separate from the public HTTP process.  A full
inventory currently touches hundreds of thousands of files, so a systemd timer
refreshes one compact snapshot while dashboard requests remain cheap and
read-only.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
import json
import os
from pathlib import Path
import shutil
import tempfile
import time
from typing import Any, Final, Iterable
from zoneinfo import ZoneInfo


STORAGE_SCHEMA_VERSION: Final[int] = 1
GROWTH_WINDOW_DAYS: Final[int] = 30
MAX_DAILY_TOTALS: Final[int] = 90
TAIPEI: Final[ZoneInfo] = ZoneInfo("Asia/Taipei")


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

    pending = [root]
    while pending:
        current = pending.pop()
        try:
            with os.scandir(current) as entries:
                for entry in entries:
                    try:
                        if entry.is_symlink():
                            continue
                        if entry.is_dir(follow_symlinks=False):
                            pending.append(Path(entry.path))
                        elif entry.is_file(follow_symlinks=False):
                            yield entry.stat(follow_symlinks=False)
                    except OSError:
                        continue
        except OSError:
            continue


def _scan_dataset(
    spec: StorageDatasetSpec,
    *,
    now: datetime,
) -> dict[str, Any]:
    local_now = now.astimezone(TAIPEI)
    today = local_now.date()
    first_day = today - timedelta(days=GROWTH_WINDOW_DAYS)
    daily = {
        (first_day + timedelta(days=offset)).isoformat(): 0
        for offset in range(GROWTH_WINDOW_DAYS)
    }
    total_bytes = 0
    file_count = 0
    latest_mtime = 0.0
    for root in spec.roots:
        for stat_result in _iter_files(root):
            size = max(0, int(stat_result.st_size))
            total_bytes += size
            file_count += 1
            latest_mtime = max(latest_mtime, float(stat_result.st_mtime))
            changed_date = (
                datetime.fromtimestamp(stat_result.st_mtime, tz=UTC)
                .astimezone(TAIPEI)
                .date()
            )
            if first_day <= changed_date < today:
                daily[changed_date.isoformat()] += size
    recent_bytes = sum(daily.values())
    active_days = sum(value > 0 for value in daily.values())
    return {
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
            {"date": date, "bytes": value} for date, value in daily.items()
        ],
    }


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
) -> dict[str, Any]:
    observed = now or datetime.now(UTC)
    if observed.tzinfo is None:
        observed = observed.replace(tzinfo=UTC)
    observed = observed.astimezone(UTC)
    started = time.monotonic()
    datasets = [
        _scan_dataset(spec, now=observed)
        for spec in (specs or default_storage_datasets(Path(repo_root)))
    ]
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
            "total_bytes": "九個互不重疊資料群組的實體檔案大小加總；不跟隨 symlink。",
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
) -> dict[str, Any]:
    selected = Path(output_path)
    try:
        previous = json.loads(selected.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        previous = None
    payload = build_shioaji_storage_snapshot(
        Path(repo_root),
        now=now,
        previous=previous if isinstance(previous, dict) else None,
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

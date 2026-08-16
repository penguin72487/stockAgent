"""Public-safe, source-backed status for every registered data collection.

The physical registry remains ``configs/data_sync/packed_datasets.json``.  This
module projects that registry together with the existing Shioaji/OpenBB status
builders and receipt-backed dataset manifests.  It intentionally distinguishes
freshness, historical completeness, process activity, and ETA; those concepts
must not be collapsed into one optimistic health flag.
"""

from __future__ import annotations

import csv
from datetime import UTC, datetime, timedelta
import json
import math
from pathlib import Path
import re
import subprocess
from typing import Any, Final, Iterable, Mapping

from stockagent.live.openbb_archive_dashboard import build_openbb_public_status
from stockagent.live.shioaji_api_dashboard import build_shioaji_public_status


DATA_MONITOR_SCHEMA_VERSION: Final[int] = 1

_GROUP_META: Final[dict[str, dict[str, Any]]] = {
    "tw-public": {
        "title": "臺灣官方公開資料",
        "provider": "TWSE / TPEx / MOPS / CBC / TDCC",
        "cadence": "每個交易日收盤後",
        "owner": "不可變快照更新器",
        "window": 72 * 3600,
    },
    "tw-minute-train": {
        "title": "台股一分鐘研究資料",
        "provider": "永豐 Shioaji",
        "cadence": "交易日持續增量",
        "owner": "Shioaji 分鐘資料建置器",
        "window": 72 * 3600,
    },
    "tw-minute-source-cold": {
        "title": "台股一分鐘原始分片",
        "provider": "永豐 Shioaji",
        "cadence": "交易日持續增量",
        "owner": "Shioaji 分鐘回補器",
        "window": 72 * 3600,
    },
    "tw-microstructure-train": {
        "title": "台股微結構研究資料",
        "provider": "永豐 Shioaji",
        "cadence": "每盤落盤後",
        "owner": "微結構資料建置器",
        "window": 7 * 86400,
    },
    "tw-microstructure-captures-cold": {
        "title": "股票／期權即時 Tick 與五檔",
        "provider": "永豐 Shioaji",
        "cadence": "盤中連續",
        "owner": "Shioaji 即時擷取服務",
        "window": 7 * 86400,
    },
    "openbb-compact": {
        "title": "OpenBB 驗證封存",
        "provider": "OpenBB 多供應商",
        "cadence": "持續回補",
        "owner": "OpenBB 封存服務",
        "window": 15 * 60,
    },
    "openbb-task-shards-local": {
        "title": "OpenBB 可續傳任務分片",
        "provider": "OpenBB 多供應商",
        "cadence": "持續回補",
        "owner": "OpenBB 封存服務",
        "window": 15 * 60,
    },
    "yahoo-market": {
        "title": "Yahoo 市場資料",
        "provider": "yfinance / Yahoo Finance",
        "cadence": "每日與盤前補齊",
        "owner": "Yahoo OHLCV 更新器",
        "window": 72 * 3600,
    },
    "okx": {
        "title": "OKX 永續合約",
        "provider": "OKX",
        "cadence": "15 分鐘增量",
        "owner": "OKX 公開行情更新器",
        "window": 6 * 3600,
    },
    "bybit": {
        "title": "Bybit 永續合約",
        "provider": "Bybit",
        "cadence": "15 分鐘增量",
        "owner": "Bybit 公開行情更新器",
        "window": 6 * 3600,
    },
    "tw-index-futures": {
        "title": "TAIFEX 全期貨日資料",
        "provider": "TAIFEX",
        "cadence": "每個交易日收盤後",
        "owner": "TAIFEX 日資料計時器",
        "window": 72 * 3600,
    },
    "tw-index-derivatives-ticks": {
        "title": "TAIFEX 台指期權逐筆成交",
        "provider": "TAIFEX",
        "cadence": "官方近 30 交易日檔案",
        "owner": "TAIFEX 逐筆回補器",
        "window": 4 * 86400,
    },
    "tw-index-options-daily": {
        "title": "TAIFEX 選擇權日資料",
        "provider": "TAIFEX",
        "cadence": "每個交易日收盤後",
        "owner": "TAIFEX 選擇權回補器",
        "window": 72 * 3600,
    },
    "tw-futures": {
        "title": "永豐期貨歷史 Tick",
        "provider": "永豐 Shioaji / TAIFEX",
        "cadence": "配額允許時持續回補",
        "owner": "Shioaji 期貨歷史回補器",
        "window": 15 * 60,
    },
    "forex-frankfurter": {
        "title": "Frankfurter 外匯",
        "provider": "Frankfurter",
        "cadence": "每日",
        "owner": "Frankfurter 更新器",
        "window": 72 * 3600,
    },
    "forex-pepperstone": {
        "title": "Pepperstone 市場資料",
        "provider": "Pepperstone",
        "cadence": "每日",
        "owner": "Pepperstone 更新器",
        "window": 72 * 3600,
    },
    "legacy-parquet": {
        "title": "舊版 Parquet 封存",
        "provider": "歷史封存",
        "cadence": "凍結／待遷移",
        "owner": "人工稽核",
        "window": None,
    },
}

_SUMMARY_CANDIDATES: Final[dict[str, tuple[str, ...]]] = {
    "tw-public": ("download_summary.json",),
    "yahoo-market": (
        "download_summary.json",
        "daily_update_summary.json",
        "incremental_update_summary.json",
    ),
    "okx": ("download_summary.json",),
    "bybit": ("download_summary.json",),
    "tw-index-futures": ("manifest.json",),
    "tw-index-derivatives-ticks": ("manifest.json",),
    "tw-index-options-daily": (
        "manifest.json",
        "manifest_weekly.json",
        "manifest_final_settlement.json",
    ),
    "tw-futures": ("shioaji_contracts/manifest.json",),
    "forex-frankfurter": ("download_summary.json",),
}

_STATUS_PRIORITY: Final[dict[str, int]] = {
    "blocked": 8,
    "unavailable": 7,
    "degraded": 6,
    "stale": 5,
    "waiting": 4,
    "updating": 3,
    "current": 2,
    "complete": 1,
    "legacy": 0,
}

_REFRESH_UNITS: Final[dict[str, tuple[str, ...]]] = {
    "registered_daily": ("stockagent-registered-data-daily.service",),
    "registered_intraday": ("stockagent-registered-data-intraday.service",),
    "taifex_futures": ("stockagent-taifex-futures-daily.service",),
    "taifex_auxiliary": ("stockagent-taifex-auxiliary-daily.service",),
}
_ANSI_RE: Final[re.Pattern[str]] = re.compile(r"\x1b\[[0-9;?]*[ -/]*[@-~]")
_TQDM_RE: Final[re.Pattern[str]] = re.compile(
    r"(?P<label>(?:precheck|repair|download):[A-Za-z0-9_:-]+):\s*"
    r"(?P<percent>\d+)%\|[^\r\n]*?\|\s*"
    r"(?P<current>\d+)/(?P<total>\d+)\s*"
    r"\[(?P<elapsed>[0-9:]+)<(?P<remaining>[0-9:?]+),"
)


def _read_json(path: Path, default: Any = None) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, OSError, UnicodeError, json.JSONDecodeError):
        return default


def _service_state(unit: str) -> dict[str, Any]:
    """Read a fixed systemd unit without exposing process or invocation IDs."""

    fields: dict[str, str] = {}
    try:
        result = subprocess.run(
            (
                "systemctl",
                "show",
                unit,
                "--property=ActiveState,SubState,NRestarts",
                "--no-pager",
            ),
            check=False,
            capture_output=True,
            text=True,
            timeout=2.0,
        )
    except (OSError, subprocess.SubprocessError):
        result = None
    if result is not None and result.returncode == 0:
        for line in result.stdout.splitlines():
            key, separator, value = line.partition("=")
            if separator:
                fields[key] = value
    active = fields.get("ActiveState") in {"active", "activating", "reloading"}
    if not fields:
        cgroup = Path("/sys/fs/cgroup/system.slice") / unit / "cgroup.procs"
        try:
            active = bool(cgroup.read_text(encoding="utf-8").strip())
        except OSError:
            active = False
        if active:
            fields = {"ActiveState": "active", "SubState": "running"}
    return {
        "active": active,
        "state": fields.get("SubState") or fields.get("ActiveState") or "unknown",
        "restarts": _integer(fields.get("NRestarts")),
    }


def _refresh_service_states() -> dict[str, dict[str, Any]]:
    return {
        name: _service_state(units[0])
        for name, units in _REFRESH_UNITS.items()
    }


def _duration_seconds(value: str) -> int | None:
    if not value or "?" in value:
        return None
    try:
        parts = [int(part) for part in value.split(":")]
    except ValueError:
        return None
    if len(parts) == 2:
        return parts[0] * 60 + parts[1]
    if len(parts) == 3:
        return parts[0] * 3600 + parts[1] * 60 + parts[2]
    return None


def _tail_text(path: Path, maximum_bytes: int = 2 * 1024 * 1024) -> str:
    try:
        with path.open("rb") as handle:
            size = handle.seek(0, 2)
            handle.seek(max(0, size - maximum_bytes))
            return handle.read().decode("utf-8", errors="replace")
    except OSError:
        return ""


def _runtime_progress(repo_root: Path) -> list[dict[str, Any]]:
    roots = (
        repo_root / "artifacts/daily_downloader/registered_daily",
        repo_root / "artifacts/daily_downloader/registered_intraday",
    )
    paths: list[Path] = []
    for directory in roots:
        try:
            candidates = sorted(
                directory.glob("*.log"),
                key=lambda path: path.stat().st_mtime,
                reverse=True,
            )
        except OSError:
            candidates = []
        paths.extend(candidates[:1])
    output: list[dict[str, Any]] = []
    for path in paths:
        text = _ANSI_RE.sub("", _tail_text(path).replace("\r", "\n"))
        latest_by_label: dict[str, dict[str, Any]] = {}
        for match in _TQDM_RE.finditer(text):
            current = int(match.group("current"))
            total = int(match.group("total"))
            if total <= 0:
                continue
            remaining = _duration_seconds(match.group("remaining"))
            if remaining == 0 and current < total:
                remaining = 1
            latest_by_label[match.group("label")] = {
                "label": match.group("label"),
                "current": min(current, total),
                "total": total,
                "ratio": min(1.0, max(0.0, current / total)),
                "remaining_seconds": remaining,
            }
        output.extend(latest_by_label.values())
    return output


def _select_runtime_progress(
    rows: Iterable[Mapping[str, Any]],
    *,
    tokens: tuple[str, ...],
) -> dict[str, Any] | None:
    candidates = [
        dict(row)
        for row in rows
        if any(token in str(row.get("label") or "") for token in tokens)
        and (_integer(row.get("current")) or 0) < (_integer(row.get("total")) or 0)
    ]
    if not candidates:
        return None
    # Aggregated groups finish only when their slowest active phase finishes.
    return max(
        candidates,
        key=lambda row: (
            _integer(row.get("remaining_seconds")) or -1,
            _integer(row.get("total")) or 0,
        ),
    )


def _integer(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    try:
        number = int(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return number if number >= 0 else None


def _number(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return number if math.isfinite(number) else None


def _parse_time(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        try:
            parsed = datetime.fromisoformat(f"{text}T23:59:59+00:00")
        except ValueError:
            return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _mtime(path: Path) -> datetime | None:
    try:
        return datetime.fromtimestamp(path.stat().st_mtime, tz=UTC)
    except OSError:
        return None


def _iso(value: datetime | None) -> str | None:
    return value.isoformat().replace("+00:00", "Z") if value else None


def _latest_time(paths: Iterable[Path], payloads: Iterable[Any] = ()) -> datetime | None:
    values = [value for value in (_mtime(path) for path in paths) if value is not None]
    for payload in payloads:
        if not isinstance(payload, Mapping):
            continue
        for key in (
            "generated_at_utc",
            "generated_at",
            "completed_at_utc",
            "completed_at_taipei",
            "updated_at",
        ):
            parsed = _parse_time(payload.get(key))
            if parsed is not None:
                values.append(parsed)
    return max(values) if values else None


def _coverage(
    current: Any,
    total: Any,
    *,
    unit: str,
    label: str,
) -> dict[str, Any] | None:
    current_value = _integer(current)
    total_value = _integer(total)
    if current_value is None or total_value is None or total_value <= 0:
        return None
    return {
        "current": min(current_value, total_value),
        "total": total_value,
        "ratio": min(1.0, max(0.0, current_value / total_value)),
        "unit": unit,
        "label": label,
    }


def _complete_eta(basis: str = "來源稽核已完成。") -> dict[str, Any]:
    return {
        "state": "complete",
        "remaining_seconds": 0,
        "estimated_complete_at_utc": None,
        "confidence": "high",
        "basis": basis,
    }


def _unknown_eta(state: str, basis: str) -> dict[str, Any]:
    return {
        "state": state,
        "remaining_seconds": None,
        "estimated_complete_at_utc": None,
        "confidence": "not_available",
        "basis": basis,
    }


def _freshness(
    latest: datetime | None,
    *,
    now: datetime,
    window_seconds: int | None,
    continuous: bool = False,
) -> dict[str, Any]:
    age = max(0.0, (now - latest).total_seconds()) if latest else None
    if continuous:
        state = "continuous"
    elif latest is None or window_seconds is None:
        state = "not_applicable" if window_seconds is None else "unknown"
    else:
        state = "current" if age <= window_seconds else "stale"
    return {
        "state": state,
        "age_seconds": round(age, 3) if age is not None else None,
        "threshold_seconds": window_seconds,
    }


def _status_counts(payloads: Iterable[Any]) -> dict[str, int]:
    output: dict[str, int] = {}

    def walk(value: Any) -> None:
        if isinstance(value, Mapping):
            for key, item in value.items():
                if str(key) == "status_counts" and isinstance(item, Mapping):
                    for raw_status, raw_count in item.items():
                        count = _integer(raw_count)
                        if count is not None:
                            status = str(raw_status)
                            output[status] = output.get(status, 0) + count
                elif isinstance(item, Mapping):
                    walk(item)

    for payload in payloads:
        walk(payload)
    return output


def _extract_data_through(payloads: Iterable[Any]) -> str | None:
    candidates: list[str] = []
    for payload in payloads:
        if not isinstance(payload, Mapping):
            continue
        for key in (
            "applied_end_date",
            "provider_end_date",
            "end_date",
            "date_end",
        ):
            value = str(payload.get(key) or "").strip()
            if value:
                candidates.append(value)
        quality = payload.get("quality")
        if isinstance(quality, Mapping):
            value = str(quality.get("last_date") or "").strip()
            if value:
                candidates.append(value)
    return max(candidates) if candidates else None


def _extract_rows(payloads: Iterable[Any]) -> int | None:
    candidates: list[int] = []
    for payload in payloads:
        if not isinstance(payload, Mapping):
            continue
        for key in ("rows_total", "row_count", "success_rows"):
            value = _integer(payload.get(key))
            if value is not None:
                candidates.append(value)
        quality = payload.get("quality")
        if isinstance(quality, Mapping):
            value = _integer(quality.get("rows"))
            if value is not None:
                candidates.append(value)
        all_futures = payload.get("all_futures_daily")
        if isinstance(all_futures, Mapping) and isinstance(
            all_futures.get("quality"), Mapping
        ):
            value = _integer(all_futures["quality"].get("rows"))
            if value is not None:
                candidates.append(value)
    return max(candidates) if candidates else None


def _generic_group(
    root: Path,
    config: Mapping[str, Any],
    *,
    now: datetime,
) -> dict[str, Any]:
    dataset = str(config.get("dataset") or "unknown")
    meta = _GROUP_META.get(dataset, {})
    source = str(config.get("source") or "")
    source_root = root / source
    candidates = [
        source_root / relative for relative in _SUMMARY_CANDIDATES.get(dataset, ())
    ]
    payloads = [_read_json(path) for path in candidates]
    payloads = [payload for payload in payloads if payload is not None]
    latest = _latest_time(candidates, payloads)
    data_through = _extract_data_through(payloads)
    data_through_time = _parse_time(data_through)
    # Daily datasets are fresh only through their audited data date.  A newly
    # rewritten manifest must not make old market rows look current.
    if data_through_time is not None and dataset not in {
        "openbb-compact",
        "openbb-task-shards-local",
    }:
        latest = data_through_time
    window = meta.get("window")
    fresh = _freshness(
        latest,
        now=now,
        window_seconds=int(window) if isinstance(window, int) else None,
    )
    counts = _status_counts(payloads)
    total = sum(counts.values())
    failure = sum(
        count
        for status, count in counts.items()
        if any(token in status.lower() for token in ("fail", "error", "mismatch"))
    )
    completed = max(0, total - failure)
    coverage = _coverage(completed, total, unit="項", label="最近批次")

    if not source_root.exists():
        status = "unavailable"
        status_label = "資料根目錄不存在"
    elif dataset == "legacy-parquet":
        status = "legacy"
        status_label = "凍結封存"
    elif failure:
        status = "degraded"
        status_label = f"最近批次有 {failure:,} 個失敗"
    elif fresh["state"] == "stale":
        status = "stale"
        status_label = "需要補到最新"
    elif latest is None:
        status = "degraded"
        status_label = "缺少可驗證摘要"
    else:
        status = "current"
        status_label = "在新鮮度範圍內"

    if status in {"current", "legacy"} and (coverage is None or failure == 0):
        eta = _complete_eta("最近可驗證批次沒有待處理失敗。")
    elif status == "unavailable":
        eta = _unknown_eta("blocked", "資料根目錄不存在，無法開始估算。")
    else:
        eta = _unknown_eta(
            "waiting_schedule",
            "尚無執行中吞吐率；更新器開始並寫入進度後才能估算。",
        )

    warning = []
    if failure:
        warning.append("最近一次摘要含失敗項目；成功檔案不代表整批完整。")
    if not payloads and source_root.exists():
        warning.append("已登錄資料根，但尚未找到可驗證的摘要或 manifest。")
    return {
        "id": f"group:{dataset}",
        "parent_id": None,
        "scope": "storage_group",
        "title": str(meta.get("title") or dataset),
        "provider": str(meta.get("provider") or "其他"),
        "category": str(config.get("role") or "unknown"),
        "status": status,
        "status_label": status_label,
        "cadence": str(meta.get("cadence") or "依來源排程"),
        "update_owner": str(meta.get("owner") or "未指定"),
        "latest_at_utc": _iso(latest),
        "data_through": data_through,
        "freshness": fresh,
        "coverage": coverage,
        "eta": eta,
        "rows": _extract_rows(payloads),
        "publishable": bool(config.get("publish")),
        "detail": str(config.get("note") or "已登錄資料群組。"),
        "warnings": warning,
        "detail_link": None,
    }


def _tw_public_sources(root: Path, *, now: datetime) -> list[dict[str, Any]]:
    base = root / "data_tw_public"
    manifest = _read_json(base / "dataset_manifest.json", [])
    summary = _read_json(base / "download_summary.json", {})
    receipt = _read_json(root / "artifacts/data_refresh/tw_public/latest.json", {})
    if not isinstance(manifest, list):
        return []
    report: dict[str, dict[str, str]] = {}
    try:
        with (base / "download_report.csv").open(encoding="utf-8", newline="") as handle:
            for row in csv.DictReader(handle):
                report[str(row.get("dataset") or "")] = dict(row)
    except (FileNotFoundError, OSError, UnicodeError, csv.Error):
        pass
    generated = _latest_time(
        [base / "download_summary.json", root / "artifacts/data_refresh/tw_public/latest.json"],
        [summary, receipt],
    )
    fresh = _freshness(generated, now=now, window_seconds=72 * 3600)
    source_unavailable = (
        summary.get("source_unavailable_by_dataset", {})
        if isinstance(summary, Mapping)
        else {}
    )
    rows: list[dict[str, Any]] = []
    for item in manifest:
        if not isinstance(item, Mapping):
            continue
        name = str(item.get("name") or "").strip()
        if not name:
            continue
        audit = report.get(name, {})
        raw_status = str(audit.get("status") or "unknown").lower()
        failed = _integer(audit.get("failed_dates")) or 0
        missing = _integer(audit.get("missing_dates_after")) or 0
        coverage_flag = str(audit.get("coverage_complete", "")).lower()
        complete = coverage_flag == "true" or (
            raw_status in {"ok", "up_to_date", "complete"}
            and failed == 0
            and missing == 0
        )
        if failed or missing or raw_status in {"failed", "incomplete", "error"}:
            status = "degraded"
            label = f"缺 {missing:,} 日／失敗 {failed:,} 日"
            eta = _unknown_eta(
                "waiting_schedule",
                "等待不可變公開資料更新器再次執行完整缺口掃描。",
            )
        elif complete and fresh["state"] == "current":
            status = "current"
            label = "完整且在新鮮度範圍內"
            eta = _complete_eta("缺口稽核為零，且最新快照已發佈。")
        elif complete:
            status = "stale"
            label = "歷史完整但需要更新"
            eta = _unknown_eta(
                "waiting_schedule",
                "目前沒有執行中吞吐率；下次更新會先掃描再補齊。",
            )
        else:
            status = "degraded"
            label = "缺少完整度證據"
            eta = _unknown_eta("unknown", "缺少逐資料集完整度回執。")
        warnings = []
        if isinstance(source_unavailable, Mapping) and name in source_unavailable:
            warnings.append("含官方來源已確認不可取得的日期；未將其偽裝成成功資料。")
        tags = item.get("tags") if isinstance(item.get("tags"), list) else []
        provider = str(item.get("source") or "臺灣官方來源")
        if "mops" in name.lower() or any(str(tag).lower() == "mops" for tag in tags):
            provider = "MOPS / 公開資訊觀測站"
        rows.append(
            {
                "id": f"tw-public:{name}",
                "parent_id": "group:tw-public",
                "scope": "logical_source",
                "title": name,
                "provider": provider,
                "category": ", ".join(str(tag) for tag in tags[:3]),
                "status": status,
                "status_label": label,
                "cadence": "每個交易日收盤後",
                "update_owner": "不可變快照更新器",
                "latest_at_utc": _iso(generated),
                "data_through": str(summary.get("end_date") or "") or None,
                "freshness": dict(fresh),
                "coverage": _coverage(1 if complete else 0, 1, unit="資料集", label="缺口稽核"),
                "eta": eta,
                "rows": _integer(audit.get("rows")),
                "publishable": True,
                "detail": str(item.get("description") or "官方公開資料集。"),
                "warnings": warnings,
                "detail_link": None,
            }
        )
    return rows


def _shioaji_sources(status: Mapping[str, Any], *, now: datetime) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    pipelines = status.get("pipelines")
    if not isinstance(pipelines, list):
        return output
    status_map = {
        "active": "updating",
        "ready": "current",
        "waiting": "waiting",
        "attention": "degraded",
        "blocked": "blocked",
    }
    for pipeline in pipelines:
        if not isinstance(pipeline, Mapping):
            continue
        raw_status = str(pipeline.get("status") or "unavailable").lower()
        row_status = status_map.get(raw_status, "unavailable")
        latest = _parse_time(pipeline.get("latest_at_utc"))
        eta = pipeline.get("eta")
        coverage = pipeline.get("coverage")
        output.append(
            {
                "id": f"shioaji:{pipeline.get('id')}",
                "parent_id": "group:tw-futures"
                if pipeline.get("id") == "futures_history"
                else "group:tw-microstructure-captures-cold",
                "scope": "logical_source",
                "title": str(pipeline.get("title") or pipeline.get("id") or "Shioaji"),
                "provider": "永豐 Shioaji",
                "category": str(pipeline.get("category") or "market-data"),
                "status": row_status,
                "status_label": str(pipeline.get("status_label") or raw_status),
                "cadence": "盤中連續" if pipeline.get("category") == "realtime" else "配額允許時持續回補",
                "update_owner": "Shioaji 資料服務",
                "latest_at_utc": _iso(latest),
                "data_through": None,
                "freshness": _freshness(
                    latest,
                    now=now,
                    window_seconds=None if pipeline.get("category") == "realtime" else 72 * 3600,
                    continuous=pipeline.get("category") == "realtime",
                ),
                "coverage": dict(coverage) if isinstance(coverage, Mapping) else None,
                "eta": dict(eta)
                if isinstance(eta, Mapping)
                else _unknown_eta("unknown", "來源尚未提供 ETA 證據。"),
                "rows": None,
                "publishable": False,
                "detail": str(pipeline.get("detail") or "Shioaji 資料管線。"),
                "warnings": [str(value) for value in pipeline.get("warnings", [])],
                "detail_link": "../shioaji/",
            }
        )
    return output


def _openbb_sources(status: Mapping[str, Any], *, now: datetime) -> list[dict[str, Any]]:
    providers = status.get("providers")
    if not isinstance(providers, list):
        return []
    latest = now
    source_age = _number(status.get("source_age_seconds"))
    if source_age is not None:
        latest = now - timedelta(seconds=source_age)
    output: list[dict[str, Any]] = []
    for provider in providers:
        if not isinstance(provider, Mapping):
            continue
        accepted = _integer(provider.get("accepted_tasks")) or 0
        backlog = _integer(provider.get("exclusive_backlog_tasks"))
        if backlog is None:
            backlog = _integer(provider.get("eligible_backlog_tasks")) or 0
        rate = _number(provider.get("recent_tasks_per_minute")) or 0.0
        active = (_integer(provider.get("active")) or 0) > 0
        cooldown = provider.get("cooldown") is True
        if backlog == 0:
            row_status = "complete"
            label = "目前無待處理任務"
            eta = _complete_eta("此供應商目前沒有專屬待處理任務。")
        elif active or rate > 0:
            row_status = "updating"
            label = "正在回補"
            seconds = int(math.ceil(backlog / rate * 60)) if rate > 0 else None
            eta = {
                "state": "estimating" if seconds is not None else "unknown",
                "remaining_seconds": seconds,
                "estimated_complete_at_utc": _iso(
                    now + timedelta(seconds=seconds)
                )
                if seconds is not None
                else None,
                "confidence": "low",
                "basis": "依此供應商最近接受任務速率與專屬 backlog 線性外推；配額會改變結果。",
            }
        elif cooldown:
            row_status = "waiting"
            label = "等待配額解除"
            eta = _unknown_eta(
                "waiting_quota",
                "配額冷卻中；在恢復有效吞吐率前不提供假完成時間。",
            )
        else:
            row_status = "waiting"
            label = "等待排程"
            eta = _unknown_eta("waiting_schedule", "目前速率為零，無法可靠外推。")
        output.append(
            {
                "id": f"openbb:{provider.get('provider')}",
                "parent_id": "group:openbb-compact",
                "scope": "logical_source",
                "title": str(provider.get("provider") or "OpenBB provider"),
                "provider": "OpenBB",
                "category": "archive-provider",
                "status": row_status,
                "status_label": label,
                "cadence": "持續回補",
                "update_owner": "OpenBB 供應商排程器",
                "latest_at_utc": _iso(latest),
                "data_through": None,
                "freshness": _freshness(latest, now=now, window_seconds=15 * 60),
                "coverage": _coverage(
                    accepted,
                    accepted + backlog,
                    unit="任務",
                    label="已接受／專屬待處理",
                ),
                "eta": eta,
                "rows": _integer(provider.get("success_rows")),
                "publishable": True,
                "detail": f"最近 {rate:,.1f} 任務/分鐘；專屬待處理 {backlog:,}。",
                "warnings": ["供應商 unavailable 與成功資料分開計數。"] if cooldown else [],
                "detail_link": "../openbb/",
            }
        )
    return output


def _specialize_groups(
    groups: list[dict[str, Any]],
    *,
    shioaji: Mapping[str, Any],
    openbb: Mapping[str, Any],
    root: Path,
    now: datetime,
    refresh_services: Mapping[str, Mapping[str, Any]],
    runtime_progress: list[dict[str, Any]],
) -> None:
    by_id = {row["id"]: row for row in groups}
    tw_summary = _read_json(root / "data_tw_public/download_summary.json", {})
    tw_receipt = _read_json(root / "artifacts/data_refresh/tw_public/latest.json", {})
    tw = by_id.get("group:tw-public")
    if tw is not None and isinstance(tw_summary, Mapping):
        total = _integer(tw_summary.get("dataset_count")) or 0
        completed = (_integer(tw_summary.get("ok_count")) or 0) + (
            _integer(tw_summary.get("up_to_date_count")) or 0
        )
        coverage_complete = tw_summary.get("coverage_complete") is True
        receipt_ok = isinstance(tw_receipt, Mapping) and tw_receipt.get("status") == "ok"
        tw["coverage"] = _coverage(completed, total, unit="資料集", label="完整稽核")
        tw["data_through"] = str(tw_summary.get("end_date") or "") or None
        tw["rows"] = _integer(tw_summary.get("rows_total"))
        if coverage_complete and receipt_ok and tw["freshness"]["state"] == "current":
            tw["status"] = "current"
            tw["status_label"] = "不可變快照完整且最新"
            tw["eta"] = _complete_eta("全部公開資料集缺口稽核通過並完成快照切換。")
        elif coverage_complete:
            tw["status"] = "stale"
            tw["status_label"] = "完整快照需要更新"

    pipeline_by_id = {
        str(row.get("id")): row
        for row in shioaji.get("pipelines", [])
        if isinstance(row, Mapping)
    }
    for group_id, pipeline_id in {
        "group:tw-minute-train": "minute_research",
        "group:tw-minute-source-cold": "stock_minute",
        "group:tw-microstructure-train": "hft_dataset",
        "group:tw-microstructure-captures-cold": "fop_stream",
        "group:tw-futures": "futures_history",
    }.items():
        group = by_id.get(group_id)
        pipeline = pipeline_by_id.get(pipeline_id)
        if group is None or pipeline is None:
            continue
        raw_status = str(pipeline.get("status") or "unavailable")
        group["status"] = {
            "active": "updating",
            "ready": "current",
            "waiting": "waiting",
            "attention": "degraded",
        }.get(raw_status, "unavailable")
        group["status_label"] = str(pipeline.get("status_label") or raw_status)
        group["coverage"] = (
            dict(pipeline["coverage"])
            if isinstance(pipeline.get("coverage"), Mapping)
            else None
        )
        group["eta"] = (
            dict(pipeline["eta"])
            if isinstance(pipeline.get("eta"), Mapping)
            else group["eta"]
        )
        group["latest_at_utc"] = pipeline.get("latest_at_utc")
        group["warnings"] = [str(value) for value in pipeline.get("warnings", [])]
        group["detail_link"] = "../shioaji/"

    archive = openbb.get("archive")
    process = openbb.get("process")
    if isinstance(archive, Mapping):
        for group_id in ("group:openbb-compact", "group:openbb-task-shards-local"):
            group = by_id.get(group_id)
            if group is None:
                continue
            resolved = _integer(archive.get("resolved_tasks")) or 0
            total = _integer(archive.get("total_tasks")) or 0
            actionable = _integer(archive.get("actionable_unresolved_tasks")) or 0
            rates = [
                _number(row.get("recent_tasks_per_minute")) or 0.0
                for row in openbb.get("providers", [])
                if isinstance(row, Mapping)
            ]
            rate = sum(rates)
            group["coverage"] = _coverage(resolved, total, unit="任務", label="已判定")
            group["rows"] = (
                _integer(archive.get("success_rows"))
                if group_id == "group:openbb-compact"
                else None
            )
            group["data_through"] = str(archive.get("end_date") or "") or None
            source_age = _number(openbb.get("source_age_seconds"))
            latest = now - timedelta(seconds=source_age) if source_age is not None else None
            group["latest_at_utc"] = _iso(latest)
            group["freshness"] = _freshness(
                latest,
                now=now,
                window_seconds=15 * 60,
            )
            alive = isinstance(process, Mapping) and process.get("downloader_alive") is True
            audit_health = str(openbb.get("audit_health") or "unknown").lower()
            if audit_health in {"critical", "degraded"}:
                group["status"] = "degraded"
                group["status_label"] = (
                    "正在回補，但完整性稽核仍為 " + audit_health
                    if alive
                    else "完整性稽核失敗且下載程序未執行"
                )
            else:
                group["status"] = "updating" if alive else "blocked"
                group["status_label"] = "正在持續回補" if alive else "下載程序未執行"
            if actionable == 0:
                group["eta"] = _complete_eta("沒有可執行的未解任務。")
            elif rate > 0:
                seconds = int(math.ceil(actionable / rate * 60))
                group["eta"] = {
                    "state": "estimating",
                    "remaining_seconds": seconds,
                    "estimated_complete_at_utc": _iso(
                        now + timedelta(seconds=seconds)
                    ),
                    "confidence": "low",
                    "basis": "依近期供應商總接受速率外推可執行缺口；配額與權限結果會改變 ETA。",
                }
            else:
                group["eta"] = _unknown_eta(
                    "waiting_quota",
                    "目前有效速率為零；等待配額或下一輪排程。",
                )
            group["detail"] = (
                f"已接受 {_integer(archive.get('accepted_tasks')) or 0:,}；"
                f"權威不可用 {_integer(archive.get('unavailable_tasks')) or 0:,}；"
                f"可執行缺口 {actionable:,}。"
            )
            alerts = openbb.get("alerts")
            if isinstance(alerts, list):
                group["warnings"] = [
                    str(alert.get("message"))
                    for alert in alerts
                    if isinstance(alert, Mapping) and alert.get("message")
                ][:3]
            group["detail_link"] = "../openbb/"

    active_group_services = {
        "group:yahoo-market": (
            "registered_daily",
            "registered_intraday",
        ),
        "group:okx": ("registered_intraday",),
        "group:bybit": ("registered_intraday",),
        "group:forex-frankfurter": ("registered_daily",),
        "group:forex-pepperstone": ("registered_daily",),
        "group:tw-index-futures": ("taifex_futures",),
        "group:tw-index-derivatives-ticks": ("taifex_auxiliary",),
        "group:tw-index-options-daily": ("taifex_auxiliary",),
    }
    for group_id, service_names in active_group_services.items():
        group = by_id.get(group_id)
        if group is None:
            continue
        if not any(
            refresh_services.get(name, {}).get("active") is True
            for name in service_names
        ):
            continue
        group["status"] = "updating"
        group["status_label"] = "完整缺口更新正在執行"
        group["eta"] = _unknown_eta(
            "running_unmeasured",
            "更新正在執行；目前下載器尚未寫出足夠的批內速率，完成後會以新回執校正。",
        )

    progress_tokens = {
        "group:yahoo-market": (":us_stocks", ":crypto"),
        "group:okx": ("download:okx",),
        "group:bybit": ("download:bybit",),
        "group:forex-frankfurter": ("download:forex:frankfurter",),
    }
    for group_id, tokens in progress_tokens.items():
        group = by_id.get(group_id)
        if group is None or group.get("status") != "updating":
            continue
        progress = _select_runtime_progress(runtime_progress, tokens=tokens)
        if progress is None:
            continue
        group["coverage"] = _coverage(
            progress["current"],
            progress["total"],
            unit="項",
            label=f"執行階段 {progress['label']}",
        )
        remaining = _integer(progress.get("remaining_seconds"))
        if remaining is not None:
            group["eta"] = {
                "state": "phase_estimate",
                "remaining_seconds": remaining,
                "estimated_complete_at_utc": _iso(now + timedelta(seconds=remaining)),
                "confidence": "low",
                "basis": "下載器目前執行階段的 tqdm 吞吐率 ETA；進入下一個掃描或修復階段後會重新估算。",
            }


def _provider_summaries(rows: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    buckets: dict[str, list[Mapping[str, Any]]] = {}
    for row in rows:
        buckets.setdefault(str(row.get("provider") or "其他"), []).append(row)
    output = []
    for provider, items in buckets.items():
        counts: dict[str, int] = {}
        for item in items:
            status = str(item.get("status") or "unavailable")
            counts[status] = counts.get(status, 0) + 1
        worst = max(counts, key=lambda value: _STATUS_PRIORITY.get(value, 99))
        output.append(
            {
                "provider": provider,
                "status": worst,
                "registered": len(items),
                "status_counts": counts,
            }
        )
    return sorted(output, key=lambda row: (-row["registered"], row["provider"].lower()))


def build_data_monitor_public_status(
    repo_root: Path,
    *,
    now: datetime | None = None,
    refresh_services: Mapping[str, Mapping[str, Any]] | None = None,
    shioaji_status: Mapping[str, Any] | None = None,
    openbb_status: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Build the complete public registry and current monitor projection."""

    root = Path(repo_root)
    observed = (now or datetime.now(UTC)).astimezone(UTC)
    registry = _read_json(root / "configs/data_sync/packed_datasets.json", {})
    configs = registry.get("datasets", []) if isinstance(registry, Mapping) else []
    groups = [
        _generic_group(root, config, now=observed)
        for config in configs
        if isinstance(config, Mapping)
    ]
    shioaji = (
        dict(shioaji_status)
        if isinstance(shioaji_status, Mapping)
        else build_shioaji_public_status(root)
    )
    openbb = (
        dict(openbb_status)
        if isinstance(openbb_status, Mapping)
        else build_openbb_public_status(root)
    )
    service_states = (
        _refresh_service_states()
        if refresh_services is None
        else {str(key): dict(value) for key, value in refresh_services.items()}
    )
    runtime_progress = _runtime_progress(root)
    _specialize_groups(
        groups,
        shioaji=shioaji,
        openbb=openbb,
        root=root,
        now=observed,
        refresh_services=service_states,
        runtime_progress=runtime_progress,
    )
    logical = (
        _tw_public_sources(root, now=observed)
        + _shioaji_sources(shioaji, now=observed)
        + _openbb_sources(openbb, now=observed)
    )
    rows = groups + logical
    status_counts: dict[str, int] = {}
    for row in rows:
        status = str(row.get("status") or "unavailable")
        status_counts[status] = status_counts.get(status, 0) + 1
    healthy = sum(
        status_counts.get(status, 0)
        for status in ("current", "complete", "updating", "waiting", "legacy")
    )
    attention = len(rows) - healthy
    worst_status = max(
        (str(row.get("status") or "unavailable") for row in rows),
        key=lambda value: _STATUS_PRIORITY.get(value, 99),
        default="unavailable",
    )
    if attention:
        health = (
            "critical"
            if worst_status in {"blocked", "unavailable"}
            else "degraded"
        )
    elif status_counts.get("updating", 0) or status_counts.get("waiting", 0):
        health = "updating"
    else:
        health = "active"
    known_rows = sum(
        value
        for value in (_integer(row.get("rows")) for row in groups)
        if value is not None
    )
    return {
        "schema_version": DATA_MONITOR_SCHEMA_VERSION,
        "generated_at_utc": _iso(observed),
        "health": health,
        "read_only": True,
        "production_control_possible": False,
        "summary": {
            "registered_items": len(rows),
            "storage_groups": len(groups),
            "logical_sources": len(logical),
            "healthy_or_progressing": healthy,
            "attention_required": attention,
            "known_group_rows": known_rows,
            "status_counts": status_counts,
            "source_level_ratio": healthy / len(rows) if rows else 0.0,
        },
        "provider_summaries": _provider_summaries(rows),
        "refresh_services": service_states,
        "active_progress": runtime_progress,
        "groups": groups,
        "sources": rows,
        "definitions": {
            "freshness": "最新回執是否落在各來源允許的更新時間窗。",
            "completeness": "該來源以自己的日期、標的、任務或資料集單位稽核；不同單位不相加。",
            "eta": "只有執行中且存在有效吞吐率時才估算；配額、休市或零速率會明示未知。",
            "source_level_progress": "面板監控項目的狀態比例，不是資料列數完成率。",
            "realtime_boundary": "即時 Tick／BidAsk 是連續流，沒有總完工日；歷史 Tick 不能重建未曾擷取的五檔委託簿。",
            "tw_public_boundary": "臺灣官方資料只透過完整稽核後的不可變快照切換，不直接修改已發佈版本。",
        },
    }


__all__ = ["DATA_MONITOR_SCHEMA_VERSION", "build_data_monitor_public_status"]

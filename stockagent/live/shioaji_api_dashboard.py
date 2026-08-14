"""Build a public-safe status snapshot for local Shioaji data pipelines.

The monitor never logs in to Shioaji.  It derives quota, backfill, and capture
state from existing receipts plus narrowly parsed systemd journal messages, so
viewing the dashboard consumes neither an API connection nor market-data quota.
"""

from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import UTC, datetime
import json
from pathlib import Path
import re
import subprocess
from typing import Any, Callable, Final, Iterable, Sequence


HISTORY_UNIT: Final[str] = "stockagent-shioaji-tx-history-backfill.service"
CAPTURE_UNIT: Final[str] = "stockagent-shioaji-taifex-bidask.service"
MINUTE_UNIT: Final[str] = "stockagent-shioaji-minute-backfill.service"
TOP200_UNIT: Final[str] = "stockagent-shioaji-top200.service"
SNAPSHOT_UNIT: Final[str] = "stockagent-tw-day-trade-simulation.service"
TRAFFIC_FRACTION_GUARD: Final[float] = 0.90
TRAFFIC_RESERVE_BYTES: Final[int] = 128 * 1024 * 1024
MAX_TRAFFIC_SAMPLES: Final[int] = 240
_TRAFFIC_PATTERN = re.compile(r"traffic=([\d,]+)/([\d,]+)")
_CONTRACT_PATTERN = re.compile(r"\bcontract=([A-Z0-9]+)")
_WAIT_PATTERN = re.compile(
    r"waiting_seconds=(\d+)\s+reason=([a-z_]+)\s+contract=([A-Z0-9]+)"
)
_CAPTURE_START_PATTERN = re.compile(
    r"capture_start=([^ ]+)\s+capture_id=[^ ]+\s+session=([^ ]+)\s+"
    r"trade_date=([^ ]+)\s+stop_at=(.+)$"
)
_WORKER_PATTERN = re.compile(
    r"worker=(\d+)/(\d+)\s+contracts=(\d+)\s+subscriptions=(\d+)"
)


CommandRunner = Callable[[Sequence[str]], subprocess.CompletedProcess[str]]


@dataclass(frozen=True)
class ShioajiMonitorPaths:
    """Known local evidence roots used by the public monitor."""

    alias_inventory: Path
    txfr1_manifest: Path
    futures_history_root: Path
    target_end_date: Path
    capture_root: Path
    daily_summary: Path | None = None
    daily_progress: Path | None = None
    minute_summary: Path | None = None
    minute_manifest: Path | None = None
    minute_audit: Path | None = None
    top200_universe_summary: Path | None = None
    top200_capture_root: Path | None = None
    hft_dataset_root: Path | None = None
    hft_audit_root: Path | None = None
    contract_inventory_manifest: Path | None = None
    snapshot_state: Path | None = None
    traffic_ledger_summary: Path | None = None
    storage_summary: Path | None = None

    @classmethod
    def from_repo(cls, repo_root: Path) -> ShioajiMonitorPaths:
        root = Path(repo_root)
        return cls(
            alias_inventory=root
            / "data_tw_futures/shioaji_contracts/continuous_contracts.csv",
            txfr1_manifest=root
            / "data_tw_index_futures/shioaji_history/TXFR1/manifest.json",
            futures_history_root=root / "data_tw_futures/shioaji_history",
            target_end_date=root
            / "artifacts/data_repair/shioaji_futures_history/target_end_date.txt",
            capture_root=root / "data_tw_index_derivatives_ticks/shioaji_fop_captures",
            daily_summary=root / "data_tw_public/shioaji/download_summary.json",
            daily_progress=root / "data_tw_public/shioaji/progress.json",
            minute_summary=root / "data_tw_minute/shioaji_1m/download_summary.json",
            minute_manifest=root / "data_tw_minute/research_dataset/manifest.json",
            minute_audit=root / "data_tw_minute/audits/full_latest.json",
            top200_universe_summary=root
            / "data_tw_microstructure/universe/top_200.summary.json",
            top200_capture_root=root / "data_tw_microstructure/captures",
            hft_dataset_root=root / "data_tw_microstructure/hft_dataset",
            hft_audit_root=root / "data_tw_microstructure/audits",
            contract_inventory_manifest=root
            / "data_tw_futures/shioaji_contracts/manifest.json",
            snapshot_state=root / "artifacts/live/tw_day_trade_simulation/state.json",
            traffic_ledger_summary=root / "artifacts/live/shioaji_traffic/summary.json",
            storage_summary=root / "artifacts/live/shioaji_storage/summary.json",
        )


def _traffic_ledger_view(payload: dict[str, Any] | None) -> dict[str, Any]:
    source = payload or {}

    def bucket(value: Any) -> dict[str, int]:
        item = value if isinstance(value, dict) else {}
        return {
            key: int(item.get(key) or 0)
            for key in (
                "events",
                "queries",
                "avoided_queries",
                "rows",
                "failures",
                "observed_usage_delta_bytes",
                "stream_observations",
                "stream_tick_events",
                "stream_book_events",
                "stream_snapshot_rows",
                "stream_dropped_events",
                "stream_stored_bytes",
            )
        }

    return {
        "ledger_date": source.get("ledger_date"),
        "updated_at_utc": source.get("updated_at_utc"),
        "totals": bucket(source.get("totals")),
        "by_consumer": [
            {"name": str(name), **bucket(value)}
            for name, value in sorted((source.get("by_consumer") or {}).items())
            if isinstance(value, dict)
        ],
        "by_method": [
            {"name": str(name), **bucket(value)}
            for name, value in sorted((source.get("by_method") or {}).items())
            if isinstance(value, dict)
        ],
        "by_asset_class": [
            {"name": str(name), **bucket(value)}
            for name, value in sorted((source.get("by_asset_class") or {}).items())
            if isinstance(value, dict)
        ],
    }


def _storage_view(payload: dict[str, Any] | None, *, now: datetime) -> dict[str, Any]:
    """Allowlist a compact storage snapshot for the public response."""

    source = payload if isinstance(payload, dict) else {}
    generated_at = source.get("generated_at_utc")
    parsed_at = _parse_datetime(generated_at)

    def optional_number(value: Any) -> int | float | None:
        return value if isinstance(value, (int, float)) and value >= 0 else None

    def optional_signed_number(value: Any) -> int | float | None:
        return value if isinstance(value, (int, float)) else None

    summary_source = source.get("summary")
    summary_item = summary_source if isinstance(summary_source, dict) else {}
    summary_keys = (
        "datasets",
        "files",
        "total_bytes",
        "source_bytes",
        "derived_bytes",
        "operations_bytes",
        "growth_window_days",
        "growth_window_bytes",
        "average_daily_growth_bytes",
        "observed_growth_days",
        "capacity_growth_estimate_bytes",
        "disk_total_bytes",
        "disk_used_bytes",
        "disk_free_bytes",
        "disk_used_ratio",
        "estimated_days_remaining",
    )
    datasets: list[dict[str, Any]] = []
    source_datasets = source.get("datasets")
    for row in (source_datasets if isinstance(source_datasets, list) else [])[:16]:
        if not isinstance(row, dict):
            continue
        datasets.append(
            {
                "id": str(row.get("id") or "unknown"),
                "title": str(row.get("title") or "未命名資料"),
                "storage_class": str(row.get("storage_class") or "unknown"),
                "quota_class": str(row.get("quota_class") or "none"),
                "description": str(row.get("description") or ""),
                "bytes": optional_number(row.get("bytes")),
                "files": optional_number(row.get("files")),
                "latest_changed_at_utc": row.get("latest_changed_at_utc"),
                "growth_window_days": optional_number(row.get("growth_window_days")),
                "growth_window_bytes": optional_number(row.get("growth_window_bytes")),
                "average_daily_growth_bytes": optional_number(
                    row.get("average_daily_growth_bytes")
                ),
                "average_active_day_growth_bytes": optional_number(
                    row.get("average_active_day_growth_bytes")
                ),
                "active_growth_days": optional_number(row.get("active_growth_days")),
                "growth_source": str(row.get("growth_source") or "unknown"),
            }
        )
    daily_growth: list[dict[str, Any]] = []
    source_growth = source.get("daily_growth")
    for row in (source_growth if isinstance(source_growth, list) else [])[-30:]:
        if not isinstance(row, dict) or not isinstance(row.get("date"), str):
            continue
        daily_growth.append(
            {"date": row["date"], "bytes": optional_number(row.get("bytes"))}
        )
    definitions = source.get("definitions")
    return {
        "status": "ready" if datasets else "collecting",
        "generated_at_utc": (
            parsed_at.isoformat().replace("+00:00", "Z") if parsed_at else None
        ),
        "age_seconds": (
            max(0.0, (now - parsed_at).total_seconds()) if parsed_at else None
        ),
        "scan_seconds": optional_number(source.get("scan_seconds")),
        "summary": {
            **{key: optional_number(summary_item.get(key)) for key in summary_keys},
            "observed_average_daily_net_growth_bytes": optional_signed_number(
                summary_item.get("observed_average_daily_net_growth_bytes")
            ),
            "capacity_growth_source": str(
                summary_item.get("capacity_growth_source") or "unknown"
            ),
        },
        "datasets": datasets,
        "daily_growth": daily_growth,
        "definitions": {
            str(key): str(value)
            for key, value in (
                definitions if isinstance(definitions, dict) else {}
            ).items()
            if isinstance(value, str)
        },
    }


_PIPELINE_CONSUMERS: Final[dict[str, tuple[str, ...]]] = {
    "futures_history": ("futures_history_backfill",),
    "stock_minute": ("stock_minute_backfill", "stock_minute_gap_recovery"),
    "stock_daily": ("stock_daily_legacy_backfill", "stock_daily_materializer"),
    "fop_stream": ("taifex_fop_stream",),
    "top200_stream": ("stock_top200_stream",),
    "on_demand_snapshots": (
        "stock_quote_provider",
        "tw_day_trade_futures_benchmark",
    ),
}


def _traffic_breakdown(
    pipelines: list[dict[str, Any]], ledger: dict[str, Any]
) -> list[dict[str, Any]]:
    consumers = {
        str(item.get("name") or ""): item
        for item in ledger.get("by_consumer", [])
        if isinstance(item, dict)
    }
    rows: list[dict[str, Any]] = []
    for pipeline in pipelines:
        pipeline_id = str(pipeline.get("id") or "unknown")
        names = _PIPELINE_CONSUMERS.get(pipeline_id, ())
        matched = [consumers[name] for name in names if name in consumers]

        def total(field: str) -> int:
            return sum(int(item.get(field) or 0) for item in matched)

        quota_class = str(pipeline.get("quota") or "none")
        if quota_class == "historical":
            usage_status = "measured" if matched else "unattributed"
            attributed_bytes: int | None = (
                total("observed_usage_delta_bytes") if matched else None
            )
            price_label = "本地無費用欄位；受每日歷史流量額度限制"
        elif quota_class == "realtime":
            usage_status = "quota_exempt"
            attributed_bytes = 0
            price_label = "本地無費用欄位；即時推送不扣歷史流量"
        else:
            usage_status = "local_only"
            attributed_bytes = 0
            price_label = "本機資料處理；不呼叫行情 API"
        rows.append(
            {
                "id": pipeline_id,
                "title": str(pipeline.get("title") or "未命名資料"),
                "api_surface": str(pipeline.get("api_surface") or "—"),
                "quota_class": quota_class,
                "price_label": price_label,
                "usage_status": usage_status,
                "attributed_bytes": attributed_bytes,
                "queries": total("queries"),
                "avoided_queries": total("avoided_queries"),
                "stream_events": total("stream_tick_events")
                + total("stream_book_events"),
                "stream_stored_bytes": total("stream_stored_bytes"),
                "consumers": list(names),
            }
        )
    return rows


def _default_command_runner(args: Sequence[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(  # noqa: S603 - fixed executable and fixed unit names
        list(args),
        check=False,
        capture_output=True,
        text=True,
        timeout=3.0,
    )


def _read_json(path: Path) -> dict[str, Any] | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def _parse_datetime(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _age_seconds(value: Any, *, now: datetime) -> float | None:
    parsed = _parse_datetime(value)
    if parsed is None:
        return None
    return round(max(0.0, (now - parsed).total_seconds()), 3)


def _file_observed_at(path: Path | None) -> str | None:
    if path is None:
        return None
    try:
        return _iso_from_epoch(path.stat().st_mtime)
    except OSError:
        return None


def _payload_time(
    payload: dict[str, Any] | None,
    path: Path | None,
    *fields: str,
) -> str | None:
    for field in fields:
        value = (payload or {}).get(field)
        parsed = _parse_datetime(value)
        if parsed is not None:
            return parsed.isoformat().replace("+00:00", "Z")
    return _file_observed_at(path)


def _service_view(service: dict[str, Any]) -> dict[str, Any]:
    return {
        "active": bool(service.get("active")),
        "state": str(service.get("state") or "unknown"),
        "restarts": service.get("restarts"),
    }


def _metric(
    label: str,
    value: Any,
    *,
    unit: str | None = None,
    value_format: str = "number",
) -> dict[str, Any]:
    return {
        "label": label,
        "value": value,
        "unit": unit,
        "format": value_format,
    }


def _coverage(
    current: int | float | None,
    total: int | float | None,
    *,
    unit: str,
    label: str,
) -> dict[str, Any] | None:
    if not isinstance(current, (int, float)) or not isinstance(total, (int, float)):
        return None
    ratio = float(current) / float(total) if total > 0 else None
    return {
        "current": current,
        "total": total,
        "unit": unit,
        "label": label,
        "ratio": ratio,
    }


def _iso_from_epoch(value: float) -> str:
    return datetime.fromtimestamp(value, tz=UTC).isoformat().replace("+00:00", "Z")


def _journal_entries(
    unit: str,
    *,
    runner: CommandRunner,
    lines: int = 5_000,
    since: str = "-24hours",
) -> list[dict[str, Any]]:
    try:
        result = runner(
            (
                "journalctl",
                "--unit",
                unit,
                f"--since={since}",
                "--output=json",
                "--no-pager",
                f"--lines={int(lines)}",
            )
        )
    except (OSError, subprocess.SubprocessError):
        return []
    if result.returncode != 0:
        return []
    entries: list[dict[str, Any]] = []
    for raw in result.stdout.splitlines():
        try:
            item = json.loads(raw)
        except json.JSONDecodeError:
            continue
        if isinstance(item, dict) and isinstance(item.get("MESSAGE"), str):
            entries.append(item)
    return entries


def _service_state(unit: str, *, runner: CommandRunner) -> dict[str, Any]:
    try:
        result = runner(
            (
                "systemctl",
                "show",
                unit,
                "--property=ActiveState,SubState,NRestarts,InvocationID",
                "--no-pager",
            )
        )
    except (OSError, subprocess.SubprocessError):
        return {"active": False, "state": "unknown", "restarts": None}
    fields: dict[str, str] = {}
    if result.returncode == 0:
        for line in result.stdout.splitlines():
            key, separator, value = line.partition("=")
            if separator:
                fields[key] = value
    active = fields.get("ActiveState") == "active"
    if not fields:
        # The public gateway intentionally blocks AF_UNIX, so systemctl cannot
        # reach D-Bus inside that hardened unit.  A non-empty, fixed systemd
        # cgroup is a read-only local fallback and exposes no process details.
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
        "restarts": (
            int(fields["NRestarts"])
            if str(fields.get("NRestarts") or "").isdigit()
            else None
        ),
        "invocation_id": fields.get("InvocationID") or None,
    }


def _inventory_count(path: Path) -> int:
    try:
        with path.open(encoding="utf-8", newline="") as handle:
            return len(
                {
                    str(row.get("contract") or "").strip()
                    for row in csv.DictReader(handle)
                    if str(row.get("contract") or "").strip()
                }
            )
    except OSError:
        return 0


def _history_manifests(paths: ShioajiMonitorPaths) -> list[dict[str, Any]]:
    candidates = [paths.txfr1_manifest]
    try:
        candidates.extend(sorted(paths.futures_history_root.glob("*/manifest.json")))
    except OSError:
        pass
    manifests: list[dict[str, Any]] = []
    seen: set[str] = set()
    for path in candidates:
        payload = _read_json(path)
        contract = str((payload or {}).get("contract") or "")
        if not payload or not contract or contract in seen:
            continue
        seen.add(contract)
        payload = dict(payload)
        try:
            payload["_observed_at"] = _iso_from_epoch(path.stat().st_mtime)
            payload["_observed_epoch"] = float(path.stat().st_mtime)
        except OSError:
            payload["_observed_at"] = None
            payload["_observed_epoch"] = 0.0
        manifests.append(payload)
    return manifests


def _entry_timestamp(entry: dict[str, Any]) -> tuple[float, str | None]:
    raw = entry.get("__REALTIME_TIMESTAMP")
    try:
        epoch = int(str(raw)) / 1_000_000.0
    except (TypeError, ValueError):
        return 0.0, None
    return epoch, _iso_from_epoch(epoch)


def _traffic_samples(
    entries: Iterable[dict[str, Any]], manifests: Iterable[dict[str, Any]]
) -> list[dict[str, Any]]:
    samples: list[tuple[float, int, int]] = []
    for entry in entries:
        match = _TRAFFIC_PATTERN.search(str(entry.get("MESSAGE") or ""))
        if not match:
            continue
        epoch, _timestamp = _entry_timestamp(entry)
        if epoch <= 0:
            continue
        samples.append(
            (
                epoch,
                int(match.group(1).replace(",", "")),
                int(match.group(2).replace(",", "")),
            )
        )
    for manifest in manifests:
        used = manifest.get("traffic_used_bytes")
        limit = manifest.get("traffic_limit_bytes")
        epoch = float(manifest.get("_observed_epoch") or 0.0)
        if isinstance(used, int) and isinstance(limit, int) and epoch > 0:
            samples.append((epoch, used, limit))
    # Keep the final observation in each minute so a busy run remains compact.
    per_minute: dict[int, tuple[float, int, int]] = {}
    for sample in sorted(samples):
        per_minute[int(sample[0] // 60)] = sample
    output = []
    for epoch, used, limit in list(per_minute.values())[-MAX_TRAFFIC_SAMPLES:]:
        output.append(
            {
                "observed_at_utc": _iso_from_epoch(epoch),
                "used_bytes": used,
                "limit_bytes": limit,
                "remaining_bytes": max(0, limit - used),
                "used_ratio": used / limit if limit > 0 else None,
                "source_id": "futures_history",
                "source_label": "期貨 Tick 回補",
            }
        )
    return output


def _latest_capture_mtime(root: Path) -> float:
    latest = 0.0
    for stream in ("ticks", "book_events", "book_1s"):
        stream_root = root / stream
        try:
            trade_dates = sorted(stream_root.glob("trade_date=*"))
            if not trade_dates:
                continue
            hours = sorted(trade_dates[-1].glob("hour=*"))
            target = hours[-1] if hours else trade_dates[-1]
            latest = max(latest, target.stat().st_mtime)
        except OSError:
            continue
    return latest


def _latest_capture_receipt(root: Path | None) -> dict[str, Any]:
    """Aggregate the latest finalized multi-worker capture without exposing IDs."""

    if root is None:
        return {}
    candidates: list[tuple[datetime, dict[str, Any]]] = []
    try:
        manifests = root.glob("manifests/**/worker=*.json")
        for manifest in manifests:
            payload = _read_json(manifest)
            started = _parse_datetime((payload or {}).get("started_at_utc"))
            if payload and started is not None:
                candidates.append((started, payload))
    except OSError:
        return {}
    if not candidates:
        return {}
    _latest_started, latest = max(candidates, key=lambda item: item[0])
    capture_key = str(latest.get("capture_id") or "")
    if not capture_key:
        selected = [latest]
    else:
        # Compatibility manifests may mirror a session-aware manifest. Keep one
        # receipt per worker so public totals are never doubled.
        by_worker: dict[int, dict[str, Any]] = {}
        for _started, item in candidates:
            if str(item.get("capture_id") or "") != capture_key:
                continue
            worker = int(item.get("worker_index") or 0)
            by_worker[worker] = item
        selected = list(by_worker.values())
    finished = [
        parsed
        for item in selected
        if (parsed := _parse_datetime(item.get("finished_at_utc"))) is not None
    ]
    started = [
        parsed
        for item in selected
        if (parsed := _parse_datetime(item.get("started_at_utc"))) is not None
    ]
    status_values = {str(item.get("status") or "unknown") for item in selected}
    return {
        "trade_date": str(latest.get("trade_date") or "") or None,
        "session": str(latest.get("capture_session") or "") or None,
        "status": (next(iter(status_values)) if len(status_values) == 1 else "mixed"),
        "workers": len(selected),
        "instruments": sum(
            int(item.get("contract_count") or item.get("symbol_count") or 0)
            for item in selected
        ),
        "subscriptions": sum(
            int(item.get("subscriptions_requested") or 0) for item in selected
        ),
        "tick_rows": sum(int(item.get("tick_rows_written") or 0) for item in selected),
        "book_rows": sum(int(item.get("book_rows_written") or 0) for item in selected),
        "snapshot_rows": sum(
            int(item.get("book_1s_rows_written") or 0) for item in selected
        ),
        "dropped_events": sum(
            int(item.get("dropped_events") or 0) for item in selected
        ),
        "missed_snapshot_seconds": sum(
            int(item.get("missed_snapshot_seconds") or 0) for item in selected
        ),
        "started_at_utc": (
            min(started).isoformat().replace("+00:00", "Z") if started else None
        ),
        "finished_at_utc": (
            max(finished).isoformat().replace("+00:00", "Z") if finished else None
        ),
    }


def _latest_audit(root: Path | None, pattern: str = "*.json") -> dict[str, Any]:
    if root is None:
        return {}
    try:
        candidates = [item for item in root.glob(pattern) if item.is_file()]
        if not candidates:
            return {}
        path = max(candidates, key=lambda item: item.stat().st_mtime)
    except OSError:
        return {}
    payload = _read_json(path) or {}
    payload = dict(payload)
    payload["_observed_at"] = _file_observed_at(path)
    return payload


def _hft_partition_totals(root: Path | None) -> dict[str, Any]:
    if root is None:
        return {}
    rows = 0
    feature_rows = 0
    dates: list[str] = []
    latest_at = None
    try:
        summaries = sorted(root.glob("trade_date=*/summary.json"))
    except OSError:
        summaries = []
    for path in summaries:
        payload = _read_json(path)
        if not payload or payload.get("status") != "ok":
            continue
        date_text = str(payload.get("trade_date") or path.parent.name.partition("=")[2])
        dates.append(date_text)
        rows += int(payload.get("rows") or 0)
        feature_rows += int(payload.get("feature_valid_rows") or 0)
        latest_at = _file_observed_at(path)
    return {
        "dates": len(dates),
        "first_date": min(dates) if dates else None,
        "last_date": max(dates) if dates else None,
        "rows": rows,
        "feature_valid_rows": feature_rows,
        "latest_at_utc": latest_at,
    }


def _top200_failure(entries: Iterable[dict[str, Any]]) -> dict[str, Any] | None:
    failure: dict[str, Any] | None = None
    for entry in entries:
        message = str(entry.get("MESSAGE") or "")
        code = None
        detail = None
        if any(
            marker in message
            for marker in (
                "runner_started=",
                "capture_start=",
                "capture_complete",
                "reason=connection_budget",
            )
        ):
            # A later clean lifecycle/wait record supersedes an older failed
            # invocation.  Historical errors remain in the journal but must
            # not permanently poison the current pipeline status.
            failure = None
        if "ModuleNotFoundError" in message and "stockagent" in message:
            code = "runtime_import_error"
            detail = "執行環境找不到 stockagent 模組"
        elif "capture_failed" in message and failure is None:
            code = "capture_failed"
            detail = "最近一次擷取未完成"
        if code is None:
            continue
        _epoch, observed_at = _entry_timestamp(entry)
        failure = {"code": code, "detail": detail, "observed_at_utc": observed_at}
    return failure


def _top200_priority_wait(entries: Iterable[dict[str, Any]]) -> dict[str, Any] | None:
    """Return the latest intentional derivatives-first connection gate."""

    waiting: dict[str, Any] | None = None
    for entry in entries:
        if "reason=connection_budget" not in str(entry.get("MESSAGE") or ""):
            continue
        _epoch, observed_at = _entry_timestamp(entry)
        waiting = {
            "code": "connection_budget",
            "detail": "連線額度依設定優先保留給期貨與選擇權即時擷取",
            "observed_at_utc": observed_at,
        }
    return waiting


def _capture_status(
    paths: ShioajiMonitorPaths,
    entries: list[dict[str, Any]],
    service: dict[str, Any],
    *,
    now: datetime,
) -> dict[str, Any]:
    invocation_id = service.get("invocation_id")
    if invocation_id:
        current = [
            item
            for item in entries
            if item.get("_SYSTEMD_INVOCATION_ID") == invocation_id
        ]
        if current:
            entries = current
    session = None
    trade_date = None
    stop_at = None
    start_at = None
    workers: dict[int, tuple[int, int]] = {}
    for entry in entries:
        message = str(entry.get("MESSAGE") or "")
        start_match = _CAPTURE_START_PATTERN.search(message)
        if start_match:
            start_at, session, trade_date, stop_at = start_match.groups()
            workers = {}
            continue
        worker_match = _WORKER_PATTERN.search(message)
        if worker_match:
            worker_index, _total, contracts, subscriptions = map(
                int, worker_match.groups()
            )
            workers[worker_index] = (contracts, subscriptions)
    latest_epoch = _latest_capture_mtime(paths.capture_root)
    age = max(0.0, now.timestamp() - latest_epoch) if latest_epoch else None
    if not service.get("active"):
        state = "stopped"
    elif age is None:
        state = "starting"
    elif age <= 120:
        state = "capturing"
    else:
        state = "quiet"
    return {
        "service_active": bool(service.get("active")),
        "service_state": str(service.get("state") or "unknown"),
        "service_restarts": service.get("restarts"),
        "state": state,
        "session": session,
        "trade_date": trade_date,
        "started_at_local": start_at,
        "scheduled_stop_at_local": stop_at,
        "workers": len(workers),
        "contracts": sum(item[0] for item in workers.values()),
        "subscriptions": sum(item[1] for item in workers.values()),
        "latest_file_at_utc": _iso_from_epoch(latest_epoch) if latest_epoch else None,
        "latest_file_age_seconds": round(age, 3) if age is not None else None,
    }


def _backfill_status(
    paths: ShioajiMonitorPaths,
    manifests: list[dict[str, Any]],
    entries: list[dict[str, Any]],
    service: dict[str, Any],
) -> dict[str, Any]:
    alias_count = _inventory_count(paths.alias_inventory)
    expected_per_contract = max(
        (int(item.get("expected_trading_dates") or 0) for item in manifests),
        default=0,
    )
    resolved = sum(int(item.get("resolved_trading_dates") or 0) for item in manifests)
    expected = alias_count * expected_per_contract
    completed = sum(item.get("status") == "complete" for item in manifests)
    current_contract = None
    waiting_reason = None
    waiting_seconds = None
    for entry in entries:
        message = str(entry.get("MESSAGE") or "")
        contract_match = _CONTRACT_PATTERN.search(message)
        if contract_match:
            current_contract = contract_match.group(1)
        wait_match = _WAIT_PATTERN.search(message)
        if wait_match:
            waiting_seconds = int(wait_match.group(1))
            waiting_reason = wait_match.group(2)
            current_contract = wait_match.group(3)
    current = next(
        (item for item in manifests if item.get("contract") == current_contract), None
    )
    if not service.get("active"):
        state = "stopped"
    elif completed >= alias_count > 0:
        state = "complete"
    elif waiting_reason == "next_quota_window":
        state = "waiting_quota"
    elif waiting_reason == "market_hours_priority_gate":
        state = "waiting_market"
    else:
        state = "downloading"
    contract_rows = sorted(
        manifests,
        key=lambda item: (
            item.get("contract") != current_contract,
            item.get("status") != "partial",
            str(item.get("contract") or ""),
        ),
    )[:12]
    try:
        target_end_date = paths.target_end_date.read_text(encoding="utf-8").strip()
    except OSError:
        target_end_date = None
    return {
        "service_active": bool(service.get("active")),
        "service_state": str(service.get("state") or "unknown"),
        "service_restarts": service.get("restarts"),
        "state": state,
        "waiting_reason": waiting_reason,
        "waiting_seconds_at_observation": waiting_seconds,
        "target_end_date": target_end_date or None,
        "inventory_contracts": alias_count,
        "started_contracts": len(manifests),
        "completed_contracts": completed,
        "expected_dates_per_contract": expected_per_contract,
        "resolved_contract_dates": resolved,
        "expected_contract_dates": expected,
        "progress_ratio": resolved / expected if expected > 0 else None,
        "rows": sum(int(item.get("rows") or 0) for item in manifests),
        "stored_bytes": sum(int(item.get("bytes") or 0) for item in manifests),
        "current_contract": current_contract,
        "current_contract_resolved_dates": int(
            (current or {}).get("resolved_trading_dates") or 0
        ),
        "current_contract_expected_dates": int(
            (current or {}).get("expected_trading_dates") or expected_per_contract
        ),
        "contracts": [
            {
                "contract": str(item.get("contract") or ""),
                "status": str(item.get("status") or "unknown"),
                "resolved_dates": int(item.get("resolved_trading_dates") or 0),
                "expected_dates": int(item.get("expected_trading_dates") or 0),
                "rows": int(item.get("rows") or 0),
                "stored_bytes": int(item.get("bytes") or 0),
                "observed_at_utc": item.get("_observed_at"),
            }
            for item in contract_rows
        ],
    }


def _build_pipelines(
    paths: ShioajiMonitorPaths,
    *,
    now: datetime,
    backfill: dict[str, Any],
    capture: dict[str, Any],
    history_service: dict[str, Any],
    capture_service: dict[str, Any],
    minute_service: dict[str, Any],
    top200_service: dict[str, Any],
    snapshot_service: dict[str, Any],
    top200_entries: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    minute_summary = _read_json(paths.minute_summary) if paths.minute_summary else None
    minute_manifest = (
        _read_json(paths.minute_manifest) if paths.minute_manifest else None
    )
    minute_audit = _read_json(paths.minute_audit) if paths.minute_audit else None
    minute_ready = bool(
        (minute_manifest or {}).get("research_ready")
        and (minute_audit or {}).get("status") == "research_ready"
    )
    minute_total = int((minute_summary or {}).get("selected_symbols") or 0)
    minute_available = int((minute_audit or {}).get("available_source_symbols") or 0)
    minute_latest = _payload_time(
        minute_manifest,
        paths.minute_manifest,
        "written_at_utc",
    )

    daily_summary = _read_json(paths.daily_summary) if paths.daily_summary else None
    daily_progress = _read_json(paths.daily_progress) if paths.daily_progress else None
    daily_total = int((daily_summary or {}).get("selected_symbols") or 0)
    daily_reported = int((daily_summary or {}).get("reported_symbols") or 0)
    if (daily_summary or {}).get("universe_coverage_complete"):
        daily_state = "complete"
        daily_label = "全部完成"
    elif (daily_summary or {}).get("stopped_for_traffic"):
        daily_state = "waiting"
        daily_label = "流量保護暫停"
    elif daily_summary:
        daily_state = "partial"
        daily_label = "部分完成"
    else:
        daily_state = "unavailable"
        daily_label = "尚無資料"
    daily_latest = _payload_time(
        daily_progress or daily_summary,
        paths.daily_progress or paths.daily_summary,
        "updated_at_utc",
        "written_at_utc",
    )

    inventory = (
        _read_json(paths.contract_inventory_manifest)
        if paths.contract_inventory_manifest
        else None
    )
    inventory_latest = _payload_time(
        inventory,
        paths.contract_inventory_manifest,
        "generated_at",
    )

    fop_receipt = _latest_capture_receipt(paths.capture_root)
    fop_audit = _latest_audit(paths.capture_root / "audits")
    fop_latest = capture.get("latest_file_at_utc") or fop_receipt.get("finished_at_utc")
    if capture.get("state") == "capturing":
        fop_state, fop_label = "active", "即時寫入"
    elif capture_service.get("active"):
        fop_state, fop_label = "waiting", "等待下一盤"
    else:
        fop_state, fop_label = "stopped", "服務停止"

    top_receipt = _latest_capture_receipt(paths.top200_capture_root)
    top_failure = _top200_failure(top200_entries)
    top_wait = _top200_priority_wait(top200_entries)
    if top200_service.get("active"):
        top_state, top_label = "active", "服務運行"
    elif top_failure:
        top_state, top_label = "failed", "最近執行失敗"
    elif top_wait:
        top_state, top_label = "waiting", "期權優先暫停"
    else:
        top_state, top_label = "stopped", "服務停止"
    top_latest = top_receipt.get("finished_at_utc")

    hft_totals = _hft_partition_totals(paths.hft_dataset_root)
    hft_audit = _latest_audit(paths.hft_audit_root, "hft_*.json")
    hft_ready = hft_totals.get("dates", 0) > 0 and hft_audit.get("status") == "ok"
    hft_latest = hft_totals.get("latest_at_utc") or hft_audit.get("_observed_at")

    snapshot_state = _read_json(paths.snapshot_state) if paths.snapshot_state else None
    benchmarks = (snapshot_state or {}).get("benchmarks")
    modes = (snapshot_state or {}).get("modes")
    safe_benchmarks = benchmarks if isinstance(benchmarks, dict) else {}
    safe_modes = modes if isinstance(modes, dict) else {}
    quote_times = [
        parsed
        for item in safe_benchmarks.values()
        if isinstance(item, dict)
        if str(item.get("source") or "").startswith("shioaji:")
        if (parsed := _parse_datetime(item.get("last_quote_at"))) is not None
    ]
    snapshot_latest = (
        max(quote_times).isoformat().replace("+00:00", "Z")
        if quote_times
        else _payload_time(snapshot_state, paths.snapshot_state, "updated_at")
    )
    snapshot_sources = {
        str(item.get("source") or "")
        for item in safe_benchmarks.values()
        if isinstance(item, dict)
        and str(item.get("source") or "").startswith("shioaji:")
    }

    backfill_state = str(backfill.get("state") or "stopped")
    history_state = {
        "downloading": ("active", "持續下載"),
        "waiting_quota": ("waiting", "等待流量重置"),
        "waiting_market": ("waiting", "即時行情優先"),
        "complete": ("complete", "全部完成"),
        "stopped": ("stopped", "服務停止"),
    }.get(backfill_state, ("unavailable", "狀態未知"))

    pipelines = [
        {
            "id": "futures_history",
            "title": "全期貨歷史 Tick",
            "category": "historical",
            "api_surface": "api.ticks",
            "quota": "historical",
            "status": history_state[0],
            "status_label": history_state[1],
            "detail": "743 個 R1/R2 連續期貨逐交易日回補，以 receipt 對帳。",
            "coverage": _coverage(
                backfill.get("resolved_contract_dates"),
                backfill.get("expected_contract_dates"),
                unit="合約交易日",
                label="Receipt 覆蓋率",
            ),
            "latest_at_utc": (
                backfill.get("contracts", [{}])[0].get("observed_at_utc")
                if backfill.get("contracts")
                else None
            ),
            "fields": ["成交時間", "價格", "數量", "買賣方向", "附帶一檔 Bid/Ask"],
            "metrics": [
                _metric("已完成合約", backfill.get("completed_contracts")),
                _metric("已開始合約", backfill.get("started_contracts")),
                _metric("Tick", backfill.get("rows"), value_format="compact"),
                _metric("落盤", backfill.get("stored_bytes"), value_format="bytes"),
            ],
            "warnings": (
                ["歷史 Tick 的附帶 Bid/Ask 不是即時五檔委託簿。"]
                + (
                    ["已到安全流量閘門，會等下一個配額窗口。"]
                    if backfill_state == "waiting_quota"
                    else []
                )
            ),
            "service": _service_view(history_service),
        },
        {
            "id": "stock_minute",
            "title": "全市場股票 1 分 K 棒",
            "category": "historical",
            "api_surface": "api.kbars",
            "quota": "historical",
            "status": "ready" if minute_ready else "partial",
            "status_label": "研究資料可用" if minute_ready else "尚未完成稽核",
            "detail": "逐檔 29 日切片回補，已產生因果 1 分鐘研究資料與全量稽核。",
            "coverage": _coverage(
                minute_available,
                minute_total,
                unit="標的",
                label="可研究標的",
            ),
            "latest_at_utc": minute_latest,
            "fields": [
                "Open",
                "High",
                "Low",
                "Close",
                "Volume",
                "Amount",
                "1 分鐘時間",
            ],
            "metrics": [
                _metric("完整標的", (minute_summary or {}).get("complete_symbols")),
                _metric(
                    "來源缺口",
                    (minute_summary or {}).get("complete_with_source_gap_symbols"),
                ),
                _metric("交易日分區", (minute_audit or {}).get("partitions")),
                _metric(
                    "研究列數", (minute_audit or {}).get("rows"), value_format="compact"
                ),
            ],
            "warnings": [
                "88 檔有來源缺口、124 檔合約不可用；研究資料以遮罩保留可用範圍。"
            ]
            if minute_ready and minute_total
            else [],
            "service": _service_view(minute_service),
        },
        {
            "id": "minute_research",
            "title": "股票分鐘因果研究資料",
            "category": "derived",
            "api_surface": "由全市場 1 分 K 棒產生",
            "quota": "none",
            "status": "ready" if minute_ready else "partial",
            "status_label": "研究稽核通過" if minute_ready else "等待完整稽核",
            "detail": "將已落盤分鐘 K 棒轉成時間因果特徵、標籤與可交易遮罩。",
            "coverage": _coverage(
                (minute_audit or {}).get("available_source_symbols"),
                minute_total,
                unit="標的",
                label="可研究標的",
            ),
            "latest_at_utc": minute_latest,
            "fields": ["因果特徵", "報酬標籤", "可交易遮罩", "交易日分區"],
            "metrics": [
                _metric("交易日分區", (minute_audit or {}).get("partitions")),
                _metric(
                    "總列數", (minute_audit or {}).get("rows"), value_format="compact"
                ),
                _metric(
                    "有效特徵列",
                    (minute_audit or {}).get("feature_valid_rows"),
                    value_format="compact",
                ),
                _metric(
                    "可研究標的", (minute_audit or {}).get("available_source_symbols")
                ),
            ],
            "warnings": ["這是本機衍生資料，不會額外呼叫 Shioaji API。"],
            "service": None,
        },
        {
            "id": "stock_daily",
            "title": "台股日 K 混合資料",
            "category": "historical",
            "api_surface": "api.kbars",
            "quota": "historical",
            "status": daily_state,
            "status_label": daily_label,
            "detail": "2020 年後以永豐日 K 取代公開來源；目前仍是部分回補成果。",
            "coverage": _coverage(
                daily_reported,
                daily_total,
                unit="標的",
                label="已處理標的",
            ),
            "latest_at_utc": daily_latest,
            "fields": ["日 Open", "High", "Low", "Close", "成交股數", "成交金額"],
            "metrics": [
                _metric("完整標的", (daily_summary or {}).get("complete_symbols")),
                _metric(
                    "不可用", (daily_summary or {}).get("contract_unavailable_symbols")
                ),
                _metric("失敗", (daily_summary or {}).get("failed_symbols")),
                _metric(
                    "上次執行用量",
                    (daily_summary or {}).get("traffic_used_bytes"),
                    value_format="bytes",
                ),
            ],
            "warnings": [
                "這是舊次執行的流量紀錄，不代表上方本日全域配額。",
                "混合資料尚未通過全市場最終完成閘門。",
            ]
            if daily_summary
            else [],
            "service": {"active": False, "state": "manual", "restarts": None},
        },
        {
            "id": "fop_stream",
            "title": "期貨／選擇權即時 Tick + 五檔",
            "category": "realtime",
            "api_surface": "quote.subscribe(Tick, BidAsk)",
            "quota": "realtime",
            "status": fop_state,
            "status_label": fop_label,
            "detail": "大台、微台與台指選擇權分三個 worker 擷取；即時推送不扣歷史流量。",
            "coverage": None,
            "latest_at_utc": fop_latest,
            "fields": [
                "逐筆成交",
                "五檔價量",
                "委託量變化",
                "交易所時間",
                "接收時間",
                "1 秒簿快照",
            ],
            "metrics": [
                _metric("目前合約", capture.get("contracts")),
                _metric("目前訂閱", capture.get("subscriptions")),
                _metric(
                    "最近落盤 Tick",
                    fop_receipt.get("tick_rows"),
                    value_format="compact",
                ),
                _metric(
                    "稽核有效簿",
                    fop_audit.get("valid_book_rows"),
                    value_format="compact",
                ),
            ],
            "warnings": [
                f"最近完成擷取遺失事件 {int(fop_receipt.get('dropped_events') or 0):,} 筆。"
            ]
            if int(fop_receipt.get("dropped_events") or 0) > 0
            else [],
            "service": _service_view(capture_service),
        },
        {
            "id": "top200_stream",
            "title": "Top‑200 股票即時 Tick + 五檔",
            "category": "realtime",
            "api_surface": "quote.subscribe(Tick, BidAsk)",
            "quota": "realtime",
            "status": top_state,
            "status_label": top_label,
            "detail": (top_failure or top_wait or {}).get("detail")
            or "依官方市值名單擷取 200 檔股票微結構。",
            "coverage": None,
            "latest_at_utc": top_latest,
            "fields": ["逐筆成交", "五檔價量", "委託量變化", "接收時間", "1 秒簿快照"],
            "metrics": [
                _metric("最近標的", top_receipt.get("instruments")),
                _metric(
                    "最近 Tick", top_receipt.get("tick_rows"), value_format="compact"
                ),
                _metric(
                    "最近簿事件", top_receipt.get("book_rows"), value_format="compact"
                ),
                _metric("遺失事件", top_receipt.get("dropped_events")),
            ],
            "warnings": [
                "期貨／選擇權擷取擁有連線優先權。",
                *([(top_failure or {}).get("detail")] if top_failure else []),
            ],
            "service": _service_view(top200_service),
        },
        {
            "id": "hft_dataset",
            "title": "Top‑200 HFT 衍生資料",
            "category": "derived",
            "api_surface": "由即時 Tick / BidAsk 產生",
            "quota": "none",
            "status": "ready" if hft_ready else "partial",
            "status_label": "稽核通過" if hft_ready else "待稽核",
            "detail": "把永豐事件流重建成每秒因果快照、微結構特徵與未來標籤。",
            "coverage": None,
            "latest_at_utc": hft_latest,
            "fields": [
                "價差",
                "簿不平衡",
                "成交方向",
                "1/5/30/60 秒標籤",
                "跨價差 Markout",
            ],
            "metrics": [
                _metric("交易日", hft_totals.get("dates")),
                _metric("總列數", hft_totals.get("rows"), value_format="compact"),
                _metric(
                    "有效特徵列",
                    hft_totals.get("feature_valid_rows"),
                    value_format="compact",
                ),
                _metric(
                    "最新有效率",
                    hft_audit.get("feature_valid_rate"),
                    value_format="percent",
                ),
            ],
            "warnings": [
                "衍生資料不再呼叫 API；上游即時擷取停止時不會自動新增交易日。"
            ],
            "service": None,
        },
        {
            "id": "contract_catalog",
            "title": "Contract V2 期貨目錄",
            "category": "reference",
            "api_surface": "contracts / update event",
            "quota": "none",
            "status": "ready" if inventory else "unavailable",
            "status_label": "目錄就緒" if inventory else "尚無目錄",
            "detail": "保存期貨商品根、R1/R2 連續合約與換月目標，供回補佇列使用。",
            "coverage": _coverage(
                (inventory or {}).get("continuous_contracts"),
                (inventory or {}).get("continuous_contracts"),
                unit="連續合約",
                label="已列舉",
            ),
            "latest_at_utc": inventory_latest,
            "fields": ["商品根", "交易所", "R1/R2", "目前 target code", "合約名稱"],
            "metrics": [
                _metric("期貨商品根", (inventory or {}).get("futures_roots")),
                _metric("R1", (inventory or {}).get("r1_contracts")),
                _metric("R2", (inventory or {}).get("r2_contracts")),
                _metric("連續合約", (inventory or {}).get("continuous_contracts")),
            ],
            "warnings": ["目錄是查詢快照；換月時 target code 仍需重新解析。"],
            "service": None,
        },
        {
            "id": "on_demand_snapshots",
            "title": "策略隨需 Snapshot 行情",
            "category": "on_demand",
            "api_surface": "api.snapshots",
            "quota": "historical",
            "status": "active" if snapshot_service.get("active") else "stopped",
            "status_label": "隨需查詢"
            if snapshot_service.get("active")
            else "服務停止",
            "detail": "供台股策略與 0050／2330／台指期基準取得當下可成交價，不持續輪詢。",
            "coverage": None,
            "latest_at_utc": snapshot_latest,
            "fields": [
                "Last",
                "Bid/Ask",
                "Open/High/Low",
                "Volume",
                "Reference",
                "漲跌停價",
            ],
            "metrics": [
                _metric("策略模式", len(safe_modes)),
                _metric("行情基準", len(safe_benchmarks)),
                _metric("永豐來源類型", len(snapshot_sources)),
                _metric("最新報價", snapshot_latest, value_format="datetime"),
            ],
            "warnings": [
                "Snapshot 是按需查詢，會反映在歷史行情用量；面板本身不會觸發查詢。"
            ],
            "service": _service_view(snapshot_service),
        },
    ]
    for pipeline in pipelines:
        pipeline["latest_age_seconds"] = _age_seconds(
            pipeline.get("latest_at_utc"), now=now
        )
    return pipelines


def build_shioaji_public_status(
    repo_root: Path,
    *,
    now: datetime | None = None,
    runner: CommandRunner = _default_command_runner,
    paths: ShioajiMonitorPaths | None = None,
) -> dict[str, Any]:
    """Return a bounded, allowlisted Shioaji monitoring payload."""

    observed = now or datetime.now(UTC)
    if observed.tzinfo is None:
        observed = observed.replace(tzinfo=UTC)
    observed = observed.astimezone(UTC)
    selected_paths = paths or ShioajiMonitorPaths.from_repo(Path(repo_root))
    history_service = _service_state(HISTORY_UNIT, runner=runner)
    capture_service = _service_state(CAPTURE_UNIT, runner=runner)
    history_entries = _journal_entries(HISTORY_UNIT, runner=runner)
    capture_entries = _journal_entries(CAPTURE_UNIT, runner=runner)
    minute_service = (
        _service_state(MINUTE_UNIT, runner=runner)
        if selected_paths.minute_summary is not None
        else {"active": False, "state": "unavailable", "restarts": None}
    )
    top200_service = (
        _service_state(TOP200_UNIT, runner=runner)
        if selected_paths.top200_capture_root is not None
        else {"active": False, "state": "unavailable", "restarts": None}
    )
    snapshot_service = (
        _service_state(SNAPSHOT_UNIT, runner=runner)
        if selected_paths.snapshot_state is not None
        else {"active": False, "state": "unavailable", "restarts": None}
    )
    top200_entries = (
        _journal_entries(TOP200_UNIT, runner=runner, lines=1_000, since="-7days")
        if selected_paths.top200_capture_root is not None
        else []
    )
    manifests = _history_manifests(selected_paths)
    traffic_history = _traffic_samples(history_entries, manifests)
    ledger_payload = (
        _read_json(selected_paths.traffic_ledger_summary)
        if selected_paths.traffic_ledger_summary is not None
        else None
    )
    ledger = _traffic_ledger_view(ledger_payload)
    storage_payload = (
        _read_json(selected_paths.storage_summary)
        if selected_paths.storage_summary is not None
        else None
    )
    storage = _storage_view(storage_payload, now=observed)
    latest_ledger_usage = (ledger_payload or {}).get("latest_usage")
    if isinstance(latest_ledger_usage, dict):
        ledger_used = latest_ledger_usage.get("used_bytes")
        ledger_limit = latest_ledger_usage.get("limit_bytes")
        ledger_at = latest_ledger_usage.get("observed_at_utc")
        parsed_at = _parse_datetime(ledger_at)
        if isinstance(ledger_used, int) and isinstance(ledger_limit, int) and parsed_at:
            traffic_history.append(
                {
                    "observed_at_utc": parsed_at.isoformat().replace("+00:00", "Z"),
                    "used_bytes": ledger_used,
                    "limit_bytes": ledger_limit,
                    "remaining_bytes": max(0, ledger_limit - ledger_used),
                    "used_ratio": ledger_used / ledger_limit
                    if ledger_limit > 0
                    else None,
                    "source_id": "traffic_ledger",
                    "source_label": str(
                        latest_ledger_usage.get("consumer") or "流量帳本"
                    ),
                }
            )
            traffic_history = sorted(
                traffic_history, key=lambda item: str(item.get("observed_at_utc") or "")
            )[-MAX_TRAFFIC_SAMPLES:]
    backfill = _backfill_status(
        selected_paths, manifests, history_entries, history_service
    )
    capture = _capture_status(
        selected_paths, capture_entries, capture_service, now=observed
    )
    pipelines = _build_pipelines(
        selected_paths,
        now=observed,
        backfill=backfill,
        capture=capture,
        history_service=history_service,
        capture_service=capture_service,
        minute_service=minute_service,
        top200_service=top200_service,
        snapshot_service=snapshot_service,
        top200_entries=top200_entries,
    )

    latest_traffic = traffic_history[-1] if traffic_history else {}
    used = int(latest_traffic.get("used_bytes") or 0)
    limit = int(latest_traffic.get("limit_bytes") or 0)
    guard_limit = (
        min(int(limit * TRAFFIC_FRACTION_GUARD), limit - TRAFFIC_RESERVE_BYTES)
        if limit > 0
        else 0
    )
    traffic = {
        "observed_at_utc": latest_traffic.get("observed_at_utc"),
        "used_bytes": used if limit > 0 else None,
        "limit_bytes": limit if limit > 0 else None,
        "remaining_bytes": max(0, limit - used) if limit > 0 else None,
        "used_ratio": used / limit if limit > 0 else None,
        "guard_fraction": TRAFFIC_FRACTION_GUARD,
        "guard_reserve_bytes": TRAFFIC_RESERVE_BYTES,
        "guard_limit_bytes": guard_limit if limit > 0 else None,
        "safe_remaining_bytes": max(0, guard_limit - used) if limit > 0 else None,
        "reset_policy": "每個交易日上午 08:00 重置",
        "pricing_policy": (
            "官方文件提供免費註冊；api.usage() 僅回傳流量、不含費用欄位，"
            "實際費用依永豐最新帳戶契約"
        ),
        "pricing_evidence_label": "usage 無費用欄位",
        "attributed_bytes": (
            int(ledger["totals"].get("observed_usage_delta_bytes") or 0)
            if ledger_payload is not None
            else None
        ),
        "unattributed_bytes": (
            max(
                0,
                used - int(ledger["totals"].get("observed_usage_delta_bytes") or 0),
            )
            if limit > 0 and ledger_payload is not None
            else None
        ),
        "history": traffic_history,
    }
    traffic_breakdown = _traffic_breakdown(pipelines, ledger)

    candidate_times: list[float] = []
    if traffic.get("observed_at_utc"):
        try:
            candidate_times.append(
                datetime.fromisoformat(
                    str(traffic["observed_at_utc"]).replace("Z", "+00:00")
                ).timestamp()
            )
        except ValueError:
            pass
    latest_capture_age = capture.get("latest_file_age_seconds")
    if isinstance(latest_capture_age, (int, float)):
        candidate_times.append(observed.timestamp() - float(latest_capture_age))
    latest_source_epoch = max(candidate_times, default=0.0)
    source_age = (
        max(0.0, observed.timestamp() - latest_source_epoch)
        if latest_source_epoch
        else None
    )
    failed_pipelines = sum(item.get("status") == "failed" for item in pipelines)
    attention_pipelines = sum(
        item.get("status") in {"failed", "unavailable", "partial", "stopped"}
        for item in pipelines
    )
    if (
        failed_pipelines
        or not history_service.get("active")
        or not capture_service.get("active")
    ):
        health = "degraded"
    elif backfill.get("state") in {"waiting_quota", "waiting_market"}:
        health = "waiting"
    elif capture.get("state") == "capturing":
        health = "active"
    else:
        health = "stale"

    return {
        "dashboard_schema_version": 4,
        "generated_at_utc": observed.isoformat().replace("+00:00", "Z"),
        "health": health,
        "source_age_seconds": round(source_age, 3) if source_age is not None else None,
        "read_only": True,
        "simulation_only": True,
        "production_order_possible": False,
        "traffic": traffic,
        "traffic_ledger": ledger,
        "traffic_breakdown": traffic_breakdown,
        "storage": storage,
        "backfill": backfill,
        "capture": capture,
        "pipeline_summary": {
            "total": len(pipelines),
            "active": sum(item.get("status") == "active" for item in pipelines),
            "ready": sum(
                item.get("status") in {"ready", "complete"} for item in pipelines
            ),
            "waiting": sum(item.get("status") == "waiting" for item in pipelines),
            "attention": attention_pipelines,
            "historical": sum(
                item.get("category") == "historical" for item in pipelines
            ),
            "realtime": sum(item.get("category") == "realtime" for item in pipelines),
        },
        "pipelines": pipelines,
        "definitions": {
            "traffic": ("Shioaji api.usage() 的歷史行情用量；即時訂閱不消耗此額度"),
            "safe_remaining": (
                "90% 用量上限與保留 128 MiB 兩個條件中較嚴格者，扣除已用量"
            ),
            "backfill_progress": (
                "已產生有效 receipt 的合約交易日數，除以 743 個連續合約的目標交易日總數"
            ),
            "pipeline_status": (
                "完成看最終稽核，執行中看服務與新鮮落盤；服務停止不會抹掉已完成資料"
            ),
            "quota_classes": (
                "historical 會消耗歷史查詢流量；realtime 是推送訂閱；none 是本機衍生或目錄"
            ),
            "traffic_attribution": (
                "可歸因流量是各功能呼叫前後的 api.usage() 正差；帳面已用與可歸因差額保留為未歸因，絕不假設為零"
            ),
            "storage_growth": (
                "mtime 指標是最近 30 個完整台北曆日的變動檔案量，不等於淨增加；"
                "每日總量快照滿 7 日後才用實測淨成長推估容量"
            ),
        },
    }


__all__ = [
    "CAPTURE_UNIT",
    "HISTORY_UNIT",
    "MINUTE_UNIT",
    "MAX_TRAFFIC_SAMPLES",
    "SNAPSHOT_UNIT",
    "ShioajiMonitorPaths",
    "TOP200_UNIT",
    "build_shioaji_public_status",
]

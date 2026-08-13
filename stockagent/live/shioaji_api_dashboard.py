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
        )


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


def _iso_from_epoch(value: float) -> str:
    return datetime.fromtimestamp(value, tz=UTC).isoformat().replace("+00:00", "Z")


def _journal_entries(
    unit: str,
    *,
    runner: CommandRunner,
    lines: int = 5_000,
) -> list[dict[str, Any]]:
    try:
        result = runner(
            (
                "journalctl",
                "--unit",
                unit,
                "--since=-24hours",
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
    manifests = _history_manifests(selected_paths)
    traffic_history = _traffic_samples(history_entries, manifests)
    backfill = _backfill_status(
        selected_paths, manifests, history_entries, history_service
    )
    capture = _capture_status(
        selected_paths, capture_entries, capture_service, now=observed
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
        "history": traffic_history,
    }

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
    if not history_service.get("active") or not capture_service.get("active"):
        health = "degraded"
    elif backfill.get("state") in {"waiting_quota", "waiting_market"}:
        health = "waiting"
    elif capture.get("state") == "capturing":
        health = "active"
    else:
        health = "stale"

    return {
        "dashboard_schema_version": 1,
        "generated_at_utc": observed.isoformat().replace("+00:00", "Z"),
        "health": health,
        "source_age_seconds": round(source_age, 3) if source_age is not None else None,
        "read_only": True,
        "simulation_only": True,
        "production_order_possible": False,
        "traffic": traffic,
        "backfill": backfill,
        "capture": capture,
        "definitions": {
            "traffic": ("Shioaji api.usage() 的歷史行情用量；即時訂閱不消耗此額度"),
            "safe_remaining": (
                "90% 用量上限與保留 128 MiB 兩個條件中較嚴格者，扣除已用量"
            ),
            "backfill_progress": (
                "已產生有效 receipt 的合約交易日數，除以 743 個連續合約的目標交易日總數"
            ),
        },
    }


__all__ = [
    "CAPTURE_UNIT",
    "HISTORY_UNIT",
    "MAX_TRAFFIC_SAMPLES",
    "ShioajiMonitorPaths",
    "build_shioaji_public_status",
]

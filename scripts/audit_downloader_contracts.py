from __future__ import annotations

import argparse
import ast
import csv
import json
import re
import subprocess
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    # Direct CLI execution starts with scripts/, unlike `python -m scripts...`.
    # Resolve the shared service parser without requiring a caller PYTHONPATH.
    sys.path.insert(0, str(ROOT))


URL_RE = re.compile(r"https?://[^\s\"')]+")
HTTP_MARKERS = (
    "urlopen(",
    "yfinance",
    ".get_json(",
    ".get_bytes(",
    ".request_bytes(",
)
SDK_MARKERS = ("api.kbars(", "api.ticks(", "api.subscribe(", "shioaji_query(")
DELEGATED_TRANSPORT_MARKERS = (
    "client.get(",
    "ResilientHttpTransport",
    "run_historical_feature_downloads(",
    "_http_get(",
)
RETRY_MARKERS = ("max_retries", "retries", "retry_after", "backoff", "Retry-After")
ATOMIC_MARKERS = (
    "os.replace(",
    ".replace(path",
    "atomic_write_",
    "_write_parquet_atomic",
    ".tmp",
)
PROGRESS_MARKERS = ("tqdm", "progress", "eta", "estimated")
RECEIPT_MARKERS = (
    "receipt",
    "manifest",
    "download_summary",
    "download_report",
    "funding_summary",
    "source_status",
)
RATE_PROFILE_CALL_RE = re.compile(r"provider_rate_limit\(\s*[\"']([^\"']+)")
ENV_ACCESS_RE = re.compile(
    r"(?:getenv\(|environ(?:\.get)?\[?\()?\s*[\"']([A-Z][A-Z0-9_]+)[\"']"
)


@dataclass(slots=True)
class AuditRow:
    file: str
    kind: str
    lines: int
    functions: int
    classes: int
    networked: bool
    transport: str
    endpoint_count: int
    endpoints: str
    has_timeout: bool
    has_retry: bool
    has_shared_rate_limiter: bool
    has_parallelism: bool
    has_progress: bool
    has_atomic_publication: bool
    has_receipt_or_manifest: bool
    has_main_guard: bool
    scheduled_by_daily_runner: bool
    provider_profiles: str
    credential_env_vars: str
    contract_grade: str
    static_flags: str
    referenced_by_services: str = ""


def _kind(path: Path, source: str) -> str:
    name = path.name
    if name.startswith("stream_"):
        return "realtime_stream"
    if name.startswith("audit_") or name == "status.py":
        return "audit_or_status"
    if name.startswith("download_") or name.startswith("update_"):
        if "import main" in source and len(source.splitlines()) < 80:
            return "wrapper"
        return "downloader"
    if name.startswith("build_") or name.startswith("backfill_"):
        return "builder_or_backfill"
    if name.startswith(("refresh_", "run_", "process_")):
        return "orchestrator"
    return "support"


def _audit_file(path: Path, root: Path, *, daily_runner: str = "") -> AuditRow:
    source = path.read_text(encoding="utf-8")
    lines = source.splitlines()
    tree = ast.parse(source, filename=str(path))
    endpoints = sorted(set(URL_RE.findall(source)))
    direct_http = any(marker in source for marker in HTTP_MARKERS) or bool(
        re.search(r"\brequests\.(?:get|post|request|Session)\b", source)
    )
    sdk = any(marker in source for marker in SDK_MARKERS)
    delegated = any(marker in source for marker in DELEGATED_TRANSPORT_MARKERS)
    networked = direct_http or sdk or delegated
    if direct_http:
        transport = "http"
    elif sdk:
        transport = "provider_sdk"
    elif delegated:
        transport = "delegated_client"
    else:
        transport = "none"
    writes_tabular = any(
        marker in source
        for marker in ("write_parquet", "to_parquet", "write_csv", "to_csv")
    )
    atomic = any(marker in source for marker in ATOMIC_MARKERS)
    has_retry = any(marker in source for marker in RETRY_MARKERS)
    has_limiter = any(
        marker in source
        for marker in (
            "SharedRateLimiter",
            "SharedRequestRateLimiter",
            "ResilientHttpTransport",
            "_global_tw_public_rate_limiter",
        )
    )
    has_receipt = any(marker in source for marker in RECEIPT_MARKERS)
    flags: list[str] = []
    kind = _kind(path, source)
    entrypoint = kind in {"downloader", "builder_or_backfill", "realtime_stream"}
    if direct_http and entrypoint and "timeout" not in source.lower():
        flags.append("network_without_explicit_timeout")
    if direct_http and entrypoint and not has_retry:
        flags.append("network_without_visible_retry_contract")
    if direct_http and entrypoint and not has_limiter:
        flags.append("no_shared_limiter_visible")
    if writes_tabular and entrypoint and not atomic:
        flags.append("tabular_write_without_visible_atomic_replace")
    if networked and entrypoint and not has_receipt:
        flags.append("no_receipt_or_manifest_visible")
    scheduled = path.name in daily_runner
    if not networked:
        grade = "not_networked"
    elif kind == "support":
        grade = "delegated_support"
    elif flags:
        grade = "review"
    elif (
        has_receipt
        and (has_limiter or transport in {"delegated_client", "provider_sdk"})
        and (not direct_http or has_retry or transport == "provider_sdk")
    ):
        grade = "contract_visible"
    else:
        grade = "partial"

    return AuditRow(
        file=str(path.relative_to(root)),
        kind=kind,
        lines=len(lines),
        functions=sum(
            isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
            for node in ast.walk(tree)
        ),
        classes=sum(isinstance(node, ast.ClassDef) for node in ast.walk(tree)),
        networked=networked,
        transport=transport,
        endpoint_count=len(endpoints),
        endpoints=" | ".join(endpoints),
        has_timeout="timeout" in source.lower(),
        has_retry=has_retry,
        has_shared_rate_limiter=has_limiter,
        has_parallelism=any(
            marker in source
            for marker in (
                "ThreadPoolExecutor",
                "ProcessPoolExecutor",
                "asyncio",
                "run_parallel_tasks",
            )
        ),
        has_progress=any(marker in source.lower() for marker in PROGRESS_MARKERS),
        has_atomic_publication=atomic,
        has_receipt_or_manifest=has_receipt,
        has_main_guard='if __name__ == "__main__"' in source,
        scheduled_by_daily_runner=scheduled,
        provider_profiles=" | ".join(sorted(set(RATE_PROFILE_CALL_RE.findall(source)))),
        credential_env_vars=" | ".join(
            sorted(
                value
                for value in set(ENV_ACCESS_RE.findall(source))
                if value.endswith(("KEY", "TOKEN", "SECRET", "PASSWORD", "EMAIL"))
                or "API" in value
            )
        ),
        contract_grade=grade,
        static_flags=" | ".join(flags),
    )


def discover_python_entrypoints(root: Path) -> list[Path]:
    """Return canonical downloader modules plus script-level data collectors."""

    paths = set((root / "downloader").glob("*.py"))
    scripts_dir = root / "scripts"
    for pattern in ("download_*.py", "fetch_*.py", "watch_*.py", "refresh_*.py",
                    "backfill_*.py", "process_*.py", "run_*.py"):
        paths.update(scripts_dir.glob(pattern))
    return sorted(path for path in paths if path.name != "__init__.py")


def script_references(source: str, root: Path, paths: Iterable[Path], *,
                      source_path: Path | None = None) -> set[str]:
    """Static references, not proof that every branch ran or loaded this code.

    Emit known repository paths only, never ExecStart arguments, env values,
    URLs or credentials. Shell wrappers often use SCRIPT_DIR plus a basename.
    """
    candidates = {path.relative_to(root).as_posix() for path in paths}
    names = set(re.findall(r"[A-Za-z0-9_-]+\.(?:py|sh)\b", source))
    modules = set(re.findall(r"\b(?:downloader|scripts)\.[A-Za-z0-9_.]+", source))
    module_paths = {module.replace(".", "/") + ".py" for module in modules}
    if source_path is not None and source_path.suffix == ".py":
        tree = ast.parse(source, filename=str(source_path))
        parts = source_path.relative_to(root).with_suffix("").parts
        for node in ast.walk(tree):
            if not isinstance(node, ast.ImportFrom):
                continue
            package = list(parts[:-node.level]) if node.level else []
            package.extend(node.module.split(".") if node.module else [])
            module_paths.add("/".join(package) + ".py")
            module_paths.update("/".join([*package, alias.name]) + ".py"
                                for alias in node.names)
    return {name for name in candidates if Path(name).name in names or name in module_paths}


def runtime_schedule_snapshot(root: Path, paths: list[Path]) -> tuple[dict, dict[str, set[str]]]:
    """Read installed production owners/timers in two bounded systemctl queries.

    No API calls, service starts or environment/command-line exports. A failed
    read is explicit, not an empty snapshot presented as healthy scheduling.
    """
    from scripts.audit_service_latency_coverage import _unit_blocks

    listed = subprocess.run(
        ["systemctl", "list-unit-files", "stockagent-*.service", "stockagent-*.timer",
         "--no-legend", "--no-pager"],
        capture_output=True, text=True, timeout=15, check=True,
    )
    unit_names = sorted({line.split()[0] for line in listed.stdout.splitlines()
                         if line.split() and "@." not in line.split()[0]
                         and not line.split()[0].startswith("stockagent-agent-")
                         and re.fullmatch(r"stockagent-[A-Za-z0-9@_.:-]+\.(?:service|timer)", line.split()[0])})
    if not unit_names:
        raise RuntimeError("systemd returned no installed StockAgent units")
    properties = (
        "Id", "LoadState", "ActiveState", "SubState", "Result", "MainPID",
        "InvocationID", "ExecMainStartTimestamp", "ExecMainExitTimestamp", "ExecMainStatus",
        "Triggers", "TriggeredBy", "UnitFileState", "TimersMonotonic", "TimersCalendar",
        "LastTriggerUSec", "NextElapseUSecRealtime", "ExecStart",
    )
    shown = subprocess.run(
        ["systemctl", "show", *unit_names, "--all", "--no-pager", "--property=" + ",".join(properties)],
        capture_output=True, text=True, timeout=20, check=True,
    )
    units = {**_unit_blocks(shown.stdout, suffix=".service"),
             **_unit_blocks(shown.stdout, suffix=".timer")}
    shells = sorted(set((root / "scripts").glob("run_*.sh")) |
                    set((root / "scripts").glob("*runner*.sh")) |
                    set((root / "downloader").glob("*.sh")))
    all_paths = sorted(set(paths) | set(shells))
    graph = {path.relative_to(root).as_posix(): script_references(
        path.read_text(encoding="utf-8"), root, all_paths, source_path=path) for path in all_paths}
    by_file: dict[str, set[str]] = {}
    for unit, values in units.items():
        roots = script_references(values.pop("ExecStart", ""), root, all_paths)
        visited: set[str] = set()
        pending = list(roots)
        while pending:
            path = pending.pop()
            if path in visited:
                continue
            visited.add(path)
            pending.extend(graph.get(path, set()) - visited)
        values["repository_entrypoints"] = sorted(roots)
        values["static_referenced_files"] = sorted(visited)
        if unit.endswith(".service"):
            for path in visited:
                by_file.setdefault(path, set()).add(unit)
    return {
        "scope": ("installed StockAgent production units (transient engineering runs excluded); "
                  "repo-local static reference closure, not loaded-code proof"),
        "units": units,
        "failed_services": sorted(unit for unit, value in units.items()
                                  if unit.endswith(".service") and value.get("ActiveState") == "failed"),
    }, by_file


def _write_csv(rows: Iterable[AuditRow], path: Path) -> None:
    records = [asdict(row) for row in rows]
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    try:
        with temporary.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(records[0]))
            writer.writeheader()
            writer.writerows(records)
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Create a reproducible static inventory of downloader contracts. "
            "Flags are review leads, not proof that behavior is incorrect."
        )
    )
    parser.add_argument("--repo-root", default=".")
    parser.add_argument("--output-dir", default="artifacts/downloader_review/latest")
    parser.add_argument("--include-runtime", action="store_true",
                        help="Join installed systemd owners/timers without contacting providers")
    args = parser.parse_args()

    root = Path(args.repo_root).resolve()
    daily_runner_path = root / "downloader" / "run_daily_all_markets.sh"
    daily_runner = (
        daily_runner_path.read_text(encoding="utf-8")
        if daily_runner_path.is_file()
        else ""
    )
    paths = discover_python_entrypoints(root)
    rows = [_audit_file(path, root, daily_runner=daily_runner) for path in paths]
    runtime = None
    if args.include_runtime:
        runtime, references = runtime_schedule_snapshot(root, paths)
        for row in rows:
            row.referenced_by_services = " | ".join(sorted(references.get(row.file, ())))
    output_dir = (root / args.output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    csv_path = output_dir / "downloader_inventory.csv"
    json_path = output_dir / "downloader_inventory.json"
    _write_csv(rows, csv_path)
    payload = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "scope": (
            "downloader/*.py plus scripts/{download,fetch,watch,refresh,backfill,process,run}_*.py"
        ),
        "file_count": len(rows),
        "networked_file_count": sum(row.networked for row in rows),
        "flagged_file_count": sum(bool(row.static_flags) for row in rows),
        "scheduled_networked_file_count": sum(
            row.networked and row.scheduled_by_daily_runner for row in rows
        ),
        "service_referenced_networked_file_count": sum(
            row.networked and bool(row.referenced_by_services) for row in rows
        ) if runtime is not None else None,
        "schedule_count_interpretation": (
            "scheduled_networked_file_count counts literal daily-runner references only; "
            "service_referenced_networked_file_count joins installed service reference closure, "
            "not executed branches or independent collectors"
        ),
        "contract_visible_file_count": sum(
            row.contract_grade == "contract_visible" for row in rows
        ),
        "warning": "Static flags require manual review and are not correctness verdicts.",
        "runtime_schedule": runtime,
        "rows": [asdict(row) for row in rows],
    }
    temporary = json_path.with_name(f".{json_path.name}.tmp")
    try:
        temporary.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        temporary.replace(json_path)
    finally:
        temporary.unlink(missing_ok=True)
    print(
        json.dumps(
            {
                key: payload[key]
                for key in (
                    "scope",
                    "file_count",
                    "networked_file_count",
                    "flagged_file_count",
                )
            }
        )
    )
    print(csv_path)
    print(json_path)


if __name__ == "__main__":
    main()

from __future__ import annotations

import argparse
import ast
import csv
import json
import re
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable


URL_RE = re.compile(r"https?://[^\s\"')]+")
HTTP_MARKERS = (
    "urlopen(",
    "yfinance",
    ".get_json(",
    ".get_bytes(",
)
SDK_MARKERS = ("api.kbars(", "api.ticks(", "api.subscribe(", "shioaji_query(")
DELEGATED_TRANSPORT_MARKERS = (
    "client.get(",
    "run_historical_feature_downloads(",
    "_http_get(",
)
RETRY_MARKERS = ("max_retries", "retries", "retry_after", "backoff", "Retry-After")
ATOMIC_MARKERS = ("os.replace(", ".replace(path", "atomic", ".tmp")
PROGRESS_MARKERS = ("tqdm", "progress", "eta", "estimated")
RECEIPT_MARKERS = ("receipt", "manifest", "download_summary", "download_report")


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
    static_flags: str


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
    return "support"


def _audit_file(path: Path, root: Path) -> AuditRow:
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
        static_flags=" | ".join(flags),
    )


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
    args = parser.parse_args()

    root = Path(args.repo_root).resolve()
    downloader_dir = root / "downloader"
    paths = sorted(downloader_dir.glob("*.py"))
    rows = [_audit_file(path, root) for path in paths]
    output_dir = (root / args.output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    csv_path = output_dir / "downloader_inventory.csv"
    json_path = output_dir / "downloader_inventory.json"
    _write_csv(rows, csv_path)
    payload = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "scope": "downloader/*.py",
        "file_count": len(rows),
        "networked_file_count": sum(row.networked for row in rows),
        "flagged_file_count": sum(bool(row.static_flags) for row in rows),
        "warning": "Static flags require manual review and are not correctness verdicts.",
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

#!/usr/bin/env python3
"""Refresh the existing Bybit 00:05 training chain without a parallel fetch stack.

The registered collectors own raw ingestion. The Bybit build and publication
share their catalogued source lease; cross-venue reports have separate health.
"""

from __future__ import annotations

import argparse
from datetime import date, datetime, timedelta, timezone
import fcntl
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
import uuid

ROOT = Path(__file__).resolve().parents[1]
DOWNLOADER = ROOT / "downloader"
sys.path.insert(0, str(DOWNLOADER))
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from artifact_io import atomic_write_json  # noqa: E402
from scripts import publish_data_releases as releases  # noqa: E402
from scripts.bybit_refresh_inputs import materialization_input_signature as _input_signature  # noqa: E402

REFRESH_CONTRACT_VERSION = 5
# Cooperative raw writers wait 180 seconds. Budget subprocess waits to leave
# cleanup room; metadata I/O/fsync is not covered by a hard wall-clock bound.
SOURCE_LEASE_SECONDS = 150.0
TRANSPORT_TIMEOUT_SECONDS = 300.0
BYBIT_WRITERS = frozenset({
    "download_bybit_perp_1m.py", "download_bybit_perp_daily.py",
    "download_bybit_funding_history.py", "materialize_bybit_perpetual_daily.py",
    "build_bybit_crypto_public_daily_features.py", "build_bybit_venue_daily_features.py",
})

RAW_WRITERS = {
    "download_bybit_perp_1m.py",
    "download_bybit_perp_daily.py",
    "download_binance_perp_1m.py",
    "download_binance_perp_15m.py",
    "download_okx_perp_1m.py",
    "download_okx_perp_daily.py",
    "okx_historical_features.py",
    "download_bybit_funding_history.py",
    "materialize_bybit_perpetual_daily.py",
    "build_bybit_crypto_public_daily_features.py",
    "build_bybit_venue_daily_features.py",
    "download_free_public_context.py",
    "download_crypto_etf_history.py",
    "download_fred_crypto_macro_vintages.py",
    "download_coinmetrics_community.py",
    "download_crypto_keyed_context.py",
}
SOURCE_SUMMARIES = (
    "data_bybit/1m/download_summary.json",
    "data_binance/1m/download_summary.json",
    "data_okx/1m/download_summary.json",
    "data_okx/download_summary.json",
    "data_free_public/download_summary.json",
    "data_fred_crypto_macro/download_summary.json",
    "data_crypto_etf/download_summary.json",
    "data_coinmetrics_community/download_summary.json",
    "data_crypto_reference/download_summary.json",
)


def source_signatures(names: tuple[str, ...] = SOURCE_SUMMARIES) -> dict[str, list[int] | None]:
    signatures = {}
    for name in names:
        try:
            stat = (ROOT / name).stat()
            signatures[name] = [stat.st_mtime_ns, stat.st_size]
        except FileNotFoundError:
            signatures[name] = None
    return signatures


def materialization_input_signature(root: Path | None = None) -> dict[str, object]:
    """Use the same metadata guard as publication, alongside its byte audit."""
    return _input_signature(ROOT if root is None else root)


def _publication_result(path: Path) -> dict[str, object]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    rows = payload.get("published", [])
    if len(rows) != 1 or rows[0].get("dataset") != "bybit" or payload.get("skipped"):
        raise RuntimeError("publisher result does not identify one Bybit release")
    result = rows[0]
    digest = result.get("inventory_sha256")
    if (
        not result.get("snapshot_id") or not isinstance(digest, str)
        or len(digest) != 64 or any(c not in "0123456789abcdef" for c in digest)
    ):
        raise RuntimeError("publisher result lacks a snapshot ID or inventory digest")
    return result


def expected_complete_date() -> str | None:
    path = ROOT / "data_bybit/1m/download_summary.json"
    if not path.is_file():
        return None
    raw_date = json.loads(path.read_text(encoding="utf-8")).get("end_date")
    if not raw_date:
        return None
    return min(
        date.fromisoformat(str(raw_date)[:10]),
        datetime.now(timezone.utc).date() - timedelta(days=1),
    ).isoformat()


def active_raw_writers(names: set[str] | frozenset[str] = RAW_WRITERS) -> list[str]:
    found: set[str] = set()
    for proc in Path("/proc").iterdir():
        if not proc.name.isdigit() or int(proc.name) == os.getpid():
            continue
        try:
            args = (proc / "cmdline").read_bytes().split(b"\0")
        except (OSError, PermissionError):
            continue
        for arg in args:
            name = Path(os.fsdecode(arg)).name
            if name in names:
                found.add(f"{proc.name}:{name}")
    return sorted(found)


def _bybit_source_entry() -> dict[str, object]:
    entry = next(
        item for item in releases._load_catalog(ROOT / "configs/data_sync/packed_datasets.json")
        if item["dataset"] == "bybit"
    )
    if not entry.get("source_coordination_lock"):
        raise RuntimeError("Bybit catalog source coordination lock is required")
    return entry


def _run_step_process(
    command: list[str], *, deadline: float | None, source_lock_fd: int | None,
) -> None:
    remaining = None if deadline is None else deadline - time.monotonic()
    if remaining is not None and remaining <= 0:
        raise subprocess.TimeoutExpired(command, SOURCE_LEASE_SECONDS)
    process = subprocess.Popen(
        command, cwd=ROOT, start_new_session=True,
        pass_fds=() if source_lock_fd is None else (source_lock_fd,),
    )
    try:
        result = process.wait(timeout=remaining)
        if result:
            raise subprocess.CalledProcessError(result, command)
    except BaseException:
        # Kill the whole owned session, not just its launcher. The inherited
        # source FD keeps the lease alive if a killed writer cannot yet exit.
        try:
            os.killpg(process.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        try:
            process.wait(timeout=2)
        except subprocess.TimeoutExpired:
            pass
        finally:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
        process.wait(timeout=5)
        raise


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--publish", action="store_true")
    parser.add_argument("--sync-root", default="/srv/stockagent-packed")
    parser.add_argument("--output-dir", type=Path, default=ROOT / "artifacts/data_quality/crypto_training_latest")
    args = parser.parse_args()
    defer_scan = args.publish and (Path(args.sync_root) / ".stockagent-d-primary").is_file()
    output = args.output_dir.resolve()
    output.mkdir(parents=True, exist_ok=True)
    lock_path = output / "refresh.lock"
    with lock_path.open("a+b") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            print("crypto training refresh already running; deferred", flush=True)
            return
        receipt: dict[str, object] = {
            "refresh_contract_version": REFRESH_CONTRACT_VERSION,
            "started_at_utc": datetime.now(timezone.utc).isoformat(),
            "state": "running",
            "steps": [],
            "step_attempts": [],
            "core_state": "pending",
            "publication_state": "not_attempted",
            "transport_state": "not_attempted" if defer_scan else "not_required",
            "reports": [],
        }

        def save() -> None:
            atomic_write_json(output / "refresh_receipt.json", receipt)

        def defer_for_raw_writer(writers: list[str]) -> None:
            receipt.update(
                state="deferred_raw_writer",
                core_state="deferred",
                active_writers=writers,
                finished_at_utc=datetime.now(timezone.utc).isoformat(),
            )
            save()
            print(json.dumps(receipt, ensure_ascii=False), flush=True)

        def defer_for_source_change(before: dict[str, object], after: dict[str, object]) -> None:
            receipt.update(
                state="deferred_source_changed",
                core_state="deferred",
                input_signature_before=before,
                input_signature_after=after,
                finished_at_utc=datetime.now(timezone.utc).isoformat(),
            )
            save()
            print(json.dumps(receipt, ensure_ascii=False), flush=True)

        def run_timed_step(
            command: list[str], *, deadline: float | None = None,
            source_lock_fd: int | None = None,
        ) -> dict[str, object]:
            attempt: dict[str, object] = {
                "command": command,
                "started_at_utc": datetime.now(timezone.utc).isoformat(),
                "status": "running",
            }
            receipt["active_step"] = attempt
            save()
            step_started = time.monotonic()
            try:
                _run_step_process(command, deadline=deadline, source_lock_fd=source_lock_fd)
            except Exception as exc:
                attempt["status"] = "timed_out" if isinstance(exc, subprocess.TimeoutExpired) else "failed"
                attempt["error_type"] = type(exc).__name__
                attempt["elapsed_seconds"] = round(time.monotonic() - step_started, 3)
                receipt["step_attempts"].append(attempt)
                receipt.pop("active_step", None)
                save()
                raise
            attempt["status"] = "process_completed"
            attempt["elapsed_seconds"] = round(time.monotonic() - step_started, 3)
            receipt["step_attempts"].append(attempt)
            receipt.pop("active_step", None)
            save()
            return attempt

        busy = active_raw_writers(BYBIT_WRITERS)
        if busy:
            defer_for_raw_writer(busy)
            return
        previous_path = output / "refresh_receipt.json"
        previous = json.loads(previous_path.read_text(encoding="utf-8")) if previous_path.is_file() else {}

        def already_current() -> bool:
            prior_inventory = previous.get("inventory_summary") or {}
            return (
                previous.get("state") == "completed"
                and previous.get("refresh_contract_version") == REFRESH_CONTRACT_VERSION
                and prior_inventory.get("materialization_current_to_raw")
                and prior_inventory.get("expected_last_complete_training_date")
                == expected_complete_date()
                and previous.get("source_signatures") == source_signatures()
                and previous.get("materialization_input_signature")
                == materialization_input_signature()
                and (not args.publish or previous.get("core_state") == "published")
            )
        core_steps = [
            [sys.executable, "downloader/download_bybit_funding_history.py", "--workers", "8"],
            [sys.executable, "downloader/materialize_bybit_perpetual_daily.py", "--execution-minute-utc", "5", "--workers", "8"],
            [sys.executable, "scripts/build_bybit_venue_daily_features.py"],
            [sys.executable, "scripts/report_crypto_training_features.py", "--output-dir", str(output / "bybit")],
        ]
        save()

        def source_transaction() -> bool | None:
            with releases._source_coordination_lock(
                _bybit_source_entry(), wait_seconds=0,
            ) as source_lock_fd:
                if already_current():
                    # The no-op decision needs the same source lease as a build.
                    receipt.update(
                        state="running", core_state=previous["core_state"],
                        publication_state=previous.get("publication_state", "not_attempted"),
                        publication_result=previous.get("publication_result"),
                        reused_previous_started_at_utc=previous.get("started_at_utc"),
                        inventory_summary=previous["inventory_summary"],
                        materialization_input_signature=previous["materialization_input_signature"],
                        source_signatures=previous["source_signatures"],
                        no_op=True,
                    )
                    save()
                    print("crypto training refresh already current for this UTC day", flush=True)
                    return True
                lease_started = time.monotonic()
                deadline = lease_started + SOURCE_LEASE_SECONDS
                receipt["source_lease_seconds"] = SOURCE_LEASE_SECONDS
                receipt["core_state"] = "running"
                materialization_inputs: dict[str, object] | None = None
                for command in core_steps:
                    busy = active_raw_writers(BYBIT_WRITERS)
                    if busy:
                        defer_for_raw_writer(busy)
                        return
                    if command[1].endswith("materialize_bybit_perpetual_daily.py"):
                        materialization_inputs = materialization_input_signature()
                    attempt = run_timed_step(
                        command, deadline=deadline, source_lock_fd=source_lock_fd,
                    )
                    if materialization_inputs is not None:
                        current_inputs = materialization_input_signature()
                        if current_inputs != materialization_inputs:
                            defer_for_source_change(materialization_inputs, current_inputs)
                            return
                    receipt["steps"].append({
                        "command": command,
                        "completed_at_utc": datetime.now(timezone.utc).isoformat(),
                        "elapsed_seconds": attempt["elapsed_seconds"],
                    })
                    save()
                inventory = json.loads((output / "bybit/summary.json").read_text(encoding="utf-8"))
                if not inventory["source_receipt_hashes_match"]:
                    raise RuntimeError("public feature receipt hashes do not match")
                if not inventory["materialization_current_to_raw"]:
                    raise RuntimeError(
                        "daily training table is behind last completed UTC day: "
                        f"{inventory['daily_last_date']} < "
                        f"{inventory['expected_last_complete_training_date']}"
                    )
                busy = active_raw_writers(BYBIT_WRITERS)
                if busy:
                    defer_for_raw_writer(busy)
                    return
                current_inputs = materialization_input_signature()
                if current_inputs != materialization_inputs:
                    defer_for_source_change(materialization_inputs, current_inputs)
                    return
                if args.publish:
                    result_path = output / f"publication-{uuid.uuid4().hex}.json"
                    receipt.update(publication_state="attempting", publication_result_path=str(result_path))
                    save()
                    command = [
                        sys.executable, "scripts/publish_data_releases.py", "publish", "bybit",
                        "--sync-root", args.sync_root,
                        "--expected-bybit-input-signature", json.dumps(materialization_inputs, sort_keys=True),
                        "--result-receipt", str(result_path),
                        *(["--defer-scan"] if defer_scan else []),
                        "--source-lock-fd", str(source_lock_fd),
                    ]
                    try:
                        attempt = run_timed_step(
                            command, deadline=deadline, source_lock_fd=source_lock_fd,
                        )
                        result = _publication_result(result_path)
                        if defer_scan and result.get("scan_policy") != "durably_queued":
                            raise RuntimeError("publisher did not prove the requested durable scan queue")
                    except Exception:
                        # The head can commit before a later scan/receipt fails.
                        # A timeout or nonzero exit is not proof of no release.
                        receipt["publication_state"] = "outcome_unknown"
                        raise
                    receipt.update(publication_state="published", publication_result=result)
                    receipt["steps"].append({
                        "command": command,
                        "completed_at_utc": datetime.now(timezone.utc).isoformat(),
                        "elapsed_seconds": attempt["elapsed_seconds"],
                    })
                    current_inputs = materialization_input_signature()
                    if current_inputs != materialization_inputs:
                        receipt.update(
                            state="needs_reconciliation", core_state="needs_reconciliation",
                            input_signature_before=materialization_inputs,
                            input_signature_after=current_inputs,
                            finished_at_utc=datetime.now(timezone.utc).isoformat(),
                        )
                        save()
                        print(json.dumps(receipt, ensure_ascii=False), flush=True)
                        return
                receipt.update(
                    core_state="published" if args.publish else "ready",
                    core_finished_at_utc=datetime.now(timezone.utc).isoformat(),
                    materialization_input_signature=current_inputs,
                    inventory_summary=inventory,
                    source_lease_elapsed_seconds=round(time.monotonic() - lease_started, 3),
                )
                save()
            return True

        def drain_transport() -> None:
            if not defer_scan:
                return
            scan_path = output / f"transport-{uuid.uuid4().hex}.json"
            command = [
                sys.executable, "scripts/retry_packed_syncthing_scans.py",
                "--dataset", "bybit", "--receipt", str(scan_path),
                "--batch-object-paths",
            ]
            receipt.update(transport_state="scanning", transport_result_path=str(scan_path))
            save()
            try:
                attempt = run_timed_step(
                    command, deadline=time.monotonic() + TRANSPORT_TIMEOUT_SECONDS,
                )
                result = json.loads(scan_path.read_text(encoding="utf-8"))
                if not isinstance(result, dict) or result.get("dataset") != "bybit" or result.get("status") not in {
                    "scan_request_acknowledged", "concurrent_scan_completed", "idle_no_pending",
                }:
                    raise RuntimeError("scan receipt did not acknowledge the Bybit notification")
                policy = result.get("requested_scan_policy")
                if (
                    not isinstance(policy, dict)
                    or set(policy) != {"batch_object_paths"}
                    or policy["batch_object_paths"] is not True
                ):
                    raise RuntimeError("scan receipt did not confirm the requested batch policy")
                receipt["transport_result"] = result
                receipt["transport_state"] = (
                    "request_acknowledged" if result["status"] == "scan_request_acknowledged" else "no_pending"
                )
                receipt["steps"].append({"command": command, "elapsed_seconds": attempt["elapsed_seconds"]})
            except Exception as exc:
                # The immutable release is already committed. Transport failure
                # cannot revoke that proof or be labelled a source-lease timeout.
                receipt.update(transport_state="pending", transport_error=f"{type(exc).__name__}: {exc}")
            save()

        try:
            if not source_transaction():
                return
            # All source children are reaped, the owner context has closed its
            # descriptor, and this independent subprocess inherits no source FD.
            drain_transport()
            if receipt.get("no_op"):
                receipt.update(
                    state="completed_with_transport_pending" if receipt["transport_state"] == "pending" else "completed",
                    finished_at_utc=datetime.now(timezone.utc).isoformat(),
                )
                save()
                print(json.dumps(receipt, ensure_ascii=False), flush=True)
                return
            # These reports remain part of the service, but unrelated live
            # venues cannot invalidate a completed Bybit source transaction.
            reports = [
                ("coverage", RAW_WRITERS, SOURCE_SUMMARIES,
                 [sys.executable, "scripts/audit_crypto_historical_coverage.py", "--output-dir", str(output)]),
                ("okx", {"download_okx_perp_1m.py", "download_okx_perp_daily.py", "okx_historical_features.py"},
                 ("data_okx/1m/download_summary.json", "data_okx/download_summary.json"),
                 [sys.executable, "scripts/report_crypto_venue_1m_features.py", "okx", "--output-dir", str(output / "okx")]),
                ("binance", {"download_binance_perp_1m.py", "download_binance_perp_15m.py"},
                 ("data_binance/1m/download_summary.json",),
                 [sys.executable, "scripts/report_crypto_venue_1m_features.py", "binance", "--output-dir", str(output / "binance")]),
            ]
            for name, writers, sources, command in reports:
                report = {"name": name, "state": "running"}
                receipt["reports"].append(report)
                busy = active_raw_writers(writers)
                if busy:
                    report.update(state="deferred_raw_writer", active_writers=busy)
                    save()
                    continue
                before = source_signatures(sources)
                try:
                    attempt = run_timed_step(command)
                except Exception:
                    report["state"] = "failed"
                    raise
                busy = active_raw_writers(writers)
                report.update(
                    state="deferred_source_changed" if busy or source_signatures(sources) != before else "completed",
                    elapsed_seconds=attempt["elapsed_seconds"],
                    evidence="source_summary_metadata_and_writer_check",
                )
                receipt["steps"].append({"command": command, "elapsed_seconds": attempt["elapsed_seconds"]})
                save()
            receipt.update(
                state=("completed_with_transport_pending" if receipt["transport_state"] == "pending" else
                       "completed" if all(r["state"] == "completed" for r in receipt["reports"])
                       else "completed_with_report_deferrals"),
                finished_at_utc=datetime.now(timezone.utc).isoformat(),
                source_signatures=source_signatures(),
            )
        except releases.SourceCoordinationLockTimeout:
            receipt.update(state="deferred_source_lock", core_state="deferred", finished_at_utc=datetime.now(timezone.utc).isoformat())
        except subprocess.TimeoutExpired:
            receipt.update(
                state="deferred_source_lease_budget",
                core_state="publication_unknown" if receipt["publication_state"] == "outcome_unknown" else "deferred",
                finished_at_utc=datetime.now(timezone.utc).isoformat(),
            )
        except Exception as exc:
            if receipt["publication_state"] == "outcome_unknown":
                receipt["core_state"] = "publication_unknown"
            elif receipt["core_state"] in {"pending", "running"}:
                receipt["core_state"] = "failed"
            receipt.update(state="failed", finished_at_utc=datetime.now(timezone.utc).isoformat(), error=f"{type(exc).__name__}: {exc}")
            save()
            raise
        save()
        print(json.dumps(receipt, ensure_ascii=False), flush=True)


if __name__ == "__main__":
    main()

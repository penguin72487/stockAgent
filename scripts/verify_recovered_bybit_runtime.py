"""Read-only fixed-release recovery provenance and canonical Bybit increment.

The restored mutable root is already evolving. Verify every original path plus
bounded original hashes and real new hot-tail rows, without relabeling mutable
data as an unchanged release or repeating cold materialization.
"""
from __future__ import annotations

import argparse
from datetime import UTC, datetime
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import polars as pl
from downloader.artifact_io import atomic_write_json
from downloader.download_bybit_perp_daily import _frame_matches_1m_interval, _latest_ms_from_date_frame
from downloader.ohlcv_hot_tail import hot_tail_path, read_logical_parquet
from scripts.benchmark_finlab_effective_partitions import file_sha
from scripts.restore_missing_crypto_workspace import COLD_ROOT, LIVE_ROOT, link_identity
from scripts.verify_finlab_minute_admission_runtime import unit_identity
from stockagent.data.finlab_acquisition_contract import process_owner
from stockagent.data_sync.packed_snapshots import _load_inventory, _validate_inventory, resolve_packed_snapshot_id

UNIT = "stockagent-registered-data-intraday.service"


def source_identity(path: Path) -> tuple:
    stat = path.stat()
    return stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns


def native_bybit_owner(unit: dict) -> dict:
    for proc in Path("/proc").iterdir():
        if not proc.name.isdigit():
            continue
        try:
            if unit["ControlGroup"] not in (proc / "cgroup").read_text():
                continue
            command = [part.decode() for part in (proc / "cmdline").read_bytes().split(b"\0") if part]
            if len(command) < 2 or Path(command[1]).resolve() != ROOT / "downloader/download_bybit_perp_1m.py":
                continue
            owner = process_owner(int(proc.name))
            if owner and (proc / "exe").resolve() == Path(sys.executable).resolve():
                return {"owner": owner, "cmdline": command, "cwd": str((proc / "cwd").resolve())}
        except (OSError, UnicodeError):
            continue
    raise ValueError("formal Bybit increment owner unavailable")


def verify_increment(base: Path, *, finished_at: datetime, now: datetime) -> dict:
    tail = hot_tail_path(base)
    if not tail.is_file() or tail.stat().st_mtime <= finished_at.timestamp():
        raise ValueError("no fresh native hot-tail data")
    identities = {str(path): source_identity(path) for path in (base, tail)}
    frame = pl.read_parquet(tail)
    columns = ("open", "max", "min", "close", "Trading_Volume")
    if frame.is_empty() or any(name not in frame.columns for name in ("date", *columns)):
        raise ValueError("empty or incomplete native tail")
    checks = {"timestamps_unique": frame["date"].n_unique() == frame.height,
        "closed_minute_grid": _frame_matches_1m_interval(frame),
        "finite_values": not frame.select(pl.any_horizontal([
            pl.col(name).is_null() | ~pl.col(name).is_finite() for name in columns]).any()).item(),
        "ohlcv_valid": frame.select(((pl.col("max") >= pl.max_horizontal("open", "min", "close"))
            & (pl.col("min") <= pl.min_horizontal("open", "max", "close"))
            & (pl.col("min") > 0) & (pl.col("Trading_Volume") >= 0)).all()).item()}
    latest = _latest_ms_from_date_frame(frame)
    checks["completed_candles_only"] = latest is not None and latest <= int(now.timestamp()*1000)//60000*60000-60000
    logical = read_logical_parquet(base, columns=["date"])
    checks["logical_native_reader_sees_tail"] = _latest_ms_from_date_frame(logical) == latest
    if identities != {str(path): source_identity(path) for path in (base, tail)}:
        raise ValueError("source generation changed during bounded readback")
    if not all(checks.values()):
        raise ValueError(f"native tail readback failed: {checks}")
    return {"base": str(base.relative_to(ROOT)) if base.is_relative_to(ROOT) else str(base),
        "tail": str(tail), "tail_rows": frame.height, "logical_rows": logical.height,
        "latest_data_at_utc": datetime.fromtimestamp(latest/1000, UTC).isoformat(),
        "source_identities": identities, "checks": checks, "accepted": True}


def verify(receipt_path: Path, *, require_live_owner: bool = False) -> dict:
    receipt_sha = file_sha(receipt_path)
    receipt = json.loads(receipt_path.read_text())
    source = ROOT / "data_bybit"
    destination = LIVE_ROOT / "data_bybit"
    if (receipt.get("state") != "restored_mutable_workspace" or receipt.get("dataset") != "bybit"
            or receipt.get("full_canonical_recovery_verified") is not True
            or receipt.get("workspace_installed") is not True or receipt.get("provider_calls") != 0
            or receipt.get("cold_objects_modified") is not False
            or receipt.get("destination") != str(destination) or destination.is_symlink()
            or not destination.is_dir() or source.resolve() != destination
            or link_identity(source) != receipt["new_link"]):
        raise ValueError("matching full recovery receipt and real mutable source required")
    resolved = resolve_packed_snapshot_id(COLD_ROOT, "bybit", receipt["snapshot_id"])
    if resolved.manifest_sha256 != receipt["manifest_sha256"]:
        raise ValueError("fixed original manifest changed")
    entries = _load_inventory(COLD_ROOT, resolved.manifest)
    counts = _validate_inventory(resolved.manifest, entries)["counts"]
    if (counts["files"] != receipt["recovered_files"]
            or counts["logical_bytes"] != receipt["recovered_logical_bytes"]
            or resolved.manifest["source"] != receipt["source_inventory"]):
        raise ValueError("complete original scope differs from recovery receipt")
    missing = []
    originals = []
    for entry in entries:
        path = destination / entry["path"]
        if (entry["kind"] == "file" and (not path.is_file() or path.is_symlink())):
            missing.append(entry["path"])
        elif entry["kind"] == "directory" and not path.is_dir():
            missing.append(entry["path"])
        elif entry["kind"] == "file" and entry["path"].startswith("1m/") and "/_hot_tail/" not in entry["path"]:
            if path.stat().st_size == entry["size"]:
                originals.append(entry)
    if missing or len(originals) < 3:
        raise ValueError("original path scope is incomplete")
    unit = unit_identity(UNIT)
    owner = None
    if unit["ActiveState"] in ("active", "activating") and unit["ControlGroup"]:
        try:
            owner = native_bybit_owner(unit)
        except ValueError:
            pass  # A completed stage is not a live owner; keep that boundary.
    if require_live_owner and owner is None:
        raise ValueError("formal increment unit not live")
    unchanged = []
    for entry in sorted(originals, key=lambda row: row["size"])[:3]:
        path = destination / entry["path"]
        before = source_identity(path)
        digest = file_sha(path)
        if source_identity(path) != before or digest != entry["sha256"]:
            raise ValueError("bounded original source hash changed")
        unchanged.append({"path": entry["path"], "size": entry["size"], "sha256": digest, "accepted": True})
    finished = datetime.fromisoformat(receipt["finished_at_utc"])
    tails = [path for path in sorted((destination / "1m/_hot_tail").glob("*_features.parquet"))
             if path.stat().st_mtime > finished.timestamp()]
    if len(tails) < 3:
        raise ValueError("new source increments not yet available")
    now = datetime.now(UTC)
    increments = [verify_increment(destination / "1m" / path.name, finished_at=finished, now=now)
                  for path in tails[:3]]
    current = resolve_packed_snapshot_id(COLD_ROOT, "bybit", receipt["snapshot_id"])
    if (unit_identity(UNIT) != unit or (owner is not None and native_bybit_owner(unit) != owner)
            or current.manifest_sha256 != resolved.manifest_sha256 or file_sha(receipt_path) != receipt_sha
            or link_identity(source) != receipt["new_link"]):
        raise ValueError("native source/owner/fixed manifest changed during acceptance")
    return {"contract": "bybit_fixed_recovery_and_native_increment_readback_v1",
        "state": ("accepted_fixed_recovery_and_native_increment" if owner is not None else
                  "accepted_source_readback_native_owner_pending"),
        "observed_at_utc": datetime.now(UTC).isoformat(),
        "recovery_receipt_sha256": receipt_sha, "snapshot_id": resolved.manifest["snapshot_id"],
        "manifest_sha256": resolved.manifest_sha256, "source_inventory_counts": counts,
        "all_original_paths_present": True, "mutable_source": str(destination), "new_link": receipt["new_link"],
        "unit": unit, "native_owner": owner, "native_owner_accepted": owner is not None,
        "native_lifecycle_complete_claim": False, "original_sample_readbacks": unchanged,
        "native_increment_readbacks": increments, "provider_calls": 0, "source_writes": 0,
        "service_restarted_by_probe": False, "cold_objects_modified": False,
        "all_mutable_file_content_reverified": False, "all_cold_object_content_reverified": False,
        "all_symbols_latest_claim": False, "full_history_complete_claim": False}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--recovery", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--require-live-owner", action="store_true")
    args = parser.parse_args()
    output = args.output.absolute()
    if output.exists() or output.is_symlink() or not output.resolve().is_relative_to(ROOT / "artifacts/operations"):
        parser.error("fresh receipt under artifacts/operations required")
    result = verify(args.recovery, require_live_owner=args.require_live_owner)
    atomic_write_json(output, result, durable=True)
    print(json.dumps({key: result[key] for key in ("state", "all_original_paths_present", "provider_calls", "source_writes")}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

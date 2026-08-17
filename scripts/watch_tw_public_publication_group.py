#!/usr/bin/env python3
"""Refresh official Taiwan public-data groups at their release boundaries.

This is the low-latency, mutable download layer.  It deliberately does not
publish an inference snapshot: the independent 08:30 job performs the full
download, strict model-safety audit, immutable snapshot publication, and
atomic pin/symlink switch.

For every run we hash the selected parquet files before and after the refresh.
The receipt therefore distinguishes a successful HTTP sweep from an actually
observed content change and builds empirical publication-time evidence for
feeds whose publishers do not promise an exact release time.
"""

from __future__ import annotations

import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, time as datetime_time
import fcntl
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import uuid
from zoneinfo import ZoneInfo

import polars as pl


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from downloader.download_tw_public_data import (  # noqa: E402
    DEFAULT_DATASETS,
    _select_specs,
)


TAIPEI = ZoneInfo("Asia/Taipei")


@dataclass(frozen=True)
class PublicationPhase:
    name: str
    anchor: datetime_time
    selectors: tuple[str, ...]
    official_basis: str
    refresh_taiex_calendar: bool = False
    require_close_publication: bool = False


# These are probe boundaries, not invented publication timestamps for every
# selected endpoint.  Fixed times come from the named TWSE data products.  A
# phase may include related TPEx/TAIFEX feeds so their first observed change is
# measured under the same receipt.  Feeds without a fixed publisher SLA are
# swept in preopen_all and conclusively checked by the 08:30 strict job.
PUBLICATION_PHASES: dict[str, PublicationPhase] = {
    "preopen_all": PublicationPhase(
        name="preopen_all",
        anchor=datetime_time(7, 50),
        selectors=("all",),
        official_basis="TWSE financial key-data preopen boundary; full 156-dataset sweep",
    ),
    "close_initial": PublicationPhase(
        name="close_initial",
        anchor=datetime_time(14, 0),
        selectors=(
            "twse_daily_ohlcv",
            "tpex_daily_ohlcv",
            "twse_market_index",
            "twse_daily_valuation",
            "tpex_daily_valuation",
            "taifex_daily_futures",
            "taifex_daily_options",
        ),
        official_basis="TWSE daily closing-data initial boundary",
        refresh_taiex_calendar=True,
        require_close_publication=True,
    ),
    "institutional_initial": PublicationPhase(
        name="institutional_initial",
        anchor=datetime_time(14, 50),
        selectors=("institutional", "flow", "open_interest"),
        official_basis="TWSE institutional-statistics initial boundary",
    ),
    "close_revision": PublicationPhase(
        name="close_revision",
        anchor=datetime_time(15, 30),
        selectors=(
            "twse_daily_ohlcv",
            "tpex_daily_ohlcv",
            "twse_market_index",
            "twse_daily_valuation",
            "tpex_daily_valuation",
            "taifex_daily_futures",
            "taifex_daily_options",
        ),
        official_basis="TWSE daily closing-data revision boundary",
        refresh_taiex_calendar=True,
        require_close_publication=True,
    ),
    "close_final": PublicationPhase(
        name="close_final",
        anchor=datetime_time(17, 30),
        selectors=(
            "twse_daily_ohlcv",
            "tpex_daily_ohlcv",
            "twse_market_index",
            "twse_daily_valuation",
            "tpex_daily_valuation",
            "taifex_daily_futures",
            "taifex_daily_options",
        ),
        official_basis="TWSE daily closing-data final boundary",
        refresh_taiex_calendar=True,
        require_close_publication=True,
    ),
    "institutional_final": PublicationPhase(
        name="institutional_final",
        anchor=datetime_time(19, 40),
        selectors=("institutional", "flow", "open_interest"),
        official_basis="TWSE institutional-statistics final boundary",
    ),
    "margin": PublicationPhase(
        name="margin",
        anchor=datetime_time(21, 0),
        selectors=("margin", "shorting"),
        official_basis="TWSE margin-balance boundary",
    ),
    "security_master": PublicationPhase(
        name="security_master",
        anchor=datetime_time(22, 0),
        selectors=("universe", "lifecycle", "calendar", "index"),
        official_basis="TWSE next-session security-master boundary",
    ),
    "next_session_reference": PublicationPhase(
        name="next_session_reference",
        anchor=datetime_time(22, 32),
        selectors=("market_rule", "market_state", "calendar"),
        official_basis=(
            "post-22:30 next-session reference sweep; exact day-trade rules are "
            "handled by the dedicated two-second watcher"
        ),
    ),
    "corporate_actions": PublicationPhase(
        name="corporate_actions",
        anchor=datetime_time(23, 0),
        selectors=("corporate_action", "dividend", "event", "material"),
        official_basis="TWSE next-session listing/ex-right boundary",
    ),
    "price_limits": PublicationPhase(
        name="price_limits",
        anchor=datetime_time(23, 30),
        selectors=("market_rule", "market_state", "shorting", "calendar"),
        official_basis="TWSE next-session price-limit boundary",
    ),
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--phase",
        choices=("auto", *PUBLICATION_PHASES),
        default="auto",
    )
    parser.add_argument(
        "--live-root",
        type=Path,
        default=Path("/srv/stockagent-live/data_tw_public"),
    )
    parser.add_argument(
        "--receipt-root",
        type=Path,
        default=Path("artifacts/data_refresh/tw_public/publications"),
    )
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--date-workers", type=int, default=4)
    parser.add_argument("--timeout", type=int, default=20)
    parser.add_argument("--retries", type=int, default=2)
    parser.add_argument(
        "--auto-window-minutes",
        type=float,
        default=20.0,
        help="maximum distance from a scheduled boundary when --phase=auto",
    )
    return parser.parse_args()


def _atomic_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + f".tmp.{uuid.uuid4().hex}")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _semantic_snapshot_sha256(path: Path) -> str:
    """Hash source content while excluding the downloader observation clock.

    Snapshot parquet serialization is not a stable content identity, and the
    audit-only ``_downloaded_at_utc`` field necessarily changes on every
    request.  Two independent 64-bit row hashes plus commutative aggregates
    make the fingerprint insensitive to row order while retaining duplicate
    multiplicity.  This fingerprint is compared only inside one process/run,
    so a future Polars hash-version change cannot create a false publication.
    """

    schema = pl.read_parquet_schema(path)
    columns = [name for name in schema if name != "_downloaded_at_utc"]
    if not columns:
        return hashlib.sha256(b"empty-schema").hexdigest()
    hashes = pl.scan_parquet(path).select(
        pl.struct(columns)
        .hash(seed=0, seed_1=1, seed_2=2, seed_3=3)
        .alias("h1"),
        pl.struct(columns)
        .hash(seed=4, seed_1=5, seed_2=6, seed_3=7)
        .alias("h2"),
    )
    aggregate = hashes.select(
        pl.len().alias("rows"),
        pl.col("h1").sum().alias("h1_sum"),
        pl.col("h1").min().alias("h1_min"),
        pl.col("h1").max().alias("h1_max"),
        pl.col("h2").sum().alias("h2_sum"),
        pl.col("h2").min().alias("h2_min"),
        pl.col("h2").max().alias("h2_max"),
    ).collect(engine="streaming")
    payload = {
        "schema": [(name, str(schema[name])) for name in columns],
        "aggregate": aggregate.row(0, named=True),
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def _dataset_fingerprint(name: str, path: Path) -> str:
    spec = DEFAULT_DATASETS[name]
    if spec.kind in {"snapshot_url", "data_gov"}:
        return _semantic_snapshot_sha256(path)
    return _sha256(path)


def _file_hashes(live_root: Path, names: list[str]) -> dict[str, dict[str, object]]:
    paths = {
        name: live_root / f"{name}.parquet"
        for name in names
        if (live_root / f"{name}.parquet").is_file()
    }
    with ThreadPoolExecutor(max_workers=min(8, max(1, len(paths)))) as executor:
        hashes = dict(
            zip(
                paths,
                executor.map(
                    lambda item: _dataset_fingerprint(*item),
                    paths.items(),
                ),
                strict=True,
            )
        )
    return {
        name: {
            "sha256": hashes[name],
            "bytes": paths[name].stat().st_size,
            "fingerprint_kind": (
                "semantic_without_download_clock"
                if DEFAULT_DATASETS[name].kind in {"snapshot_url", "data_gov"}
                else "file_sha256"
            ),
        }
        for name in sorted(paths)
    }


def _minutes_since_midnight(value: datetime_time) -> float:
    return value.hour * 60.0 + value.minute + value.second / 60.0


def resolve_phase(observed: datetime, *, window_minutes: float) -> PublicationPhase:
    local = observed.astimezone(TAIPEI)
    current = _minutes_since_midnight(local.timetz().replace(tzinfo=None))
    distances = [
        (abs(current - _minutes_since_midnight(phase.anchor)), phase)
        for phase in PUBLICATION_PHASES.values()
    ]
    distance, phase = min(distances, key=lambda item: item[0])
    if distance > window_minutes:
        # A Persistent systemd timer may catch up a missed event at an arbitrary
        # boot time.  A full mutable sweep is the only honest fallback because
        # systemd does not pass the missed OnCalendar expression to the service.
        return PUBLICATION_PHASES["preopen_all"]
    return phase


def _taiex_calendar_command(*, live_root: Path, args: argparse.Namespace) -> list[str]:
    return [
        sys.executable,
        str(REPO_ROOT / "downloader" / "download_tw_taiex_ohlc.py"),
        "--mode",
        "daily",
        "--output-dir",
        str(live_root),
        "--end-date",
        "today",
        "--workers",
        "2",
        "--timeout",
        str(args.timeout),
        "--retries",
        str(args.retries),
        "--daily-overlap-days",
        "2",
        "--skip-raw",
    ]


def _download_command(
    *,
    live_root: Path,
    phase: PublicationPhase,
    args: argparse.Namespace,
) -> list[str]:
    command = [
        sys.executable,
        str(REPO_ROOT / "downloader" / "download_tw_public_data.py"),
        "--mode",
        "daily",
        "--datasets",
        *phase.selectors,
        "--end-date",
        "today",
        "--output-dir",
        str(live_root),
        "--workers",
        str(args.workers),
        "--date-workers",
        str(args.date_workers),
        "--timeout",
        str(args.timeout),
        "--retries",
        str(args.retries),
        "--daily-overlap-days",
        "2",
        "--allow-daily-publication-lag",
        "--require-taiex-session-calendar",
        "--no-progress",
        "--no-write-run-metadata",
    ]
    if phase.require_close_publication:
        command.append("--require-daily-close-publication")
    return command


def _changed_files(
    before: dict[str, dict[str, object]],
    after: dict[str, dict[str, object]],
) -> list[dict[str, object]]:
    changed: list[dict[str, object]] = []
    for name in sorted(set(before) | set(after)):
        old = before.get(name)
        new = after.get(name)
        if (
            old is not None
            and new is not None
            and old.get("sha256") == new.get("sha256")
            and old.get("fingerprint_kind") == new.get("fingerprint_kind")
        ):
            continue
        changed.append(
            {
                "dataset": name,
                "previous_sha256": old.get("sha256") if old else None,
                "sha256": new.get("sha256") if new else None,
                "previous_bytes": old.get("bytes") if old else None,
                "bytes": new.get("bytes") if new else None,
            }
        )
    return changed


def _write_receipts(
    receipt_root: Path,
    payload: dict[str, object],
    *,
    phase: PublicationPhase,
    started: datetime,
) -> None:
    phase_root = receipt_root / phase.name
    _atomic_json(phase_root / "latest.json", payload)
    run_name = started.astimezone(TAIPEI).strftime("%Y%m%dT%H%M%S%f") + ".json"
    _atomic_json(phase_root / "runs" / run_name, payload)


def main() -> int:
    args = parse_args()
    if args.workers <= 0 or args.date_workers <= 0:
        raise ValueError("worker counts must be positive")
    if args.timeout <= 0 or args.retries < 0:
        raise ValueError("timeout must be positive and retries non-negative")
    if args.auto_window_minutes <= 0:
        raise ValueError("--auto-window-minutes must be positive")

    started = datetime.now(TAIPEI)
    phase = (
        resolve_phase(started, window_minutes=float(args.auto_window_minutes))
        if args.phase == "auto"
        else PUBLICATION_PHASES[args.phase]
    )
    live_root = args.live_root.expanduser().resolve(strict=True)
    receipt_root = (
        args.receipt_root
        if args.receipt_root.is_absolute()
        else REPO_ROOT / args.receipt_root
    ).resolve(strict=False)
    specs = _select_specs(list(phase.selectors))
    selected_names = sorted(spec.name for spec in specs)
    sources = dict(sorted(Counter(spec.source for spec in specs).items()))
    lock_path = live_root.parent / ".locks" / "tw-public-refresh.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    lock_started = time.perf_counter()
    commands: list[list[str]] = []
    return_codes: list[int] = []
    with lock_path.open("a+", encoding="utf-8") as lock_handle:
        fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX)
        lock_wait_ms = (time.perf_counter() - lock_started) * 1000.0
        lock_acquired = datetime.now(TAIPEI)
        before = _file_hashes(live_root, selected_names)
        if phase.refresh_taiex_calendar:
            commands.append(_taiex_calendar_command(live_root=live_root, args=args))
        commands.append(_download_command(live_root=live_root, phase=phase, args=args))
        for command in commands:
            completed = subprocess.run(command, cwd=REPO_ROOT, check=False)
            return_codes.append(int(completed.returncode))
            if completed.returncode != 0:
                break
        after = _file_hashes(live_root, selected_names)

    completed_at = datetime.now(TAIPEI)
    changed = _changed_files(before, after)
    status = "ok" if all(code == 0 for code in return_codes) else "failed"
    payload: dict[str, object] = {
        "schema_version": 1,
        "status": status,
        "phase": phase.name,
        "official_basis": phase.official_basis,
        "scheduled_boundary": phase.anchor.isoformat(),
        "started_at_taipei": started.isoformat(),
        "lock_acquired_at_taipei": lock_acquired.isoformat(),
        "completed_at_taipei": completed_at.isoformat(),
        "lock_wait_ms": lock_wait_ms,
        "selected_dataset_count": len(selected_names),
        "selected_datasets": selected_names,
        "source_counts": sources,
        "changed_dataset_count": len(changed),
        "changed_datasets": changed,
        "content_change_observed": bool(changed),
        "commands": commands,
        "return_codes": return_codes,
        "live_root": str(live_root),
        "strict_publication_deferred_to_0830": True,
    }
    _write_receipts(receipt_root, payload, phase=phase, started=started)
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
    return 0 if status == "ok" else 1


if __name__ == "__main__":
    raise SystemExit(main())

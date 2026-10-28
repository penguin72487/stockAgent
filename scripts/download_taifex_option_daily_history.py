#!/usr/bin/env python3
"""Download official TAIFEX daily options and build ATM plus full-chain data.

Completed calendar years use the official annual ZIP archive.  The current
year uses one-month daily requests because the TAIFEX endpoint limits each
query to a month.  Raw receipts are immutable and SHA-256 recorded before the
normalized research dataset is rebuilt.
"""

from __future__ import annotations

import argparse
from collections import Counter
from datetime import date, datetime, timedelta, timezone
import fcntl
from functools import partial
import hashlib
import json
from pathlib import Path
import stat
import sys
import time
from typing import Final, Mapping

import numpy as np

REPO_ROOT = Path(__file__).resolve().parents[1]
FULL_CHAIN_SHARD_CACHE = REPO_ROOT / "artifacts/cache/taifex_option_full_chain_shards"
ATM_SOURCE_CACHE = REPO_ROOT / "artifacts/cache/taifex_option_atm_sources"
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.taifex_daily_download_common import (  # noqa: E402
    atomic_write_json,
    download_taifex_attachment,
    month_ranges,
    parse_iso_date,
    sha256_path,
    validate_taifex_receipt,
)
from stockagent.data.tw_index_options_daily import (  # noqa: E402
    TAIFEX_OPTION_SERIES_SCOPES,
    TAIFEX_OPTIONS_DAILY_DATA_CONTRACT_VERSION,
    TAIFEX_OPTIONS_FULL_CHAIN_DATA_CONTRACT_VERSION,
    TAIFEX_OPTIONS_DAILY_PRICE_SOURCE,
    AtmSourceProjection,
    TaifexTxOpeningReference,
    build_taifex_option_full_chain,
    build_taifex_opening_atm_straddles,
    load_taifex_opening_atm_straddles,
    load_taifex_tx_opening_reference,
    project_taifex_atm_source,
)
from stockagent.data_sync.acquisition_capacity_sampler import (  # noqa: E402
    AcquisitionCapacitySampler,
)
from scripts.check_acquisition_capacity import priority_dispatch_status  # noqa: E402


TAIFEX_OPTION_DAILY_PAGE: Final[str] = (
    "https://www.taifex.com.tw/cht/3/optDailyMarketView"
)
TAIFEX_OPTION_DOWNLOAD_URL: Final[str] = (
    "https://www.taifex.com.tw/cht/3/optDataDown"
)
# Same extra ledger headroom used by the existing finite FinLab priority
# dispatcher, plus the maximum bytes of this single public-source response.
FINITE_PRIORITY_HEADROOM_BYTES: Final[int] = 24 * 1024**3
FINITE_PRIORITY_RESPONSE_BYTES: Final[int] = 64 * 1024**2


def _builder_fingerprint() -> dict[str, str]:
    """Invalidate output reuse when a direct normalization dependency changes."""

    relative_paths = (
        "scripts/download_taifex_option_daily_history.py",
        "scripts/taifex_daily_download_common.py",
        "stockagent/data/tw_index_options_daily.py",
        "stockagent/data/tw_index_futures.py",
        "stockagent/data/tw_index_derivatives_tick.py",
        "stockagent/data/tw_price_rules.py",
    )
    return {name: sha256_path(REPO_ROOT / name) for name in relative_paths}


def _file_signature(path: Path) -> tuple[int, int, int, int, int]:
    """Detect a source replacement or write after its full SHA-256 was read."""

    info = path.stat(follow_symlinks=False)
    if not stat.S_ISREG(info.st_mode):
        raise ValueError(f"TAIFEX source is not a regular file: {path}")
    return (
        info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns
    )


def _hashed_source_receipt(path: Path) -> tuple[dict[str, object], tuple[int, ...]]:
    before = _file_signature(path)
    digest = sha256_path(path)
    if _file_signature(path) != before:
        raise ValueError(f"TAIFEX source changed while hashing: {path}")
    return {"path": str(path), "bytes": before[2], "sha256": digest}, before


def _require_unchanged_inputs(
    signatures: dict[Path, tuple[int, ...]],
    builder_fingerprint: dict[str, str],
) -> None:
    for path, expected in signatures.items():
        if _file_signature(path) != expected:
            raise ValueError(f"TAIFEX source changed during normalization: {path}")
    if _builder_fingerprint() != builder_fingerprint:
        raise ValueError("TAIFEX normalizer code changed during normalization")


def _can_reuse_normalized(
    manifest_path: Path,
    *,
    dataset: str,
    scope: str,
    start_year: int,
    end_date: date,
    futures_path: Path,
    futures_sha256: str,
    normalized: Path,
    full_chain: Path,
    receipts: list[dict[str, object]],
    builder_fingerprint: dict[str, str],
) -> bool:
    """Reuse only byte-verified output built from identical source and code."""

    try:
        previous = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (FileNotFoundError, OSError, ValueError):
        return False
    if not isinstance(previous, dict):
        return False
    expected = {
        "dataset": dataset,
        "contract_version": TAIFEX_OPTIONS_DAILY_DATA_CONTRACT_VERSION,
        "status": "complete",
        "start_year": start_year,
        "end_date": end_date.isoformat(),
        "futures_path": str(futures_path),
        "futures_sha256": futures_sha256,
        "normalized_path": str(normalized),
        "full_chain_path": str(full_chain),
        "receipts": receipts,
        "builder_fingerprint": builder_fingerprint,
        "series_scope": (
            "nearest_unexpired_monthly_only"
            if scope == "monthly"
            else "nearest_expiry_weekly_only"
        ),
    }
    if any(previous.get(key) != value for key, value in expected.items()):
        return False
    if not isinstance(previous.get("quality"), dict) or not isinstance(
        previous.get("full_chain_quality"), dict
    ):
        return False
    try:
        return (
            sha256_path(normalized) == previous.get("normalized_sha256")
            and sha256_path(full_chain) == previous.get("full_chain_sha256")
        )
    except (FileNotFoundError, OSError):
        return False


def _atm_futures_fingerprint(
    source_dates: list[str], tx_by_date: Mapping[date, tuple[str, float]]
) -> str:
    values = [
        (day, None if (payload := tx_by_date.get(date.fromisoformat(day))) is None
         else (payload[0], payload[1].hex()))
        for day in source_dates
    ]
    return hashlib.sha256(
        json.dumps(values, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def _atm_projection_payload(projection: AtmSourceProjection) -> dict[str, object]:
    return {
        "source_dates": [day.isoformat() for day in sorted(projection.all_txo_dates)],
        "rows": [
            {**projection.selected[day], "date": day.isoformat()}
            for day in sorted(projection.selected)
        ],
    }


def _atm_payload_digest(payload: dict[str, object]) -> str:
    encoded = json.dumps(
        payload, sort_keys=True, separators=(",", ":"), allow_nan=False
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _prepare_atm_source_projections(
    receipt_manifest: list[dict[str, object]],
    cache_root: Path,
    *,
    scope: str,
    tx_by_date: Mapping[date, tuple[str, float]],
    builder_fingerprint: dict[str, str],
) -> tuple[dict[Path, AtmSourceProjection], int]:
    if cache_root.is_symlink() or (cache_root / scope).is_symlink():
        raise ValueError(f"ATM source cache is a symlink: {cache_root}")
    projections: dict[Path, AtmSourceProjection] = {}
    rebuilt = 0
    for source in receipt_manifest:
        source_path = Path(str(source["path"])).resolve()
        identifier = hashlib.sha256(str(source_path).encode()).hexdigest()[:24]
        cache_path = cache_root / scope / f"{identifier}.json"
        cached: AtmSourceProjection | None = None
        if not cache_path.is_symlink():
            try:
                record = json.loads(cache_path.read_text(encoding="utf-8"))
                payload = record["projection"]
                source_dates = payload["source_dates"]
                rows = payload["rows"]
                if (
                    not isinstance(source_dates, list)
                    or not isinstance(rows, list)
                    or source_dates != sorted(set(source_dates))
                    or any(not isinstance(day, str) for day in source_dates)
                ):
                    raise ValueError("invalid ATM source dates")
                expected = {
                    "contract_version": TAIFEX_OPTIONS_DAILY_DATA_CONTRACT_VERSION,
                    "scope": scope,
                    "source_path": str(source_path),
                    "source_sha256": source["sha256"],
                    "builder_fingerprint": builder_fingerprint,
                    "futures_fingerprint": _atm_futures_fingerprint(
                        source_dates, tx_by_date
                    ),
                    "projection_sha256": _atm_payload_digest(payload),
                }
                if any(record.get(key) != value for key, value in expected.items()):
                    raise ValueError("ATM source cache receipt mismatch")
                selected = {
                    date.fromisoformat(row["date"]): {
                        **row, "date": date.fromisoformat(row["date"])
                    }
                    for row in rows
                }
                if len(selected) != len(rows) or not set(selected) <= {
                    date.fromisoformat(day) for day in source_dates
                }:
                    raise ValueError("invalid ATM projection rows")
                cached = AtmSourceProjection(
                    selected, {date.fromisoformat(day) for day in source_dates}
                )
            except (FileNotFoundError, OSError, ValueError, TypeError, KeyError):
                pass
        if cached is None:
            cached = project_taifex_atm_source(
                source_path, series_scope=scope, tx_by_date=tx_by_date
            )
            payload = _atm_projection_payload(cached)
            record = {
                "contract_version": TAIFEX_OPTIONS_DAILY_DATA_CONTRACT_VERSION,
                "scope": scope,
                "source_path": str(source_path),
                "source_sha256": source["sha256"],
                "builder_fingerprint": builder_fingerprint,
                "futures_fingerprint": _atm_futures_fingerprint(
                    payload["source_dates"], tx_by_date
                ),
                "projection_sha256": _atm_payload_digest(payload),
                "projection": payload,
            }
            atomic_write_json(cache_path, record)
            rebuilt += 1
        projections[source_path] = cached
    return projections, rebuilt


def _futures_open_fingerprint(
    source_dates: list[str], tx_open_by_date: Mapping[date, float]
) -> str:
    """Fingerprint the exact futures inputs used by one source receipt."""

    values = [
        (day, None if (value := tx_open_by_date.get(date.fromisoformat(day))) is None
         else value.hex())
        for day in source_dates
    ]
    return hashlib.sha256(
        json.dumps(values, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def _full_chain_shard_paths(
    cache_root: Path, *, scope: str, source_path: Path
) -> tuple[Path, Path]:
    identifier = hashlib.sha256(str(source_path).encode("utf-8")).hexdigest()[:24]
    shard = cache_root / scope / f"{identifier}.parquet"
    return shard, shard.with_suffix(".receipt.json")


def _load_full_chain_shard(
    shard: Path,
    receipt_path: Path,
    *,
    scope: str,
    source_path: Path,
    source_sha256: str,
    tx_open_by_date: Mapping[date, float],
    builder_fingerprint: dict[str, str],
) -> dict[str, object] | None:
    if shard.is_symlink() or receipt_path.is_symlink():
        return None
    try:
        receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
        if not isinstance(receipt, dict):
            return None
        source_dates = receipt.get("source_dates")
        if (
            not isinstance(source_dates, list)
            or any(not isinstance(day, str) for day in source_dates)
            or source_dates != sorted(set(source_dates))
        ):
            return None
        expected = {
            "contract_version": TAIFEX_OPTIONS_FULL_CHAIN_DATA_CONTRACT_VERSION,
            "status": "complete",
            "scope": scope,
            "source_path": str(source_path),
            "source_sha256": source_sha256,
            "shard_path": str(shard),
            "builder_fingerprint": builder_fingerprint,
            "futures_open_fingerprint": _futures_open_fingerprint(
                source_dates, tx_open_by_date
            ),
        }
        if any(receipt.get(key) != value for key, value in expected.items()):
            return None
        if sha256_path(shard) != receipt.get("shard_sha256"):
            return None
        return receipt
    except (FileNotFoundError, OSError, ValueError, TypeError):
        return None


def _prepare_full_chain_shards(
    receipt_manifest: list[dict[str, object]],
    futures_reference: TaifexTxOpeningReference,
    cache_root: Path,
    *,
    scope: str,
    builder_fingerprint: dict[str, str],
) -> tuple[list[dict[str, object]], int]:
    futures_path = futures_reference.source_path
    futures_reference.require_unchanged(futures_path)
    tx_open_by_date = futures_reference.open_by_date
    shards: list[dict[str, object]] = []
    built = 0
    for source in receipt_manifest:
        source_path = Path(str(source["path"]))
        source_sha256 = str(source["sha256"])
        shard, receipt_path = _full_chain_shard_paths(
            cache_root, scope=scope, source_path=source_path
        )
        if cache_root.is_symlink() or shard.parent.is_symlink():
            raise ValueError(f"full-chain shard cache is a symlink: {cache_root}")
        previous = _load_full_chain_shard(
            shard, receipt_path, scope=scope, source_path=source_path,
            source_sha256=source_sha256, tx_open_by_date=tx_open_by_date,
            builder_fingerprint=builder_fingerprint,
        )
        if previous is not None:
            shards.append(previous)
            continue
        observed_dates: set[date] = set()
        build_taifex_option_full_chain(
            [source_path], futures_path, shard,
            series_scope=scope, source_dates=observed_dates, allow_empty=True,
            futures_reference=futures_reference,
        )
        if sha256_path(source_path) != source_sha256:
            raise ValueError(f"TAIFEX source changed while building {source_path}")
        source_dates = [day.isoformat() for day in sorted(observed_dates)]
        receipt: dict[str, object] = {
            "contract_version": TAIFEX_OPTIONS_FULL_CHAIN_DATA_CONTRACT_VERSION,
            "status": "complete",
            "scope": scope,
            "source_path": str(source_path),
            "source_sha256": source_sha256,
            "shard_path": str(shard),
            "shard_sha256": sha256_path(shard),
            "source_dates": source_dates,
            "futures_open_fingerprint": _futures_open_fingerprint(
                source_dates, tx_open_by_date
            ),
            "builder_fingerprint": builder_fingerprint,
        }
        futures_reference.require_unchanged(futures_path)
        atomic_write_json(receipt_path, receipt)
        shards.append(receipt)
        built += 1
    futures_reference.require_unchanged(futures_path)
    return shards, built


def _merge_full_chain_shards(
    shards: list[dict[str, object]], output_path: Path, *, scope: str,
    futures_reference: TaifexTxOpeningReference,
) -> Path:
    import pyarrow.parquet as pq

    futures_reference.require_unchanged(futures_reference.source_path)
    seen_dates: set[str] = set()
    for receipt in shards:
        if receipt.get("futures_open_fingerprint") != _futures_open_fingerprint(
            receipt["source_dates"], futures_reference.open_by_date
        ):
            raise ValueError("full-chain shard futures reference mismatch")
        dates = set(receipt["source_dates"])
        if overlap := seen_dates & dates:
            raise ValueError(f"overlapping full-chain receipts contain {min(overlap)}")
        seen_dates.update(dates)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = output_path.with_suffix(output_path.suffix + ".tmp")
    writer = None
    total_rows = 0
    try:
        for receipt in shards:
            shard = Path(str(receipt["shard_path"]))
            if sha256_path(shard) != receipt["shard_sha256"]:
                raise ValueError(f"full-chain shard changed before merge: {shard}")
            table = pq.read_table(shard)
            metadata = table.schema.metadata or {}
            if (
                metadata.get(b"stockagent.series_scope") != scope.encode("ascii")
                or int(metadata.get(b"stockagent.contract_version", b"-1"))
                != TAIFEX_OPTIONS_FULL_CHAIN_DATA_CONTRACT_VERSION
            ):
                raise ValueError(f"full-chain shard schema mismatch: {shard}")
            if writer is None:
                writer = pq.ParquetWriter(
                    temporary, table.schema, compression="zstd"
                )
            elif table.schema != writer.schema:
                raise ValueError(f"full-chain shard schema drift: {shard}")
            if table.num_rows:
                writer.write_table(table)
                total_rows += table.num_rows
            if sha256_path(shard) != receipt["shard_sha256"]:
                raise ValueError(f"full-chain shard changed during merge: {shard}")
        if total_rows == 0:
            raise ValueError(f"no normalized TXO {scope} full-chain rows were found")
    except Exception:
        if writer is not None:
            writer.close()
        temporary.unlink(missing_ok=True)
        raise
    if writer is None:
        raise ValueError("no full-chain shard schema")
    writer.close()
    try:
        futures_reference.require_unchanged(futures_reference.source_path)
    except (ValueError, OSError):
        temporary.unlink(missing_ok=True)
        raise
    temporary.replace(output_path)
    return output_path


def _parse_date(value: str) -> date:
    try:
        return parse_iso_date(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"expected ISO date YYYY-MM-DD, got {value!r}"
        ) from exc


def _download(
    payload: dict[str, str],
    target: Path,
    *,
    attempts: int,
    request_interval: float,
    max_response_bytes: int | None = None,
) -> Path:
    return download_taifex_attachment(
        TAIFEX_OPTION_DOWNLOAD_URL,
        payload,
        target,
        attempts=attempts,
        request_interval=request_interval,
        user_agent="stockAgent/taifex-option-daily-research",
        cooldown_after_download=True,
        max_response_bytes=max_response_bytes,
    )


def _download_all_products_raw(
    output_dir: Path,
    *,
    start_date: date,
    end_date: date,
    attempts: int,
    request_interval: float,
    finite_priority: bool = False,
    finite_priority_monthly: bool = False,
) -> int:
    """Acquire all option products as immutable daily-source ranges.

    These sources are daily market records, not intraday fills or a normalized
    options training dataset. The auxiliary lock is shared with the existing
    registered official downloader; the capacity gate precedes every request.
    """
    if start_date.year != end_date.year or start_date > end_date:
        raise ValueError("all-products range must be ordered within one calendar year")
    if finite_priority_monthly and not finite_priority:
        raise ValueError("monthly priority requires finite priority admission")
    if finite_priority and not finite_priority_monthly and start_date != end_date:
        raise ValueError("finite priority is restricted to one exact calendar day")
    state_dir = output_dir / "state"
    state_dir.mkdir(parents=True, exist_ok=True)
    lock_path = state_dir / "registered_auxiliary_daily.lock"
    with lock_path.open("a") as lock:
        try:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            print("[taifex-option-all] shared auxiliary downloader is active", flush=True)
            return 75
        if finite_priority:
            sampler = AcquisitionCapacitySampler(
                output_dir,
                probe=partial(
                    priority_dispatch_status,
                    additional_headroom_bytes=(FINITE_PRIORITY_HEADROOM_BYTES
                                               + FINITE_PRIORITY_RESPONSE_BYTES),
                ),
            )
        else:
            sampler = AcquisitionCapacitySampler(output_dir)
        admitted = {"admitted"} if finite_priority else {"admitted", "not_enrolled"}
        # Priority admission may need both the ordinary and priority physical
        # probes. Wait for the same in-flight sample, but never accept it past
        # the sampler's unchanged 30-second freshness limit.
        capacity_timeout = 29 if finite_priority else 20
        for range_start, range_end in month_ranges(start_date, end_date):
            target = output_dir / "raw" / "all_products" / "ranges" / (
                f"{range_start.isoformat()}_{range_end.isoformat()}_all.csv"
            )
            intent_path = target.with_suffix(".intent.json")
            receipt_path = target.with_suffix(".receipt.json")
            payload = {
                "down_type": "1",
                "queryStartDate": range_start.strftime("%Y/%m/%d"),
                "queryEndDate": range_end.strftime("%Y/%m/%d"),
                "commodity_id": "all",
                "commodity_id2": "",
            }
            identity = {
                "contract": "taifex_option_all_products_daily_raw_v1",
                "url": TAIFEX_OPTION_DOWNLOAD_URL,
                "request": payload,
                "range_start": range_start.isoformat(),
                "range_end": range_end.isoformat(),
                "point_in_time_snapshot": False,
            }
            if any(path.is_symlink() for path in (target, intent_path, receipt_path)):
                raise ValueError(f"all-products source path is a symlink: {target}")
            if receipt_path.exists():
                receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
                if any(receipt.get(key) != value for key, value in identity.items()):
                    raise ValueError(f"all-products receipt identity mismatch: {receipt_path}")
                validate_taifex_receipt(target)
                if receipt.get("bytes") != target.stat().st_size or receipt.get("sha256") != sha256_path(target):
                    raise ValueError(f"all-products receipt digest mismatch: {target}")
                print(f"[taifex-option-all] verified existing {target}", flush=True)
                continue
            if intent_path.exists():
                intent = json.loads(intent_path.read_text(encoding="utf-8"))
                if intent != identity:
                    raise ValueError(f"all-products request identity mismatch: {intent_path}")
            elif target.exists():
                raise ValueError(f"all-products source without request provenance: {target}")
            else:
                capacity = sampler.initial_sample(timeout_seconds=capacity_timeout)
                if capacity.get("state") not in admitted:
                    print(f"[taifex-option-all] capacity deferred: {capacity}", flush=True)
                    return 75
                atomic_write_json(intent_path, identity)
            if not target.exists():
                capacity = sampler.initial_sample(timeout_seconds=capacity_timeout)
                if capacity.get("state") not in admitted:
                    print(f"[taifex-option-all] capacity deferred: {capacity}", flush=True)
                    return 75
                _download(payload, target, attempts=attempts, request_interval=request_interval,
                          max_response_bytes=(FINITE_PRIORITY_RESPONSE_BYTES
                                              if finite_priority else None))
            if finite_priority and target.stat().st_size > FINITE_PRIORITY_RESPONSE_BYTES:
                raise ValueError("existing finite-priority source exceeds its response bound")
            validate_taifex_receipt(target)
            source, signature = _hashed_source_receipt(target)
            receipt = {
                **identity,
                "path": str(target),
                "bytes": source["bytes"],
                "sha256": source["sha256"],
                "verified_at_utc": datetime.now(timezone.utc).isoformat(),
            }
            if _file_signature(target) != signature:
                raise ValueError(f"all-products source changed during receipt creation: {target}")
            atomic_write_json(receipt_path, receipt)
            print(f"[taifex-option-all] acquired {target} ({source['bytes']:,} bytes)", flush=True)
    return 0


def _quality_summary(normalized: Path, *, series_scope: str) -> dict[str, object]:
    table = load_taifex_opening_atm_straddles(
        normalized,
        expected_series_scope=series_scope,
    )
    frame = table.select(
        [
            "date",
            "executable",
            "exclusion_reason",
            "option_series",
            "strike",
        ]
    ).to_pandas()
    dates = frame["date"].astype(str)
    duplicate_dates = int(dates.duplicated().sum())
    reasons: Counter[str] = Counter()
    for raw in frame.loc[~frame["executable"], "exclusion_reason"].dropna():
        reasons.update(str(raw).split("|"))
    executable = int(frame["executable"].sum())
    rows = int(len(frame))
    return {
        "rows": rows,
        "first_date": dates.min() if rows else None,
        "last_date": dates.max() if rows else None,
        "duplicate_dates": duplicate_dates,
        "executable_rows": executable,
        "excluded_rows": rows - executable,
        "executable_share": executable / rows if rows else 0.0,
        "exclusion_reason_counts": dict(sorted(reasons.items())),
        "series_scope": series_scope,
        "series_identity_valid": bool(
            frame["option_series"]
            .dropna()
            .astype(str)
            .str.fullmatch(
                r"\d{6}" if series_scope == "monthly" else r"\d{6}[WF][1-5]"
            )
            .all()
        ),
        "selected_strike_rows": int(frame["strike"].notna().sum()),
    }


def _full_chain_quality_summary(normalized: Path) -> dict[str, object]:
    import pyarrow.parquet as pq

    table = pq.read_table(
        normalized,
        columns=["date", "option_slot", "executable"],
    )
    dates = np.asarray(table.column("date").to_numpy(), dtype="datetime64[D]")
    slots = np.asarray(table.column("option_slot").to_numpy(), dtype=np.int32)
    executable = np.asarray(table.column("executable").to_numpy(), dtype=bool)
    if dates.size:
        _, daily_counts = np.unique(dates[executable], return_counts=True)
    else:
        daily_counts = np.empty(0, dtype=np.int64)
    return {
        "rows": int(dates.size),
        "first_date": str(dates.min()) if dates.size else None,
        "last_date": str(dates.max()) if dates.size else None,
        "executable_rows": int(executable.sum()),
        "distinct_slots": int(np.unique(slots).size),
        "maximum_executable_legs_per_day": int(daily_counts.max()) if daily_counts.size else 0,
    }


def main() -> int:
    run_started = time.perf_counter()
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--output-dir",
        default="data_tw_index_options_daily",
        help="Raw receipts and normalized parquet root.",
    )
    parser.add_argument(
        "--futures-path",
        default="data_tw_index_futures/day_session_contracts.parquet",
        help="Official normalized front-month TX day-session parquet.",
    )
    parser.add_argument("--start-year", type=int, default=2001)
    parser.add_argument(
        "--series-scope",
        choices=(*TAIFEX_OPTION_SERIES_SCOPES, "all"),
        default="monthly",
        help=(
            "Monthly series, nearest-expiry weekly series, or both from one "
            "shared immutable receipt download."
        ),
    )
    parser.add_argument(
        "--end-date",
        type=_parse_date,
        default=date.today() - timedelta(days=1),
        help="Last completed candidate session (default: yesterday).",
    )
    parser.add_argument("--request-interval", type=float, default=1.0)
    parser.add_argument("--attempts", type=int, default=3)
    parser.add_argument(
        "--all-products-raw-only", action="store_true",
        help="Acquire all products' daily source ranges without building a TXO dataset.",
    )
    parser.add_argument(
        "--raw-start-date", type=_parse_date,
        help="Start date for --all-products-raw-only (default: January 1 of end year).",
    )
    priority = parser.add_mutually_exclusive_group()
    priority.add_argument(
        "--finite-priority-one-day", action="store_true",
        help="For one exact all-products day, use the bounded priority reserve and response guard.",
    )
    priority.add_argument(
        "--finite-priority-year-monthly", action="store_true",
        help="For one calendar year, use bounded priority for each exact monthly request.",
    )
    args = parser.parse_args()

    if args.start_year < 2001:
        parser.error("--start-year cannot precede the listed TAIFEX option archive (2001)")
    if args.end_date.year < args.start_year:
        parser.error("--end-date precedes --start-year")
    if args.request_interval < 0.0:
        parser.error("--request-interval must be non-negative")
    if args.attempts < 1:
        parser.error("--attempts must be positive")

    output_dir = Path(args.output_dir).expanduser().resolve()
    if args.all_products_raw_only:
        raw_start = args.raw_start_date or date(args.end_date.year, 1, 1)
        if raw_start.year != args.end_date.year or raw_start > args.end_date:
            parser.error("--raw-start-date must be no later than --end-date in the same year")
        if args.finite_priority_one_day and raw_start != args.end_date:
            parser.error("--finite-priority-one-day requires exactly one date")
        return _download_all_products_raw(
            output_dir, start_date=raw_start, end_date=args.end_date,
            attempts=args.attempts, request_interval=args.request_interval,
            finite_priority=args.finite_priority_one_day or args.finite_priority_year_monthly,
            finite_priority_monthly=args.finite_priority_year_monthly,
        )
    if args.finite_priority_one_day or args.finite_priority_year_monthly:
        parser.error("finite priority requires --all-products-raw-only")
    if args.raw_start_date is not None:
        parser.error("--raw-start-date requires --all-products-raw-only")
    if any(
        cache_root.resolve().is_relative_to(output_dir)
        for cache_root in (FULL_CHAIN_SHARD_CACHE, ATM_SOURCE_CACHE)
    ):
        parser.error("local projection cache must not be inside the published source")
    futures_path = Path(args.futures_path).expanduser().resolve()
    if not futures_path.is_file():
        parser.error(f"futures parquet does not exist: {futures_path}")
    raw_dir = output_dir / "raw"
    receipts: list[Path] = []
    receipt_started = time.perf_counter()

    for year in range(args.start_year, args.end_date.year):
        target = raw_dir / "annual" / f"{year}_opt.zip"
        path = _download(
            {"down_type": "2", "his_year": str(year)},
            target,
            attempts=args.attempts,
            request_interval=args.request_interval,
        )
        validate_taifex_receipt(path)
        receipts.append(path)
        print(f"verified annual option receipt {year}: {path.stat().st_size:,} bytes", flush=True)

    current_start = date(args.end_date.year, 1, 1)
    for range_start, range_end in month_ranges(current_start, args.end_date):
        target = raw_dir / "ranges" / (
            f"{range_start.isoformat()}_{range_end.isoformat()}_TXO.csv"
        )
        path = _download(
            {
                "down_type": "1",
                "queryStartDate": range_start.strftime("%Y/%m/%d"),
                "queryEndDate": range_end.strftime("%Y/%m/%d"),
                "commodity_id": "TXO",
                "commodity_id2": "",
            },
            target,
            attempts=args.attempts,
            request_interval=args.request_interval,
        )
        validate_taifex_receipt(path)
        receipts.append(path)
        print(
            f"verified option receipt {range_start}..{range_end}: "
            f"{path.stat().st_size:,} bytes",
            flush=True,
        )

    receipt_seconds = time.perf_counter() - receipt_started
    print(
        f"[taifex-option-daily-timing] stage=receipts "
        f"seconds={receipt_seconds:.3f} count={len(receipts)}",
        flush=True,
    )

    output_names = {
        "monthly": "monthly_opening_atm_pairs.parquet",
        "weekly": "weekly_nearest_expiry_opening_atm_pairs.parquet",
    }
    full_chain_output_names = {
        "monthly": "monthly_full_chain.parquet",
        "weekly": "weekly_full_chain.parquet",
    }
    dataset_names = {
        "monthly": "taifex_monthly_opening_atm_straddles",
        "weekly": "taifex_nearest_expiry_weekly_opening_atm_straddles",
    }
    selected_scopes = (
        TAIFEX_OPTION_SERIES_SCOPES
        if args.series_scope == "all"
        else (args.series_scope,)
    )
    manifest_started = time.perf_counter()
    receipt_manifest: list[dict[str, object]] = []
    input_signatures: dict[Path, tuple[int, ...]] = {}
    for path in receipts:
        source_receipt, signature = _hashed_source_receipt(path)
        receipt_manifest.append(source_receipt)
        input_signatures[path] = signature
    print(
        f"[taifex-option-daily-timing] stage=receipt_manifest "
        f"seconds={time.perf_counter() - manifest_started:.3f}",
        flush=True,
    )
    futures_receipt, input_signatures[futures_path] = _hashed_source_receipt(
        futures_path
    )
    futures_sha256 = str(futures_receipt["sha256"])
    builder_fingerprint = _builder_fingerprint()
    futures_reference: TaifexTxOpeningReference | None = None
    for series_scope in selected_scopes:
        normalized = output_dir / output_names[series_scope]
        full_chain = output_dir / full_chain_output_names[series_scope]
        manifest_name = (
            "manifest.json" if series_scope == "monthly" else "manifest_weekly.json"
        )
        if _can_reuse_normalized(
            output_dir / manifest_name,
            dataset=dataset_names[series_scope],
            scope=series_scope,
            start_year=int(args.start_year),
            end_date=args.end_date,
            futures_path=futures_path,
            futures_sha256=futures_sha256,
            normalized=normalized,
            full_chain=full_chain,
            receipts=receipt_manifest,
            builder_fingerprint=builder_fingerprint,
        ):
            _require_unchanged_inputs(input_signatures, builder_fingerprint)
            print(
                f"[taifex-option-daily-timing] stage={series_scope}.reuse "
                "seconds=0.000 reason=verified_identical_sources_and_outputs",
                flush=True,
            )
            continue
        stage_started = time.perf_counter()
        if futures_reference is None:
            futures_reference = load_taifex_tx_opening_reference(futures_path)
            _require_unchanged_inputs(input_signatures, builder_fingerprint)
        atm_projections, atm_rebuilt = _prepare_atm_source_projections(
            receipt_manifest, ATM_SOURCE_CACHE,
            scope=series_scope, tx_by_date=futures_reference.contract_open_by_date,
            builder_fingerprint=builder_fingerprint,
        )
        build_taifex_opening_atm_straddles(
            receipts,
            futures_path,
            normalized,
            series_scope=series_scope,
            source_projections=atm_projections,
            futures_reference=futures_reference,
        )
        print(
            f"[taifex-option-daily-timing] stage={series_scope}.atm "
            f"seconds={time.perf_counter() - stage_started:.3f} "
            f"sources_rebuilt={atm_rebuilt} "
            f"sources_reused={len(atm_projections) - atm_rebuilt}",
            flush=True,
        )
        stage_started = time.perf_counter()
        shards, rebuilt_shards = _prepare_full_chain_shards(
            receipt_manifest, futures_reference, FULL_CHAIN_SHARD_CACHE,
            scope=series_scope,
            builder_fingerprint=builder_fingerprint,
        )
        _merge_full_chain_shards(
            shards, full_chain, scope=series_scope,
            futures_reference=futures_reference,
        )
        print(
            f"[taifex-option-daily-timing] stage={series_scope}.full_chain "
            f"seconds={time.perf_counter() - stage_started:.3f} "
            f"shards_rebuilt={rebuilt_shards} shards_reused={len(shards) - rebuilt_shards}",
            flush=True,
        )
        stage_started = time.perf_counter()
        quality = _quality_summary(normalized, series_scope=series_scope)
        full_chain_quality = _full_chain_quality_summary(full_chain)
        _require_unchanged_inputs(input_signatures, builder_fingerprint)
        manifest = {
            "dataset": dataset_names[series_scope],
            "contract_version": TAIFEX_OPTIONS_DAILY_DATA_CONTRACT_VERSION,
            "status": "complete",
            "official_page": TAIFEX_OPTION_DAILY_PAGE,
            "official_download_endpoint": TAIFEX_OPTION_DOWNLOAD_URL,
            "product": "TXO",
            "session": "一般",
            "series_scope": (
                "nearest_unexpired_monthly_only"
                if series_scope == "monthly"
                else "nearest_expiry_weekly_only"
            ),
            "atm_reference": "official_front_month_TX_day_session_open",
            "price_source": TAIFEX_OPTIONS_DAILY_PRICE_SOURCE,
            "price_boundary": (
                "option open/close are each leg's first/last official transaction; "
                "they are not simultaneous executable bid/ask quotes"
            ),
            "start_year": int(args.start_year),
            "end_date": args.end_date.isoformat(),
            "futures_path": str(futures_path),
            "futures_sha256": futures_sha256,
            "normalized_path": str(normalized),
            "normalized_sha256": sha256_path(normalized),
            "quality": quality,
            "full_chain_path": str(full_chain),
            "full_chain_sha256": sha256_path(full_chain),
            "full_chain_quality": full_chain_quality,
            "receipts": receipt_manifest,
            "builder_fingerprint": builder_fingerprint,
        }
        atomic_write_json(output_dir / manifest_name, manifest)
        print(
            f"[taifex-option-daily-timing] stage={series_scope}.quality_manifest "
            f"seconds={time.perf_counter() - stage_started:.3f}",
            flush=True,
        )
        print(
            f"built {normalized} from {len(receipts)} official receipt(s); "
            f"{quality['executable_rows']:,}/{quality['rows']:,} candidate sessions executable",
            flush=True,
        )
    print(
        f"[taifex-option-daily-timing] stage=total "
        f"seconds={time.perf_counter() - run_started:.3f}",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

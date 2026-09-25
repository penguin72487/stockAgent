"""Receipt-bound lazy source for the physical TW day-trade FIFO executor.

This module converts immutable daily/minute sources into executor-only sessions.
It performs the expensive Parquet scan once, commits a content-addressed cache,
and then memory-maps only the dates requested by a chronological trainer batch.
No array produced here is a model feature or a broker fill receipt.
"""

from __future__ import annotations

from datetime import date
import fcntl
import hashlib
import json
import os
from pathlib import Path
import tempfile
import threading
import time
from typing import Any

import numpy as np
import torch

from stockagent.backtest.tw_day_trade_carry import (
    DayTradeCarrySession,
    compact_day_trade_carry_session,
)
from stockagent.backtest.tw_day_trade_contract import BOARD_LOT_SHARES
from stockagent.data.tw_day_trade_schedule import (
    PAPER_MINUTE_SCHEDULE_ABI,
    paper_minute_opportunities,
)
from stockagent.data.panel import (
    _load_exact_cash_entitlements,
    _resolve_corporate_action_reference_paths,
)
from stockagent.data.tw_price_rules import (
    limit_price_numpy,
    price_on_tick_grid_numpy,
    tick_size_numpy,
)
from stockagent.data.tw_security import classify_tw_stock_or_etf
from stockagent.training.day_trade_carry_bridge import (
    PackedDayTradeCarrySession,
    PreparedDayTradeCarrySource,
)


def compact_packed_day_trade_carry_session(
    packed: PackedDayTradeCarrySession,
) -> DayTradeCarrySession:
    """Compact the verified sparse source without materializing 270 x 2 exits.

    This is the same source-only statistic as compacting a densified session.
    The model-dependent long/short choice still happens inside the executor.
    """
    packed.validate()
    symbols = int(packed.official_open.numel())
    if packed.exit_flat.numel() and not bool(
        (packed.exit_flat[1:] > packed.exit_flat[:-1]).all()
    ):
        raise ValueError("packed exit identities must be strictly ordered")
    retained = (
        torch.isfinite(packed.exit_price)
        | (packed.exit_capacity != 0)
        | ~torch.isfinite(packed.exit_capacity)
    )
    flat = packed.exit_flat[retained]
    prices = packed.exit_price[retained]
    capacity = packed.exit_capacity[retained]
    event_symbols = torch.div(flat, 270 * 2, rounding_mode="floor")
    event_sides = flat.remainder(2)
    symbol_axis = torch.arange(symbols, dtype=torch.int64)
    starts = torch.searchsorted(event_symbols, symbol_axis, right=False)
    ends = torch.searchsorted(event_symbols, symbol_axis, right=True)

    price_ok = torch.isfinite(prices) & (prices > 0)
    side_slot = event_symbols * 2 + event_sides
    minimum = prices.new_full((symbols * 2,), float("inf"))
    maximum = prices.new_full((symbols * 2,), float("-inf"))
    minimum.scatter_reduce_(
        0, side_slot, torch.where(price_ok, prices, float("inf")), reduce="amin"
    )
    maximum.scatter_reduce_(
        0, side_slot, torch.where(price_ok, prices, float("-inf")), reduce="amax"
    )
    has_exit = torch.isfinite(minimum)
    minimum = torch.where(has_exit, minimum, 0).reshape(symbols, 2)
    maximum = torch.where(has_exit, maximum, 0).reshape(symbols, 2)
    mark_ok = torch.isfinite(packed.marks) & (packed.marks > 0)
    mark_path_valid = mark_ok.all(dim=1)
    minimum_marks = torch.where(
        mark_path_valid,
        torch.where(mark_ok, packed.marks, float("inf")).amin(dim=1),
        0,
    )
    maximum_marks = torch.where(
        mark_path_valid,
        torch.where(mark_ok, packed.marks, float("-inf")).amax(dim=1),
        0,
    )

    # The no-event slot is inert, but the fixed sparse ABI requires one entry.
    if flat.numel() == 0:
        prices = packed.exit_price.new_full((1,), float("nan"))
        capacity = packed.exit_capacity.new_zeros((1,))
        event_symbols = packed.exit_flat.new_full((1,), symbols - 1)
        event_sides = packed.exit_flat.new_zeros((1,))

    return DayTradeCarrySession(
        day=packed.day,
        official_open=packed.official_open,
        opening_marks=packed.opening_marks,
        entry_price=packed.entry_price,
        entry_volume=packed.entry_volume,
        lower_limit=packed.lower_limit,
        upper_limit=packed.upper_limit,
        halted=packed.halted,
        exit_prices=prices,
        exit_capacity=capacity,
        marks=packed.marks[:, -1:],
        action_mask=packed.action_mask,
        share_ratio=packed.share_ratio,
        cash_per_old_share=packed.cash_per_old_share,
        payment_day=packed.payment_day,
        stock_delivery_day=packed.stock_delivery_day,
        source_gap_mask=packed.source_gap_mask,
        unresolved_action_gap_mask=packed.unresolved_action_gap_mask,
        daily_proxy_mask=packed.daily_proxy_mask,
        exit_symbol_indices=event_symbols,
        exit_sides=event_sides,
        symbol_event_starts=starts,
        symbol_event_ends=ends,
        minimum_marks=minimum_marks,
        maximum_marks=maximum_marks,
        mark_path_valid=mark_path_valid.to(torch.float64),
        minimum_exit_prices=minimum,
        maximum_exit_prices=maximum,
        terminal_liquidation_price=packed.terminal_liquidation_price,
    )


PHYSICAL_SOURCE_CACHE_ABI = (
    "tw_day_trade_physical_source_cache_v12_subscription_right_reference_value"
)
_PREDECESSOR_PHYSICAL_SOURCE_CACHE_ABI = (
    "tw_day_trade_physical_source_cache_v11_share_replacement_session"
)
PHYSICAL_PRICE_LIMIT_CACHE_ABI = "tw_day_trade_physical_price_limits_v1"
PHYSICAL_SOURCE_RUN_RECEIPT_SCHEMA = 1
PHYSICAL_SOURCE_RUN_RECEIPT_ENV = (
    "STOCKAGENT_PINNED_DAY_TRADE_SOURCE_RECEIPT"
)
PATH_CHANNELS = (
    "long_exit_price",
    "short_exit_price",
    "long_exit_capacity",
    "short_exit_capacity",
    "mark",
)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _hash_array(digest: Any, name: str, value: np.ndarray) -> None:
    array = np.ascontiguousarray(value)
    digest.update(name.encode("utf-8") + b"\0")
    digest.update(str(array.dtype).encode("ascii") + b"\0")
    digest.update(json.dumps(array.shape).encode("ascii") + b"\0")
    digest.update(array.view(np.uint8))


def _ordinal(day: np.datetime64) -> int:
    return int(day.astype("datetime64[D]").astype(np.int64)) + date(1970, 1, 1).toordinal()


def _atomic_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", dir=path.parent,
        prefix=f".{path.name}.", suffix=".tmp", delete=False,
    ) as handle:
        temporary = Path(handle.name)
        json.dump(payload, handle, ensure_ascii=False, indent=2, sort_keys=True)
        handle.flush()
        os.fsync(handle.fileno())
    try:
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _atomic_npy(path: Path, value: np.ndarray) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        dir=path.parent, prefix=f".{path.name}.", suffix=".tmp", delete=False,
    ) as handle:
        temporary = Path(handle.name)
        np.save(handle, value, allow_pickle=False)
        handle.flush()
        os.fsync(handle.fileno())
    try:
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _load_canonical_sparse_exact_inventory_cache(
    path: Path,
    *,
    exact_action_mask: np.ndarray,
    exact_share_ratio: np.ndarray,
    exact_cash: np.ndarray,
    exact_payment_day: np.ndarray,
    exact_stock_delivery_day: np.ndarray,
) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    """Load a sparse action cache only when it equals the accepted source.

    A complete subscription right may have zero economic value when its
    official reference price does not exceed the subscription price. Such an
    event is intentionally retained as a resolved no-op action, so the old
    blanket ``ratio == 1 and cash == 0`` rejection was not a valid cache
    invariant. The stronger invariant is exact equality with the canonical
    arrays already derived from the receipt-verified source for this run.
    """
    with np.load(path, allow_pickle=False) as packed_actions:
        expected_fields = {
            "action_flat",
            "total_share_ratio",
            "cash_per_old_share",
            "payment_day",
            "stock_delivery_day",
        }
        if set(packed_actions.files) != expected_fields:
            raise RuntimeError(
                "invalid physical exact-inventory cache fields: "
                f"expected={sorted(expected_fields)} "
                f"actual={sorted(packed_actions.files)}"
            )
        actual = {
            name: np.array(packed_actions[name], copy=True)
            for name in expected_fields
        }

    action_flat = np.flatnonzero(exact_action_mask.reshape(-1)).astype(
        np.int64, copy=False
    )
    expected = {
        "action_flat": action_flat,
        "total_share_ratio": exact_share_ratio.reshape(-1)[action_flat].astype(
            np.float64, copy=False
        ),
        "cash_per_old_share": exact_cash.reshape(-1)[action_flat].astype(
            np.float64, copy=False
        ),
        "payment_day": exact_payment_day.reshape(-1)[action_flat].astype(
            np.int64, copy=False
        ),
        "stock_delivery_day": exact_stock_delivery_day.reshape(-1)[
            action_flat
        ].astype(np.int64, copy=False),
    }
    for name, expected_value in expected.items():
        actual_value = actual[name]
        if (
            actual_value.dtype != expected_value.dtype
            or actual_value.shape != expected_value.shape
        ):
            raise RuntimeError(
                "invalid physical sparse exact-inventory cache layout: "
                f"field={name} expected_dtype={expected_value.dtype} "
                f"actual_dtype={actual_value.dtype} "
                f"expected_shape={expected_value.shape} "
                f"actual_shape={actual_value.shape}"
            )
        if not np.array_equal(actual_value, expected_value):
            mismatch = np.flatnonzero(actual_value != expected_value)
            sparse_row = int(mismatch[0]) if mismatch.size else -1
            raise RuntimeError(
                "physical sparse exact-inventory cache differs from the "
                "receipt-derived canonical action: "
                f"field={name} sparse_row={sparse_row} "
                f"cached={actual_value[sparse_row]!r} "
                f"expected={expected_value[sparse_row]!r}"
            )
    return (
        actual["action_flat"],
        actual["total_share_ratio"],
        actual["cash_per_old_share"],
        actual["payment_day"],
        actual["stock_delivery_day"],
    )


def _atomic_npz(path: Path, **payload: np.ndarray) -> None:
    """Write an uncompressed sparse archive; fast reads matter every epoch."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        dir=path.parent, prefix=f".{path.name}.", suffix=".tmp", delete=False,
    ) as handle:
        temporary = Path(handle.name)
        np.savez(handle, **payload)
        handle.flush()
        os.fsync(handle.fileno())
    try:
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _boot_id() -> str:
    try:
        return Path("/proc/sys/kernel/random/boot_id").read_text(
            encoding="utf-8"
        ).strip()
    except OSError:
        return "unavailable"


def _cache_stat_fingerprint(paths: list[Path]) -> str:
    """Fingerprint immutable file identities without rereading their bytes."""

    digest = hashlib.sha256()
    for path in sorted(paths, key=lambda item: item.name):
        stat = path.stat()
        identity = (
            path.name,
            int(stat.st_size),
            int(stat.st_mtime_ns),
            int(stat.st_ctime_ns),
            int(stat.st_ino),
            int(stat.st_dev),
        )
        digest.update(
            json.dumps(identity, separators=(",", ":")).encode("utf-8")
        )
        digest.update(b"\0")
    return digest.hexdigest()


def _write_run_verification_receipt(
    *, cache_root: Path, manifest_path: Path, ready_path: Path,
    source_digest: str, required: list[Path],
) -> Path:
    receipt_root = cache_root.parent / ".run-verification"
    receipt_root.mkdir(parents=True, exist_ok=True)
    receipt_path = receipt_root / (
        f"{source_digest}-{_boot_id()}-{os.getpid()}.json"
    )
    _atomic_json(
        receipt_path,
        {
            "schema_version": PHYSICAL_SOURCE_RUN_RECEIPT_SCHEMA,
            "abi": PHYSICAL_SOURCE_CACHE_ABI,
            "source_digest": source_digest,
            "cache_root": str(cache_root),
            "manifest_sha256": _sha256(manifest_path),
            "ready_sha256": _sha256(ready_path),
            "file_count": len(required),
            "verified_bytes": int(sum(path.stat().st_size for path in required)),
            "stat_fingerprint": _cache_stat_fingerprint(required),
            "boot_id": _boot_id(),
            "issuer_pid": os.getpid(),
        },
    )
    return receipt_path


def _reuse_run_verification_receipt(
    *, cache_root: Path, manifest_path: Path, ready_path: Path,
    source_digest: str, required: list[Path],
) -> Path | None:
    raw_path = os.environ.get(PHYSICAL_SOURCE_RUN_RECEIPT_ENV, "").strip()
    if not raw_path:
        return None
    path = Path(raw_path).expanduser().resolve()
    receipt_root = (cache_root.parent / ".run-verification").resolve()
    if not path.is_relative_to(receipt_root):
        return None
    try:
        receipt = json.loads(path.read_text(encoding="utf-8"))
        issuer_pid = int(receipt.get("issuer_pid", -1))
        if (
            receipt.get("schema_version")
            != PHYSICAL_SOURCE_RUN_RECEIPT_SCHEMA
            or receipt.get("abi") != PHYSICAL_SOURCE_CACHE_ABI
            or receipt.get("source_digest") != source_digest
            or receipt.get("cache_root") != str(cache_root)
            or receipt.get("boot_id") != _boot_id()
            or issuer_pid <= 0
            or not Path(f"/proc/{issuer_pid}").is_dir()
            or receipt.get("manifest_sha256") != _sha256(manifest_path)
            or receipt.get("ready_sha256") != _sha256(ready_path)
            or receipt.get("file_count") != len(required)
            or receipt.get("verified_bytes")
            != int(sum(item.stat().st_size for item in required))
            or receipt.get("stat_fingerprint")
            != _cache_stat_fingerprint(required)
        ):
            return None
    except (OSError, ValueError, TypeError, json.JSONDecodeError):
        return None
    return path


def _verify_physical_cache_files(
    *, cache_root: Path, manifest_path: Path, ready_path: Path,
    source_digest: str, required: list[Path],
    expected: dict[str, Any],
) -> tuple[bool, Path | None, str]:
    if not isinstance(expected, dict) or set(expected) != {
        path.name for path in required
    }:
        return False, None, "manifest_file_set_mismatch"
    if any(
        not path.is_file()
        or expected.get(path.name, {}).get("bytes") != path.stat().st_size
        for path in required
    ):
        return False, None, "file_identity_mismatch"
    pinned = _reuse_run_verification_receipt(
        cache_root=cache_root,
        manifest_path=manifest_path,
        ready_path=ready_path,
        source_digest=source_digest,
        required=required,
    )
    if pinned is not None:
        return True, pinned, "run_receipt"
    if any(
        expected.get(path.name, {}).get("sha256") != _sha256(path)
        for path in required
    ):
        return False, None, "sha256_mismatch"
    receipt = _write_run_verification_receipt(
        cache_root=cache_root,
        manifest_path=manifest_path,
        ready_path=ready_path,
        source_digest=source_digest,
        required=required,
    )
    return True, receipt, "full_sha256"


def _public_root(public_feature_path: str | Path) -> Path:
    path = Path(public_feature_path).resolve()
    if path.name != "tw_public_stock_daily.parquet" or path.parent.name != "features":
        raise ValueError(
            "physical day-trade source requires the canonical public feature path"
        )
    root = path.parent.parent
    required = (
        root / "twse_daily_ohlcv.parquet",
        root / "tpex_daily_ohlcv.parquet",
        root / "tw_corporate_action_reference.parquet",
        root / "tw_corporate_action_entitlements.parquet",
        root / "tw_corporate_action_entitlements.summary.json",
        root / "tw_share_replacement_reference.parquet",
        root / "tw_share_replacement_reference.summary.json",
    )
    missing = [str(item) for item in required if not item.is_file()]
    if missing:
        raise FileNotFoundError(
            "physical day-trade source is missing public inputs: " + ", ".join(missing)
        )
    return root


def _source_identity(
    *, minute_root: Path, public_root: Path, dates: np.ndarray,
    symbols: tuple[str, ...], opens: np.ndarray, closes: np.ndarray,
    volumes: np.ndarray, action_mask: np.ndarray,
    unresolved_action_mask: np.ndarray, force_exit: np.ndarray,
    exact_action_mask: np.ndarray, exact_share_ratio: np.ndarray,
    exact_cash: np.ndarray, exact_payment_day: np.ndarray,
    exact_stock_delivery_day: np.ndarray,
    no_regular_execution: np.ndarray,
    unresolved_replacement_block: np.ndarray,
) -> tuple[
    str,
    dict[str, dict[str, Any]],
    dict[np.datetime64, tuple[Path, str]],
    dict[str, Any],
]:
    manifest = minute_root / "manifest.json"
    if not manifest.is_file():
        raise FileNotFoundError(f"physical minute source has no manifest: {manifest}")
    try:
        minute_receipt = json.loads(manifest.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"invalid physical minute manifest: {manifest}") from exc
    if not (
        isinstance(minute_receipt, dict)
        and minute_receipt.get("source") == "shioaji_kbars_1m"
        and minute_receipt.get("research_ready") is True
        and minute_receipt.get("status") == "research_ready"
        and isinstance(minute_receipt.get("partitions"), list)
    ):
        raise RuntimeError("physical minute source manifest is not research_ready")
    partitions: dict[np.datetime64, tuple[Path, str]] = {}
    selected_days = set(dates.astype("datetime64[D]"))
    seen_manifest_days: set[np.datetime64] = set()
    partition_scope: dict[str, Any] = {
        "selected_panel_partitions": 0,
        "quarantined_non_panel_session_partitions": [],
        "unselected_outside_panel_horizon_partitions": [],
    }
    for item in minute_receipt["partitions"]:
        if not isinstance(item, dict):
            raise RuntimeError("physical minute manifest partition must be structured")
        trade_date = item.get("trade_date")
        relative = item.get("output")
        output_sha256 = item.get("output_sha256")
        if (
            item.get("status") != "ok"
            or not isinstance(trade_date, str)
            or not isinstance(relative, str)
            or relative != f"trade_date={trade_date}/data.parquet"
            or not isinstance(output_sha256, str)
            or len(output_sha256) != 64
        ):
            raise RuntimeError(
                f"physical minute manifest has an unaccepted partition: {trade_date}"
            )
        day = np.datetime64(trade_date, "D")
        output = (minute_root / relative).resolve()
        try:
            output.relative_to(minute_root.resolve())
        except ValueError as exc:
            raise RuntimeError("physical minute partition escapes its exact release") from exc
        if day in seen_manifest_days:
            raise RuntimeError(f"physical minute manifest repeats partition {trade_date}")
        seen_manifest_days.add(day)
        if day in selected_days:
            partitions[day] = (output, output_sha256)
            partition_scope["selected_panel_partitions"] += 1
            continue
        if not output.is_file():
            raise FileNotFoundError(
                f"unselected physical minute partition is missing: {output}"
            )
        actual_sha256 = _sha256(output)
        if actual_sha256 != output_sha256:
            raise RuntimeError(
                "unselected physical minute partition differs from its manifest: "
                f"date={trade_date} expected={output_sha256} actual={actual_sha256}"
            )
        receipt = {
            "trade_date": trade_date,
            "output": relative,
            "output_sha256": output_sha256,
            "rows": int(item.get("rows", -1)),
            "symbols": int(item.get("symbols", -1)),
        }
        if dates[0] < day < dates[-1]:
            partition_scope["quarantined_non_panel_session_partitions"].append(
                receipt
            )
        else:
            partition_scope["unselected_outside_panel_horizon_partitions"].append(
                receipt
            )
    if not partitions:
        raise RuntimeError("physical minute manifest declares no accepted partition")
    source_paths = {
        "minute_manifest": manifest,
        "twse_daily": public_root / "twse_daily_ohlcv.parquet",
        "tpex_daily": public_root / "tpex_daily_ohlcv.parquet",
        "corporate_reference": public_root / "tw_corporate_action_reference.parquet",
        "corporate_entitlements": public_root / "tw_corporate_action_entitlements.parquet",
        "corporate_entitlements_receipt": public_root / "tw_corporate_action_entitlements.summary.json",
        "share_replacement": public_root / "tw_share_replacement_reference.parquet",
        "share_replacement_receipt": public_root / "tw_share_replacement_reference.summary.json",
    }
    receipts = {
        name: {"path": str(path), "bytes": path.stat().st_size, "sha256": _sha256(path)}
        for name, path in source_paths.items()
    }
    digest = _source_content_digest(
        source_cache_abi=PHYSICAL_SOURCE_CACHE_ABI,
        receipts=receipts,
        symbols=symbols,
        dates=dates,
        opens=opens,
        closes=closes,
        volumes=volumes,
        action_mask=action_mask,
        unresolved_action_mask=unresolved_action_mask,
        force_exit=force_exit,
        exact_action_mask=exact_action_mask,
        exact_share_ratio=exact_share_ratio,
        exact_cash=exact_cash,
        exact_payment_day=exact_payment_day,
        exact_stock_delivery_day=exact_stock_delivery_day,
        no_regular_execution=no_regular_execution,
        unresolved_replacement_block=unresolved_replacement_block,
    )
    return digest, receipts, partitions, partition_scope


def _source_content_digest(
    *, source_cache_abi: str, receipts: dict[str, dict[str, Any]],
    symbols: tuple[str, ...], dates: np.ndarray, opens: np.ndarray,
    closes: np.ndarray, volumes: np.ndarray, action_mask: np.ndarray,
    unresolved_action_mask: np.ndarray, force_exit: np.ndarray,
    exact_action_mask: np.ndarray, exact_share_ratio: np.ndarray,
    exact_cash: np.ndarray, exact_payment_day: np.ndarray,
    exact_stock_delivery_day: np.ndarray, no_regular_execution: np.ndarray,
    unresolved_replacement_block: np.ndarray,
) -> str:
    """Hash already-verified source inputs under one explicit executor ABI."""

    digest = hashlib.sha256()
    digest.update(source_cache_abi.encode("ascii") + b"\0")
    digest.update(PAPER_MINUTE_SCHEDULE_ABI.encode("ascii") + b"\0")
    for name in sorted(receipts):
        digest.update(name.encode("utf-8") + b"\0")
        digest.update(receipts[name]["sha256"].encode("ascii") + b"\0")
    digest.update("\0".join(symbols).encode("utf-8"))
    for name, value in (
        ("dates", dates.astype("datetime64[D]").astype(np.int64)),
        ("official_open", opens), ("official_close", closes),
        ("daily_volume", volumes), ("corporate_avoidance", action_mask),
        ("unresolved_corporate_action", unresolved_action_mask),
        ("exact_inventory_action", exact_action_mask),
        ("exact_total_share_ratio", exact_share_ratio),
        ("exact_cash_per_old_share", exact_cash),
        ("exact_cash_payment_day", exact_payment_day),
        ("exact_stock_delivery_day", exact_stock_delivery_day),
        ("official_no_regular_execution", no_regular_execution),
        ("unresolved_replacement_block", unresolved_replacement_block),
        ("force_exit", force_exit),
    ):
        _hash_array(digest, name, np.asarray(value))
    return digest.hexdigest()


def _exact_inventory_action_arrays(
    *, public_feature_path: Path, dates: np.ndarray, symbols: tuple[str, ...],
) -> tuple[
    np.ndarray,
    np.ndarray,
    np.ndarray,
    np.ndarray,
    np.ndarray,
    dict[str, Any],
]:
    """Map receipt-verified cash, stock, and subscription rights to sessions.

    Avoid mode still asks the policy to flatten before every corporate action.
    This executor-only ledger is the physical fallback for a residual that 50%
    participation could not liquidate.  Stock rights enter economic holdings on
    the ex-date but remain non-executable until their official delivery/listing
    date.  A pure cash-capital-increase subscription right is settled at its
    official ex-right reference value ``ratio * max(reference - exercise, 0)``.
    This symmetric signed claim prevents either long or short residuals from
    receiving a free mechanical ex-right price move without fabricating an
    exercise, payment date, or stock-delivery date.  It is not a model feature
    and never relaxes incomplete or mixed terms.
    """
    paths = _resolve_corporate_action_reference_paths(
        public_feature_path, include_rules=True
    )
    if paths is None:
        raise FileNotFoundError(
            "physical carry source requires the canonical corporate-action receipts"
        )
    _cash_terms, _short_terms, coverage_start, coverage_end = (
        _load_exact_cash_entitlements(paths)
    )
    if coverage_start is None or coverage_end is None:
        raise ValueError(
            "physical carry source requires a complete exact entitlement baseline"
        )
    if dates[0] < coverage_start or dates[-1] > coverage_end:
        raise ValueError(
            "physical carry horizon exceeds exact-cash entitlement coverage: "
            f"panel={dates[0]}..{dates[-1]} "
            f"archive={coverage_start}..{coverage_end}"
        )

    shape = (dates.size, len(symbols))
    event_mask = np.zeros(shape, dtype=np.bool_)
    share_ratio = np.ones(shape, dtype=np.float64)
    cash = np.zeros(shape, dtype=np.float64)
    payment_day = np.zeros(shape, dtype=np.int64)
    stock_delivery_day = np.zeros(shape, dtype=np.int64)
    symbol_index = {symbol: index for index, symbol in enumerate(symbols)}
    mapped = 0
    outside_universe = 0
    outside_horizon = 0
    mapped_after_closed_date = 0
    import polars as pl

    parquet_path = paths.entitlements_parquet
    assert parquet_path is not None
    available = set(pl.scan_parquet(parquet_path).collect_schema().names())
    required = {
        "date",
        "symbol",
        "handling",
        "handling_reason",
        "reference_price",
        "cash_dividend_per_share",
        "cash_payment_date",
        "stock_dividend_ratio",
        "stock_delivery_date",
        "stock_terms_complete",
        "subscription_ratio",
        "subscription_price",
    }
    missing = required - available
    if missing:
        raise ValueError(
            "physical carry source requires pending-stock entitlement columns: "
            f"{sorted(missing)}; rebuild the official entitlement release"
        )
    actions = (
        pl.read_parquet(parquet_path, columns=sorted(required))
        .filter(
            pl.col("handling").is_in(["exact_cash", "exact_inventory"])
            | (
                pl.col("handling").eq("avoid")
                & pl.col("handling_reason").eq("stock_or_subscription_action")
                & pl.col("stock_terms_complete").fill_null(False)
                & (pl.col("cash_dividend_per_share") == 0.0)
                & (pl.col("stock_dividend_ratio") == 0.0)
                & (pl.col("subscription_ratio") > 0.0)
                & (pl.col("subscription_price") > 0.0)
                & (pl.col("reference_price") > 0.0)
            )
        )
        .sort(["date", "symbol"])
    )
    exact_cash_events = 0
    exact_stock_events = 0
    subscription_right_events = 0
    zero_value_subscription_right_events = 0
    subscription_right_flat_indices: list[int] = []
    for item in actions.iter_rows(named=True):
        symbol = str(item["symbol"] or "").strip().upper()
        column = symbol_index.get(symbol)
        if column is None:
            outside_universe += 1
            continue
        event_date = np.datetime64(item["date"], "D")
        row = int(np.searchsorted(dates, event_date, side="left"))
        if row >= dates.size or event_date < dates[0]:
            outside_horizon += 1
            continue
        effective_day = dates[row]
        if event_mask[row, column]:
            raise ValueError(
                "multiple exact inventory events map to one physical session: "
                f"date={effective_day} symbol={symbol}"
            )
        amount = float(item["cash_dividend_per_share"] or 0.0)
        stock_increment = float(item["stock_dividend_ratio"] or 0.0)
        subscription = float(item["subscription_ratio"] or 0.0)
        subscription_price = float(item["subscription_price"] or 0.0)
        reference_price = float(item["reference_price"] or 0.0)
        is_subscription_right = item["handling"] == "avoid"
        if is_subscription_right:
            subscription_valid = (
                item["handling_reason"] == "stock_or_subscription_action"
                and item["stock_terms_complete"] is True
                and amount == 0.0
                and stock_increment == 0.0
                and np.isfinite(subscription)
                and subscription > 0.0
                and np.isfinite(subscription_price)
                and subscription_price > 0.0
                and np.isfinite(reference_price)
                and reference_price > 0.0
            )
            if not subscription_valid:
                raise ValueError(
                    "invalid subscription-right terms after session mapping: "
                    f"event={event_date} effective={effective_day} symbol={symbol}"
                )
            amount = subscription * max(reference_price - subscription_price, 0.0)
        payment_date = (
            np.datetime64("NaT", "D")
            if item["cash_payment_date"] is None
            else np.datetime64(item["cash_payment_date"], "D")
        )
        delivery_date = (
            np.datetime64("NaT", "D")
            if item["stock_delivery_date"] is None
            else np.datetime64(item["stock_delivery_date"], "D")
        )
        if is_subscription_right and amount > 0.0:
            # This is an explicit ex-date cash-equivalent settlement of the
            # official reference value, not a fabricated issuer payment date.
            payment_date = effective_day
        cash_valid = (
            np.isfinite(amount)
            and amount >= 0.0
            and (
                amount == 0.0
                or (not np.isnat(payment_date) and payment_date >= effective_day)
            )
        )
        stock_valid = (
            np.isfinite(stock_increment)
            and stock_increment >= 0.0
            and (
                stock_increment == 0.0
                or (
                    not np.isnat(delivery_date)
                    and delivery_date >= effective_day
                )
            )
        )
        if (
            not cash_valid
            or not stock_valid
            or (
                not is_subscription_right
                and (
                    not np.isfinite(subscription)
                    or subscription != 0.0
                    or (amount == 0.0 and stock_increment == 0.0)
                )
            )
            or (
                item["handling"] == "exact_cash"
                and stock_increment != 0.0
            )
            or (
                item["handling"] == "exact_inventory"
                and stock_increment <= 0.0
            )
        ):
            raise ValueError(
                "invalid exact inventory action after session mapping: "
                f"event={event_date} effective={effective_day} symbol={symbol}"
            )
        event_mask[row, column] = True
        share_ratio[row, column] = 1.0 + stock_increment
        cash[row, column] = amount
        payment_day[row, column] = (
            0
            if amount == 0.0
            else _ordinal(effective_day if is_subscription_right else payment_date)
        )
        stock_delivery_day[row, column] = (
            0 if stock_increment == 0.0 else _ordinal(delivery_date)
        )
        exact_cash_events += int(amount > 0.0)
        exact_stock_events += int(stock_increment > 0.0)
        subscription_right_events += int(is_subscription_right)
        zero_value_subscription_right_events += int(
            is_subscription_right and amount == 0.0
        )
        if is_subscription_right:
            subscription_right_flat_indices.append(row * len(symbols) + column)
        mapped += 1
        mapped_after_closed_date += int(effective_day != event_date)
    return event_mask, share_ratio, cash, payment_day, stock_delivery_day, {
        "mapped_events": mapped,
        "mapped_cash_events": exact_cash_events,
        "mapped_pending_stock_events": exact_stock_events,
        "mapped_subscription_right_events": subscription_right_events,
        "mapped_zero_value_subscription_right_events": (
            zero_value_subscription_right_events
        ),
        "mapped_after_closed_date": mapped_after_closed_date,
        "outside_universe_events": outside_universe,
        "outside_horizon_events": outside_horizon,
        "coverage_start": str(coverage_start),
        "coverage_end": str(coverage_end),
        "policy": (
            "exact_inventory_on_first_exchange_session_on_or_after_ex_date_"
            "with_stock_locked_until_official_delivery_and_pure_subscription_"
            "rights_settled_at_official_reference_value"
        ),
        "subscription_right_value_policy": (
            "subscription_ratio_times_max_official_ex_right_reference_minus_"
            "subscription_price_zero_as_symmetric_signed_claim"
        ),
        # Private build metadata. It is removed before the public manifest is
        # written and is used only to reconstruct the exact v11 predecessor
        # fingerprint for a guarded optimizer-checkpoint resume.
        "_subscription_right_flat_indices": subscription_right_flat_indices,
    }


def _share_replacement_arrays(
    *, public_root: Path, dates: np.ndarray, symbols: tuple[str, ...],
) -> tuple[
    np.ndarray,
    np.ndarray,
    np.ndarray,
    np.ndarray,
    np.ndarray,
    np.ndarray,
    np.ndarray,
    dict[str, Any],
]:
    """Map official replacement lifecycles without fabricating missing terms.

    Fully specified events transform residual FIFO inventory at resumption.
    Incomplete events use a conservative prefix exclusion so the symbol is
    provably flat through the unknown action.  That sacrifices some history,
    but never manufactures a price, quantity, payment date, or fractional-share
    disposition.
    """
    import polars as pl

    parquet = public_root / "tw_share_replacement_reference.parquet"
    receipt_path = parquet.with_suffix(".summary.json")
    try:
        receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError("invalid share-replacement lifecycle receipt") from exc
    output_receipt = receipt.get("output_receipt") or {}
    expected_size = output_receipt.get("size", output_receipt.get("bytes"))
    if (
        int(receipt.get("schema_version", 0)) < 3
        or receipt.get("source_download_complete") is not True
        or receipt.get("failure_count") != 0
        or receipt.get("complete_all_markets") is not True
        or receipt.get("lifecycle_catalog_complete") is not True
        or set(receipt.get("covered_markets") or ()) != {"twse", "tpex"}
        or output_receipt.get("sha256") != _sha256(parquet)
        or expected_size != parquet.stat().st_size
    ):
        raise ValueError("share-replacement lifecycle source is not accepted")
    try:
        coverage_start = np.datetime64(receipt.get("coverage_start"), "D")
        coverage_end = np.datetime64(receipt.get("coverage_end"), "D")
    except (TypeError, ValueError) as exc:
        raise ValueError("share-replacement coverage is invalid") from exc
    if (
        np.isnat(coverage_start)
        or np.isnat(coverage_end)
        or dates[0] < coverage_start
        or dates[-1] > coverage_end
    ):
        raise ValueError(
            "physical carry horizon exceeds share-replacement coverage: "
            f"panel={dates[0]}..{dates[-1]} "
            f"archive={coverage_start}..{coverage_end}"
        )

    required = {
        "symbol", "market", "resume_date", "suspension_date",
        "new_shares_per_1000_old", "cash_return_per_old_share",
        "cash_payment_date", "cash_dividend_per_old_share",
        "subscription_shares_per_1000", "subscription_terms_present",
        "historical_halt_evidence", "executable_price", "contract",
    }
    schema = pl.scan_parquet(parquet).collect_schema()
    missing = required - set(schema.names())
    if missing:
        raise ValueError(
            "share-replacement lifecycle schema is incomplete: "
            f"{sorted(missing)}"
        )
    frame = pl.read_parquet(parquet, columns=sorted(required)).sort(
        ["resume_date", "symbol"]
    )
    if frame.height != receipt.get("rows"):
        raise ValueError("share-replacement lifecycle row count mismatch")

    shape = (dates.size, len(symbols))
    halted = np.zeros(shape, dtype=np.bool_)
    event_mask = np.zeros(shape, dtype=np.bool_)
    share_ratio = np.ones(shape, dtype=np.float64)
    cash = np.zeros(shape, dtype=np.float64)
    payment_day = np.zeros(shape, dtype=np.int64)
    stock_delivery_day = np.zeros(shape, dtype=np.int64)
    unresolved_block = np.zeros(shape, dtype=np.bool_)
    symbol_index = {symbol: index for index, symbol in enumerate(symbols)}
    seen: set[tuple[str, np.datetime64]] = set()
    counts: dict[str, Any] = {
        "catalog_events": int(frame.height),
        "mapped_exact_events": 0,
        "mapped_exact_cash_events": 0,
        "mapped_after_closed_date": 0,
        "mapped_halt_symbol_days": 0,
        "unresolved_events": 0,
        "unresolved_prefix_block_symbol_days": 0,
        "outside_universe_events": 0,
        "outside_horizon_events": 0,
    }
    for item in frame.iter_rows(named=True):
        symbol = str(item["symbol"] or "").strip().upper()
        market = str(item["market"] or "").strip().lower()
        resume = (
            np.datetime64("NaT", "D")
            if item["resume_date"] is None
            else np.datetime64(item["resume_date"], "D")
        )
        key = (symbol, resume)
        if (
            not symbol
            or market not in {"twse", "tpex"}
            or np.isnat(resume)
            or key in seen
            or item["contract"] != "exchange_share_replacement_reference_v1"
            or item["executable_price"] is not False
        ):
            raise ValueError("invalid or duplicate share-replacement lifecycle row")
        seen.add(key)
        column = symbol_index.get(symbol)
        if column is None:
            counts["outside_universe_events"] += 1
            continue
        suspension = (
            np.datetime64("NaT", "D")
            if item["suspension_date"] is None
            else np.datetime64(item["suspension_date"], "D")
        )
        if resume <= dates[0]:
            counts["outside_horizon_events"] += 1
            continue
        if resume > dates[-1]:
            # The official list intentionally includes already-announced
            # resumptions beyond the requested observation cutoff. They must
            # never create an action on a non-existent panel session or mask
            # earlier history. If the suspension itself has already started,
            # retain only that observed no-execution interval through the panel
            # end; the physical conversion belongs to a later dataset release.
            if not np.isnat(suspension) and suspension <= dates[-1]:
                in_halt = (dates >= suspension) & (dates < resume)
                halted[in_halt, column] = True
            counts["outside_horizon_events"] += 1
            continue

        ratio_per_1000 = item["new_shares_per_1000_old"]
        amount = item["cash_return_per_old_share"]
        payment = (
            np.datetime64("NaT", "D")
            if item["cash_payment_date"] is None
            else np.datetime64(item["cash_payment_date"], "D")
        )
        exact = (
            item["historical_halt_evidence"] is True
            and not np.isnat(suspension)
            and suspension < resume
            and ratio_per_1000 is not None
            and np.isfinite(ratio_per_1000)
            and ratio_per_1000 > 0
            and abs(float(ratio_per_1000) - round(float(ratio_per_1000))) <= 1e-8
            and amount is not None
            and np.isfinite(amount)
            and amount >= 0
            and not (item["cash_dividend_per_old_share"] or 0)
            and not (item["subscription_shares_per_1000"] or 0)
            and not item["subscription_terms_present"]
            and (
                amount == 0
                or (not np.isnat(payment) and payment >= resume)
            )
        )
        if exact:
            row = int(np.searchsorted(dates, resume, side="left"))
            if row >= dates.size:
                raise ValueError(
                    "share-replacement resumption has no later panel session: "
                    f"date={resume} symbol={symbol}"
                )
            effective_day = dates[row]
            in_halt = (dates >= suspension) & (dates < effective_day)
            halted[in_halt, column] = True
            if event_mask[row, column]:
                raise ValueError(
                    "duplicate share-replacement action on one session: "
                    f"event={resume} effective={effective_day} symbol={symbol}"
                )
            event_mask[row, column] = True
            share_ratio[row, column] = float(ratio_per_1000) / 1000.0
            cash[row, column] = float(amount)
            payment_day[row, column] = 0 if amount == 0 else _ordinal(payment)
            counts["mapped_exact_events"] += 1
            counts["mapped_exact_cash_events"] += int(amount > 0)
            counts["mapped_after_closed_date"] += int(effective_day != resume)
            continue

        # A 50%-volume exit cannot prove that an existing residual reaches zero.
        # Starting from the empty account, disabling acquisition through the
        # unknown transition is the only conservative no-fabrication fallback.
        stop = int(np.searchsorted(dates, resume, side="left"))
        unresolved_block[:stop, column] = True
        counts["unresolved_events"] += 1

    counts["mapped_halt_symbol_days"] = int(halted.sum())
    counts["unresolved_prefix_block_symbol_days"] = int(unresolved_block.sum())
    counts["coverage_start"] = str(coverage_start)
    counts["coverage_end"] = str(coverage_end)
    counts["policy"] = "exact_fifo_conversion_or_empty_state_prefix_exclusion"
    return (
        halted,
        event_mask,
        share_ratio,
        cash,
        payment_day,
        stock_delivery_day,
        unresolved_block,
        counts,
    )


def _official_no_regular_execution_mask(
    *, public_root: Path, dates: np.ndarray, symbols: tuple[str, ...],
) -> tuple[np.ndarray, dict[str, int]]:
    """Identify official symbol rows that contain no regular-market OHLC.

    Such a row is positive evidence of no executable board-lot price, not a
    missing download. Existing inventory may retain the previous observable
    regular-market mark, while entry/exit capacity remains exactly zero. A
    completely absent official row is deliberately NOT classified here.
    """
    import polars as pl

    lookup = pl.DataFrame(
        {"symbol": list(symbols), "_column": np.arange(len(symbols), dtype=np.int32)}
    )
    shape = (dates.size, len(symbols))
    result = np.zeros(shape, dtype=np.bool_)
    counts: dict[str, int] = {}
    specifications = (
        ("twse", public_root / "twse_daily_ohlcv.parquet", "證券代號", "開盤價", "收盤價"),
        ("tpex", public_root / "tpex_daily_ohlcv.parquet", "代號", "開盤", "收盤"),
    )
    start = date.fromisoformat(str(dates[0]))
    end = date.fromisoformat(str(dates[-1]))
    for market, path, symbol_column, open_column, close_column in specifications:
        schema = pl.scan_parquet(path).collect_schema()
        required = {"date", symbol_column, open_column, close_column}
        missing = required - set(schema.names())
        if missing:
            raise ValueError(
                f"official {market} daily source lacks no-execution fields: {sorted(missing)}"
            )
        frame = (
            pl.scan_parquet(path)
            .select(
                pl.col("date").cast(pl.String).str.to_date(strict=False).alias("date"),
                pl.col(symbol_column).cast(pl.String).str.strip_chars().alias("symbol"),
                pl.col(open_column).cast(pl.String).str.replace_all(",", "")
                .cast(pl.Float64, strict=False).alias("open"),
                pl.col(close_column).cast(pl.String).str.replace_all(",", "")
                .cast(pl.Float64, strict=False).alias("close"),
            )
            .filter(pl.col("date").is_between(start, end, closed="both"))
            .join(lookup.lazy(), on="symbol", how="inner", validate="m:1")
            .filter(pl.col("open").is_null() & pl.col("close").is_null())
            .select("date", "_column")
            .collect(engine="streaming")
        )
        market_count = 0
        seen: set[tuple[int, int]] = set()
        for item in frame.iter_rows(named=True):
            if item["date"] is None:
                continue
            day = np.datetime64(item["date"], "D")
            row = int(np.searchsorted(dates, day, side="left"))
            column = int(item["_column"])
            if row >= dates.size or dates[row] != day:
                continue
            key = (row, column)
            if key in seen:
                raise ValueError(
                    "official no-regular-execution source repeats symbol/date: "
                    f"market={market} date={day} symbol={symbols[column]}"
                )
            seen.add(key)
            result[row, column] = True
            market_count += 1
        counts[f"{market}_symbol_days"] = market_count
    counts["total_symbol_days"] = int(result.sum())
    return result, counts


def _price_limits(
    *, public_root: Path, dates: np.ndarray, symbols: tuple[str, ...],
    closes: np.ndarray,
) -> tuple[np.ndarray, np.ndarray, np.ndarray, dict[str, int]]:
    """Build only source-proven limits; unsupported TWSE ETFs remain missing."""
    import polars as pl

    rows, width = closes.shape
    kinds = np.asarray(
        [classify_tw_stock_or_etf(symbol) or "unsupported" for symbol in symbols]
    )
    if np.any(kinds == "unsupported"):
        bad = np.flatnonzero(kinds == "unsupported")
        raise ValueError(
            "physical day-trade universe contains unsupported securities: "
            + ", ".join(symbols[index] for index in bad[:8])
        )
    tpex_path = public_root / "tpex_daily_ohlcv.parquet"
    # The TPEx columns are next-session limits.  Keep enough pre-panel rows to
    # resolve the first panel session without pretending that same-day limits
    # were known one session earlier.
    tpex_start = dates[0].astype("datetime64[D]") - np.timedelta64(45, "D")
    tpex = (
        pl.scan_parquet(tpex_path)
        .select(
            pl.col("date").cast(pl.Date, strict=False).alias("date"),
            pl.col("代號").cast(pl.String).str.strip_chars().alias("symbol"),
            *[
                pl.col(column).cast(pl.String).str.strip_chars()
                .str.replace_all(",", "").cast(pl.Float64, strict=False).alias(alias)
                for column, alias in (
                    ("次日漲停價", "upper"), ("次日跌停價", "lower")
                )
            ],
        )
        .filter(
            pl.col("date").is_between(
                pl.lit(date.fromisoformat(str(tpex_start))),
                pl.lit(date.fromisoformat(str(dates[-1].astype("datetime64[D]")))),
                closed="both",
            )
        )
        .collect(engine="streaming")
    )
    tpex_symbols = set(tpex.get_column("symbol").drop_nulls().to_list())
    # Resolve TPEx next-session limits as one columnar join.  The previous
    # implementation performed ``rows * symbols`` Python dictionary probes
    # (8.5M for the production panel) even though the official source is
    # already a columnar table.  Keep its exact semantics: only valid rows,
    # last duplicate wins, and the most recent TPEx source day strictly before
    # each selected panel session owns that session's limit.
    valid_tpex = (
        tpex.filter(
            pl.col("date").is_not_null()
            & pl.col("symbol").is_not_null()
            & pl.col("upper").is_finite()
            & pl.col("lower").is_finite()
            & (pl.col("upper") > pl.col("lower"))
            & (pl.col("lower") > 0)
        )
        .unique(subset=["date", "symbol"], keep="last", maintain_order=True)
    )
    tpex_days = np.asarray(
        sorted(valid_tpex.get_column("date").unique().to_list()),
        dtype="datetime64[D]",
    )
    corporate = pl.read_parquet(
        public_root / "tw_corporate_action_reference.parquet",
        columns=["date", "symbol", "reference_price"],
    )
    references_by_day: dict[np.datetime64, list[tuple[str, float]]] = {}
    for item in corporate.iter_rows(named=True):
        if (
            item["date"] is not None and item["symbol"]
            and item["reference_price"] is not None
            and np.isfinite(item["reference_price"])
            and item["reference_price"] > 0
        ):
            references_by_day.setdefault(np.datetime64(item["date"], "D"), []).append(
                (str(item["symbol"]), float(item["reference_price"]))
            )
    lower = np.full((rows, width), np.nan, dtype=np.float64)
    upper = np.full_like(lower, np.nan)
    last_close = np.full(width, np.nan, dtype=np.float64)
    symbol_index = {symbol: index for index, symbol in enumerate(symbols)}
    twse_stock = np.asarray(
        [kind == "stock" and symbol not in tpex_symbols for symbol, kind in zip(symbols, kinds)],
        dtype=bool,
    )
    for row, day in enumerate(dates.astype("datetime64[D]")):
        reference = last_close.copy()
        for symbol, price in references_by_day.get(day, ()):
            if symbol in symbol_index:
                reference[symbol_index[symbol]] = price
        valid = twse_stock & np.isfinite(reference) & (reference > 0)
        if valid.any():
            day_grid = np.full(int(valid.sum()), day, dtype="datetime64[D]")
            lower[row, valid] = limit_price_numpy(reference[valid], 0.90, day_grid)
            upper[row, valid] = limit_price_numpy(reference[valid], 1.10, day_grid)
        quote = closes[row]
        last_close = np.where(np.isfinite(quote) & (quote > 0), quote, last_close)
    if tpex_days.size and valid_tpex.height:
        prior_indices = np.searchsorted(
            tpex_days,
            dates.astype("datetime64[D]"),
            side="left",
        ) - 1
        target_rows = np.flatnonzero(prior_indices >= 0).astype(
            np.int32, copy=False
        )
        if target_rows.size:
            source_days = tpex_days[prior_indices[target_rows]]
            day_to_row = pl.DataFrame(
                {
                    "date": [date.fromisoformat(str(day)) for day in source_days],
                    "_row": target_rows,
                }
            )
            symbol_to_column = pl.DataFrame(
                {
                    "symbol": list(symbols),
                    "_column": np.arange(width, dtype=np.int32),
                }
            )
            mapped = (
                valid_tpex.join(day_to_row, on="date", how="inner")
                .join(
                    symbol_to_column,
                    on="symbol",
                    how="inner",
                    validate="m:1",
                )
                .select("_row", "_column", "lower", "upper")
            )
            mapped_rows = mapped.get_column("_row").to_numpy()
            mapped_columns = mapped.get_column("_column").to_numpy()
            lower[mapped_rows, mapped_columns] = mapped.get_column(
                "lower"
            ).to_numpy()
            upper[mapped_rows, mapped_columns] = mapped.get_column(
                "upper"
            ).to_numpy()
    valid_limits = np.isfinite(lower) & np.isfinite(upper) & (lower > 0) & (upper > lower)
    return lower, upper, kinds, {
        "valid_symbol_days": int(valid_limits.sum()),
        "missing_symbol_days": int(valid_limits.size - valid_limits.sum()),
        "unsupported_twse_etf_symbol_days": int(
            ((kinds == "etf") & ~np.asarray([symbol in tpex_symbols for symbol in symbols]))
            .sum() * rows
        ),
    }


def _load_or_build_price_limit_cache(
    *, cache_dir: Path, source_digest: str, public_root: Path,
    dates: np.ndarray, symbols: tuple[str, ...], closes: np.ndarray,
) -> tuple[np.ndarray, np.ndarray, np.ndarray, dict[str, int]]:
    """Reuse deterministic price-rule tensors across folds and DDP ranks."""

    key = hashlib.sha256(
        (PHYSICAL_PRICE_LIMIT_CACHE_ABI + "\0" + source_digest).encode("ascii")
    ).hexdigest()[:24]
    root = cache_dir / f"physical-price-limits-{key}"
    manifest_path = root / "manifest.json"
    ready_path = root / "READY.json"
    lock_path = cache_dir / f".{root.name}.lock"
    paths = {
        "lower": root / "lower.npy",
        "upper": root / "upper.npy",
        "security_types": root / "security_types.npy",
    }
    cache_dir.mkdir(parents=True, exist_ok=True)
    started = time.monotonic()
    with lock_path.open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        manifest: dict[str, Any] | None = None
        try:
            candidate = json.loads(manifest_path.read_text(encoding="utf-8"))
            ready = json.loads(ready_path.read_text(encoding="utf-8"))
            files = candidate.get("files", {})
            if (
                candidate.get("abi") == PHYSICAL_PRICE_LIMIT_CACHE_ABI
                and candidate.get("source_digest") == source_digest
                and candidate.get("rows") == int(dates.size)
                and candidate.get("symbols") == len(symbols)
                and set(files) == {path.name for path in paths.values()}
                and ready.get("abi") == PHYSICAL_PRICE_LIMIT_CACHE_ABI
                and ready.get("source_digest") == source_digest
                and ready.get("manifest_sha256") == _sha256(manifest_path)
                and all(
                    path.is_file()
                    and files.get(path.name, {}).get("bytes") == path.stat().st_size
                    and files.get(path.name, {}).get("sha256") == _sha256(path)
                    for path in paths.values()
                )
            ):
                manifest = candidate
        except (OSError, AttributeError, json.JSONDecodeError):
            manifest = None
        if manifest is None:
            lower, upper, security_types, counts = _price_limits(
                public_root=public_root,
                dates=dates,
                symbols=symbols,
                closes=closes,
            )
            root.mkdir(parents=True, exist_ok=True)
            for name, value in (
                ("lower", lower),
                ("upper", upper),
                ("security_types", security_types),
            ):
                _atomic_npy(paths[name], np.asarray(value))
            files = {
                path.name: {
                    "bytes": path.stat().st_size,
                    "sha256": _sha256(path),
                }
                for path in paths.values()
            }
            manifest = {
                "abi": PHYSICAL_PRICE_LIMIT_CACHE_ABI,
                "source_digest": source_digest,
                "rows": int(dates.size),
                "symbols": len(symbols),
                "counts": counts,
                "files": files,
            }
            _atomic_json(manifest_path, manifest)
            _atomic_json(
                ready_path,
                {
                    "abi": PHYSICAL_PRICE_LIMIT_CACHE_ABI,
                    "source_digest": source_digest,
                    "manifest_sha256": _sha256(manifest_path),
                    "verified_files": len(files),
                },
            )
            status = "built"
        else:
            status = "accepted"
        lower = np.load(paths["lower"], allow_pickle=False, mmap_mode="c")
        upper = np.load(paths["upper"], allow_pickle=False, mmap_mode="c")
        security_types = np.load(
            paths["security_types"], allow_pickle=False, mmap_mode="c"
        )
        if (
            lower.shape != closes.shape
            or upper.shape != closes.shape
            or security_types.shape != (len(symbols),)
            or lower.dtype != np.float64
            or upper.dtype != np.float64
            or security_types.dtype.kind not in {"U", "S"}
        ):
            raise RuntimeError("invalid physical price-limit cache arrays")
        print(
            "[physical source] price-limit cache "
            f"status={status} abi={PHYSICAL_PRICE_LIMIT_CACHE_ABI} "
            f"elapsed={time.monotonic() - started:.1f}s",
            flush=True,
        )
        return lower, upper, security_types, {
            str(name): int(value)
            for name, value in manifest["counts"].items()
        }


def _dense_bars(path: Path, symbols: tuple[str, ...]) -> tuple[np.ndarray, np.ndarray]:
    import polars as pl

    width = len(symbols)
    dense = np.full((width, 270, 6), np.nan, dtype=np.float64)
    dense[..., 5] = 0.0
    if not path.is_file():
        return dense, np.zeros(width, dtype=np.float64)
    lookup = pl.DataFrame(
        {"symbol": list(symbols), "_slot": np.arange(width, dtype=np.int32)}
    )
    frame = (
        pl.read_parquet(
            path,
            columns=[
                "symbol", "minutes_from_open", "Open", "High", "Low", "Close",
                "Amount", "volume_shares",
            ],
        )
        .with_columns(
            pl.col("symbol").cast(pl.String),
            pl.col("minutes_from_open").cast(pl.Int32, strict=False),
        )
        .filter(pl.col("minutes_from_open").is_between(1, 270, closed="both"))
        .join(lookup, on="symbol", how="inner", validate="m:1")
    )
    if frame.is_empty():
        return dense, np.zeros(width, dtype=np.float64)
    slots = frame.get_column("_slot").to_numpy().astype(np.int64, copy=False)
    minutes = (
        frame.get_column("minutes_from_open").to_numpy().astype(np.int64, copy=False)
        - 1
    )
    keys = slots * 270 + minutes
    if np.unique(keys).size != keys.size:
        counts = np.bincount(keys, minlength=width * 270)
        duplicate = int(np.flatnonzero(counts > 1)[0])
        slot, minute = divmod(duplicate, 270)
        raise ValueError(
            f"duplicate physical minute row: symbol={symbols[slot]} minute={minute + 1}"
        )
    columns = [
        frame.get_column(name).cast(pl.Float64, strict=False).to_numpy()
        for name in ("Open", "High", "Low", "Close", "Amount", "volume_shares")
    ]
    opens, highs, lows, closes, amounts, volumes = columns
    vwaps = np.where(
        np.isfinite(amounts) & np.isfinite(volumes) & (volumes > 0),
        amounts / np.maximum(volumes, np.finfo(np.float64).tiny),
        closes,
    )
    dense[slots, minutes] = np.column_stack(
        (opens, highs, lows, closes, vwaps, volumes)
    )
    valid_volume = np.where(np.isfinite(volumes) & (volumes > 0), volumes, 0.0)
    totals = np.bincount(slots, weights=valid_volume, minlength=width).astype(
        np.float64, copy=False
    )
    return dense, totals


def build_prepared_day_trade_carry_source(
    *, panel: Any, minute_root: str | Path, public_feature_path: str | Path,
    cache_dir: str | Path, allow_daily_proxy: bool,
    daily_proxy_price_policy: str, corporate_action_mode: str,
    terminal_liquidation_unlimited_capacity: bool = False,
    sparse_event_slots: int | None = None,
) -> PreparedDayTradeCarrySource:
    if corporate_action_mode != "avoid":
        raise ValueError(
            "physical carry source supports the requested avoid action policy only"
        )
    if daily_proxy_price_policy != "official_open_close":
        raise ValueError("physical source requires the official OPEN/CLOSE proxy policy")
    minute_path = Path(minute_root).resolve()
    public_path = _public_root(public_feature_path)
    dates = np.asarray(panel.dates, dtype="datetime64[D]").reshape(-1)
    symbols = tuple(str(symbol) for symbol in panel.symbols)
    raw_opens = np.asarray(panel.open_prices)
    raw_closes = np.asarray(panel.close_prices)
    opens = np.asarray(raw_opens, dtype=np.float64).copy()
    closes = np.asarray(raw_closes, dtype=np.float64).copy()
    volumes = np.asarray(panel.daily_volumes, dtype=np.float64)
    shape = (dates.size, len(symbols))
    if (
        not dates.size or np.isnat(dates).any() or np.any(dates[1:] <= dates[:-1])
        or opens.shape != shape or closes.shape != shape or volumes.shape != shape
    ):
        raise ValueError("physical source requires an aligned ordered daily panel")
    action = np.asarray(panel.corporate_action_avoidance_mask, dtype=bool)
    unresolved_action = np.asarray(
        panel.unresolved_corporate_action_mask, dtype=bool
    )
    force_exit = np.asarray(panel.force_exit_mask, dtype=bool)
    if (
        action.shape != shape
        or unresolved_action.shape != shape
        or force_exit.shape != shape
    ):
        raise ValueError("physical source requires exact action/terminal masks")
    (
        exact_action,
        exact_share_ratio,
        exact_cash,
        exact_payment_day,
        exact_stock_delivery_day,
        exact_action_counts,
    ) = (
        _exact_inventory_action_arrays(
            public_feature_path=Path(public_feature_path).resolve(),
            dates=dates,
            symbols=symbols,
        )
    )
    subscription_right_flat = np.asarray(
        exact_action_counts.pop("_subscription_right_flat_indices", ()),
        dtype=np.int64,
    )
    no_regular_execution, no_execution_counts = (
        _official_no_regular_execution_mask(
            public_root=public_path, dates=dates, symbols=symbols
        )
    )
    (
        replacement_halted,
        replacement_action,
        replacement_share_ratio,
        replacement_cash,
        replacement_payment_day,
        replacement_stock_delivery_day,
        unresolved_replacement_block,
        replacement_counts,
    ) = _share_replacement_arrays(
        public_root=public_path, dates=dates, symbols=symbols
    )
    overlap = exact_action & replacement_action
    if overlap.any():
        row, column = np.argwhere(overlap)[0]
        raise ValueError(
            "entitlement and share-replacement actions overlap: "
            f"date={dates[row]} symbol={symbols[column]}"
        )
    exact_action |= replacement_action
    exact_share_ratio = np.where(
        replacement_action, replacement_share_ratio, exact_share_ratio
    )
    exact_cash = np.where(replacement_action, replacement_cash, exact_cash)
    exact_payment_day = np.where(
        replacement_action, replacement_payment_day, exact_payment_day
    )
    exact_stock_delivery_day = np.where(
        replacement_action,
        replacement_stock_delivery_day,
        exact_stock_delivery_day,
    )
    no_regular_execution |= replacement_halted
    exact_action_counts = {
        **exact_action_counts,
        "mapped_events": (
            int(exact_action_counts["mapped_events"])
            + int(replacement_counts["mapped_exact_events"])
        ),
        "mapped_cash_events": (
            int(exact_action_counts["mapped_cash_events"])
            + int(replacement_counts["mapped_exact_cash_events"])
        ),
        "share_replacement": replacement_counts,
    }
    digest, receipts, minute_partitions, partition_scope = _source_identity(
        minute_root=minute_path, public_root=public_path, dates=dates,
        symbols=symbols, opens=opens, closes=closes, volumes=volumes,
        action_mask=action, unresolved_action_mask=unresolved_action,
        force_exit=force_exit, exact_action_mask=exact_action,
        exact_share_ratio=exact_share_ratio,
        exact_cash=exact_cash, exact_payment_day=exact_payment_day,
        exact_stock_delivery_day=exact_stock_delivery_day,
        no_regular_execution=no_regular_execution,
        unresolved_replacement_block=unresolved_replacement_block,
    )
    resume_compatible_release_ids: list[str] = []
    if subscription_right_flat.size:
        # V12 extends the physical objective only where v11 deterministically
        # failed on a held, source-verified pure subscription right. Recreate
        # the exact old content identity so a checkpoint from a trajectory
        # which had not entered that formerly undefined branch can resume. No
        # other data, feature, execution, fee, or model fingerprint is relaxed.
        predecessor_action = exact_action.copy()
        predecessor_ratio = exact_share_ratio.copy()
        predecessor_cash = exact_cash.copy()
        predecessor_payment = exact_payment_day.copy()
        predecessor_delivery = exact_stock_delivery_day.copy()
        predecessor_action.reshape(-1)[subscription_right_flat] = False
        predecessor_ratio.reshape(-1)[subscription_right_flat] = 1.0
        predecessor_cash.reshape(-1)[subscription_right_flat] = 0.0
        predecessor_payment.reshape(-1)[subscription_right_flat] = 0
        predecessor_delivery.reshape(-1)[subscription_right_flat] = 0
        predecessor_digest = _source_content_digest(
            source_cache_abi=_PREDECESSOR_PHYSICAL_SOURCE_CACHE_ABI,
            receipts=receipts,
            symbols=symbols,
            dates=dates,
            opens=opens,
            closes=closes,
            volumes=volumes,
            action_mask=action,
            unresolved_action_mask=unresolved_action,
            force_exit=force_exit,
            exact_action_mask=predecessor_action,
            exact_share_ratio=predecessor_ratio,
            exact_cash=predecessor_cash,
            exact_payment_day=predecessor_payment,
            exact_stock_delivery_day=predecessor_delivery,
            no_regular_execution=no_regular_execution,
            unresolved_replacement_block=unresolved_replacement_block,
        )
        resume_compatible_release_ids.append(
            f"tw-day-trade-carry:{predecessor_digest}"
        )
    cache_root = Path(cache_dir).resolve() / f"physical-{digest}"
    manifest_path = cache_root / "manifest.json"
    ready_path = cache_root / "READY.json"
    lock_path = cache_root.parent / f".{cache_root.name}.lock"
    cache_root.parent.mkdir(parents=True, exist_ok=True)
    partition_dates = sorted(minute_partitions)
    if not partition_dates:
        raise FileNotFoundError("physical minute source has no dated partitions")
    first_minute = partition_dates[0]
    if not allow_daily_proxy and bool((dates < first_minute).any()):
        raise ValueError("physical source forbids the required pre-minute daily proxy")

    lower, upper, security_types, limit_counts = _load_or_build_price_limit_cache(
        cache_dir=Path(cache_dir).resolve(),
        source_digest=digest,
        public_root=public_path,
        dates=dates,
        symbols=symbols,
        closes=closes,
    )
    # The daily panel intentionally stores float32. Preserve that source dtype
    # while validating the dated stock/ETF tick grid; converting first to
    # float64 loses the original ULP tolerance and turns a legal ETF quote such
    # as 35.77 into 35.770000457... off-grid. Once accepted, restore the exact
    # grid decimal for currency accounting rather than carrying storage noise.
    # Price-rule functions broadcast dates and security types.  Apply the
    # complete [session, symbol] grid once instead of dispatching 6,192 small
    # NumPy kernels from Python.  This is the same dated tick test and the same
    # cent-grid canonicalization, only with the loop moved into NumPy.
    date_grid = dates[:, None]
    security_grid = security_types[None, :]
    open_on_grid = price_on_tick_grid_numpy(
        raw_opens, date_grid, security_types=security_grid
    )
    close_on_grid = price_on_tick_grid_numpy(
        raw_closes, date_grid, security_types=security_grid
    )
    normalized_counts: list[int] = []
    for values, valid in (
        (opens, open_on_grid),
        (closes, close_on_grid),
    ):
        ticks = tick_size_numpy(
            values,
            date_grid,
            security_types=security_grid,
        )
        canonical = np.rint(values / ticks) * ticks
        canonical = np.rint(canonical * 100.0) / 100.0
        normalized_counts.append(int((valid & (canonical != values)).sum()))
        values[valid] = canonical[valid]
    normalized_open_cells, normalized_close_cells = normalized_counts
    last_close = np.full(len(symbols), np.nan, dtype=np.float64)
    opening_marks = np.full(shape, np.nan, dtype=np.float64)
    for row in range(dates.size):
        opening_marks[row] = np.where(
            np.isfinite(opens[row]) & (opens[row] > 0), opens[row], last_close
        )
        last_close = np.where(
            np.isfinite(closes[row]) & (closes[row] > 0), closes[row], last_close
        )

    source_started = time.monotonic()
    print(
        "[physical source] validating content-addressed cache "
        f"abi={PHYSICAL_SOURCE_CACHE_ABI} release=tw-day-trade-carry:{digest} ",
        flush=True,
    )
    with lock_path.open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        manifest: dict[str, Any] | None = None
        if manifest_path.is_file():
            try:
                candidate = json.loads(manifest_path.read_text(encoding="utf-8"))
                ready = json.loads(ready_path.read_text(encoding="utf-8"))
                if (
                    candidate.get("abi") == PHYSICAL_SOURCE_CACHE_ABI
                    and candidate.get("source_digest") == digest
                    and candidate.get("rows") == int(dates.size)
                    and candidate.get("symbols") == len(symbols)
                    and ready.get("abi") == PHYSICAL_SOURCE_CACHE_ABI
                    and ready.get("source_digest") == digest
                    and ready.get("manifest_sha256") == _sha256(manifest_path)
                    and ready.get("verified_files") == len(candidate.get("files", {}))
                ):
                    manifest = candidate
            except (OSError, json.JSONDecodeError, AttributeError):
                manifest = None
        gap_path = cache_root / "source_gaps.npy"
        unresolved_gap_path = cache_root / "unresolved_action_gaps.npy"
        action_path = cache_root / "exact_inventory_actions.npz"
        required = [gap_path, unresolved_gap_path, action_path]
        for day in dates:
            if day >= first_minute:
                day_text = np.datetime_as_string(day, unit="D")
                required.append(cache_root / f"session-{day_text}.npz")
        run_verification_receipt: Path | None = None
        verification_mode = "rebuilt"
        if manifest is not None:
            expected = manifest.get("files", {})
            accepted, run_verification_receipt, verification_mode = (
                _verify_physical_cache_files(
                    cache_root=cache_root,
                    manifest_path=manifest_path,
                    ready_path=ready_path,
                    source_digest=digest,
                    required=required,
                    expected=expected,
                )
            )
            if not accepted:
                manifest = None
        if manifest is None:
            cache_root.mkdir(parents=True, exist_ok=True)
            print(
                "[physical source] rebuilding exact hybrid sessions "
                f"rows={dates.size} symbols={len(symbols)} "
                f"minute_sessions={int((dates >= first_minute).sum())}",
                flush=True,
            )
            gaps = np.zeros(shape, dtype=np.bool_)
            unresolved_gaps = unresolved_replacement_block.copy()
            valuation_known = np.zeros(shape, dtype=np.bool_)
            # If an unresolved/terminal interval ends without liquidation, the
            # next session is an exact account-source failure for that held
            # symbol. A receipt-verified exact cash/stock action or a complete
            # pure subscription-right valuation is instead applied to the
            # residual FIFO cohorts below; it must never be called missing.
            if dates.size > 1:
                action_ends = action[:-1] & ~action[1:]
                unresolved_gaps[1:] |= action_ends & ~exact_action[1:]
                unresolved_gaps[1:] |= (
                    force_exit[:-1] & ~force_exit[1:]
                )
            files: dict[str, dict[str, Any]] = {}
            mode_counts = {
                "minute_symbol_days": 0,
                "daily_proxy_pre_minute_symbol_days": 0,
                "daily_proxy_post_minute_symbol_days": 0,
                "post_minute_missing_or_empty_rows": 0,
                "post_minute_volume_exceeds_official_day": 0,
                "post_minute_missing_historical_limits": 0,
                "official_no_regular_execution_symbol_days": int(
                    no_regular_execution.sum()
                ),
            }
            total_minute_sessions = int((dates >= first_minute).sum())
            minute_sessions_built = 0
            for row, day in enumerate(dates):
                valid_limits = (
                    np.isfinite(lower[row]) & np.isfinite(upper[row])
                    & (lower[row] > 0) & (upper[row] > lower[row])
                )
                active_daily = (
                    (np.isfinite(volumes[row]) & (volumes[row] > 0))
                    | (np.isfinite(opens[row]) & (opens[row] > 0))
                    | (np.isfinite(closes[row]) & (closes[row] > 0))
                )
                valid_proxy = (
                    np.isfinite(opens[row]) & (opens[row] > 0)
                    & np.isfinite(closes[row]) & (closes[row] > 0)
                    & np.isfinite(volumes[row]) & (volumes[row] >= 0)
                    & open_on_grid[row] & close_on_grid[row]
                )
                if day < first_minute:
                    gaps[row] |= (
                        active_daily & ~valid_proxy & ~no_regular_execution[row]
                    )
                    usable_proxy = valid_proxy & ~gaps[row]
                    carried_mark = (
                        no_regular_execution[row]
                        & (gaps[row] == 0)
                        & np.isfinite(opening_marks[row])
                        & (opening_marks[row] > 0)
                    )
                    valuation_known[row] = usable_proxy | carried_mark
                    mode_counts["daily_proxy_pre_minute_symbol_days"] += int(
                        usable_proxy.sum()
                    )
                    continue
                day_text = np.datetime_as_string(day, unit="D")
                partition = minute_partitions.get(day)
                if partition is None:
                    dense = np.full((len(symbols), 270, 6), np.nan, dtype=np.float64)
                    dense[..., 5] = 0.0
                    minute_total = np.zeros(len(symbols), dtype=np.float64)
                else:
                    partition_path, expected_sha256 = partition
                    if not partition_path.is_file():
                        raise FileNotFoundError(
                            f"accepted physical minute partition is missing: {partition_path}"
                        )
                    actual_sha256 = _sha256(partition_path)
                    if actual_sha256 != expected_sha256:
                        raise RuntimeError(
                            "physical minute partition differs from its manifest: "
                            f"date={day_text} expected={expected_sha256} actual={actual_sha256}"
                        )
                    dense, minute_total = _dense_bars(partition_path, symbols)
                official_volume_valid = (
                    np.isfinite(volumes[row]) & (volumes[row] >= 0)
                )
                # The retained KBars cover regular-session observations while
                # the official daily total includes additional mechanisms.
                # Equality is therefore neither expected nor meaningful.  A
                # positive minute sum may not, however, exceed the independent
                # official whole-day bound; such a symbol-day uses the explicit
                # daily proxy instead of overstating executable liquidity.
                volume_exceeds_day = (
                    official_volume_valid
                    & (minute_total > volumes[row] + 1.0)
                )
                missing_or_empty_minute = (
                    (partition is None) | (minute_total <= 0)
                )
                minute_candidate = (
                    ~missing_or_empty_minute
                    & official_volume_valid
                    & ~volume_exceeds_day
                    & valid_limits
                    & ~no_regular_execution[row]
                )
                use_proxy = (
                    ~minute_candidate & valid_proxy & ~no_regular_execution[row]
                )
                gaps[row] |= (
                    active_daily
                    & ~(minute_candidate | use_proxy)
                    & ~no_regular_execution[row]
                )
                good = minute_candidate & ~gaps[row]
                proxy = use_proxy & ~gaps[row]
                mode_counts["minute_symbol_days"] += int(good.sum())
                mode_counts["daily_proxy_post_minute_symbol_days"] += int(
                    proxy.sum()
                )
                mode_counts["post_minute_missing_or_empty_rows"] += int(
                    (proxy & missing_or_empty_minute).sum()
                )
                mode_counts["post_minute_volume_exceeds_official_day"] += int(
                    (proxy & volume_exceeds_day).sum()
                )
                mode_counts["post_minute_missing_historical_limits"] += int(
                    (proxy & ~valid_limits).sum()
                )
                path_array = np.full((len(symbols), 270, len(PATH_CHANNELS)), np.nan, dtype=np.float64)
                path_array[..., 2:4] = 0.0
                # price, source volume, explicit daily-proxy bit
                entry_array = np.full((len(symbols), 3), np.nan, dtype=np.float64)
                entry_array[:, 1] = 0.0
                entry_array[:, 2] = 0.0
                if good.any():
                    opportunities = paper_minute_opportunities(
                        dense[good], trading_date=day,
                        lower_limit=lower[row, good], upper_limit=upper[row, good],
                        security_types=security_types[good],
                    )
                    path_array[good, :, :2] = opportunities.prices
                    path_array[good, :, 2:4] = opportunities.capacity_shares
                    marks = opportunities.marks
                    leading = ~np.isfinite(marks) | (marks <= 0)
                    marks = np.where(leading, opening_marks[row, good, None], marks)
                    path_array[good, :, 4] = marks
                    entry_array[good, 0] = dense[good, 0, 4]
                    entry_array[good, 1] = dense[good, 0, 5]
                if proxy.any():
                    path_array[proxy, :, 4] = opens[row, proxy, None]
                    path_array[proxy, -1, 4] = closes[row, proxy]
                    path_array[proxy, -1, :2] = closes[row, proxy, None]
                    proxy_capacity = (
                        np.floor(
                            volumes[row, proxy]
                            / 271.0
                            * 0.5
                            / BOARD_LOT_SHARES
                        )
                        * BOARD_LOT_SHARES
                    )
                    path_array[proxy, -1, 2:4] = proxy_capacity[:, None]
                    entry_array[proxy, 0] = opens[row, proxy]
                    entry_array[proxy, 1] = volumes[row, proxy] / 271.0
                    entry_array[proxy, 2] = 1.0
                carried_mark = (
                    no_regular_execution[row]
                    & (gaps[row] == 0)
                    & np.isfinite(opening_marks[row])
                    & (opening_marks[row] > 0)
                )
                if carried_mark.any():
                    path_array[carried_mark, :, 4] = opening_marks[
                        row, carried_mark, None
                    ]
                valuation_known[row] = good | proxy | carried_mark
                exit_view = path_array[..., :2].reshape(-1)
                exit_flat = np.flatnonzero(np.isfinite(exit_view)).astype(
                    np.int32, copy=False
                )
                mark_view = path_array[..., 4].reshape(-1)
                mark_flat = np.flatnonzero(np.isfinite(mark_view)).astype(
                    np.int32, copy=False
                )
                output = cache_root / f"session-{day_text}.npz"
                _atomic_npz(
                    output,
                    exit_flat=exit_flat,
                    exit_price=exit_view[exit_flat].astype(np.float64, copy=False),
                    exit_capacity=path_array[..., 2:4].reshape(-1)[exit_flat].astype(
                        np.float64, copy=False
                    ),
                    mark_flat=mark_flat,
                    mark=mark_view[mark_flat].astype(np.float64, copy=False),
                    entry=entry_array,
                )
                files[output.name] = {
                    "bytes": output.stat().st_size, "sha256": _sha256(output)
                }
                minute_sessions_built += 1
                if minute_sessions_built % 100 == 0:
                    print(
                        "[physical source] rebuild progress "
                        f"minute_sessions={minute_sessions_built}/"
                        f"{total_minute_sessions} "
                        f"elapsed={time.monotonic() - source_started:.1f}s",
                        flush=True,
                    )
            # Any internal absence that is not explained by a positive official
            # halt/action source is unsafe for potentially carried inventory.
            # A model-independent preflight cannot assume the 50%-volume exit
            # will flatten a prior position, so exclude acquisition from the
            # empty initial state through the last such gap for each symbol.
            previously_known = np.maximum.accumulate(valuation_known, axis=0)
            later_known = np.maximum.accumulate(
                valuation_known[::-1], axis=0
            )[::-1]
            internal_unknown = (
                previously_known & later_known & ~valuation_known
            )
            row_numbers = np.arange(dates.size, dtype=np.int64)[:, None]
            last_unknown = np.where(
                internal_unknown,
                row_numbers,
                -1,
            ).max(axis=0)
            internal_prefix_block = row_numbers <= last_unknown[None, :]
            unresolved_gaps |= internal_prefix_block
            _atomic_npy(gap_path, gaps)
            files[gap_path.name] = {
                "bytes": gap_path.stat().st_size, "sha256": _sha256(gap_path)
            }
            _atomic_npy(unresolved_gap_path, unresolved_gaps)
            files[unresolved_gap_path.name] = {
                "bytes": unresolved_gap_path.stat().st_size,
                "sha256": _sha256(unresolved_gap_path),
            }
            action_flat = np.flatnonzero(exact_action.reshape(-1)).astype(
                np.int64, copy=False
            )
            _atomic_npz(
                action_path,
                action_flat=action_flat,
                total_share_ratio=exact_share_ratio.reshape(-1)[action_flat].astype(
                    np.float64, copy=False
                ),
                cash_per_old_share=exact_cash.reshape(-1)[action_flat].astype(
                    np.float64, copy=False
                ),
                payment_day=exact_payment_day.reshape(-1)[action_flat].astype(
                    np.int64, copy=False
                ),
                stock_delivery_day=exact_stock_delivery_day.reshape(-1)[
                    action_flat
                ].astype(np.int64, copy=False),
            )
            files[action_path.name] = {
                "bytes": action_path.stat().st_size,
                "sha256": _sha256(action_path),
            }
            manifest = {
                "abi": PHYSICAL_SOURCE_CACHE_ABI,
                "schedule_abi": PAPER_MINUTE_SCHEDULE_ABI,
                "source_digest": digest,
                "release_id": f"tw-day-trade-carry:{digest}",
                "resume_compatible_release_ids": resume_compatible_release_ids,
                "rows": int(dates.size), "symbols": len(symbols),
                "date_start": str(dates[0]), "date_end": str(dates[-1]),
                "first_minute_date": str(first_minute),
                "daily_proxy": "official_open_close_without_adverse_tick",
                "daily_proxy_capacity": "floor(daily_volume/271*0.5/1000)*1000",
                "daily_proxy_intraday_marks": "official_open_carried_to_official_close_not_observed_minutes",
                "daily_proxy_source_precision": {
                    "validation": "preserve_panel_source_dtype_ulp_then_require_dated_tick_grid",
                    "accounting": "canonical_dated_tick_decimal_float64",
                    "normalized_open_cells": normalized_open_cells,
                    "normalized_close_cells": normalized_close_cells,
                },
                "minute_volume_reconciliation": "regular_session_positive_volume_must_not_exceed_official_whole_day_volume_else_daily_proxy",
                "session_encoding": "sorted_flat_sparse_exit_mark_and_exact_inventory_uncompressed_npz",
                "corporate_action_policy": {
                    "policy_target": "avoid_all_announced_actions_before_event",
                    "residual_cash_entitlement": "receipt_verified_exact_amount_and_payment_date",
                    "residual_stock_entitlement": "economic_on_ex_date_and_nonexecutable_until_official_delivery",
                    "residual_subscription_right": (
                        "source_complete_pure_right_settled_on_ex_date_at_"
                        "official_reference_value_symmetrically_for_long_and_short"
                    ),
                    "unresolved_residual": "fail_closed_per_symbol_on_first_post_event_session",
                    **exact_action_counts,
                },
                "official_no_regular_execution": {
                    **no_execution_counts,
                    "share_replacement_symbol_days": int(
                        replacement_halted.sum()
                    ),
                    "combined_total_symbol_days": int(
                        no_regular_execution.sum()
                    ),
                    "valuation": "carry_previous_observable_regular_market_mark",
                    "entry_exit_capacity": "zero",
                    "absent_official_row": "not_classified_and_never_auto_filled",
                },
                "twse_etf_limit_policy": "official_daily_proxy_without_fabricated_limit_when_historical_limit_receipt_is_missing",
                "mode_counts": mode_counts,
                "minute_partition_scope": partition_scope,
                "limit_counts": limit_counts, "source_receipts": receipts,
                "source_gap_symbol_days": int(gaps.sum()),
                "unresolved_action_gap_symbol_days": int(unresolved_gaps.sum()),
                "internal_unknown_valuation": {
                    "symbol_days": int(internal_unknown.sum()),
                    "symbols": int(internal_unknown.any(axis=0).sum()),
                    "prefix_block_symbol_days": int(
                        internal_prefix_block.sum()
                    ),
                    "policy": (
                        "empty_initial_state_prefix_exclusion_through_last_"
                        "unclassified_internal_gap"
                    ),
                },
                "files": files,
            }
            _atomic_json(manifest_path, manifest)
            _atomic_json(
                ready_path,
                {
                    "abi": PHYSICAL_SOURCE_CACHE_ABI,
                    "source_digest": digest,
                    "manifest_sha256": _sha256(manifest_path),
                    "verified_files": len(files),
                    "verified_bytes": int(
                        sum(int(item["bytes"]) for item in files.values())
                    ),
                    "verification": "sha256_each_file_at_atomic_build",
                },
            )
            run_verification_receipt = _write_run_verification_receipt(
                cache_root=cache_root,
                manifest_path=manifest_path,
                ready_path=ready_path,
                source_digest=digest,
                required=required,
            )
        if run_verification_receipt is None:
            raise RuntimeError(
                "physical source cache lacks a run-scoped verification receipt"
            )
        gaps = np.load(gap_path, allow_pickle=False, mmap_mode="r")
        if gaps.shape != shape or gaps.dtype != np.dtype(bool):
            raise RuntimeError("invalid physical source gap cache")
        unresolved_gaps = np.load(
            unresolved_gap_path, allow_pickle=False, mmap_mode="r"
        )
        if unresolved_gaps.shape != shape or unresolved_gaps.dtype != np.dtype(bool):
            raise RuntimeError("invalid unresolved corporate-action gap cache")
        sparse_capacity_audit = None
        if sparse_event_slots is not None:
            if (
                sparse_event_slots <= 0
                or sparse_event_slots & (sparse_event_slots - 1)
            ):
                raise ValueError("sparse_event_slots must be a positive power of two")
            proxy_rows = dates < first_minute
            proxy_executable = (
                np.isfinite(opens[proxy_rows])
                & (opens[proxy_rows] > 0)
                & np.isfinite(closes[proxy_rows])
                & (closes[proxy_rows] > 0)
                & ~np.asarray(gaps[proxy_rows])
            )
            proxy_max = (
                2 * int(proxy_executable.sum(axis=1).max())
                if proxy_executable.shape[0]
                else 0
            )
            minute_max = 0
            minute_max_file = None
            session_files = sorted(
                name for name in manifest["files"]
                if name.startswith("session-") and name.endswith(".npz")
            )
            for name in session_files:
                with np.load(cache_root / name, allow_pickle=False) as packed:
                    flat = packed["exit_flat"]
                if flat.ndim != 1:
                    raise RuntimeError(f"invalid sparse event shape: {name}")
                if flat.size > minute_max:
                    minute_max = int(flat.size)
                    minute_max_file = name
            required_slots = max(1, proxy_max, minute_max)
            if required_slots > sparse_event_slots:
                raise ValueError(
                    "sparse event session exceeds the fixed compiled ABI before "
                    f"training: required={required_slots} configured={sparse_event_slots} "
                    f"max_minute_file={minute_max_file}; increase "
                    "training.day_trade_sparse_event_slots to the next power of two"
                )
            sparse_capacity_audit = {
                "configured_slots": int(sparse_event_slots),
                "required_slots": required_slots,
                "daily_proxy_max_events": proxy_max,
                "minute_max_events": minute_max,
                "minute_max_file": minute_max_file,
                "session_files_checked": len(session_files),
            }
            print(
                "[physical source] sparse event capacity accepted "
                f"required={required_slots} configured={sparse_event_slots} "
                f"minute_files={len(session_files)} max_file={minute_max_file}",
                flush=True,
            )
        (
            action_flat,
            action_ratio,
            action_cash,
            action_payment,
            action_delivery,
        ) = _load_canonical_sparse_exact_inventory_cache(
            action_path,
            exact_action_mask=exact_action,
            exact_share_ratio=exact_share_ratio,
            exact_cash=exact_cash,
            exact_payment_day=exact_payment_day,
            exact_stock_delivery_day=exact_stock_delivery_day,
        )
        print(
            "[physical source] accepted cache "
            f"manifest_sha256={_sha256(manifest_path)} "
            f"verification={verification_mode} "
            f"minute_symbol_days={manifest['mode_counts']['minute_symbol_days']} "
            "daily_proxy_symbol_days="
            f"{manifest['mode_counts']['daily_proxy_pre_minute_symbol_days'] + manifest['mode_counts']['daily_proxy_post_minute_symbol_days']} "
            f"source_gap_symbol_days={manifest['source_gap_symbol_days']} "
            f"elapsed={time.monotonic() - source_started:.1f}s",
            flush=True,
        )

    # Re-decoding the same immutable NPZ on every one of 1000 epochs is not a
    # semantic operation. Enable an all-or-nothing per-rank RAM cache only when
    # the complete release fits a conservative, topology-aware budget. Partial
    # chronological LRU caching would thrash from opposite ends each epoch.
    vector_fields = 15
    estimated_session_bytes = int(
        len(symbols) * (vector_fields + 5 * 270) * np.dtype(np.float64).itemsize
    )
    estimated_cache_bytes = estimated_session_bytes * len(dates)
    cache_setting = os.environ.get(
        "STOCKAGENT_DAY_TRADE_SOURCE_CACHE_GIB", "auto"
    ).strip().lower()
    if cache_setting == "auto":
        available_bytes = 0
        try:
            with Path("/proc/meminfo").open("r", encoding="utf-8") as handle:
                for line in handle:
                    if line.startswith("MemAvailable:"):
                        available_bytes = int(line.split()[1]) * 1024
                        break
        except OSError:
            available_bytes = 0
        world_size = max(1, int(os.environ.get("WORLD_SIZE", "1")))
        reserve_bytes = min(
            64 * 1024**3,
            max(8 * 1024**3, available_bytes // 4),
        )
        cache_budget_bytes = min(
            128 * 1024**3,
            max(0, available_bytes - reserve_bytes) // world_size,
        )
    else:
        try:
            cache_budget_bytes = int(float(cache_setting) * 1024**3)
        except ValueError as exc:
            raise ValueError(
                "STOCKAGENT_DAY_TRADE_SOURCE_CACHE_GIB must be auto or nonnegative"
            ) from exc
        if cache_budget_bytes < 0:
            raise ValueError(
                "STOCKAGENT_DAY_TRADE_SOURCE_CACHE_GIB must be auto or nonnegative"
            )
    cache_enabled = estimated_cache_bytes <= cache_budget_bytes
    session_cache: dict[int, DayTradeCarrySession] = {}
    compact_session_cache: dict[int, DayTradeCarrySession] = {}
    packed_session_cache: dict[int, PackedDayTradeCarrySession] = {}
    session_cache_lock = threading.Lock()
    print(
        "[physical source] dense RAM cache "
        f"enabled={str(cache_enabled).lower()} "
        f"estimated={estimated_cache_bytes / 1024**3:.1f}GiB "
        f"budget={cache_budget_bytes / 1024**3:.1f}GiB",
        flush=True,
    )

    def _load_packed_session_uncached(row: int) -> PackedDayTradeCarrySession:
        day = dates[row]
        day_text = np.datetime_as_string(day, unit="D")
        gap = np.asarray(gaps[row], dtype=np.float64)
        unresolved_gap = np.asarray(unresolved_gaps[row], dtype=np.float64)
        action_mask = np.zeros(len(symbols), dtype=np.float64)
        share_ratio = np.ones(len(symbols), dtype=np.float64)
        cash_per_old_share = np.zeros(len(symbols), dtype=np.float64)
        payment_day = np.zeros(len(symbols), dtype=np.float64)
        stock_delivery_day = np.zeros(len(symbols), dtype=np.float64)
        flat_start = row * len(symbols)
        event_start = int(np.searchsorted(action_flat, flat_start, side="left"))
        event_stop = int(
            np.searchsorted(
                action_flat, flat_start + len(symbols), side="left"
            )
        )
        if event_stop > event_start:
            event_columns = (
                action_flat[event_start:event_stop] - flat_start
            ).astype(np.int64, copy=False)
            action_mask[event_columns] = 1.0
            share_ratio[event_columns] = action_ratio[event_start:event_stop]
            cash_per_old_share[event_columns] = action_cash[event_start:event_stop]
            payment_day[event_columns] = action_payment[event_start:event_stop]
            stock_delivery_day[event_columns] = action_delivery[
                event_start:event_stop
            ]
        if day < first_minute:
            marks = np.full((len(symbols), 270), np.nan, dtype=np.float64)
            executable = (
                np.isfinite(opens[row]) & (opens[row] > 0)
                & np.isfinite(closes[row]) & (closes[row] > 0)
                & (gap == 0)
            )
            capacity = (
                np.floor(np.maximum(volumes[row], 0) / 271.0 * 0.5 / BOARD_LOT_SHARES)
                * BOARD_LOT_SHARES
            )
            marks[executable] = opening_marks[row, executable, None]
            marks[executable, -1] = closes[row, executable]
            carried_mark = (
                no_regular_execution[row]
                & (gap == 0)
                & np.isfinite(opening_marks[row])
                & (opening_marks[row] > 0)
            )
            marks[carried_mark] = opening_marks[row, carried_mark, None]
            entry_price = np.where(executable, opens[row], np.nan)
            entry_volume = np.where(executable, np.maximum(volumes[row], 0) / 271.0, 0)
            daily_proxy_mask = executable.astype(np.float64, copy=False)
            # The proxy owns only the two final-minute exit sides. Construct
            # their sorted flat identities directly; the dense tape would be
            # allocated solely to discover these exact same cells.
            executable_symbols = np.flatnonzero(executable).astype(np.int64)
            exit_flat = np.repeat(executable_symbols * 540 + 538, 2)
            exit_flat[1::2] += 1
            exit_value = np.repeat(closes[row, executable], 2)
            exit_cap = np.repeat(capacity[executable], 2)
        else:
            with np.load(
                cache_root / f"session-{day_text}.npz", allow_pickle=False
            ) as packed:
                if set(packed.files) != {
                    "exit_flat", "exit_price", "exit_capacity", "mark_flat",
                    "mark", "entry",
                }:
                    raise RuntimeError(f"invalid physical session fields: {day_text}")
                exit_flat = np.array(packed["exit_flat"], copy=True)
                exit_value = np.array(packed["exit_price"], copy=True)
                exit_cap = np.array(packed["exit_capacity"], copy=True)
                mark_flat = np.array(packed["mark_flat"], copy=True)
                mark_value = np.array(packed["mark"], copy=True)
                entry = np.array(packed["entry"], copy=True)
            exit_size = len(symbols) * 270 * 2
            mark_size = len(symbols) * 270
            if (
                exit_flat.dtype != np.int32
                or mark_flat.dtype != np.int32
                or exit_value.dtype != np.float64
                or exit_cap.dtype != np.float64
                or mark_value.dtype != np.float64
                or entry.dtype != np.float64
                or exit_flat.ndim != 1
                or mark_flat.ndim != 1
                or exit_value.shape != exit_flat.shape
                or exit_cap.shape != exit_flat.shape
                or mark_value.shape != mark_flat.shape
                or entry.shape != (len(symbols), 3)
                or (exit_flat.size and (
                    exit_flat[0] < 0 or exit_flat[-1] >= exit_size
                    or np.any(exit_flat[1:] <= exit_flat[:-1])
                ))
                or (mark_flat.size and (
                    mark_flat[0] < 0 or mark_flat[-1] >= mark_size
                    or np.any(mark_flat[1:] <= mark_flat[:-1])
                ))
                or not np.isfinite(exit_value).all()
                or not np.isfinite(exit_cap).all()
                or not np.isfinite(mark_value).all()
            ):
                raise RuntimeError(f"invalid physical sparse session cache: {day_text}")
            marks = np.full((len(symbols), 270), np.nan, dtype=np.float64)
            marks.reshape(-1)[mark_flat] = mark_value
            entry_price, entry_volume = entry[:, 0], entry[:, 1]
            daily_proxy_mask = entry[:, 2]
        tensor = lambda value: torch.from_numpy(np.asarray(value, dtype=np.float64))
        terminal_liquidation_price = None
        if terminal_liquidation_unlimited_capacity:
            terminal_price_valid = (
                np.isfinite(closes[row])
                & (closes[row] > 0)
                & (gap == 0)
                & ~no_regular_execution[row]
            )
            terminal_liquidation_price = tensor(
                np.where(terminal_price_valid, closes[row], np.nan)
            )
        packed_session = PackedDayTradeCarrySession(
            day=_ordinal(day), official_open=tensor(opens[row]),
            opening_marks=tensor(opening_marks[row]), entry_price=tensor(entry_price),
            entry_volume=tensor(entry_volume), lower_limit=tensor(lower[row]),
            upper_limit=tensor(upper[row]),
            halted=tensor(no_regular_execution[row]),
            exit_flat=torch.from_numpy(
                np.asarray(exit_flat, dtype=np.int64)
            ),
            exit_price=tensor(exit_value),
            exit_capacity=tensor(exit_cap),
            marks=tensor(marks), action_mask=tensor(action_mask),
            share_ratio=tensor(share_ratio),
            cash_per_old_share=tensor(cash_per_old_share),
            payment_day=tensor(payment_day),
            stock_delivery_day=tensor(stock_delivery_day),
            source_gap_mask=tensor(gap),
            unresolved_action_gap_mask=tensor(unresolved_gap),
            daily_proxy_mask=tensor(daily_proxy_mask),
            terminal_liquidation_price=terminal_liquidation_price,
        )
        packed_session.validate()
        return packed_session

    def _densify_packed_session(
        packed: PackedDayTradeCarrySession,
    ) -> DayTradeCarrySession:
        symbols_count = int(packed.official_open.numel())
        exit_size = symbols_count * 270 * 2
        exit_prices = torch.full((exit_size,), float("nan"), dtype=torch.float64)
        exit_capacity = torch.zeros((exit_size,), dtype=torch.float64)
        if packed.exit_flat.numel():
            exit_prices.index_copy_(0, packed.exit_flat, packed.exit_price)
            exit_capacity.index_copy_(0, packed.exit_flat, packed.exit_capacity)
        return DayTradeCarrySession(
            day=packed.day,
            official_open=packed.official_open,
            opening_marks=packed.opening_marks,
            entry_price=packed.entry_price,
            entry_volume=packed.entry_volume,
            lower_limit=packed.lower_limit,
            upper_limit=packed.upper_limit,
            halted=packed.halted,
            exit_prices=exit_prices.reshape(symbols_count, 270, 2),
            exit_capacity=exit_capacity.reshape(symbols_count, 270, 2),
            marks=packed.marks,
            action_mask=packed.action_mask,
            share_ratio=packed.share_ratio,
            cash_per_old_share=packed.cash_per_old_share,
            payment_day=packed.payment_day,
            stock_delivery_day=packed.stock_delivery_day,
            source_gap_mask=packed.source_gap_mask,
            unresolved_action_gap_mask=packed.unresolved_action_gap_mask,
            daily_proxy_mask=packed.daily_proxy_mask,
            terminal_liquidation_price=packed.terminal_liquidation_price,
        )

    def _load_session_uncached(row: int) -> DayTradeCarrySession:
        return _densify_packed_session(_load_packed_session_uncached(row))

    def load_session(row: int) -> DayTradeCarrySession:
        if cache_enabled:
            with session_cache_lock:
                cached = session_cache.get(int(row))
            if cached is not None:
                return cached
        session = _load_session_uncached(int(row))
        if cache_enabled:
            with session_cache_lock:
                # A concurrent evaluator may have won the same immutable row.
                session = session_cache.setdefault(int(row), session)
        return session

    def load_packed_session(row: int) -> PackedDayTradeCarrySession:
        index = int(row)
        with session_cache_lock:
            cached = packed_session_cache.get(index)
        if cached is not None:
            return cached
        packed = _load_packed_session_uncached(index)
        with session_cache_lock:
            packed = packed_session_cache.setdefault(index, packed)
        return packed

    def load_compact_session(row: int) -> DayTradeCarrySession:
        index = int(row)
        with session_cache_lock:
            cached = compact_session_cache.get(index)
        if cached is not None:
            return cached
        # Preserve the immutable packed source instead of constructing a
        # [symbol,270,2] NaN/zero tape merely to remove it immediately.
        compact = compact_packed_day_trade_carry_session(
            _load_packed_session_uncached(index)
        )
        with session_cache_lock:
            compact = compact_session_cache.setdefault(index, compact)
        return compact

    return PreparedDayTradeCarrySource(
        (), symbols, str(manifest["release_id"]),
        session_days=tuple(_ordinal(day) for day in dates), session_loader=load_session,
        compact_session_loader=load_compact_session,
        packed_session_loader=load_packed_session,
        audit_receipt={
            "abi": manifest["abi"],
            "cache_manifest": str(manifest_path),
            "cache_manifest_sha256": _sha256(manifest_path),
            "ready_receipt": str(ready_path),
            "ready_receipt_sha256": _sha256(ready_path),
            "run_verification_receipt": str(run_verification_receipt),
            "run_verification_mode": verification_mode,
            "resume_compatible_release_ids": list(
                manifest.get("resume_compatible_release_ids", ())
            ),
            "source_gap_symbol_days": int(manifest["source_gap_symbol_days"]),
            "unresolved_action_gap_symbol_days": int(
                manifest["unresolved_action_gap_symbol_days"]
            ),
            "mode_counts": manifest["mode_counts"],
            "corporate_action_policy": manifest["corporate_action_policy"],
            "official_no_regular_execution": manifest[
                "official_no_regular_execution"
            ],
            "internal_unknown_valuation": manifest[
                "internal_unknown_valuation"
            ],
            "daily_proxy_source_precision": manifest[
                "daily_proxy_source_precision"
            ],
            "minute_partition_scope": manifest["minute_partition_scope"],
            "limit_counts": manifest["limit_counts"],
            **(
                {"sparse_event_capacity": sparse_capacity_audit}
                if sparse_capacity_audit is not None
                else {}
            ),
            "terminal_liquidation_policy": (
                "official_close_unlimited_capacity_reduction_only"
                if terminal_liquidation_unlimited_capacity
                else "source_minute_capacity_then_physical_margin_carry"
            ),
        },
    )


__all__ = [
    "PHYSICAL_SOURCE_CACHE_ABI",
    "PHYSICAL_SOURCE_RUN_RECEIPT_ENV",
    "build_prepared_day_trade_carry_source",
]

from __future__ import annotations

import argparse
from datetime import date, datetime, timedelta, timezone
import hashlib
import json
import math
from pathlib import Path
from typing import Any

import polars as pl


SHIOAJI_SOURCE = "shioaji_kbars_1m"
STORAGE_FREQUENCY = "daily"
HYBRID_SOURCE = "tw_public_before_shioaji_after"
ALLOWED_DOWNLOAD_STATUSES = {
    "complete",
    "contract_unavailable",
    "not_yet_listed",
    "outside_source_window",
}
ALLOWED_BUILD_STATUSES = {
    "hybrid",
    "public_only_contract_unavailable",
    "public_only_not_yet_listed",
    "public_only_outside_source_window",
}
QUOTE_COLUMNS = ("open", "max", "min", "close", "Trading_Volume")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Fail-closed audit for the receipt-backed Shioaji download and the "
            "public-before/Shioaji-after per-symbol hybrid dataset."
        )
    )
    parser.add_argument(
        "--base-stock-root", type=Path, default=Path("data_tw_public/stocks")
    )
    parser.add_argument(
        "--shioaji-root", type=Path, default=Path("data_tw_public/shioaji")
    )
    parser.add_argument(
        "--dataset-root", type=Path, default=Path("data_tw_public/shioaji/stocks")
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("artifacts/data_quality/tw_shioaji_audit.json"),
    )
    parser.add_argument(
        "--skip-chunk-checksums",
        action="store_true",
        help="Faster diagnostic only; final completion audit must not use this.",
    )
    return parser.parse_args()


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise RuntimeError(f"unreadable JSON {path}: {type(exc).__name__}: {exc}") from exc
    if not isinstance(payload, dict):
        raise RuntimeError(f"JSON root must be an object: {path}")
    return payload


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _receipt_matches(path: Path, receipt: Any, *, checksum: bool = True) -> bool:
    if not isinstance(receipt, dict) or not path.is_file():
        return False
    try:
        if int(receipt.get("size", -1)) != int(path.stat().st_size):
            return False
        return not checksum or str(receipt.get("sha256", "")) == _sha256(path)
    except (OSError, TypeError, ValueError):
        return False


def _report_map(path: Path) -> dict[str, dict[str, Any]]:
    frame = pl.read_csv(path, infer_schema_length=0)
    if not {"symbol", "status"} <= set(frame.columns):
        raise RuntimeError(f"report lacks symbol/status columns: {path}")
    symbols = [str(value).strip().upper() for value in frame["symbol"]]
    if len(symbols) != len(set(symbols)):
        raise RuntimeError(f"report contains duplicate symbols: {path}")
    return {
        str(row["symbol"]).strip().upper(): row
        for row in frame.iter_rows(named=True)
    }


def _manifest_symbols(path: Path) -> set[str]:
    frame = pl.read_csv(path, infer_schema_length=0)
    required = {"code", "security_type"}
    if not required <= set(frame.columns):
        raise RuntimeError(f"symbol manifest lacks {sorted(required)}: {path}")
    return {
        str(row["code"]).strip().upper()
        for row in frame.iter_rows(named=True)
        if str(row["security_type"]).strip().lower() in {"stock", "etf"}
    }


def _audit_daily_chunks(
    root: Path,
    symbol: str,
    daily_summary: dict[str, Any],
    *,
    verify_checksum: bool,
) -> tuple[int, int]:
    chunk_root = root / "daily_chunks" / symbol
    receipt_paths = sorted(chunk_root.glob("*.receipt.json"))
    expected_count = int(daily_summary.get("chunks", -1))
    if len(receipt_paths) != expected_count:
        raise RuntimeError(
            f"{symbol}: daily chunk receipt count={len(receipt_paths)} "
            f"expected={expected_count}"
        )
    requested_start = date.fromisoformat(str(daily_summary["requested_start"]))
    requested_end = date.fromisoformat(str(daily_summary["requested_end"]))
    ranges: list[tuple[date, date]] = []
    source_minute_rows = 0
    daily_rows = 0
    ok_receipts = 0
    daily_frames: list[pl.DataFrame] = []
    for receipt_path in receipt_paths:
        receipt = _read_json(receipt_path)
        if not (
            receipt.get("source") == SHIOAJI_SOURCE
            and receipt.get("storage_frequency") == STORAGE_FREQUENCY
            and receipt.get("symbol") == symbol
            and receipt.get("status") in {"ok", "empty"}
        ):
            raise RuntimeError(
                f"{symbol}: invalid daily chunk receipt identity: {receipt_path}"
            )
        start = date.fromisoformat(str(receipt["start_date"]))
        end = date.fromisoformat(str(receipt["end_date"]))
        if start > end or (end - start).days >= 30:
            raise RuntimeError(
                f"{symbol}: invalid daily chunk receipt range {start}..{end}"
            )
        ranges.append((start, end))
        rows = int(receipt.get("rows", -1))
        receipt_daily_rows = int(receipt.get("daily_rows", -1))
        receipt_source_rows = int(receipt.get("source_minute_rows", -1))
        if receipt["status"] == "empty":
            if (
                rows != 0
                or receipt_daily_rows != 0
                or receipt_source_rows != 0
                or int(receipt.get("expected_positive_volume_sessions", -1)) != 0
            ):
                raise RuntimeError(f"{symbol}: invalid empty receipt: {receipt_path}")
            continue
        chunk_path = chunk_root / (
            receipt_path.name.removesuffix(".receipt.json") + ".parquet"
        )
        if not _receipt_matches(
            chunk_path,
            receipt.get("output_receipt"),
            checksum=verify_checksum,
        ):
            raise RuntimeError(
                f"{symbol}: daily chunk output receipt mismatch: {chunk_path}"
            )
        chunk = pl.read_parquet(chunk_path)
        actual_rows = chunk.height
        if int(actual_rows) != rows or rows != receipt_daily_rows:
            raise RuntimeError(
                f"{symbol}: daily chunk row mismatch receipt={rows} "
                f"daily_rows={receipt_daily_rows} actual={actual_rows}"
            )
        if chunk.get_column("date").n_unique() != chunk.height:
            raise RuntimeError(f"{symbol}: duplicate dates inside daily chunk")
        if int(chunk.get_column("shioaji_minute_bars").sum()) != receipt_source_rows:
            raise RuntimeError(f"{symbol}: source minute count mismatch in daily chunk")
        source_minute_rows += receipt_source_rows
        daily_rows += rows
        daily_frames.append(chunk)
        ok_receipts += 1
    ranges.sort()
    if ranges:
        if ranges[0][0] != requested_start or ranges[-1][1] != requested_end:
            raise RuntimeError(
                f"{symbol}: daily chunk coverage boundary mismatch "
                f"{ranges[0][0]}..{ranges[-1][1]} vs "
                f"{requested_start}..{requested_end}"
            )
        for previous, current in zip(ranges, ranges[1:], strict=False):
            if previous[1] + timedelta(days=1) != current[0]:
                raise RuntimeError(
                    f"{symbol}: non-contiguous daily chunk ranges "
                    f"{previous} then {current}"
                )
    if source_minute_rows != int(daily_summary.get("source_minute_rows", -1)):
        raise RuntimeError(
            f"{symbol}: source minute row reconciliation failed "
            f"{source_minute_rows} vs {daily_summary.get('source_minute_rows')}"
        )
    if daily_rows != int(daily_summary.get("daily_rows", -1)):
        raise RuntimeError(
            f"{symbol}: daily chunk row reconciliation failed {daily_rows} vs "
            f"{daily_summary.get('daily_rows')}"
        )
    combined = (
        pl.concat(daily_frames, how="vertical_relaxed").sort("date")
        if daily_frames
        else pl.DataFrame()
    )
    final_daily = pl.read_parquet(root / "daily" / f"{symbol}.parquet").sort("date")
    if combined.height != final_daily.height or (
        combined.height
        and combined.get_column("date").to_list()
        != final_daily.get_column("date").to_list()
    ):
        raise RuntimeError(f"{symbol}: final daily file differs from daily chunks")
    for column in QUOTE_COLUMNS:
        if combined.height and not _allclose(combined[column], final_daily[column]):
            raise RuntimeError(f"{symbol}: final daily {column} differs from chunks")
    return len(receipt_paths), ok_receipts


def _allclose(left: pl.Series, right: pl.Series) -> bool:
    if len(left) != len(right):
        return False
    for a, b in zip(left, right, strict=True):
        try:
            if not math.isclose(float(a), float(b), rel_tol=1e-12, abs_tol=1e-9):
                return False
        except (TypeError, ValueError):
            return False
    return True


def audit(
    *,
    base_stock_root: Path,
    shioaji_root: Path,
    dataset_root: Path,
    verify_chunk_checksums: bool,
) -> dict[str, Any]:
    legacy_raw_root = shioaji_root / "raw"
    if legacy_raw_root.is_dir() and any(path.is_file() for path in legacy_raw_root.rglob("*")):
        raise RuntimeError(
            "minute-level raw files are forbidden in the daily Shioaji dataset"
        )
    base_symbols = _manifest_symbols(base_stock_root / "symbols.csv")
    output_symbols = _manifest_symbols(dataset_root / "symbols.csv")
    if base_symbols != output_symbols:
        raise RuntimeError(
            f"dataset symbol manifest mismatch missing={sorted(base_symbols-output_symbols)[:20]} "
            f"extra={sorted(output_symbols-base_symbols)[:20]}"
        )
    download_summary = _read_json(shioaji_root / "download_summary.json")
    if not (
        download_summary.get("source") == SHIOAJI_SOURCE
        and download_summary.get("storage_frequency") == STORAGE_FREQUENCY
        and download_summary.get("universe_coverage_complete") is True
        and int(download_summary.get("failed_symbols", -1)) == 0
        and int(download_summary.get("partial_symbols", -1)) == 0
    ):
        raise RuntimeError("Shioaji download summary does not prove complete coverage")
    materialization_mode = str(download_summary.get("materialization_mode") or "")
    local_materialization = materialization_mode == "verified_local_minute"
    source_minute_summary_receipt_verified = False
    if local_materialization:
        if int(download_summary.get("api_requests_started", -1)) != 0:
            raise RuntimeError("local daily materialization started Shioaji API requests")
        source_receipt = download_summary.get("source_minute_summary_receipt")
        source_path = Path(str((source_receipt or {}).get("path", "")))
        source_minute_summary_receipt_verified = _receipt_matches(
            source_path, source_receipt, checksum=True
        )
        if not source_minute_summary_receipt_verified:
            raise RuntimeError("source minute download-summary receipt mismatch")
    download_report = _report_map(shioaji_root / "download_report.csv")
    if set(download_report) != base_symbols:
        raise RuntimeError("Shioaji download report does not account for the full universe")
    bad_download = {
        symbol: row["status"]
        for symbol, row in download_report.items()
        if str(row["status"]) not in ALLOWED_DOWNLOAD_STATUSES
    }
    if bad_download:
        raise RuntimeError(f"nonterminal download statuses: {list(bad_download.items())[:20]}")

    dataset_summary = _read_json(dataset_root / "shioaji_dataset_summary.json")
    if dataset_summary.get("source") != HYBRID_SOURCE:
        raise RuntimeError("hybrid dataset summary source mismatch")
    build_report = _report_map(dataset_root / "shioaji_dataset_report.csv")
    if set(build_report) != base_symbols:
        raise RuntimeError("hybrid build report does not account for the full universe")
    bad_build = {
        symbol: row["status"]
        for symbol, row in build_report.items()
        if str(row["status"]) not in ALLOWED_BUILD_STATUSES
    }
    if bad_build:
        raise RuntimeError(f"nonterminal hybrid statuses: {list(bad_build.items())[:20]}")

    hybrid_symbols = 0
    public_only_symbols = 0
    public_only_not_yet_listed_symbols = 0
    public_only_outside_source_window_symbols = 0
    public_source_gap_fallback_rows = 0
    daily_chunk_receipts = 0
    daily_chunk_ok_receipts = 0
    hybrid_rows = 0
    shioaji_rows = 0
    public_rows = 0
    for symbol in sorted(base_symbols):
        download_status = str(download_report[symbol]["status"])
        build_status = str(build_report[symbol]["status"])
        base_path = base_stock_root / f"{symbol}_features.parquet"
        output_path = dataset_root / f"{symbol}_features.parquet"
        if not output_path.is_file():
            raise RuntimeError(f"missing hybrid symbol parquet: {output_path}")
        if download_status in {
            "contract_unavailable",
            "not_yet_listed",
            "outside_source_window",
        }:
            expected_build_status = (
                "public_only_not_yet_listed"
                if download_status == "not_yet_listed"
                else "public_only_outside_source_window"
                if download_status == "outside_source_window"
                else "public_only_contract_unavailable"
            )
            if build_status != expected_build_status:
                raise RuntimeError(f"{symbol}: unavailable contract/build status mismatch")
            if _sha256(base_path) != _sha256(output_path):
                raise RuntimeError(f"{symbol}: public-only output differs from base parquet")
            if download_status == "not_yet_listed":
                public_only_not_yet_listed_symbols += 1
            elif download_status == "outside_source_window":
                public_only_outside_source_window_symbols += 1
            else:
                public_only_symbols += 1
            rows = pl.scan_parquet(output_path).select(pl.len()).collect().item()
            public_rows += int(rows)
            hybrid_rows += int(rows)
            continue

        if build_status != "hybrid":
            raise RuntimeError(f"{symbol}: complete download/build status mismatch")
        daily_path = shioaji_root / "daily" / f"{symbol}.parquet"
        daily_summary = _read_json(daily_path.with_suffix(".summary.json"))
        if not (
            daily_summary.get("source") == SHIOAJI_SOURCE
            and daily_summary.get("storage_frequency") == STORAGE_FREQUENCY
            and daily_summary.get("symbol") == symbol
            and _receipt_matches(
                daily_path,
                daily_summary.get("output_receipt"),
                checksum=verify_chunk_checksums,
            )
        ):
            raise RuntimeError(f"{symbol}: invalid daily receipt")
        if local_materialization:
            minute_manifest_receipt = daily_summary.get("minute_manifest_receipt")
            minute_manifest_path = Path(
                str((minute_manifest_receipt or {}).get("path", ""))
            )
            if not (
                daily_summary.get("materialization_mode")
                == "verified_local_minute"
                and _receipt_matches(
                    minute_manifest_path,
                    minute_manifest_receipt,
                    checksum=True,
                )
                and int(daily_summary.get("source_minute_chunks_verified", 0)) > 0
            ):
                raise RuntimeError(f"{symbol}: invalid local minute lineage")
            receipt_count = int(daily_summary["source_minute_chunks_verified"])
            ok_count = receipt_count
        else:
            receipt_count, ok_count = _audit_daily_chunks(
                shioaji_root,
                symbol,
                daily_summary,
                verify_checksum=verify_chunk_checksums,
            )
        daily_chunk_receipts += receipt_count
        daily_chunk_ok_receipts += ok_count
        daily = pl.read_parquet(
            daily_path,
            columns=["date", *QUOTE_COLUMNS],
        ).sort("date")
        output = pl.read_parquet(
            output_path,
            columns=[
                "date",
                *QUOTE_COLUMNS,
                "data_source",
                "fallback_reason",
                "adjclose",
            ],
        ).sort("date")
        if daily.height != int(daily_summary.get("daily_rows", -1)) or daily.is_empty():
            raise RuntimeError(f"{symbol}: invalid daily row count")
        first_shioaji = daily["date"].min()
        output_after = output.filter(pl.col("date") >= first_shioaji)
        output_before = output.filter(pl.col("date") < first_shioaji)
        base_quotes = pl.read_parquet(base_path, columns=["date", *QUOTE_COLUMNS])
        base_before = base_quotes.select("date").filter(
            pl.col("date") < first_shioaji
        )
        if output_before["date"].to_list() != base_before["date"].to_list():
            raise RuntimeError(f"{symbol}: public pre-cutover dates changed")
        declared_gaps = {
            date.fromisoformat(str(value))
            for value in daily_summary.get("declared_source_gap_dates", [])
        }
        daily_dates = set(daily["date"])
        fallback_dates = {
            value
            for value in declared_gaps
            if value >= first_shioaji
            and value in set(base_quotes["date"])
            and value not in daily_dates
        }
        if output_after["date"].to_list() != sorted(daily_dates | fallback_dates):
            raise RuntimeError(
                f"{symbol}: post-cutover dates do not match Shioaji plus declared gaps"
            )
        output_shioaji = output_after.filter(
            pl.col("data_source") == SHIOAJI_SOURCE
        )
        output_fallback = output_after.filter(
            pl.col("data_source") != SHIOAJI_SOURCE
        )
        if output_shioaji["date"].to_list() != daily["date"].to_list():
            raise RuntimeError(f"{symbol}: Shioaji output dates changed")
        if (
            set(output_fallback["date"]) != fallback_dates
            or output_fallback.filter(
                pl.col("fallback_reason") != "shioaji_declared_source_gap"
            ).height
        ):
            raise RuntimeError(f"{symbol}: undeclared public fallback after cutover")
        for column in QUOTE_COLUMNS:
            if not _allclose(output_shioaji[column], daily[column]):
                raise RuntimeError(f"{symbol}: quote mismatch in {column}")
        if fallback_dates:
            base_fallback = base_quotes.filter(
                pl.col("date").is_in(sorted(fallback_dates))
            ).sort("date")
            for column in QUOTE_COLUMNS:
                if not _allclose(output_fallback[column], base_fallback[column]):
                    raise RuntimeError(
                        f"{symbol}: public source-gap fallback mismatch in {column}"
                    )
        invalid_adjclose = output.filter(
            pl.col("adjclose").is_null()
            | ~pl.col("adjclose").is_finite()
            | (pl.col("adjclose") <= 0.0)
        ).height
        if invalid_adjclose:
            raise RuntimeError(f"{symbol}: {invalid_adjclose} invalid adjclose rows")
        hybrid_symbols += 1
        hybrid_rows += output.height
        shioaji_rows += daily.height
        public_rows += output_before.height + output_fallback.height
        public_source_gap_fallback_rows += output_fallback.height

    expected = {
        "symbols": len(base_symbols),
        "hybrid_symbols": hybrid_symbols,
        "public_only_contract_unavailable_symbols": public_only_symbols,
        "public_only_not_yet_listed_symbols": public_only_not_yet_listed_symbols,
        "public_only_outside_source_window_symbols": (
            public_only_outside_source_window_symbols
        ),
        "public_source_gap_fallback_rows": public_source_gap_fallback_rows,
        "rows": hybrid_rows,
        "public_rows": public_rows,
        "shioaji_rows": shioaji_rows,
    }
    for key, value in expected.items():
        if int(dataset_summary.get(key, -1)) != value:
            raise RuntimeError(
                f"dataset summary reconciliation failed {key}: "
                f"summary={dataset_summary.get(key)} actual={value}"
            )
    return {
        "schema_version": 1,
        "status": "ok",
        **expected,
        "storage_frequency": STORAGE_FREQUENCY,
        "daily_chunk_receipts": 0 if local_materialization else daily_chunk_receipts,
        "daily_chunk_ok_receipts": (
            0 if local_materialization else daily_chunk_ok_receipts
        ),
        "source_minute_chunk_receipts": (
            daily_chunk_receipts if local_materialization else 0
        ),
        "source_minute_chunk_ok_receipts": (
            daily_chunk_ok_receipts if local_materialization else 0
        ),
        "daily_chunk_checksums_verified": verify_chunk_checksums,
        "download_end_date": download_summary.get("end_date"),
        "materialization_mode": materialization_mode,
        "api_requests_started": download_summary.get("api_requests_started"),
        "source_minute_summary_receipt_verified": (
            source_minute_summary_receipt_verified
        ),
        "written_at_utc": datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat(),
    }


def main() -> None:
    args = parse_args()
    result = audit(
        base_stock_root=args.base_stock_root,
        shioaji_root=args.shioaji_root,
        dataset_root=args.dataset_root,
        verify_chunk_checksums=not bool(args.skip_chunk_checksums),
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    temporary = args.output.with_suffix(args.output.suffix + ".tmp")
    temporary.write_text(
        json.dumps(result, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    temporary.replace(args.output)
    print(
        f"[tw-shioaji-audit] status=ok symbols={result['symbols']} "
        f"hybrid={result['hybrid_symbols']} public_only="
        f"{result['public_only_contract_unavailable_symbols']} "
        f"daily_chunk_receipts={result['daily_chunk_receipts']} "
        f"source_minute_chunk_receipts="
        f"{result['source_minute_chunk_receipts']} "
        f"output={args.output}",
        flush=True,
    )


if __name__ == "__main__":
    main()

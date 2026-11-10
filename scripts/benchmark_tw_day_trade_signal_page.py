#!/usr/bin/env python3
"""Measure one complete TW signal page in an isolated process.

Use the same persistent projection directory as the public gateway. This may
refresh derived private cache shards, but never changes the canonical ledger.
An incomplete scan or a source change during the run is a failed benchmark.
"""

from __future__ import annotations

import argparse
from datetime import date
import hashlib
import json
import os
from pathlib import Path
import resource
import sys
import time
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from stockagent.live import tw_day_trade_dashboard as dashboard  # noqa: E402

build_dashboard_signal_page = dashboard.build_dashboard_signal_page


_SUMMARY_FIELDS = (
    "session_date", "market", "signal_id", "symbol", "target_weight",
    "filled_weight", "filled_shares", "inventory_weight_after",
    "sizing_capital_twd", "ask", "bid", "sizing_open_price",
    "execution_price", "requested_shares", "reason", "status",
)


def _summary_sha256(direction: Any, audit: Any) -> str:
    # The dashboard appends state-dependent completeness hints after its
    # source aggregate. Compare the source aggregate itself across methods.
    source_audit = {
        market: {
            key: value for key, value in item.items()
            if key not in {"expected_model_signal_row_count", "model_signal_rows_complete"}
        }
        for market, item in audit.items()
    }
    encoded = json.dumps(
        [direction, source_audit], sort_keys=True, ensure_ascii=False, default=str
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


def _narrow_summary_probe(
    *, source: Path, state: Path, start_date: str, end_date: str
) -> tuple[int, dict[str, dict[str, Any]], dict[str, dict[str, Any]], dict[str, float]]:
    """Experimental read-only projection; never substitutes for a full page."""

    import polars as pl

    index = dashboard._ledger_session_index(source, recorded_at_fallback=False)
    root = dashboard._detail_session_projection_root(
        source, dashboard._SIGNAL_PAGE_COLUMN_TYPES
    )
    if index is None or root is None:
        raise ValueError("exact ledger index and projection root are required")
    selected = sorted(day for day in index.spans if start_date <= day <= end_date)
    if not selected:
        raise ValueError("no ledger sessions in the selected range")
    validated: list[tuple[Path, tuple[int, int, int, int, int]]] = []
    expected_rows = 0
    started = time.perf_counter()
    for day in selected:
        spans = index.spans[day]
        shard = dashboard._validated_projected_ledger_session_file(
            source,
            index=index,
            session_date=day,
            projected_schema=dashboard._SIGNAL_PAGE_COLUMN_TYPES,
        )
        if shard is None:
            raise ValueError(f"stale or corrupt projection: {day}")
        parquet_path, _, rows, signature = shard
        expected_rows += rows
        validated.append((parquet_path, signature))
    validation_ms = round((time.perf_counter() - started) * 1_000, 3)
    started = time.perf_counter()
    scans = [pl.scan_parquet(path) for path, _ in validated]
    lazy = pl.concat(scans, how="diagonal_relaxed")
    present = set(lazy.collect_schema().names())
    frame = lazy.select(name for name in _SUMMARY_FIELDS if name in present).collect()
    if frame.height != expected_rows:
        raise ValueError("projection row count mismatch")
    load_ms = round((time.perf_counter() - started) * 1_000, 3)
    started = time.perf_counter()
    resolved = pl.col("target_weight").cast(pl.Float64, strict=False).fill_null(0.0)
    filtered = frame.with_columns(
        resolved.alias("__resolved_weight"),
        resolved.abs().alias("__absolute_weight"),
    )
    from stockagent.live.paper_mode_snapshot import read_paper_state
    raw_state = read_paper_state(state)
    capitals = {
        str(market): dashboard._finite_float(raw.get("initial_capital_twd"))
        for market, raw in (raw_state.get("modes") or {}).items()
        if isinstance(raw, dict)
    }
    direction, audit = dashboard._summarize_columnar_signals(
        frame, filtered, capitals
    )
    summary_ms = round((time.perf_counter() - started) * 1_000, 3)
    if any(_signature(path) != signature for path, signature in validated):
        raise ValueError("projection changed during narrow probe")
    return frame.height, direction, audit, {
        "projection_validation": validation_ms,
        "narrow_load": load_ms,
        "narrow_summary": summary_ms,
        "narrow_frame_bytes": frame.estimated_size(),
    }


def _signature(path: Path) -> tuple[int, int, int, int, int] | None:
    try:
        stat = path.stat()
    except OSError:
        return None
    return stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--state-dir", type=Path,
        default=REPO_ROOT / "artifacts/live/tw_day_trade_simulation",
    )
    parser.add_argument(
        "--cache-dir", type=Path,
        default=Path(os.environ.get("STOCKAGENT_DASHBOARD_INDEX_CACHE_DIR") or "."),
        help="Existing STOCKAGENT_DASHBOARD_INDEX_CACHE_DIR from the gateway unit",
    )
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--mode", default="all", help="Exact dashboard mode filter; all retains the full universe")
    parser.add_argument("--offset", type=int, default=0, help="Measure this zero-based result offset")
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--hide-zero-targets", action="store_true")
    parser.add_argument("--revision-scope", choices=("all", "latest"), default="all")
    parser.add_argument(
        "--public-price-root", type=Path,
        help="Also measure exact public quote enrichment, disclosure and JSON encoding; no network",
    )
    parser.add_argument("--expected-public-sha", help="Optional exact public DTO comparison")
    parser.add_argument("--expected-total", type=int)
    parser.add_argument(
        "--probe-narrow-summary", action="store_true",
        help="Measure only a verified narrow summary, never the full page",
    )
    parser.add_argument(
        "--legacy-full-projection", action="store_true",
        help="Use the complete-width projection as an exact A/B control",
    )
    parser.add_argument(
        "--expected-summary-sha",
        help="Required for a narrow probe; obtain from the baseline result",
    )
    args = parser.parse_args(argv)
    if args.offset < 0:
        parser.error("offset must be non-negative")
    if (
        not args.cache_dir.is_dir()
        or args.cache_dir == Path(".")
        or not args.state_dir.is_dir()
    ):
        parser.error("existing state and explicit gateway projection cache directories are required")
    if args.public_price_root is not None and not args.public_price_root.is_dir():
        parser.error("public-price-root must be an existing canonical public data directory")
    if args.expected_public_sha and args.public_price_root is None:
        parser.error("expected-public-sha requires public-price-root")
    try:
        if date.fromisoformat(args.start_date) > date.fromisoformat(args.end_date):
            parser.error("start-date must not exceed end-date")
    except ValueError as exc:
        parser.error(f"invalid ISO date: {exc}")

    source = args.state_dir / "signals.jsonl"
    state = args.state_dir / "state.json"
    before = (_signature(source), _signature(state))
    if None in before:
        parser.error("canonical signals.jsonl and state.json are required")
    os.environ["STOCKAGENT_DASHBOARD_INDEX_CACHE_DIR"] = str(args.cache_dir.resolve())
    if args.probe_narrow_summary and not args.expected_summary_sha:
        parser.error("--expected-summary-sha is required for a narrow probe")
    if args.probe_narrow_summary and args.expected_total is None:
        parser.error("--expected-total is required for a narrow probe")
    if args.probe_narrow_summary and args.legacy_full_projection:
        parser.error("narrow probe and legacy full projection are mutually exclusive")
    if args.probe_narrow_summary and args.public_price_root is not None:
        parser.error("a narrow summary probe cannot measure the public full page")
    if args.probe_narrow_summary and args.mode != "all":
        parser.error("the diagnostic narrow summary probe does not implement mode filtering")
    if args.probe_narrow_summary and args.revision_scope != "all":
        parser.error("the diagnostic narrow summary probe does not implement revision filtering")
    if args.probe_narrow_summary and args.offset:
        parser.error("the diagnostic narrow summary probe does not implement pagination")
    quote_source = None
    quote_before = None
    if args.public_price_root is not None:
        from stockagent.live.public_daily_quotes import OfficialDailyQuoteSource
        quote_source = OfficialDailyQuoteSource(args.public_price_root)
        quote_before = quote_source.signature()
    stages: dict[str, float] = {}
    started = time.perf_counter()
    if args.probe_narrow_summary:
        try:
            total, direction, audit, stages = _narrow_summary_probe(
                source=source,
                state=state,
                start_date=args.start_date,
                end_date=args.end_date,
            )
        except (OSError, ValueError, json.JSONDecodeError) as exc:
            print(json.dumps({"complete": False, "error": str(exc)}))
            return 2
        summary_sha = _summary_sha256(direction, audit)
        result = {"total": total, "returned": None, "scan_limit_reached": False}
    else:
        result = build_dashboard_signal_page(
            state_dir=args.state_dir,
            start_date=args.start_date,
            end_date=args.end_date,
            mode=args.mode,
            offset=args.offset,
            limit=args.limit,
            hide_zero_targets=args.hide_zero_targets,
            revision_scope=args.revision_scope,
            timing_ms=stages,
            _allow_narrow_projection=not args.legacy_full_projection,
        )
        summary_sha = _summary_sha256(
            result.get("direction_summary") or {},
            result.get("opening_execution_audit") or {},
        )
    public_sha = None
    public_bytes = None
    public_quote_rows = None
    if quote_source is not None:
        from stockagent.live.public_dashboards import sanitize_tw_signals
        phase = time.perf_counter()
        public_payload = sanitize_tw_signals(result, quote_source=quote_source)
        stages["public_quote_projection"] = round((time.perf_counter() - phase) * 1_000, 3)
        phase = time.perf_counter()
        encoded = json.dumps(public_payload, sort_keys=True, ensure_ascii=False,
                             separators=(",", ":"), allow_nan=False).encode("utf-8")
        public_sha = hashlib.sha256(encoded).hexdigest()
        public_bytes = len(encoded)
        public_quote_rows = sum(isinstance(row, dict) and "official_daily_quote" in row
                                for row in public_payload.get("rows", []))
        stages["public_json_encoding"] = round((time.perf_counter() - phase) * 1_000, 3)
    elapsed_ms = round((time.perf_counter() - started) * 1_000, 3)
    after = (_signature(source), _signature(state))
    quotes_unchanged = quote_source is None or quote_before == quote_source.signature()
    complete = (
        before == after
        and quotes_unchanged
        and result.get("scan_limit_reached") is False
        and (
            args.probe_narrow_summary
            or result.get("opening_execution_audit_scope")
            == "complete_current_signal_rows_per_mode"
        )
        and (
            args.expected_total is None
            or result.get("total") == args.expected_total
        )
        and (
            args.expected_summary_sha is None
            or summary_sha == args.expected_summary_sha
        )
        and (args.expected_public_sha is None or public_sha == args.expected_public_sha)
    )
    digest = (
        None if args.probe_narrow_summary else hashlib.sha256(
            json.dumps(result, sort_keys=True, ensure_ascii=False, default=str).encode()
        ).hexdigest()
    )
    print(json.dumps({
        "schema_version": 1,
        "variant": "narrow_summary_probe" if args.probe_narrow_summary else "full_page",
        "projection_method": (
            "narrow_probe" if args.probe_narrow_summary else
            "legacy_full" if args.legacy_full_projection else "auto"
        ),
        "complete": complete,
        "start_date": args.start_date,
        "end_date": args.end_date,
        "mode": args.mode,
        "offset": args.offset,
        "limit": args.limit,
        "hide_zero_targets": args.hide_zero_targets,
        "revision_scope": args.revision_scope,
        "total": result.get("total"),
        "returned": result.get("returned"),
        "source_rows_scanned": result.get("source_rows_scanned"),
        "record_count": result.get("record_count"),
        "scan_limit_reached": result.get("scan_limit_reached"),
        "source_unchanged": before == after,
        "signals_signature": list(before[0]) if before[0] is not None else None,
        "state_signature": list(before[1]) if before[1] is not None else None,
        "elapsed_ms": elapsed_ms,
        "maxrss_kib": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
        "stages_ms": stages,
        "summary_sha256": summary_sha,
        "result_sha256": digest,
        "public_result_sha256": public_sha,
        "public_json_bytes": public_bytes,
        "public_rows_with_official_daily_quote": public_quote_rows,
        "public_quote_source_unchanged": quotes_unchanged if quote_source is not None else None,
        "public_quote_source_signature": quote_before,
        "network_measured": False,
    }, separators=(",", ":")))
    return 0 if complete else 2


if __name__ == "__main__":
    raise SystemExit(main())

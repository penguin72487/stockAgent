#!/usr/bin/env python3
"""Rebuild TW training features only when model inputs or close coverage change.

Mutable OpenAPI snapshots use complete source-byte receipts. Original release
archives use a feature-semantic receipt so an unchanged value/clock does not
rebuild 9.5 million rows merely because official HTML markup was rewrapped.
Any changed input still requires a full-history rebuild: it could revise an
arbitrarily old observation, so a blind tail rewrite is unsafe.
"""

from __future__ import annotations

import argparse
from contextlib import contextmanager, nullcontext
from datetime import date, datetime, time
import fcntl
import json
from pathlib import Path
import subprocess
import sys
from zoneinfo import ZoneInfo


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from stockagent.data.tw_public_features import (  # noqa: E402
    DEFAULT_MARKET_SYMBOL,
    _incremental_base_is_compatible,
    _source_content_receipts,
    _symbol_universe_receipt,
)
from stockagent.live.data_monitor_inventory import parquet_footer_stats  # noqa: E402


def _read_json(path: Path) -> dict:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"JSON receipt is not an object: {path}")
    return payload


def _inside_taiwan_market_hours(local_clock: time) -> bool:
    return time(9, 0) <= local_clock < time(13, 30)


@contextmanager
def _source_update_lock(root: Path):
    path = root.resolve().parent / ".locks" / "tw-public-refresh.lock"
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+") as handle:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RuntimeError("canonical TW public source update is active") from exc
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def _accepted_close_end_date(input_dir: Path, publication_root: Path) -> str | None:
    """Advance past a stale morning summary only with a verified close event."""

    close_dates: list[str] = []
    for phase in ("close_initial", "close_final"):
        receipt = publication_root / phase / "latest.json"
        try:
            accepted = _read_json(receipt)
        except (OSError, ValueError, TypeError):
            continue
        summary = accepted.get("download_summary")
        if (
            accepted.get("status") != "ok"
            or accepted.get("phase") != phase
            or Path(str(accepted.get("live_root") or "")).resolve(strict=False)
            != input_dir.resolve(strict=False)
            or not isinstance(summary, dict)
            or summary.get("coverage_complete") is not True
            or summary.get("blocking_failed_count") not in (0, "0")
            or not {"twse_daily_ohlcv", "tpex_daily_ohlcv"}
            <= set(accepted.get("selected_datasets") or ())
        ):
            continue
        candidate = str(summary.get("end_date") or "")
        try:
            date.fromisoformat(candidate)
        except ValueError:
            continue
        verified = []
        for name in ("twse_daily_ohlcv", "tpex_daily_ohlcv"):
            stats = parquet_footer_stats(input_dir / f"{name}.parquet")
            verified.append(str((stats or {}).get("last") or "")[:10])
        if all(last >= candidate for last in verified):
            close_dates.append(candidate)
    return max(close_dates) if close_dates else None


def needs_rebuild(input_dir: Path, symbols_root: Path, output_path: Path) -> tuple[bool, str]:
    download = _read_json(input_dir / "download_summary.json")
    if download.get("coverage_complete") is not True or int(download.get("blocking_failed_count", -1)) != 0:
        raise RuntimeError("TW public download receipt does not prove complete close-source coverage")
    target_end = str(download.get("end_date") or "")
    if not target_end:
        raise RuntimeError("TW public download receipt lacks end_date")
    accepted_close = _accepted_close_end_date(
        input_dir, REPO_ROOT / "artifacts/data_refresh/tw_public/publications"
    )
    if accepted_close is not None:
        target_end = max(target_end, accepted_close)
    compatible = _incremental_base_is_compatible(
        output_path,
        output_path.with_suffix(".summary.json"),
        market_symbol=DEFAULT_MARKET_SYMBOL,
        source_receipts=_source_content_receipts(input_dir),
        symbol_universe_receipt=_symbol_universe_receipt(symbols_root),
    )
    footer = parquet_footer_stats(output_path) if output_path.is_file() else None
    prior = _read_json(output_path.with_suffix(".summary.json")) if compatible else {}
    built_through = str(prior.get("requested_end_date") or "")
    if not built_through and footer:
        # Backward compatibility for a pre-cutoff-receipt output.  A future
        # rebuild records the requested cutoff even on an empty holiday.
        built_through = str(footer.get("last") or "")[:10]
    if built_through and target_end < built_through:
        raise RuntimeError(
            f"TW public reconcile would regress feature coverage: "
            f"target={target_end} existing={built_through}"
        )
    if compatible and built_through >= target_end:
        return False, target_end
    return True, target_end


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-dir", type=Path, default=Path("data_tw_public"))
    parser.add_argument("--symbols-root", type=Path, default=Path("data_tw_public/stocks"))
    parser.add_argument("--output-path", type=Path, default=Path("data_tw_public/features/tw_public_stock_daily.parquet"))
    parser.add_argument(
        "--research-output-path", type=Path, default=None,
        help="Also refresh the separate estimated-vintage wide research table after canonical verification.",
    )
    parser.add_argument(
        "--research-macro-events-path", type=Path,
        default=Path("artifacts/data_quality/tw_public_provisional_macro/events.parquet"),
    )
    parser.add_argument(
        "--research-taifex-output-path", type=Path, default=None,
        help="Also refresh the separate TAIFEX-enriched local research ABI after the wide table.",
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--source-update-lock-held", action="store_true",
        help="Only the canonical archive orchestrator may use this while holding the producer lock.",
    )
    parser.add_argument(
        "--defer-market-hours", action="store_true",
        help="Timer catch-up must not run a full rebuild during the Taiwan stock session.",
    )
    args = parser.parse_args()
    local_now = datetime.now(ZoneInfo("Asia/Taipei"))
    if args.defer_market_hours and _inside_taiwan_market_hours(local_now.time()):
        print("[tw-public-feature-reconcile] deferred=taiwan_market_hours", flush=True)
        return 0
    lock = (
        nullcontext()
        if args.dry_run or args.source_update_lock_held
        else _source_update_lock(args.input_dir)
    )
    try:
        with lock:
            rebuild, end_date = needs_rebuild(args.input_dir, args.symbols_root, args.output_path)
            print(f"[tw-public-feature-reconcile] rebuild={str(rebuild).lower()} end_date={end_date}", flush=True)
            if args.dry_run:
                return 0
            if rebuild:
                subprocess.run(
                    [sys.executable, str(REPO_ROOT / "scripts/build_tw_public_training_features.py"),
                     "--input-dir", str(args.input_dir), "--symbols-root", str(args.symbols_root),
                     "--output-path", str(args.output_path), "--end-date", end_date,
                     "--incremental-tail-days", "7"],
                    cwd=REPO_ROOT,
                    check=True,
                )
                rebuild_again, checked_end = needs_rebuild(args.input_dir, args.symbols_root, args.output_path)
                if rebuild_again or checked_end != end_date:
                    raise RuntimeError("TW public feature receipt changed or remained stale after rebuild")
            if args.research_output_path is not None:
                # The research table consumes the provisional macro ledger,
                # which must be refreshed under the same source lock. A new
                # source release must not wait for a separate archive timer.
                subprocess.run(
                    [sys.executable, str(REPO_ROOT / "scripts/build_tw_public_provisional_macro.py"),
                     "--input-dir", str(args.input_dir),
                     "--output-path", str(args.research_macro_events_path),
                     "--source-update-lock-held"],
                    cwd=REPO_ROOT,
                    check=True,
                )
                subprocess.run(
                    [sys.executable, str(REPO_ROOT / "scripts/build_tw_public_research_features.py"),
                     "--input-dir", str(args.input_dir), "--base-path", str(args.output_path),
                     "--macro-events-path", str(args.research_macro_events_path),
                     "--output-path", str(args.research_output_path),
                     "--source-update-lock-held"],
                    cwd=REPO_ROOT,
                    check=True,
                )
                if args.research_taifex_output_path is not None:
                    subprocess.run(
                        [sys.executable, str(REPO_ROOT / "scripts/build_tw_public_research_taifex.py"),
                         "--base-path", str(args.research_output_path),
                         "--output-path", str(args.research_taifex_output_path)],
                        cwd=REPO_ROOT,
                        check=True,
                    )
            elif args.research_taifex_output_path is not None:
                raise ValueError("--research-taifex-output-path requires --research-output-path")
    except RuntimeError as exc:
        if str(exc) != "canonical TW public source update is active":
            raise
        print("[tw-public-feature-reconcile] deferred=producer_busy", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

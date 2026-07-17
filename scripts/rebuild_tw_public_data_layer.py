from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import shutil
import subprocess
import sys
import time
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Callable

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from stockagent.config import load_config
from scripts.build_tw_yahoo_fallback_archive import (
    _load_verified_transfer_adjustments,
)


_DATA_LAYER_LOCK_HANDLE = None


@dataclass
class StageRecord:
    name: str
    status: str
    command: list[str]
    started_at_utc: str
    finished_at_utc: str
    elapsed_seconds: float
    required_outputs: list[str]
    output_receipts: list[dict[str, Any]]
    message: str = ""


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _acquire_data_layer_lock(path: Path) -> None:
    global _DATA_LAYER_LOCK_HANDLE
    path.parent.mkdir(parents=True, exist_ok=True)
    handle = path.open("a+", encoding="utf-8")
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError as exc:
        handle.close()
        raise RuntimeError(
            f"another TW official data-layer process is running: lock={path}"
        ) from exc
    handle.seek(0)
    handle.truncate()
    handle.write(f"pid={os.getpid()} acquired_at_utc={_utc_now()}\n")
    handle.flush()
    _DATA_LAYER_LOCK_HANDLE = handle


def _read_manifest(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"schema_version": 1, "stages": []}
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"invalid rebuild manifest: {path}")
    return payload


def _write_manifest(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")
    tmp.replace(path)


def _outputs_exist(outputs: list[Path]) -> bool:
    return bool(outputs) and all(path.exists() for path in outputs)


def _file_receipt(path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise ValueError(f"stage output must be a regular file: {path}")
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return {
        "path": str(path),
        "size": int(path.stat().st_size),
        "sha256": digest.hexdigest(),
    }


def _output_receipts(outputs: list[Path]) -> list[dict[str, Any]]:
    return [_file_receipt(path) for path in outputs]


def _receipts_match(stage: dict[str, Any], outputs: list[Path]) -> bool:
    recorded = stage.get("output_receipts")
    if not isinstance(recorded, list) or len(recorded) != len(outputs):
        return False
    try:
        return recorded == _output_receipts(outputs)
    except (OSError, ValueError):
        return False


def _completed_stage(manifest: dict[str, Any], name: str) -> dict[str, Any] | None:
    for stage in reversed(manifest.get("stages", [])):
        if stage.get("name") == name and stage.get("status") == "complete":
            return stage
    return None


def _validate_official_symbol_build_summary(path: Path) -> None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise RuntimeError(
            f"official symbol build summary is unreadable: {path}: "
            f"{type(exc).__name__}: {exc}"
        ) from exc
    if not isinstance(payload, dict):
        raise RuntimeError(
            f"official symbol build summary is not a JSON object: {path}"
        )
    missing_adjustment_rows = payload.get("missing_adjustment_rows")
    if isinstance(missing_adjustment_rows, bool) or not isinstance(
        missing_adjustment_rows,
        int,
    ):
        raise RuntimeError(
            "official symbol build summary has no valid "
            f"missing_adjustment_rows count: {path}"
        )
    if missing_adjustment_rows < 0:
        raise RuntimeError(
            "official symbol build summary has a negative "
            f"missing_adjustment_rows count: {path}"
        )
    explicitly_resolved = payload.get("all_adjustments_resolved")
    if explicitly_resolved is not None and explicitly_resolved is not True:
        raise RuntimeError(
            "official symbol build did not certify all adjustments as resolved: "
            f"all_adjustments_resolved={explicitly_resolved!r} summary={path}"
        )
    if missing_adjustment_rows:
        raise RuntimeError(
            "official symbol build left unresolved adjustment rows: "
            f"missing_adjustment_rows={missing_adjustment_rows} summary={path}"
        )


def _validate_corporate_action_entitlements(
    output: Path,
    *,
    reference: Path,
) -> None:
    summary_path = output.with_suffix(".summary.json")
    try:
        summary = json.loads(summary_path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise RuntimeError(
            f"corporate-action entitlement receipt is unreadable: {summary_path}: "
            f"{type(exc).__name__}: {exc}"
        ) from exc
    if not isinstance(summary, dict):
        raise RuntimeError("corporate-action entitlement receipt is not a JSON object")
    if int(summary.get("schema_version", -1)) < 3:
        raise RuntimeError("corporate-action entitlement schema_version must be >= 3")
    if not bool(summary.get("baseline_established")) or not bool(
        summary.get("coverage_complete")
    ):
        raise RuntimeError("corporate-action entitlement baseline is incomplete")
    if int(summary.get("failure_count", -1)) != 0:
        raise RuntimeError("corporate-action entitlement download contains failures")
    manifest_receipt = summary.get("raw_receipt_manifest")
    manifest_relative = (
        str(manifest_receipt.get("relative_path", "")).strip()
        if isinstance(manifest_receipt, dict)
        else ""
    )
    if not manifest_relative:
        raise RuntimeError(
            "corporate-action entitlement raw receipt manifest is missing"
        )
    output_root = output.parent.resolve()
    manifest_path = (output_root / manifest_relative).resolve()
    if not manifest_path.is_relative_to(output_root) or not manifest_path.is_file():
        raise RuntimeError(
            "corporate-action entitlement raw receipt manifest path is invalid"
        )
    actual_manifest = _file_receipt(manifest_path)
    if {
        "size": int(manifest_receipt.get("size", -1)),
        "sha256": str(manifest_receipt.get("sha256", "")),
    } != {
        "size": actual_manifest["size"],
        "sha256": actual_manifest["sha256"],
    }:
        raise RuntimeError(
            "corporate-action entitlement raw receipt manifest mismatch"
        )
    if manifest_path.stem != actual_manifest["sha256"]:
        raise RuntimeError(
            "corporate-action entitlement raw receipt manifest is not "
            "content-addressed"
        )
    with manifest_path.open("rb") as manifest_handle:
        manifest_rows = sum(1 for line in manifest_handle if line.strip())
    if manifest_rows != int(manifest_receipt.get("entries", -1)):
        raise RuntimeError(
            "corporate-action entitlement raw receipt manifest row mismatch"
        )
    expected_output = summary.get("output_receipt")
    if not isinstance(expected_output, dict) or {
        "size": int(expected_output.get("size", -1)),
        "sha256": str(expected_output.get("sha256", "")),
    } != {
        "size": _file_receipt(output)["size"],
        "sha256": _file_receipt(output)["sha256"],
    }:
        raise RuntimeError("corporate-action entitlement output receipt mismatch")
    expected_reference = summary.get("reference_receipt")
    actual_reference = _file_receipt(reference)
    if not isinstance(expected_reference, dict) or {
        "size": int(expected_reference.get("size", -1)),
        "sha256": str(expected_reference.get("sha256", "")),
    } != {
        "size": actual_reference["size"],
        "sha256": actual_reference["sha256"],
    }:
        raise RuntimeError(
            "corporate-action entitlement ledger was built from another reference"
        )


class RebuildRunner:
    def __init__(
        self,
        *,
        manifest_path: Path,
        resume: bool,
        dry_run: bool,
    ) -> None:
        self.manifest_path = manifest_path
        self.resume = bool(resume)
        self.dry_run = bool(dry_run)
        self.manifest = _read_manifest(manifest_path)
        self.upstream_changed = False

    def update_metadata(self, **values: Any) -> None:
        self.manifest.update(values)
        self.manifest["updated_at_utc"] = _utc_now()
        _write_manifest(self.manifest_path, self.manifest)

    def run(
        self,
        name: str,
        command: list[str],
        *,
        outputs: list[Path],
        allow_resume: bool = True,
        validate_outputs: Callable[[], None] | None = None,
    ) -> None:
        previous = _completed_stage(self.manifest, name)
        resume_match = (
            allow_resume
            and self.resume
            and not self.upstream_changed
            and previous is not None
            and previous.get("command") == command
            and _outputs_exist(outputs)
            and _receipts_match(previous, outputs)
        )
        if resume_match:
            if validate_outputs is None:
                print(f"[tw-data-rebuild] stage={name} status=resume-skip", flush=True)
                return
            started = _utc_now()
            started_clock = time.perf_counter()
            try:
                validate_outputs()
            except BaseException as exc:
                self.upstream_changed = True
                message = f"{type(exc).__name__}: {exc}"
                record = StageRecord(
                    name=name,
                    status="failed",
                    command=command,
                    started_at_utc=started,
                    finished_at_utc=_utc_now(),
                    elapsed_seconds=time.perf_counter() - started_clock,
                    required_outputs=[str(path) for path in outputs],
                    output_receipts=_output_receipts(outputs),
                    message=message,
                )
                self.manifest.setdefault("stages", []).append(asdict(record))
                self.update_metadata()
                raise RuntimeError(f"stage {name} failed: {message}") from exc
            print(f"[tw-data-rebuild] stage={name} status=resume-skip", flush=True)
            return

        printable = " ".join(command)
        print(f"[tw-data-rebuild] stage={name} command={printable}", flush=True)
        if self.dry_run:
            return

        self.upstream_changed = True
        started = _utc_now()
        started_clock = time.perf_counter()
        status = "complete"
        message = ""
        receipts: list[dict[str, Any]] = []
        try:
            subprocess.run(command, cwd=REPO_ROOT, check=True)
            missing = [str(path) for path in outputs if not path.exists()]
            if missing:
                raise RuntimeError(f"stage completed without required outputs: {missing}")
            receipts = _output_receipts(outputs)
            if validate_outputs is not None:
                validate_outputs()
        except BaseException as exc:
            status = "failed"
            message = f"{type(exc).__name__}: {exc}"
        record = StageRecord(
            name=name,
            status=status,
            command=command,
            started_at_utc=started,
            finished_at_utc=_utc_now(),
            elapsed_seconds=time.perf_counter() - started_clock,
            required_outputs=[str(path) for path in outputs],
            output_receipts=receipts,
            message=message,
        )
        self.manifest.setdefault("stages", []).append(asdict(record))
        self.update_metadata()
        if status != "complete":
            raise RuntimeError(f"stage {name} failed: {message}")


def _python_command(*args: str) -> list[str]:
    return [sys.executable, *args]


def _transfer_adjustment_command(
    args: argparse.Namespace,
    *,
    public_dir: Path,
    yahoo_source_dir: Path,
    output_path: Path,
) -> list[str]:
    command = _python_command(
        "downloader/download_tw_transfer_adjustments.py",
        "--mode",
        str(args.mode),
        "--official-input-dir",
        str(public_dir),
        "--yahoo-source-dir",
        str(yahoo_source_dir),
        "--output-path",
        str(output_path),
        "--start-date",
        str(args.fallback_start_date),
        "--end-date",
        str(args.end_date),
        "--workers",
        str(args.public_workers),
        "--timeout",
        str(args.timeout),
        "--retries",
        str(args.retries),
        "--resume" if args.resume else "--no-resume",
    )
    if args.request_interval is not None:
        command.extend(["--request-interval", str(args.request_interval)])
    return command


def _validate_transfer_adjustment_reference(
    path: Path,
    *,
    start: date,
    end: date,
) -> None:
    _load_verified_transfer_adjustments(path, start=start, end=end)


def _taiex_command(args: argparse.Namespace, public_dir: Path) -> list[str]:
    command = _python_command(
        "downloader/download_tw_taiex_ohlc.py",
        "--mode",
        str(args.mode),
        "--output-dir",
        str(public_dir),
        "--start-date",
        str(args.taiex_start_date),
        "--end-date",
        str(args.end_date),
        "--workers",
        str(args.public_workers),
        "--timeout",
        str(args.timeout),
        "--retries",
        str(args.retries),
        "--retry-backoff",
        str(args.retry_backoff),
        "--daily-overlap-days",
        str(args.daily_overlap_days),
        "--resume" if args.resume else "--no-resume",
    )
    if args.request_interval is not None:
        command.extend(["--request-interval", str(args.request_interval)])
    if args.skip_raw:
        command.append("--skip-raw")
    return command


def _public_command(args: argparse.Namespace, public_dir: Path) -> list[str]:
    public_mode = "rebuild" if args.operation == "from-zero" else args.mode
    command = _python_command(
        "downloader/download_tw_public_data.py",
        "--mode",
        public_mode,
        "--datasets",
        "all",
        "--start-date",
        args.public_start_date,
        "--end-date",
        args.end_date,
        "--output-dir",
        str(public_dir),
        "--workers",
        str(args.public_workers),
        "--date-workers",
        str(args.date_workers),
        "--timeout",
        str(args.timeout),
        "--retries",
        str(args.retries),
        "--retry-backoff",
        str(args.retry_backoff),
        "--sleep",
        str(args.sleep),
        "--flush-every-dates",
        str(args.flush_every_dates),
        "--daily-overlap-days",
        str(args.daily_overlap_days),
        "--empty-recheck-days",
        str(args.empty_recheck_days),
        "--require-taiex-session-calendar",
        "--resume" if args.resume else "--no-resume",
    )
    if args.request_interval is not None:
        command.extend(["--request-interval", str(args.request_interval)])
    if args.skip_raw:
        command.append("--skip-raw")
    return command


def _cleanup_published_partials(
    partial_dir: Path,
    *,
    preserve_prefixes: tuple[str, ...] = (),
) -> None:
    """Remove published partials without deleting another stage's resume state."""

    if not partial_dir.is_dir():
        return
    for path in partial_dir.iterdir():
        if any(path.name.startswith(prefix) for prefix in preserve_prefixes):
            continue
        if path.is_dir():
            shutil.rmtree(path)
        else:
            path.unlink()
    try:
        partial_dir.rmdir()
    except OSError:
        pass


def _cleanup_partial_prefix(partial_dir: Path, prefix: str) -> None:
    if not partial_dir.is_dir():
        return
    for path in partial_dir.glob(f"{prefix}*"):
        if path.is_dir():
            shutil.rmtree(path)
        else:
            path.unlink()
    try:
        partial_dir.rmdir()
    except OSError:
        pass


def _run_market_history_stages(
    args: argparse.Namespace,
    runner: RebuildRunner,
    public_dir: Path,
) -> None:
    """Build the certified session calendar before any daily public source."""

    if args.skip_public:
        return

    if not args.skip_taiex_ohlc:
        taiex_path = public_dir / "twse_taiex_ohlc.parquet"
        runner.run(
            "twse_taiex_ohlc",
            _taiex_command(args, public_dir),
            outputs=[taiex_path, taiex_path.with_suffix(".summary.json")],
        )
        if not args.dry_run:
            _cleanup_partial_prefix(
                public_dir / "state" / "partials",
                "twse_taiex_ohlc.",
            )

    # Even when an operator explicitly skips the monthly download, the public
    # child remains strict and will accept only an already complete, receipt-
    # verified calendar covering the whole requested range.
    runner.run(
        "official_public_sources",
        _public_command(args, public_dir),
        outputs=[public_dir / "download_summary.json", public_dir / "download_report.csv"],
    )
    if not args.dry_run:
        # Keep a skipped/unfinished monthly stage's source-scoped resume state;
        # daily public-source partials are redundant after successful publication.
        _cleanup_published_partials(
            public_dir / "state" / "partials",
            preserve_prefixes=("twse_taiex_ohlc.",),
        )


def _production_paths(args: argparse.Namespace, config) -> tuple[Path, Path, Path]:
    production_stocks = Path(args.stock_root or config.data.parquet_root)
    production_public_feature = Path(
        args.public_feature_path or config.data.tw_public_feature_path
    )
    production_public = Path(args.public_dir or production_public_feature.parent.parent)
    return production_stocks, production_public, production_public_feature


def _data_paths(args: argparse.Namespace, config) -> tuple[Path, Path, Path]:
    production_stocks, production_public, production_public_feature = _production_paths(
        args, config
    )
    if args.operation != "from-zero":
        return production_stocks, production_public, production_public_feature

    stage_root = Path(args.stage_root)
    public = stage_root / "data_tw_public"
    try:
        stock_relative_path = production_stocks.relative_to(production_public)
        feature_relative_path = production_public_feature.relative_to(production_public)
    except ValueError as exc:
        raise ValueError(
            "TW official stock and feature outputs must be inside the public-data tree for atomic promotion"
        ) from exc
    return public / stock_relative_path, public, public / feature_relative_path


def _preflight(
    *,
    operation: str,
    stage_root: Path,
    stock_root: Path,
    public_dir: Path,
    min_free_gb: float,
) -> None:
    if operation == "from-zero":
        stage_root.mkdir(parents=True, exist_ok=True)
        usage = shutil.disk_usage(stage_root)
        free_gb = usage.free / 1024**3
        if free_gb < float(min_free_gb):
            raise RuntimeError(
                f"insufficient free disk for staged rebuild: free={free_gb:.1f}GB "
                f"required={float(min_free_gb):.1f}GB"
            )
    elif operation in {"audit", "daily"}:
        if not stock_root.exists():
            raise FileNotFoundError(f"TW official stock root is missing: {stock_root}")
        if not public_dir.exists():
            raise FileNotFoundError(f"TW public-data root is missing: {public_dir}")


def _audit_command(
    args: argparse.Namespace,
    *,
    stock_root: Path,
    public_dir: Path,
    public_feature_path: Path,
    output_dir: Path,
) -> list[str]:
    command = _python_command(
        "scripts/audit_tw_public_data_layer.py",
        "--config",
        str(args.config),
        "--parquet-root",
        str(stock_root),
        "--public-dir",
        str(public_dir),
        "--public-feature-path",
        str(public_feature_path),
        "--output-dir",
        str(output_dir),
        "--build-panel",
        "--strict",
    )
    if args.require_live_selected_features:
        command.append("--require-live-selected-features")
    return command


def _promote_one(staged: Path, production: Path, backup: Path) -> None:
    if not staged.exists():
        raise FileNotFoundError(f"staged path is missing: {staged}")
    production.parent.mkdir(parents=True, exist_ok=True)
    backup.parent.mkdir(parents=True, exist_ok=True)
    moved_old = False
    try:
        if production.exists():
            if backup.exists():
                raise FileExistsError(f"backup target already exists: {backup}")
            os.replace(production, backup)
            moved_old = True
        os.replace(staged, production)
    except BaseException:
        if moved_old and backup.exists() and not production.exists():
            os.replace(backup, production)
        raise


def _rollback_promoted_tree(
    *,
    staged: Path,
    production: Path,
    backup: Path,
) -> None:
    if staged.exists():
        raise FileExistsError(f"cannot roll back over existing staged path: {staged}")
    if production.exists():
        staged.parent.mkdir(parents=True, exist_ok=True)
        os.replace(production, staged)
    if backup.exists():
        production.parent.mkdir(parents=True, exist_ok=True)
        os.replace(backup, production)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Manage the TWSE/TPEx-first data layer in rebuild, repair, or daily mode. "
            "Yahoo may fill otherwise-missing OHLCV rows from 2000 onward; every "
            "derived artifact is then audited fail-closed."
        )
    )
    parser.add_argument("--config", type=Path, default=Path("configs/markets/tw_public.yaml"))
    parser.add_argument("--public-dir", type=Path, default=None)
    parser.add_argument("--stock-root", type=Path, default=None)
    parser.add_argument("--public-feature-path", type=Path, default=None)
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument(
        "--mode",
        choices=("rebuild", "repair", "daily"),
        default=None,
        help=(
            "rebuild stages a from-zero replacement; repair checks and fills historical gaps; "
            "daily refreshes a recent overlap and appends new sessions"
        ),
    )
    mode_group.add_argument(
        "--operation",
        choices=("audit", "repair", "from-zero"),
        default=None,
        help=argparse.SUPPRESS,
    )
    parser.add_argument("--stage-root", type=Path, default=None)
    parser.add_argument(
        "--lock-file",
        type=Path,
        default=Path("artifacts/data_locks/tw_official_data.lock"),
    )
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--end-date", default=date.today().isoformat())
    parser.add_argument("--public-start-date", default="earliest")
    parser.add_argument(
        "--taiex-start-date",
        default="1999-01-05",
        help=(
            "Official MI_5MINS_HIST archive start. Keep the 1999 history so the "
            "first 2000 session has a previous official close."
        ),
    )
    parser.add_argument("--short-start-year", type=int, default=1995)
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--public-workers", type=int, default=2)
    parser.add_argument("--date-workers", type=int, default=8)
    parser.add_argument("--timeout", type=int, default=30)
    parser.add_argument("--retries", type=int, default=4)
    parser.add_argument("--retry-backoff", type=float, default=1.0)
    parser.add_argument("--sleep", type=float, default=0.15)
    parser.add_argument("--request-interval", type=float, default=None)
    parser.add_argument("--flush-every-dates", type=int, default=250)
    parser.add_argument("--daily-overlap-days", type=int, default=7)
    parser.add_argument("--empty-recheck-days", type=int, default=30)
    parser.add_argument("--min-free-gb", type=float, default=40.0)
    parser.add_argument(
        "--ohlcv-fallback",
        choices=("yahoo", "none"),
        default="yahoo",
        help=(
            "Lower-priority source for date-symbol OHLCV rows absent from official "
            "TWSE/TPEx data. Yahoo fallback is restricted to 2000 onward."
        ),
    )
    parser.add_argument("--fallback-start-date", default="2000-01-01")
    parser.add_argument(
        "--yahoo-fallback-dir",
        type=Path,
        default=None,
        help=(
            "Yahoo per-symbol source directory. Defaults to "
            "<public-dir>/fallback/yahoo_tw_stocks so staged rebuilds remain portable."
        ),
    )
    parser.add_argument("--yahoo-workers", type=int, default=1)
    parser.add_argument("--yahoo-retries", type=int, default=3)
    parser.add_argument("--yahoo-request-interval", type=float, default=1.5)
    parser.add_argument(
        "--skip-yahoo-download",
        action="store_true",
        help="Use the existing Yahoo fallback directory without contacting Yahoo.",
    )
    parser.add_argument("--skip-symbol-build", action="store_true")
    parser.add_argument("--skip-feature-build", action="store_true")
    parser.add_argument("--skip-public", action="store_true")
    parser.add_argument("--skip-taiex-ohlc", action="store_true")
    parser.add_argument("--skip-corporate-actions", action="store_true")
    parser.add_argument("--skip-short-rules", action="store_true")
    parser.add_argument("--skip-raw", action="store_true")
    parser.add_argument(
        "--legacy-official-ohlcv",
        type=Path,
        action="append",
        default=[],
        help=(
            "Provenance-backed TWSE/TPEx archive; repeatable. It may contain adjclose, "
            "signed_change, or reference_price and is merged before first=10 reconstruction."
        ),
    )
    parser.add_argument("--legacy-source-name", default=None)
    parser.add_argument("--promote", action="store_true")
    parser.add_argument("--backup-root", type=Path, default=None)
    parser.add_argument("--resume", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--require-live-selected-features", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    requested_mode = args.mode
    if args.mode is None:
        args.mode = {
            "from-zero": "rebuild",
            "repair": "repair",
            "audit": "audit",
            None: "repair",
        }[args.operation]
    args.operation = {
        "rebuild": "from-zero",
        "repair": "repair",
        "daily": "daily",
        "audit": "audit",
    }[args.mode]
    _acquire_data_layer_lock(args.lock_file)
    run_id = args.run_id or _run_id()
    if args.stage_root is None:
        args.stage_root = Path("artifacts/data_rebuild") / run_id
    else:
        args.stage_root = Path(args.stage_root)
    if args.operation != "from-zero" and args.promote:
        raise ValueError("--promote is valid only with --operation from-zero")
    if args.legacy_official_ohlcv and not args.legacy_source_name:
        raise ValueError("--legacy-source-name is required with --legacy-official-ohlcv")
    missing_legacy_paths = [
        str(path) for path in args.legacy_official_ohlcv if not Path(path).is_file()
    ]
    if missing_legacy_paths and not args.dry_run:
        raise FileNotFoundError(
            f"legacy official OHLCV input is missing: {missing_legacy_paths}"
        )
    fallback_start = date.fromisoformat(args.fallback_start_date)
    requested_end = date.fromisoformat(args.end_date)
    if fallback_start < date(2000, 1, 1):
        raise ValueError("Yahoo OHLCV fallback is restricted to 2000-01-01 and later")
    if fallback_start > requested_end:
        raise ValueError("--fallback-start-date must not be after --end-date")
    if args.yahoo_request_interval < 0.0:
        raise ValueError("--yahoo-request-interval must be >= 0")

    config = load_config(args.config)
    expected_first_year = config.walk_forward.expected_first_year
    if (
        args.operation == "from-zero"
        and expected_first_year is not None
        and int(expected_first_year) < 2004
        and not args.legacy_official_ohlcv
        and args.ohlcv_fallback == "none"
    ):
        raise ValueError(
            f"config requires official history from {int(expected_first_year)}, but the free TWSE "
            "daily OHLCV archive starts in 2004. Keep --ohlcv-fallback yahoo or supply a "
            "provenance-backed TWSE/TPEx archive with --legacy-official-ohlcv and "
            "--legacy-source-name; fold IDs must not be renumbered."
        )
    production_stocks, production_public, production_public_feature = _production_paths(
        args, config
    )
    stock_root, public_dir, public_feature_path = _data_paths(args, config)
    yahoo_fallback_dir = (
        Path(args.yahoo_fallback_dir)
        if args.yahoo_fallback_dir is not None
        else public_dir / "fallback" / "yahoo_tw_stocks"
    )
    yahoo_fallback_archive = public_dir / "fallback" / "yahoo_tw_ohlcv.parquet"
    transfer_adjustment_reference = (
        public_dir / "tw_transfer_adjustment_reference.parquet"
    )
    audit_output = Path(args.stage_root) / "audit"
    manifest_path = Path(args.stage_root) / "rebuild_manifest.json"
    runner = RebuildRunner(
        manifest_path=manifest_path,
        resume=bool(args.resume),
        dry_run=bool(args.dry_run),
    )
    runner.update_metadata(
        run_id=run_id,
        operation=args.operation,
        mode=args.mode,
        requested_mode=requested_mode,
        config=str(args.config),
        stage_root=str(args.stage_root),
        stock_root=str(stock_root),
        public_dir=str(public_dir),
        public_feature_path=str(public_feature_path),
        production_stocks=str(production_stocks),
        production_public=str(production_public),
        ohlcv_fallback=args.ohlcv_fallback,
        fallback_start_date=fallback_start.isoformat(),
        yahoo_fallback_dir=str(yahoo_fallback_dir),
        yahoo_fallback_archive=str(yahoo_fallback_archive),
        transfer_adjustment_reference=str(transfer_adjustment_reference),
        yahoo_request_interval=float(args.yahoo_request_interval),
        started_at_utc=runner.manifest.get("started_at_utc", _utc_now()),
    )
    _preflight(
        operation=args.operation,
        stage_root=Path(args.stage_root),
        stock_root=stock_root,
        public_dir=public_dir,
        min_free_gb=args.min_free_gb,
    )

    if args.operation == "audit":
        runner.run(
            "audit",
            _audit_command(
                args,
                stock_root=stock_root,
                public_dir=public_dir,
                public_feature_path=public_feature_path,
                output_dir=audit_output,
            ),
            outputs=[audit_output / "summary.json", audit_output / "report.md"],
            allow_resume=False,
        )
        return

    public_dir.mkdir(parents=True, exist_ok=True)
    _run_market_history_stages(args, runner, public_dir)

    if not args.skip_corporate_actions:
        corporate_mode = "rebuild" if args.operation == "from-zero" else args.mode
        corporate_command = _python_command(
            "downloader/download_tw_corporate_action_reference.py",
            "--output-dir",
            str(public_dir),
            "--mode",
            corporate_mode,
            "--start-year",
            "2000",
            "--end-date",
            args.end_date,
            "--workers",
            str(args.public_workers),
            "--timeout",
            str(args.timeout),
            "--retries",
            str(args.retries),
            "--workers",
            str(args.workers),
        )
        if args.request_interval is not None:
            corporate_command.extend(["--request-interval", str(args.request_interval)])
        if args.skip_raw:
            corporate_command.append("--skip-raw")
        runner.run(
            "corporate_action_references",
            corporate_command,
            outputs=[
                public_dir / "tw_corporate_action_reference.parquet",
                public_dir / "tw_corporate_action_reference.summary.json",
            ],
        )

    if not args.skip_symbol_build and args.ohlcv_fallback == "yahoo":
        yahoo_fallback_dir.mkdir(parents=True, exist_ok=True)
        yahoo_bootstrap = not any(yahoo_fallback_dir.glob("*_features.parquet"))
        if yahoo_bootstrap:
            yahoo_mode = "download"
        elif args.operation == "from-zero" and args.yahoo_fallback_dir is None:
            # A fixed staged rebuild is resumable: download mode skips files that
            # were already written atomically by a prior attempt.
            yahoo_mode = "download"
        elif args.mode == "daily":
            yahoo_mode = "incremental"
        else:
            yahoo_mode = "repair"
        if not args.skip_yahoo_download:
            yahoo_command = _python_command(
                "downloader/download_yahoo_ohlcv.py",
                "--mode",
                yahoo_mode,
                "--asset",
                "tw_stocks",
                "--output-dir",
                str(yahoo_fallback_dir),
                "--output-root",
                str(yahoo_fallback_dir.parent),
                "--start-date",
                fallback_start.isoformat(),
                "--end-date",
                args.end_date,
                "--workers",
                str(args.yahoo_workers),
                "--retries",
                str(args.yahoo_retries),
                "--request-interval",
                str(args.yahoo_request_interval),
                "--include-tw-delisted",
                "--tw-delisted-dir",
                str(public_dir),
                "--verify-tw-delisted-history",
                "--retry-blacklisted-repair-symbols",
                "--fail-on-any-error",
            )
            yahoo_report = (
                yahoo_fallback_dir / "download_report.csv"
                if yahoo_mode == "download"
                else yahoo_fallback_dir / "repair_report.csv"
            )
            runner.run(
                "yahoo_ohlcv_fallback_download",
                yahoo_command,
                outputs=list(
                    dict.fromkeys(
                        [
                            yahoo_fallback_dir / "symbols.csv",
                            yahoo_report,
                            yahoo_fallback_dir / "download_report.csv",
                            yahoo_fallback_dir / "download_summary.json",
                        ]
                    )
                ),
            )
        runner.run(
            "tw_transfer_adjustment_reference",
            _transfer_adjustment_command(
                args,
                public_dir=public_dir,
                yahoo_source_dir=yahoo_fallback_dir,
                output_path=transfer_adjustment_reference,
            ),
            outputs=[
                transfer_adjustment_reference,
                transfer_adjustment_reference.with_suffix(".summary.json"),
            ],
            validate_outputs=lambda: _validate_transfer_adjustment_reference(
                transfer_adjustment_reference,
                start=fallback_start,
                end=requested_end,
            ),
        )
        runner.run(
            "yahoo_ohlcv_fallback_archive",
            _python_command(
                "scripts/build_tw_yahoo_fallback_archive.py",
                "--input-dir",
                str(yahoo_fallback_dir),
                "--official-input-dir",
                str(public_dir),
                "--output-path",
                str(yahoo_fallback_archive),
                "--start-date",
                fallback_start.isoformat(),
                "--end-date",
                args.end_date,
                "--workers",
                str(args.workers),
                "--transfer-adjustment-reference",
                str(transfer_adjustment_reference),
            ),
            outputs=[
                yahoo_fallback_archive,
                yahoo_fallback_archive.with_suffix(".inputs.json"),
                yahoo_fallback_archive.with_suffix(".summary.json"),
                yahoo_fallback_archive.with_suffix(".report.csv"),
            ],
        )

    if not args.skip_symbol_build:
        symbol_build_command = _python_command(
            "scripts/build_tw_official_symbol_parquets.py",
            "--input-dir",
            str(public_dir),
            "--output-dir",
            str(stock_root),
            "--workers",
            str(args.workers),
        )
        for input_path in args.legacy_official_ohlcv:
            symbol_build_command.extend(["--legacy-official-ohlcv", str(input_path)])
        if args.legacy_source_name:
            symbol_build_command.extend(["--legacy-source-name", str(args.legacy_source_name)])
        if args.ohlcv_fallback == "yahoo":
            symbol_build_command.extend(
                [
                    "--fallback-ohlcv",
                    str(yahoo_fallback_archive),
                    "--fallback-source-name",
                    "yahoo_fallback",
                ]
            )
        runner.run(
            "official_symbol_parquets",
            symbol_build_command,
            outputs=[
                stock_root / "2330_features.parquet",
                stock_root / "symbols.csv",
                stock_root / "official_symbol_build_summary.json",
                stock_root / "official_symbol_build_report.csv",
                stock_root / "return_price_provenance.json",
            ],
            validate_outputs=lambda: _validate_official_symbol_build_summary(
                stock_root / "official_symbol_build_summary.json"
            ),
        )

    if not args.skip_corporate_actions:
        entitlement_output = (
            public_dir / "tw_corporate_action_entitlements.parquet"
        )
        entitlement_command = _python_command(
            "downloader/download_tw_corporate_action_entitlements.py",
            "--output-dir",
            str(public_dir),
            "--reference",
            str(public_dir / "tw_corporate_action_reference.parquet"),
            "--universe-report",
            str(stock_root / "official_symbol_build_report.csv"),
            "--start-date",
            str(config.data.panel_start_date or fallback_start.isoformat()),
            "--end-date",
            str(args.end_date),
            "--mode",
            str(args.mode),
            "--timeout",
            str(args.timeout),
            "--retries",
            str(args.retries),
        )
        if args.request_interval is not None:
            entitlement_command.extend(
                ["--request-interval", str(args.request_interval)]
            )
        runner.run(
            "corporate_action_entitlements",
            entitlement_command,
            outputs=[
                entitlement_output,
                entitlement_output.with_suffix(".summary.json"),
            ],
            validate_outputs=lambda: _validate_corporate_action_entitlements(
                entitlement_output,
                reference=public_dir / "tw_corporate_action_reference.parquet",
            ),
        )

    if not args.skip_short_rules:
        short_start_year = (
            int(args.end_date[:4]) if args.mode == "daily" else int(args.short_start_year)
        )
        short_rule_command = _python_command(
            "downloader/download_tw_short_sale_restrictions.py",
            "--output-dir",
            str(public_dir),
            "--start-year",
            str(short_start_year),
            "--end-year",
            str(int(args.end_date[:4])),
            "--workers",
            str(args.public_workers),
            "--timeout",
            str(args.timeout),
            "--retries",
            str(args.retries),
            "--retry-backoff",
            str(args.retry_backoff),
        )
        if args.request_interval is not None:
            short_rule_command.extend(["--request-interval", str(args.request_interval)])
        runner.run(
            "short_sale_and_lifecycle_rules",
            short_rule_command,
            outputs=[
                public_dir / "tw_delisting_short_sale_announcements.parquet",
                public_dir / "tw_short_sale_download_report.json",
            ],
        )

    if not args.skip_feature_build:
        runner.run(
            "build_public_features",
            _python_command(
                "scripts/build_tw_public_training_features.py",
                "--input-dir",
                str(public_dir),
                "--output-path",
                str(public_feature_path),
                "--symbols-root",
                str(stock_root),
                "--market-symbol",
                str(config.data.tw_public_market_symbol),
            ),
            outputs=[public_feature_path, public_feature_path.with_suffix(".summary.json")],
        )

    runner.run(
        "audit",
        _audit_command(
            args,
            stock_root=stock_root,
            public_dir=public_dir,
            public_feature_path=public_feature_path,
            output_dir=audit_output,
        ),
        outputs=[audit_output / "summary.json", audit_output / "report.md"],
        allow_resume=False,
    )

    if args.dry_run:
        print("[tw-data-rebuild] dry run complete; no data outputs were created", flush=True)
        return

    summary = json.loads((audit_output / "summary.json").read_text(encoding="utf-8"))
    if not bool(summary.get("model_safe")):
        raise RuntimeError("audit did not certify the rebuilt data layer as model-safe")

    if args.promote:
        backup_root = args.backup_root or Path("artifacts/data_backups") / run_id
        try:
            production_stocks.relative_to(production_public)
        except ValueError as exc:
            raise ValueError(
                "official stock parquet root must be contained by data_tw_public for promotion"
            ) from exc
        public_backup = Path(backup_root) / "data_tw_public"
        _promote_one(public_dir, production_public, public_backup)
        post_audit_output = Path(args.stage_root) / "audit_post_promote"
        try:
            runner.run(
                "post_promote_audit",
                _audit_command(
                    args,
                    stock_root=production_stocks,
                    public_dir=production_public,
                    public_feature_path=production_public_feature,
                    output_dir=post_audit_output,
                ),
                outputs=[post_audit_output / "summary.json", post_audit_output / "report.md"],
                allow_resume=False,
            )
        except BaseException:
            _rollback_promoted_tree(
                staged=public_dir,
                production=production_public,
                backup=public_backup,
            )
            runner.update_metadata(
                promoted=False,
                rolled_back=True,
                rolled_back_at_utc=_utc_now(),
            )
            raise
        runner.update_metadata(
            promoted=True,
            promoted_at_utc=_utc_now(),
            backup_root=str(backup_root),
        )
        print(
            f"[tw-data-rebuild] promoted production data; backup={backup_root}",
            flush=True,
        )
    else:
        runner.update_metadata(promoted=False)
        print(
            f"[tw-data-rebuild] validated staged data at {args.stage_root}; promotion not requested",
            flush=True,
        )


if __name__ == "__main__":
    main()

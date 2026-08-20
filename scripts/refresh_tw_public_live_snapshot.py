#!/usr/bin/env python3
"""Refresh the shared TW public live tree, then publish and pin one snapshot.

All four TW day-trade deployments call this same entrypoint.  The mutable
download tree, Syncthing/desync CAS, and verified inference snapshot remain
separate; the repository's ``data_tw_public`` symlink changes only after the
new snapshot has passed downloader and content verification gates.
"""

from __future__ import annotations

import argparse
from datetime import datetime
import fcntl
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import uuid
from zoneinfo import ZoneInfo


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from stockagent.data_sync.desync_snapshots import (  # noqa: E402
    fetch_snapshot,
    publish_snapshot,
    write_pin,
)
from stockagent.live.market_config import LiveMarketConfig  # noqa: E402
from stockagent.live.market_status import expected_latest_data_date  # noqa: E402
from stockagent.live.tw_day_trade_simulation import (  # noqa: E402
    require_exact_session_eligibility,
)


TAIPEI = ZoneInfo("Asia/Taipei")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("configs/markets/tw_day_trade_10m.yaml"),
    )
    parser.add_argument(
        "--live-root", type=Path, default=Path("/srv/stockagent-live/data_tw_public")
    )
    parser.add_argument(
        "--sync-root", type=Path, default=Path("/srv/stockagent-sync")
    )
    parser.add_argument(
        "--materialized-root",
        type=Path,
        default=Path("/srv/stockagent-snapshots"),
    )
    parser.add_argument(
        "--pin",
        type=Path,
        default=Path("/srv/stockagent-snapshots/tw-public.pin.json"),
    )
    parser.add_argument(
        "--receipt",
        type=Path,
        default=Path("artifacts/data_refresh/tw_public/latest.json"),
    )
    parser.add_argument(
        "--reuse-live-download",
        action="store_true",
        help=(
            "reuse the mutable live tree after validating its download summary "
            "and rerunning the strict model-safety audit before publication"
        ),
    )
    parser.add_argument("--force", action="store_true")
    return parser.parse_args()


def _json(path: Path) -> dict[str, object]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise RuntimeError(f"JSON root is not an object: {path}")
    return payload


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _atomic_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + f".tmp.{uuid.uuid4().hex}")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


def _expected_latest(live_root: Path) -> str:
    cfg = LiveMarketConfig(
        market="tw_day_trade_shared_data",
        label="TW day-trade shared data",
        config_path="configs/markets/tw_day_trade_10m.yaml",
        market_type="tw",
        timezone="Asia/Taipei",
        data_ready_time="13:40",
    )
    expected = expected_latest_data_date(
        cfg,
        market_type="tw",
        now=datetime.now(TAIPEI),
        parquet_root=live_root / "stocks",
    )
    if expected is None:
        raise RuntimeError("cannot resolve expected latest TW trading date")
    return expected


def _validate_download_summary(
    summary: dict[str, object], *, expected_latest: str
) -> None:
    failures: list[str] = []
    end_date = str(summary.get("end_date") or "")
    if end_date < expected_latest:
        failures.append(f"end_date={end_date!r} < expected={expected_latest!r}")
    if summary.get("daily_close_ready") is not True:
        failures.append("daily_close_ready is not true")
    if summary.get("coverage_complete") is not True:
        failures.append("coverage_complete is not true")
    if int(summary.get("blocking_failed_count") or 0) != 0:
        failures.append(
            f"blocking_failed_count={summary.get('blocking_failed_count')}"
        )
    if int(summary.get("missing_dates_after") or 0) != 0:
        failures.append(f"missing_dates_after={summary.get('missing_dates_after')}")
    if failures:
        raise RuntimeError("TW public refresh failed closed: " + "; ".join(failures))


def _bootstrap_live_tree(live_root: Path, source: Path) -> None:
    if live_root.exists():
        return
    source = source.resolve(strict=True)
    staging = live_root.parent / f".{live_root.name}.bootstrap.{uuid.uuid4().hex}"
    staging.mkdir(parents=True, exist_ok=False)
    try:
        subprocess.run(
            [
                "cp",
                "-a",
                "--reflink=auto",
                "--sparse=always",
                f"{source}/.",
                str(staging),
            ],
            check=True,
        )
        os.replace(staging, live_root)
    except Exception:
        # Preserve an interrupted staging tree for operator inspection; never
        # delete a large partial dataset implicitly.
        raise


def _switch_repo_symlink(target: Path) -> None:
    link = REPO_ROOT / "data_tw_public"
    if link.exists() and not link.is_symlink():
        raise RuntimeError(f"refusing to replace non-symlink dataset path: {link}")
    temporary = REPO_ROOT / f".data_tw_public.next.{uuid.uuid4().hex}"
    os.symlink(target.resolve(strict=True), temporary, target_is_directory=True)
    os.replace(temporary, link)


def _can_reuse_today(
    receipt_path: Path, *, expected_latest: str, active_link: Path
) -> dict[str, object] | None:
    if not receipt_path.is_file() or not active_link.is_symlink():
        return None
    try:
        receipt = _json(receipt_path)
        target = Path(str(receipt["materialized_path"])).resolve(strict=True)
        active = active_link.resolve(strict=True)
    except (KeyError, OSError, RuntimeError, ValueError):
        return None
    if receipt.get("status") != "ok" or target != active:
        return None
    if str(receipt.get("expected_latest_date") or "") != expected_latest:
        return None
    return receipt


def _downloader_command(
    *, python: str, downloader: Path, config_path: Path, live_root: Path
) -> list[str]:
    return [
        python,
        str(downloader),
        "--mode",
        "daily",
        "--config",
        str(config_path),
        "--public-dir",
        str(live_root),
        "--stock-root",
        str(live_root / "stocks"),
        "--public-feature-path",
        str(live_root / "features" / "tw_public_stock_daily.parquet"),
        "--public-workers",
        "2",
        "--date-workers",
        "4",
        "--workers",
        "8",
        "--ohlcv-fallback",
        "none",
        "--fallback-start-date",
        "2000-01-01",
        "--skip-raw",
        "--require-live-selected-features",
    ]


def _same_session_rule_command(
    *,
    python: str,
    live_root: Path,
    trading_date: str,
) -> list[str]:
    return [
        python,
        str(REPO_ROOT / "downloader" / "download_tw_public_data.py"),
        "--mode",
        "daily",
        "--datasets",
        "twse_day_trade_eligibility",
        "tpex_day_trade_eligibility",
        "--end-date",
        trading_date,
        "--same-session-rule-date",
        trading_date,
        "--output-dir",
        str(live_root),
        "--workers",
        "2",
        "--date-workers",
        "2",
        "--daily-overlap-days",
        "1",
        "--require-taiex-session-calendar",
        "--no-progress",
        "--no-write-run-metadata",
    ]


def _refresh_same_session_rules(
    live_root: Path,
    *,
    observed: datetime,
) -> dict[str, object]:
    trading_date = observed.astimezone(TAIPEI).date()
    try:
        coverage = require_exact_session_eligibility(
            rule_data_dir=live_root,
            parquet_root=live_root / "stocks",
            trading_date=trading_date,
        )
    except RuntimeError:
        coverage = None
    if coverage is not None:
        return {
            "trading_date": trading_date.isoformat(),
            "venues": coverage,
            "refresh_attempted": False,
            "source": "existing_exact_session_coverage",
        }
    subprocess.run(
        _same_session_rule_command(
            python=sys.executable,
            live_root=live_root,
            trading_date=trading_date.isoformat(),
        ),
        cwd=REPO_ROOT,
        check=True,
    )
    coverage = require_exact_session_eligibility(
        rule_data_dir=live_root,
        parquet_root=live_root / "stocks",
        trading_date=trading_date,
    )
    return {
        "trading_date": trading_date.isoformat(),
        "venues": coverage,
        "refresh_attempted": True,
        "source": "same_session_official_refresh",
    }


def _audit_command(
    *,
    python: str,
    config_path: Path,
    live_root: Path,
    output_dir: Path,
) -> list[str]:
    return [
        python,
        str(REPO_ROOT / "scripts" / "audit_tw_public_data_layer.py"),
        "--config",
        str(config_path),
        "--parquet-root",
        str(live_root / "stocks"),
        "--public-dir",
        str(live_root),
        "--public-feature-path",
        str(live_root / "features" / "tw_public_stock_daily.parquet"),
        "--output-dir",
        str(output_dir),
        "--build-panel",
        "--strict",
        "--require-live-selected-features",
    ]


def main() -> int:
    args = parse_args()
    live_root = args.live_root.expanduser().resolve(strict=False)
    sync_root = args.sync_root.expanduser().resolve(strict=True)
    materialized_root = args.materialized_root.expanduser().resolve(strict=True)
    pin_path = args.pin.expanduser().resolve(strict=False)
    receipt_path = (
        args.receipt
        if args.receipt.is_absolute()
        else (REPO_ROOT / args.receipt)
    )
    config_path = (
        args.config if args.config.is_absolute() else (REPO_ROOT / args.config)
    ).resolve(strict=True)
    lock_path = live_root.parent / ".locks" / "tw-public-refresh.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)

    with lock_path.open("a+", encoding="utf-8") as lock_handle:
        fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX)
        observed = datetime.now(TAIPEI)
        expected_latest = _expected_latest(
            live_root if live_root.exists() else (REPO_ROOT / "data_tw_public")
        )
        active_source = (REPO_ROOT / "data_tw_public").resolve(strict=True)
        if live_root == active_source:
            raise RuntimeError(
                "mutable live root resolves to the active inference snapshot"
            )
        _bootstrap_live_tree(live_root, active_source)
        if not args.force:
            reused = _can_reuse_today(
                receipt_path,
                expected_latest=expected_latest,
                active_link=REPO_ROOT / "data_tw_public",
            )
            if reused is not None:
                same_session = _refresh_same_session_rules(
                    live_root,
                    observed=observed,
                )
                reused_payload = {
                    **reused,
                    "same_session_eligibility": same_session,
                    "reused": True,
                }
                _atomic_json(receipt_path, reused_payload)
                print(json.dumps(reused_payload, ensure_ascii=False))
                return 0

        summary_path = live_root / "download_summary.json"
        if args.reuse_live_download:
            summary = _json(summary_path)
            _validate_download_summary(summary, expected_latest=expected_latest)
            audit_output = (
                REPO_ROOT
                / "artifacts"
                / "data_refresh"
                / "tw_public"
                / "audit"
                / datetime.now(TAIPEI).strftime("%Y%m%dT%H%M%S")
            )
            subprocess.run(
                _audit_command(
                    python=sys.executable,
                    config_path=config_path,
                    live_root=live_root,
                    output_dir=audit_output,
                ),
                cwd=REPO_ROOT,
                check=True,
            )
        else:
            downloader = REPO_ROOT / "downloader" / "download_tw_official_data.py"
            command = _downloader_command(
                python=sys.executable,
                downloader=downloader,
                config_path=config_path,
                live_root=live_root,
            )
            subprocess.run(command, cwd=REPO_ROOT, check=True)

        summary = _json(summary_path)
        _validate_download_summary(summary, expected_latest=expected_latest)
        same_session = _refresh_same_session_rules(
            live_root,
            observed=observed,
        )
        resolved = publish_snapshot(
            sync_root,
            "tw-public",
            live_root,
            metadata={
                "audit": "strict",
                "storage_frequency": "daily",
                "expected_latest_date": expected_latest,
                "download_summary_sha256": _sha256(summary_path),
            },
            repo_root=REPO_ROOT,
        )
        target = fetch_snapshot(sync_root, materialized_root, resolved)
        write_pin(pin_path, resolved)
        _switch_repo_symlink(target)

        receipt: dict[str, object] = {
            "schema_version": 1,
            "status": "ok",
            "completed_at_taipei": datetime.now(TAIPEI).isoformat(),
            "expected_latest_date": expected_latest,
            "download_end_date": summary.get("end_date"),
            "coverage_complete": summary.get("coverage_complete"),
            "download_summary": str(summary_path),
            "download_summary_sha256": _sha256(summary_path),
            "snapshot_id": resolved.manifest["snapshot_id"],
            "manifest_sha256": resolved.manifest_sha256,
            "materialized_path": str(target),
            "pin_path": str(pin_path),
            "config_path": str(config_path),
            "same_session_eligibility": same_session,
            "reused_live_download": bool(args.reuse_live_download),
            "reused": False,
        }
        _atomic_json(receipt_path, receipt)
        print(json.dumps(receipt, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

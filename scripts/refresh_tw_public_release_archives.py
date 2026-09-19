#!/usr/bin/env python3
"""Refresh verified Taiwan macro release originals, then reconcile training features.

DGBAS and CBC use independent hosts, so they run concurrently. CBC archives
share a host and are deliberately serialized. A failed archive cannot advance
the model-facing feature table; the prior atomic Parquet remains in place.
"""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import datetime
import fcntl
import json
from pathlib import Path
import subprocess
import sys
from zoneinfo import ZoneInfo


REPO_ROOT = Path(__file__).resolve().parents[1]
ARCHIVES = ("dgbas_release_vintages", "cbc_fx_reserve_release_vintages",
            "cbc_money_release_vintages", "cbc_overnight_official_pages")
SCRIPTS = {
    "dgbas_release_vintages": "download_tw_dgbas_release_archive.py",
    "cbc_fx_reserve_release_vintages": "download_tw_cbc_fx_release_archive.py",
    "cbc_money_release_vintages": "download_tw_cbc_money_release_archive.py",
    "cbc_overnight_official_pages": "download_tw_cbc_overnight_pages.py",
}


def _money_recent_pages(root: Path, *, full_index: bool) -> int:
    if full_index:
        return 0
    try:
        state = json.loads((root / "state" / "cbc_money_release_vintages.json").read_text(
            encoding="utf-8"
        ))
        if state.get("complete") is True and state.get("status") == "complete":
            return 2
    except (OSError, ValueError, TypeError):
        pass
    return 0


def _mof_recent_pages(root: Path) -> int:
    if datetime.now(ZoneInfo("Asia/Taipei")).weekday() == 6:
        return 0
    try:
        state = json.loads((root / "state/mof_macro_release_dates.json").read_text(
            encoding="utf-8"
        ))
        if state.get("full_history_scanned") and "tax_pdf_fallback" in state:
            return 2
    except (OSError, ValueError, TypeError):
        pass
    return 0


def _command(name: str, root: Path, *, money_recent_pages: int) -> list[str]:
    command = [sys.executable, str(REPO_ROOT / "downloader" / SCRIPTS[name]),
               "--output-dir", str(root)]
    if name != "cbc_overnight_official_pages":
        command += ["--workers", "4"]
    if name == "dgbas_release_vintages":
        command += ["--request-interval", "1.0"]
    elif name == "cbc_fx_reserve_release_vintages":
        command += ["--request-interval", "0.25"]
    elif name == "cbc_money_release_vintages":
        command += ["--request-interval", "0.25", "--recent-pages", str(money_recent_pages)]
    else:
        command += ["--request-interval", "0.5", "--recent-pages", "0" if money_recent_pages == 0 else "10",
                    "--source-update-lock-held"]
    return command


@contextmanager
def _source_update_lock(root: Path):
    # Share the canonical TW-public producer lock with the completed-session
    # finalizer and publication sweep. An overlapping build must never take a
    # source receipt while originals are being promoted underneath it.
    path = root.parent / ".locks" / "tw-public-refresh.lock"
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+") as handle:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RuntimeError("Taiwan public source update is already active; retry later") from exc
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def _refresh(root: Path, commands: dict[str, list[str]]) -> None:
    def run(name: str) -> None:
        subprocess.run(commands[name], cwd=REPO_ROOT, check=True)

    failures: list[str] = []
    with ThreadPoolExecutor(max_workers=2) as pool:
        dgbas = pool.submit(run, "dgbas_release_vintages")
        for name in ("cbc_fx_reserve_release_vintages", "cbc_money_release_vintages",
                     "cbc_overnight_official_pages"):
            try:
                run(name)
            except subprocess.CalledProcessError as exc:
                failures.append(f"{name}: exit {exc.returncode}")
        try:
            dgbas.result()
        except subprocess.CalledProcessError as exc:
            failures.append(f"dgbas_release_vintages: exit {exc.returncode}")
    if failures:
        raise RuntimeError("official release refresh failed: " + "; ".join(failures))

    # Annual CBC FX pages are mostly static; scan on first acquisition and
    # weekly thereafter, not three times each trading day.
    annual_path = root / "supplemental/cbc_usdtwd_annual_pages.parquet"
    if not annual_path.is_file() or datetime.now(ZoneInfo("Asia/Taipei")).weekday() == 6:
        subprocess.run(
            [sys.executable, str(REPO_ROOT / "downloader" /
             "download_tw_cbc_fx_annual_pages.py"),
             "--output-dir", str(root), "--request-interval", "0.5",
             "--source-update-lock-held"],
            cwd=REPO_ROOT, check=True,
        )

    audit = [sys.executable, str(REPO_ROOT / "scripts" /
            "audit_tw_official_release_archives.py"), "--root", str(root),
             "--require-complete"]
    subprocess.run(audit, cwd=REPO_ROOT, check=True)
    reconcile = [sys.executable, str(REPO_ROOT / "scripts" /
                 "reconcile_tw_public_training_features.py"),
                 "--input-dir", str(root), "--symbols-root", str(root / "stocks"),
                 "--output-path", str(root / "features" / "tw_public_stock_daily.parquet"),
                 "--defer-market-hours", "--source-update-lock-held"]
    subprocess.run(reconcile, cwd=REPO_ROOT, check=True)
    # MOF is a separate host and only enriches the research-only publication
    # ledger. A failure here cannot roll back the already-audited strict table.
    subprocess.run(
        [sys.executable, str(REPO_ROOT / "downloader" /
         "download_tw_mof_macro_release_dates.py"),
         "--output-dir", str(root), "--request-interval", "0.5",
         "--recent-pages", str(_mof_recent_pages(root)),
         "--source-update-lock-held"],
        cwd=REPO_ROOT, check=True,
    )
    subprocess.run(
        [sys.executable, str(REPO_ROOT / "downloader" /
         "download_tw_mof_trade_release_values.py"),
         "--output-dir", str(root), "--request-interval", "0.5",
         "--source-update-lock-held"],
        cwd=REPO_ROOT, check=True,
    )
    # Preserve the indexed original MOF documents after the strict feature
    # reconciliation. An old subject month does not establish first-published
    # bytes, so this archive is never used to upgrade PIT eligibility here.
    mof_archive = [sys.executable, str(REPO_ROOT / "downloader" /
                   "download_tw_mof_release_archive.py"),
                   "--output-dir", str(root), "--request-interval", "0.5",
                   "--source-update-lock-held"]
    if datetime.now(ZoneInfo("Asia/Taipei")).weekday() == 6:
        mof_archive.append("--full-recheck")
    subprocess.run(mof_archive, cwd=REPO_ROOT, check=True)
    # MOF's latest original-release clock may refine the provisional ledger.
    # Refresh it only after all original archives, then publish the separate
    # wide research table without altering the strict PIT feature contract.
    subprocess.run(
        [sys.executable, str(REPO_ROOT / "scripts" /
         "build_tw_public_provisional_macro.py"),
         "--input-dir", str(root), "--source-update-lock-held"],
        cwd=REPO_ROOT, check=True,
    )
    subprocess.run(
        [sys.executable, str(REPO_ROOT / "scripts" /
         "build_tw_public_research_features.py"),
         "--input-dir", str(root),
         "--base-path", str(root / "features" / "tw_public_stock_daily.parquet"),
         "--output-path", str(root / "features" / "tw_public_research_wide_2014_v1.parquet"),
         "--source-update-lock-held"],
        cwd=REPO_ROOT, check=True,
    )
    subprocess.run(
        [sys.executable, str(REPO_ROOT / "scripts" /
         "build_tw_public_research_taifex.py"),
         "--base-path", str(root / "features" / "tw_public_research_wide_2014_v1.parquet")],
        cwd=REPO_ROOT, check=True,
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, default=Path("data_tw_public"))
    parser.add_argument("--full-index", action="store_true",
                        help="Scan every CBC money index page; use weekly to catch old-page additions.")
    parser.add_argument("--auto-full-sunday", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    root = args.output_dir.resolve()
    full_index = args.full_index or (
        args.auto_full_sunday and datetime.now(ZoneInfo("Asia/Taipei")).weekday() == 6
    )
    recent_pages = _money_recent_pages(root, full_index=full_index)
    commands = {name: _command(name, root, money_recent_pages=recent_pages)
                for name in ARCHIVES}
    if args.dry_run:
        print(json.dumps({"full_index": full_index, "commands": commands},
                         ensure_ascii=False, indent=2))
        return 0
    with _source_update_lock(root):
        _refresh(root, commands)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Fill the pre-2000 CFTC legacy COT gap from official compressed archives."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
from io import BytesIO
import json
from pathlib import Path
import re
import sys
import zipfile

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from stockagent.data_sync.desync_snapshots import (  # noqa: E402
    atomic_write_bytes as _atomic_write_bytes,
    atomic_write_json as _write_json,
    sha256_file as sha256_path,
)
from downloader.http_transport import (  # noqa: E402
    HttpRequestPolicy,
    ResilientHttpTransport,
)


CONTRACT_VERSION = 1
ARCHIVES = (
    (
        "legacy_futures_only",
        "https://www.cftc.gov/files/dea/history/deacot1986_2016.zip",
        "FUT86_16.txt",
    ),
    (
        "legacy_futures_and_options_combined",
        "https://www.cftc.gov/files/dea/history/deahistfo_1995_2016.zip",
        "Com95_16.txt",
    ),
)
DATE_COLUMN = "as_of_date_in_form_yyyy_mm_dd"


def _relative(path: Path, root: Path) -> str:
    return path.relative_to(root).as_posix()


def _sha256_bytes(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _column_name(value: object) -> str:
    text = str(value).strip().casefold()
    text = text.replace("%", " pct ").replace("#", " number ")
    return re.sub(r"[^a-z0-9]+", "_", text).strip("_")


def _stable_legacy_types(frame: pd.DataFrame) -> pd.DataFrame:
    """Make year/report variants Arrow-compatible without losing code zeros."""
    import pandas as pd

    result = frame.copy()
    text_tokens = (
        "name",
        "code",
        "initial",
        "report_mode",
        "availability_rule",
        "source_zip_sha256",
    )
    for column in result.columns:
        if pd.api.types.is_datetime64_any_dtype(result[column]):
            continue
        if any(token in column for token in text_tokens):
            result[column] = result[column].astype("string").str.strip()
            continue
        source = result[column].astype("string").str.strip()
        present = source.notna() & source.ne("")
        numeric = pd.to_numeric(source.where(present), errors="coerce")
        if int(numeric.notna().sum()) == int(present.sum()):
            result[column] = numeric.astype("Float64")
        else:
            result[column] = source
    return result


def _normalize(
    frame: pd.DataFrame, *, report_mode: str, source_sha256: str
) -> pd.DataFrame:
    import pandas as pd
    frame = frame.copy()
    frame.columns = [_column_name(column) for column in frame.columns]
    if DATE_COLUMN not in frame:
        raise ValueError(f"CFTC archive omitted {DATE_COLUMN}")
    frame[DATE_COLUMN] = pd.to_datetime(frame[DATE_COLUMN], errors="raise")
    frame = frame.loc[frame[DATE_COLUMN] < pd.Timestamp("2000-01-01")].copy()
    if frame.empty:
        raise ValueError("CFTC archive had no pre-2000 rows")
    frame.insert(0, "report_mode", report_mode)
    frame.insert(1, "date", frame.pop(DATE_COLUMN))
    # Historical archives omit an exact release timestamp.  Seven calendar
    # days after the Tuesday as-of date is a conservative, leakage-safe bound.
    frame.insert(2, "conservative_available_date", frame["date"] + pd.Timedelta(days=7))
    frame.insert(3, "availability_rule", "as_of_date_plus_7_calendar_days_conservative")
    frame.insert(4, "source_zip_sha256", source_sha256)
    frame = frame.sort_values(
        ["date", "cftc_contract_market_code"], kind="stable"
    ).reset_index(drop=True)
    return _stable_legacy_types(frame)


def _write_parquet(frame: pd.DataFrame, path: Path) -> None:
    import pyarrow as pa
    from downloader.artifact_io import atomic_write_parquet
    table = pa.Table.from_pandas(frame, preserve_index=False)
    atomic_write_parquet(path, table, compression="zstd")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", default="data_cftc_legacy")
    parser.add_argument("--max-retries", type=int, default=4)
    parser.add_argument("--timeout", type=float, default=300.0)
    parser.add_argument("--sync-root", type=Path, default=Path("/srv/stockagent-packed"))
    parser.add_argument("--retention-config", type=Path,
                        default=REPO_ROOT / "configs/data_sync/packed_retention.json")
    parser.add_argument("--adopt-closed-release", metavar="FIXED_SNAPSHOT_ID")
    parser.add_argument("--source-lock-fd", type=int)
    parser.add_argument("--retire-closed-raw", action="store_true",
                        help="Dry-run exact raw retirement through the original storage owner")
    parser.add_argument("--apply-raw-retirement", metavar="PLAN_FINGERPRINT")
    parser.add_argument("--retirement-policy", type=Path,
                        default=REPO_ROOT / "configs/data_sync/artifact_retirement.json")
    parser.add_argument("--retirement-state-root", type=Path,
                        default=Path("/var/lib/stockagent-cold-artifacts"))
    args = parser.parse_args(argv)
    if args.apply_raw_retirement and not args.retire_closed_raw:
        parser.error("--apply-raw-retirement requires --retire-closed-raw")
    if args.adopt_closed_release and args.retire_closed_raw:
        parser.error("adopt and verify the closed release before its separate retirement")
    root = Path(args.output_dir).expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    # Direct and scheduled invocations borrow/acquire the same original lock.
    # Keep it through the complete transaction, including ordinary downloads.
    from scripts.publish_data_releases import _source_coordination_lock
    entry = {"dataset": "cftc-legacy-pre2000", "source": str(root),
             "source_coordination_lock_scope": "source_parent",
             "source_coordination_lock": root.name + "/state/collector.lock"}
    with _source_coordination_lock(entry, inherited_fd=args.source_lock_fd) as owner:
        return _collect(root, args, owner)


def _collect(root: Path, args: argparse.Namespace, owner: int) -> int:
    from stockagent.data_sync.closed_source import (
        BINDING, adopt_closed_source, configured_cache_root, resolve_closed_source,
    )
    if args.retire_closed_raw or args.adopt_closed_release or (root / BINDING).exists() or (root / BINDING).is_symlink():
        cold = args.sync_root.absolute()
        cache = configured_cache_root(cold, args.retention_config, repo_root=REPO_ROOT)
        if args.retire_closed_raw:
            from dataclasses import replace
            from stockagent.data_sync.packed_retention import RetentionConfig
            from stockagent.data_sync.artifact_retirement import (
                apply_closed_source_raw_retirement, plan_closed_source_raw_retirement,
                load_retirement_peer_names,
            )
            from scripts.manage_packed_retention import _syncthing
            config = RetentionConfig.load(args.retention_config, repo_root=REPO_ROOT)
            peers = load_retirement_peer_names(args.retirement_policy,
                                               authority_node_id=config.authority_node_id)
            config = replace(config, required_peer_names=peers)
            options = {"source": root, "config": config, "policy_path": args.retirement_policy,
                "state_root": args.retirement_state_root, "source_lock_fd": owner,
                "repo_root": REPO_ROOT, "peer_probe": lambda: _syncthing(config)}
            result = (apply_closed_source_raw_retirement(
                expected_fingerprint=args.apply_raw_retirement, **options)
                if args.apply_raw_retirement else plan_closed_source_raw_retirement(**options))
            print(json.dumps(result, ensure_ascii=False), flush=True)
            return 75 if result.get("blockers") else 0
        if args.adopt_closed_release:
            closed = adopt_closed_source(root, cold, cache,
                snapshot_id=args.adopt_closed_release, source_lock_fd=owner)
        else:
            closed = resolve_closed_source(root, cold, cache)
        print(json.dumps({"status": "closed_history_verified", "storage": closed[1],
            "provider_API_calls": 0, "parquet_rewrites": 0, "raw_downloads": 0,
            "source_files_deleted": 0}, ensure_ascii=False), flush=True)
        return 0
    import pandas as pd

    transport = ResilientHttpTransport(
        HttpRequestPolicy(
            provider="cftc_public_archive",
            timeout_seconds=args.timeout,
            max_retries=args.max_retries,
            retry_base_seconds=1.0,
            retry_cap_seconds=60.0,
        )
    )
    summaries: list[dict[str, object]] = []
    frames: list[pd.DataFrame] = []
    for report_mode, url, member in ARCHIVES:
        receipt_path = root / "receipts" / f"{report_mode}.json"
        cached_receipt: dict[str, object] | None = None
        if receipt_path.is_file():
            candidate = json.loads(receipt_path.read_text(encoding="utf-8"))
            cached_path = root / str(candidate.get("raw_path", ""))
            if (
                candidate.get("contract_version") == CONTRACT_VERSION
                and candidate.get("dataset") == report_mode
                and cached_path.is_file()
                and cached_path.stat().st_size == int(candidate.get("raw_bytes", -1))
                and sha256_path(cached_path) == candidate.get("raw_sha256")
            ):
                cached_receipt = candidate
                content = cached_path.read_bytes()
        if cached_receipt is None:
            content = transport.request_bytes(
                url,
                headers={"User-Agent": "stockAgent/cftc-official-legacy-archive"},
            ).body
        if not zipfile.is_zipfile(BytesIO(content)):
            raise ValueError(f"CFTC response is not a ZIP: {url}")
        digest = _sha256_bytes(content)
        raw_path = root / "raw" / f"{report_mode}_{digest}.zip"
        _atomic_write_bytes(raw_path, content)
        with zipfile.ZipFile(BytesIO(content)) as archive:
            if member not in archive.namelist():
                raise ValueError(f"CFTC ZIP omitted {member}: {url}")
            with archive.open(member) as handle:
                source = pd.read_csv(handle, low_memory=False)
        frame = _normalize(source, report_mode=report_mode, source_sha256=digest)
        output = root / "normalized" / f"{report_mode}_pre2000.parquet"
        _write_parquet(frame, output)
        receipt = {
            "contract_version": CONTRACT_VERSION,
            "dataset": report_mode,
            "status": "complete",
            "source_url": url,
            "source_member": member,
            "downloaded_at_utc": _utc_now(),
            "raw_path": _relative(raw_path, root),
            "raw_bytes": raw_path.stat().st_size,
            "raw_sha256": sha256_path(raw_path),
            "rows": len(frame),
            "date_start": frame["date"].min().date().isoformat(),
            "date_end": frame["date"].max().date().isoformat(),
            "normalized_path": _relative(output, root),
            "normalized_bytes": output.stat().st_size,
            "normalized_sha256": sha256_path(output),
            "availability_rule": "as_of_date_plus_7_calendar_days_conservative",
        }
        _write_json(receipt_path, receipt)
        summaries.append(receipt)
        frames.append(frame)
        print(
            f"[cftc] {report_mode} rows={len(frame):,} "
            f"{receipt['date_start']}..{receipt['date_end']}",
            flush=True,
        )

    combined = _stable_legacy_types(pd.concat(frames, ignore_index=True, sort=False))
    combined_output = root / "normalized" / "legacy_pre2000.parquet"
    _write_parquet(combined, combined_output)
    openbb_path = Path("data_openBB/compact/cftc/cot/archive.parquet").resolve()
    manifest = {
        "contract_version": CONTRACT_VERSION,
        "dataset": "cftc_legacy_pre2000",
        "status": "complete",
        "scope": "official_legacy_cot_rows_strictly_before_2000_only",
        "post_2000_authority": str(openbb_path),
        "post_2000_authority_present": openbb_path.is_file(),
        "post_2000_authority_sha256": sha256_path(openbb_path)
        if openbb_path.is_file()
        else None,
        "rows": len(combined),
        "date_start": combined["date"].min().date().isoformat(),
        "date_end": combined["date"].max().date().isoformat(),
        "report_modes": summaries,
        "output_path": _relative(combined_output, root),
        "output_bytes": combined_output.stat().st_size,
        "output_sha256": sha256_path(combined_output),
        "completed_at_utc": _utc_now(),
    }
    _write_json(root / "manifest.json", manifest)
    print(json.dumps(manifest, ensure_ascii=False, indent=2), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

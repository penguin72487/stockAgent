from __future__ import annotations

import argparse
import json
import os
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from common import resolve_end_date, run_parallel_tasks

try:
    import pyarrow.parquet as pq
except Exception:  # pragma: no cover - optional dependency guard
    pq = None


FEATURE_SUFFIX = "_features.parquet"
REQUIRED_COLUMNS = ("date", "open", "max", "min", "close")
READ_COLUMNS = ("date", "open", "max", "min", "close", "adjclose", "Trading_Volume")
INTRADAY_ROOT_HINTS = ("crypto", "data_okx", "data_bybit")
DEFAULT_ROOTS = (
    "data_yahoo/tw_stocks",
    "data_yahoo/us_stocks",
    "data_yahoo/forex",
    "data_yahoo/crypto",
    "data_okx",
    "data_bybit",
    "data_forex_frankfurter",
    "data_peperstone",
)


@dataclass(slots=True)
class AuditResult:
    root: str
    code: str
    path: str
    status: str
    rows: int = 0
    first_date: str | None = None
    last_date: str | None = None
    stale_lag_days: int | None = None
    duplicate_dates: int = 0
    invalid_dates: int = 0
    max_gap_seconds: float | None = None
    gap_count: int = 0
    missing_columns: str | None = None
    nan_ohlc_rows: int = 0
    bad_ohlc_rows: int = 0
    nonpositive_price_rows: int = 0
    negative_volume_rows: int = 0
    issues: str | None = None
    message: str | None = None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Audit saved OHLCV parquet files for common data quality issues.")
    parser.add_argument("--roots", nargs="+", default=list(DEFAULT_ROOTS), help="Data roots to scan.")
    parser.add_argument("--output-dir", default="artifacts/data_quality", help="Directory for audit artifacts.")
    parser.add_argument("--run-id", default=None, help="Optional run id. Defaults to UTC timestamp.")
    parser.add_argument("--workers", type=int, default=max(1, os.cpu_count() or 1), help="Concurrent file readers.")
    parser.add_argument("--end-date", default="today", help="Target end date for stale checks.")
    parser.add_argument("--stale-max-lag-days", type=int, default=14, help="Warn when latest row is older than this.")
    parser.add_argument("--daily-gap-days", type=int, default=10, help="Warn on daily data gaps larger than this.")
    parser.add_argument(
        "--intraday-gap-multiple",
        type=float,
        default=4.0,
        help="Warn when intraday gaps exceed inferred interval times this multiple.",
    )
    parser.add_argument("--limit", type=int, default=None, help="Optional file limit for smoke checks.")
    return parser.parse_args()


def _utc_run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _code_from_path(path: Path) -> str:
    name = path.name
    if name.endswith(FEATURE_SUFFIX):
        return name[: -len(FEATURE_SUFFIX)]
    return path.stem


def _is_intraday_root(root: Path) -> bool:
    root_text = str(root).lower()
    return any(hint in root_text for hint in INTRADAY_ROOT_HINTS)


def _schema_columns(path: Path) -> set[str] | None:
    if pq is None:
        return None
    try:
        return set(pq.read_schema(path).names)
    except Exception:
        return None


def _read_audit_frame(path: Path) -> tuple[pd.DataFrame, set[str]]:
    schema_columns = _schema_columns(path)
    if schema_columns is not None:
        columns = [column for column in READ_COLUMNS if column in schema_columns]
        if not columns:
            return pd.DataFrame(), schema_columns
        return pd.read_parquet(path, columns=columns), schema_columns

    frame = pd.read_parquet(path)
    return frame[[column for column in READ_COLUMNS if column in frame.columns]].copy(), set(frame.columns)


def _summarize_gaps(
    timestamps: pd.Series,
    *,
    root: Path,
    daily_gap_days: int,
    intraday_gap_multiple: float,
) -> tuple[float | None, int]:
    if len(timestamps) < 3:
        return None, 0

    sorted_ts = timestamps.sort_values()
    deltas = sorted_ts.diff().dropna().dt.total_seconds()
    deltas = deltas[deltas > 0]
    if deltas.empty:
        return None, 0

    median_delta = float(deltas.median())
    max_gap = float(deltas.max())
    intraday = _is_intraday_root(root) or median_delta < 12 * 60 * 60
    if intraday:
        expected_seconds = max(1.0, median_delta)
        threshold = expected_seconds * max(1.0, float(intraday_gap_multiple))
    else:
        threshold = max(1, int(daily_gap_days)) * 24 * 60 * 60
    return max_gap, int((deltas > threshold).sum())


def _audit_file(payload: tuple[Path, Path, argparse.Namespace]) -> AuditResult:
    root, path, args = payload
    code = _code_from_path(path)
    result = AuditResult(root=str(root), code=code, path=str(path), status="ok")
    issues: list[str] = []

    try:
        frame, columns = _read_audit_frame(path)
    except Exception as exc:
        return AuditResult(
            root=str(root),
            code=code,
            path=str(path),
            status="failed",
            issues="read_error",
            message=str(exc),
        )

    result.rows = int(len(frame))
    missing_required = [column for column in REQUIRED_COLUMNS if column not in columns]
    if missing_required:
        issues.append("missing_columns")
        result.missing_columns = ",".join(missing_required)
    if "date" not in frame.columns or frame.empty:
        result.status = "failed"
        result.issues = "|".join(issues or ["empty_or_missing_date"])
        return result

    parsed_dates = pd.to_datetime(frame["date"], errors="coerce", utc=True)
    valid_dates = parsed_dates.dropna()
    result.invalid_dates = int(parsed_dates.isna().sum())
    if valid_dates.empty:
        result.status = "failed"
        issues.append("no_valid_dates")
        result.issues = "|".join(sorted(set(issues)))
        return result

    result.first_date = valid_dates.min().date().isoformat()
    result.last_date = valid_dates.max().date().isoformat()
    target_end = pd.Timestamp(resolve_end_date(str(args.end_date))).date()
    result.stale_lag_days = int((target_end - valid_dates.max().date()).days)
    if result.stale_lag_days > args.stale_max_lag_days:
        issues.append("stale")

    date_key = valid_dates.dt.floor("s") if _is_intraday_root(root) else valid_dates.dt.normalize()
    result.duplicate_dates = int(date_key.duplicated(keep=False).sum())
    if result.duplicate_dates:
        issues.append("duplicate_dates")

    max_gap, gap_count = _summarize_gaps(
        valid_dates,
        root=root,
        daily_gap_days=args.daily_gap_days,
        intraday_gap_multiple=args.intraday_gap_multiple,
    )
    result.max_gap_seconds = max_gap
    result.gap_count = gap_count
    if gap_count:
        issues.append("large_gaps")

    ohlc_columns = [column for column in ("open", "max", "min", "close") if column in frame.columns]
    if ohlc_columns:
        ohlc = frame[ohlc_columns].apply(pd.to_numeric, errors="coerce")
        result.nan_ohlc_rows = int(ohlc.isna().any(axis=1).sum())
        if result.nan_ohlc_rows:
            issues.append("nan_ohlc")

        if {"open", "max", "min", "close"}.issubset(ohlc.columns):
            bad_ohlc = (
                (ohlc["max"] < ohlc["min"])
                | (ohlc["max"] < ohlc["open"])
                | (ohlc["max"] < ohlc["close"])
                | (ohlc["min"] > ohlc["open"])
                | (ohlc["min"] > ohlc["close"])
            )
            result.bad_ohlc_rows = int(bad_ohlc.fillna(False).sum())
            if result.bad_ohlc_rows:
                issues.append("bad_ohlc")

        result.nonpositive_price_rows = int((ohlc <= 0).any(axis=1).sum())
        if result.nonpositive_price_rows:
            issues.append("nonpositive_price")

    if "Trading_Volume" in frame.columns:
        volume = pd.to_numeric(frame["Trading_Volume"], errors="coerce")
        result.negative_volume_rows = int((volume < 0).fillna(False).sum())
        if result.negative_volume_rows:
            issues.append("negative_volume")

    if result.invalid_dates:
        issues.append("invalid_dates")
    result.issues = "|".join(sorted(set(issues))) or None
    result.status = "warn" if result.issues else "ok"
    return result


def _collect_files(roots: list[str], limit: int | None) -> list[tuple[Path, Path]]:
    items: list[tuple[Path, Path]] = []
    for root_text in roots:
        root = Path(root_text)
        if not root.exists():
            continue
        for path in sorted(root.glob(f"*{FEATURE_SUFFIX}")):
            items.append((root, path))
            if limit is not None and len(items) >= limit:
                return items
    return items


def main() -> None:
    args = parse_args()
    run_id = args.run_id or _utc_run_id()
    output_dir = Path(args.output_dir) / run_id
    output_dir.mkdir(parents=True, exist_ok=True)

    files = _collect_files(args.roots, args.limit)
    payloads = [(root, path, args) for root, path in files]
    results = run_parallel_tasks(
        payloads,
        _audit_file,
        max_workers=args.workers,
        desc="audit:ohlcv",
        unit="file",
    )
    results.sort(key=lambda item: (item.status, item.root, item.code))

    report_path = output_dir / "data_quality_report.csv"
    summary_path = output_dir / "data_quality_summary.json"
    latest_summary_path = Path(args.output_dir) / "latest_summary.json"

    report_frame = pd.DataFrame([asdict(item) for item in results])
    report_frame.to_csv(report_path, index=False)

    status_counts = report_frame["status"].value_counts().to_dict() if not report_frame.empty else {}
    issue_counts: dict[str, int] = {}
    if not report_frame.empty and "issues" in report_frame.columns:
        for raw_issues in report_frame["issues"].dropna().astype(str):
            for issue in raw_issues.split("|"):
                if issue:
                    issue_counts[issue] = issue_counts.get(issue, 0) + 1

    summary = {
        "run_id": run_id,
        "roots": args.roots,
        "file_count": len(files),
        "status_counts": {str(k): int(v) for k, v in status_counts.items()},
        "issue_counts": issue_counts,
        "report_path": str(report_path),
        "summary_path": str(summary_path),
    }
    summary_text = json.dumps(summary, ensure_ascii=False, indent=2)
    summary_path.write_text(summary_text, encoding="utf-8")
    latest_summary_path.parent.mkdir(parents=True, exist_ok=True)
    latest_summary_path.write_text(summary_text, encoding="utf-8")

    print(f"[audit] report -> {report_path}")
    print(f"[audit] summary -> {summary_path}")
    print(f"[audit] done: {json.dumps(summary, ensure_ascii=False)}")


if __name__ == "__main__":
    main()

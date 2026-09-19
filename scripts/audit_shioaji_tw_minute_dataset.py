from __future__ import annotations

import argparse
from datetime import date
import hashlib
import json
import os
from pathlib import Path
import sys
from typing import Any

import numpy as np
import polars as pl

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.build_shioaji_tw_minute_dataset import (  # noqa: E402
    EXECUTOR_ONLY_COLUMNS,
    MODEL_FEATURE_COLUMNS,
    NS_PER_MINUTE,
    SCHEMA_VERSION,
    VOLUME_NOTIONAL_TOLERANCE,
)
from stockagent.data.tw_exchange_price_classification import (  # noqa: E402
    VERIFIED_EMERGING_TO_TPEX_LISTINGS,
    classify_tw_broker_security_on_date,
    classify_tw_exchange_security,
)
from stockagent.data.tw_price_rules import price_on_tick_grid_numpy  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Audit one Shioaji api.kbars minute-research partition."
    )
    parser.add_argument(
        "--dataset-root",
        type=Path,
        default=Path("data_tw_minute/research_dataset"),
    )
    parser.add_argument("--trade-date")
    parser.add_argument(
        "--all-partitions",
        action="store_true",
        help="Audit every partition and its manifest fingerprint.",
    )
    parser.add_argument(
        "--calendar-root",
        type=Path,
        help=(
            "Compare date partitions to the receipt-backed official TAIEX calendar. "
            "Required for a full research_ready audit."
        ),
    )
    parser.add_argument('--allow-research-subset', action='store_true',
                        help='Audit an explicitly selected repair subset without marking it research_ready.')
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data_tw_minute/audits/latest.json"),
    )
    return parser.parse_args()


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _partition_path(dataset_root: Path, date_text: str) -> Path:
    """Audit the selected release, never a manifest's producer workspace.

    ``output`` is provenance from the build host. After materialization it can
    still name an existing, mutable or stale file on either host. The explicit
    dataset root owns every partition being certified by this audit.
    """
    trade_date = date.fromisoformat(date_text)
    if trade_date.isoformat() != date_text:
        raise RuntimeError(f"noncanonical minute partition date: {date_text}")
    root = dataset_root.resolve(strict=True)
    path = root / f"trade_date={date_text}" / "data.parquet"
    if not path.is_file():
        raise RuntimeError(f"minute partition is missing from selected release: {path}")
    resolved = path.resolve(strict=True)
    if not resolved.is_relative_to(root):
        raise RuntimeError(f"minute partition escapes selected release: {path}")
    return resolved


def audit_frame(frame: pl.DataFrame, *, trade_date: date) -> dict[str, Any]:
    required = {
        "date",
        "ts",
        "symbol",
        "feature_valid",
        "source_volume_multiplier",
        "source_volume_unit_valid",
        "volume_shares",
        "label_valid_1m",
        "execution_open_next_1m",
        "exit_close_next_1m",
        *MODEL_FEATURE_COLUMNS,
        *EXECUTOR_ONLY_COLUMNS,
    }
    missing = required - set(frame.columns)
    if missing:
        raise RuntimeError(f"minute dataset is missing columns: {sorted(missing)}")
    duplicates = frame.group_by("ts", "symbol").len().filter(pl.col("len") > 1).height
    minute_of_day = pl.col("ts").dt.hour().cast(pl.Int16) * 60 + pl.col(
        "ts"
    ).dt.minute().cast(pl.Int16)
    out_of_session = frame.filter(
        (minute_of_day < 9 * 60 + 1)
        | (minute_of_day > 13 * 60 + 30)
        | (pl.col("ts").dt.second() != 0)
    ).height
    wrong_date = frame.filter(pl.col("date") != pl.lit(trade_date)).height
    raw_invalid = frame.filter(
        ~pl.all_horizontal(
            *[
                pl.col(name).is_not_null() & pl.col(name).is_finite()
                for name in (
                    "Open",
                    "High",
                    "Low",
                    "Close",
                    "Volume",
                    "Amount",
                    "contract_unit",
                )
            ]
        )
        | pl.any_horizontal(
            *[(pl.col(name) <= 0.0) for name in ("Open", "High", "Low", "Close")]
        )
        | (pl.col("Volume") < 0.0)
        | (pl.col("Amount") < 0.0)
        | (pl.col("contract_unit") <= 0.0)
        | (pl.col("High") < pl.max_horizontal("Open", "Low", "Close"))
        | (pl.col("Low") > pl.min_horizontal("Open", "High", "Close"))
    ).height
    invalid_volume_unit = frame.filter(~pl.col("source_volume_unit_valid")).height
    tolerance = VOLUME_NOTIONAL_TOLERANCE
    invalid_volume_notional = frame.filter(
        (pl.col("Volume") > 0.0)
        & pl.col("source_volume_unit_valid")
        & (
            (
                pl.col("Amount")
                < pl.col("volume_shares") * pl.col("Low") * (1.0 - tolerance)
            )
            | (
                pl.col("Amount")
                > pl.col("volume_shares") * pl.col("High") * (1.0 + tolerance)
            )
        )
    ).height
    invalid_volume_shares = frame.filter(
        pl.col("source_volume_unit_valid")
        & (
            (
                pl.col("volume_shares")
                - pl.col("Volume") * pl.col("source_volume_multiplier")
            ).abs()
            > 1e-8
        )
    ).height
    symbol_values = frame["symbol"].cast(pl.String).to_numpy()
    market_values = frame["market"].cast(pl.String).to_numpy()
    kind_map = {
        key: classify_tw_exchange_security(key[0], key[1])
        for key in set(zip(market_values.tolist(), symbol_values.tolist()))
    }
    security_types = np.asarray(
        [
            kind_map[(market, symbol)] or "unknown"
            for market, symbol in zip(market_values, symbol_values)
        ],
        dtype="U16",
    )
    for code in VERIFIED_EMERGING_TO_TPEX_LISTINGS:
        candidates = np.flatnonzero((market_values == "tpex") & (symbol_values == code))
        for index in candidates:
            security_types[index] = classify_tw_broker_security_on_date(
                "tpex", code, trade_date
            ) or "unknown"
    supported_price_rows = security_types != "unknown"
    unknown_price_security_rows = int(np.count_nonzero(~supported_price_rows))
    date_values = np.full(frame.height, np.datetime64(trade_date.isoformat(), "D"))
    off_grid_price_values = 0
    for field in ("Open", "High", "Low", "Close"):
        values = frame[field].cast(pl.Float64).to_numpy()[supported_price_rows]
        good = price_on_tick_grid_numpy(
            values,
            date_values[supported_price_rows],
            security_types=security_types[supported_price_rows],
        )
        observed = np.isfinite(values) & (values > 0.0)
        off_grid_price_values += int(np.count_nonzero(observed & ~good))
    invalid_rows_with_labels = frame.filter(
        ~pl.col("label_valid_1m")
        & pl.any_horizontal(
            [
                pl.col(name).is_not_null()
                for name in (
                    "execution_open_next_1m",
                    "exit_close_next_1m",
                    "future_high_next_1m",
                    "future_low_next_1m",
                    "future_volume_shares_next_1m",
                    "long_gross_return_next_1m",
                    "short_gross_return_next_1m",
                )
            ]
        )
    ).height
    invalid_session_rows_with_labels = frame.filter(
        ~pl.col("session_exit_valid")
        & pl.any_horizontal(
            [
                pl.col("long_gross_return_to_session_close").is_not_null(),
                pl.col("short_gross_return_to_session_close").is_not_null(),
            ]
        )
    ).height
    ordered = frame.sort(["symbol", "date", "ts"])
    expected_next_ts = pl.col("ts").shift(-1).over(["symbol", "date"])
    bad_label_alignment = ordered.filter(
        pl.col("label_valid_1m")
        & (
            expected_next_ts.cast(pl.Int64) - pl.col("ts").cast(pl.Int64)
            != NS_PER_MINUTE
        )
    ).height
    bad_label_values = ordered.filter(
        pl.col("label_valid_1m")
        & (
            (
                pl.col("execution_open_next_1m")
                != pl.col("Open").shift(-1).over(["symbol", "date"])
            )
            | (
                pl.col("exit_close_next_1m")
                != pl.col("Close").shift(-1).over(["symbol", "date"])
            )
        )
    ).height
    failures = {
        "duplicate_keys": int(duplicates),
        "out_of_session_rows": int(out_of_session),
        "wrong_date_rows": int(wrong_date),
        "raw_invalid_rows": int(raw_invalid),
        "invalid_volume_unit_rows": int(invalid_volume_unit),
        "invalid_volume_notional_rows": int(invalid_volume_notional),
        "invalid_volume_shares_rows": int(invalid_volume_shares),
        "unknown_price_security_rows": unknown_price_security_rows,
        "off_grid_price_values": off_grid_price_values,
        "invalid_rows_with_labels": int(invalid_rows_with_labels),
        "invalid_session_rows_with_labels": int(invalid_session_rows_with_labels),
        "bad_label_alignment_rows": int(bad_label_alignment),
        "bad_label_value_rows": int(bad_label_values),
    }
    if any(failures.values()):
        raise RuntimeError(f"minute dataset audit failed: {failures}")
    positive_volume = pl.col("Volume") > 0.0
    volume_stats = frame.select(
        positive_volume.sum().alias("positive_volume_rows"),
        (positive_volume & (pl.col("source_volume_multiplier") == 1.0))
        .sum()
        .alias("volume_multiplier_1_rows"),
        (positive_volume & (pl.col("source_volume_multiplier") == 10.0))
        .sum()
        .alias("volume_multiplier_10_rows"),
        (positive_volume & (pl.col("source_volume_multiplier") == 100.0))
        .sum()
        .alias("volume_multiplier_100_rows"),
        (positive_volume & (pl.col("source_volume_multiplier") == 1_000.0))
        .sum()
        .alias("volume_multiplier_1000_rows"),
        (
            positive_volume
            & (pl.col("source_volume_multiplier") == pl.col("contract_unit"))
        )
        .sum()
        .alias("volume_rows_using_contract_unit"),
        (
            positive_volume
            & (pl.col("source_volume_multiplier") != pl.col("contract_unit"))
        )
        .sum()
        .alias("volume_rows_using_non_contract_unit"),
    ).row(0, named=True)
    return {
        "schema_version": SCHEMA_VERSION,
        "status": "ok",
        "trade_date": trade_date.isoformat(),
        "rows": frame.height,
        "symbols": frame["symbol"].n_unique(),
        "bars": frame["ts"].n_unique(),
        "feature_valid_rows": int(frame["feature_valid"].sum()),
        "label_valid_rows": int(frame["label_valid_1m"].sum()),
        **{name: int(value or 0) for name, value in volume_stats.items()},
        "model_feature_columns": list(MODEL_FEATURE_COLUMNS),
        "executor_only_columns": list(EXECUTOR_ONLY_COLUMNS),
        "failures": failures,
    }


def _audit_manifest_status(manifest: dict, *, allow_subset: bool) -> str:
    if manifest.get('schema_version') == SCHEMA_VERSION:
        if manifest.get('research_ready') is True and manifest.get('status') == 'research_ready':
            return 'research_ready'
        if (allow_subset and manifest.get('research_ready') is False
                and manifest.get('status') == 'research_subset'
                and isinstance(manifest.get('symbols'), list) and manifest['symbols']
                and len(set(manifest['symbols'])) == len(manifest['symbols'])):
            return 'research_subset'
    raise RuntimeError(f'minute full audit requires a schema-{SCHEMA_VERSION} research_ready manifest'
                       ' or an explicitly permitted research_subset')


def main() -> None:
    args = parse_args()
    if bool(args.all_partitions):
        manifest_path = args.dataset_root / "manifest.json"
        if not manifest_path.is_file():
            raise RuntimeError(f"minute dataset manifest is missing: {manifest_path}")
        manifest_bytes = manifest_path.read_bytes()
        manifest_sha256 = hashlib.sha256(manifest_bytes).hexdigest()
        manifest = json.loads(manifest_bytes)
        audit_status = _audit_manifest_status(manifest,
            allow_subset=bool(getattr(args, 'allow_research_subset', False)))
        if audit_status == "research_ready" and args.calendar_root is None:
            raise RuntimeError(
                "minute research_ready audit requires --calendar-root; "
                "partition integrity alone cannot prove trading-session identity"
            )
        partitions = manifest.get("partitions", [])
        if not partitions or len(partitions) != len(manifest.get("dates", [])):
            raise RuntimeError("minute manifest partition accounting is incomplete")
        declared_dates = {str(value) for value in manifest.get("dates", [])}
        stored_dates = {
            path.name.removeprefix("trade_date=")
            for path in args.dataset_root.glob("trade_date=*")
        }
        if stored_dates != declared_dates:
            raise RuntimeError(
                "stored minute partitions differ from manifest: "
                f"undeclared={sorted(stored_dates - declared_dates)} "
                f"missing={sorted(declared_dates - stored_dates)}"
            )
        calendar = {"checked": False}
        if args.calendar_root is not None:
            from downloader.download_tw_public_data import (
                _validated_taiex_session_dates,
            )

            sessions, calendar_sha256 = _validated_taiex_session_dates(
                args.calendar_root,
                date.fromisoformat(min(declared_dates)),
                date.fromisoformat(max(declared_dates)),
            )
            expected_dates = {day.isoformat() for day in sessions}
            missing = sorted(expected_dates - declared_dates)
            if missing:
                raise RuntimeError(
                    f"official sessions lack minute partitions: {missing}"
                )
            extra = sorted(declared_dates - expected_dates)
            if extra:
                raise RuntimeError(
                    f"minute dataset contains non-session partitions: {extra}"
                )
            calendar = {
                "checked": True,
                "root": str(args.calendar_root.resolve()),
                "sha256": calendar_sha256,
                "official_session_count": len(sessions),
                "missing_session_partitions": missing,
                "extra_non_session_partitions": extra,
                "per_symbol_session_completeness_checked": False,
            }
        totals = {
            "rows": 0,
            "feature_valid_rows": 0,
            "label_valid_rows": 0,
            "positive_volume_rows": 0,
            "volume_multiplier_1_rows": 0,
            "volume_multiplier_10_rows": 0,
            "volume_multiplier_100_rows": 0,
            "volume_multiplier_1000_rows": 0,
            "volume_rows_using_contract_unit": 0,
            "volume_rows_using_non_contract_unit": 0,
        }
        seen_dates: set[str] = set()
        maximum_symbols = 0
        for position, summary in enumerate(partitions, start=1):
            date_text = str(summary["trade_date"])
            if date_text in seen_dates:
                raise RuntimeError(f"duplicate minute partition date: {date_text}")
            seen_dates.add(date_text)
            path = _partition_path(args.dataset_root, date_text)
            actual_sha256 = _sha256(path)
            if actual_sha256 != str(summary.get("output_sha256", "")):
                raise RuntimeError(f"minute partition fingerprint mismatch: {path}")
            result = audit_frame(
                pl.read_parquet(path), trade_date=date.fromisoformat(date_text)
            )
            if int(result["rows"]) != int(summary["rows"]):
                raise RuntimeError(f"minute partition row count changed: {path}")
            for name in totals:
                totals[name] += int(result[name])
            maximum_symbols = max(maximum_symbols, int(result["symbols"]))
            if position == 1 or position % 100 == 0 or position == len(partitions):
                print(
                    f"[tw-minute-audit] partitions={position}/{len(partitions)} "
                    f"date={date_text} rows={totals['rows']}",
                    flush=True,
                )
        if seen_dates != set(manifest.get("dates", [])):
            raise RuntimeError("minute manifest dates differ from audited partitions")
        if _sha256(manifest_path) != manifest_sha256:
            raise RuntimeError("minute manifest changed during audit")
        result = {
            "schema_version": SCHEMA_VERSION,
            "status": audit_status,
            "audited_symbols": manifest.get('symbols', []),
            "audit_scope": "partition_integrity_and_row_semantics_not_full_training_readiness",
            "dataset_root": str(args.dataset_root.resolve()),
            "manifest_sha256": manifest_sha256,
            "calendar": calendar,
            "corporate_action_completeness_checked": False,
            "source": "shioaji_kbars_1m",
            "partitions": len(partitions),
            "first_date": min(seen_dates),
            "last_date": max(seen_dates),
            "available_source_symbols": int(
                manifest.get("available_source_symbols", 0)
            ),
            "source_gap_symbols": int(manifest.get("source_gap_symbols", 0)),
            "contract_unavailable_symbols": int(
                manifest.get("contract_unavailable_symbols", 0)
            ),
            "maximum_symbols_per_day": maximum_symbols,
            **totals,
            "failures": {},
        }
        output = args.output
        if output == Path("data_tw_minute/audits/latest.json"):
            output = Path("data_tw_minute/audits/full_latest.json")
        output.parent.mkdir(parents=True, exist_ok=True)
        temporary = output.with_suffix(output.suffix + ".tmp")
        temporary.write_text(
            json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        os.replace(temporary, output)
        print(
            f"[tw-minute-audit] status={audit_status} "
            f"partitions={len(partitions)} rows={totals['rows']} output={output}",
            flush=True,
        )
        return
    if not args.trade_date:
        raise ValueError("--trade-date is required unless --all-partitions is used")
    trade_date = date.fromisoformat(args.trade_date)
    path = args.dataset_root / f"trade_date={trade_date}" / "data.parquet"
    if not path.is_file():
        raise RuntimeError(f"minute research partition does not exist: {path}")
    result = audit_frame(pl.read_parquet(path), trade_date=trade_date)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    temporary = args.output.with_suffix(args.output.suffix + ".tmp")
    temporary.write_text(
        json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, args.output)
    print(
        f"[tw-minute-audit] status=ok date={trade_date} "
        f"rows={result['rows']} symbols={result['symbols']} output={args.output}",
        flush=True,
    )


if __name__ == "__main__":
    main()

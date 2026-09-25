"""Local-only FinLab feature overlay for the separate TW research ABI.

The provider's current revision is not a historical vintage. Calendar dates
without a verified publication clock enter on the *next* exchange session;
quarterly EPS uses a documented filing-deadline proxy. Never use this output
for strict/live training or copy it to the fleet cold store.
"""

from __future__ import annotations

from bisect import bisect_left, bisect_right
from datetime import date, datetime, time
import gc
import hashlib
import json
import os
from pathlib import Path

import polars as pl

from stockagent.data.tw_mops_publication_timing import _deadline_proxy
from stockagent.data.tw_public_research_taifex import MARKET_SYMBOL


STOCK_WIDE = {
    "monthly_revenue:當月營收": "twfl_monthly_revenue_raw",
    "financial_statement:每股盈餘": "twfl_eps_raw",
    "security_lending:借券餘額": "twfl_security_lending_balance_raw",
    "foreign_investors_shareholding:全體外資及陸資持股比率": "twfl_foreign_shareholding_ratio_raw",
    "etl:inventory:大於四百張佔比": "twfl_tdcc_over_400_ratio_raw",
    "block_trade:成交金額": "twfl_block_trade_amount_raw",
    "tw_etf_nav_daily:折溢價(%)": "twfl_etf_nav_premium_discount_pct_raw",
}
MARKET_WIDE = {
    "tw_business_indicators:景氣對策信號(分)": "twfl_business_indicator_score_raw",
    "tw_total_pmi:製造業PMI": "twfl_manufacturing_pmi_raw",
    "taiex_total_index:收盤指數": "twfl_taiex_close_raw",
}
EVENTS = {
    "trading_attention": "twfl_attention_event_count",
    "disposal_information": "twfl_disposal_event_count",
    "important_info_announcement": "twfl_material_announcement_count",
    "investors_conference": "twfl_investors_conference_event_count",
}
FUTURES_KEY = "futures_institutional_investors_trading_summary:多空未平倉口數淨額"
ETF_AGGREGATE_KEY = "tw_etf_beneficiary_stats"
CONTRACT_VERSION = 1


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _verified_source(root: Path, key: str) -> tuple[Path, dict] | None:
    receipts = root / "receipts"
    for receipt_path in receipts.glob("*.json"):
        receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
        if receipt.get("dataset") != key:
            continue
        relative = Path(str(receipt.get("parquet_path") or ""))
        if (relative.is_absolute() or ".." in relative.parts
                or relative.parts[:1] != ("datasets",)):
            raise ValueError(f"unsafe FinLab receipt path for {key}")
        path = root / relative
        if not path.is_file() or _sha256(path) != receipt.get("sha256"):
            raise ValueError(f"FinLab source missing or hash mismatch: {key}")
        if receipt.get("publication_time_status") != "not_verified":
            raise ValueError(f"unexpected FinLab publication policy: {key}")
        return path, receipt
    return None


def _calendar(base_path: Path) -> list[date]:
    values = (
        pl.scan_parquet(base_path).select("date").unique().sort("date")
        .collect(engine="streaming").get_column("date").to_list()
    )
    if not values or len(values) != len(set(values)):
        raise ValueError("research base has no unique exchange calendar")
    return values


def _available_on(value: object, calendar: list[date], *, event_clock: bool = False) -> date | None:
    if value is None:
        return None
    text = str(value)
    if len(text) == 7 and text[4] == "-" and text[5] == "Q":
        deadline, _ = _deadline_proxy(text.replace("-", ""), "ifrs")
        day = deadline
        index = bisect_right(calendar, day)
    else:
        try:
            observed = datetime.fromisoformat(text)
        except ValueError:
            return None
        day = observed.date()
        if event_clock and observed.time() < time(9, 0):
            index = bisect_left(calendar, day)
        else:
            index = bisect_right(calendar, day)
    return calendar[index] if index < len(calendar) else None


def _date_lookup(indexes: list[str], calendar: list[date]) -> pl.DataFrame:
    return pl.DataFrame({
        "source_index": indexes,
        "date": [_available_on(value, calendar) for value in indexes],
    }).filter(pl.col("date").is_not_null())


def _wide_stock(path: Path, feature: str, calendar: list[date]) -> pl.DataFrame:
    frame = pl.read_parquet(path)
    fields = [name for name in frame.columns if name != "source_index"]
    if not fields:
        raise ValueError(f"no stock fields for {feature}")
    lookup = _date_lookup(frame.get_column("source_index").to_list(), calendar)
    long = (
        frame.join(lookup, on="source_index", how="inner")
        .unpivot(on=fields, index=["date", "source_index"], variable_name="symbol", value_name=feature)
        .with_columns(pl.col(feature).cast(pl.Float64, strict=False))
        .filter(pl.col(feature).is_finite())
    )
    # Weekend/holiday observations can all become usable on the same next
    # session. The latest provider observation then supersedes earlier ones.
    return long.sort("date", "symbol", "source_index").group_by(
        "date", "symbol", maintain_order=True,
    ).agg(pl.col(feature).last())


def _wide_market(path: Path, feature: str, calendar: list[date]) -> pl.DataFrame:
    frame = pl.read_parquet(path)
    values = [name for name in frame.columns if name != "source_index" and name.strip()]
    if len(values) != 1:
        raise ValueError(f"expected one market value for {feature}")
    lookup = _date_lookup(frame.get_column("source_index").to_list(), calendar)
    return (
        frame.join(lookup, on="source_index", how="inner")
        .sort("source_index")
        .select("date", pl.lit(MARKET_SYMBOL).alias("symbol"),
                pl.col(values[0]).cast(pl.Float64, strict=False).alias(feature))
        .filter(pl.col(feature).is_finite())
        .group_by("date", "symbol", maintain_order=True).agg(pl.col(feature).last())
    )


def _futures_market(path: Path, calendar: list[date]) -> tuple[pl.DataFrame, dict[str, str]]:
    frame = pl.read_parquet(path)
    fields = sorted(name for name in frame.columns if name != "source_index" and name.strip())
    lookup = _date_lookup(frame.get_column("source_index").to_list(), calendar)
    mapping = {f"twfl_futures_net_oi_{n:03d}_raw": field for n, field in enumerate(fields, 1)}
    result = frame.join(lookup, on="source_index", how="inner").sort("source_index").select(
        "date", pl.lit(MARKET_SYMBOL).alias("symbol"),
        *[pl.col(field).cast(pl.Float64, strict=False).alias(name)
          for name, field in mapping.items()],
    ).group_by("date", "symbol", maintain_order=True).agg(
        *[pl.col(name).last() for name in mapping]
    )
    return result, mapping


def _etf_aggregate_market(path: Path, calendar: list[date]) -> tuple[pl.DataFrame, dict[str, str]]:
    frame = pl.read_parquet(path)
    fields = sorted(name for name in frame.columns if name not in {
        "source_index", "symbol", "date", "key_date", "stock_id",
    } and name.strip())
    mapping = {f"twfl_etf_aggregate_{n:02d}_raw": field for n, field in enumerate(fields, 1)}
    frame = frame.with_columns(
        pl.col("date").map_elements(
            lambda value: _available_on(value, calendar), return_dtype=pl.Date,
        ).alias("available_on")
    )
    result = frame.select(
        pl.col("available_on").alias("date"),
        pl.lit(MARKET_SYMBOL).alias("symbol"),
        *[pl.col(field).cast(pl.Float64, strict=False).alias(name)
          for name, field in mapping.items()],
    ).filter(pl.col("date").is_not_null())
    if result.get_column("date").n_unique() != result.height:
        raise ValueError("ETF aggregate is not one row per available session")
    return result, mapping


def _event_counts(path: Path, feature: str, calendar: list[date], *, precise: bool) -> pl.DataFrame:
    frame = pl.read_parquet(path, columns=["symbol", "date"])
    lookup = {value: _available_on(value, calendar, event_clock=precise)
              for value in frame.get_column("date").unique().to_list()}
    result = frame.with_columns(
        pl.col("date").replace_strict(lookup, default=None).alias("available_on")
    ).select(
        pl.col("available_on").alias("date"),
        pl.col("symbol").cast(pl.String),
    ).filter(pl.col("date").is_not_null() & pl.col("symbol").is_not_null())
    return result.group_by("date", "symbol").len(name=feature).with_columns(
        pl.col(feature).cast(pl.Float64)
    )


def build_finlab_research_overlay(*, finlab_root: Path, base_path: Path, output_path: Path) -> dict:
    finlab_root, base_path, output_path = map(Path, (finlab_root, base_path, output_path))
    if output_path.resolve() == base_path.resolve():
        raise ValueError("FinLab research output must not overwrite the base table")
    calendar = _calendar(base_path)
    base_sha = _sha256(base_path)
    eligible_keys = [*STOCK_WIDE, *MARKET_WIDE, *EVENTS, FUTURES_KEY, ETF_AGGREGATE_KEY]
    verified_inputs = {key: item for key in eligible_keys
                       if (item := _verified_source(finlab_root, key)) is not None}
    receipt_path = output_path.with_suffix(".finlab_research.json")
    if output_path.is_file() and receipt_path.is_file():
        try:
            prior = json.loads(receipt_path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            prior = {}
        previous_sources = prior.get("inputs", {}).get("sources", {})
        if (
            prior.get("inputs", {}).get("contract_version") == CONTRACT_VERSION
            and prior.get("inputs", {}).get("base_sha256") == base_sha
            and set(previous_sources) == set(verified_inputs)
            and all(previous_sources[key].get("sha256") == receipt["sha256"]
                    for key, (_, receipt) in verified_inputs.items())
            and prior.get("output_sha256") == _sha256(output_path)
        ):
            return {**prior, "reused": True}
    sources: dict[str, dict] = {}
    overlays: list[tuple[str, Path, list[str]]] = []
    future_field_map: dict[str, str] = {}
    etf_field_map: dict[str, str] = {}
    calendar_hash = hashlib.sha256(
        ",".join(day.isoformat() for day in calendar).encode()
    ).hexdigest()[:16]

    def stage(key: str, receipt: dict, table: pl.DataFrame) -> None:
        # Materialize one source at a time. Keeping all 10M-row unpivots in
        # memory made a full-table join approach exceed a 16 GiB process cap.
        names = [name for name in table.columns if name not in {"date", "symbol"}]
        token = hashlib.sha256(
            f"{CONTRACT_VERSION}|{calendar_hash}|{key}|{receipt['sha256']}".encode()
        ).hexdigest()[:24]
        path = finlab_root / "normalized" / f"{token}.parquet"
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_suffix(".parquet.tmp")
        try:
            table.write_parquet(temporary, compression="zstd", statistics=True)
            os.replace(temporary, path)
        finally:
            temporary.unlink(missing_ok=True)
        overlays.append((key, path, names))
        sources[key] = {
            "sha256": receipt["sha256"], "rows": table.height,
            "feature_columns": names,
            "source_index_semantics": receipt.get("index_semantics"),
        }
    for key, feature in {**STOCK_WIDE, **MARKET_WIDE, **EVENTS}.items():
        verified = verified_inputs.get(key)
        if verified is None:
            continue
        path, receipt = verified
        if key in STOCK_WIDE:
            table = _wide_stock(path, feature, calendar)
        elif key in MARKET_WIDE:
            table = _wide_market(path, feature, calendar)
        else:
            table = _event_counts(path, feature, calendar,
                                  precise=key == "important_info_announcement")
        stage(key, receipt, table)
        del table
        gc.collect()
    verified = verified_inputs.get(FUTURES_KEY)
    if verified is not None:
        path, receipt = verified
        table, future_field_map = _futures_market(path, calendar)
        stage(FUTURES_KEY, receipt, table)
        del table
        gc.collect()
    verified = verified_inputs.get(ETF_AGGREGATE_KEY)
    if verified is not None:
        path, receipt = verified
        table, etf_field_map = _etf_aggregate_market(path, calendar)
        stage(ETF_AGGREGATE_KEY, receipt, table)
        del table
        gc.collect()
    if not overlays:
        raise ValueError("no verified local FinLab numeric or event research inputs")
    fingerprint = {"contract_version": CONTRACT_VERSION, "base_sha256": base_sha,
                   "sources": sources}
    base = pl.scan_parquet(base_path)
    base_schema = set(base.collect_schema().names())
    columns = [name for _, _, names in overlays for name in names]
    if len(columns) != len(set(columns)) or set(columns) & base_schema:
        raise ValueError("FinLab feature names duplicate base or one another")
    build_id = hashlib.sha256(json.dumps(
        fingerprint, sort_keys=True, ensure_ascii=False,
    ).encode()).hexdigest()[:24]
    parts_root = finlab_root / "normalized" / "builds" / build_id
    parts_root.mkdir(parents=True, exist_ok=True)
    part_paths: list[Path] = []
    for year in sorted({day.year for day in calendar}):
        year_base = base.filter(pl.col("date").dt.year() == year)
        joined = year_base
        for _, path, _ in overlays:
            same_year = pl.scan_parquet(path).filter(pl.col("date").dt.year() == year)
            joined = joined.join(same_year, on=["date", "symbol"], how="left", validate="m:1")
        part = parts_root / f"part-{year}.parquet"
        part_temp = part.with_suffix(".parquet.tmp")
        try:
            joined.sink_parquet(part_temp, compression="zstd", statistics=True)
            os.replace(part_temp, part)
        finally:
            part_temp.unlink(missing_ok=True)
        part_paths.append(part)
        print(f"[finlab-research] built {year} partition", flush=True)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = output_path.with_suffix(output_path.suffix + ".tmp")
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq

        schema = pq.ParquetFile(part_paths[0]).schema_arrow
        with pq.ParquetWriter(temporary, schema=schema, compression="zstd") as writer:
            for part in part_paths:
                source = pq.ParquetFile(part)
                if not source.schema_arrow.equals(schema):
                    raise ValueError(f"FinLab research part schema drift: {part.name}")
                for batch in source.iter_batches(batch_size=16_384):
                    writer.write_table(pa.Table.from_batches([batch]))
        if _sha256(base_path) != base_sha:
            raise RuntimeError("research base changed during FinLab join")
        for key, source in sources.items():
            current = _verified_source(finlab_root, key)
            if current is None or current[1].get("sha256") != source["sha256"]:
                raise RuntimeError(f"FinLab source changed during research join: {key}")
        rows = pl.scan_parquet(temporary).select(pl.len()).collect().item()
        expected = base.select(pl.len()).collect().item()
        if rows != expected:
            raise ValueError(f"FinLab research join changed row count: {expected} -> {rows}")
        os.replace(temporary, output_path)
    finally:
        temporary.unlink(missing_ok=True)
    summary = {
        "inputs": fingerprint, "output_path": str(output_path.resolve()),
        "output_sha256": _sha256(output_path), "rows": rows,
        "feature_columns": columns, "futures_field_mapping": future_field_map,
        "etf_aggregate_field_mapping": etf_field_map,
        "research_only": True, "historical_point_in_time": False,
        "strict_training_eligible": False, "cold_publishable": False,
        "availability_policy": "date-only rows next exchange session; exact material-event times before 09:00 same session, otherwise next; EPS quarterly deadline proxy; no imputation or event forward-fill",
        "source_revision_policy": "current provider revision only; not reconstructed historical vintage",
        "reused": False,
    }
    temp_receipt = receipt_path.with_suffix(receipt_path.suffix + ".tmp")
    temp_receipt.write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    os.replace(temp_receipt, receipt_path)
    return summary

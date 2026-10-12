"""Receipt-pinned historical venue evidence for documented FinMind minute units.

Reuse the official downloader's journal selection and date/schema parser. A
filename, current StockInfo or a price ratio alone never proves a dated venue.
"""
from __future__ import annotations

from datetime import date
import hashlib
from pathlib import Path

import polars as pl

from downloader.download_tw_public_data import (
    HISTORICAL_DAILY_DATASETS, _iter_historical_journal_records,
    _historical_request_info, _historical_urls_match_receipt,
    _parse_historical_response_content, _historical_parser_contract_version,
    _twse_official_host_fallback_url, HistoricalResponseError,
)
from downloader.parquet_integrity import signature


CONTRACT = "tw_minute_dated_official_market_evidence_v1"
SCHEMA = {"symbol": pl.String, "date": pl.String, "venue": pl.String, "source_ref": pl.String}
SPECS = {spec.name: spec for spec in HISTORICAL_DAILY_DATASETS
         if spec.name in {"twse_daily_ohlcv", "tpex_daily_ohlcv"}}


def pin_official_market_inputs(root: Path, days: set[str]) -> list[dict]:
    """Pin latest journal outcome per date, preserving exact raw evidence bytes."""
    root = root.resolve()
    records = []
    for name, spec in sorted(SPECS.items()):
        journal = root / "state/journals" / f"{name}.jsonl"
        latest = {}
        for day, value in _iter_historical_journal_records(journal, spec):
            if day.isoformat() in days:
                latest[day.isoformat()] = value
        for day, receipt in sorted(latest.items()):
            if receipt.get("status") != "data":
                continue
            if not receipt.get("raw_path") or not receipt.get("raw_sha256") or not receipt.get("raw_size"):
                # A parsed-only outcome is not an independently pinned raw
                # receipt. Leave this source/day without venue evidence.
                continue
            relative = Path(receipt.get("raw_path") or "")
            raw = root / relative
            if (relative.is_absolute() or ".." in relative.parts or not relative.name
                    or not raw.resolve().is_relative_to(root)):
                raise ValueError("unsafe official dated unit evidence path")
            expected_url, kind = _historical_request_info(spec, date.fromisoformat(day))
            official_urls = [expected_url, _twse_official_host_fallback_url(expected_url)]
            if not any(url and _historical_urls_match_receipt(str(receipt.get("url", "")), url)
                       for url in official_urls):
                raise ValueError("official dated unit evidence URL mismatch")
            records.append({"contract": CONTRACT, "dataset": name, "day": day,
                "venue": "twse" if name.startswith("twse") else "tpex",
                "path": str(raw.resolve()), "sha256": receipt["raw_sha256"],
                "size": receipt["raw_size"], "journal": str(journal),
                "journal_record": receipt, "url": receipt["url"], "response_kind": kind,
                "parser_contract_version": _historical_parser_contract_version(spec)})
    return records


def read_official_market_evidence(record: dict, *, symbols: list[str]) -> pl.DataFrame:
    """Only symbols observed in the official report on this exact date qualify."""
    if record.get("contract") != CONTRACT or record["dataset"] not in SPECS:
        raise ValueError("unsupported official dated unit evidence contract")
    spec = SPECS[record["dataset"]]
    if record["parser_contract_version"] != _historical_parser_contract_version(spec):
        raise ValueError("dated unit evidence parser changed; repin inventory")
    path = Path(record["path"])
    before = signature(path)
    raw = path.read_bytes()
    if signature(path) != before or len(raw) != record["size"] or hashlib.sha256(raw).hexdigest() != record["sha256"]:
        raise ValueError("dated unit evidence raw fingerprint mismatch")
    try:
        frame, _ = _parse_historical_response_content(spec, date.fromisoformat(record["day"]),
                                                     raw, record["response_kind"])
    except HistoricalResponseError as error:
        raise ValueError(f"invalid dated official market response: {error}") from error
    identity = "證券代號" if record["venue"] == "twse" else "代號"
    frame = frame.select(pl.col(identity).alias("symbol")).unique()
    if symbols:
        frame = frame.filter(pl.col("symbol").is_in(symbols))
    return frame.with_columns(pl.lit(record["day"]).alias("date"),
        pl.lit(record["venue"]).alias("venue"), pl.lit(record["sha256"]).alias("source_ref"))

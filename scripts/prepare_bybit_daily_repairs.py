"""Build a small, receipt-backed local Bybit research view, never a snapshot.

Unchanged daily files are symlinks to one resolved immutable release. Only the
three affected daily tables are derived here. This is not a producer workspace
or a publication target; no source, READY proof, or prior training is modified.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sys
from urllib.parse import parse_qs, urlparse

import numpy as np
import polars as pl

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "downloader"))
from artifact_io import atomic_write_parquet, sha256_file
from common import atomic_write_text
from materialize_bybit_perpetual_daily import _daily_bars, _attach_funding_total_return
from stockagent.data.crypto_lifecycle import announced_exit_mask

DEFAULT_PROBE = "artifacts/markets/bybit_perpetual_daily_0005_training_audit/source_probe.json"
DEFAULT_OUTPUT = "artifacts/cache/bybit_perpetual_daily_repaired"


def canonical_hash(value: object) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()).hexdigest()


def checked_probe(document: dict, endpoint: str, symbol: str) -> tuple[dict, dict]:
    matches = []
    for probe in document["api_probes"]:
        url = urlparse(probe["url"])
        query = parse_qs(url.query)
        if url.path == endpoint and query.get("symbol") == [symbol]:
            if url.scheme != "https" or url.hostname != "api.bybit.com" or query.get("category") != ["linear"]:
                raise ValueError("repair requires official Bybit linear API")
            payload = probe["response_json"]
            if probe["http_status"] != 200 or payload.get("retCode") != 0:
                raise ValueError("unsuccessful source probe")
            result = payload.get("result", {})
            if result.get("category", "linear") != "linear" or result.get("symbol", symbol) != symbol:
                raise ValueError("source response product identity mismatch")
            if canonical_hash(payload) != probe["response_json_canonical_sha256"]:
                raise ValueError("source probe JSON hash mismatch")
            matches.append((payload, query))
    if len(matches) != 1:
        raise ValueError(f"expected exactly one official probe: {symbol} {endpoint}")
    return matches[0]


def merge_icx_funding(old: pl.DataFrame, document: dict) -> tuple[pl.DataFrame, dict]:
    rates, query = checked_probe(document, "/v5/market/funding/history", "ICXUSDT")
    marks, mark_query = checked_probe(document, "/v5/market/mark-price-kline", "ICXUSDT")
    if mark_query.get("interval") != ["60"]:
        raise ValueError("funding valuation requires hourly mark opens")
    events = rates["result"]["list"]
    if not events or len(events) >= int(query["limit"][0]):
        raise ValueError("funding probe may be truncated; paginate before repair")
    start, end = int(query["startTime"][0]), int(query["endTime"][0])
    mark_by_time = {int(row[0]): float(row[1]) for row in marks["result"]["list"]}
    if len(mark_by_time) != len(marks["result"]["list"]):
        raise ValueError("duplicate mark timestamp")
    prior = {int(row["funding_timestamp_ms"]): row for row in old.to_dicts()}
    if len(prior) != old.height or old.is_empty():
        raise ValueError("old funding must be nonempty and unique")
    if set(old["symbol"].to_list()) != {"ICXUSDT"} or set(old["category"].to_list()) != {"linear"}:
        raise ValueError("wrong old funding identity")
    if set(old["bybit_funding_contract_version"].to_list()) != {3}:
        raise ValueError("incompatible old funding contract")
    template = dict(prior[max(prior)])
    old_covered_to = datetime.fromisoformat(str(template["download_snapshot_utc"]).replace("Z", "+00:00"))
    if old_covered_to.tzinfo is None:
        old_covered_to = old_covered_to.replace(tzinfo=timezone.utc)
    if start > old_covered_to.timestamp() * 1000:
        raise ValueError("funding tail does not overlap existing coverage")
    seen = set()
    overlap = 0
    new_rows = []
    for event in events:
        stamp = int(event["fundingRateTimestamp"])
        rate = float(event["fundingRate"])
        mark = mark_by_time.get(stamp, float("nan"))
        if event.get("symbol") != "ICXUSDT" or stamp in seen or not start <= stamp <= end:
            raise ValueError("invalid funding identity/range/duplicate")
        if not np.isfinite(rate) or not np.isfinite(mark) or mark <= 0:
            raise ValueError("funding event lacks a finite positive official mark")
        seen.add(stamp)
        if stamp in prior:
            row = prior[stamp]
            if not np.isclose(rate, row["funding_rate"], rtol=1e-10, atol=1e-12) or not np.isclose(mark, row["funding_mark_price"], rtol=1e-10, atol=1e-12):
                raise ValueError("official funding overlap conflicts with retained source")
            overlap += 1
            continue
        if stamp <= max(prior):
            raise ValueError("repair must not silently rewrite an old internal funding gap")
        row = dict(template)
        row.update(funding_timestamp_ms=stamp,
                   funding_time_utc=datetime.fromtimestamp(stamp / 1000, timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                   funding_rate=rate, funding_mark_price=mark,
                   download_snapshot_utc=document["checked_at_utc"])
        new_rows.append(row)
    if not overlap or {t for t in prior if start <= t <= min(end, max(prior))} - seen:
        raise ValueError("funding overlap is incomplete")
    merged = pl.concat([old, pl.DataFrame(new_rows).cast(old.schema)], how="vertical").sort("funding_timestamp_ms") if new_rows else old
    coverage = {"head_complete": True,
                "coverage_start_utc": template["funding_coverage_start_utc"],
                "coverage_end_utc": datetime.fromtimestamp(end / 1000, timezone.utc).isoformat()}
    return merged, {"coverage": coverage, "added_events": len(new_rows), "verified_overlap_events": overlap}


def attach_announcement(frame: pl.DataFrame, event: dict) -> pl.DataFrame:
    metadata = event["metadata"]
    if canonical_hash(metadata) != event["metadata_canonical_sha256"]:
        raise ValueError("announcement metadata hash mismatch")
    frame = frame.with_columns(
        pl.lit(metadata["published_date"]).alias("crypto_delisting_announced_date"),
        pl.lit(metadata["exchange_delisting_time_utc"]).alias("crypto_delisting_time_utc"),
        pl.lit(metadata["url"]).alias("crypto_delisting_source_url"),
    )
    announced_exit_mask(np.asarray(frame["date"], dtype="datetime64[D]"), frame.to_dict(as_series=False))
    return frame


def prepare(base: Path, probe_path: Path, output: Path) -> dict:
    base = base.resolve(strict=True)
    output = output.absolute()
    cache_root = (ROOT / "artifacts/cache").resolve()
    # Only this explicit node-local derived layer may be written. A root user
    # being able to write a READY tree is not permission to mutate it.
    if not output.resolve().is_relative_to(cache_root) or output.resolve() == cache_root:
        raise ValueError("output must be a bounded child of artifacts/cache")
    if output.is_symlink() or output.resolve().is_relative_to(base) or base.is_relative_to(output.resolve()):
        raise ValueError("repair output must not overlap the immutable source")
    document = json.loads(probe_path.read_text())
    sources = sorted((base / "perpetual_daily").glob("*_features.parquet"))
    if not sources:
        raise ValueError("no base daily files")
    source_hashes = {str(path): sha256_file(path) for path in sources}
    funding_path = base / "funding/ICXUSDT_funding.parquet"
    minute_path = base / "1m/ICXUSDT_features.parquet"
    for path in (funding_path, minute_path, base / "1m/_hot_tail/ICXUSDT_features.parquet", probe_path.resolve()):
        if path.exists():
            source_hashes[str(path)] = sha256_file(path)
    receipt_path = output / "repair_manifest.json"
    if output.exists():
        if not receipt_path.is_file():
            raise ValueError("existing output has no completed repair receipt; will not overwrite")
        receipt = json.loads(receipt_path.read_text())
        if receipt["sources_sha256"] != source_hashes or receipt.get("schema_version") != 1:
            raise ValueError("repair sources changed; explicit rebuild review required")
        for name, digest in receipt["derived_sha256"].items():
            if sha256_file(output / name) != digest:
                raise ValueError(f"derived repair hash mismatch: {name}")
        for source in sources:
            target = output / "perpetual_daily" / source.name
            if source.stem.removesuffix("_features") not in receipt["symbols"] and (not target.is_symlink() or target.resolve() != source):
                raise ValueError("base link changed")
        return receipt
    events = {row["metadata"]["symbol"]: row for row in document["official_announcements"]}
    if set(events) != {"HFTUSDT", "VINEUSDT", "ICXUSDT"}:
        raise ValueError("unexpected repair event scope")
    old_funding = pl.read_parquet(funding_path)
    funding, funding_audit = merge_icx_funding(old_funding, document)
    daily, _, _ = _daily_bars(minute_path, execution_minutes_utc=5)
    rebuilt, _, _ = _attach_funding_total_return(daily, funding, funding_audit["coverage"], execution_minutes_utc=5)
    frames = {}
    details = {}
    for symbol in sorted(events):
        original = pl.read_parquet(base / "perpetual_daily" / f"{symbol}_features.parquet")
        result = original
        if symbol == "ICXUSDT":
            boundary = original["date"].max()
            anchor = original.filter(pl.col("date") == boundary)["adjclose"].item()
            tail = rebuilt.filter(pl.col("date") >= boundary)
            rebuilt_anchor = tail.filter(pl.col("date") == boundary)["adjclose"].item()
            if not np.isfinite(anchor) or not np.isfinite(rebuilt_anchor) or min(anchor, rebuilt_anchor) <= 0:
                raise ValueError("invalid total-return splice anchor")
            tail = tail.with_columns(pl.col("adjclose") * (anchor / rebuilt_anchor))
            result = pl.concat([original.filter(pl.col("date") < boundary), tail], how="vertical_relaxed")
            # Previously valid history is byte-value preserved; only the stale
            # final forward label and subsequent actual sessions are rebuilt.
            if not result.filter(pl.col("date") < boundary).equals(original.filter(pl.col("date") < boundary)):
                raise ValueError("repair changed previously retained history")
        result = attach_announcement(result, events[symbol])
        frames[symbol] = result
        mask = announced_exit_mask(np.asarray(result["date"], dtype="datetime64[D]"), result.to_dict(as_series=False))
        details[symbol] = {"old_rows": original.height, "new_rows": result.height,
                           "last_date": result["date"].max(),
                           "first_exit_request_date": result.filter(pl.Series(mask))["date"].min(),
                           "unvalued_forward_rows": int(result["return_quarantined"].sum())}
    # Build all data in memory and recheck sources before installing this small
    # view. A partial failed view is never accepted without its final receipt.
    if any(sha256_file(Path(name)) != digest for name, digest in source_hashes.items()):
        raise ValueError("source changed during repair")
    panel_dir = output / "perpetual_daily"
    panel_dir.mkdir(parents=True, exist_ok=False)
    derived = {}
    for source in sources:
        symbol = source.stem.removesuffix("_features")
        target = panel_dir / source.name
        if symbol in frames:
            atomic_write_parquet(target, frames[symbol], compression="snappy", write_statistics=True)
            derived[str(target.relative_to(output))] = sha256_file(target)
        else:
            target.symlink_to(source)
    receipt = {"schema_version": 1, "base_release": base.name, "base_root": str(base),
               "sources_sha256": source_hashes, "derived_sha256": derived,
               "symbols": details, "funding_repair": funding_audit,
               "storage_contract": "local derived view; unchanged files symlinked; no source writes or publication",
               "exit_contract": "date-only announcement known next UTC day; no new entries; capacity-limited exit requests, never synthetic settlement",
               "missing_held_valuation": "still raises; masks do not erase residual positions"}
    atomic_write_text(receipt_path, json.dumps(receipt, indent=2, sort_keys=True) + "\n")
    return receipt


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-root", default="data_bybit")
    parser.add_argument("--source-probe", default=DEFAULT_PROBE)
    parser.add_argument("--output-root", default=DEFAULT_OUTPUT)
    args = parser.parse_args()
    receipt = prepare(Path(args.base_root), Path(args.source_probe), Path(args.output_root))
    print(json.dumps({"base_release": receipt["base_release"], "symbols": receipt["symbols"],
                      "funding_repair": receipt["funding_repair"], "output": args.output_root}, indent=2))


if __name__ == "__main__":
    main()

from __future__ import annotations

from copy import deepcopy
from dataclasses import asdict
import hashlib
import json
from pathlib import Path

import polars as pl
import pytest

from scripts.prepare_bybit_daily_repairs import canonical_hash, checked_probe, merge_icx_funding, prepare
from stockagent.config import load_config
from stockagent.data.crypto_exchange_scope import validate_bybit_repair_view


def inputs():
    old = pl.DataFrame({
        "funding_timestamp_ms": [0], "funding_time_utc": ["1970-01-01 00:00:00"],
        "funding_rate": [0.001], "funding_mark_price": [2.0],
        "funding_mark_price_source": ["bybit_hourly_mark_kline_open"],
        "symbol": ["ICXUSDT"], "category": ["linear"],
        "bybit_funding_contract_version": [3], "funding_prefix_quarantined_events": [0],
        "funding_coverage_start_utc": ["1970-01-01 00:00:00"],
        "download_snapshot_utc": ["1970-01-01 01:00:00"],
    })
    # Variable schedule: preserve actual events, do not synthesize an 8h grid.
    events = [{"symbol": "ICXUSDT", "fundingRateTimestamp": str(t), "fundingRate": "0.001"}
              for t in (0, 3600000, 32400000)]
    rates = {"retCode": 0, "result": {"category": "linear", "list": events}}
    marks = {"retCode": 0, "result": {"category": "linear", "symbol": "ICXUSDT",
                                      "list": [[str(t), "2"] for t in (0, 3600000, 32400000)]}}
    probes = []
    for path, payload, query in [
        ("funding/history", rates, "startTime=0&endTime=36000000&limit=200"),
        ("mark-price-kline", marks, "interval=60&start=0&end=36000000&limit=400"),
    ]:
        probes.append({"url": f"https://api.bybit.com/v5/market/{path}?category=linear&symbol=ICXUSDT&{query}",
                       "http_status": 200, "response_json": payload,
                       "response_json_canonical_sha256": canonical_hash(payload)})
    return old, {"checked_at_utc": "1970-01-02T00:00:00Z", "api_probes": probes}


def rehash(document):
    for probe in document["api_probes"]:
        probe["response_json_canonical_sha256"] = canonical_hash(probe["response_json"])


def test_funding_tail_preserves_history_and_variable_schedule():
    old, document = inputs()
    before = deepcopy(document)
    merged, audit = merge_icx_funding(old, document)
    assert audit["added_events"] == 2
    assert audit["verified_overlap_events"] == 1
    assert merged.head(1).equals(old)
    assert merged["funding_timestamp_ms"].to_list() == [0, 3600000, 32400000]
    assert document == before


@pytest.mark.parametrize("fault", ["hash", "foreign", "result_identity", "failed", "truncated", "missing_mark", "duplicate", "overlap", "missing_overlap"])
def test_official_evidence_faults_fail_closed(fault):
    old, document = inputs()
    funding, mark = document["api_probes"]
    events = funding["response_json"]["result"]["list"]
    if fault == "hash":
        funding["response_json_canonical_sha256"] = "0" * 64
    elif fault == "foreign":
        funding["url"] = funding["url"].replace("api.bybit.com", "example.com")
    elif fault == "result_identity":
        mark["response_json"]["result"]["symbol"] = "BTCUSDT"
    elif fault == "failed":
        funding["response_json"]["retCode"] = 10001
    elif fault == "truncated":
        funding["url"] = funding["url"].replace("limit=200", "limit=3")
    elif fault == "missing_mark":
        mark["response_json"]["result"]["list"].pop()
    elif fault == "duplicate":
        events.append(dict(events[-1]))
    elif fault == "overlap":
        events[0]["fundingRate"] = "0.1"
    elif fault == "missing_overlap":
        events.pop(0)
    if fault != "hash":
        rehash(document)
    with pytest.raises(ValueError):
        merge_icx_funding(old, document)


def test_repair_cannot_write_source_or_arbitrary_directory(tmp_path):
    source = tmp_path / "immutable"
    source.mkdir()
    with pytest.raises(ValueError, match="bounded child"):
        prepare(source, tmp_path / "unused.json", source)


def test_v6_changes_only_repaired_view_and_artifact_identity():
    root = Path(__file__).resolve().parents[1]
    directory = root / "configs/markets"
    prefix = "bybit_perpetual_daily_0005_historical_public_pit_learned_cash_trajectory_"
    control = load_config(directory / f"{prefix}v5.yaml")
    candidate = load_config(directory / f"{prefix}v6.yaml")
    expected = asdict(control)
    expected["experiment_name"] = candidate.experiment_name
    expected["runner"]["output_dir"] = candidate.runner.output_dir
    expected["data"]["parquet_root"] = "artifacts/cache/bybit_perpetual_daily_repaired/perpetual_daily"
    expected["data"]["panel_cache_root"] = "artifacts/cache/bybit_perpetual_daily_repaired/panel"
    assert asdict(candidate) == expected
    assert candidate.runner.output_dir.endswith("_v6")


def test_repair_view_requires_same_universe_links_hashes_and_release(tmp_path):
    base = tmp_path / "data_bybit"
    (base / "perpetual_daily").mkdir(parents=True)
    view = tmp_path / "view/perpetual_daily"
    view.mkdir(parents=True)
    receipt = {"schema_version": 1, "base_root": str(base), "sources_sha256": {},
               "derived_sha256": {}, "symbols": {s: {} for s in ("HFTUSDT", "ICXUSDT", "VINEUSDT")}}
    for symbol in ["BTCUSDT", *receipt["symbols"]]:
        name = f"{symbol}_features.parquet"
        original = base / "perpetual_daily" / name
        original.write_bytes(b"source")
        receipt["sources_sha256"][str(original)] = hashlib.sha256(b"source").hexdigest()
        target = view / name
        if symbol == "BTCUSDT":
            target.symlink_to(original)
        else:
            target.write_bytes(b"derived")
            receipt["derived_sha256"][f"perpetual_daily/{name}"] = hashlib.sha256(b"derived").hexdigest()
    evidence = base / "funding_evidence.json"
    evidence.write_bytes(b"official evidence")
    receipt["sources_sha256"][str(evidence)] = hashlib.sha256(b"official evidence").hexdigest()
    (view.parent / "repair_manifest.json").write_text(json.dumps(receipt))
    validate_bybit_repair_view(view, venue_root=base)
    evidence.write_bytes(b"altered evidence")
    with pytest.raises(ValueError, match="input evidence changed"):
        validate_bybit_repair_view(view, venue_root=base)
    evidence.write_bytes(b"official evidence")
    (view / "ICXUSDT_features.parquet").write_bytes(b"tampered")
    with pytest.raises(ValueError, match="derived repair changed"):
        validate_bybit_repair_view(view, venue_root=base)
    with pytest.raises(ValueError, match="active base release"):
        validate_bybit_repair_view(view, venue_root=tmp_path / "other_release")

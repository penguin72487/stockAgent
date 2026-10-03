from datetime import date, datetime

import polars as pl
import pytest

from downloader.artifact_io import atomic_write_json, atomic_write_parquet, sha256_file
from downloader.download_shioaji_historical_market_data import (
    HistoryContract, RECEIPT_SCHEMA_VERSION, SOURCE, _kbar_paths, _write_inventory,
)
from stockagent.data.tw_stock_futures_kbars import (
    kbar_day_session_proof, read_futures_kbar_sources, validate_kbar_completion,
    validate_kbar_product_session,
)


DAY = date(2026, 9, 3)
NEXT = date(2026, 9, 4)


def _completion(hour):
    return {"start": str(DAY), "end": str(DAY),
            "finished_at_utc": f"2026-09-03T{hour-8:02}:00:00Z"}


@pytest.mark.parametrize("product,asset_class", [("OCF", "etf_future"), ("TJF", "index_future")])
def test_extended_product_requires_its_completed_day_session(product, asset_class):
    with pytest.raises(ValueError, match="before the completed"):
        validate_kbar_completion(_completion(14), product=product, asset_class=asset_class)
    validate_kbar_completion(_completion(17), product=product, asset_class=asset_class)
    exact = dict(_completion(16), finished_at_utc="2026-09-03T08:15:00Z")
    validate_kbar_completion(exact, product=product, asset_class=asset_class)
    with pytest.raises(ValueError, match="before the completed"):
        validate_kbar_completion(dict(exact, finished_at_utc="2026-09-03T08:14:59Z"),
                                 product=product, asset_class=asset_class)


def test_legacy_stock_and_domestic_index_completion_keep_1345_boundary():
    validate_kbar_completion(_completion(14))
    validate_kbar_completion(_completion(14), product="TX", asset_class="index_future")
    with pytest.raises(ValueError, match="before the completed"):
        validate_kbar_completion(dict(_completion(14), finished_at_utc="2026-09-03T05:44:59Z"))


def _write_sources(root, *, product="OCF", kind="etf_future", extra_time=(16, 0),
                   conflict=True, finished="2026-09-04T12:00:00Z", overlap=True):
    code = product + "I6"
    contract = HistoryContract(collection="exact_futures", priority=2, security_type="FUT",
                               asset_class="futures", code=code, root=product, name=product,
                               exchange="TAIFEX", begin_date=DAY, end_date=NEXT,
                               delivery_date=date(2026, 9, 16), delivery_month="202609")
    _write_inventory(root, [contract], completed_session=NEXT)
    for end, extra_qty in ([(DAY, 1), (NEXT, 2 if conflict else 1)] if overlap else [(DAY, 1)]):
        times = [datetime(2026, 9, 3, 8, 46), datetime(2026, 9, 3, *extra_time)]
        frame = pl.DataFrame({
            "ts": pl.Series(times).cast(pl.Datetime("ns")).cast(pl.Int64),
            "trading_date": [DAY, DAY], "query_contract": [code, code], "security_type": ["FUT", "FUT"],
            "Open": [20., 20.], "High": [20., 20.], "Low": [20., 20.], "Close": [20., 20.],
            "Volume": [1, extra_qty], "Amount": [20., 20. * extra_qty],
        })
        path, receipt_path = _kbar_paths(root, contract, DAY, end)
        atomic_write_parquet(path, frame)
        atomic_write_json(receipt_path, {
            "schema_version": RECEIPT_SCHEMA_VERSION, "source": SOURCE, "method": "kbars", "status": "complete",
            "contract": code, "security_type": "FUT", "start": str(DAY), "end": str(end), "rows": 2,
            "observed_trading_dates": [str(DAY)], "sha256": sha256_file(path), "finished_at_utc": finished,
        })
    return pl.DataFrame({"date": [DAY], "physical_contract": [product + ":202609"],
                         "product": [product], "asset_class": [kind], "volume": [3]})


@pytest.mark.parametrize("product,kind,extra_time", [
    ("OCF", "etf_future", (16, 0)), ("TJF", "index_future", (8, 10)),
])
def test_overlapping_chunks_compare_whole_product_day_not_stock_hours(tmp_path, product, kind, extra_time):
    selected = _write_sources(tmp_path, product=product, kind=kind, extra_time=extra_time)
    with pytest.raises(ValueError, match="conflicting overlapping"):
        read_futures_kbar_sources(tmp_path, selected, [DAY], all_futures=True)


def test_matching_extended_chunks_emit_bound_session_proof(tmp_path):
    selected = _write_sources(tmp_path, conflict=False)
    bars, sources, missing = read_futures_kbar_sources(tmp_path, selected, [DAY], all_futures=True)
    assert not missing and bars["minute"].to_list() == [526]
    source = sources[0]["contracts"][0]
    assert source["observed_day_volume"] == 2
    assert source["product"] == "OCF" and source["asset_class"] == "etf_future"
    assert source["day_session"] == kbar_day_session_proof(DAY, product="OCF", asset_class="etf_future")
    validate_kbar_product_session(source, day=DAY)
    for wrong in [dict(source, day_session=None), dict(source, product="NYF"),
                  dict(source, day_session={**source["day_session"], "end_minute": 825})]:
        with pytest.raises(ValueError, match="session|identity"):
            validate_kbar_product_session(wrong, day=DAY)


def test_reader_rejects_extended_session_receipt_completed_at_1400(tmp_path):
    selected = _write_sources(tmp_path, overlap=False, finished="2026-09-03T06:00:00Z")
    with pytest.raises(ValueError, match="before the completed"):
        read_futures_kbar_sources(tmp_path, selected, [DAY], all_futures=True)


def test_minute_validator_rechecks_product_proof_after_hash_valid_rewrite(tmp_path):
    from scripts.build_tw_stock_futures_0900_entries import _all_futures_coverage, _add_all_futures_coverage_manifest
    from stockagent.data.tw_stock_futures_kbars import KBAR_SOURCE, contract_sources_digest
    from stockagent.data.tw_stock_futures_minute import MINUTE_DATASET, MINUTE_CONTRACT_VERSION, validate_futures_minute_data

    root = tmp_path / "raw"
    selected = _write_sources(root, overlap=False)
    bars, sources, missing = read_futures_kbar_sources(root, selected, [DAY], all_futures=True)
    path = tmp_path / "bundle" / "minutes.parquet"
    atomic_write_parquet(path, bars)
    manifest = {"dataset": MINUTE_DATASET, "contract_version": MINUTE_CONTRACT_VERSION,
                "source_kind": KBAR_SOURCE, "status": "partial", "partial_supplement_only": True,
                "source_daily_sha256": "daily", "covered_dates": [str(DAY)], "sources": sources,
                "outputs": {"minutes": {"sha256": sha256_file(path)}}}
    _add_all_futures_coverage_manifest(manifest, _all_futures_coverage(selected, sources, kbars=True), path.parent)
    atomic_write_json(path.with_name("manifest.json"), manifest)
    validate_futures_minute_data(path, daily_sha256="daily", require_complete=False)
    with pytest.raises(ValueError, match="completeness"):
        validate_futures_minute_data(path, daily_sha256="daily")
    source = sources[0]["contracts"][0]
    source["day_session"]["end_minute"] = 825
    sources[0]["sha256"] = contract_sources_digest(sources[0]["contracts"])
    atomic_write_json(path.with_name("manifest.json"), manifest)
    with pytest.raises(ValueError, match="session proof"):
        validate_futures_minute_data(path, daily_sha256="daily", require_complete=False)

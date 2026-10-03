from datetime import date, datetime
import json
from pathlib import Path
from types import SimpleNamespace

import polars as pl
import pytest

from downloader.artifact_io import atomic_write_json, atomic_write_parquet, sha256_file
from stockagent.data.tw_stock_futures_history import build_continuous_history
from stockagent.data.tw_stock_futures_minute import select_futures_minute_candidates, validate_futures_minute_data
from stockagent.data.tw_stock_futures_repair import ExactMinuteRecovery, NO_CAPACITY, NO_TRADE
from test_tw_stock_futures_day_trade import _candidate
from test_tw_stock_futures_history import ticks


DAY = date(2020, 3, 23)


def source_rows():
    rows = []
    for product, kind, multiplier in [("CDF", "stock_future", 2000.), ("OCF", "etf_future", 10000.)]:
        rows.append({**_candidate(day=DAY, product=product, prior_volume=100, current_volume=10,
                                  open_price=100., close_price=108.),
                     "physical_contract": f"{product}:202004", "contract": "202004",
                     "shioaji_roots": product, "high": 110., "low": 100.,
                     "asset_class": kind, "contract_multiplier": multiplier,
                     "underlying_symbol": "2330" if kind == "stock_future" else None})
    return pl.DataFrame(rows)


def write_ticks(root, product):
    alias = product + "R1"
    frame = ticks(DAY).with_columns(pl.lit(alias).alias("query_contract"))
    path = root / alias / "ticks" / f"trading_date={DAY}" / "data.parquet"
    receipt = root / alias / "receipts" / f"trading_date={DAY}.json"
    atomic_write_parquet(path, frame)
    atomic_write_json(receipt, {"source": "shioaji_continuous_futures_historical_ticks_v1",
                               "schema_version": 1, "contract": alias, "trading_date": str(DAY),
                               "status": "complete", "rows": frame.height, "sha256": sha256_file(path),
                               "session_finalized": True})
    return receipt


def args_for(tmp_path):
    return SimpleNamespace(daily_proxy_before=str(DAY), check_only=False,
                           shioaji_ticks_root=tmp_path / "raw", output_dir=tmp_path / "base",
                           work_dir=tmp_path / "cache", daily_data_path=tmp_path / "daily.parquet",
                           workers=2, assemble_cached=False, capacity_participation=.5,
                           capacity_rounding="floor", scope="stock_front")


def test_all_scope_uses_existing_selector_without_stock_tier_or_underlying_filter():
    source = source_rows()
    assert select_futures_minute_candidates(source)["product"].to_list() == ["CDF"]
    assert select_futures_minute_candidates(source, scope="all_futures_intraday")["product"].to_list() == ["CDF", "OCF"]
    duplicate = pl.concat([source, source.tail(1)])
    with pytest.raises(ValueError, match="duplicate physical"):
        select_futures_minute_candidates(duplicate, scope="all_futures_intraday")


def test_incremental_base_is_immutable_and_missing_product_uses_second_root(tmp_path):
    args = args_for(tmp_path)
    source = source_rows()
    atomic_write_parquet(args.daily_data_path, source)
    digest = sha256_file(args.daily_data_path)
    old_receipt = write_ticks(args.shioaji_ticks_root, "CDF")
    assert build_continuous_history(args, source, [DAY], digest) == 0
    before = {path.name: sha256_file(path) for path in args.output_dir.iterdir()}
    # The base is a verified immutable snapshot; its original raw endpoint may
    # no longer retain the historical query. Reuse does not rewrite this proof.
    old_receipt.unlink()
    args.base_minute_bundle = args.output_dir
    args.scope = "all_futures_intraday"
    args.output_dir = tmp_path / "expanded"
    args.additional_shioaji_ticks_root = [tmp_path / "separate_index_history"]
    write_ticks(args.additional_shioaji_ticks_root[0], "OCF")
    assert build_continuous_history(args, source, [DAY], digest) == 0
    assert {path.name: sha256_file(path) for path in args.base_minute_bundle.iterdir()} == before
    coverage = pl.read_parquet(args.output_dir / "coverage.parquet")
    assert coverage.height == 2 and coverage["status"].to_list() == ["minute_verified"] * 2
    manifest = json.loads((args.output_dir / "manifest.json").read_text())
    assert manifest["base_bundle"]["reused_contract_days"] == 1
    assert manifest["expected_contract_days"] == 2
    assert manifest["source_scope"] == "all_futures_intraday"
    validate_futures_minute_data(args.output_dir / "minutes.parquet", daily_sha256=digest,
                                daily_proxy_before=str(DAY), participation=.5)
    args.assemble_cached = True
    assert build_continuous_history(args, source, [DAY], digest) == 0
    shard = next(p for p in args.work_dir.rglob(f"{DAY}.json") if "all_futures_intraday" in str(p))
    payload = json.loads(shard.read_text())
    payload["expected_keys_sha256"] = "stock-only-does-not-prove-all-futures"
    atomic_write_json(shard, payload)
    with pytest.raises(ValueError, match="missing/corrupt dated shard"):
        build_continuous_history(args, source, [DAY], digest)


def test_all_scope_cannot_assemble_stock_cache_or_inherit_daily_proxy(tmp_path):
    args = args_for(tmp_path)
    source = source_rows()
    atomic_write_parquet(args.daily_data_path, source)
    digest = sha256_file(args.daily_data_path)
    write_ticks(args.shioaji_ticks_root, "CDF")
    assert build_continuous_history(args, source, [DAY], digest) == 0
    args.scope = "all_futures_intraday"
    args.output_dir = tmp_path / "all"
    args.assemble_cached = True
    with pytest.raises(ValueError, match="missing/corrupt dated shard"):
        build_continuous_history(args, source, [DAY], digest)
    args.daily_proxy_before = "2020-03-24"
    with pytest.raises(ValueError, match="daily proxies"):
        build_continuous_history(args, source, [DAY], digest)


@pytest.mark.parametrize("volume,rounding,expected", [(0, "floor", NO_TRADE), (1, "floor", NO_CAPACITY),
                                                       (1, "ceil", "missing_receipt"), (2, "floor", "missing_receipt")])
def test_independent_official_proof_never_fabricates_minute_bars(volume, rounding, expected):
    from stockagent.data.tw_stock_futures_history import MINUTE_SCHEMA
    row = source_rows().row(1, named=True)
    official = pl.DataFrame([{"date": DAY, "physical_contract": row["physical_contract"],
                              "outright_volume": volume, "official_reason": "zero_total_volume" if volume == 0 else "outright_positive",
                              "official_day_sources": '["independently-verified-sha"]'}])
    recovery = ExactMinuteRecovery(None, official, participation=.5, capacity_rounding=rounding)
    bars, evidence = recovery.recover(row, pl.DataFrame(schema=MINUTE_SCHEMA), {"status": "missing_receipt"})
    assert bars.is_empty() and evidence["status"] == expected


def test_builder_config_selects_all_scope_without_stock_quarantine(monkeypatch):
    import sys
    from scripts import build_tw_stock_futures_0900_entries as builder
    monkeypatch.setattr(sys, "argv", ["builder", "--config", "configs/markets/tw_futures_v8_intraday.yaml",
                                    "--shioaji-ticks-root", "raw", "--check-only"])
    args = builder.parse_args()
    assert args.scope == "all_futures_intraday" and args.capacity_rounding == "floor"
    assert args.execution_policy == "scheduled_0846"
    assert args.quarantine_dates == [] and args.quarantine_contract_days == []
    assert args.daily_proxy_before == args.start_date
    assert isinstance(args.daily_data_path, Path)
    from stockagent.config import load_config
    configured = load_config("configs/markets/tw_futures_v8_intraday.yaml")
    assert args.daily_data_path == Path(configured.trading.tw_futures_portfolio_data_path)


@pytest.mark.parametrize("partial", [False, True])
def test_all_futures_kbars_resolve_exchange_product_alias_and_bind_coverage(tmp_path, monkeypatch, partial):
    from downloader.download_shioaji_historical_market_data import (
        HistoryContract, RECEIPT_SCHEMA_VERSION, SOURCE, _kbar_paths, _write_inventory,
    )
    from scripts import build_tw_stock_futures_0900_entries as builder
    from stockagent.data.tw_futures_portfolio_daily import TAIFEX_FUTURES_PORTFOLIO_DATA_CONTRACT_VERSION
    from test_tw_stock_futures_kbars import kbars
    day = date(2026, 9, 3)
    source = source_rows().head(1).with_columns(
        pl.lit(day).alias("date"), pl.lit("TX").alias("product"), pl.lit("index_future").alias("asset_class"),
        pl.lit("TXF").alias("shioaji_roots"), pl.lit("TX:202609").alias("physical_contract"),
        pl.lit("202609").alias("contract"), pl.lit(None, dtype=pl.String).alias("underlying_symbol"),
        pl.lit(200.).alias("contract_multiplier"),
    )
    daily = tmp_path / "daily" / "continuous_daily.parquet"
    if partial:
        source = pl.concat([source, source.with_columns(
            pl.lit("MTX").alias("product"), pl.lit("MXF").alias("shioaji_roots"),
            pl.lit("MTX:202609").alias("physical_contract"),
        )])
    atomic_write_parquet(daily, source)
    digest = sha256_file(daily)
    atomic_write_json(daily.with_name("manifest.json"), {
        "contract_version": TAIFEX_FUTURES_PORTFOLIO_DATA_CONTRACT_VERSION,
        "outputs": {"continuous_daily": {"sha256": digest}},
    })
    root = tmp_path / "kbars"
    contract = HistoryContract(collection="exact_futures", priority=2, security_type="FUT",
                               asset_class="futures", code="TXFI6", root="TXF", name="TX",
                               exchange="TAIFEX", begin_date=day, end_date=day,
                               delivery_date=date(2026, 9, 16), delivery_month="202609")
    _write_inventory(root, [contract], completed_session=day)
    data_path, receipt_path = _kbar_paths(root, contract, day, day)
    raw = kbars().with_columns(pl.lit(contract.code).alias("query_contract"))
    atomic_write_parquet(data_path, raw)
    atomic_write_json(receipt_path, {
        "schema_version": RECEIPT_SCHEMA_VERSION, "source": SOURCE, "method": "kbars",
        "status": "complete", "contract": contract.code, "security_type": "FUT",
        "start": str(day), "end": str(day), "rows": raw.height,
        "observed_trading_dates": [str(day)], "sha256": sha256_file(data_path),
        "finished_at_utc": "2026-09-03T10:00:00Z",
    })
    args = SimpleNamespace(start_date=str(day), end_date=str(day), daily_data_path=daily,
                           output_dir=tmp_path / "output", execution_policy="scheduled_0846", scope="all_futures_intraday",
                           check_only=False, minute_root=root, ticks_root=None, archive_override=[])
    args.allow_partial_supplement = partial
    monkeypatch.setattr(builder, "parse_args", lambda: args)
    assert builder.main() == (2 if partial else 0)
    coverage = pl.read_parquet(args.output_dir / "coverage.parquet")
    assert coverage["physical_contract"].to_list() == ["TX:202609"]
    if partial:
        with pytest.raises(ValueError, match="completeness"):
            validate_futures_minute_data(args.output_dir / "minutes.parquet", daily_sha256=digest)
    validate_futures_minute_data(args.output_dir / "minutes.parquet", daily_sha256=digest,
                                require_complete=not partial)
    # A coverage SHA alone cannot relabel a different physical contract.
    coverage = coverage.with_columns(pl.lit("MTX:202609").alias("physical_contract"))
    manifest_path = args.output_dir / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    builder._add_all_futures_coverage_manifest(manifest, coverage, args.output_dir)
    atomic_write_json(manifest_path, manifest)
    with pytest.raises(ValueError, match="completed raw contract"):
        validate_futures_minute_data(args.output_dir / "minutes.parquet", daily_sha256=digest,
                                    require_complete=not partial)


@pytest.mark.parametrize("case", ["valid", "roll", "overlap", "wrong_query_date", "invalid_receipt"])
def test_foreign_etf_event_day_reconstruction_preserves_raw_partition_identity(tmp_path, case):
    from stockagent.data.tw_stock_futures_history import _contract_day, _reuse_verified_contract
    write_ticks(tmp_path, "OCF")
    next_day = date(2020, 3, 24)
    times = [datetime(2020, 3, 23, 16, 12), datetime(2020, 3, 24, 8, 45)]
    if case == "overlap":
        times[0] = datetime(2020, 3, 23, 13, 44, 59)
    tail = pl.DataFrame({"event_ts": times, "close": [111., 115.], "volume": [2, 3],
                         "source_row_index": [0, 1], "trading_date": [next_day] * 2,
                         "query_contract": ["OCFR1"] * 2}).with_columns(pl.col("event_ts").cast(pl.Datetime("ns")))
    tail = tail.with_columns(pl.col("event_ts").cast(pl.Int64).alias("ts"))
    if case == "wrong_query_date":
        tail = tail.with_columns(pl.lit(DAY).alias("trading_date"))
    data_path = tmp_path / "OCFR1" / "ticks" / f"trading_date={next_day}" / "data.parquet"
    receipt_path = tmp_path / "OCFR1" / "receipts" / f"trading_date={next_day}.json"
    atomic_write_parquet(data_path, tail)
    atomic_write_json(receipt_path, {"source": "shioaji_continuous_futures_historical_ticks_v1",
                                   "schema_version": 1, "contract": "OCFR1", "trading_date": str(next_day),
                                   "status": "complete", "rows": 2, "sha256": sha256_file(data_path),
                                   "session_finalized": case != "invalid_receipt"})
    original_sha = sha256_file(data_path)
    row = {**source_rows().row(1, named=True), "calendar_physical": "OCF:202004", "high": 111., "close": 111., "volume": 12,
           "_next_query_date": next_day, "_next_calendar_physical": "OCF:202005" if case == "roll" else "OCF:202004",
           "_source_scope": "all_futures_intraday"}
    bars, evidence = _contract_day(row, tmp_path, DAY)
    assert sha256_file(data_path) == original_sha
    if case == "valid":
        assert evidence["status"] == "minute_verified" and evidence["tick_volume"] == 12
        assert bars["volume"].sum() == 12 and 973 in bars["minute"].to_list()
        partitions = json.loads(evidence["source_partitions"])
        assert [p["query_date"] for p in partitions] == [str(DAY), str(next_day)]
        assert _reuse_verified_contract(tmp_path, evidence)
        receipt_path.write_text(receipt_path.read_text() + "\n")
        assert not _reuse_verified_contract(tmp_path, evidence)
    else:
        assert bars.is_empty() and evidence["status"] != "minute_verified"


@pytest.mark.parametrize("product,alias_root,expected", [("TX", "TXF", "minute_verified"),
                                                         ("MTX", "TXF", "invalid_receipt"),
                                                         ("CDF", "CDF", "invalid_receipt")])
def test_legacy_tx_receipt_is_reused_only_for_txfr1_identity(tmp_path, product, alias_root, expected):
    from stockagent.data.tw_stock_futures_history import _contract_day
    receipt_path = write_ticks(tmp_path, alias_root)
    receipt = json.loads(receipt_path.read_text())
    receipt["source"] = "shioaji_txfr1_historical_ticks_v1"
    atomic_write_json(receipt_path, receipt)
    physical = product + ":202004"
    row = {**source_rows().row(0, named=True), "product": product, "physical_contract": physical,
           "calendar_physical": physical, "shioaji_roots": alias_root,
           "asset_class": "index_future" if product in {"TX", "MTX"} else "stock_future",
           "_source_scope": "all_futures_intraday"}
    _, evidence = _contract_day(row, tmp_path, DAY)
    assert evidence["status"] == expected


def test_all_scope_volume_bound_rejects_raw_and_reused_facts_without_clipping(tmp_path):
    from stockagent.data.tw_stock_futures_history import _contract_day, _enforce_observed_volume_bound
    write_ticks(tmp_path, "CDF")
    row = {**source_rows().row(0, named=True), "calendar_physical": "CDF:202004", "volume": 9,
           "_source_scope": "all_futures_intraday"}
    bars, evidence = _contract_day(row, tmp_path, DAY)
    assert bars.is_empty() and evidence["status"] == "invalid_source"
    assert evidence["tick_volume"] == 10 and "exceeds official" in evidence["detail"]
    for repair_kind in ["", "exact_kbars", "exact_ticks", "finmind_ticks"]:
        _, rejected = _enforce_observed_volume_bound(row, bars, dict(evidence, status="minute_verified", repair_kind=repair_kind))
        assert rejected["status"] == "invalid_source" and rejected["tick_volume"] == 10
    old_row = {k: v for k, v in row.items() if k != "_source_scope"}
    assert _contract_day(old_row, tmp_path, DAY)[1]["status"] == "minute_verified"


def test_incremental_revalidation_never_removes_scope_or_reaccepts_excess_base(tmp_path):
    args = args_for(tmp_path)
    source = source_rows().head(1).with_columns(pl.lit(9.).alias("volume"))
    atomic_write_parquet(args.daily_data_path, source)
    digest = sha256_file(args.daily_data_path)
    write_ticks(args.shioaji_ticks_root, "CDF")
    assert build_continuous_history(args, source, [DAY], digest) == 0
    args.base_minute_bundle, args.output_dir = args.output_dir, tmp_path / "expanded"
    args.scope = "all_futures_intraday"
    assert build_continuous_history(args, source, [DAY], digest) == 2
    coverage = pl.read_parquet(args.output_dir / "coverage.parquet")
    assert coverage.height == 1 and coverage["status"].item() == "invalid_source"
    args.revalidate_contract_days = tmp_path / "revalidate.json"
    atomic_write_json(args.revalidate_contract_days, [{"date": str(DAY), "physical_contract": "OCF:202004"}])
    with pytest.raises(ValueError, match="must belong to the selected universe"):
        build_continuous_history(args, source, [DAY], digest)


def test_supplemental_bundle_fills_only_missing_physical_keys(tmp_path):
    args = args_for(tmp_path)
    source = source_rows()
    atomic_write_parquet(args.daily_data_path, source)
    digest = sha256_file(args.daily_data_path)
    first_receipt = write_ticks(args.shioaji_ticks_root, "CDF")
    assert build_continuous_history(args, source, [DAY], digest) == 0
    first = args.output_dir
    first_receipt.unlink()
    args.scope, args.output_dir = "all_futures_intraday", tmp_path / "supplement"
    write_ticks(args.shioaji_ticks_root, "OCF")
    assert build_continuous_history(args, source, [DAY], digest) == 2
    args.base_minute_bundle, args.supplemental_minute_bundle = first, [args.output_dir]
    args.output_dir = tmp_path / "merged"
    assert build_continuous_history(args, source, [DAY], digest) == 0
    coverage = pl.read_parquet(args.output_dir / "coverage.parquet")
    assert coverage.height == 2 and coverage["status"].to_list() == ["minute_verified"] * 2
    manifest = json.loads((args.output_dir / "manifest.json").read_text())
    assert manifest["base_bundle"]["reused_contract_days"] == 2
    assert [r["reused_contract_days"] for r in manifest["base_bundle"]["bundles"]] == [1, 1]


def test_all_scope_exact_kbar_repair_rejects_excess_before_price_quarantine(tmp_path):
    from test_tw_stock_futures_repair import recovery_fixture
    from stockagent.data.tw_stock_futures_history import MINUTE_SCHEMA
    recovery, row, receipt_path = recovery_fixture(tmp_path, volume=2, empty=False)
    raw_path = receipt_path.with_name("data.parquet")
    raw = pl.read_parquet(raw_path).with_columns(pl.lit(3).alias("Volume"), pl.lit(300.).alias("Amount"))
    atomic_write_parquet(raw_path, raw)
    receipt = json.loads(receipt_path.read_text())
    receipt["sha256"] = sha256_file(raw_path)
    atomic_write_json(receipt_path, receipt)
    row.update(_source_scope="all_futures_intraday", product="CDF", asset_class="stock_future", volume=2)
    bars, evidence = recovery.recover(row, pl.DataFrame(schema=MINUTE_SCHEMA), {"status": "missing_receipt"})
    assert bars.is_empty() and evidence["status"] == "invalid_repair_source"
    assert "volume exceeds" in evidence["detail"]


def test_full_official_bounds_reject_previously_accepted_base_and_validator_metadata(tmp_path):
    args = args_for(tmp_path)
    source = source_rows().head(1)
    atomic_write_parquet(args.daily_data_path, source)
    digest = sha256_file(args.daily_data_path)
    write_ticks(args.shioaji_ticks_root, "CDF")
    assert build_continuous_history(args, source, [DAY], digest) == 0
    args.base_minute_bundle, args.output_dir = args.output_dir, tmp_path / "expanded"
    args.scope = "all_futures_intraday"
    args.official_evidence_dir = tmp_path / "proof"
    official = pl.DataFrame([{"date": DAY, "physical_contract": "CDF:202004", "outright_volume": 9,
                             "official_volume": 10, "official_reason": "outright_volume",
                             "official_day_sources": '["verified-official-archive"]'}])
    atomic_write_parquet(args.official_evidence_dir / "official_evidence.parquet", official)
    atomic_write_json(args.official_evidence_dir / "official_evidence_manifest.json", {
        "source": "taifex_complete_daily_and_spread_legs_v1", "sources": [],
        "sha256": sha256_file(args.official_evidence_dir / "official_evidence.parquet")})
    assert build_continuous_history(args, source, [DAY], digest) == 2
    coverage_path = args.output_dir / "coverage.parquet"
    coverage = pl.read_parquet(coverage_path)
    assert coverage.height == 1 and coverage["status"].item() == "invalid_source"
    assert "volume bound 9" in coverage["detail"].item()
    # Even rewritten output hashes cannot turn contradictory quantities into
    # an accepted source. The reader checks the same full official bounds.
    atomic_write_parquet(coverage_path, coverage.with_columns(pl.lit("minute_verified").alias("status")))
    manifest_path = args.output_dir / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["outputs"]["coverage"]["sha256"] = sha256_file(coverage_path)
    atomic_write_json(manifest_path, manifest)
    with pytest.raises(ValueError, match="volume exceeds official capacity"):
        validate_futures_minute_data(args.output_dir / "minutes.parquet", daily_sha256=digest,
                                    daily_proxy_before=str(DAY), participation=.5, require_complete=False)

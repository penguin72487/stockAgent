from datetime import date, datetime
import json

import numpy as np
import polars as pl
import pytest
import torch

from downloader.artifact_io import atomic_write_json, atomic_write_parquet, sha256_file
from stockagent.config import load_config
from stockagent.data.tw_stock_futures_history import (
    HISTORY_DATASET, HISTORY_SOURCE, MINUTE_SCHEMA, _contract_day, normalize_continuous_ticks, _reuse_verified_contract,
    build_continuous_history,
)
from stockagent.data.tw_stock_futures_minute import (
    TAPE_FIELDS, HYBRID_TAPE_FIELDS, load_futures_minute_tape, validate_futures_minute_data,
)
from stockagent.data.tw_price_rules import TW_DERIVATIVE_PRICE_CONTRACT_VERSION
from stockagent.training.checkpoint_contract import _trading_checkpoint_contract, _configuration_fingerprint_snapshot
from stockagent.training.trainer import _mode_artifact_contract_for_config
from test_tw_stock_futures_minute import tape, execute


def ticks(day=date(2020, 3, 23)):
    times = [datetime.combine(day, datetime.strptime(t, "%H:%M:%S").time())
             for t in ["08:45:59", "08:45:00", "13:19:59", "13:24:01", "13:29:59", "13:44:59"]]
    return pl.DataFrame({"event_ts": times, "close": [102., 100., 105., 110., 109., 108.],
                         "volume": [1, 3, 1, 3, 1, 1], "source_row_index": range(6),
                         "trading_date": [day] * 6, "query_contract": ["CDFR1"] * 6}).with_columns(
        pl.col("event_ts").cast(pl.Datetime("ns")),
    ).with_columns(pl.col("event_ts").cast(pl.Int64).alias("ts"))


def test_wall_clock_sort_and_full_minutes():
    frame, stats = normalize_continuous_ticks(ticks(), day=date(2020, 3, 23), alias="CDFR1",
                                              physical="CDF:202004", digest="a" * 64)
    assert stats["tick_open"] == 100 and stats["tick_close"] == 108
    assert frame.filter(pl.col("minute") == 526)["vwap"].item() == 100.5
    assert frame.filter(pl.col("minute") == 805)["close"].item() == 110
    assert 825 in frame["minute"].to_list()  # Retain full minutes separately from execution events.
    bad = ticks().with_columns(pl.lit("CDFR2").alias("query_contract"))
    with pytest.raises(ValueError, match="identity"):
        normalize_continuous_ticks(bad, day=date(2020, 3, 23), alias="CDFR1", physical="CDF:202004", digest="x")


def test_historical_futures_print_uses_its_session_tick_before_vwap():
    before = ticks(date(2026, 7, 3)).with_columns(
        pl.when(pl.col("source_row_index") == 0).then(pl.lit(1001.))
        .otherwise(pl.col("close")).alias("close")
    )
    with pytest.raises(ValueError, match="off-grid dated"):
        normalize_continuous_ticks(before, day=date(2026, 7, 3), alias="CDFR1",
                                   physical="CDF:202607", digest="a" * 64)
    after = ticks(date(2026, 7, 6)).with_columns(
        pl.when(pl.col("source_row_index") == 0).then(pl.lit(1001.))
        .otherwise(pl.col("close")).alias("close")
    )
    frame, _ = normalize_continuous_ticks(after, day=date(2026, 7, 6), alias="CDFR1",
                                          physical="CDF:202607", digest="a" * 64)
    assert frame.filter(pl.col("minute") == 526)["close"].item() == 1001.


def test_historical_mapping_ignores_current_target_and_rejects_mismatched_prices(tmp_path):
    day = date(2020, 3, 23)
    root = tmp_path / "CDFR1"
    path = root / "ticks" / f"trading_date={day}" / "data.parquet"
    atomic_write_parquet(path, ticks(day))
    receipt_path = root / "receipts" / f"trading_date={day}.json"
    atomic_write_json(receipt_path, {"source": "shioaji_continuous_futures_historical_ticks_v1",
                                   "schema_version": 1, "contract": "CDFR1", "trading_date": str(day),
                                   "status": "complete", "rows": 6, "sha256": sha256_file(path),
                                   "resolved_target_code_at_query": "CDFI6"})
    row = {"date": day, "physical_contract": "CDF:202004", "calendar_physical": "CDF:202004",
           "product": "CDF", "shioaji_roots": "CDF", "volume": 100, "source_row_observed": True,
           "open": 100., "high": 110., "low": 100., "close": 108.}
    bars, receipt = _contract_day(row, tmp_path, date(2020, 1, 1))
    assert receipt["status"] == "minute_verified" and receipt["tick_volume"] == 10
    assert bars["physical_contract"].unique().to_list() == ["CDF:202004"]
    assert bars["volume"].sum() == 10  # Never inflate volume to the official daily total.
    assert _reuse_verified_contract(tmp_path, receipt)
    raw = receipt_path.read_text()
    receipt_path.write_text(raw + "\n")
    assert not _reuse_verified_contract(tmp_path, receipt)
    receipt_path.write_text(raw)
    bars, receipt = _contract_day({**row, "open": 99.}, tmp_path, date(2020, 1, 1))
    assert bars.is_empty() and receipt["status"] == "ohlc_identity_mismatch"
    bars, receipt = _contract_day({**row, "calendar_physical": "CDF:202005"}, tmp_path, date(2020, 1, 1))
    assert bars.is_empty() and receipt["status"] == "unverified_calendar"


@pytest.mark.parametrize("direction", [1, -1])
def test_daily_proxy_and_minute_share_integer_accounting_and_have_gradients(direction):
    minute = tape(rows=2)
    hybrid = torch.nn.functional.pad(minute, (0, HYBRID_TAPE_FIELDS - TAPE_FIELDS))
    hybrid[0, ..., :TAPE_FIELDS] = 0
    hybrid[0, ..., :3] = minute[0, ..., :3]
    hybrid[0, ..., TAPE_FIELDS:] = torch.tensor([1, 100, 110, 100])
    w = torch.full((2, 1), direction * .25, requires_grad=True)
    actual, expected = execute(w, hybrid), execute(w, minute)
    torch.testing.assert_close(actual.strategy_returns, expected.strategy_returns)
    torch.testing.assert_close(actual.contract_quantities_history, expected.contract_quantities_history)
    torch.testing.assert_close(actual.turnovers, expected.turnovers)
    actual.strategy_returns.sum().backward()
    assert torch.isfinite(w.grad).all() and (w.grad.abs() > 0).all()
    assert actual.final_alive and not actual.residual_contract_quantities_history.any()


def test_hybrid_never_rescues_post_cutoff_failed_exit():
    x = torch.nn.functional.pad(tape(), (0, 4))
    x[..., 3 + 6 * 5:TAPE_FIELDS] = 0
    x[..., TAPE_FIELDS + 1:] = torch.tensor([100, 999, 100])
    result = execute(torch.tensor([[.25]]), x)
    assert not result.final_alive and result.residual_contract_quantities_history.any()


def historical_fixture(tmp_path, day=date(2019, 12, 31)):
    path = tmp_path / "minutes.parquet"
    atomic_write_parquet(path, pl.DataFrame(schema=MINUTE_SCHEMA))
    coverage = pl.DataFrame({"date": [day], "physical_contract": ["CDF:202001"],
                             "status": ["daily_open_close_proxy"], "source_file_sha256": [""]})
    atomic_write_parquet(tmp_path / "coverage.parquet", coverage)
    manifest = {"dataset": HISTORY_DATASET, "source_kind": HISTORY_SOURCE, "contract_version": 2,
                "status": "complete", "source_daily_sha256": "daily", "daily_proxy_before": "2020-01-01",
                "covered_dates": [str(day)], "requested_dates": [str(day)], "outputs": {
                    k: {"file": f"{k}.parquet", "sha256": sha256_file(tmp_path / f"{k}.parquet")}
                    for k in ("minutes", "coverage")}}
    atomic_write_json(tmp_path / "manifest.json", manifest)
    keys = pl.DataFrame({"date": [day], "physical_contract": ["CDF:202001"], "underlying_symbol": ["2330"],
                        "candidate_slot": [0], "contract_multiplier": [2000.], "open": [100.], "close": [110.],
                        "previous_volume": [20], "volume": [100], "source_row_observed": [True], "executable": [True]})
    return path, keys, manifest


def test_loader_date_boundary_causal_policy_and_no_daily_prices_in_minute_channels(tmp_path):
    path, keys, _ = historical_fixture(tmp_path)
    def load(keys):
        return load_futures_minute_tape(path, keys, np.array(["2019-12-31"], dtype="datetime64[D]"), ("2330",),
                                       daily_sha256="daily", fee=40, participation=.5, daily_proxy_before="2020-01-01")[0]
    x = load(keys)
    assert x.shape == (1, 1, 2, HYBRID_TAPE_FIELDS)
    assert not x[..., 3:TAPE_FIELDS].any()
    assert x[0, 0, 0, TAPE_FIELDS:].tolist() == [1., 100., 110., 10.]
    changed = load(keys.with_columns(pl.lit(0).alias("volume"), pl.lit(None, dtype=pl.Float64).alias("close")))
    np.testing.assert_array_equal(x[..., :3], changed[..., :3])  # Future data never shrinks candidate mask.
    with pytest.raises(ValueError, match="cutoff"):
        validate_futures_minute_data(path, daily_sha256="daily", daily_proxy_before="2020-03-22")


def test_no_post_cutoff_daily_fallback_and_no_missing_date_zero_returns(tmp_path):
    path, _, manifest = historical_fixture(tmp_path, date(2020, 1, 2))
    with pytest.raises(ValueError, match="crosses"):
        validate_futures_minute_data(path, daily_sha256="daily", daily_proxy_before="2020-01-01")
    manifest.update(status="partial", covered_dates=[])
    atomic_write_json(tmp_path / "manifest.json", manifest)
    with pytest.raises(ValueError, match="uncovered"):
        validate_futures_minute_data(path, daily_sha256="daily", daily_proxy_before="2020-01-01")


def test_checkpoint_and_reporting_reject_semantically_interchangeable_claim():
    old = load_config("configs/markets/tw_stock_futures_day_trade_0845_minute.yaml")
    new = load_config("configs/markets/tw_stock_futures_day_trade_0845_historical.yaml")
    a = _trading_checkpoint_contract(old)["taiwan_stock_futures_day_trade"]
    b = _trading_checkpoint_contract(new)["taiwan_stock_futures_day_trade"]
    assert a["data_contract_version"] == 1 and b["data_contract_version"] == 2
    assert len(b["execution_tensor_channels"]) == HYBRID_TAPE_FIELDS
    assert b["daily_proxy_before"] == "2020-01-01" and a != b
    assert "tw_stock_futures_day_trade_daily_proxy_before" not in _configuration_fingerprint_snapshot(old)["trading"]
    assert _configuration_fingerprint_snapshot(new)["trading"]["tw_stock_futures_day_trade_daily_proxy_before"] == "2020-01-01"
    assert 'tw_stock_futures_day_trade_quarantine_dates' not in _configuration_fingerprint_snapshot(old)['trading']
    expected = [{'date': '2021-06-21', 'physical_contract': 'LVF:202107'}]
    assert 'tw_stock_futures_day_trade_quarantine_contract_days' not in _configuration_fingerprint_snapshot(old)['trading']
    assert 'tw_stock_futures_day_trade_quarantine_dates' not in _configuration_fingerprint_snapshot(new)['trading']
    assert _configuration_fingerprint_snapshot(new)['trading']['tw_stock_futures_day_trade_quarantine_contract_days'] == expected
    assert b['quarantined_contract_days'] == expected
    assert b['contract_day_quarantine_version'] == 1
    assert b['sample_calendar'] == 'all_verified_panel_sessions_including_no_entry_fills'
    report = _mode_artifact_contract_for_config(new)
    assert report['mode_details']['quarantined_contract_days'] == expected
    assert 'quarantined_decision_dates' not in report['mode_details']
    assert report["mode_details"]["execution_contract_version"] == 2
    assert "daily_close_flat_before_2020-01-01" in report["terminal_policy"]
    assert "daily_open_close_before_2020-01-01" in report["benchmark_contract"]


def test_unaccepted_historical_minutes_stay_out_of_cold_release():
    from pathlib import Path
    catalog = json.loads(Path("configs/data_sync/packed_datasets.json").read_text())
    entry = next(e for e in catalog["datasets"] if e["dataset"] == "tw-futures")
    assert "taifex_stock_futures_minute_history_v2" in entry["excluded_subtrees"]


@pytest.mark.parametrize("damage", ["missing", "corrupt", "missing_price_contract"])
def test_cached_assembly_preserves_snapshot_and_rejects_bad_shards(tmp_path, damage):
    from types import SimpleNamespace
    from test_tw_stock_futures_day_trade import _candidate

    days = [date(2019, 12, 31), date(2020, 3, 23)]
    source = pl.DataFrame([
        {**_candidate(day=day, product="CDF", prior_volume=100, current_volume=10,
                      open_price=100., close_price=108.),
         "physical_contract": "CDF:202004", "contract": "202004",
         "shioaji_roots": "CDF", "high": 110., "low": 100.}
        for day in days
    ])
    daily_path = tmp_path / "daily.parquet"
    atomic_write_parquet(daily_path, source)
    raw_root = tmp_path / "raw"
    path = raw_root / "CDFR1" / "ticks" / f"trading_date={days[1]}" / "data.parquet"
    atomic_write_parquet(path, ticks(days[1]))
    receipt_path = raw_root / "CDFR1" / "receipts" / f"trading_date={days[1]}.json"
    receipt = {"source": "shioaji_continuous_futures_historical_ticks_v1", "schema_version": 1,
               "contract": "CDFR1", "trading_date": str(days[1]), "status": "complete",
               "rows": 6, "sha256": sha256_file(path)}
    atomic_write_json(receipt_path, receipt)
    args = SimpleNamespace(daily_proxy_before="2020-01-01", check_only=False,
                           shioaji_ticks_root=raw_root, output_dir=tmp_path / "output",
                           work_dir=tmp_path / "cache", daily_data_path=daily_path,
                           workers=2, assemble_cached=False)
    digest = sha256_file(daily_path)
    assert build_continuous_history(args, source, days, digest) == 0
    assert json.loads((args.output_dir / "manifest.json").read_text())[
        "price_rule_contract_version"
    ] == TW_DERIVATIVE_PRICE_CONTRACT_VERSION
    hashes = {p.name: sha256_file(p) for p in args.output_dir.iterdir()
              if p.name != "build_progress.json"}
    # Assembly freezes an already verified snapshot; normal builds refresh raw evidence.
    atomic_write_json(receipt_path, {**receipt, "status": "source_empty"})
    args.assemble_cached = True
    assert build_continuous_history(args, source, days, digest) == 0
    assert {name: sha256_file(args.output_dir / name) for name in hashes} == hashes
    args.assemble_cached = False
    assert build_continuous_history(args, source, days, digest) == 2
    assert pl.read_parquet(args.output_dir / "gaps.parquet")["status"].to_list() == ["source_empty_unresolved"]
    manifest_hash = sha256_file(args.output_dir / "manifest.json")
    shard = next(args.work_dir.rglob(f"{days[1]}.parquet"))
    if damage == "missing":
        shard.unlink()
    elif damage == "corrupt":
        shard.write_bytes(shard.read_bytes() + b"corruption")
    else:
        shard_receipt = next(args.work_dir.rglob(f"{days[1]}.json"))
        payload = json.loads(shard_receipt.read_text())
        payload.pop("price_rule_contract_version")
        atomic_write_json(shard_receipt, payload)
    args.assemble_cached = True
    with pytest.raises(ValueError, match="missing/corrupt dated shard"):
        build_continuous_history(args, source, days, digest)
    assert sha256_file(args.output_dir / "manifest.json") == manifest_hash


@pytest.mark.skipif(not torch.cuda.is_available(), reason="CUDA compiler parity")
@pytest.mark.parametrize("fields", [TAPE_FIELDS, HYBRID_TAPE_FIELDS])
@pytest.mark.filterwarnings("ignore:The .grad attribute of a Tensor that is not a leaf Tensor:UserWarning")
def test_compiled_daily_kernel_preserves_mixed_ledger_gradients_and_failure(fields):
    # Vary the symbol width too: expanding folds must retain the same contract.
    for symbols in (3, 7):
        x = tape(rows=5, symbols=symbols).cuda()
        if fields == HYBRID_TAPE_FIELDS:
            x = torch.nn.functional.pad(x, (0, fields - TAPE_FIELDS))
            x[0, ..., TAPE_FIELDS:] = x.new_tensor([1, 100, 110, 100])
        x[3, ..., 3 + 6 * 5:TAPE_FIELDS] = 0  # Cannot close the third active day.
        weights = torch.full((5, symbols), .25 / symbols, device="cuda")
        weights[:, 1::2] *= -1
        advance = torch.tensor([True, True, False, True, True], device="cuda")
        eager_weights = weights.clone().requires_grad_()
        compiled_weights = weights.clone().requires_grad_()
        eager = execute(eager_weights, x, state_advance_mask=advance, use_compile=False)
        compiled = execute(compiled_weights, x, state_advance_mask=advance, use_compile=True)
        for field in ("strategy_returns", "turnovers", "weights_history", "equity_scale_history",
                      "final_equity_scale", "contract_quantities_history",
                      "residual_contract_quantities_history", "default_history", "final_alive"):
            torch.testing.assert_close(getattr(compiled, field), getattr(eager, field), rtol=2e-5, atol=2e-7)
        eager.strategy_returns.sum().backward()
        compiled.strategy_returns.sum().backward()
        torch.testing.assert_close(compiled_weights.grad, eager_weights.grad, rtol=2e-5, atol=2e-7)
        assert torch.isfinite(compiled_weights.grad).all()
        assert not compiled.contract_quantities_history[2].any()
        assert not compiled.final_alive and compiled.default_history[3]

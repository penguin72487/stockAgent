"""The real-data diagnostic must never mutate its captured originals."""
from datetime import date
import json
from pathlib import Path

import polars as pl
import pyarrow.parquet as pq
import pytest

from scripts import benchmark_factorized_daily_owner as benchmark
from stockagent.data.factorized_panel import file_sha256


@pytest.fixture
def fixture(tmp_path, monkeypatch):
    from stockagent.data_sync import cache_capacity
    controls = tmp_path / "capacity-controls"
    controls.mkdir()
    monkeypatch.setattr(cache_capacity, "CONTROL_ROOT", controls)
    monkeypatch.setattr(cache_capacity, "_admit_backing_volume", lambda *_: {})
    root = tmp_path / "diagnostic"
    source = root / "public_complete"
    (source / "stocks").mkdir(parents=True)
    (source / "features").mkdir()
    days = [date(2026, 10, 16), date(2026, 10, 19)]
    pl.DataFrame({"date": days, "open": [-0., 9.], "lifecycle_episode_id": [0, 0]}).write_parquet(source / "stocks/2330_features.parquet")
    (source / "stocks/symbols.csv").write_text("code\n2330\n")
    pl.DataFrame({"date": days, "symbol": ["2330"] * 2, "value": [-0., 10.]}).write_parquet(source / "features/tw_public_stock_daily.parquet")
    (source / "features/tw_public_stock_daily.summary.json").write_text(json.dumps({"requested_end_date": "2026-10-19"}))
    paths = list(source.rglob("*"))
    for path in paths:
        if path.is_file():
            path.chmod(0o444)
    return root, {str(p): file_sha256(p) for p in paths if p.is_file()}


def test_prior_fixture_and_latest_restore_keep_original_bits_and_membership(fixture):
    root, original = fixture
    benchmark.working_public(root, cutoff=date(2026, 10, 16))
    stock = root / "public_work/stocks/2330_features.parquet"
    assert pq.read_table(stock).num_rows == 1
    assert stock.stat().st_mode & 0o222 == 0
    assert (root / "public_work/stocks/symbols.csv").read_text() == "code\n2330\n"
    summary = json.loads((root / "public_work/features/tw_public_stock_daily.summary.json").read_bytes())
    assert summary["requested_end_date"] == "2026-10-16"
    assert (summary["rows"], summary["stock_rows"], summary["market_rows"]) == (1, 1, 0)
    metadata = pq.read_schema(stock).metadata
    assert metadata[b"stockagent.official_checked_through"] == b"2026-10-16"
    assert metadata[b"stockagent.first_date"] == metadata[b"stockagent.last_date"] == b"2026-10-16"
    assert summary["diagnostic_fixture"]["retrospective_subset_not_historical_pit"] is True
    assert summary["output_receipt"]["sha256"] == file_sha256(root / "public_work/features/tw_public_stock_daily.parquet")
    benchmark.working_public(root)
    assert pq.read_table(stock).num_rows == 2
    for path, digest in original.items():
        assert file_sha256(Path(path)) == digest
        corresponding = root / "public_work" / Path(path).relative_to(root / "public_complete")
        assert file_sha256(corresponding) == digest


def test_working_fixture_refuses_mutable_capture_without_sealing_source(fixture):
    root, _ = fixture
    path = root / "public_complete/stocks/2330_features.parquet"
    path.chmod(0o644)
    with pytest.raises(ValueError, match="sealed"):
        benchmark.working_public(root)
    assert path.stat().st_mode & 0o222


def test_cutoff_metadata_uses_actual_last_trade_and_recounts_market_rows(fixture):
    root, _ = fixture
    source = root / "public_complete"
    retired = source / "stocks/retired_features.parquet"
    pl.DataFrame({"date": [date(2026, 10, 15)], "open": [12.]}).write_parquet(retired)
    retired.chmod(0o444)
    newly_listed = source / "stocks/new_features.parquet"
    pl.DataFrame({"date": [date(2026, 10, 19)], "open": [13.]}).write_parquet(newly_listed)
    newly_listed.chmod(0o444)
    features = source / "features/tw_public_stock_daily.parquet"
    features.chmod(0o644)
    original = pl.read_parquet(features)
    pl.concat([original, original.with_columns(pl.lit("__MARKET__").alias("symbol"))]).write_parquet(features)
    features.chmod(0o444)
    benchmark.working_public(root, cutoff=date(2026, 10, 16))
    work = root / "public_work"
    assert pq.read_schema(work / "stocks" / retired.name).metadata[b"stockagent.last_date"] == b"2026-10-15"
    metadata = pq.read_schema(work / "stocks" / newly_listed.name).metadata
    assert b"stockagent.first_date" not in metadata and b"stockagent.last_date" not in metadata
    summary = json.loads((work / "features/tw_public_stock_daily.summary.json").read_bytes())
    assert (summary["rows"], summary["stock_rows"], summary["market_rows"]) == (2, 1, 1)


def test_independent_base_probe_never_uses_daily_cache(fixture, monkeypatch):
    from dataclasses import dataclass
    import numpy as np
    import requests
    import torch
    import urllib.request
    from scripts import benchmark_dashboard_daily_incremental as probe
    from stockagent.data.panel import PanelData
    root, _ = fixture
    parent = root / "parent/factorized_manifest.json"
    parent.parent.mkdir()
    parent.write_text(json.dumps({"symbols": ["2330"], "base_feature_names": ["open_raw"]}))
    parent.chmod(0o444)
    paths = [p for p in root.rglob("*") if p.is_file()]
    snapshot = {"contract": "captured_factorized_real_daily_sources_v1", "original_start": "2026-10-16",
        "latest_completed": "2026-10-19", "proofs": [
            {"path": str(p), "bytes": p.stat().st_size, "sha256": file_sha256(p)} for p in paths]}
    (root / "progress.json").write_text(json.dumps({"stages": {"capture_real_sources": {"result": snapshot}}}))
    @dataclass
    class Data:
        parquet_root: str = ""
        tw_public_feature_path: str = ""
        day_trade_physical_public_feature_path: str = ""
        panel_cache_root: str = ""
        panel_start_date: str = ""
    @dataclass
    class Config:
        data: Data
    monkeypatch.setattr(benchmark.owner, "load_config", lambda *_: Config(Data()))
    monkeypatch.setattr(benchmark.owner, "_build_panel_kwargs", lambda c: {"panel_cache_root": c.data.panel_cache_root})
    called = []
    def build(stock_root, *, panel_cache_root):
        called.append((stock_root, panel_cache_root))
        return PanelData(dates=np.array(["2026-10-16", "2026-10-19"], dtype="datetime64[ns]"),
            symbols=["2330"], feature_names=["open_raw"], features=np.zeros((2, 1, 1), dtype=np.float32),
            returns_1d=np.zeros((2, 1)), tradable_mask=np.ones((2, 1), dtype=bool),
            alive_mask=np.ones((2, 1), dtype=bool), benchmark_returns=np.zeros(2),
            close_prices=np.ones((2, 1)))
    monkeypatch.setattr(benchmark.owner, "build_panel", build)
    # The production diagnostic intentionally forbids these process-wide;
    # restore them with the test fixture even after its direct assignments.
    monkeypatch.setattr(requests.sessions.Session, "request", requests.sessions.Session.request)
    monkeypatch.setattr(urllib.request, "urlopen", urllib.request.urlopen)
    monkeypatch.setattr(torch.cuda, "_lazy_init", torch.cuda._lazy_init)
    monkeypatch.setattr(torch.cuda, "is_initialized", lambda: False)
    output = root / "independent"
    output.mkdir()
    result = probe.real_factorized_base(output, root)
    assert called == [(str(root / "public_complete/stocks"), str(output / "independent_base_cache"))]
    assert result["private_cache_initially_absent"] and result["paired_daily_receipt_still_required"]
    assert result["complete_base_oracle"]["symbols"] == 1
    (output / "independent_base_cache").mkdir()
    with pytest.raises(ValueError, match="empty private cache"):
        probe.real_factorized_base(output, root)


@pytest.fixture
def pair_receipts(fixture, monkeypatch):
    from scripts import benchmark_dashboard_daily_incremental as probe
    root, _ = fixture
    monkeypatch.setattr(probe, "ROOT", root)
    captured = root / "progress.json"
    source = root / "public_complete/stocks/2330_features.parquet"
    snapshot = {"proofs": [{"path": str(source), "bytes": source.stat().st_size,
                             "sha256": file_sha256(source)}]}
    captured.write_text(json.dumps({"stages": {"capture_real_sources": {"result": snapshot}}}))
    expected = {"sha256": "base-digest", "fields": {"mask": "all-array-bits"}}
    projection = {"every_individual_common_value_bit_equal": True, "all_cell_sha256": "all-cells"}
    daily = {"status": "accepted_real_source_daily_owner_not_pit_live_or_inference_readiness",
        "code_sha256": {str(Path(probe.__file__)): file_sha256(Path(probe.__file__))},
        "stages": {"reuse_captured_real_sources": {"result": snapshot},
            "new_session_complete_owner": {"result": {"complete_base_oracles": [expected]}},
            "fresh_full_latest_owner": {"result": {"complete_base_oracles": [expected]}},
            "complete_independent_full_output_oracle": {"result": projection}}}
    independent = {"status": "accepted_captured_source_stage_oracles_not_daily_or_live_readiness",
        "real_factorized_base": {"private_cache_initially_absent": True, "code_sha256": daily["code_sha256"],
            "complete_base_oracle": expected,
            "capture_receipt": {"path": str(captured), "sha256": file_sha256(captured)}}}
    monkeypatch.setattr(benchmark, "compare_complete", lambda *_: projection)
    return probe, root, daily, independent


def _pair(pair_receipts):
    probe, root, daily, independent = pair_receipts
    first, second = root / "daily.json", root / "independent.json"
    first.write_text(json.dumps(daily))
    second.write_text(json.dumps(independent))
    output = root / "artifacts/benchmarks/paired"
    return probe.verify_real_factorized_pair(output, first, second), output


def test_pair_acceptance_requires_full_fields_and_keeps_non_live_boundary(pair_receipts):
    result, output = _pair(pair_receipts)
    assert result["status"] == "accepted_real_source_daily_and_independent_base_not_pit_live_or_inference_readiness"
    assert result["retained_source_members_verified"] == 1
    assert result["production_writes"] is False
    assert json.loads((output / "receipt.json").read_text()) == result


@pytest.mark.parametrize("failure", ["base_fields", "capture", "projection", "writable", "not_accepted", "code"])
def test_pair_acceptance_fails_closed_before_success_receipt(pair_receipts, failure, monkeypatch):
    probe, root, daily, independent = pair_receipts
    if failure == "base_fields":
        independent["real_factorized_base"]["complete_base_oracle"] = {
            "sha256": "base-digest", "fields": {"mask": "different-bits"}}
    elif failure == "capture":
        daily["stages"]["reuse_captured_real_sources"]["result"] = {"proofs": []}
    elif failure == "projection":
        monkeypatch.setattr(benchmark, "compare_complete", lambda *_: {"all_cell_sha256": "changed"})
    elif failure == "writable":
        (root / "public_complete/stocks/2330_features.parquet").chmod(0o644)
    elif failure == "not_accepted":
        daily["status"] = "still_running"
    else:
        independent["real_factorized_base"]["code_sha256"] = {str(Path(probe.__file__)): "changed"}
    with pytest.raises(ValueError):
        _pair(pair_receipts)
    assert not (root / "artifacts/benchmarks/paired/receipt.json").exists()


def test_pair_wait_observes_failure_without_restart(tmp_path):
    from scripts import benchmark_dashboard_daily_incremental as probe
    run = tmp_path / "run.json"
    run.write_text(json.dumps({"state": "failed", "exit_code": 1}))
    with pytest.raises(ValueError, match="did not succeed"):
        probe.wait_for_daily_run(run)
    run.write_text(json.dumps({"state": "succeeded", "exit_code": 0}))
    probe.wait_for_daily_run(run)


def test_bounded_complete_oracle_matches_original_every_bit_and_hash(tmp_path):
    import numpy as np
    from scripts.recover_factorized_daily_acceptance import compare_complete_bounded
    from stockagent.data.factorized_panel import write_array_block
    root = tmp_path / "projection"
    root.mkdir()
    values = np.arange(9*3*4, dtype=np.float32).reshape(9, 3, 4)
    values[2, 1, 1] = -0.0
    values[4, 2, 0] = np.finfo(np.float32).tiny
    common = np.arange(18, dtype=np.float32).reshape(9, 2)
    np.save(root / "common.npy", common)
    blocks = [{**write_array_block(root / f"{start}.zst", values[start:start+3],
                                  omit_zero_columns=True), "start": start} for start in range(0, 9, 3)]
    manifest = {"dates": list(map(str, range(9))), "symbols": ["a", "b", "c"],
        "base_feature_names": ["base"], "individual_channels": list("wxyz"),
        "common_channels": ["c1", "c2"], "model_channel_policy": "value_only",
        "blocks": blocks, "common": {"path": "common.npy", "sha256": file_sha256(root / "common.npy")}}
    path = root / "factorized_manifest.json"
    path.write_text(json.dumps(manifest))
    result = {"manifest": str(path)}
    assert compare_complete_bounded(result, result) == benchmark.compare_complete(result, result)


def test_recovery_rejects_non_failed_run_before_any_owner_work(tmp_path, monkeypatch):
    from scripts import recover_factorized_daily_acceptance as recovery
    monkeypatch.setattr(recovery, "ROOT", tmp_path)
    run, progress = tmp_path / "run.json", tmp_path / "progress.json"
    run.write_text(json.dumps({"state": "running", "exit_code": None}))
    progress.write_text("{}")
    with pytest.raises(ValueError, match="finished failed"):
        recovery.recover(tmp_path / "artifacts/benchmarks/recover", run, progress, tmp_path / "unused")


@pytest.mark.parametrize("failure", ["wrong_owner", "other_failure", "missing_stage", "changed_code"])
def test_recovery_requires_exact_capacity_failure_and_unchanged_original_abi(tmp_path, monkeypatch, failure):
    from scripts import recover_factorized_daily_acceptance as recovery
    monkeypatch.setattr(recovery, "ROOT", tmp_path)
    original = tmp_path / "artifacts/benchmarks/original"
    original.mkdir(parents=True)
    captured = tmp_path / "artifacts/benchmarks/captured"
    captured.mkdir()
    operations = tmp_path / "operations"
    operations.mkdir()
    run, progress = operations / "run.json", original / "progress.json"
    argv = ["run_fintech_python", "scripts/benchmark_factorized_daily_owner.py", "--output",
            str(original.relative_to(tmp_path)), "--capture-root", str(captured.relative_to(tmp_path))]
    if failure == "wrong_owner":
        argv[1] = "scripts/other_owner.py"
    run.write_text(json.dumps({"state": "failed", "exit_code": 1, "argv": argv}))
    (operations / "run.log").write_text("CapacityDeferred: completed-forward capacity deferred: " + (
        "unknown_capacity_failure" if failure == "other_failure" else "filesystem_headroom_exhausted"))
    names = ("reuse_captured_real_sources", "retrospective_prior_fixture", "prior_full_owner",
             "restore_latest_real_fixture", "new_session_complete_owner", "fresh_full_latest_owner",
             "complete_independent_full_output_oracle")
    stages = {key: {"result": {}} for key in names}
    stages["complete_independent_full_output_oracle"]["result"] = {"every_individual_common_value_bit_equal": True}
    if failure == "missing_stage":
        del stages["prior_full_owner"]
    progress.write_text(json.dumps({"contract": "complete_factorized_daily_owner_benchmark_v1",
        "stages": stages, "code_sha256": {str(Path(recovery.__file__)): "changed"}}))
    with pytest.raises(ValueError):
        recovery.recover(tmp_path / "artifacts/benchmarks/recovery", run, progress, tmp_path / "unused")
    assert not (tmp_path / "artifacts/benchmarks/recovery").exists()

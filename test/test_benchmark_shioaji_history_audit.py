from datetime import UTC, date, datetime
import json
from pathlib import Path
import shutil

import polars as pl
import pytest

from downloader.artifact_io import atomic_write_json, sha256_file
from scripts import benchmark_shioaji_history_audit as benchmark


def test_private_benchmark_rejects_existing_and_outside_targets(tmp_path):
    with pytest.raises(ValueError, match="under artifacts/benchmarks"):
        benchmark.bounded_new_path(tmp_path / "new.json")
    with pytest.raises(FileExistsError):
        benchmark.bounded_new_path(benchmark.ROOT / "artifacts/benchmarks")


def test_copied_pair_keeps_source_identities_and_all_bytes(tmp_path):
    source, fixture = tmp_path / "source", tmp_path / "fixture"
    source.mkdir()
    receipt, data = source / "receipt.json", source / "data.parquet"
    receipt.write_bytes(b'{"status":"source_empty"}')
    data.write_bytes(b"preserve even unclaimed source bytes")
    before = [benchmark.signature(path) for path in (receipt, data)]
    result = benchmark.copy_group((receipt, data), source, fixture)
    assert [benchmark.signature(path) for path in (receipt, data)] == before
    assert len(result) == 2
    for item in result:
        assert (source / item["path"]).read_bytes() == (fixture / item["path"]).read_bytes()
        assert item["sha256"] == sha256_file(fixture / item["path"])


def test_retry_does_not_retain_a_private_copy_removed_by_the_source(tmp_path, monkeypatch):
    source, fixture = tmp_path / "source", tmp_path / "fixture"
    source.mkdir()
    receipt, data = source / "receipt.json", source / "data.parquet"
    receipt.write_bytes(b'{"status":"source_empty"}')
    data.write_bytes(b"previous source generation")
    original_copy = shutil.copyfile

    def replaced_during_copy(src, dst):
        result = original_copy(src, dst)
        if src == data:
            data.unlink()  # Simulate an independent source writer, not the copier.
        return result

    monkeypatch.setattr(benchmark.shutil, "copyfile", replaced_during_copy)
    copied = benchmark.copy_group((receipt, data), source, fixture)
    assert [item["path"] for item in copied] == ["receipt.json"]
    assert not (fixture / "data.parquet").exists()


def test_target_inventory_does_not_scan_retained_obsolete_ranges(tmp_path, monkeypatch):
    contract = benchmark.current.HistoryContract(
        collection="indices", priority=3, security_type="IND", asset_class="index",
        code="001", root="001", name="index", exchange="TSE",
        begin_date=date(2026, 10, 12), end_date=date(2026, 10, 13),
    )
    retained = tmp_path / "contracts/index/001/kbars/obsolete/receipt.json"
    retained.parent.mkdir(parents=True)
    retained.write_text("must not be inspected")
    groups = list(benchmark.kbar_groups(tmp_path, [contract]))
    assert len(groups) == 1 and retained not in groups[0]
    queried_roots = []
    monkeypatch.setattr(benchmark.current, "observed_tick_dates",
                        lambda archive, row, **kwargs:
                        queried_roots.append(archive) or [date(2026, 10, 12)])
    activity = benchmark.repair.FuturesActivity()
    ticks = list(benchmark.tick_groups(tmp_path, tmp_path / "copied", [contract], activity))
    assert queried_roots == [tmp_path / "copied"]
    assert len(ticks) == 1 and "trading_date=2026-10-12" in str(ticks[0][0])


def test_fixture_fence_rejects_same_size_in_place_change(tmp_path):
    archive = tmp_path / "archive"
    archive.mkdir()
    data = archive / "data.parquet"
    data.write_bytes(b"old")
    (tmp_path / "reference.py").write_bytes(b"comparator")
    (tmp_path / "activity.parquet").write_bytes(b"official")
    item = {"path": "data.parquet", "fixture_signature": benchmark.signature(data)}
    (tmp_path / "files.jsonl").write_text(json.dumps(item) + "\n")
    manifest = {
        "catalog_sha256": sha256_file(tmp_path / "files.jsonl"),
        "baseline_sha256": sha256_file(tmp_path / "reference.py"),
        "activity_sha256": sha256_file(tmp_path / "activity.parquet"),
    }
    assert benchmark.fixture_fence(tmp_path, manifest)
    data.write_bytes(b"new")
    with pytest.raises(AssertionError, match="fixture bytes/signature changed"):
        benchmark.fixture_fence(tmp_path, manifest)


@pytest.mark.parametrize("variant", ["baseline", "candidate"])
def test_worker_audits_and_persists_exact_full_outputs_without_login(tmp_path, variant):
    collector = benchmark.current
    archive = tmp_path / "archive"
    day = date(2026, 10, 13)
    contract = collector.HistoryContract(
        collection="exact_futures", priority=2, security_type="FUT",
        asset_class="futures", code="TXFJ6", root="TXF", name="TX",
        exchange="TAIFEX", begin_date=day, end_date=day,
        delivery_date=date(2026, 10, 21), delivery_month="202610",
    )
    collector._write_inventory(archive, [contract], completed_session=day)
    _, path = collector._kbar_paths(archive, contract, day, day)
    atomic_write_json(path, {
        "schema_version": collector.RECEIPT_SCHEMA_VERSION, "source": collector.SOURCE,
        "method": "kbars", "contract": contract.code, "status": "source_empty",
        "start": str(day), "end": str(day), "observed_trading_dates": [],
        "rows": 0, "size": 0,
    })
    pl.DataFrame({
        "date": [day], "product": ["TX"], "contract": ["202610"],
        "resolved_last_trade_date": [contract.delivery_date], "shioaji_roots": ["TXF"], "volume": [1],
    }).write_parquet(tmp_path / "activity.parquet")
    shutil.copyfile(benchmark.COLLECTOR, tmp_path / "reference.py")
    atomic_write_json(tmp_path / "manifest.json", {
        "freeze_at_utc": datetime(2026, 10, 14, tzinfo=UTC).isoformat(),
    })
    result = benchmark.worker(tmp_path, variant)
    assert result["provider_requests"] == 0 and result["contracts"] == 1
    assert result["pending_queries"] == 2  # One real empty KBar retry and one absent tick receipt.
    assert len(set(result["summary_sha256"])) == 1
    assert result["final_summary"] == json.loads((archive / "summary.json").read_bytes())
    assert result["peak_rss_bytes"] > 0 and result["cpu_seconds"] > 0
    assert [item["query"] for item in result["summary_timings"]] == [1, 250, 500]

import csv

import polars as pl
import pytest

from scripts import benchmark_yahoo_forex_precheck as probe


def cohort(root, *, source="yahoo", through="2026-06-11"):
    codes = ["EURUSD", "GBPUSD"]
    with (root / "repair_report.csv").open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["code", "precheck_status"])
        writer.writeheader()
        writer.writerows({"code": code, "precheck_status": "schema_mismatch"} for code in codes)
    for code in codes:
        frame = pl.DataFrame({
            "date": [through], "open": [1.1], "max": [1.2],
            "min": [1.0], "close": [1.15], "adjclose": [1.15],
            "Trading_Volume": [0.0],
        })
        probe.yahoo._write_feature_parquet_atomic(
            frame, root / f"{code}_features.parquet", asset_class="forex", source=source,
            requested_start_date="2000-01-01", requested_end_date=through,
        )
    return {path.name: probe.sha256_file(path) for path in root.iterdir()}


def test_probe_compares_same_files_without_network_or_source_mutation(tmp_path, monkeypatch):
    before = cohort(tmp_path)
    fetch = lambda *_args, **_kwargs: pytest.fail("provider requests are forbidden")
    monkeypatch.setattr(probe.yahoo, "_fetch_with_hard_timeout", fetch)
    columns = probe.yahoo._repair_required_columns
    result = probe.run_probe(tmp_path, end_date="2026-06-11")
    assert result["state"] == "accepted_readonly_planning_candidate"
    assert result["cohort_symbols"] == 2
    assert [t["variant"] for t in result["trials"]] == [
        "legacy_required_volume", "asset_aware", "asset_aware", "legacy_required_volume",
    ]
    assert [t["full_schema_rebuilds"] for t in result["trials"]] == [2, 0, 0, 2]
    assert [t["pending_repairs"] for t in result["trials"]] == [2, 0, 0, 2]
    assert result["source_files_written"] == result["source_reports_written"] == result["network_calls"] == 0
    assert result["full_job_measured"] is result["promoted"] is False
    assert result["source_report_hash_unchanged"] and all(result["source_hashes_unchanged"].values())
    assert {path.name: probe.sha256_file(path) for path in tmp_path.iterdir()} == before
    assert probe.yahoo._fetch_with_hard_timeout is fetch
    assert probe.yahoo._repair_required_columns is columns


def test_probe_keeps_legitimate_incremental_repairs(tmp_path):
    cohort(tmp_path, through="2026-06-10")
    result = probe.run_probe(tmp_path, end_date="2026-06-11")
    assert result["state"] == "accepted_readonly_planning_candidate"
    assert [t["incremental_repairs"] for t in result["trials"]] == [0, 2, 2, 0]
    assert [t["pending_repairs"] for t in result["trials"]] == [2, 2, 2, 2]


def test_probe_rejects_other_provider_instead_of_relabelling_source(tmp_path):
    cohort(tmp_path, source="cboe")
    result = probe.run_probe(tmp_path, end_date="2026-06-11")
    assert result["state"] == "not_accepted" and result["trials"] == []
    assert result["failure"]["type"] == "ValueError"
    assert all(result["source_hashes_unchanged"].values())


@pytest.mark.parametrize("code", ["../secret", "EURUSD", "NOTFX"])
def test_probe_rejects_duplicate_or_invalid_codes(tmp_path, code):
    cohort(tmp_path)
    with (tmp_path / "repair_report.csv").open("a") as handle:
        handle.write(f"{code},schema_mismatch\n")
    with pytest.raises(ValueError, match="unique six-letter"):
        probe.run_probe(tmp_path, end_date="2026-06-11")


def test_probe_rejects_large_report_before_source_access(tmp_path):
    (tmp_path / "repair_report.csv").write_bytes(b"x" * (probe.MAX_REPORT_BYTES + 1))
    with pytest.raises(ValueError, match="report exceeds"):
        probe.run_probe(tmp_path, end_date="2026-06-11")


def test_probe_rejects_source_symlinks(tmp_path):
    cohort(tmp_path)
    source = tmp_path / "EURUSD_features.parquet"
    saved = tmp_path / "saved.parquet"
    source.rename(saved)
    source.symlink_to(saved)
    with pytest.raises(ValueError, match="regular, direct"):
        probe.run_probe(tmp_path, end_date="2026-06-11")


def test_probe_detects_disappearing_source_after_validation(tmp_path, monkeypatch):
    cohort(tmp_path)
    original = probe.yahoo._resolve_repair_plan

    def remove_file(*args, **kwargs):
        result = original(*args, **kwargs)
        (tmp_path / "EURUSD_features.parquet").unlink(missing_ok=True)
        return result

    monkeypatch.setattr(probe.yahoo, "_resolve_repair_plan", remove_file)
    result = probe.run_probe(tmp_path, end_date="2026-06-11")
    assert result["state"] == "not_accepted"
    assert not result["source_hashes_unchanged"]["EURUSD"]

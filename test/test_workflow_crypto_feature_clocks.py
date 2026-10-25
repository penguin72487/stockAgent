"""Native CEX clocks retain worker grain, evidence identity and visible gaps."""
import csv
import gzip
import hashlib
import io
import json

import pytest

from scripts import profile_workflow_speeds as profile


def fixture_pair(tmp_path, provider="binance"):
    report, summary = tmp_path / "workers.csv", tmp_path / "summary.json"
    row = dict(code="BTC,\nPERP", status="updated", rows="314159", output_path="unused.parquet",
        changed="true", stage_status_json='{"mark_price":"updated"}', coverage_json="{}", errors_json="{}",
        stage_elapsed_seconds_json='{"read_existing":0.0,"mark_price":1.125,"derive":-0.0}',
        total_elapsed_seconds="2.0", **{provider + "_symbol": "BTC-PERP"})
    empty = {**row, "code": "EMPTY", "stage_elapsed_seconds_json": "{}", "stage_status_json": "{}",
             "total_elapsed_seconds": "0.0"}
    payload = dict(asset_class="crypto_" + provider + "_" + ("usdm_perp" if provider == "binance" else "perp"),
        interval="1m", historical_features_enabled=True, historical_feature_report_is_current_run=True,
        historical_feature_report=str(report), started_at_utc="2026-10-23T00:00:00Z",
        ended_at_utc="2026-10-23T00:00:04Z", work_elapsed_seconds=3.5,
        lock_wait_seconds=.25, elapsed_seconds=4.0, historical_feature_stage_latency={
            "read_existing": dict(samples=1, p50_seconds=0., p95_seconds=0., aggregate_worker_seconds=0.),
            "mark_price": dict(samples=1, p50_seconds=1.125, p95_seconds=1.125, aggregate_worker_seconds=1.125),
            "derive": dict(samples=1, p50_seconds=-0., p95_seconds=-0., aggregate_worker_seconds=-0.)})
    return report, summary, [row, empty], payload


def csv_bytes(rows):
    stream = io.StringIO(newline="")
    writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
    writer.writeheader()
    writer.writerows(rows)
    return stream.getvalue().encode("utf-8-sig")


def collect_pair(pair, provider="binance", ledger=None):
    report, summary, rows, payload = pair
    raw, summary_raw = csv_bytes(rows), json.dumps(payload).encode()
    ledger = ledger or profile.Ledger()
    result = profile.collect_crypto_feature_samples(ledger, provider, report, raw,
        hashlib.sha256(raw).hexdigest(), summary_raw, hashlib.sha256(summary_raw).hexdigest())
    return ledger, result


@pytest.mark.parametrize("provider", ["binance", "okx"])
def test_native_clocks_keep_bits_locators_gaps_and_no_endpoint_or_throughput(tmp_path, provider):
    pair = fixture_pair(tmp_path, provider)
    ledger, result = collect_pair(pair, provider)
    assert [(r["stage"], r["seconds"]) for r in ledger.samples] == [
        ("read_existing", 0.), ("mark_price", 1.125), ("derive", -0.),
        ("worker_return_total", 2.), ("run_work_before_summary_publication", 3.5)]
    assert ledger.samples[2]["seconds"].hex() == (-0.).hex()
    assert result["workers_with_stage_clocks"] == 1
    assert result["worker_stage_and_total_samples"] == 4
    assert result["empty_stage_clock_gaps"][0]["record"] == 2
    assert result["recorded_lock_wait_seconds"] == .25
    for index, sample in enumerate(ledger.samples[:4]):
        assert sample["context"]["report_record"] == 1
        assert sample["context"]["instrument_code"] == "BTC,\nPERP"
        assert sample["context"]["existing_output_rows_not_work_denominator"] == 314159
        assert sample["locator"].startswith("csv/records/1/")
        assert sample["source_sha256"] == hashlib.sha256(csv_bytes(pair[2])).hexdigest()
    assert all(r["work"] == {} and r["units_per_second"] is None for r in ledger.samples)
    ledger.inventory = [{"kind": "source_endpoint", "id": provider + ":mark_price"}]
    assert profile.bind_source_stages(ledger) == 0
    assert all("source_endpoint_id" not in r for r in ledger.samples)
    assert all(r["context"]["source_binding_eligible"] is False for r in ledger.samples)


@pytest.mark.parametrize("mutation", ["negative", "boolean", "nan", "unknown_stage", "missing_start",
    "beyond_total", "bad_status", "negative_rows", "duplicate_code", "bad_json", "no_workers"])
def test_invalid_native_worker_is_rejected_before_any_sample(tmp_path, mutation):
    pair = fixture_pair(tmp_path)
    rows = pair[2]
    if mutation == "no_workers":
        raw = csv_bytes(rows[:1]).splitlines(keepends=True)[0]
    else:
        if mutation == "negative": rows[0]["stage_elapsed_seconds_json"] = '{"read_existing":-1}'
        if mutation == "boolean": rows[0]["stage_elapsed_seconds_json"] = '{"read_existing":true}'
        if mutation == "nan": rows[0]["total_elapsed_seconds"] = "NaN"
        if mutation == "unknown_stage": rows[0]["stage_elapsed_seconds_json"] = '{"read_existing":0,"made_up":1}'
        if mutation == "missing_start": rows[0]["stage_elapsed_seconds_json"] = '{"mark_price":1}'
        if mutation == "beyond_total": rows[0]["stage_elapsed_seconds_json"] = '{"read_existing":2.000002}'
        if mutation == "bad_status": rows[0]["stage_status_json"] = '{"mark_price":3}'
        if mutation == "negative_rows": rows[0]["rows"] = "-1"
        if mutation == "duplicate_code": rows[1]["code"] = rows[0]["code"]
        if mutation == "bad_json": rows[0]["stage_elapsed_seconds_json"] = "["
        raw = csv_bytes(rows)
    ledger = profile.Ledger()
    with pytest.raises(ValueError):
        profile.collect_crypto_feature_samples(ledger, "binance", pair[0], raw, "a" * 64,
            json.dumps(pair[3]).encode(), "b" * 64)
    assert ledger.samples == []


@pytest.mark.parametrize("field,value", [
    ("asset_class", "crypto_okx_perp"), ("interval", "15m"),
    ("historical_feature_report", "other.csv"), ("historical_feature_report_is_current_run", False),
    ("historical_features_enabled", False), ("historical_feature_stage_latency", {}),
    ("work_elapsed_seconds", float("inf"))])
def test_wrong_or_unpaired_summary_retains_no_samples(tmp_path, field, value):
    pair = fixture_pair(tmp_path)
    pair[3][field] = value
    ledger = profile.Ledger()
    with pytest.raises(ValueError):
        collect_pair(pair, ledger=ledger)
    assert ledger.samples == []


def test_duplicate_summary_export_keeps_all_bytes_and_emits_once(tmp_path):
    report, summary, rows, payload = fixture_pair(tmp_path)
    report.write_bytes(csv_bytes(rows))
    summary.write_text(json.dumps(payload))
    copy = tmp_path / "summary-export.json"
    copy.write_bytes(summary.read_bytes())
    output = tmp_path / "observation"
    output.mkdir()
    store = profile.Evidence(output, max_file_bytes=1 << 20, max_total_bytes=4 << 20)
    ledger = profile.Ledger()
    assert profile.collect_crypto_feature_operations(ledger, store, [
        ("binance", report, summary), ("binance", report, copy)]) == 5
    assert len(ledger.samples) == 5 and len(ledger.inventory) == 1
    assert len(store.sources) == 3 and not store.issues
    for path in (report, summary, copy):
        identity = store.sources[str(path)]
        assert gzip.decompress((output / identity["snapshot"]).read_bytes()) == path.read_bytes()


def test_invalid_pair_keeps_both_sources_and_visible_issue(tmp_path):
    report, summary, rows, payload = fixture_pair(tmp_path)
    report.write_bytes(csv_bytes(rows))
    payload["historical_feature_stage_latency"]["mark_price"]["samples"] = 7
    summary.write_text(json.dumps(payload))
    output = tmp_path / "observation"
    output.mkdir()
    store = profile.Evidence(output, max_file_bytes=1 << 20, max_total_bytes=4 << 20)
    ledger = profile.Ledger()
    assert profile.collect_crypto_feature_operations(ledger, store, [("binance", report, summary)]) == 0
    assert not ledger.samples and not ledger.inventory
    assert len(store.sources) == 2
    assert store.issues[-1]["reason"] == "invalid_crypto_feature_clock_pair"


@pytest.mark.parametrize("explicit", [False, True])
def test_cli_keeps_explicit_scope_and_discovers_available_default_pairs(tmp_path, monkeypatch, explicit):
    monkeypatch.setattr(profile, "ROOT", tmp_path)
    for provider in ("binance", "okx"):
        root = tmp_path / ("data_" + provider) / "1m"
        root.mkdir(parents=True)
        (root / "historical_feature_report.csv").touch()
    captured = []
    monkeypatch.setattr(profile, "collect", lambda args: captured.append(args) or {})
    argv = ["--output-dir", str(tmp_path / "observation")]
    if explicit:
        argv += ["--crypto-feature-report", "binance", "selected.csv", "selected.json"]
    assert profile.main(argv) == 0
    selected = captured[0].crypto_feature_report
    assert [row[0] for row in selected] == (["binance"] if explicit else ["binance", "okx"])
    if explicit:
        assert selected[0][1:] == ["selected.csv", "selected.json"]
    else:
        assert all(row[1].endswith("historical_feature_report.csv") for row in selected)


def test_cli_unknown_provider_does_not_start_collection(tmp_path, monkeypatch):
    def forbidden(args):
        raise AssertionError("invalid source scope must fail before collection")
    monkeypatch.setattr(profile, "collect", forbidden)
    with pytest.raises(SystemExit) as exc:
        profile.main(["--output-dir", str(tmp_path), "--crypto-feature-report", "unknown", "a.csv", "b.json"])
    assert exc.value.code == 2


def test_missing_summary_is_a_gap_and_does_not_emit_partial_worker_clocks(tmp_path):
    report, summary, rows, payload = fixture_pair(tmp_path)
    report.write_bytes(csv_bytes(rows))
    output = tmp_path / "observation"
    output.mkdir()
    store = profile.Evidence(output, max_file_bytes=1 << 20, max_total_bytes=4 << 20)
    ledger = profile.Ledger()
    assert profile.collect_crypto_feature_operations(ledger, store, [("binance", report, summary)]) == 0
    assert not ledger.samples and not ledger.inventory
    assert store.sources[str(report)]["state"] == "captured"
    assert store.issues[-1]["reason"] == "crypto_feature_clock_pair_unavailable"

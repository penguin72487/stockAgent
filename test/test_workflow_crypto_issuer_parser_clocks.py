"""Retained issuer spans require original bytes, complete rows and offline scope."""
import copy
import gzip
import hashlib
import io
import json
from pathlib import Path
from types import SimpleNamespace

import polars as pl
import pytest

from scripts import profile_workflow_speeds as profile


def sha(body):
    return hashlib.sha256(body).hexdigest()


def bundle(tmp_path, monkeypatch):
    root, archive = tmp_path / "logical", tmp_path / "archive"
    archive.mkdir()
    monkeypatch.setattr(profile, "ROOT", root)
    inputs, code = [], []

    def retain(path, body, target=inputs):
        path = str(path)
        stored = archive / (str(len(inputs) + len(code)) + ".bin")
        stored.write_bytes(body)
        row = dict(path=path, archive=str(stored), sha256=sha(body))
        if target is inputs:
            row.update(stable=True, bytes=len(body))
        target.append(row)
        return row

    specs, issuers, outputs, clocks = [], [], [], []
    normalized = {key: [] for key in profile.CRYPTO_ISSUER_STREAMS}
    adapters = ["ishares_gross_proceeds_xls"] * 4 + ["ishares_holdings_csv"] * 2 + ["bitwise_next_data"]
    for i, adapter in enumerate(adapters):
        spec = dict(id="issuer_" + str(i), provider="iShares" if i < 6 else "Bitwise",
                    ticker="F" + str(i), asset="BTC", adapter=adapter, url="https://example.test/" + str(i))
        specs.append(spec)
        raw = ("original issuer " + str(i)).encode()
        raw_path = root / "data_crypto_etf/raw" / (spec["id"] + (".xls" if i < 4 else ".csv.gz"))
        retain(raw_path, raw if i < 4 else gzip.compress(raw, mtime=0))
        item = dict(spec=copy.deepcopy(spec), raw_path=str(raw_path), raw_sha256=sha(raw),
                    observed_at_utc="2026-10-23T22:30:00+00:00")
        issuers.append(item)
        receipt = {key: item[key] for key in ("raw_path", "raw_sha256", "observed_at_utc")}
        retain(root / "data_crypto_etf/receipts/issuers" / (spec["id"] + ".json"), json.dumps(receipt).encode())
        provenance = dict(source_id=spec["id"], provider=spec["provider"], ticker=spec["ticker"], asset="BTC",
            raw_sha256=item["raw_sha256"], observed_at_utc=item["observed_at_utc"],
            available_at_utc=item["observed_at_utc"], point_in_time_state="historical_first_observed_at_retrieval")
        parsed = dict(daily_metrics=[dict(provenance, event_date="2026-10-22", metric="nav_per_share",
                                         value=-0.0 if i == 0 else None if i == 1 else float(i), unit="USD/share")],
                      holdings=[], reserves=[])
        if i == 5:
            parsed["holdings"] = [dict(provenance, as_of_date="2026-10-22", holding_ticker="BTC", quantity=1.25)]
        if i == 6:
            parsed["reserves"] = [dict(provenance, snapshot_at_utc="2026-10-22T00:00:00Z", reserve_asset_units=2.5)]
        rows = sum(len(value) for value in parsed.values())
        for key in normalized:
            normalized[key].extend(parsed[key])
        parsed["result"] = dict(source_id=spec["id"], provider=spec["provider"], status="complete", rows=rows,
                                requests=1, output_paths=[], message=None)
        outputs.append(parsed)
        clocks.append(dict(source_endpoint_id="crypto-etf:issuer:" + spec["id"], raw_sha256=sha(raw),
            wall_ns=1_000_001 + i, process_cpu_ns=500_003 + i, input_bytes=len(raw), output_rows=rows))
    retain(root / "configs/crypto_etf_sources.json", json.dumps(dict(schema_version=1, issuer_sources=specs)).encode())
    for stream, (filename, _) in profile.CRYPTO_ISSUER_STREAMS.items():
        sink = io.BytesIO()
        pl.DataFrame(normalized[stream]).write_parquet(sink)
        retain(root / "data_crypto_etf/normalized" / filename, sink.getvalue())
    retain(root / "downloader/download_crypto_etf_history.py", b"immutable producer source", code)
    protocol = dict(inputs=inputs, code=code, issuers=issuers, original_bytes=sum(row["bytes"] for row in inputs),
        native_parser_only=True, no_new_provider_or_GPU_jobs=True, all_seven_issuer_sources_required=True,
        complete_original_normalized_row_oracle_required=True, source_or_receipts_modified=False,
        source_bindings_globally_recomputed=False, full_acquisition_measured=False)
    timing = dict(state="measured_all_seven_retained_native_issuer_parsers", scope=profile.CRYPTO_ISSUER_PARSER_SCOPE,
        profiler_enabled=False, source_modified=False, full_acquisition_measured=False, new_provider_calls=0,
        parser_loaded_from_exact_archived_source=True, native_result_request_counts_are_preserved_metadata_not_new_HTTP=True,
        per_issuer=clocks, native_workflow_wall_ns=sum(x["wall_ns"] for x in clocks) + 1001,
        native_workflow_process_cpu_ns=sum(x["process_cpu_ns"] for x in clocks) + 1003,
        pandas_version="fixture-pandas", polars_version="fixture-polars")
    readback = dict(state="verified_all_seven_native_outputs_against_original_normalized_rows",
        output_sha256=sha(json.dumps(outputs).encode()), original_observation_clocks_and_raw_SHA_preserved=True,
        source_stage_bindings_globally_recomputed=False, acquisition_completeness_verified=False,
        streams=[dict(stream=key, complete_current_raw_rows_verified=len(value),
                      native_ordered_rows_compared_between_both_full_calls=True) for key, value in normalized.items()])
    data = [timing, protocol, outputs, readback]
    paths = [tmp_path / (name + ".json") for name in ("timing", "protocol", "outputs", "readback")]

    def persist(resign_outputs=False):
        if resign_outputs:
            readback["output_sha256"] = sha(json.dumps(outputs).encode())
        for path, value in zip(paths, data):
            path.write_text(json.dumps(value))

    persist()
    return SimpleNamespace(root=root, data=data, paths=paths, inputs=inputs, persist=persist)


def collect(fixture, tmp_path, reports=None):
    output = tmp_path / "observation"
    output.mkdir()
    evidence = profile.Evidence(output, max_file_bytes=64 << 10, max_total_bytes=8 << 20)
    ledger = profile.Ledger()
    count = profile.collect_crypto_issuer_parser_operations(ledger, evidence, reports or [fixture.paths])
    return ledger, evidence, count


def test_all_original_rows_ns_units_and_offline_bindings_are_retained(tmp_path, monkeypatch):
    fixture = bundle(tmp_path, monkeypatch)
    ledger, evidence, count = collect(fixture, tmp_path)
    assert count == len(ledger.samples) == 16 and not evidence.issues
    assert len(ledger.inventory) == 1 and ledger.inventory[0]["native_rows"] == 9
    for index, row in enumerate(ledger.samples[:14]):
        native = fixture.data[0]["per_issuer"][index // 2]
        key = "wall_ns" if index % 2 == 0 else "process_cpu_ns"
        assert row["seconds"].hex() == (native[key] / 1e9).hex()
        assert row["context"]["native_nanoseconds"] == native[key]
        assert row["locator"] == f"per_issuer/{index // 2}/{key}"
        assert row["work"]["units"] == native["output_rows"]
        assert row["context"]["new_provider_calls"] == 0
        assert row["context"]["native_result_requests"] == 1
        assert row["context"]["whole_loaded_source_identity_verified"] is False
        assert row["context"]["acquisition_completeness_verified"] is False
    ledger.inventory.extend(dict(kind="source_endpoint", id="crypto-etf:issuer:issuer_" + str(i)) for i in range(7))
    ledger.inventory.append(dict(kind="source_endpoint", id="crypto-etf:sec-edgar"))
    assert profile.bind_source_stages(ledger) == 7
    assert all(row["source_endpoint_id"].startswith("crypto-etf:issuer:") for row in ledger.samples[:14])
    assert all("source_endpoint_id" not in row for row in ledger.samples[14:])
    assert "timed_stages" not in ledger.inventory[-1]


@pytest.mark.parametrize("field", ["wall_ns", "process_cpu_ns"])
@pytest.mark.parametrize("value", [-1, True, 1.0, float("nan"), float("inf"), 10**400])
def test_invalid_final_issuer_clock_never_leaves_partial_samples(tmp_path, monkeypatch, field, value):
    fixture = bundle(tmp_path, monkeypatch)
    fixture.data[0]["per_issuer"][-1][field] = value
    fixture.persist()
    ledger, evidence, count = collect(fixture, tmp_path)
    assert count == 0 and ledger.samples == [] and ledger.inventory == []
    assert evidence.issues[-1]["reason"] in {"invalid_crypto_issuer_parser_bundle", "crypto_issuer_parser_bundle_unavailable"}


@pytest.mark.parametrize("index,field,value", [
    (0, "scope", "HTTP_acquisition"), (0, "profiler_enabled", True), (0, "source_modified", True),
    (0, "new_provider_calls", True), (0, "new_provider_calls", 1),
    (0, "full_acquisition_measured", True), (0, "parser_loaded_from_exact_archived_source", False),
    (0, "native_result_request_counts_are_preserved_metadata_not_new_HTTP", False),
    (1, "native_parser_only", False), (1, "no_new_provider_or_GPU_jobs", False),
    (1, "all_seven_issuer_sources_required", 1), (1, "complete_original_normalized_row_oracle_required", False),
    (1, "source_or_receipts_modified", True), (1, "source_bindings_globally_recomputed", True),
    (1, "original_bytes", 0), (3, "output_sha256", "0" * 64),
    (3, "acquisition_completeness_verified", True), (3, "original_observation_clocks_and_raw_SHA_preserved", False),
    (3, "source_stage_bindings_globally_recomputed", True),
])
def test_wrong_scope_or_proof_is_a_retained_gap(tmp_path, monkeypatch, index, field, value):
    fixture = bundle(tmp_path, monkeypatch)
    fixture.data[index][field] = value
    fixture.persist()
    ledger, evidence, count = collect(fixture, tmp_path)
    assert count == 0 and not ledger.samples and not ledger.inventory
    assert all(str(path) in evidence.sources for path in fixture.paths)
    assert evidence.issues[-1]["reason"] == "invalid_crypto_issuer_parser_bundle"


@pytest.mark.parametrize("change", ["missing_issuer", "reorder", "alias", "rows", "bytes", "raw_sha", "native_clock", "normal_value", "signed_zero"])
def test_complete_source_set_denominators_and_normalized_values_are_required(tmp_path, monkeypatch, change):
    fixture = bundle(tmp_path, monkeypatch)
    timing, protocol, outputs, readback = fixture.data
    if change == "missing_issuer":
        protocol["issuers"].pop()
    elif change == "reorder":
        outputs.reverse()
    elif change == "alias":
        timing["per_issuer"][-1]["source_endpoint_id"] += ":alias"
    elif change == "rows":
        timing["per_issuer"][-1]["output_rows"] = True
    elif change == "bytes":
        timing["per_issuer"][-1]["input_bytes"] += 1
    elif change == "raw_sha":
        timing["per_issuer"][-1]["raw_sha256"] = "0" * 64
    elif change == "native_clock":
        outputs[-1]["daily_metrics"][0]["available_at_utc"] = "2026-10-24T00:00:00Z"
    elif change == "normal_value":
        outputs[-1]["daily_metrics"][0]["value"] += .25
    else:
        outputs[0]["daily_metrics"][0]["value"] = 0.0
    fixture.persist(resign_outputs=True)
    ledger, evidence, count = collect(fixture, tmp_path)
    assert count == 0 and not ledger.samples and not ledger.inventory
    assert evidence.issues[-1]["reason"] == "invalid_crypto_issuer_parser_bundle"


@pytest.mark.parametrize("change", ["bad_sha", "bad_parquet", "duplicate_normalized", "oversized_gzip"])
def test_corrupt_or_oversized_archived_originals_fail_closed(tmp_path, monkeypatch, change):
    fixture = bundle(tmp_path, monkeypatch)
    protocol = fixture.data[1]
    if change == "oversized_gzip":
        row = fixture.inputs[12]
        assert row["path"].endswith(".gz")
        payload = gzip.compress(b"x" * (128 << 10), mtime=0)
    else:
        row = next(row for row in fixture.inputs if row["path"].endswith("issuer_daily_fund_metrics.parquet"))
        payload = b"invalid parquet"
        if change == "duplicate_normalized":
            frame = pl.read_parquet(row["archive"])
            sink = io.BytesIO()
            pl.concat([frame, frame.head(1)]).write_parquet(sink)
            payload = sink.getvalue()
    Path(row["archive"]).write_bytes(payload)
    if change != "bad_sha":
        row.update(sha256=sha(payload), bytes=len(payload))
        protocol["original_bytes"] = sum(row["bytes"] for row in fixture.inputs)
    fixture.persist()
    ledger, evidence, count = collect(fixture, tmp_path)
    assert count == 0 and not ledger.samples and not ledger.inventory
    assert evidence.issues[-1]["reason"] == "invalid_crypto_issuer_parser_bundle"


@pytest.mark.parametrize("value", [None, []])
def test_malformed_native_receipt_remains_a_gap(tmp_path, monkeypatch, value):
    fixture = bundle(tmp_path, monkeypatch)
    row = next(row for row in fixture.inputs if row["path"].endswith("/issuers/issuer_6.json"))
    payload = json.dumps(value).encode()
    Path(row["archive"]).write_bytes(payload)
    row.update(sha256=sha(payload), bytes=len(payload))
    fixture.data[1]["original_bytes"] = sum(row["bytes"] for row in fixture.inputs)
    fixture.persist()
    ledger, evidence, count = collect(fixture, tmp_path)
    assert count == 0 and not ledger.samples and not ledger.inventory
    assert evidence.issues[-1]["reason"] == "invalid_crypto_issuer_parser_bundle"


def test_export_duplicates_retain_all_bytes_and_emit_one_bundle(tmp_path, monkeypatch):
    fixture = bundle(tmp_path, monkeypatch)
    exported = tmp_path / "exported-timing.json"
    exported.write_bytes(fixture.paths[0].read_bytes())
    ledger, evidence, count = collect(fixture, tmp_path, [fixture.paths, [exported, *fixture.paths[1:]]])
    assert count == 16 and len(ledger.inventory) == 1 and not evidence.issues
    for path in [*fixture.paths, exported]:
        row = evidence.sources[str(path)]
        assert gzip.decompress((evidence.output / row["snapshot"]).read_bytes()) == path.read_bytes()


def test_missing_output_preserves_available_evidence_without_samples(tmp_path, monkeypatch):
    fixture = bundle(tmp_path, monkeypatch)
    fixture.paths[2].unlink()
    ledger, evidence, count = collect(fixture, tmp_path)
    assert count == 0 and not ledger.samples and not ledger.inventory
    assert all(str(path) in evidence.sources for path in fixture.paths)
    assert evidence.issues[-1]["reason"] == "crypto_issuer_parser_bundle_unavailable"


@pytest.mark.parametrize("explicit", [False, True])
def test_cli_retains_explicit_bundle_scope_without_default_discovery(tmp_path, monkeypatch, explicit):
    seen = []
    monkeypatch.setattr(profile, "collect", lambda args: seen.append(args.crypto_issuer_parser_report) or {})
    argv = ["--output-dir", str(tmp_path / "output")]
    if explicit:
        argv += ["--crypto-issuer-parser-report", "timing.json", "protocol.json", "outputs.json", "readback.json"]
    assert profile.main(argv) == 0
    assert seen == ([[["timing.json", "protocol.json", "outputs.json", "readback.json"]]] if explicit else [[]])

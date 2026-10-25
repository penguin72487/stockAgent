"""SEC replay spans keep mapping definitions, financial rows and cache proofs apart."""
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


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def bundle(tmp_path, monkeypatch):
    root, archive, native = tmp_path / "logical", tmp_path / "archive", tmp_path / "native"
    archive.mkdir()
    monkeypatch.setattr(profile, "ROOT", root)
    original_root = root / "data_crypto_etf"
    inputs, code = [], []

    def retain(path, body, destination=inputs):
        saved = archive / (str(len(inputs) + len(code)) + ".bin")
        saved.write_bytes(body)
        row = dict(path=str(path), archive=str(saved), sha256=sha(body))
        if destination is inputs:
            row.update(bytes=len(body), stable=True)
        destination.append(row)
        return row

    def output(path, body):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(body)

    def parquet(frame):
        sink = io.BytesIO()
        frame.write_parquet(sink)
        return sink.getvalue()

    def response(path, body):
        stored = gzip.compress(body, mtime=0)
        retain(path, stored)
        output(native / path.relative_to(original_root), stored)

    config = dict(sec=dict(funds=[dict(ticker="F" + str(i)) for i in range(40)]))
    retain(root / "configs/crypto_etf_sources.json", json.dumps(config).encode())
    retain(root / "downloader/download_crypto_etf_history.py", b"retained native SEC source", code)
    mapping_plain = json.dumps({str(i): dict(cik_str=i + 1, ticker="F" + str(i)) for i in range(40)}).encode()
    mapping_raw = original_root / "raw/sec/company_tickers" / (sha(mapping_plain) + ".json.gz")
    response(mapping_raw, mapping_plain)
    mapping = dict(raw_path=str(mapping_raw), raw_sha256=sha(mapping_plain), resolved_entities=40,
                   missing_tickers=[], observed_at_utc="2026-10-23T00:00:00Z")
    retain(original_root / "receipts/sec/company_tickers.json", json.dumps(mapping).encode())
    output(native / "receipts/sec/company_tickers.json", json.dumps(dict(mapping, raw_path=str(native / mapping_raw.relative_to(original_root)))).encode())
    items, clocks, checks, results, original_results = [], [], [], [], []
    mapping_clock = dict(stage="resolve_mapping", wall_ns=1_000_001, process_cpu_ns=500_003,
                         entities=40, native_result_requests=1)
    rows_total, document_count = 0, 0
    for i in range(40):
        cik, identifier = f"{i + 1:010d}", "sec_cik_" + f"{i + 1:010d}"
        observed, completed = "2026-10-23T00:00:00Z", "2026-10-23T00:01:00Z"
        submissions = json.dumps(dict(cik=cik, filings=dict(recent={}))).encode()
        submission_path = original_root / "raw/sec/submissions" / cik / (sha(submissions) + ".json.gz")
        response(submission_path, submissions)
        facts, facts_path = None, None
        if i not in (1, 2):
            facts = json.dumps(dict(cik=cik, facts={})).encode()
            facts_path = original_root / "raw/sec/companyfacts" / cik / (sha(facts) + ".json.gz")
            response(facts_path, facts)
        paths = [str(native / submission_path.relative_to(original_root)), str(native / "normalized/sec" / cik / "filings.parquet")]
        if facts_path:
            paths.append(str(native / facts_path.relative_to(original_root)))
        paths += [str(native / "normalized/sec" / cik / "companyfacts.parquet"),
                  str(native / "normalized/sec" / cik / "manifest.json")]
        for filename, values in [("filings.parquet", [-0.0 if i == 0 else float(i)]),
                                 ("companyfacts.parquet", [None] if facts is not None else [])]:
            frame = pl.DataFrame(dict(retrieved_at_utc=[observed] * len(values), value=values),
                                 schema={"retrieved_at_utc": pl.String, "value": pl.Float64})
            original_path = original_root / "normalized/sec" / cik / filename
            body = parquet(frame)
            retain(original_path, body)
            output(native / original_path.relative_to(original_root), body)
        documents = []
        if i % 8 == 0:
            plain = ("retained primary document " + cik).encode()
            path = original_root / "raw/sec/primary_documents" / cik / (sha(plain) + ".htm.gz")
            response(path, plain)
            documents.append(dict(raw_path=str(path), raw_sha256=sha(plain), raw_bytes=len(plain)))
            document_count += 1
        manifest = dict(cik=cik, filing_rows=1, companyfact_rows=int(facts is not None),
            primary_documents=documents, submissions_raw_sha256=sha(submissions),
            companyfacts_raw_sha256=sha(facts) if facts is not None else None,
            completed_at_utc=completed, documents_downloaded_this_run=0, tickers=["F" + str(i)])
        original_path = original_root / "normalized/sec" / cik / "manifest.json"
        retain(original_path, json.dumps(manifest).encode())
        native_manifest = copy.deepcopy(manifest)
        for d in native_manifest["primary_documents"]:
            d["raw_path"] = str(native / Path(d["raw_path"]).relative_to(original_root))
        output(native / original_path.relative_to(original_root), json.dumps(native_manifest).encode())
        item = dict(manifest=manifest, manifest_path=str(original_path), retrieved_at_utc=observed,
                    submissions_path=str(submission_path), companyfacts_path=str(facts_path) if facts_path else None)
        items.append(item)
        rows = 1 + int(facts is not None)
        clock = dict(stage="native_cached_entity", source_endpoint_id="crypto-etf:sec-edgar", source_entity_id=identifier,
            wall_ns=2_000_001 + i, process_cpu_ns=700_003 + i, rows=rows,
            cached_documents=len(documents), native_result_requests=1 + int(facts is not None),
            original_manifest_path=str(original_path))
        clocks.append(clock)
        results.append(dict(source_id=identifier, provider="SEC EDGAR", status="complete", rows=rows,
                            requests=clock["native_result_requests"], output_paths=paths))
        original_results.append(dict(source_id=identifier, provider="SEC EDGAR", status="complete", rows=rows))
        checks.append(dict(cik=cik, complete_rows=rows, all_ordered_values_schema_null_masks_and_float_bits_equal=True,
            complete_native_manifest_and_original_document_bytes_equal_after_verified_output_root_mapping=True))
        rows_total += rows
    original_results.append(dict(source_id="sec_company_tickers", provider="SEC EDGAR", status="complete", rows=40))
    retain(original_root / "download_summary.json", json.dumps(dict(results=original_results)).encode())
    protocol = dict(state="prepared_complete_retained_SEC_mapping_and_40_entity_replays", inputs=inputs, code=code,
        config=config, mapping_receipt=mapping, entities=items, original_bytes=sum(r["bytes"] for r in inputs),
        native_cached_document_state_preserved=True, complete_original_rows_manifests_and_documents_required=True,
        numerical_values_null_masks_and_floating_bits_required=True, status_and_clocks_replayed_from_retained_originals=True,
        mapping_rows_are_entity_definitions_not_filing_or_companyfact_observations=True,
        full_external_acquisition_measured=False, production_worker_concurrency_reproduced=False,
        source_bindings_globally_recomputed=False, new_provider_calls=0, new_gpu_jobs=0,
        entity_data_rows=rows_total, mapping_entity_definitions=40, original_summary_SEC_rows=rows_total + 40)
    timing = dict(state="measured_all_retained_native_SEC_mapping_and_entities", scope=profile.RETAINED_SEC_NATIVE_SCOPE,
        profiler_enabled=False, new_provider_calls=0, native_result_requests_are_replayed_metadata=True,
        retrieval_and_manifest_clocks_supplied_from_retained_originals=True,
        missing_companyfacts_branch_is_receipt_supplied_not_observed_HTTP_status=True,
        native_source_loaded_from_exact_archive=True, full_external_acquisition_verified=False,
        global_source_bindings_recomputed=False, per_stage=[mapping_clock, *clocks], replay_client_calls=81,
        native_workflow_wall_ns=sum(c["wall_ns"] for c in [mapping_clock, *clocks]) + 17,
        native_workflow_process_cpu_ns=sum(c["process_cpu_ns"] for c in [mapping_clock, *clocks]) + 19)
    readback = dict(state="verified_both_complete_SEC_replays_against_all_original_rows_manifests_and_documents",
        native_timing_sha256="pending", full_external_acquisition_verified=False, production_concurrency_reproduced=False,
        global_source_bindings_recomputed=False, whole_loaded_production_identity_verified=False,
        entities=checks, complete_rows=rows_total, cached_documents=document_count)
    data = [timing, protocol, results, readback]
    paths = [tmp_path / (name + ".json") for name in ("timing", "protocol", "results", "readback")]

    def persist():
        paths[0].write_text(json.dumps(timing))
        readback["native_timing_sha256"] = sha(paths[0].read_bytes())
        for path, value in zip(paths[1:], data[1:]):
            path.write_text(json.dumps(value))

    persist()
    return SimpleNamespace(root=root, data=data, paths=paths, inputs=inputs, persist=persist)


def collect(fixture, tmp_path, reports=None):
    output = tmp_path / "observation"
    output.mkdir()
    evidence = profile.Evidence(output, max_file_bytes=256 << 10, max_total_bytes=16 << 20)
    ledger = profile.Ledger()
    count = profile.collect_retained_sec_operations(ledger, evidence, reports or [fixture.paths])
    return ledger, evidence, count


def test_all_40_entity_ns_native_rows_mapping_and_cached_documents_are_separate(tmp_path, monkeypatch):
    fixture = bundle(tmp_path, monkeypatch)
    ledger, evidence, count = collect(fixture, tmp_path)
    assert count == len(ledger.samples) == 84 and not evidence.issues
    assert ledger.inventory[0]["native_rows"] == 78 and ledger.inventory[0]["cached_documents"] == 5
    for i, sample in enumerate(ledger.samples[:82]):
        clock = fixture.data[0]["per_stage"][i // 2]
        field = "wall_ns" if i % 2 == 0 else "process_cpu_ns"
        assert sample["seconds"].hex() == (clock[field] / 1e9).hex()
        assert sample["context"]["native_nanoseconds"] == clock[field]
        assert sample["context"]["new_provider_calls"] == 0
    assert ledger.samples[0]["work"] == dict(unit="mapped_entity_definitions", units=40)
    assert ledger.samples[2]["work"] == dict(unit="financial_data_rows", units=2)
    ledger.inventory.extend(dict(kind="source_endpoint", id=name) for name in ["crypto-etf:sec-edgar", "crypto-etf:issuer:other"])
    assert profile.bind_source_stages(ledger) == 1
    assert all(s["source_endpoint_id"] == "crypto-etf:sec-edgar" for s in ledger.samples[:82])
    assert all("source_endpoint_id" not in s for s in ledger.samples[82:])
    assert "timed_stages" not in ledger.inventory[-1]


@pytest.mark.parametrize("field,value", [("wall_ns", -1), ("wall_ns", True), ("wall_ns", 10**400),
    ("process_cpu_ns", 1.0), ("rows", True), ("cached_documents", 0), ("native_result_requests", True),
    ("source_endpoint_id", "crypto-etf:other"), ("source_entity_id", "sec_cik_alias")])
def test_bad_final_entity_clock_never_leaves_partial_samples(tmp_path, monkeypatch, field, value):
    fixture = bundle(tmp_path, monkeypatch)
    fixture.data[0]["per_stage"][-1][field] = value
    # The final entity has no documents; use a contradictory count.
    if field == "cached_documents":
        fixture.data[0]["per_stage"][-1][field] = 1
    fixture.persist()
    ledger, evidence, count = collect(fixture, tmp_path)
    assert count == 0 and not ledger.samples and not ledger.inventory
    assert evidence.issues[-1]["reason"] == "invalid_retained_sec_bundle"


@pytest.mark.parametrize("change", ["mapping_count", "mix_row_grains", "profile", "provider_calls", "concurrency_claim",
    "signed_zero", "financial_value", "cached_document", "native_manifest", "native_mapping", "raw_response", "archive_sha"])
def test_complete_original_and_output_checks_do_not_trust_readback_flags(tmp_path, monkeypatch, change):
    fixture = bundle(tmp_path, monkeypatch)
    timing, protocol, results, readback = fixture.data
    native_root = Path(results[0]["output_paths"][1]).parents[3]
    if change == "mapping_count":
        timing["per_stage"][0]["entities"] = 39
    elif change == "mix_row_grains":
        protocol["entity_data_rows"] += 40;readback["complete_rows"] += 40
    elif change == "profile":
        timing["profiler_enabled"] = True
    elif change == "provider_calls":
        timing["new_provider_calls"] = True
    elif change == "concurrency_claim":
        readback["production_concurrency_reproduced"] = True
    elif change in {"signed_zero", "financial_value"}:
        path = Path(results[0]["output_paths"][1])
        frame = pl.read_parquet(path)
        frame.with_columns(pl.lit(0.0 if change == "signed_zero" else 7.5).alias("value")).write_parquet(path)
    elif change == "cached_document":
        manifest = protocol["entities"][0]["manifest"]
        path = native_root / Path(manifest["primary_documents"][0]["raw_path"]).relative_to(fixture.root / "data_crypto_etf")
        path.write_bytes(gzip.compress(b"changed document", mtime=0))
    elif change == "native_manifest":
        path = Path(results[0]["output_paths"][-1])
        item = json.loads(path.read_bytes());item["completed_at_utc"] = "invented"
        path.write_text(json.dumps(item))
    elif change == "native_mapping":
        path = native_root / "receipts/sec/company_tickers.json"
        item = json.loads(path.read_bytes());item["resolved_entities"] = 39
        path.write_text(json.dumps(item))
    elif change == "raw_response":
        Path(results[0]["output_paths"][0]).write_bytes(gzip.compress(b"{}", mtime=0))
    else:
        Path(fixture.inputs[-1]["archive"]).write_bytes(b"corrupt original")
    fixture.persist()
    ledger, evidence, count = collect(fixture, tmp_path)
    assert count == 0 and not ledger.samples and not ledger.inventory
    assert evidence.issues[-1]["reason"] == "invalid_retained_sec_bundle"


@pytest.mark.parametrize("index,field,value", [
    (0, "scope", "concurrent_HTTP"), (0, "replay_client_calls", True),
    (1, "new_gpu_jobs", True), (1, "native_cached_document_state_preserved", False),
    (1, "full_external_acquisition_measured", True), (1, "mapping_entity_definitions", 39),
    (1, "original_summary_SEC_rows", 78), (3, "cached_documents", 0),
])
def test_wrong_scope_or_mixed_grain_proof_never_changes_the_ledger(tmp_path, monkeypatch, index, field, value):
    fixture = bundle(tmp_path, monkeypatch)
    fixture.data[index][field] = value
    fixture.persist()
    ledger, evidence, count = collect(fixture, tmp_path)
    assert count == 0 and not ledger.samples and not ledger.inventory
    assert evidence.issues[-1]["reason"] == "invalid_retained_sec_bundle"


@pytest.mark.parametrize("malformed", [None, []])
def test_malformed_final_native_result_is_a_retained_gap(tmp_path, monkeypatch, malformed):
    fixture = bundle(tmp_path, monkeypatch)
    fixture.data[2][-1] = malformed
    fixture.persist()
    ledger, evidence, count = collect(fixture, tmp_path)
    assert count == 0 and not ledger.samples and not ledger.inventory
    assert evidence.issues[-1]["reason"] == "invalid_retained_sec_bundle"


def test_retained_document_expansion_is_bounded_before_decoding(tmp_path, monkeypatch):
    fixture = bundle(tmp_path, monkeypatch)
    row = next(row for row in fixture.inputs if "/primary_documents/" in row["path"])
    raw = gzip.compress(b"x" * (300 << 10), mtime=0)
    Path(row["archive"]).write_bytes(raw)
    row.update(bytes=len(raw), sha256=sha(raw))
    fixture.data[1]["original_bytes"] = sum(row["bytes"] for row in fixture.inputs)
    fixture.persist()
    ledger, evidence, count = collect(fixture, tmp_path)
    assert count == 0 and not ledger.samples and not ledger.inventory
    assert evidence.issues[-1]["reason"] == "invalid_retained_sec_bundle"


def test_duplicate_export_keeps_all_report_bytes_and_emits_once(tmp_path, monkeypatch):
    fixture = bundle(tmp_path, monkeypatch)
    exported = tmp_path / "exported_timing.json"
    exported.write_bytes(fixture.paths[0].read_bytes())
    ledger, evidence, count = collect(fixture, tmp_path, [fixture.paths, [exported, *fixture.paths[1:]]])
    assert count == 84 and len(ledger.inventory) == 1 and not evidence.issues
    assert all(str(path) in evidence.sources for path in [*fixture.paths, exported])


def test_missing_native_results_keeps_the_other_report_bytes(tmp_path, monkeypatch):
    fixture = bundle(tmp_path, monkeypatch)
    fixture.paths[2].unlink()
    ledger, evidence, count = collect(fixture, tmp_path)
    assert count == 0 and not ledger.samples and not ledger.inventory
    assert all(str(path) in evidence.sources for path in fixture.paths)
    assert evidence.issues[-1]["reason"] == "retained_sec_bundle_unavailable"


@pytest.mark.parametrize("explicit", [False, True])
def test_cli_does_not_implicitly_discover_or_run_SEC_work(tmp_path, monkeypatch, explicit):
    seen = []
    monkeypatch.setattr(profile, "collect", lambda args: seen.append(args.retained_sec_report) or {})
    argv = ["--output-dir", str(tmp_path / "output")]
    if explicit:
        argv += ["--retained-sec-report", "timing", "protocol", "results", "readback"]
    assert profile.main(argv) == 0
    assert seen == ([[["timing", "protocol", "results", "readback"]]] if explicit else [[]])

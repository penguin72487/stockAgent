"""Offline Dune clock ingestion keeps complete original/output evidence."""
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
    inputs, code = [], []

    def retain(path, raw, destination=inputs):
        saved = archive / (str(len(inputs) + len(code)) + ".bin")
        saved.write_bytes(raw)
        row = dict(path=str(path), archive=str(saved), sha256=sha(raw))
        if destination is inputs:
            row.update(stable=True, bytes=len(raw))
        destination.append(row)
        return row

    def output(path, raw):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(raw)

    def parquet(frame):
        sink = io.BytesIO()
        frame.write_parquet(sink)
        return sink.getvalue()

    specs = [dict(id=name, expected_columns=["event_date", "value"], primary_key=["event_date"])
             for name in ("query_a", "query_b", "query_without_payload")]
    retain(root / "configs/dune_crypto_queries.json", json.dumps(dict(queries=specs)).encode())
    retain(root / "downloader/download_dune_crypto_history.py", b"retained native source", code)
    sql_path = root / "configs/query.sql"
    sql = b"select event_date, value from retained_fixture"
    retain(sql_path, sql)
    items, clocks, checks, compressed, rows_total = [], [], [], 0, 0
    for index, spec in enumerate((specs[0], specs[0], specs[1])):
        partition = str(index)
        receipt_path = root / "data_dune_crypto/receipts" / spec["id"] / (partition + ".json")
        original_parquet = root / "data_dune_crypto/normalized" / spec["id"] / (partition + ".parquet")
        relative = original_parquet.relative_to(root / "data_dune_crypto")
        native_parquet = native / relative
        old_partition = "legacy_partition_1_range" if index == 1 else partition
        raw_root = root / "data_dune_crypto/raw" / spec["id"] / old_partition / "observed_execution"
        native_raw_root = native / "raw" / spec["id"] / partition / "observed_execution"
        values = [] if index == 0 else [-0.0, None] if index == 1 else [1.25]
        frame = pl.DataFrame(dict(event_date=["2026-10-" + str(20 + i) for i in range(len(values))], value=values),
                             schema={"event_date": pl.String, "value": pl.Float64})
        raw = gzip.compress(json.dumps(dict(result=dict(rows=frame.to_dicts()))).encode(), mtime=0)
        page = raw_root / "page_000000000000.json.gz"
        retain(page, raw)
        output(native_raw_root / page.name, raw)
        frame = frame.with_columns(pl.lit(spec["id"]).alias("_dune_contract_id"),
                                   pl.lit("observed_execution").alias("_dune_execution_id"))
        original_frame = frame.select("event_date", "value", "_dune_execution_id", "_dune_contract_id")
        retain(original_parquet, parquet(original_frame))
        output(native_parquet, parquet(frame))
        receipt = dict(status="complete", query_id=spec["id"], partition_id=partition, execution_id="observed_execution",
            expected_columns=spec["expected_columns"], primary_key=spec["primary_key"],
            sql_sha256=sha(sql), rows=len(values), pages=1, parquet_path=str(original_parquet),
            raw_root=str(raw_root), available_at_utc="2026-10-22T00:00:00Z",
            completed_at_utc="2026-10-23T00:00:00Z")
        migration = dict(lineage_upgraded_at_utc="2026-10-23T01:00:00Z",
                         migrated_from_partition_id=old_partition) if index == 1 else {}
        receipt.update(migration)
        receipt_raw = json.dumps(receipt).encode()
        retain(receipt_path, receipt_raw)
        native_receipt = {k:v for k,v in receipt.items() if k not in migration}
        native_receipt.update(parquet_path=str(native_parquet), raw_root=str(native_raw_root))
        native_receipt_path = native / receipt_path.relative_to(root / "data_dune_crypto")
        output(native_receipt_path, json.dumps(native_receipt).encode())
        items.append(dict(spec=copy.deepcopy(spec), receipt=receipt, receipt_path=str(receipt_path),
                          sql_path=str(sql_path), pages=[str(page)]))
        clocks.append(dict(source_endpoint_id="dune-query:" + spec["id"], partition_id=partition,
            original_receipt_path=str(receipt_path), wall_ns=1_000_001 + index, process_cpu_ns=500_003 + index,
            rows=len(values), pages=1, parquet_path=str(native_parquet), receipt_path=str(native_receipt_path)))
        checks.append(dict(query_id=spec["id"], partition_id=partition, rows=len(values), original_receipt_sha256=sha(receipt_raw),
            all_ordered_values_schema_null_masks_and_floating_bits_equal=True,
            all_native_receipt_fields_except_verified_output_roots_equal=True,
            historical_metadata_column_order_retained_in_original_and_compared_by_name=True,
            both_full_replays_raw_uncompressed_page_bytes_equal=True, original_migration_metadata_retained=migration))
        compressed += len(raw)
        rows_total += len(values)
    other_page = root / "data_dune_crypto/raw/query_a/other_partition/other_execution/page_000000000000.json.gz"
    retain(other_page, gzip.compress(b'{"result":{"rows":[]}}', mtime=0))
    protocol = dict(state="prepared_all_complete_retained_Dune_partition_replays", inputs=inputs, code=code,
        original_bytes=sum(r["bytes"] for r in inputs), partitions=items, original_raw_pages=4,
        original_rows=rows_total, completed_partition_raw_pages=3, unreferenced_pages=[str(other_page)],
        unreferenced_pages_are_not_completed_partition_evidence=True,
        no_source_modification=True, original_observation_clocks_supplied_from_receipts=True,
        complete_ordered_row_and_raw_page_oracle_required=True, numerical_values_null_masks_and_floating_bits_required=True,
        full_external_acquisition_measured=False, source_bindings_globally_recomputed=False)
    protocol.update(new_provider_calls=0, new_provider_executions=0)
    timing = dict(state="measured_all_complete_retained_Dune_partitions", scope=profile.RETAINED_DUNE_PARTITION_SCOPE,
        profiler_enabled=False, new_provider_calls=0, new_provider_executions=0,
        status_and_retrieval_clock_supplied_from_retained_receipts=True, native_source_loaded_from_exact_archive=True,
        full_external_acquisition_verified=False, global_source_bindings_recomputed=False, partitions=clocks,
        native_workflow_wall_ns=sum(r["wall_ns"] for r in clocks) + 17,
        native_workflow_process_cpu_ns=sum(r["process_cpu_ns"] for r in clocks) + 19)
    readback = dict(state="verified_both_full_retained_Dune_replays_against_every_original_row_and_page",
        native_timing_sha256="pending", source_or_receipts_modified=False, no_new_provider_calls=True,
        full_external_acquisition_verified=False, global_source_bindings_recomputed=False,
        whole_production_loaded_identity_verified=False, partitions=checks, total_rows=rows_total,
        raw_compressed_bytes=compressed)
    data = [timing, protocol, readback]
    paths = [tmp_path / (name + ".json") for name in ("timing", "protocol", "readback")]

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
    evidence = profile.Evidence(output, max_file_bytes=64 << 10, max_total_bytes=4 << 20)
    ledger = profile.Ledger()
    count = profile.collect_retained_dune_operations(ledger, evidence, reports or [fixture.paths])
    return ledger, evidence, count


def test_all_native_ns_original_rows_empty_partitions_and_exact_query_bindings(tmp_path, monkeypatch):
    fixture = bundle(tmp_path, monkeypatch)
    ledger, evidence, count = collect(fixture, tmp_path)
    assert count == len(ledger.samples) == 8 and not evidence.issues
    assert ledger.inventory[0]["native_rows"] == 3 and ledger.inventory[0]["retained_other_execution_pages"] == 1
    for i, sample in enumerate(ledger.samples[:6]):
        clock = fixture.data[0]["partitions"][i // 2]
        field = "wall_ns" if i % 2 == 0 else "process_cpu_ns"
        assert sample["seconds"].hex() == (clock[field] / 1e9).hex()
        assert sample["context"]["native_nanoseconds"] == clock[field]
        assert sample["work"]["units"] == clock["rows"]
        assert sample["context"]["whole_loaded_source_identity_verified"] is False
        assert sample["context"]["acquisition_completeness_verified"] is False
    ledger.inventory.extend(dict(kind="source_endpoint", id="dune-query:" + name)
                            for name in ("query_a", "query_b", "query_without_payload"))
    assert profile.bind_source_stages(ledger) == 2
    assert all(s["source_endpoint_id"].startswith("dune-query:") for s in ledger.samples[:6])
    assert all("source_endpoint_id" not in s for s in ledger.samples[6:])
    assert "timed_stages" not in ledger.inventory[-1]


def test_migrated_raw_partition_keeps_original_bytes_and_canonical_native_root(tmp_path, monkeypatch):
    fixture = bundle(tmp_path, monkeypatch)
    original = fixture.data[1]["partitions"][1]["receipt"]
    native = json.loads(Path(fixture.data[0]["partitions"][1]["receipt_path"]).read_bytes())
    assert Path(original["raw_root"]).parent.name == original["migrated_from_partition_id"]
    assert Path(native["raw_root"]).parent.name == original["partition_id"]
    ledger, evidence, count = collect(fixture, tmp_path)
    assert count == 8 and not evidence.issues
    assert ledger.inventory[0]["native_rows"] == 3


@pytest.mark.parametrize("component", ["query", "partition", "execution"])
def test_identical_raw_page_cannot_authorize_another_native_root(tmp_path, monkeypatch, component):
    fixture = bundle(tmp_path, monkeypatch)
    receipt_path = Path(fixture.data[0]["partitions"][1]["receipt_path"])
    receipt = json.loads(receipt_path.read_bytes())
    previous = Path(receipt["raw_root"])
    parts = list(previous.parts)
    parts[{"query": -3, "partition": -2, "execution": -1}[component]] += "_other"
    different = Path(*parts)
    different.mkdir(parents=True)
    page = "page_000000000000.json.gz"
    (different / page).write_bytes((previous / page).read_bytes())
    receipt["raw_root"] = str(different)
    receipt_path.write_text(json.dumps(receipt))
    ledger, evidence, count = collect(fixture, tmp_path)
    assert count == 0 and not ledger.samples and not ledger.inventory
    assert evidence.issues[-1]["contract_violation"] == "Dune replay native receipt fields differ"


@pytest.mark.parametrize("field", ["wall_ns", "process_cpu_ns", "rows", "pages"])
@pytest.mark.parametrize("value", [-1, True, 1.0, float("nan"), 10**400])
def test_invalid_final_clock_or_denominator_never_emits_partial_rows(tmp_path, monkeypatch, field, value):
    fixture = bundle(tmp_path, monkeypatch)
    fixture.data[0]["partitions"][-1][field] = value
    fixture.persist()
    ledger, evidence, count = collect(fixture, tmp_path)
    assert count == 0 and not ledger.samples and not ledger.inventory
    assert evidence.issues[-1]["reason"] == "invalid_retained_dune_bundle"


@pytest.mark.parametrize("index,field,value", [
    (0, "scope", "HTTP_acquisition"), (0, "profiler_enabled", True), (0, "new_provider_calls", True),
    (0, "new_provider_executions", 1), (0, "native_source_loaded_from_exact_archive", False),
    (0, "status_and_retrieval_clock_supplied_from_retained_receipts", False),
    (0, "full_external_acquisition_verified", True), (1, "original_bytes", 0),
    (1, "complete_ordered_row_and_raw_page_oracle_required", False), (1, "no_source_modification", False),
    (1, "original_rows", 4), (1, "original_raw_pages", 3), (1, "unreferenced_pages", []),
    (2, "whole_production_loaded_identity_verified", True), (2, "raw_compressed_bytes", 0),
])
def test_incompatible_scope_or_complete_denominator_is_retained_as_a_gap(tmp_path, monkeypatch, index, field, value):
    fixture = bundle(tmp_path, monkeypatch)
    fixture.data[index][field] = value
    fixture.persist()
    ledger, evidence, count = collect(fixture, tmp_path)
    assert count == 0 and not ledger.samples and not ledger.inventory
    assert all(str(path) in evidence.sources for path in fixture.paths)
    assert evidence.issues[-1]["reason"] == "invalid_retained_dune_bundle"


@pytest.mark.parametrize("change", ["value", "signed_zero", "native_receipt", "raw_page", "archive_sha",
                                   "missing_partition", "duplicate_page", "clock_alias", "migration"])
def test_readback_booleans_cannot_hide_changed_original_or_native_data(tmp_path, monkeypatch, change):
    fixture = bundle(tmp_path, monkeypatch)
    timing, protocol, readback = fixture.data
    clock = timing["partitions"][1]
    if change in {"value", "signed_zero"}:
        path = Path(clock["parquet_path"])
        frame = pl.read_parquet(path)
        frame.with_columns(pl.lit(0.0 if change == "signed_zero" else 7.5).alias("value")).write_parquet(path)
    elif change == "native_receipt":
        path = Path(clock["receipt_path"])
        receipt = json.loads(path.read_bytes());receipt["completed_at_utc"] = "invented clock"
        path.write_text(json.dumps(receipt))
    elif change == "raw_page":
        receipt = json.loads(Path(clock["receipt_path"]).read_bytes())
        (Path(receipt["raw_root"]) / "page_000000000000.json.gz").write_bytes(gzip.compress(b"{}", mtime=0))
    elif change == "archive_sha":
        Path(fixture.inputs[-1]["archive"]).write_bytes(b"corrupt original")
    elif change == "missing_partition":
        protocol["partitions"].pop();timing["partitions"].pop();readback["partitions"].pop()
        protocol["original_rows"] -= 1;readback["total_rows"] -= 1
    elif change == "duplicate_page":
        protocol["partitions"][-1]["pages"] *= 2
    elif change == "clock_alias":
        timing["partitions"][-1]["source_endpoint_id"] += ":alias"
    else:
        readback["partitions"][1]["original_migration_metadata_retained"] = {}
    fixture.persist()
    ledger, evidence, count = collect(fixture, tmp_path)
    assert count == 0 and not ledger.samples and not ledger.inventory
    assert evidence.issues[-1]["reason"] == "invalid_retained_dune_bundle"


def test_duplicate_export_retains_both_original_reports_but_emits_once(tmp_path, monkeypatch):
    fixture = bundle(tmp_path, monkeypatch)
    exported = tmp_path / "exported_timing.json"
    exported.write_bytes(fixture.paths[0].read_bytes())
    ledger, evidence, count = collect(fixture, tmp_path, [fixture.paths, [exported, *fixture.paths[1:]]])
    assert count == 8 and len(ledger.inventory) == 1 and not evidence.issues
    assert all(str(path) in evidence.sources for path in [*fixture.paths, exported])


def test_missing_bundle_member_keeps_available_evidence(tmp_path, monkeypatch):
    fixture = bundle(tmp_path, monkeypatch)
    fixture.paths[-1].unlink()
    ledger, evidence, count = collect(fixture, tmp_path)
    assert count == 0 and not ledger.samples and not ledger.inventory
    assert all(str(path) in evidence.sources for path in fixture.paths)
    assert evidence.issues[-1]["reason"] == "retained_dune_bundle_unavailable"


@pytest.mark.parametrize("explicit", [False, True])
def test_cli_requires_explicit_retained_partition_bundle(tmp_path, monkeypatch, explicit):
    seen = []
    monkeypatch.setattr(profile, "collect", lambda args: seen.append(args.retained_dune_report) or {})
    argv = ["--output-dir", str(tmp_path / "output")]
    if explicit:
        argv += ["--retained-dune-report", "timing.json", "protocol.json", "readback.json"]
    assert profile.main(argv) == 0
    assert seen == ([[["timing.json", "protocol.json", "readback.json"]]] if explicit else [[]])

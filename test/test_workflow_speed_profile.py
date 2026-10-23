"""Speed evidence must not manufacture coverage, throughput or comparability."""
import gzip
import hashlib
import json
from pathlib import Path
import sqlite3

import pytest

from scripts.profile_workflow_speeds import (
    Evidence, Ledger, bind_source_stages, capture_benchmark, collect_benchmark, collect_finlab, collect_finmind, collect_http, collect_services, collect_training, discover,
    finite_seconds, inspect_benchmarks, reported_metrics, select_benchmark_paths, storage_activity_timings, verify_collection, visible_code, write_csv,
)


def test_evidence_manifest_preserves_bytes_and_atomic_durable_owner(tmp_path, monkeypatch):
    from scripts import profile_workflow_speeds as profile
    from downloader.artifact_io import atomic_write_json

    evidence = Evidence(tmp_path, max_file_bytes=4096, max_total_bytes=8192)
    evidence.sources["/測試"] = {"path": "/測試", "bytes": 2**128, "json_parsed": True}
    evidence.issues.append({"path": "/missing", "reason": "missing"})
    gaps = [{"root": "/測試", "reason": "bounded", "visited": 2}]
    expected = tmp_path / "reference.json"
    atomic_write_json(expected, {"files": list(evidence.sources.values()), "issues": evidence.issues,
                                "traversal_gaps": gaps})
    writes = []
    owner = profile.atomic_write_bytes

    def observe(path, payload, **kwargs):
        writes.append(kwargs)
        return owner(path, payload, **kwargs)

    monkeypatch.setattr(profile, "atomic_write_bytes", observe)
    target = tmp_path / "manifest.json"
    profile.write_evidence_manifest(target, evidence, gaps)
    assert target.read_bytes() == expected.read_bytes()
    assert writes == [{"durable": True}]
    assert evidence.sources["/測試"]["bytes"] == 2**128


def test_proven_empty_tree_still_validates_previously_parsed_json(tmp_path, monkeypatch):
    from scripts import profile_workflow_speeds as profile
    output = tmp_path / "output"
    output.mkdir()
    path = tmp_path / "receipt.json"
    raw = b'{"source_sha256":"abc","items":[1,true,null,{"foo_s":5}]}'
    path.write_bytes(raw)
    original = profile.private_json.loads
    parses = []

    def parse(body):
        parses.append(body)
        return original(body)

    def unexpected_walk(value, **kwargs):
        raise AssertionError("a proven empty shallow metric walk should be elided")

    monkeypatch.setattr(profile.private_json, "loads", parse)
    monkeypatch.setattr(profile, "reported_metrics", unexpected_walk)
    store = Evidence(output, max_file_bytes=4096, max_total_bytes=8192)
    assert capture_benchmark(store, path) is None
    assert parses == [raw]
    source = store.sources[str(path)]
    assert source["classification"] == "parsed_no_reported_timing_fields"
    assert source["json_parsed"] is True
    assert source["sha256"] == hashlib.sha256(raw).hexdigest()
    broken = tmp_path / "broken.json"
    broken.write_bytes(b'{"source_sha256":')
    assert capture_benchmark(store, broken) is None
    assert store.issues == [{"path": str(broken), "reason": "JSONDecodeError"}]


@pytest.mark.parametrize("raw", [
    b'{"wall_\\u0073econds":1}',
    '{"wall_seconds":1}'.encode("utf-16"),
    '{"wall_seconds":1}'.encode("utf-32"),
    b'{"prefix_wall_seconds":1}',
])
def test_negative_tree_proof_cannot_hide_encoded_timing_keys(tmp_path, raw):
    output = tmp_path / "output"
    output.mkdir()
    path = tmp_path / "receipt.json"
    path.write_bytes(raw)
    store = Evidence(output, max_file_bytes=4096, max_total_bytes=4096)
    _, _, metrics = capture_benchmark(store, path)
    assert len(metrics) == 1
    assert metrics[0]["reported_value"] == 1
    assert metrics[0]["reported_unit"] == "seconds"
    assert not store.issues


def test_deep_negative_and_explicit_receipts_keep_original_metric_walk(tmp_path, monkeypatch):
    from scripts import profile_workflow_speeds as profile
    payload = {"source_sha256": "abc"}
    for _ in range(100):
        payload = {"node": payload}
    path = tmp_path / "deep.json"
    path.write_text(json.dumps(payload))
    output = tmp_path / "output"
    output.mkdir()
    original, walks = profile.reported_metrics, []

    def walk(value, **kwargs):
        walks.append(1)
        yield from original(value, **kwargs)

    monkeypatch.setattr(profile, "reported_metrics", walk)
    store = Evidence(output, max_file_bytes=8192, max_total_bytes=16384)
    assert capture_benchmark(store, path) is None
    assert walks
    walks.clear()
    explicit = tmp_path / "explicit.json"
    explicit.write_bytes(b'{"source_sha256":"abc"}')
    assert capture_benchmark(store, explicit, explicit=True) is not None
    assert walks


def test_csv_wire_preserves_ragged_rows_nested_json_and_negative_zero(tmp_path):
    path = tmp_path / "wire.csv"
    write_csv(path, [{"a": -0.0, "c": {"z": 2, "a": 1}},
                     {"a": True, "b": '字,"x"\nline'},
                     {"b": None, "c": [1, False]}])
    expected = ('a,b,c\r\n'
                '-0.0,,"{""a"": 1, ""z"": 2}"\r\n'
                'True,"字,""x""\nline",\r\n'
                ',,"[1, false]"\r\n')
    assert path.read_bytes() == b"\xef\xbb\xbf" + expected.encode()
    empty = tmp_path / "empty.csv"
    write_csv(empty, [])
    assert empty.read_bytes() == b"\xef\xbb\xbf\r\n"


def test_csv_mapping_subclass_retains_legacy_items_semantics(tmp_path):
    class Custom(dict):
        def __iter__(self):
            return iter(("overridden",))

        def items(self):
            return {"overridden": "內容"}.items()

    path = tmp_path / "custom.csv"
    write_csv(path, [Custom({"ignored": 1})])
    assert path.read_bytes() == b"\xef\xbb\xbf" + "overridden\r\n內容\r\n".encode()


def test_freeze_original_bytes_and_reject_changing_read(tmp_path, monkeypatch):
    output = tmp_path / "observation"
    output.mkdir()
    source = tmp_path / "source.json"
    raw = b'{"wall_seconds":1.25}\n'
    source.write_bytes(raw)
    store = Evidence(output, max_file_bytes=1024, max_total_bytes=4096)
    captured, sha = store.read(source)
    assert captured == raw and sha == hashlib.sha256(raw).hexdigest()
    source.write_text("new generation")
    assert store.read(source) == (raw, sha)
    assert gzip.decompress((output / f"evidence/{sha}.gz").read_bytes()) == raw

    other = tmp_path / "changing.json"
    other.write_bytes(raw)
    original = Path.read_bytes

    def changing(path):
        value = original(path)
        if path == other:
            path.write_text("changed during observation")
        return value

    monkeypatch.setattr(Path, "read_bytes", changing)
    assert store.read(other) is None
    assert store.sources[str(other)]["state"] == "changed_during_read"


def test_unavailable_and_oversized_evidence_stay_visible(tmp_path):
    output = tmp_path / "observation"
    output.mkdir()
    store = Evidence(output, max_file_bytes=4, max_total_bytes=8)
    missing = tmp_path / "missing.json"
    assert store.read(missing) is None
    large = tmp_path / "large.json"
    large.write_text("123456")
    assert store.read(large) is None
    assert len(store.issues) == 2
    assert store.sources[str(large)]["state"] == "size_budget_exceeded"


@pytest.mark.parametrize("workers", [1, 2, 4])
def test_prefetch_preserves_complete_order_budget_failures_and_originals(tmp_path, workers):
    paths = []
    bodies = [b'{"wall_seconds":-0.0}', b'{"features":[1,2]}',
              b'{"wall_seconds":', b'{"elapsed_ms":2}', b'x' * 8192,
              '{"wall_seconds":1}'.encode("utf-16")]
    for index in range(70):
        path = tmp_path / f"source{index}.json"
        if index != 34:
            path.write_bytes(bodies[index % len(bodies)])
        paths.append(path)
    paths.extend([paths[0], paths[1]])
    results = []
    for count in (0, workers):
        output = tmp_path / str(count)
        output.mkdir()
        store = Evidence(output, max_file_bytes=4096, max_total_bytes=100)
        ledger = Ledger()
        metrics, catalogue = inspect_benchmarks(store, ledger, paths,
            explicit_receipts=[paths[3]], read_workers=count)
        snapshots = {p.name: p.read_bytes() for p in (output / "evidence").iterdir()}
        results.append((metrics, catalogue, list(store.sources.values()), store.issues,
                        store.total_bytes, ledger.samples, snapshots))
    assert results[0] == results[1]
    assert results[1][2][34]["state"] == "unavailable"
    assert results[1][4] <= 100


@pytest.mark.parametrize("mutation", ["write", "replace", "symlink"])
def test_prefetch_rechecks_generation_before_capture(tmp_path, mutation):
    path = tmp_path / "source.json"
    old, new = b'{"wall_seconds":1}', b'{"wall_seconds":2}'
    original = tmp_path / "original.json"
    original.write_bytes(old)
    if mutation == "symlink":
        path.symlink_to(original)
    else:
        path.write_bytes(old)
    output = tmp_path / "output"
    output.mkdir()
    store = Evidence(output, max_file_bytes=1024, max_total_bytes=4096)
    reads = store.ordered_reads([path], workers=2, batch_size=1)
    try:
        selected, prepared = next(reads)
        assert prepared is not None
        if mutation == "write":
            path.write_bytes(new)
        else:
            replacement = tmp_path / "replacement.json"
            replacement.write_bytes(new)
            if mutation == "replace":
                replacement.replace(path)
            else:
                path.unlink()
                path.symlink_to(replacement)
        raw, sha = store.read(selected, prepared=prepared)
        assert raw == new and sha == hashlib.sha256(new).hexdigest()
        assert store.read_stats["prefetch_generation_retries"] == 1
        path.write_bytes(old)
        assert store.read(path) == (new, sha)
    finally:
        reads.close()


def test_prefetch_bounds_growing_reads_and_keeps_changed_read_failure(tmp_path, monkeypatch):
    path = tmp_path / "source.json"
    path.write_bytes(b'x' * 1024)
    output = tmp_path / "output"
    output.mkdir()
    store = Evidence(output, max_file_bytes=16384, max_total_bytes=32768)
    original_open = Path.open

    def growing_open(selected, mode='r', *args, **kwargs):
        if selected == path and mode == "rb":
            with original_open(path, "wb") as handle:
                handle.write(b'y' * 8192)
        return original_open(selected, mode, *args, **kwargs)

    monkeypatch.setattr(Path, "open", growing_open)
    prepared = store._prepare_reads([path], frozenset(), 1024)[0][1]
    assert len(prepared[2]) == 1025
    assert store.read(path, prepared=prepared) is None
    assert store.sources[str(path)]["state"] == "changed_during_read"
    assert store.issues == [{"path": str(path), "reason": "changed_during_read"}]
    assert not list((output / "evidence").iterdir())


def test_prefetch_does_not_read_explicit_payload_after_budget_failure(tmp_path, monkeypatch):
    output = tmp_path / "output"
    output.mkdir()
    path = tmp_path / "explicit.json"
    path.write_bytes(b'{"wall_seconds":1}')
    store = Evidence(output, max_file_bytes=4096, max_total_bytes=1)
    original_open = Path.open

    def reject_payload(selected, mode='r', *args, **kwargs):
        assert selected != path or mode != "rb", "budget gate must precede explicit payload I/O"
        return original_open(selected, mode, *args, **kwargs)

    monkeypatch.setattr(Path, "open", reject_payload)
    metrics, catalogue = inspect_benchmarks(store, Ledger(), [path],
                                          explicit_receipts=[path], read_workers=4)
    assert not metrics and catalogue[0]["state"] == "size_budget_exceeded"
    assert store.total_bytes == 0


def test_prefetch_is_bounded_and_closes_workers_when_consumer_stops(tmp_path):
    import threading
    output = tmp_path / "output"
    output.mkdir()
    paths = []
    for index in range(30):
        path = tmp_path / f"{index}.json"
        path.write_bytes(b'x' * 256)
        paths.append(path)
    store = Evidence(output, max_file_bytes=4096, max_total_bytes=16384)
    pulled = []

    def source():
        for path in paths:
            pulled.append(path)
            yield path

    batch = store._prepare_reads(paths[:4], frozenset(), 512)
    assert sum(len(value[2]) for _, value in batch if value) <= 513
    assert sum(value is not None for _, value in batch) == 2
    reads = store.ordered_reads(source(), workers=2, batch_size=2, batch_bytes=512)
    next(reads)
    reads.close()
    assert len(pulled) == 6
    assert not any(thread.name.startswith("evidence-read") for thread in threading.enumerate())
    assert not store.sources and not store.issues and store.total_bytes == 0


def test_inspection_closes_prefetch_after_consumer_failure(tmp_path, monkeypatch):
    import threading
    from scripts import profile_workflow_speeds as profile
    path = tmp_path / "source.json"
    path.write_bytes(b'{"wall_seconds":1}')
    output = tmp_path / "output"
    output.mkdir()
    store = Evidence(output, max_file_bytes=4096, max_total_bytes=16384)

    def fail(*args, **kwargs):
        raise RuntimeError("consumer failure")

    monkeypatch.setattr(profile, "capture_benchmark", fail)
    with pytest.raises(RuntimeError, match="consumer failure"):
        inspect_benchmarks(store, Ledger(), [path] * 100, read_workers=4)
    assert not any(thread.name.startswith("evidence-read") for thread in threading.enumerate())
    assert not store.sources


@pytest.mark.parametrize("backend", ["python", "cpp"])
def test_report_decoration_preserves_producer_objects_numeric_identity_and_raw_bytes(tmp_path, monkeypatch, backend):
    from copy import deepcopy
    import math
    from scripts import profile_workflow_speeds as profile
    path = tmp_path / "profile_receipt.json"
    raw = ('{"wall_seconds":-0.0,"elapsed_ms":' + str(10**70) +
           ',"a~/b":{"wall_seconds":2},"token":{"wall_seconds":99},'
           '"source_path":"producer","source_sha256":"producer"}').encode()
    path.write_bytes(raw)
    output = tmp_path / "output"
    output.mkdir()
    store = Evidence(output, max_file_bytes=4096, max_total_bytes=8192)
    original = profile.capture_benchmark
    captured = []

    def observe(*args, **kwargs):
        value = original(*args, **kwargs)
        payload, _, rows = value
        captured.append((payload, deepcopy(payload), [row["reported_value"] for row in rows]))
        return value

    monkeypatch.setattr(profile, "capture_benchmark", observe)
    profile.configure_metric_backend(backend)
    try:
        metrics, catalogue = inspect_benchmarks(store, Ledger(), [path], explicit_receipts=[path])
    finally:
        profile.configure_metric_backend("python")
    payload, before, numbers = captured[0]
    assert payload == before and payload["a~/b"] == {"wall_seconds": 2}
    assert [row["pointer"] for row in metrics] == ["/wall_seconds", "/elapsed_ms", "/a~0~1b/wall_seconds"]
    assert all(row["reported_value"] is value for row, value in zip(metrics, numbers))
    assert math.copysign(1, metrics[0]["reported_value"]) == -1
    assert metrics[1]["reported_value"] == 10**70 and metrics[1]["reported_unit"] == "milliseconds"
    assert catalogue[0]["timing_fields"] == 3
    sha = hashlib.sha256(raw).hexdigest()
    assert gzip.decompress((output / f"evidence/{sha}.gz").read_bytes()) == raw


@pytest.mark.parametrize("threads", [0, 1, 2, 4])
def test_training_discovery_threads_keep_depth_gaps_and_child_measurement(tmp_path, threads):
    from scripts.profile_workflow_speeds import discover_training
    selected = tmp_path / "startup_timing.jsonl"
    selected.write_bytes(b'{}\n')
    nested = tmp_path / "a/b"
    nested.mkdir(parents=True)
    (nested / "startup_timing.jsonl").write_bytes(b'{}\n')
    (tmp_path / "other.json").write_bytes(b'{}')
    observations = []
    paths, gaps = discover_training(tmp_path, depth=1, threads=threads, observations=observations)
    assert paths == [selected]
    assert gaps == [{"path": str(tmp_path), "reason": "training_depth_budget", "files_beyond_depth": 1}]
    assert len(observations) == 1
    observed = observations[0]
    assert observed["exit_code"] == 0 and observed["wall_seconds"] >= 0
    assert ("--threads" in observed["command"]) == bool(threads)
    if observed["child_resource_counters_available"]:
        assert all(observed["child_" + name] >= 0 for name in
                   ("utime", "stime", "minflt", "majflt", "inblock", "oublock", "nvcsw", "nivcsw"))


@pytest.mark.parametrize("failure", ["exit", "missing", "timeout"])
def test_training_discovery_failure_still_records_child_resources(tmp_path, monkeypatch, failure):
    import subprocess
    from scripts import profile_workflow_speeds as profile

    def failed(command, **kwargs):
        if failure == "exit":
            return subprocess.CompletedProcess(command, 2, b'', b'failure')
        if failure == "missing":
            raise FileNotFoundError("rg unavailable")
        raise subprocess.TimeoutExpired(command, 60)

    monkeypatch.setattr(profile.subprocess, "run", failed)
    observations = []
    paths, gaps = profile.discover_training(tmp_path, depth=1, observations=observations)
    assert not paths and len(gaps) == 1 and len(observations) == 1
    assert observations[0]["wall_seconds"] >= 0
    if failure == "exit":
        assert gaps[0]["exit_code"] == observations[0]["exit_code"] == 2
    else:
        assert observations[0]["error_type"] in gaps[0]["reason"]


def test_non_timing_payload_does_not_displace_complete_measurement_evidence(tmp_path):
    output = tmp_path / "observation"
    output.mkdir()
    payload = tmp_path / "dictionary.json"
    raw = json.dumps({"feature_names": ["price"] * 100}).encode()
    payload.write_bytes(raw)
    timing = tmp_path / "timing.json"
    timing_raw = b'{"wall_seconds":1.25}'
    timing.write_bytes(timing_raw)
    store = Evidence(output, max_file_bytes=4096, max_total_bytes=len(timing_raw))
    assert capture_benchmark(store, payload) is None
    entry = store.sources[str(payload)]
    assert entry["state"] == "not_timing_evidence"
    assert entry["sha256"] == hashlib.sha256(raw).hexdigest()
    assert entry["json_parsed"] is False
    assert store.total_bytes == 0 and store.issues == []
    result, sha, metrics = capture_benchmark(store, timing)
    assert result == {"wall_seconds": 1.25} and len(metrics) == 1
    assert gzip.decompress((output / f"evidence/{sha}.gz").read_bytes()) == timing_raw
    assert store.total_bytes == len(timing_raw)


def test_negative_inspection_reuse_requires_full_content_hash_and_never_hides_explicit_receipts(tmp_path, monkeypatch):
    from scripts import profile_workflow_speeds as profile
    output = tmp_path / "observation"
    output.mkdir()
    raw = b'{"feature_s":[1,2]}'
    sources = [tmp_path / f"source{index}.json" for index in range(4)]
    for path in sources:
        path.write_bytes(raw)
    sources[2].write_bytes(b'{"wall_seconds":2}')
    original = profile.private_json.loads
    calls = []

    def counted(body):
        calls.append(body)
        return original(body)

    monkeypatch.setattr(profile.private_json, "loads", counted)
    store = Evidence(output, max_file_bytes=4096, max_total_bytes=4096)
    assert capture_benchmark(store, sources[0]) is None
    assert capture_benchmark(store, sources[1]) is None
    assert len(calls) == 1
    reused = store.sources[str(sources[1])]
    assert reused["classification_source_path"] == str(sources[0])
    assert reused["classification_reused_from_sha256"] == hashlib.sha256(raw).hexdigest()
    assert reused["json_parsed"] is False
    payload, _, metrics = capture_benchmark(store, sources[2])
    assert payload == {"wall_seconds": 2} and len(metrics) == 1
    payload, sha, metrics = capture_benchmark(store, sources[3], explicit=True)
    assert payload == {"feature_s": [1, 2]} and metrics == []
    assert gzip.decompress((output / f"evidence/{sha}.gz").read_bytes()) == raw
    assert len(calls) == 3


@pytest.mark.parametrize("raw", [b'{"elapsed\\u005fms":10}',
                                  '{"wall_seconds":2}'.encode("utf-16"),
                                  b'{"nested":[0,null,{"total_s":3}]}'])
def test_encoded_and_nested_timing_keys_retain_complete_originals(tmp_path, raw):
    output = tmp_path / "observation"
    output.mkdir()
    source = tmp_path / "source.json"
    source.write_bytes(raw)
    store = Evidence(output, max_file_bytes=4096, max_total_bytes=4096)
    _, sha, metrics = capture_benchmark(store, source)
    assert len(metrics) == 1
    assert gzip.decompress((output / f"evidence/{sha}.gz").read_bytes()) == raw


def test_explicit_zero_timing_receipt_is_retained_and_malformed_candidate_stays_visible(tmp_path):
    output = tmp_path / "observation"
    output.mkdir()
    source = tmp_path / "status.json"
    source.write_bytes(b'{"state":"waiting_source"}')
    store = Evidence(output, max_file_bytes=4096, max_total_bytes=4096)
    payload, _, metrics = capture_benchmark(store, source, explicit=True)
    assert payload == {"state": "waiting_source"} and metrics == []
    broken = tmp_path / "broken.json"
    broken.write_bytes(b'{"wall_seconds":')
    assert capture_benchmark(store, broken) is None
    assert store.sources[str(broken)]["state"] == "invalid_json"
    assert store.issues[-1]["reason"] == "JSONDecodeError"


@pytest.mark.parametrize("value", [None, True, False, float("nan"), float("inf"), -1, "1"])
def test_invalid_durations_are_never_zero_or_fast_samples(value):
    assert not finite_seconds(value)
    ledger = Ledger()
    ledger.add(workflow="x", stage="y", seconds=value, source="sha", locator="a", origin="test")
    assert ledger.samples == []


def test_explicit_zero_has_no_infinite_throughput_and_different_inputs_do_not_pool():
    ledger = Ledger()
    for seconds, context in [(0, {"rows": 0}), (1, {"rows": 20}), (3, {"rows": 20}), (9, {"rows": 40})]:
        ledger.add(workflow="x", stage="whole", seconds=seconds, source="sha", locator="a", origin="test",
                   context=context, work={"units": context["rows"], "unit": "rows"})
    assert ledger.samples[0]["units_per_second"] is None
    summaries = ledger.summaries()
    assert len(summaries) == 3
    twenty = next(row for row in summaries if row["context"]["rows"] == 20)
    assert twenty["samples"] == 2 and twenty["median_seconds"] == 2


def test_failed_process_and_unmatched_steps_cannot_be_successful_operations():
    ledger = Ledger()
    collect_services(ledger, {"observed_at_utc": "now", "services": [{
        "unit": "stockagent-sample.service", "last_attempt_wall_seconds": 12,
        "last_attempt_outcome": "failed", "operation_latency_coverage": "last_completed_process_wall_only",
        "business_receipt": {"matches_last_attempt": False, "step_receipts_scope": "exact_matched_run",
                             "step_receipts": [{"step": "download", "elapsed_seconds": 5}]},
    }]}, "sha")
    assert len(ledger.samples) == 1
    assert ledger.samples[0]["outcome"] == "failed"
    assert ledger.inventory[0]["coverage"] == "last_completed_process_wall_only"


def test_http_errors_audiences_and_first_observation_are_separate():
    ledger = Ledger()
    payload = {"base_url": "http://127.0.0.1:8770", "http": [{
        "path": "/api/status", "errors": 1,
        "first_observed": {"ok": False, "status": 403, "total_ms": 2},
        "samples": [{"ok": True, "status": 200, "total_ms": 30, "decoded_bytes": 10},
                    {"ok": True, "status": 200, "total_ms": 50, "decoded_bytes": 11}],
    }]}
    collect_http(ledger, payload, "sha")
    assert len(ledger.samples) == 3
    assert ledger.samples[0]["seconds"] == .002
    summaries = ledger.summaries()
    success = next(row for row in summaries if row["outcome"] == "http_success")
    assert success["samples"] == 2 and success["median_seconds"] == .04
    payload["base_url"] = "http://127.0.0.1:8771"
    collect_http(ledger, payload, "other_sha")
    assert len(ledger.summaries()) == 4


def test_training_uses_max_rank_scope_keeps_warmup_separate_and_retains_valid_prefix():
    ledger, issues = Ledger(), []
    rows = [{"epoch": 1, "epoch_wall_s_max_rank": 100, "train_total_s": 80, "timing_rank_count": 2},
            {"epoch": 3, "epoch_wall_s_max_rank": 20, "train_total_s": 10, "timing_rank_count": 2}]
    raw = b"\n".join(json.dumps(row).encode() for row in rows) + b'\n{"epoch":4'
    collect_training(ledger, Path("run/train/epoch_curve.jsonl"), raw, "sha", issues)
    max_rank = [row for row in ledger.samples if row["stage"] == "epoch_wall_s_max_rank"]
    assert [row["seconds"] for row in max_rank] == [100, 20]
    assert {row["context"]["epoch_regime"] for row in max_rank} == {"first_two_epochs", "epoch_3_plus"}
    assert all(row["context"]["world_size"] == 2 for row in max_rank)
    assert issues[0]["line"] == 3
    assert all(row["outcome"] != "complete_fold" for row in ledger.samples)


def test_reported_index_is_labelled_and_does_not_read_secret_subtrees():
    result = list(reported_metrics({"trials": [{"wall_seconds": 2.5, "elapsed_ms": 30, "rows": 100}],
                                    "api_key": {"wall_seconds": 99}, "estimated_time": 10,
                                    "total_seconds": False, "duration_s": -1}))
    assert len(result) == 2
    assert result[1]["reported_unit"] == "milliseconds"
    assert all(row["semantic_state"] == "producer_reported_requires_scope_review" for row in result)


def test_metric_search_keeps_pointer_order_unicode_redaction_and_non_timing_big_integers():
    payload = {"business_value": 10**400, "unrecognized_x_s": 10**400,
               "x/y~z": [0, None, {"wall_seconds": 2.5, "elapsed_ms": 30,
                                   "api_Key_elapsed_ms": 99}],
               "AUTHORIZATION": {"nested": [{"seconds": 999}]},
               "seconds": False, "duration_s": -1}
    result = list(reported_metrics(payload))
    assert [(row["pointer"], row["reported_value"], row["reported_unit"]) for row in result] == [
        ("/x~1y~0z/2/wall_seconds", 2.5, "seconds"),
        ("/x~1y~0z/2/elapsed_ms", 30, "milliseconds"),
    ]


def test_reused_key_metadata_never_reuses_values_or_container_decisions():
    # The same key is a subtree, a rejected boolean, then a real zero duration.
    assert [row["pointer"] for row in reported_metrics({"seconds": {"wall_seconds": 3}})] == ["/seconds/wall_seconds"]
    assert list(reported_metrics({"seconds": True})) == []
    assert list(reported_metrics({"seconds": 0}))[0]["reported_value"] == 0
    for index in range(4200):
        assert list(reported_metrics({f"business_field_{index}": index})) == []
    payload = {"seconds": {"wall_seconds": 7}, "api_Key_elapsed_ms": {"seconds": 999},
               "values": [{"elapsed_ms": -0.0}, {"elapsed_ms": "2"}, {"elapsed_ms": 4.5}]}
    rows = list(reported_metrics(payload))
    assert [(row["pointer"], row["reported_value"], row["reported_unit"]) for row in rows] == [
        ("/seconds/wall_seconds", 7, "seconds"),
        ("/values/0/elapsed_ms", -0.0, "milliseconds"),
        ("/values/2/elapsed_ms", 4.5, "milliseconds"),
    ]
    assert '"reported_value": -0.0' in json.dumps(rows)


def test_long_uncached_key_names_preserve_timing_and_pointer_escaping():
    scope = "scope/" * 60 + "~"
    metric = "x" * 300 + "_wall_seconds"
    rows = list(reported_metrics({scope: {metric: 1.5}, "credential": {metric: 999}}))
    assert rows == [{"pointer": "/" + "scope~1" * 60 + "~0/" + metric,
                     "reported_value": 1.5, "reported_unit": "seconds",
                     "semantic_state": "producer_reported_requires_scope_review"}]


@pytest.mark.parametrize("read_workers", [0, 1, 2, 4])
def test_shared_inspection_keeps_complete_selection_receipts_and_failed_evidence(tmp_path, read_workers):
    output = tmp_path / "observation"
    output.mkdir()
    raw = {
        "profile_selected.json": b'{"samples":[{"wall_seconds":1.25,"cpu_seconds":0.5}]}',
        "automatic.json": b'{"nested":{"elapsed_ms":4}}',
        "dictionary.json": b'{"contract_s":"not a duration"}',
        "duplicate.json": b'{"contract_s":"not a duration"}',
        "broken.json": b'{"wall_seconds":',
    }
    paths = []
    for name, body in raw.items():
        path = tmp_path / name
        path.write_bytes(body)
        paths.append(path)
    paths.append(tmp_path / "missing.json")
    store = Evidence(output, max_file_bytes=4096, max_total_bytes=16384)
    ledger = Ledger()
    metrics, catalogue = inspect_benchmarks(store, ledger, paths,
                                          explicit_receipts=[paths[0], paths[0]], read_workers=read_workers)
    assert [row["path"] for row in catalogue] == [str(path) for path in paths]
    assert [row["state"] for row in catalogue] == ["captured", "captured", "not_timing_evidence",
                                                  "not_timing_evidence", "invalid_json", "unavailable"]
    assert catalogue[0]["category"] == "benchmark"
    assert [row["pointer"] for row in metrics] == ["/samples/0/wall_seconds", "/samples/0/cpu_seconds", "/nested/elapsed_ms"]
    assert [row["seconds"] for row in ledger.samples] == [1.25, 0.5]
    assert len(store.issues) == 2
    assert catalogue[3]["classification_source_path"] == str(paths[2])
    for path in paths[:2]:
        source = store.sources[str(path)]
        assert gzip.decompress((output / source["snapshot"]).read_bytes()) == raw[path.name]


def test_discovery_does_not_follow_data_links_and_reports_depth_gaps(tmp_path):
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "hidden.json").write_text("{}")
    root = tmp_path / "root"
    root.mkdir()
    (root / "source.json").write_text("{}")
    (root / "data-link").symlink_to(outside, target_is_directory=True)
    nested = root / "one" / "two"
    nested.mkdir(parents=True)
    (nested / "deep.json").write_text("{}")
    paths, gaps = discover(root, depth=1)
    assert paths == [root / "source.json"]
    assert gaps and gaps[0]["reason"] == "depth_or_directory_budget"


def test_discovery_reaches_deep_receipts_and_prunes_retained_input_roots(tmp_path):
    root = tmp_path / "benchmarks"
    source = root / "experiment" / "cache" / "run" / "fold" / "report.json"
    source.parent.mkdir(parents=True)
    source.write_text("{}")
    retained = root / "retained"
    retained.mkdir()
    (retained / "input.json").write_text("{}")
    paths, gaps = discover(root, depth=15, exclude_roots=[retained])
    assert paths == [source]
    assert gaps == []


def test_discovery_preserves_dotfile_suffixes_and_custom_predicates(tmp_path):
    for name in (".json", ".metadata.json", "report.json", "double..json", "upper.JSON", "data.json.x", "log.jsonl"):
        (tmp_path / name).write_text("{}")
    paths, gaps = discover(tmp_path, depth=1)
    assert paths == sorted(tmp_path / name for name in (".metadata.json", "report.json", "double..json"))
    assert gaps == []
    paths, gaps = discover(tmp_path, depth=1, wanted=lambda path: path.suffix == ".jsonl")
    assert paths == [tmp_path / "log.jsonl"]
    assert gaps == []


def test_code_inventory_keeps_non_entrypoint_sources_and_broken_script_evidence(tmp_path):
    import subprocess
    subprocess.run(["git", "init", "--quiet", str(tmp_path)], check=True)
    (tmp_path / "scripts").mkdir()
    (tmp_path / "stockagent").mkdir()
    (tmp_path / "stockagent/library.py").write_text("parser.add_argument('--unused')\ntime.perf_counter()\n")
    (tmp_path / "scripts/entry.py").write_text(
        '"""Entry description."""\nimport time\nif __name__ == "__main__":\n'
        "    parser.add_argument('--flag')\n    time.perf_counter()\n")
    (tmp_path / "scripts/broken.py").write_text("if (")
    subprocess.run(["git", "-C", str(tmp_path), "add", "--force", "--all"], check=True)
    rows, code = visible_code(tmp_path)
    assert len(code) == 3
    assert {row["path"] for row in code} == {"scripts/entry.py", "scripts/broken.py", "stockagent/library.py"}
    by_name = {row["id"]: row for row in rows}
    assert set(by_name) == {"scripts/entry.py", "scripts/broken.py"}
    assert by_name["scripts/entry.py"]["argument_flags"] == ["--flag"]
    assert by_name["scripts/entry.py"]["timing_call_lines"] == [5]
    assert by_name["scripts/entry.py"]["description"] == "Entry description."
    assert by_name["scripts/broken.py"]["discovery_error"]


def test_discovery_keeps_unreadable_directories_as_coverage_gaps(tmp_path, monkeypatch):
    from scripts import profile_workflow_speeds as profile
    def denied(root):
        raise PermissionError(13, "denied", str(tmp_path / "unreadable"))
    monkeypatch.setattr(profile.os, "scandir", denied)
    paths, gaps = discover(tmp_path, depth=15)
    assert paths == []
    assert gaps == [{"path": str(tmp_path / "unreadable"),
                     "reason": "directory_unreadable", "error_type": "PermissionError"}]


@pytest.mark.parametrize("root_name", [".", "tree"])
@pytest.mark.parametrize("absolute_exclusion", [False, True])
def test_discovery_relative_root_keeps_exclusions(tmp_path, monkeypatch, root_name, absolute_exclusion):
    root = tmp_path if root_name == "." else tmp_path / root_name
    for name in ("keep", "skip"):
        (root / name).mkdir(parents=True)
        (root / name / "source.json").write_text("{}")
    monkeypatch.chdir(tmp_path)
    selected_root = Path(root_name)
    excluded = root / "skip" if absolute_exclusion else selected_root / "skip"
    paths, gaps = discover(selected_root, depth=2, exclude_roots=[excluded])
    assert paths == [selected_root / "keep/source.json"]
    assert gaps == []


def test_discovery_directory_budget_keeps_queued_sibling_files(tmp_path):
    root = tmp_path / "root"
    for name in ("first", "second"):
        directory = root / name
        (directory / "nested").mkdir(parents=True)
        (directory / "source.json").write_text("{}")
        (directory / "nested/hidden.json").write_text("{}")
    paths, gaps = discover(root, depth=15, directory_budget=2)
    assert paths == sorted(root / name / "source.json" for name in ("first", "second"))
    assert {row["path"] for row in gaps} == {str(root / name) for name in ("first", "second")}
    assert all(row["reason"] == "depth_or_directory_budget" and row["directories"] == 1 for row in gaps)


def test_discovery_rechecks_links_after_custom_predicate_can_retarget(tmp_path):
    root, outside = tmp_path / "root", tmp_path / "outside"
    (root / "child").mkdir(parents=True)
    outside.mkdir()
    (root / "child/original.json").write_text("{}")
    (outside / "private.json").write_text("{}")
    trigger = root / "trigger.json"
    trigger.write_text("{}")

    def predicate(path):
        if path == trigger:
            (root / "child").rename(tmp_path / "preserved-child")
            (root / "child").symlink_to(outside, target_is_directory=True)
        return path.suffix == ".json"

    paths, gaps = discover(root, depth=15, wanted=predicate)
    assert paths == [trigger] and gaps == []


def test_discovery_iteration_error_discards_partial_directory_and_records_gap(tmp_path, monkeypatch):
    from scripts import profile_workflow_speeds as profile

    class Entry:
        name = "partial.json"

        def is_dir(self):
            return False

    class Interrupted:
        count = 0

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def __iter__(self):
            return self

        def __next__(self):
            self.count += 1
            if self.count == 1:
                return Entry()
            raise PermissionError(13, "denied during iteration", str(tmp_path))

    monkeypatch.setattr(profile.os, "scandir", lambda root: Interrupted())
    paths, gaps = discover(tmp_path, depth=15)
    assert paths == []
    assert gaps == [{"path": str(tmp_path), "reason": "directory_unreadable", "error_type": "PermissionError"}]


def test_benchmark_selection_keeps_original_order_and_file_link_exclusions(tmp_path):
    root = tmp_path / "receipts"
    root.mkdir()
    output = tmp_path / "observation"
    output.mkdir()
    kept = root / "kept.json"
    kept.write_text("{}")
    retained = output / "retained.json"
    retained.write_text("{}")
    link = root / "file-link.json"
    link.symlink_to(retained)
    dangling = root / "missing.json"
    dangling.symlink_to(root / "not-yet-created")
    missing_parent = root / "deleted-directory" / "receipt.json"
    (root / "nested").mkdir()
    neighbor = tmp_path / "observation_extra" / "receipt.json"
    neighbor.parent.mkdir()
    neighbor.write_text("{}")
    inputs = [dangling, link, kept, missing_parent, kept, root / "workflow_speed_private.json",
              root / "nested" / ".." / "kept.json", neighbor]
    expected = [path for path in inputs if not path.resolve().is_relative_to(output)
                and "workflow_speed_" not in str(path)]
    assert select_benchmark_paths(inputs, output) == expected


def test_benchmark_selection_invalidates_retargeted_parent(tmp_path):
    included = tmp_path / "included"
    output = tmp_path / "observation"
    included.mkdir()
    output.mkdir()
    for directory in (included, output):
        (directory / "a.json").write_text("{}")
        (directory / "b.json").write_text("{}")
    alias = tmp_path / "alias"
    alias.symlink_to(included, target_is_directory=True)

    def paths():
        yield alias / "a.json"
        alias.unlink()
        alias.symlink_to(output, target_is_directory=True)
        yield alias / "b.json"

    assert select_benchmark_paths(paths(), output) == [alias / "a.json"]


def test_benchmark_selection_invalidates_retargeted_ancestor(tmp_path):
    included = tmp_path / "included"
    output = tmp_path / "observation"
    for directory in (included, output):
        (directory / "nested").mkdir(parents=True)
        (directory / "nested/a.json").write_text("{}")
        (directory / "nested/b.json").write_text("{}")
    alias = tmp_path / "alias"
    alias.symlink_to(included, target_is_directory=True)

    def paths():
        yield alias / "nested/a.json"
        alias.unlink()
        alias.symlink_to(output, target_is_directory=True)
        yield alias / "nested/b.json"

    assert select_benchmark_paths(paths(), output) == [alias / "nested/a.json"]


def test_benchmark_selection_preserves_legacy_inaccessible_file_resolution(tmp_path, monkeypatch):
    import os
    source = tmp_path / "receipt.json"
    source.write_text("{}")
    output = tmp_path / "observation"
    original_lstat = os.lstat

    def denied(name, *args, **kwargs):
        if os.fspath(name) == str(source):
            raise PermissionError(13, "denied", str(source))
        return original_lstat(name, *args, **kwargs)

    monkeypatch.setattr(os, "lstat", denied)
    assert not source.resolve().is_relative_to(output)
    assert select_benchmark_paths([source], output) == [source]


def test_benchmark_selection_preserves_legacy_file_link_loop_failure(tmp_path):
    source = tmp_path / "loop.json"
    source.symlink_to(source.name)
    with pytest.raises(RuntimeError, match="[Ss]ymlink loop"):
        select_benchmark_paths([source], tmp_path / "observation")


@pytest.mark.parametrize("stable", [True, False])
def test_complete_history_profile_units_and_unstable_inputs_remain_explicit(stable):
    ledger = Ledger()
    collect_benchmark(ledger, {"local_history": {
        "source_stable": stable, "outputs_identical": True,
        "full_source_rebuild": True, "wire_and_independent_readback": True,
        "inputs": {"marks": [1, 2, 3]}, "source_sha256": "producer",
        "samples": [{"build_ms": 100, "workflow_ms": 250, "cpu_seconds": .2,
                     "independent_readback_ms": 50, "point_count": 20, "sha256": "output"}],
    }}, "receipt", Path("history.json"))
    sample = next(row for row in ledger.samples if row["stage"] == "history:workflow_ms")
    assert sample["seconds"] == .25
    assert sample["units_per_second"] == 80
    assert sample["outcome"] == ("stable_history_profile" if stable else "source_or_output_unstable")
    assert next(row for row in ledger.samples if row["stage"] == "history:cpu_seconds")["metric"] == "cpu_seconds"


def test_finmind_reads_existing_tables_without_schema_changes_and_deduplicates_cycle_wall(tmp_path):
    root = tmp_path / "data_finmind"
    (root / "complement").mkdir(parents=True)
    database = root / "complement/queue.sqlite3"
    with sqlite3.connect(database) as connection:
        connection.execute("CREATE TABLE finmind_json_pipeline_samples (id INTEGER PRIMARY KEY, dataset TEXT, "
                           "seconds REAL, rows INTEGER, cycle_wall_verified INTEGER, cycle_id TEXT, cycle_wall_seconds REAL)")
        connection.executemany("INSERT INTO finmind_json_pipeline_samples VALUES (?,?,?,?,?,?,?)", [
            (1, "dataset_a", 2.0, 20, 1, "shared_cycle", 10.0),
            (2, "dataset_b", 3.0, 40, 1, "shared_cycle", 10.0),
        ])
    before = database.read_bytes()
    output = tmp_path / "observation"
    output.mkdir()
    evidence = Evidence(output, max_file_bytes=65536, max_total_bytes=65536)
    ledger = Ledger()
    collect_finmind(ledger, evidence, root)
    assert database.read_bytes() == before
    assert len(ledger.samples) == 3
    cycles = [row for row in ledger.samples if row["stage"] == "verified_complete_cycle_wall"]
    assert len(cycles) == 1 and cycles[0]["seconds"] == 10
    snapshot = json.loads((output / "finmind_telemetry_snapshot.json").read_bytes())
    assert snapshot["provider_calls"] == 0 and snapshot["query_only"] is True


def test_selected_benchmark_converts_ms_and_keeps_variants_separate():
    ledger = Ledger()
    payload = {"scope": "isolated_publication", "samples": [
        {"variant": "a", "elapsed_ms": 500, "phases": [{"stage": "write", "elapsed_ms": 200}]},
        {"variant": "b", "elapsed_ms": 700, "complete_cpu_seconds": .3,
         "writes": [{"file": "state.json", "elapsed_ms": 100}],
         "stages": {"model_inference_ms": 50, "cache_hit": True}},
    ]}
    collect_benchmark(ledger, payload, "sha", Path("benchmark/publication.json"))
    assert [row["seconds"] for row in ledger.samples] == [.5, .2, .7, .3, .1, .05]
    assert ledger.samples[3]["metric"] == "cpu_seconds"
    assert len(ledger.summaries()) == 6


def test_browser_clock_units_audience_and_nonadditive_boundaries_are_retained():
    ledger = Ledger()
    payload = {"contract": "read_only_dashboard_browser_timing_v1", "url": "https://example.test/",
        "boundary": "server cache uncontrolled", "errors": [], "sseMessages": 2,
        "initial": {"readyMs": 1250, "navigation": [{"responseEndMs": 120}],
                    "paint": [{"name": "first-contentful-paint", "startMs": 500}]},
        "fullHistory": {"loadMs": 2200, "rebuildMs": 30, "points": 372946,
                        "series": 9, "identicalCanvas": True},
        "final": {"resources": [{"path": "/api/history", "durationMs": 750}]}}
    collect_benchmark(ledger, payload, "sha", Path("benchmark/browser.json"))
    observed = {row["locator"]: row for row in ledger.samples}
    assert observed["/initial/readyMs"]["seconds"] == 1.25
    assert observed["/initial/navigation/0/responseEndMs"]["stage"] == "navigation_to:responseEndMs"
    full = observed["/fullHistory/rebuildMs"]
    assert full["seconds"] == 0.03 and full["context"]["points"] == 372946
    assert full["context"]["identical_canvas"] is True
    assert full["context"]["url"] == "https://example.test/"
    assert full["units_per_second"] is None
    assert observed["/final/resources/0/durationMs"]["seconds"] == 0.75


def test_complete_rebuild_benchmark_preserves_workflow_and_readback_intervals():
    ledger = Ledger()
    collect_benchmark(ledger, {"scope": "full output plus readback", "samples": [
        {"variant": "candidate", "build_seconds": 10, "workflow_seconds": 13,
         "independent_readback_seconds": 0.5, "process_seconds": 14}]},
        "sha", Path("benchmark/history.json"))
    assert {row["stage"]: row["seconds"] for row in ledger.samples} == {
        "build_seconds": 10, "workflow_seconds": 13, "independent_readback_seconds": 0.5,
        "process_seconds": 14}


def test_source_stage_binding_requires_exact_registry_id_and_excludes_shared_cycle():
    ledger = Ledger()
    ledger.inventory = [{"kind": "source_endpoint", "id": "finmind:TaiwanFuturesTick"}]
    for context in ({"dataset": "TaiwanFuturesTick"}, {"dataset": "Unknown"},
                    {"dataset": "TaiwanFuturesTick", "input_identity": "producer_cycle"}):
        ledger.add(workflow="finmind", stage="transfer", seconds=1, source="sha", locator="row",
                   origin="recorded_finmind_telemetry", context=context)
    assert bind_source_stages(ledger) == 1
    assert ledger.samples[0]["source_endpoint_id"] == "finmind:TaiwanFuturesTick"
    assert "source_endpoint_id" not in ledger.samples[1]
    assert "source_endpoint_id" not in ledger.samples[2]
    assert ledger.inventory[0]["speed_sample_count"] == 1
    assert ledger.inventory[0]["coverage"] == "recorded_stage_timing_only"


def test_storage_pairs_same_scheduled_start_and_terminal_without_timing_running_work():
    def event(identifier, second, key, value):
        return {"eventId": str(identifier), "eventTime": f"2026-10-21T10:00:{second:02d}Z", key: value}

    events = [
        event(1, 0, "activityTaskScheduledEventAttributes", {"activityId": "1", "activityType": {"name": "replicate"}}),
        event(2, 4, "activityTaskStartedEventAttributes", {"scheduledEventId": "1", "attempt": 2}),
        event(3, 6, "activityTaskCompletedEventAttributes", {"scheduledEventId": "1", "startedEventId": "2"}),
        event(4, 7, "activityTaskScheduledEventAttributes", {"activityId": "2", "activityType": {"name": "verify"}}),
        event(5, 8, "activityTaskStartedEventAttributes", {"scheduledEventId": "4", "attempt": 1}),
        event(6, 9, "activityTaskScheduledEventAttributes", {"activityId": "3", "activityType": {"name": "publish"}}),
        # An unrelated started event must not supply the missing start clock.
        event(7, 10, "activityTaskFailedEventAttributes", {"scheduledEventId": "6", "startedEventId": "2"}),
    ]
    rows, pending = storage_activity_timings({"events": events})
    assert rows[0]["server_start_to_terminal_seconds"] == 2
    assert rows[0]["schedule_to_recorded_start_seconds"] == 4
    assert rows[0]["attempt"] == 2
    assert rows[1]["server_start_to_terminal_seconds"] is None
    assert pending == [{"scheduled_event_id": 4, "activity": "verify", "state": "no_terminal_event_in_snapshot", "seconds": None}]


def test_finlab_retains_failed_and_unchanged_attempts_without_inventing_download_throughput(tmp_path):
    from stockagent.data.finlab_acquisition_contract import safe_stem
    root = tmp_path / "finlab"
    dataset = "price:收盤價"
    for kind, values in (("receipts", {"status": "downloaded_unverified_for_pit", "last_check_result": "unchanged",
                                      "last_fetch_elapsed_seconds": 2, "rows": 3000, "parquet_size_bytes": 9000}),
                         ("attempts", {"status": "quota_exhausted", "last_attempt_elapsed_seconds": 0.5})):
        (root / kind).mkdir(parents=True)
        (root / kind / (safe_stem(dataset) + ".json")).write_text(json.dumps({"dataset": dataset, **values}))
    (root / "receipts/wrong.json").write_text(json.dumps({"dataset": dataset, "last_fetch_elapsed_seconds": 99}))
    output = tmp_path / "observation"
    output.mkdir()
    store = Evidence(output, max_file_bytes=4096, max_total_bytes=16384)
    ledger = Ledger()
    ledger.inventory = [{"kind": "source_endpoint", "id": "finlab:" + dataset}]
    collect_finlab(ledger, store, root)
    assert len(ledger.samples) == 2
    assert {row["outcome"] for row in ledger.samples} == {"unchanged", "quota_exhausted"}
    assert all(row["units_per_second"] is None for row in ledger.samples)
    assert bind_source_stages(ledger) == 1
    assert all(row["source_endpoint_id"] == "finlab:" + dataset for row in ledger.samples)
    assert store.issues[0]["reason"] == "finlab_dataset_receipt_identity_mismatch"


def test_nested_explainability_timings_keep_component_scope_and_original_sha():
    ledger = Ledger()
    collect_training(ledger, Path("run/explainability_timing.json"),
                     json.dumps({"total_s": 7, "compute_timing": {"forward_s": 3, "count": 4},
                                 "write_timing": {"plot_s": 2, "invalid_s": -1}}).encode(), "original", [])
    assert {row["stage"]: row["seconds"] for row in ledger.samples} == {
        "total_s": 7, "compute_timing/forward_s": 3, "write_timing/plot_s": 2}
    assert all(row["source_sha256"] == "original" for row in ledger.samples)


def test_readback_recomputes_summary_and_rejects_changed_original(tmp_path):
    output = tmp_path / "observation"
    output.mkdir()
    original = tmp_path / "producer.json"
    original.write_text('{"seconds":1}')
    store = Evidence(output, max_file_bytes=1024, max_total_bytes=4096)
    _, sha = store.read(original)
    for name in ("dictionary_a.json", "dictionary_b.json"):
        path = tmp_path / name
        path.write_text('{"feature_names":["price"]}')
        assert capture_benchmark(store, path) is None
    ledger = Ledger()
    for seconds in (1, 3):
        ledger.add(workflow="transfer", stage="complete", seconds=seconds, source=sha,
                   locator="sample", origin="fixture", work={"units": 6})
    (output / "speed_samples.jsonl").write_text("".join(json.dumps(row) + "\n" for row in ledger.samples))
    write_csv(output / "speed_summary.csv", ledger.summaries())
    (output / "inventory.json").write_text('{"rows":[]}')
    (output / "evidence_manifest.json").write_text(json.dumps({"files": list(store.sources.values())}))
    report = {"speed_samples": 2, "summary_groups": 1, "sample_counts": {"fixture": 2}, "inventory_counts": {},
              "non_timing_inspections_reused": 1}

    def seal():
        report["outputs"] = {p.name: {"bytes": p.stat().st_size, "sha256": hashlib.sha256(p.read_bytes()).hexdigest()}
                             for p in output.iterdir() if p.is_file() and p.name != "collection.json"}
        (output / "collection.json").write_text(json.dumps(report))

    seal()
    verified = verify_collection(output)
    assert verified["speed_samples_verified"] == 2
    assert verified["non_timing_identities_recorded"] == 2
    assert verified["classification_reuse_links_verified"] == 1
    report["phase_timings"] = {"scan": {"wall_seconds": 1.0, "cpu_seconds": 0.5},
                               "index": {"wall_seconds": 2.0, "cpu_seconds": 1.5}}
    report["pre_manifest_wall_seconds"] = 3.0
    seal()
    assert verify_collection(output)["collector_phases_verified"] == 2
    report["phase_timings"]["scan"]["wall_seconds"] = float("nan")
    seal()
    with pytest.raises(ValueError, match="invalid collector phase duration"):
        verify_collection(output)
    report["phase_timings"]["scan"]["wall_seconds"] = 1.0
    report["pre_manifest_wall_seconds"] = 4.0
    seal()
    with pytest.raises(ValueError, match="collector phase total differs"):
        verify_collection(output)
    report["pre_manifest_wall_seconds"] = 3.0
    seal()
    saved = json.loads((output / "evidence_manifest.json").read_bytes())
    wrong = json.loads(json.dumps(saved))
    wrong["files"][-1]["classification_reused_from_sha256"] = "0" * 64
    (output / "evidence_manifest.json").write_text(json.dumps(wrong))
    seal()
    with pytest.raises(ValueError, match="classification reuse identity differs"):
        verify_collection(output)
    (output / "evidence_manifest.json").write_text(json.dumps(saved))
    seal()
    summaries = ledger.summaries()
    summaries[0]["median_seconds"] = 1
    write_csv(output / "speed_summary.csv", summaries)
    seal()  # Hashes match, but a wrong summary still must fail.
    with pytest.raises(ValueError, match="summary median_seconds differs"):
        verify_collection(output)
    write_csv(output / "speed_summary.csv", ledger.summaries())
    seal()
    (output / f"evidence/{sha}.gz").write_bytes(gzip.compress(b"changed producer"))
    with pytest.raises(ValueError, match="frozen producer SHA"):
        verify_collection(output)


@pytest.mark.parametrize("workers", [0, 1, 4])
def test_inspection_profiles_preserve_evidence_and_separate_worker_ranges(tmp_path, workers):
    paths = []
    bodies = [b'{"wall_seconds":-0.0,"value":123456789012345678901234567890}',
              b'{"features":[1,2]}', b'{"elapsed_ms":2}', b'{"wall_seconds":']
    for index in range(70):
        path = tmp_path / f"input{index}.json"
        path.write_bytes(bodies[index % len(bodies)])
        paths.append(path)
    paths.extend([paths[0], paths[1]])
    results = []
    for enabled in (False, True):
        output = tmp_path / str(enabled)
        output.mkdir()
        store = Evidence(output, max_file_bytes=4096, max_total_bytes=16384)
        ledger = Ledger()
        metrics, catalogue = inspect_benchmarks(store, ledger, paths,
            explicit_receipts=[paths[2]], read_workers=workers,
            profile_output=output / "profile.json" if enabled else None)
        assert "_prepare_reads" not in store.__dict__
        assert "read" not in store.__dict__
        assert store.inspection_timings is None
        results.append((metrics, catalogue, store.sources, store.issues, store.total_bytes,
                        ledger.samples, {p.name: p.read_bytes() for p in (output / "evidence").iterdir()}))
    assert results[0] == results[1]
    assert not (tmp_path / "False/profile.json").exists()
    report = json.loads((tmp_path / "True/profile.json").read_bytes())
    assert report["diagnostic_only"] is True
    consumers = [row for row in report["profiles"] if "evidence_read_inclusive" in row["stages"]]
    readers = [row for row in report["profiles"] if "read_batch_inclusive" in row["stages"]]
    assert len(consumers) == 1
    assert consumers[0]["stages"]["evidence_read_inclusive"]["calls"] == len(paths)
    assert consumers[0]["stages"]["json_decode"]["failed_calls"] > 0
    assert not readers if workers == 0 else 1 <= len(readers) <= workers
    for thread in report["profiles"]:
        assert all(0 <= row["failed_calls"] <= row["calls"] and row["wall_nanoseconds"] >= 0
                   and row["cpu_nanoseconds"] >= 0 for row in thread["stages"].values())


def test_inspection_profile_failure_restores_read_owner(tmp_path, monkeypatch):
    import scripts.profile_workflow_speeds as profile

    output = tmp_path / "output"
    output.mkdir()
    source = tmp_path / "input.json"
    source.write_bytes(b'{"seconds":1}')
    store = Evidence(output, max_file_bytes=4096, max_total_bytes=8192)
    original = store._prepare_reads
    store._prepare_reads = original

    def fail(*args, **kwargs):
        raise RuntimeError("profiled capture failed")

    monkeypatch.setattr(profile, "capture_benchmark", fail)
    with pytest.raises(RuntimeError, match="profiled capture failed"):
        inspect_benchmarks(store, Ledger(), [source] * 100, read_workers=1,
                           profile_output=output / "profile.json")
    assert store._prepare_reads is original
    assert store.inspection_timings is None
    assert "read" not in store.__dict__
    assert not (output / "profile.json").exists()


def test_negative_cache_capacity_and_hits_preserve_fifo_provenance(tmp_path):
    output = tmp_path / "output"
    output.mkdir()
    store = Evidence(output, max_file_bytes=4096, max_total_bytes=8192)
    body = b'{"value_ms":null,"unique":1}'
    first, hit, reread, novel = [tmp_path / name for name in ("first.json", "hit.json", "reread.json", "novel.json")]
    for path in (first, hit, reread):
        path.write_bytes(body)
    novel.write_bytes(b'{"value_ms":null,"unique":2}')
    assert capture_benchmark(store, first) is None
    initial_sha = store.sources[str(first)]["sha256"]
    for index in range(8191):
        store.benchmark_negatives[f"unused-{index}"] = ("parsed_no_reported_timing_fields", True, str(first))
    assert capture_benchmark(store, hit) is None
    assert store.sources[str(hit)]["classification_source_path"] == str(first)
    assert next(iter(store.benchmark_negatives)) == initial_sha  # A hit does not refresh FIFO age.
    assert capture_benchmark(store, novel) is None
    assert len(store.benchmark_negatives) == 8192
    assert initial_sha not in store.benchmark_negatives
    assert next(iter(store.benchmark_negatives)) == "unused-0"
    assert capture_benchmark(store, reread) is None
    assert store.sources[str(reread)]["json_parsed"] is True
    assert "classification_source_path" not in store.sources[str(reread)]
    assert len(store.benchmark_negatives) == 8192
    assert next(iter(store.benchmark_negatives)) == "unused-1"


def test_read_batch_budget_changes_transport_only(tmp_path):
    source = tmp_path / "large.json"
    body = b'{"seconds":1,"padding":"' + b'x' * (3 << 20) + b'"}'
    source.write_bytes(body)
    results, reads = [], []
    for mib in (2, 8):
        output = tmp_path / str(mib)
        output.mkdir()
        store = Evidence(output, max_file_bytes=8 << 20, max_total_bytes=16 << 20)
        ledger = Ledger()
        metrics, catalogue = inspect_benchmarks(store, ledger, [source], read_workers=1,
                                               read_batch_bytes=mib << 20)
        results.append((metrics, catalogue, store.sources, store.total_bytes,
                        {p.name: p.read_bytes() for p in (output / "evidence").iterdir()}))
        reads.append(store.read_stats)
    assert results[0] == results[1]
    assert reads[0]["synchronous_reads"] == 1 and not reads[0]["prefetched_reads"]
    assert reads[1]["prefetched_reads"] == 1 and not reads[1]["synchronous_reads"]

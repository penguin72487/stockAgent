from pathlib import Path
from types import SimpleNamespace

import pytest

from downloader.artifact_io import atomic_write_json
from scripts import verify_taifex_column_cache_runtime as verifier


@pytest.fixture
def base_delta_acceptance(tmp_path, monkeypatch):
    root = tmp_path / "repo"
    source, fixture = root / "source", root / "fixture"
    source.mkdir(parents=True)
    fixture.mkdir()
    paths = [root / name for name in ("cache.py", "dashboard.py", "selection.py", "segments.py",
        "scripts/benchmark_taifex_history_segments.py", "scripts/benchmark_taifex_history_selection.py")]
    for path in paths:
        path.parent.mkdir(exist_ok=True)
        path.write_text("# accepted source\n")
    monkeypatch.setattr(verifier, "ROOT", root)
    monkeypatch.setattr(verifier, "SOURCE", source)
    monkeypatch.setattr(verifier, "cache", SimpleNamespace(__file__=str(paths[0])))
    keys = ("1h", "1d", "1w", "1mo", "1q", "1y", "all")
    monkeypatch.setattr(verifier, "dashboard", SimpleNamespace(__file__=str(paths[1]),
        HISTORY_RANGE_SECONDS=dict.fromkeys(keys)))
    monkeypatch.setattr(verifier, "selection", SimpleNamespace(__file__=str(paths[2])))
    monkeypatch.setattr(verifier, "segments", SimpleNamespace(__file__=str(paths[3])))
    atomic_write_json(fixture / "fixture.json", {"source": str(source)})
    original = root / "original_column_cache.py"
    original.write_text("# preserved original\n")
    control = root / "control.json"
    expected = dict.fromkeys(keys, "a" * 64)
    atomic_write_json(control, {"fixture": str(fixture), "exact_output_parity": True,
        "trials": [{"output_sha256": expected}]})
    metrics = {"baseline": {"wall_seconds": 10, "cpu_seconds": 20, "rss_peak_bytes": 100000,
                            "parquet_written_bytes": 30000000, "warm_wall_seconds": 1, "warm_cpu_seconds": 2},
               "candidate": {"wall_seconds": 8, "cpu_seconds": 14, "rss_peak_bytes": 90000,
                             "parquet_written_bytes": 16000, "warm_wall_seconds": 1, "warm_cpu_seconds": 2}}
    rows = [{"variant": variant, **metrics[variant], "threads": 4,
        "allocator_conf": "dirty_decay_ms:500," + verifier.ALLOCATOR_CONF,
        "output_sha256": expected, "warm_exact_output_parity": True,
        "warm_parquet_written_bytes": 0, "warm_sink_calls": 0,
        "provider_calls": 0, "model_forward_calls": 0, "production_writes": False,
        "ranges": [{"range": key, "rows": 69, "json_bytes": 1000} for key in keys]}
        for variant in ["baseline", "candidate", "candidate", "baseline"] * 2]
    value = {"contract": verifier.BASE_DELTA_CONTRACT, "fixture": str(fixture),
        "independent_control": {"path": str(control), "sha256": verifier.sha(control)},
        "seed": {"fixture": str(fixture), "source_sha256": {"source": "b" * 64},
                 "original_code_sha256": verifier.sha(original)},
        "source_sha256": {"source": "b" * 64}, "source_unchanged": True, "balanced_abba": True,
        "model_forward_calls": 0, "provider_calls": 0, "production_writes": False,
        "code_stable": True, "exact_output_parity": True,
        "medians": metrics, "trials": rows, "code_sha256": {str(path): verifier.sha(path) for path in paths}}
    acceptance = root / "measurement.json"
    return acceptance, value


def test_base_delta_requires_actual_balanced_full_workflow_and_current_code(base_delta_acceptance):
    path, value = base_delta_acceptance
    atomic_write_json(path, value)
    assert verifier.accepted_code(path) == value["code_sha256"]
    for field, damage in (("balanced_abba", False), ("source_unchanged", False),
                          ("model_forward_calls", 1), ("trials", value["trials"][:-1])):
        atomic_write_json(path, {**value, field: damage})
        with pytest.raises(ValueError, match="accepted_measurement"):
            verifier.accepted_code(path)


@pytest.mark.parametrize("damage", ["warm_write", "source", "range", "summary", "order", "speed", "original"])
def test_base_delta_rejects_incomplete_or_unbound_claims(base_delta_acceptance, damage):
    path, base = base_delta_acceptance
    import json

    value = json.loads(json.dumps(base))
    if damage == "warm_write":
        value["trials"][1]["warm_parquet_written_bytes"] = 1
    elif damage == "source":
        value["source_sha256"] = {}
    elif damage == "range":
        value["trials"][1]["ranges"] = value["trials"][1]["ranges"][:-1]
    elif damage == "summary":
        value["medians"]["candidate"]["cpu_seconds"] = 1
    elif damage == "order":
        value["trials"][0], value["trials"][1] = value["trials"][1], value["trials"][0]
    elif damage == "speed":
        value["medians"]["candidate"]["wall_seconds"] = 100
        for row in value["trials"]:
            if row["variant"] == "candidate":
                row["wall_seconds"] = 100
    else:
        (path.parent / "original_column_cache.py").write_text("# changed original\n")
    atomic_write_json(path, value)
    with pytest.raises(ValueError, match="accepted_measurement"):
        verifier.accepted_code(path)


def test_native_segment_observation_requires_real_pinned_inodes(tmp_path, monkeypatch):
    import os

    part = tmp_path / "part.parquet"
    part.write_bytes(b"known engineering inode")
    fd = os.open(part, os.O_RDONLY)
    try:
        info = os.fstat(fd)
        columns = [{"payloads": [{"device": info.st_dev, "inode": info.st_ino}]}]
        monkeypatch.setattr(verifier, "observe_apis", lambda: {"ranges": "seven"})
        result = verifier.observe_segmented_apis({"MainPID": str(os.getpid())}, columns)
        assert result["native_payload_inodes"] == [[info.st_dev, info.st_ino]]
        assert result["native_new_source_update_observed"] is False
    finally:
        os.close(fd)
    with pytest.raises(ValueError, match="no_verified_segment"):
        verifier.observe_segmented_apis({"MainPID": str(os.getpid())}, [])


def test_acceptance_binds_exact_full_output_current_code_and_real_owner_source(tmp_path, monkeypatch):
    root = tmp_path / "repo"
    source = root / "source"
    source.mkdir(parents=True)
    helper, projector, selector = root / "helper.py", root / "projector.py", root / "selector.py"
    benchmark = root / "scripts/benchmark_taifex_history_columns.py"
    benchmark.parent.mkdir()
    for path in (helper, projector, selector, benchmark):
        path.write_text("# accepted code\n")
    monkeypatch.setattr(verifier, "ROOT", root)
    monkeypatch.setattr(verifier, "SOURCE", source)
    monkeypatch.setattr(verifier, "cache", SimpleNamespace(__file__=str(helper)))
    monkeypatch.setattr(verifier, "dashboard", SimpleNamespace(__file__=str(projector)))
    monkeypatch.setattr(verifier, "selection", SimpleNamespace(__file__=str(selector)))
    fixture = root / "fixture"
    fixture.mkdir()
    atomic_write_json(fixture / "fixture.json", {"source": str(source)})
    hashes = {str(path): verifier.sha(path) for path in (helper, projector, selector, benchmark)}
    receipt = root / "accepted.json"
    value = {"contract": "taifex_history_complete_curve_comparison_v1", "exact_output_parity": True,
             "code_stable": True, "production_writes": False, "provider_calls": 0,
             "code_sha256": hashes, "source_fixture": str(fixture),
             "medians": {"baseline": {"cpu_seconds": 20, "rss_peak_bytes": 100},
                         "candidate": {"cpu_seconds": 4, "rss_peak_bytes": 20}}}
    atomic_write_json(receipt, value)
    assert verifier.accepted_code(receipt) == hashes
    for field, damaged in (("exact_output_parity", False), ("provider_calls", 1)):
        atomic_write_json(receipt, {**value, field: damaged})
        with pytest.raises(ValueError, match="accepted_measurement"):
            verifier.accepted_code(receipt)
    atomic_write_json(receipt, value)
    helper.write_text("# different generation\n")
    with pytest.raises(ValueError, match="accepted_measurement"):
        verifier.accepted_code(receipt)


def test_thread_acceptance_requires_balanced_complete_controls_current_code_and_gain(tmp_path, monkeypatch):
    root = tmp_path / "repo"
    source, fixture = root / "source", root / "fixture"
    source.mkdir(parents=True)
    fixture.mkdir()
    paths = [root / name for name in ("helper.py", "projector.py", "selector.py",
        "scripts/benchmark_taifex_curve_threads.py", "scripts/benchmark_taifex_history_selection.py")]
    for path in paths:
        path.parent.mkdir(exist_ok=True)
        path.write_text("# current implementation\n")
    monkeypatch.setattr(verifier, "ROOT", root)
    monkeypatch.setattr(verifier, "SOURCE", source)
    monkeypatch.setattr(verifier, "cache", SimpleNamespace(__file__=str(paths[0])))
    monkeypatch.setattr(verifier, "dashboard", SimpleNamespace(__file__=str(paths[1]),
                        HISTORY_RANGE_SECONDS={key: 0 for key in ("1h", "1d", "1w", "1mo", "1q", "1y", "all")}))
    monkeypatch.setattr(verifier, "selection", SimpleNamespace(__file__=str(paths[2])))
    atomic_write_json(fixture / "fixture.json", {"source": str(source)})
    control, receipt = root / "control.json", root / "acceptance.json"
    expected = {key: "a"*64 for key in verifier.dashboard.HISTORY_RANGE_SECONDS}
    atomic_write_json(control, {"fixture": str(fixture), "exact_output_parity": True,
                                "trials": [{"output_sha256": expected}]})
    order = [16, 4, 4, 16]*2
    hashes = {str(path): verifier.sha(path) for path in paths}
    value = {"contract": "taifex_complete_seven_curve_thread_comparison_v1", "fixture": str(fixture),
        "source_unchanged": True, "balanced_abba": True, "trial_order": order,
        "trials": [{"threads": count, "variant": "window", "output_sha256": expected} for count in order],
        "independent_control": {"path": str(control), "sha256": verifier.sha(control)},
        "code_sha256": hashes, "exact_output_parity": True, "code_stable": True,
        "production_writes": False, "provider_calls": 0, "model_forward_calls": 0,
        "medians": {"16": {"cpu_seconds": 20, "wall_seconds": 10, "rss_peak_bytes": 100},
                    "4": {"cpu_seconds": 10, "wall_seconds": 10, "rss_peak_bytes": 80}}}
    atomic_write_json(receipt, value)
    assert verifier.accepted_code(receipt) == hashes
    for field, changed in (("balanced_abba", False), ("source_unchanged", False),
                           ("model_forward_calls", 1), ("trials", value["trials"][:-1])):
        atomic_write_json(receipt, {**value, field: changed})
        with pytest.raises(ValueError, match="accepted_measurement"):
            verifier.accepted_code(receipt)
    atomic_write_json(receipt, {**value, "medians": {**value["medians"], "4": {
        **value["medians"]["4"], "wall_seconds": 12}}})
    with pytest.raises(ValueError, match="accepted_measurement"):
        verifier.accepted_code(receipt)
    atomic_write_json(receipt, value)
    paths[2].write_text("# unverified selector change\n")
    with pytest.raises(ValueError, match="accepted_measurement"):
        verifier.accepted_code(receipt)


@pytest.fixture
def allocator_acceptance(tmp_path, monkeypatch):
    root = tmp_path / "repo"
    source, fixture = root / "source", root / "fixture"
    source.mkdir(parents=True)
    fixture.mkdir()
    paths = [root / name for name in ("helper.py", "projector.py", "selector.py",
        "scripts/benchmark_taifex_allocator_idle.py", "scripts/benchmark_taifex_history_selection.py",
        "polars/__init__.py")]
    for path in paths:
        path.parent.mkdir(exist_ok=True)
        path.write_text("# measured implementation\n")
    monkeypatch.setattr(verifier, "ROOT", root)
    monkeypatch.setattr(verifier, "SOURCE", source)
    monkeypatch.setattr(verifier, "cache", SimpleNamespace(__file__=str(paths[0])))
    monkeypatch.setattr(verifier, "dashboard", SimpleNamespace(__file__=str(paths[1]),
                        HISTORY_RANGE_SECONDS={key: 0 for key in ("1h", "1d", "1w", "1mo", "1q", "1y", "all")}))
    monkeypatch.setattr(verifier, "selection", SimpleNamespace(__file__=str(paths[2])))
    monkeypatch.setattr(verifier, "pl", SimpleNamespace(__file__=str(paths[-1])))
    atomic_write_json(fixture / "fixture.json", {"source": str(source)})
    control, receipt = root / "control.json", root / "acceptance.json"
    expected = {key: "a"*64 for key in verifier.dashboard.HISTORY_RANGE_SECONDS}
    atomic_write_json(control, {"fixture": str(fixture), "exact_output_parity": True,
                                "trials": [{"output_sha256": expected}]})
    order = ["baseline", "decay", "decay", "baseline"]*2
    metrics = {"wall_seconds": 10., "cpu_seconds": 12., "rss_peak_bytes": 100.,
               "idle_rss_bytes": 90., "idle_cpu_seconds": .1,
               "warm_read_wall_seconds": .2, "warm_read_cpu_seconds": .3}
    medians = {"baseline": metrics, "decay": {**metrics, "idle_rss_bytes": 15., "warm_read_wall_seconds": .26}}
    hashes = {str(path): verifier.sha(path) for path in paths}
    trials = [{**medians[variant], "threads": 4, "variant": "window", "allocator_variant": variant,
               "allocator_conf": verifier.ALLOCATOR_CONF if variant == "decay" else "muzzy_decay_ms:-1",
               "output_sha256": expected.copy(), "warm_read_exact_output_parity": True,
               "idle_samples": [{"elapsed_seconds": elapsed, "rss_bytes": medians[variant]["idle_rss_bytes"]}
                                for elapsed in (0., 1., 3., 10.)]} for variant in order]
    value = {"contract": verifier.ALLOCATOR_CONTRACT, "fixture": str(fixture),
             "source_unchanged": True, "balanced_abba": True, "trial_order": order,
             "decay_conf": verifier.ALLOCATOR_CONF, "trials": trials, "medians": medians,
             "independent_control": {"path": str(control), "sha256": verifier.sha(control)},
             "code_sha256": hashes, "exact_output_parity": True, "code_stable": True,
             "production_writes": False, "provider_calls": 0, "model_forward_calls": 0}
    atomic_write_json(receipt, value)
    return receipt, value, paths


def test_allocator_acceptance_requires_complete_idle_warm_output_and_installed_version(allocator_acceptance):
    receipt, value, paths = allocator_acceptance
    assert verifier.accepted_code(receipt) == value["code_sha256"]
    paths[-1].write_text("# different installed allocator\n")
    with pytest.raises(ValueError, match="accepted_measurement"):
        verifier.accepted_code(receipt)


@pytest.fixture
def source_proof_acceptance(tmp_path, monkeypatch):
    root, fixture = tmp_path / "repo", tmp_path / "fixture"
    source = root / "source"
    source.mkdir(parents=True)
    (fixture / "state").mkdir(parents=True)
    frozen_source = fixture / "state/marks.jsonl"
    frozen_source.write_bytes(b"source\n")
    size = frozen_source.stat().st_size
    names = ("helper.py", "projector.py", "selector.py",
             "scripts/benchmark_taifex_source_proof_memo.py",
             "scripts/benchmark_taifex_history_selection.py",
             "scripts/benchmark_taifex_history_columns.py", "downloader/artifact_io.py",
             "polars/__init__.py")
    paths = [root / name for name in names] + [fixture.parent / "original_column_cache.py"]
    for path in paths:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("# measured implementation\n")
    monkeypatch.setattr(verifier, "ROOT", root)
    monkeypatch.setattr(verifier, "SOURCE", source)
    monkeypatch.setattr(verifier, "cache", SimpleNamespace(__file__=str(paths[0])))
    keys = ("1h", "1d", "1w", "1mo", "1q", "1y", "all")
    monkeypatch.setattr(verifier, "dashboard", SimpleNamespace(__file__=str(paths[1]),
                                                              HISTORY_RANGE_SECONDS=dict.fromkeys(keys, 0)))
    monkeypatch.setattr(verifier, "selection", SimpleNamespace(__file__=str(paths[2])))
    monkeypatch.setattr(verifier, "pl", SimpleNamespace(__file__=str(paths[7])))
    atomic_write_json(fixture / "fixture.json", {"source": str(source)})
    atomic_write_json(fixture.parent / "original-code-proof.json", {"sha256": verifier.sha(paths[-1])})
    expected = dict.fromkeys(keys, "a" * 64)
    raw_path = fixture.parent / "raw.json"
    atomic_write_json(raw_path, {"variant": "baseline", "range": "1d", "provider_calls": 0,
                                "production_writes": False, "output_sha256": expected["1d"]})
    order = ["baseline", "candidate", "candidate", "baseline"] * 2
    trials = []
    for index, variant in enumerate(order):
        proofs = [{"path": str(frozen_source), "kind": "build", "bytes": size}]
        if variant == "baseline":
            proofs.append({"path": str(frozen_source), "kind": "verify", "bytes": size})
        trials.append({"trial": index, "phase": "update", "variant": variant,
            "threads": 4, "output_sha256": expected.copy(), "provider_calls": 0,
            "model_forward_calls": 0, "production_writes": False,
            "durable_complete_readback": True, "warm_exact_output_parity": True,
            "proofs": proofs, "proof_bytes": sum(row["bytes"] for row in proofs),
            "warm_proof_bytes": 0, "wall_seconds": 10. if variant == "baseline" else 9.,
            "cpu_seconds": 20. if variant == "baseline" else 16., "rss_peak_bytes": 100.,
            "warm_wall_seconds": 1., "warm_cpu_seconds": 1.,
            "ranges": [{"range": key, "rows": 1, "json_bytes": 100} for key in keys]})
    metrics = ("wall_seconds", "cpu_seconds", "rss_peak_bytes", "proof_bytes",
               "warm_wall_seconds", "warm_cpu_seconds", "warm_proof_bytes")
    medians = {variant: {metric: trials[index][metric] for metric in metrics}
               for index, variant in enumerate(("baseline", "candidate"))}
    value = {"contract": verifier.SOURCE_PROOF_CONTRACT, "fixture": str(fixture),
        "trials": trials, "trial_order": order, "medians": medians, "balanced_abba": True,
        "source_unchanged": True, "code_stable": True, "exact_output_parity": True,
        "model_forward_calls": 0, "provider_calls": 0, "production_writes": False,
        "code_sha256": {str(path): verifier.sha(path) for path in paths},
        "independent_raw_1d": {"path": str(raw_path), "sha256": verifier.sha(raw_path),
                               "output_sha256": expected["1d"]}}
    receipt = root / "acceptance.json"
    atomic_write_json(receipt, value)
    return receipt, value, paths


def test_source_proof_acceptance_binds_complete_trials_original_raw_code_and_gain(source_proof_acceptance):
    receipt, value, paths = source_proof_acceptance
    assert verifier.accepted_code(receipt) == value["code_sha256"]
    paths[-1].write_text("# changed original control\n")
    with pytest.raises(ValueError, match="accepted_measurement"):
        verifier.accepted_code(receipt)


@pytest.mark.parametrize("damage", ["missing_trial", "summary_only", "no_source_proof",
    "missing_range", "wrong_threads", "warm_hash", "raw_mismatch", "provider_call",
    "durable_failure", "reordered", "warm_regression"])
def test_source_proof_acceptance_rejects_partial_or_unsupported_measurements(source_proof_acceptance, damage):
    receipt, value, _ = source_proof_acceptance
    candidate = value["trials"][1]
    if damage == "missing_trial":
        value["trials"].pop()
    elif damage == "summary_only":
        value["medians"]["candidate"]["cpu_seconds"] = 1.
    elif damage == "no_source_proof":
        candidate["proofs"].clear()
    elif damage == "missing_range":
        candidate["ranges"].pop()
    elif damage == "wrong_threads":
        candidate["threads"] = 16
    elif damage == "warm_hash":
        candidate["warm_proof_bytes"] = 1
    elif damage == "raw_mismatch":
        value["independent_raw_1d"]["output_sha256"] = "b" * 64
    elif damage == "provider_call":
        candidate["provider_calls"] = 1
    elif damage == "durable_failure":
        candidate["durable_complete_readback"] = False
    elif damage == "reordered":
        value["trials"][0], value["trials"][1] = value["trials"][1], value["trials"][0]
    elif damage == "warm_regression":
        for row in value["trials"]:
            if row["variant"] == "candidate":
                row["warm_cpu_seconds"] = 100.
        value["medians"]["candidate"]["warm_cpu_seconds"] = 100.
    atomic_write_json(receipt, value)
    with pytest.raises(ValueError, match="accepted_measurement"):
        verifier.accepted_code(receipt)


@pytest.mark.parametrize("damage", ["missing_trial", "summary_only", "short_idle", "wrong_pool",
    "wrong_allocator", "warm_parity", "partial_curve", "idle_memory_gain", "whole_wall_regression", "warm_read_delay"])
def test_allocator_rejects_partial_unproven_or_excessive_cost_measurements(allocator_acceptance, damage):
    receipt, value, _ = allocator_acceptance
    candidate = value["trials"][1]
    if damage == "missing_trial":
        value["trials"].pop()
    elif damage == "summary_only":
        value["medians"]["decay"]["idle_rss_bytes"] = 1.
    elif damage == "short_idle":
        candidate["idle_samples"][-1]["elapsed_seconds"] = 1.
    elif damage == "wrong_pool":
        candidate["threads"] = 16
    elif damage == "wrong_allocator":
        candidate["allocator_conf"] = "muzzy_decay_ms:-1"
    elif damage == "warm_parity":
        candidate["warm_read_exact_output_parity"] = False
    elif damage == "partial_curve":
        candidate["output_sha256"].pop("1y")
    else:
        metric, changed = {"idle_memory_gain": ("idle_rss_bytes", 89.),
                           "whole_wall_regression": ("wall_seconds", 12.),
                           "warm_read_delay": ("warm_read_wall_seconds", .6)}[damage]
        value["medians"]["decay"][metric] = changed
        for row in value["trials"]:
            if row["allocator_variant"] == "decay":
                row[metric] = changed
                if metric == "idle_rss_bytes":
                    row["idle_samples"][-1]["rss_bytes"] = changed
    atomic_write_json(receipt, value)
    with pytest.raises(ValueError, match="accepted_measurement"):
        verifier.accepted_code(receipt)


@pytest.fixture
def runtime(tmp_path, monkeypatch):
    owner = {"ActiveState": "active", "InvocationID": "before", "MainPID": "1"}
    protected = {name: {"InvocationID": name, "MainPID": "2", "ActiveState": "active"} for name in verifier.PROTECTED}
    monkeypatch.setattr(verifier, "identity", lambda name: owner.copy() if name == verifier.UNIT else protected[name].copy())
    monkeypatch.setattr(verifier, "accepted_code", lambda _: {"helper": "fixed"})
    monkeypatch.setattr(verifier, "sha", lambda _: "fixed-launcher")
    enabled = [False]
    monkeypatch.setattr(verifier, "owner_environment", lambda _: (tmp_path, enabled[0]))
    column = {"path": "private-columns.json", "receipt_mtime_ns": 1, "rows": 10}
    monkeypatch.setattr(verifier, "cache_readback", lambda _: [column.copy()])
    monkeypatch.setattr(verifier.dashboard, "build_dashboard_history_snapshot", lambda **_: {"history": [{"price": 1}]})
    calls = []
    monkeypatch.setattr(verifier, "observe_apis", lambda: calls.append("readonly-api") or {"ranges": "all-seven", "strict_data_ready": False})
    monkeypatch.setenv(verifier.cache.CACHE_ENV, "unchanged")
    monkeypatch.setenv(verifier.cache.ENABLE_ENV, "0")
    baseline = tmp_path / "prewarm.json"
    verifier.main(["--acceptance", "unused", "--output", str(baseline)])
    assert verifier.metadata(baseline)["state"] == "isolated_prewarm_verified"
    assert calls == []
    enabled[0], owner["InvocationID"], column["receipt_mtime_ns"] = True, "after", 2
    return baseline, owner, protected, column, calls, enabled


def test_runtime_requires_new_owner_real_column_refresh_and_untouched_trading_owners(runtime, tmp_path):
    baseline, _, _, _, calls, _ = runtime
    output = tmp_path / "verified.json"
    verifier.main(["--acceptance", "unused", "--baseline", str(baseline), "--output", str(output)])
    value = verifier.metadata(output)
    assert value["state"] == "deployed_physical_cache_verified"
    assert value["api"]["strict_data_ready"] is False
    assert value["source_mutations"] is False and value["service_restart_requested"] is False
    assert value["provider_calls"] == value["gpu_calls"] == 0
    assert calls == ["readonly-api"]


@pytest.mark.parametrize("damage", [None, "old_refresh", "newer_code"])
def test_source_proof_runtime_requires_refresh_after_new_process_and_older_bound_code(
    runtime, tmp_path, monkeypatch, damage
):
    baseline, _, _, column, _, _ = runtime
    root = tmp_path / "repo"
    code = root / "scripts/benchmark_taifex_source_proof_memo.py"
    code.parent.mkdir(parents=True)
    code.write_text("# bound code\n")
    start = code.stat().st_mtime_ns + 1
    column["receipt_mtime_ns"] = start + 1
    hashes = {str(code): "fixed"}
    value = verifier.metadata(baseline)
    value.update(code_sha256=hashes, prewarm_polars_threads=4,
                 prewarm_allocator_conf=verifier.ALLOCATOR_CONF)
    atomic_write_json(baseline, value)
    monkeypatch.setattr(verifier, "ROOT", root)
    monkeypatch.setattr(verifier, "accepted_code", lambda _: hashes)
    monkeypatch.setattr(verifier, "owner_polars_limit", lambda _: 4)
    monkeypatch.setattr(verifier, "owner_allocator_override", lambda _: verifier.ALLOCATOR_CONF)
    monkeypatch.setattr(verifier, "owner_start_wall_ns", lambda _: start)
    if damage == "old_refresh":
        column["receipt_mtime_ns"] = start - 1
    elif damage == "newer_code":
        start = code.stat().st_mtime_ns - 1
    output = tmp_path / "readback.json"
    args = ["--acceptance", "unused", "--baseline", str(baseline), "--output", str(output)]
    if damage is None:
        verifier.main(args)
        assert verifier.metadata(output)["owner_start_wall_ns"] == start
    else:
        with pytest.raises(ValueError, match="actual_new_owner|newer_than_display"):
            verifier.main(args)
        assert verifier.metadata(output)["state"] == "failed_readback"


@pytest.mark.parametrize("damage", [None, "changed_owner", "future_start", "no_start"])
def test_owner_start_readback_fences_exact_systemd_generation(monkeypatch, damage):
    owner = {"MainPID": "123", "InvocationID": "current"}
    fields = {**owner, "ExecMainStartTimestampMonotonic": "500"}
    if damage == "changed_owner":
        fields["InvocationID"] = "other"
    elif damage == "future_start":
        fields["ExecMainStartTimestampMonotonic"] = "1500"
    elif damage == "no_start":
        fields["ExecMainStartTimestampMonotonic"] = "0"
    monkeypatch.setattr(verifier.subprocess, "run", lambda *args, **kwargs:
                        SimpleNamespace(stdout="\n".join(f"{key}={value}" for key, value in fields.items())))
    monkeypatch.setattr(verifier.time, "monotonic_ns", lambda: 1_000_000)
    monkeypatch.setattr(verifier.time, "time_ns", lambda: 10_000_000)
    if damage is None:
        assert verifier.owner_start_wall_ns(owner) == 9_500_000
    else:
        with pytest.raises(ValueError, match="owner_changed|start_time"):
            verifier.owner_start_wall_ns(owner)


@pytest.mark.parametrize("pool", [4, 16, None])
def test_thread_runtime_requires_loaded_role_specific_pool(runtime, tmp_path, monkeypatch, pool):
    baseline, _, _, _, _, _ = runtime
    value = verifier.metadata(baseline)
    hashes = {str(verifier.ROOT / "scripts/benchmark_taifex_curve_threads.py"): "fixed"}
    value["code_sha256"] = hashes
    atomic_write_json(baseline, value)
    monkeypatch.setattr(verifier, "accepted_code", lambda _: hashes)
    monkeypatch.setattr(verifier, "owner_polars_limit", lambda _: pool)
    output = tmp_path / "readback.json"
    if pool == 4:
        verifier.main(["--acceptance", "unused", "--baseline", str(baseline), "--output", str(output)])
        assert verifier.metadata(output)["owner_polars_max_threads"] == 4
    else:
        with pytest.raises(ValueError, match="thread_pool"):
            verifier.main(["--acceptance", "unused", "--baseline", str(baseline), "--output", str(output)])
        assert verifier.metadata(output)["state"] == "failed_readback"


@pytest.mark.parametrize("setting", [verifier.ALLOCATOR_CONF, "muzzy_decay_ms:-1", None])
def test_allocator_runtime_requires_real_new_owner_setting_and_measured_prewarm(runtime, tmp_path, monkeypatch, setting):
    baseline, _, _, _, _, _ = runtime
    value = verifier.metadata(baseline)
    hashes = {str(verifier.ROOT / "scripts/benchmark_taifex_allocator_idle.py"): "fixed"}
    value.update(code_sha256=hashes, prewarm_polars_threads=4,
                 prewarm_allocator_conf="dirty_decay_ms:500,muzzy_decay_ms:-1," + verifier.ALLOCATOR_CONF)
    atomic_write_json(baseline, value)
    monkeypatch.setattr(verifier, "accepted_code", lambda _: hashes)
    monkeypatch.setattr(verifier, "owner_polars_limit", lambda _: 4)
    monkeypatch.setattr(verifier, "owner_allocator_override", lambda _: setting)
    output = tmp_path / "allocator-readback.json"
    if setting == verifier.ALLOCATOR_CONF:
        verifier.main(["--acceptance", "unused", "--baseline", str(baseline), "--output", str(output)])
        assert verifier.metadata(output)["owner_allocator_override"] == verifier.ALLOCATOR_CONF
    else:
        with pytest.raises(ValueError, match="measured_allocator"):
            verifier.main(["--acceptance", "unused", "--baseline", str(baseline), "--output", str(output)])
        assert verifier.metadata(output)["state"] == "failed_readback"


@pytest.mark.parametrize("damage", ["old_owner", "no_refresh", "protected_owner", "disabled"])
def test_bad_runtime_writes_failed_evidence_and_does_not_claim_deployment(runtime, tmp_path, damage):
    baseline, owner, protected, column, _, enabled = runtime
    if damage == "old_owner":
        owner["InvocationID"] = "before"
    elif damage == "no_refresh":
        column["receipt_mtime_ns"] = 1
    elif damage == "disabled":
        enabled[0] = False
    else:
        protected[verifier.PROTECTED[0]]["InvocationID"] = "unexpected-restart"
    output = tmp_path / f"failed-{damage}.json"
    with pytest.raises(ValueError):
        verifier.main(["--acceptance", "unused", "--baseline", str(baseline), "--output", str(output)])
    assert verifier.metadata(output)["state"] == "failed_readback"


def test_launcher_opts_in_only_the_readonly_display_and_keeps_disable_override():
    launcher = Path(verifier.LAUNCHER).read_text()
    assert '${STOCKAGENT_TAIFEX_HISTORY_COLUMN_CACHE:-1}' in launcher
    assert "serve_shioaji_taifex_simulation_dashboard.py" in launcher
    assert "stream_shioaji" not in launcher
    assert '${POLARS_MAX_THREADS:-${STOCKAGENT_TAIFEX_DASHBOARD_POLARS_THREADS:-4}}' in launcher
    assert '${_RJEM_MALLOC_CONF:-${STOCKAGENT_TAIFEX_DASHBOARD_ALLOCATOR_CONF:-muzzy_decay_ms:1000}}' in launcher


@pytest.mark.parametrize("lag", [60, 3600])
def test_apis_keep_strict_strategy_gap_visible_and_reject_stale_generations(monkeypatch, lag):
    from datetime import datetime, timedelta, timezone

    now = datetime(2026, 10, 13, 18, tzinfo=timezone.utc)
    source = now.isoformat()
    end = (now-timedelta(seconds=lag)).isoformat()
    rows = [{"strategy_id": "a", "decision_ts_ns": int((now-timedelta(seconds=lag)).timestamp()*1e9)},
            {"strategy_id": "b", "decision_ts_ns": 1}]
    monkeypatch.setattr(verifier, "fetch_json", lambda url: (
        {"range": url.split("range=")[-1], "history": rows, "coverage_end_utc": end, "anchor_at_utc": end}
        if "history" in url else {"source_updated_at_utc": source, "health": "degraded",
                                    "simulation_only": True, "production_order_possible": False,
                                    "strategies": [{"strategy_id": "a"}, {"strategy_id": "b"}]}, 1))
    if lag == 3600:
        with pytest.raises(ValueError, match="caught_up"):
            verifier.observe_apis()
    else:
        value = verifier.observe_apis()
        assert value["strict_data_ready"] is False and len(value["ranges"]) == 7
        assert all("current strategy endpoint" in row["strict_data_error"] for row in value["ranges"])

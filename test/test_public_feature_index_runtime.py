import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from downloader.artifact_io import atomic_write_json
from scripts import verify_public_feature_index_runtime as verifier


@pytest.fixture
def runtime(tmp_path, monkeypatch):
    owner = {"ActiveState": "active", "InvocationID": "before", "MainPID": "1"}
    protected = {unit: {"InvocationID": unit, "MainPID": "2"} for unit in verifier.PROTECTED}
    monkeypatch.setattr(verifier, "identity", lambda unit: owner.copy() if unit == verifier.UNIT else protected[unit].copy())
    monkeypatch.setattr(verifier, "check_reader", lambda _: None)
    monkeypatch.setattr(verifier, "accepted_measurement", lambda _: {"current": "code"})
    monkeypatch.setattr(verifier, "snapshot", lambda: ((1, 2, 3, 4, 5), "source-hash", SimpleNamespace(rows=[1, 2])))
    monkeypatch.setattr(verifier, "observe_live_pages", lambda: {"rows": 2, "exact_bytes": True})
    baseline = tmp_path / "baseline.json"
    verifier.main(["--acceptance", "unused", "--output", str(baseline)])
    owner["InvocationID"] = "after"
    return baseline, owner, protected


def test_readback_requires_new_reader_and_keeps_actual_trading_source_owners(runtime, tmp_path):
    baseline, _, _ = runtime
    output = tmp_path / "readback.json"
    verifier.main(["--acceptance", "unused", "--baseline", str(baseline), "--output", str(output)])
    value = json.loads(output.read_bytes())
    assert value["state"] == "actual_gateway_feature_pages_verified"
    assert value["gpu_calls"] == value["provider_calls"] == 0
    assert value["source_mutations"] is value["service_restart_requested"] is False


@pytest.mark.parametrize("damage", ["old_owner", "changed_protected", "unaccepted_code"])
def test_readback_preserves_failed_evidence_and_cannot_claim_changed_owners(runtime, tmp_path, damage):
    baseline, owner, protected = runtime
    if damage == "old_owner":
        owner["InvocationID"] = "before"
    elif damage == "changed_protected":
        protected[verifier.PROTECTED[0]]["InvocationID"] = "changed"
    else:
        value = json.loads(baseline.read_bytes())
        value["code_and_input_sha256"] = {"wrong": "generation"}
        atomic_write_json(baseline, value)
    output = tmp_path / "failed.json"
    with pytest.raises(ValueError, match="bound_to_baseline"):
        verifier.main(["--acceptance", "unused", "--baseline", str(baseline), "--output", str(output)])
    assert json.loads(output.read_bytes())["state"] == "failed_readback"


def test_http_matches_original_full_live_oracle_and_retries_source_swaps(tmp_path, monkeypatch):
    source = tmp_path / "features.json"
    source.write_text("source")
    monkeypatch.setattr(verifier, "SOURCE", source)
    index = SimpleNamespace(rows=[1, 2], revision="a"*32, page=lambda **query: {"query": query, "value": -0.0})
    current = verifier.metadata_signature(source.stat())
    signatures = iter(((0, 0, 0, 0, 0), current))
    monkeypatch.setattr(verifier, "snapshot", lambda: (next(signatures), "source-hash", index))
    def response(query):
        query = {**query}
        if "q" in query:
            query["search"] = query.pop("q")
        if "revision" in query:
            query["requested_revision"] = query.pop("revision")
        return json.dumps(index.page(**query), ensure_ascii=False, allow_nan=False, separators=(",", ":")).encode()
    monkeypatch.setattr(verifier, "http_page", response)
    result = verifier.observe_live_pages()
    assert result["rows"] == 2 and len(result["pages"]) == 4
    assert all(row["exact_bytes"] for row in result["pages"])


def test_live_probe_uses_actual_gateway_query_contract_not_internal_parameter_names(tmp_path, monkeypatch):
    from scripts.serve_public_dashboards import PublicDashboardHandler
    from urllib.parse import urlencode
    source = tmp_path / "features.json"
    source.write_text("source")
    monkeypatch.setattr(verifier, "SOURCE", source)
    signature = verifier.metadata_signature(source.stat())
    index = SimpleNamespace(rows=[], revision="a"*32, page=lambda **_: {})
    monkeypatch.setattr(verifier, "snapshot", lambda: (signature, "source-hash", index))
    queries = []
    def response(query):
        parsed = PublicDashboardHandler._feature_page_query(urlencode(query))
        queries.append(parsed)
        return b"{}"
    monkeypatch.setattr(verifier, "http_page", response)
    verifier.observe_live_pages()
    assert len(queries) == 4
    assert queries[0]["search"] == "TWSE" and queries[1]["search"] == "台"
    assert all(query["revision"] is not None for query in queries if query["offset"] > 0)


def test_source_swaps_are_bounded_and_do_not_fabricate_live_parity(tmp_path, monkeypatch):
    source = tmp_path / "features.json"
    source.write_text("source")
    monkeypatch.setattr(verifier, "SOURCE", source)
    calls = []
    def snapshot():
        calls.append(1)
        return (0, 0, 0, 0, 0), "source-hash", SimpleNamespace(rows=[], page=lambda **_: {}, revision="old")
    monkeypatch.setattr(verifier, "snapshot", snapshot)
    monkeypatch.setattr(verifier, "http_page", lambda _: b"{}")
    with pytest.raises(ValueError, match="bounded_readback"):
        verifier.observe_live_pages()
    assert len(calls) == 3


def test_actual_http_payload_difference_is_not_hidden(tmp_path, monkeypatch):
    source = tmp_path / "features.json"
    source.write_text("source")
    monkeypatch.setattr(verifier, "SOURCE", source)
    signature = verifier.metadata_signature(source.stat())
    monkeypatch.setattr(verifier, "snapshot", lambda: (signature, "source-hash",
        SimpleNamespace(rows=[], page=lambda **_: {"value": 1}, revision="current")))
    monkeypatch.setattr(verifier, "http_page", lambda _: b'{"value":2}')
    with pytest.raises(ValueError, match="original_live_oracle"):
        verifier.observe_live_pages()


@pytest.fixture
def acceptance(tmp_path, monkeypatch):
    root, fixture = tmp_path / "repo", tmp_path / "fixture"
    fixture.mkdir()
    paths = [root / name for name in ("scripts/benchmark_public_feature_residency.py",
        "scripts/serve_public_dashboards.py", "stockagent/live/data_monitor_feature_pages.py",
        "native/__init__.py", "native/_core.so")]
    for path in paths:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("current fixture code")
    source = fixture / "features.json"
    source.write_text("original complete fixture")
    manifest = fixture / "fixture.json"
    atomic_write_json(manifest, {"rows": 3})
    paths += [source, manifest]
    hashes = {str(path): verifier.sha(path) for path in paths}
    monkeypatch.setattr(verifier, "ROOT", root)
    monkeypatch.setattr(verifier, "features", SimpleNamespace(_native=SimpleNamespace(__file__=str(paths[3]),
                         _core=SimpleNamespace(__file__=str(paths[4])))))
    control = tmp_path / "control.json"
    atomic_write_json(control, {"exact_output_parity": True, "input_and_code_sha256": {str(source): hashes[str(source)]},
                                "trials": [{"output_sha256": "original", "requests": 4}]})
    medians = {"baseline": {"cpu_seconds": 2., "wall_seconds": 2., "rss_peak_bytes": 100., "retained_index_rss_bytes": 100.},
               "shared": {"cpu_seconds": 2., "wall_seconds": 2., "rss_peak_bytes": 60., "retained_index_rss_bytes": 80.}}
    order = ["baseline", "shared", "shared", "baseline"]*2
    value = {"contract": "public_feature_complete_paging_residency_v1", "fixture": str(fixture),
             "exact_output_parity": True, "code_stable": True, "source_unchanged": True, "balanced_abba": True,
             "production_writes": False, "provider_calls": 0, "gpu_calls": 0, "medians": medians,
             "input_and_code_sha256": hashes, "independent_control": {"path": str(control), "sha256": verifier.sha(control)},
             "trial_order": order, "trials": [{**medians[variant], "variant": variant, "output_sha256": "original",
             "rows": 3, "requests": 4, "metadata_decoder_backend": "msgspec_shared_labels"} for variant in order]}
    path = tmp_path / "acceptance.json"
    atomic_write_json(path, value)
    return path, value


def test_acceptance_requires_exact_current_files_full_parity_and_memory_gain(acceptance, monkeypatch):
    path, value = acceptance
    assert verifier.accepted_measurement(path) == value["input_and_code_sha256"]
    original = verifier.sha
    monkeypatch.setattr(verifier, "sha", lambda name: "different" if str(name).endswith("data_monitor_feature_pages.py") else original(name))
    with pytest.raises(ValueError, match="accepted_measurement"):
        verifier.accepted_measurement(path)


@pytest.mark.parametrize("damage", ["population", "requests", "parity", "fake_median", "partial_code"])
def test_partial_workflow_or_fabricated_summary_cannot_pass_acceptance(acceptance, damage):
    path, value = acceptance
    if damage == "population":
        value["trials"][1]["rows"] = 1
    elif damage == "requests":
        value["trials"][1]["requests"] = 1
    elif damage == "parity":
        value["trials"][1]["output_sha256"] = "changed"
    elif damage == "fake_median":
        value["medians"]["shared"]["cpu_seconds"] = .1
    else:
        value["input_and_code_sha256"].pop(next(name for name in value["input_and_code_sha256"] if name.endswith("_core.so")))
    atomic_write_json(path, value)
    with pytest.raises(ValueError, match="accepted_measurement"):
        verifier.accepted_measurement(path)

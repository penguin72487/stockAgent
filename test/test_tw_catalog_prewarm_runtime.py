"""Portable adoption gates; no service, source, broker or GPU operations."""
from copy import deepcopy
from contextlib import nullcontext
from io import BytesIO
import json
from types import SimpleNamespace
from urllib.parse import urlsplit

import pytest

from scripts import verify_tw_catalog_prewarm_runtime as verify


@pytest.fixture
def acceptance(tmp_path, monkeypatch):
    monkeypatch.setattr(verify, "ROOT", tmp_path)
    files = [tmp_path / name for name in ("benchmark.py", "dashboard.py", "projection.py",
                                          "scripts/profile_public_gateway_residency.py")]
    for path in files:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("immutable code")
    monkeypatch.setattr(verify.workflow, "__file__", str(files[0]))
    monkeypatch.setattr(verify.workflow, "dashboard", SimpleNamespace(__file__=str(files[1])))
    monkeypatch.setattr(verify.workflow, "projection", SimpleNamespace(__file__=str(files[2])))
    source = tmp_path / "artifacts/live/tw_day_trade_simulation"
    source.mkdir(parents=True)
    history = source / "benchmark_history.json"
    history.write_text("{}")
    fingerprint = {"history": "immutable", "history_state_consumed_fields": {"product": "tw_day_trade"}}
    monkeypatch.setattr(verify.workflow, "source_inventory", lambda _: fingerprint)
    order = ["baseline", "catalog", "catalog", "baseline"]*2
    metrics = {"baseline": {"wall_seconds": 4., "cpu_seconds": 4., "idle_rss_bytes": 400,
                             "rss_peak_bytes": 450, "warm_wall_seconds": .5},
               "catalog": {"wall_seconds": 3., "cpu_seconds": 3., "idle_rss_bytes": 250,
                            "rss_peak_bytes": 300, "warm_wall_seconds": .5}}
    trials = [{"variant": variant, **metrics[variant], "warm_exact_output_parity": True,
               "output_sha256": {"all": "complete"}, "index_counts": {"days": 156},
               "outputs": [{"returned_points": 341952}]} for variant in order]
    value = {"contract": "tw_complete_default_history_catalog_prewarm_idle_v1",
             "source_root": str(source), "trial_order": order, "trials": trials,
             "code_sha256": {str(path): verify.workflow.sha(path) for path in files},
             "source_signatures": fingerprint, "source_sha256": {str(history): verify.workflow.sha(history)},
             "medians": metrics, "balanced_abba": True, "exact_output_parity": True,
             "consumed_inputs_unchanged": True, "code_stable": True,
             "source_writes": False, "live_cache_writes": False, "provider_calls": 0, "gpu_calls": 0}
    path = tmp_path / "acceptance.json"
    path.write_text(json.dumps(value))
    return path, value


def test_accepts_balanced_whole_work_and_recomputes_metrics(acceptance):
    path, value = acceptance
    assert verify.accepted_measurement(path)["medians"] == value["medians"]


@pytest.mark.parametrize("change", ["partial", "output", "order", "fake_median", "code", "source", "gpu"])
def test_rejects_fabricated_or_drifted_acceptance(acceptance, change):
    path, value = acceptance
    value = deepcopy(value)
    if change == "partial":
        value["trials"].pop()
    elif change == "output":
        value["trials"][1]["output_sha256"]["all"] = "different"
    elif change == "order":
        value["trial_order"].reverse()
        value["trial_order"][0] = "catalog"
    elif change == "fake_median":
        value["medians"]["catalog"]["wall_seconds"] = 1.
    elif change == "code":
        value["code_sha256"][next(iter(value["code_sha256"]))] = "wrong"
    elif change == "source":
        value["source_signatures"] = {}
    else:
        value["gpu_calls"] = 1
    path.write_text(json.dumps(value))
    with pytest.raises(ValueError):
        verify.accepted_measurement(path)


@pytest.mark.parametrize("failure", [None, "protected", "owner", "curve"])
def test_real_owner_readback_retains_failure_receipt_and_exact_byte_gate(tmp_path, monkeypatch, failure):
    new = {"MainPID": "20", "InvocationID": "new", "ActiveState": "active"}
    old = {**new, "MainPID": "10", "InvocationID": "old"}
    protected = {unit: {"InvocationID": unit} for unit in verify.PROTECTED}
    monkeypatch.setattr(verify, "identity", lambda unit: new if unit == verify.UNIT else protected[unit])
    monkeypatch.setattr(verify, "check_reader", lambda _: None)
    monkeypatch.setattr(verify, "accepted_measurement", lambda _: {"bound": True})
    curves = [{"sha256": "complete actual wire", "bytes": 16000000}]
    monkeypatch.setattr(verify, "http_curves", lambda: curves)
    baseline_value = {"state": "actual_previous_reader_curves_captured", "owner": old,
                      "protected": deepcopy(protected), "accepted_measurement": {"bound": True},
                      "curves": deepcopy(curves)}
    if failure == "protected":
        baseline_value["protected"][verify.PROTECTED[0]] = {"InvocationID": "different"}
    elif failure == "owner":
        baseline_value["owner"] = new
    elif failure == "curve":
        baseline_value["curves"][0]["sha256"] = "incomplete"
    baseline = tmp_path / "baseline.json"
    baseline.write_text(json.dumps(baseline_value))
    output = tmp_path / "readback.json"
    if failure:
        with pytest.raises(ValueError):
            verify.verify(tmp_path / "acceptance.json", output, baseline)
        assert json.loads(output.read_bytes())["state"] == "failed_readback"
    else:
        assert verify.verify(tmp_path / "acceptance.json", output, baseline)["exact_http_byte_parity"] is True


def test_history_probe_uses_the_actual_public_query_parser(monkeypatch):
    from scripts.serve_public_dashboards import PublicDashboardHandler

    latest = "2026-10-13"
    monkeypatch.setattr(verify.workflow, "projection", SimpleNamespace(
        load_benchmark_projection=lambda **_: SimpleNamespace(session_entries={latest: {}})))
    queries = []
    payload = {"simulation_only": True, "production_order_possible": False,
               "returned_points": 2192, "minute_series": [{"id": "complete"}]}
    body = json.dumps(payload).encode()

    def public_response(url, *, timeout):
        assert timeout == 30
        parsed = urlsplit(url)
        assert parsed.path == "/tw-day-trade/api/history"
        queries.append(PublicDashboardHandler._tw_history_query(parsed.query))
        return nullcontext(BytesIO(body))

    monkeypatch.setattr(verify, "urlopen", public_response)
    results = verify.http_curves()
    assert queries == [
        {"range_key": "all", "resolution": "1m", "history_encoding": "minute_columns_v2",
         "start_date": None, "end_date": None},
        {"range_key": "all", "resolution": "1m", "history_encoding": "minute_columns_v2",
         "start_date": latest, "end_date": latest},
    ]
    assert all(row["bytes"] == len(body) and row["returned_points"] == 2192
               and row["series_count"] == 1 for row in results)

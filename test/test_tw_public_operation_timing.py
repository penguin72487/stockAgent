from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, replace
import hashlib
import json
from types import SimpleNamespace

import pytest

from downloader import download_tw_public_data as twpub
from downloader import operation_timing


ROUTES = (
    ("historical_json_table", "_download_historical"),
    ("delisted_history", "_download_delisted_history"),
    ("snapshot_url", "_download_snapshot_url"),
    ("data_gov", "_download_data_gov"),
    ("gcis_catalog", "_download_gcis_catalog"),
    ("fsc_catalog", "_download_fsc_catalog"),
)


def registered(kind="historical_json_table"):
    return next(spec for spec in twpub.EXTENDED_DATASETS.values() if spec.kind == kind)


def events(capsys):
    lines = capsys.readouterr().out.splitlines()
    assert lines
    assert all(line.startswith(twpub.OPERATION_TIMING_PREFIX) for line in lines)
    return [json.loads(line[len(twpub.OPERATION_TIMING_PREFIX):]) for line in lines]


@pytest.mark.parametrize("kind,adapter", ROUTES)
def test_native_routes_preserve_result_and_bind_only_valid_body_clock(
    kind, adapter, monkeypatch, tmp_path, capsys
):
    spec = registered(kind)
    args = SimpleNamespace(mode="repair", refresh=False, api_key="private-secret")
    result = twpub.DownloadResult(spec.name, "ok", 1200, "private-output",
                                  message="private-message", fetched_dates=2)
    before = asdict(result)
    calls = []

    def native(*actual):
        calls.append(actual)
        return result

    monkeypatch.setattr(twpub, adapter, native)
    assert twpub.download_dataset(spec, args, tmp_path) is result
    assert calls == [(spec, args, tmp_path)]
    assert asdict(result) == before
    event, = events(capsys)
    assert event["contract"] == "tw_public_dataset_operation_timing_v1"
    assert event["source_endpoint_id"] == "tw-public:" + spec.name
    assert event["source_binding_eligible"] is True
    assert event["returned_dataset"] == spec.name
    assert event["result_dataset_matches_request"] is True
    expected_spec = json.dumps(asdict(spec), sort_keys=True, ensure_ascii=False,
                               allow_nan=False).encode()
    assert event["dataset_spec_sha256"] == hashlib.sha256(expected_spec).hexdigest()
    clock = event["operation_timing"]
    assert clock["contract"] == operation_timing.CONTRACT
    assert clock["clock"] == "perf_counter_ns"
    assert clock["started_monotonic_ns"] > 0
    assert clock["seconds"] >= 0 and clock["invalid_clock"] is False
    assert clock["stages"] == []
    assert "excludes_executor_queue_timing_log_parent_reports_and_later_audits" in clock["scope"]
    assert event["producer_pid"] > 0 and event["producer_thread_id"] > 0
    assert event["reported_work"]["rows"] == 1200
    assert event["reported_work"]["fetched_dates"] == 2
    assert event["invalid_reported_work_fields"] == []
    assert event["work_units_denominator"] is None
    for key in ("configuration_is_resolved_request_range", "clock_is_source_publication_time",
                "acquisition_completeness_verified", "live_readiness_verified"):
        assert event[key] is False
    assert "private-" not in json.dumps(event)


@pytest.mark.parametrize("status", ["up_to_date", "empty", "no_distribution", "incomplete", "failed"])
def test_status_is_not_reinterpreted_as_success_or_completed_work(status, monkeypatch, tmp_path, capsys):
    spec = registered()
    result = twpub.DownloadResult(spec.name, status, 999, None, fetched_dates=0,
                                  failed_dates=2, coverage_complete=False)
    monkeypatch.setattr(twpub, "_download_historical", lambda *args: result)
    assert twpub.download_dataset(spec, SimpleNamespace(), tmp_path) is result
    event, = events(capsys)
    assert event["status"] == status and event["outcome"] == "returned"
    assert event["source_binding_eligible"] is True
    assert event["acquisition_completeness_verified"] is False
    assert event["work_units_denominator"] is None
    assert event["reported_work"]["rows"] == 999


def test_native_error_keeps_failure_message_outside_timing_log(monkeypatch, tmp_path, capsys):
    spec = registered()

    def native(*args):
        raise RuntimeError("private-credential-in-original-message")

    monkeypatch.setattr(twpub, "_download_historical", native)
    result = twpub.download_dataset(spec, SimpleNamespace(), tmp_path)
    assert result.status == "failed" and result.message == "private-credential-in-original-message"
    event, = events(capsys)
    assert event["status"] == "failed" and event["outcome"] == "returned"
    assert "private-" not in json.dumps(event)


@pytest.mark.parametrize("error", [KeyboardInterrupt(), SystemExit(7)])
@pytest.mark.parametrize("broken_logger", [False, True])
def test_termination_identity_survives_logging_failure(
    error, broken_logger, monkeypatch, tmp_path, capsys
):
    spec = registered()

    def native(*args):
        raise error

    def broken(*args):
        raise OSError("timing sink unavailable")

    monkeypatch.setattr(twpub, "_download_historical", native)
    if broken_logger:
        monkeypatch.setattr(twpub, "_emit_dataset_operation_timing", broken)
    with pytest.raises(type(error)) as caught:
        twpub.download_dataset(spec, SimpleNamespace(), tmp_path)
    assert caught.value is error
    if not broken_logger:
        event, = events(capsys)
        assert event["outcome"] == "raised" and event["status"] is None
        assert event["source_binding_eligible"] is False
        assert event["returned_dataset"] is None


@pytest.mark.parametrize("failure", ["constructor", "snapshot", "emitter"])
def test_telemetry_failure_does_not_repeat_or_change_native_download(
    failure, monkeypatch, tmp_path
):
    spec = registered()
    result = twpub.DownloadResult(spec.name, "ok", 1, None)
    calls = []

    def native(*args):
        calls.append(args)
        return result

    def broken(*args, **kwargs):
        raise ValueError("observation failure")

    monkeypatch.setattr(twpub, "_download_historical", native)
    if failure == "constructor":
        monkeypatch.setattr(twpub, "OperationTiming", broken)
    elif failure == "snapshot":
        monkeypatch.setattr(twpub.OperationTiming, "snapshot", broken)
    else:
        monkeypatch.setattr(twpub, "_emit_dataset_operation_timing", broken)
    assert twpub.download_dataset(spec, SimpleNamespace(), tmp_path) is result
    assert len(calls) == 1


@pytest.mark.parametrize("mismatch", ["spec", "returned_dataset", "unsupported"])
def test_mismatched_or_unsupported_source_never_binds(mismatch, monkeypatch, tmp_path, capsys):
    spec = registered()
    returned_name = spec.name
    if mismatch == "spec":
        spec = replace(spec, url="https://example.test/private-secret")
    elif mismatch == "returned_dataset":
        returned_name = "other-dataset"
    else:
        spec = replace(spec, kind="unrecognized")
    result = twpub.DownloadResult(returned_name, "ok", 1, None)
    monkeypatch.setattr(twpub, "_download_historical", lambda *args: result)
    actual = twpub.download_dataset(spec, SimpleNamespace(), tmp_path)
    if mismatch == "unsupported":
        assert actual.status == "unsupported" and actual.message == "kind=unrecognized"
    else:
        assert actual is result
    event, = events(capsys)
    assert event["source_binding_eligible"] is False
    if mismatch != "returned_dataset":
        assert event["source_endpoint_id"] is None
    assert "private-secret" not in json.dumps(event)


def test_request_policy_changes_are_visible_without_claiming_resolved_work(monkeypatch, tmp_path, capsys):
    spec = registered()
    result = twpub.DownloadResult(spec.name, "up_to_date", 100, None)
    monkeypatch.setattr(twpub, "_download_historical", lambda *args: result)
    policies = [SimpleNamespace(mode="repair", refresh=False, request_interval=1),
                SimpleNamespace(mode="daily", refresh=False, request_interval=1),
                SimpleNamespace(mode="repair", refresh=True, request_interval=1),
                SimpleNamespace(mode="repair", refresh=False, request_interval=2)]
    for policy in policies:
        assert twpub.download_dataset(spec, policy, tmp_path) is result
    records = events(capsys)
    assert len({row["request_policy_sha256"] for row in records}) == len(policies)
    assert all(row["configuration_is_resolved_request_range"] is False for row in records)


def test_regressing_clock_is_unknown_and_cannot_bind(monkeypatch, tmp_path, capsys):
    spec = registered()
    now = [1000]
    monkeypatch.setattr(operation_timing.time, "perf_counter_ns", lambda: now[0])
    result = twpub.DownloadResult(spec.name, "ok", 1, None)

    def native(*args):
        now[0] = 900
        return result

    monkeypatch.setattr(twpub, "_download_historical", native)
    assert twpub.download_dataset(spec, SimpleNamespace(), tmp_path) is result
    event, = events(capsys)
    assert event["operation_timing"]["seconds"] is None
    assert event["operation_timing"]["invalid_clock"] is True
    assert event["source_binding_eligible"] is False


def test_malformed_work_counts_remain_in_native_result_and_unknown_in_telemetry(
    monkeypatch, tmp_path, capsys
):
    spec = registered()
    result = twpub.DownloadResult(spec.name, "incomplete", -3, None,
                                  fetched_dates=True, failed_dates="2")
    original = asdict(result)
    monkeypatch.setattr(twpub, "_download_historical", lambda *args: result)
    assert twpub.download_dataset(spec, SimpleNamespace(), tmp_path) is result
    assert asdict(result) == original
    event, = events(capsys)
    assert event["invalid_reported_work_fields"] == ["failed_dates", "fetched_dates", "rows"]
    assert all(event["reported_work"][key] is None for key in event["invalid_reported_work_fields"])


def test_parallel_dataset_clocks_are_individually_parseable(monkeypatch, tmp_path, capsys):
    spec = registered()

    def native(*args):
        return twpub.DownloadResult(spec.name, "ok", 1, None)

    monkeypatch.setattr(twpub, "_download_historical", native)
    with ThreadPoolExecutor(max_workers=4) as pool:
        results = list(pool.map(lambda _: twpub.download_dataset(spec, SimpleNamespace(), tmp_path), range(24)))
    records = events(capsys)
    assert len(results) == len(records) == 24
    assert all(row["source_binding_eligible"] is True for row in records)
    assert all(row["operation_timing"]["seconds"] >= 0 for row in records)

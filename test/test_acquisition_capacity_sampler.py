from pathlib import Path
import threading

from stockagent.data_sync.acquisition_capacity_sampler import AcquisitionCapacitySampler, capacity_retry_delay_seconds


def test_waiter_rechecks_unfinished_probe_before_the_sample_expires():
    assert capacity_retry_delay_seconds({'state': 'deferred', 'reason': 'capacity_sample_pending'}) == 5
    assert capacity_retry_delay_seconds({'state': 'deferred', 'reason': 'capacity_sample_expired'}) == 5
    assert capacity_retry_delay_seconds({'state': 'deferred', 'reason': 'physical_backing_reserve'}) == 60
    assert capacity_retry_delay_seconds({'state': 'deferred', 'reason': 'capacity_probe_failed'}) == 60


def test_physical_capacity_observation_keeps_headroom_without_private_paths(tmp_path):
    sampler = AcquisitionCapacitySampler(tmp_path, probe=lambda _: {
        'state': 'deferred', 'reason': 'physical_backing_reserve',
        'physical_free_bytes': 100, 'required_bytes_including_reserve': 200,
        'path': '/private/never-publish',
    })
    result = sampler.initial_sample()
    assert result['physical_free_bytes'] == 100 and result['required_bytes_including_reserve'] == 200
    assert 'path' not in result


def test_pending_and_expired_samples_fail_closed_with_only_one_probe(tmp_path: Path):
    clock = [0.0]
    entered, release, refresh_release = threading.Event(), threading.Event(), threading.Event()
    calls = []

    def probe(root):
        calls.append(root)
        entered.set()
        assert (release if len(calls) == 1 else refresh_release).wait(2)
        return {"state": "admitted", "path": "private", "available_bytes": 123}

    sampler = AcquisitionCapacitySampler(tmp_path, interval_seconds=5, max_age_seconds=10,
                                         probe=probe, clock=lambda: clock[0])
    assert sampler.initial_sample(timeout_seconds=0)["reason"] == "capacity_sample_pending"
    assert entered.wait(2)
    clock[0] = 6
    assert sampler.sample()["state"] == "deferred"
    assert len(calls) == 1
    release.set()
    assert sampler._done.wait(2)
    value = sampler.sample()
    assert value["state"] == "admitted" and "path" not in value
    # A probe started six seconds ago may finish now, but its age remains six.
    clock[0] = 11
    assert sampler.sample()["reason"] == "capacity_sample_expired"
    refresh_release.set()


def test_probe_failure_and_invalid_result_are_not_admission(tmp_path: Path):
    def failure(_root):
        raise ValueError("private diagnostic")

    sampler = AcquisitionCapacitySampler(tmp_path, probe=failure)
    result = sampler.initial_sample(timeout_seconds=2)
    assert result["state"] == "deferred" and result["reason"] == "capacity_probe_failed"
    assert result["error_type"] == "ValueError" and "private diagnostic" not in str(result)

    sampler = AcquisitionCapacitySampler(tmp_path, probe=lambda _: {"state": "unknown"})
    assert sampler.initial_sample(timeout_seconds=2)["state"] == "deferred"


def test_successful_refresh_replaces_an_expired_sample(tmp_path: Path):
    clock = [0.0]
    values = iter([{"state": "admitted"}, {"state": "deferred", "reason": "filesystem_reserve"},
                   {"state": "admitted"}])
    sampler = AcquisitionCapacitySampler(tmp_path, interval_seconds=5, max_age_seconds=10,
                                         probe=lambda _: next(values), clock=lambda: clock[0])
    assert sampler.initial_sample(timeout_seconds=2)["state"] == "admitted"
    clock[0] = 11
    assert sampler.initial_sample(timeout_seconds=2)["reason"] == "filesystem_reserve"
    clock[0] = 22
    assert sampler.initial_sample(timeout_seconds=2)["state"] == "admitted"

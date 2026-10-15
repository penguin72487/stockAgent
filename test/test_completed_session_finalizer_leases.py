"""Completed-close reads share the source fence; real builds remain exclusive."""
from copy import deepcopy
import fcntl
import json
from types import SimpleNamespace

import pytest

from scripts import finalize_tw_public_completed_session as finalizer


LABELS = (
    "corporate_action_reference", "stock_panel",
    "corporate_action_entitlements", "public_features",
)


def state(*stale):
    errors = {name: (["stale dependency"] if name in stale else []) for name in LABELS}
    return {
        "dates": {name: "2026-10-14" for name in LABELS},
        "derived_errors": errors,
        "errors": [f"{name}: stale dependency" for name in stale],
        "current": not stale,
    }


@pytest.fixture
def configured(tmp_path, monkeypatch):
    live = tmp_path / "live" / "data_tw_public"
    live.mkdir(parents=True)
    args = SimpleNamespace(
        workers=2, step_attempts=2, step_retry_base_seconds=0,
        public_feature_incremental_days=14, live_root=live,
        publication_root=tmp_path / "publications", receipt=tmp_path / "receipt.json",
        expected_date="2026-10-14", publication_phase=None, force=False,
    )
    monkeypatch.setattr(finalizer, "parse_args", lambda: args)
    monkeypatch.setattr(finalizer, "_accepted_close_publication", lambda *a, **k: (
        "close_final", {"completed_at_taipei": "2026-10-14T14:40:00+08:00"}, {},
    ))
    monkeypatch.setattr(finalizer, "_build_commands", lambda **k: [[name] for name in LABELS])
    return args, live.parent / ".locks" / "tw-public-refresh.lock"


def forbidden(*args, **kwargs):
    raise AssertionError("already-current close attempted a build or provider request")


def assert_lock(lock_path, *, reader_allowed):
    with lock_path.open("a+") as other:
        if reader_allowed:
            fcntl.flock(other, fcntl.LOCK_SH | fcntl.LOCK_NB)
            fcntl.flock(other, fcntl.LOCK_UN)
        else:
            with pytest.raises(BlockingIOError):
                fcntl.flock(other, fcntl.LOCK_SH | fcntl.LOCK_NB)
        with pytest.raises(BlockingIOError):
            fcntl.flock(other, fcntl.LOCK_EX | fcntl.LOCK_NB)


def test_current_generation_validates_once_allows_readers_and_excludes_writers(configured, monkeypatch):
    args, lock_path = configured
    observed = []

    def validated(*a, **k):
        assert_lock(lock_path, reader_allowed=True)
        observed.append(k)
        return state()

    monkeypatch.setattr(finalizer, "_derived_state", validated)
    monkeypatch.setattr(finalizer, "_build_commands", forbidden)
    monkeypatch.setattr(finalizer.subprocess, "run", forbidden)
    assert finalizer.main() == 0
    receipt = json.loads(args.receipt.read_bytes())
    assert len(observed) == receipt["derived_validation_count"] == 1
    assert receipt["source_lease_mode"] == "shared"
    assert receipt["before"] == receipt["after"] == state()
    assert receipt["reused"] is True and receipt["steps"] == []
    assert receipt["status"] == "ok"
    assert receipt["derived_validation_contract"] == "completed_session_read_then_write_v2"
    with lock_path.open("a+") as writer:
        fcntl.flock(writer, fcntl.LOCK_EX | fcntl.LOCK_NB)


def test_upgrade_revalidates_and_reuses_generation_completed_by_another_writer(configured, monkeypatch):
    args, lock_path = configured
    observations = []
    real_flock = fcntl.flock
    upgraded = False

    def convert(fd, operation):
        nonlocal upgraded
        if operation == fcntl.LOCK_EX:
            # Simulate the documented non-atomic conversion: an intervening
            # producer finishes before this owner obtains its exclusive lease.
            real_flock(fd, fcntl.LOCK_UN)
            with lock_path.open("a+") as producer:
                real_flock(producer, fcntl.LOCK_EX | fcntl.LOCK_NB)
                upgraded = True
                real_flock(producer, fcntl.LOCK_UN)
        return real_flock(fd, operation)

    def validated(*a, **k):
        observations.append(upgraded)
        return state() if upgraded else state("stock_panel")

    monkeypatch.setattr(finalizer.fcntl, "flock", convert)
    monkeypatch.setattr(finalizer, "_derived_state", validated)
    monkeypatch.setattr(finalizer, "_build_commands", forbidden)
    monkeypatch.setattr(finalizer.subprocess, "run", forbidden)
    assert finalizer.main() == 0
    receipt = json.loads(args.receipt.read_bytes())
    assert observations == [False, True]
    assert receipt["before"] == receipt["after"] == state()
    assert receipt["source_lease_mode"] == "exclusive"
    assert receipt["derived_validation_count"] == 2 and receipt["reused"] is True


def test_stale_steps_and_retries_still_run_exclusively_and_require_final_validation(configured, monkeypatch):
    args, lock_path = configured
    stale = set(LABELS)
    calls = []
    validation_count = 0

    def validated(*a, **k):
        nonlocal validation_count
        validation_count += 1
        assert_lock(lock_path, reader_allowed=validation_count == 1)
        return state(*(name for name in LABELS if name in stale))

    def build(command, **kwargs):
        assert_lock(lock_path, reader_allowed=False)
        calls.append(command[0])
        if len(calls) == 1:
            return SimpleNamespace(returncode=1)
        stale.discard(command[0])
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(finalizer, "_derived_state", validated)
    monkeypatch.setattr(finalizer.subprocess, "run", build)
    assert finalizer.main() == 0
    receipt = json.loads(args.receipt.read_bytes())
    assert calls == [LABELS[0], *LABELS]
    assert receipt["derived_validation_count"] == validation_count == 12
    assert receipt["source_lease_mode"] == "exclusive" and receipt["reused"] is False
    assert receipt["before"]["current"] is False and receipt["after"] == state()
    assert [row["attempt_count"] for row in receipt["steps"]] == [2, 1, 1, 1]


def test_force_keeps_all_four_builds_even_when_the_original_generation_is_current(configured, monkeypatch):
    args, lock_path = configured
    args.force = True
    calls = []
    monkeypatch.setattr(finalizer, "_derived_state", lambda *a, **k: state())

    def build(command, **kwargs):
        assert_lock(lock_path, reader_allowed=False)
        calls.append(command[0])
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(finalizer.subprocess, "run", build)
    assert finalizer.main() == 0
    receipt = json.loads(args.receipt.read_bytes())
    assert calls == list(LABELS) and receipt["derived_validation_count"] == 11
    assert receipt["source_lease_mode"] == "exclusive" and receipt["reused"] is False


def test_failed_dependency_is_not_accepted_or_hidden_by_reuse(configured, monkeypatch):
    args, _ = configured
    monkeypatch.setattr(finalizer, "_derived_state", lambda *a, **k: state("stock_panel"))
    monkeypatch.setattr(finalizer.subprocess, "run", lambda *a, **k: SimpleNamespace(returncode=1))
    assert finalizer.main() == 1
    receipt = json.loads(args.receipt.read_bytes())
    assert receipt["status"] == "failed" and receipt["after"]["current"] is False
    assert receipt["reused"] is False and len(receipt["steps"]) == 1
    assert receipt["steps"][0]["attempt_count"] == 2


def test_missing_publication_never_obtains_a_source_lease_or_calls_a_builder(configured, monkeypatch):
    args, lock_path = configured
    monkeypatch.setattr(finalizer, "_accepted_close_publication", lambda *a, **k: (
        None, {}, {"close_final": ["official close incomplete"]},
    ))
    monkeypatch.setattr(finalizer, "_derived_state", forbidden)
    monkeypatch.setattr(finalizer.subprocess, "run", forbidden)
    assert finalizer.main() == 75
    receipt = json.loads(args.receipt.read_bytes())
    assert receipt["status"] == "waiting_source" and not lock_path.exists()


@pytest.mark.parametrize("defect", ["date", "dependency", "coverage"])
def test_every_real_derived_error_still_prevents_fast_current_reuse(configured, monkeypatch, defect):
    args, _ = configured
    broken = deepcopy(state("public_features"))
    broken["errors"] = [defect]
    monkeypatch.setattr(finalizer, "_derived_state", lambda *a, **k: deepcopy(broken))
    monkeypatch.setattr(finalizer.subprocess, "run", lambda *a, **k: SimpleNamespace(returncode=0))
    assert finalizer.main() == 1
    receipt = json.loads(args.receipt.read_bytes())
    assert receipt["source_lease_mode"] == "exclusive" and receipt["status"] == "failed"
    assert receipt["steps"][0]["attempt_count"] == 2

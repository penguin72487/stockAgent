from __future__ import annotations

import fcntl
import json

import pytest

from scripts.check_legacy_return_ready import readiness


@pytest.fixture
def cohort(tmp_path):
    for name in ("inventory.json", "archive-catalog.json", "cohort-owner.lock"):
        (tmp_path / name).write_text("{}")
    (tmp_path / "progress.json").write_text(json.dumps({"items": [
        {"files": 10, "state": "return-failed-source-preserved"}
    ]}))
    return tmp_path


def test_retained_failure_retries_without_modifying_receipts(cohort):
    before = {path.name: path.read_bytes() for path in cohort.iterdir()}
    assert readiness(cohort) == (True, "retained_cohort_has_retryable_items")
    assert {path.name: path.read_bytes() for path in cohort.iterdir()} == before


def test_protected_loose_files_do_not_block_retained_directory_retry(cohort):
    path = cohort / "progress.json"
    path.write_text(json.dumps({"items": [
        {"relative_root": "markets/run.log", "bytes": 12,
         "state": "non-directory-protected", "signature": [1, 2, 12]},
        {"relative_root": "ablations/old-run", "files": 10,
         "state": "return-failed-source-preserved"},
    ]}))
    before = path.read_bytes()
    assert readiness(cohort) == (True, "retained_cohort_has_retryable_items")
    assert path.read_bytes() == before


def test_only_protected_loose_files_do_not_launch_remote_work(cohort):
    (cohort / "progress.json").write_text(json.dumps({"items": [
        {"relative_root": "markets/run.lock", "bytes": 0,
         "state": "non-directory-protected"},
    ]}))
    assert readiness(cohort) == (False, "no_retryable_items_in_retained_cohort")


def test_live_owner_skips_then_finished_owner_allows_retry(cohort):
    with (cohort / "cohort-owner.lock").open("a") as owner:
        fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
        assert readiness(cohort) == (False, "existing_cohort_owner_active")
    assert readiness(cohort)[0] is True


@pytest.mark.parametrize("state", [
    "remote-source-retired", "source-protected", "non-directory-protected",
    "empty-directory-protected"
])
def test_retired_or_protected_items_do_not_trigger_remote_work(cohort, state):
    (cohort / "progress.json").write_text(json.dumps({"items": [{"files": 10, "state": state}]}))
    assert readiness(cohort) == (False, "no_retryable_items_in_retained_cohort")


@pytest.mark.parametrize("items", [None, [{"files": True, "state": "retry"}], [None],
                                   [{"files": -1, "state": "retry"}],
                                   [{"state": "return-failed-source-preserved"}],
                                   [{"state": "non-directory-protected", "files": None}],
                                   [{"state": "non-directory-protected", "files": True}],
                                   [{"state": "non-directory-protected", "files": -1}]])
def test_invalid_progress_cannot_launch_a_worker(cohort, items):
    (cohort / "progress.json").write_text(json.dumps({"items": items}))
    assert readiness(cohort) == (False, "progress_items_invalid")


def test_redirected_progress_is_rejected(cohort, tmp_path):
    path = cohort / "progress.json"
    path.rename(cohort / "other.json")
    path.symlink_to(cohort / "other.json")
    assert readiness(cohort) == (False, "cohort_receipts_missing_or_redirected")


def test_uninitialized_cohort_is_not_created(tmp_path):
    missing = tmp_path / "missing"
    assert readiness(missing) == (False, "cohort_not_initialized")
    assert not missing.exists()

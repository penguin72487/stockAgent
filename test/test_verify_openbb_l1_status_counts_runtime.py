from copy import deepcopy

import pytest

from scripts.openbb_l1_status_counts import POLICY
from scripts.verify_openbb_l1_status_counts_runtime import validate_publication


def _facts():
    counts = {"a": 2, "b": 3}
    audit = {"policy": POLICY, "path": "endpoint_index_seeks", "endpoints": 2, "members": 5}
    status = {"systemd_invocation_id": "fresh", "state": "completed", "l0_deleted": False,
              "member_count_audit": audit, "compacted_files": 5, "success_files": 5,
              "compacted_rows": 10, "success_rows": 10, "active_segments": 2, "endpoints": 2,
              "endpoint_status": [{"endpoint": key, "compacted_files": value} for key, value in counts.items()]}
    attempt = deepcopy(status)
    attempt.update(publication_stage="status_published", exit_code=0)
    unit = {"ActiveState": "inactive", "MainPID": "0", "Result": "success",
            "ExecMainStatus": "0", "InvocationID": "fresh"}
    return attempt, status, unit, counts


def test_full_independent_native_counts_are_accepted():
    attempt, status, unit, counts = _facts()
    validate_publication(attempt, status, unit, "previous", counts)


@pytest.mark.parametrize("key,value", [("ActiveState", "activating"), ("MainPID", "123"),
    ("Result", "timeout"), ("ExecMainStatus", "1"), ("InvocationID", "previous"), ("InvocationID", "")])
def test_running_old_or_failed_owner_is_not_acceptance(key, value):
    attempt, status, unit, counts = _facts()
    unit[key] = value
    with pytest.raises(ValueError, match="completed native"):
        validate_publication(attempt, status, unit, "previous", counts)


@pytest.mark.parametrize("target,key,value", [("attempt", "systemd_invocation_id", "other"),
    ("status", "state", "running"), ("attempt", "publication_stage", "status_publish_started"),
    ("attempt", "exit_code", 1), ("status", "l0_deleted", True)])
def test_incomplete_or_mismatched_publication_is_rejected(target, key, value):
    attempt, status, unit, counts = _facts()
    (attempt if target == "attempt" else status)[key] = value
    with pytest.raises(ValueError, match="exact completed"):
        validate_publication(attempt, status, unit, "previous", counts)


@pytest.mark.parametrize("key,value", [("policy", "label_only"), ("path", "canonical_group_by"),
    ("endpoints", 1), ("members", 4)])
def test_metadata_label_or_partial_counts_do_not_prove_execution(key, value):
    attempt, status, unit, counts = _facts()
    status["member_count_audit"][key] = value
    with pytest.raises(ValueError, match="independently verified"):
        validate_publication(attempt, status, unit, "previous", counts)


def test_equal_totals_with_endpoint_misassignment_are_rejected():
    attempt, status, unit, counts = _facts()
    status["endpoint_status"][0]["compacted_files"] = 3
    status["endpoint_status"][1]["compacted_files"] = 2
    with pytest.raises(ValueError, match="legacy source member"):
        validate_publication(attempt, status, unit, "previous", counts)


def test_attempt_row_counts_cannot_lag_publication():
    attempt, status, unit, counts = _facts()
    attempt["success_rows"] = 9
    with pytest.raises(ValueError, match="counts disagree"):
        validate_publication(attempt, status, unit, "previous", counts)

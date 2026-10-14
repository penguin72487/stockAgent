from copy import deepcopy
import pytest

from scripts import verify_openbb_l1_metadata_adoption as observer


def accepted():
    invocation = "a" * 32
    unit = {"ActiveState": "inactive", "SubState": "dead", "MainPID": "0",
            "InvocationID": invocation, "Result": "success", "ExecMainStatus": "0"}
    counts = dict(success_files=12, success_rows=100, compacted_files=12, compacted_rows=100,
        pending_files=0, pending_rows=0, active_segments=3, endpoints=2, new_segments=0,
        stale_segments=0, failed_segments=0, deferred_failed_segments=0)
    audit = {"policy": observer.POLICY, "footer_open_attempts": 3, "physical_cache": {
        "hits": 1, "misses": 1, "bypasses": 0, "evictions": 0, "entries": 1,
        "max_entries": 64, "max_serialized_bytes": 2 * 1024**2, "retained_serialized_bytes": 100}}
    attempt = dict(counts, state="completed", exit_code=0, systemd_invocation_id=invocation,
                   publication_stage="status_published", derivative_metadata_audit=audit)
    published = deepcopy(attempt)
    fixture = {"all_success_segments": True, "selected_segments": 3}
    controls = {"state": "accepted_phase_comparison", "candidate": "schema_cache",
        "exact_canonical_complete_output_parity": True, "all_success_segments": True,
        "trials": [{"view_proofs": 2}]}
    return unit, invocation, attempt, published, fixture, controls


def test_terminal_native_complete_owner_and_counts_accept():
    observer.validate(*accepted())


@pytest.mark.parametrize("broken", ["old_invocation", "running", "publication", "subset",
    "not_complete_controls", "footer_count", "unbounded", "waived_proof", "count_disagreement", "pending"])
def test_no_liveness_or_partial_proof_can_substitute_for_acceptance(broken):
    unit, invocation, attempt, published, fixture, controls = accepted()
    if broken == "old_invocation":
        unit["InvocationID"] = "b" * 32
    elif broken == "running":
        unit["ActiveState"] = "activating"
    elif broken == "publication":
        published["systemd_invocation_id"] = "b" * 32
    elif broken == "subset":
        fixture["all_success_segments"] = False
    elif broken == "not_complete_controls":
        controls["exact_canonical_complete_output_parity"] = False
    elif broken in {"footer_count", "unbounded", "waived_proof"}:
        audit = attempt["derivative_metadata_audit"]
        if broken == "footer_count":
            audit["footer_open_attempts"] = 2
        elif broken == "unbounded":
            audit["physical_cache"]["max_entries"] = 100000
        else:
            audit["physical_cache"]["hits"] = 0
        published["derivative_metadata_audit"] = deepcopy(audit)
    elif broken == "count_disagreement":
        published["success_rows"] += 1
    else:
        published["pending_rows"] = attempt["pending_rows"] = 1
    with pytest.raises(ValueError):
        observer.validate(unit, invocation, attempt, published, fixture, controls)

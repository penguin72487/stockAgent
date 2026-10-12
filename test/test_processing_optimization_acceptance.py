import pytest

from scripts.validate_project_processing_optimization import require_accepted_evidence


def test_existing_dashboard_verified_state_requires_full_acceptance_proofs():
    accepted = {"state": "verified", "complete_bytes_parity": True,
                "all_source_identities_checked": True, "code_abi_stable": True}
    require_accepted_evidence("dashboard_projection.json", accepted)
    for key in accepted:
        broken = {**accepted, key: False}
        with pytest.raises(ValueError, match="not accepted"):
            require_accepted_evidence("dashboard_projection.json", broken)


@pytest.mark.parametrize("state", ["running", "failed", None])
def test_status_only_does_not_count_as_an_accepted_processing_benchmark(state):
    with pytest.raises(ValueError, match="not accepted"):
        require_accepted_evidence("optimized_workflows.json", {"state": state})

from copy import deepcopy
from pathlib import Path
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


def accepted_io():
    unit, invocation, attempt, published, fixture, _ = accepted()
    variants = ["adopted", "local", "metadata_only", "prefetch", "prefetch", "metadata_only", "local", "adopted"]
    io = {"policy": "openbb_metadata_only_footer_io_v1", "metadata_only_supported": True,
          "local_filesystem": True, "actual_footer_opens": 3, "metadata_only_opens": 3,
          "canonical_retries": 0}
    attempt["derivative_metadata_audit"]["footer_io"] = deepcopy(io)
    published["derivative_metadata_audit"] = deepcopy(attempt["derivative_metadata_audit"])
    published["l0_deleted"] = False
    trials = [{"contract": observer.IO_CONTRACT, "state": "completed_trial", "variant": variant,
               "selected_segments": 3, "footer_open_attempts": 3, "all_success_segments": True,
               "stale_count": 0, "provider_calls": 0, "production_writes": False,
               "view_proofs": 2, "complete_audit_output_sha256": "f" * 64,
               "footer_io": deepcopy(io) if variant in {"metadata_only", "prefetch"} else None}
              for variant in variants]
    controls = {"contract": observer.IO_CONTRACT, "state": "accepted_complete_audit_output_parity",
                "exact_complete_output_parity": True, "all_success_segments": True,
                "provider_calls": 0, "production_writes": False, "trials": trials}
    return unit, invocation, attempt, published, fixture, controls


def test_full_io_controls_and_native_metadata_execution_accept():
    observer.validate(*accepted_io())


@pytest.mark.parametrize("broken", ["subset", "wrong_order", "truncated", "no_parity", "proof_count", "proof_hash",
    "empty_hash", "provider", "writes", "skipped_footer", "unsupported", "no_local", "extra_opens",
    "no_metadata_opens", "retry", "native_old_io", "deleted_source"])
def test_full_io_cannot_use_partial_controls_or_unexecuted_native_capability(broken):
    unit, invocation, attempt, published, fixture, controls = accepted_io()
    trial = controls["trials"][2]
    if broken == "subset":
        trial["all_success_segments"] = False
    elif broken == "wrong_order":
        trial["variant"] = "local"
    elif broken == "truncated":
        controls["trials"].pop()
    elif broken == "no_parity":
        controls["exact_complete_output_parity"] = False
    elif broken == "proof_count":
        trial["view_proofs"] -= 1
    elif broken == "proof_hash":
        trial["complete_audit_output_sha256"] = "e" * 64
    elif broken == "empty_hash":
        for row in controls["trials"]:
            row["complete_audit_output_sha256"] = ""
    elif broken == "provider":
        trial["provider_calls"] = 1
    elif broken == "writes":
        trial["production_writes"] = True
    elif broken == "skipped_footer":
        trial["footer_open_attempts"] -= 1
    elif broken in {"unsupported", "no_local", "extra_opens", "no_metadata_opens", "retry"}:
        key, value = {"unsupported": ("metadata_only_supported", False), "no_local": ("local_filesystem", False),
            "extra_opens": ("actual_footer_opens", 4), "no_metadata_opens": ("metadata_only_opens", 0),
            "retry": ("canonical_retries", 1)}[broken]
        trial["footer_io"][key] = value
    elif broken == "native_old_io":
        attempt["derivative_metadata_audit"]["footer_io"]["metadata_only_supported"] = False
        published["derivative_metadata_audit"] = deepcopy(attempt["derivative_metadata_audit"])
    else:
        published["l0_deleted"] = True
    with pytest.raises(ValueError):
        observer.validate(unit, invocation, attempt, published, fixture, controls)


def test_native_module_allows_only_exact_capability_and_observational_snapshot():
    reference = """import hashlib
def _mark_stale_segments():
    footer_open_attempts = 0
    if True:
            parquet_file = pq.ParquetFile(str(path))
    audit.update(
            footer_open_attempts=footer_open_attempts,
    )
def other():
    return 'unchanged source/accounting'
"""
    current = "from scripts.openbb_l1_footer_io import FooterOpener\n" + observer.metadata_io_source(reference)
    observer.validate_native_source(reference, current)
    for broken in (current.replace("unchanged source/accounting", "changed"),
                   current.replace("footer_opener(str(path))", "None"),
                   current.replace("import FooterOpener", "import FooterOpener as Other")):
        with pytest.raises(ValueError):
            observer.validate_native_source(reference, broken)


def test_current_complete_module_candidate_has_no_other_changes():
    reference = (Path(__file__).resolve().parents[1] / "scripts/compact_openbb_l1.py").read_text()
    # After adoption the portable fixture is already tested above; the current
    # source may no longer contain the original marker, which must be refused.
    if "    footer_opener = FooterOpener()\n" in reference:
        with pytest.raises(ValueError, match="shape changed"):
            observer.metadata_io_source(reference)
        return
    current = "from scripts.openbb_l1_footer_io import FooterOpener\n" + observer.metadata_io_source(reference)
    observer.validate_native_source(reference, current)


def test_rejected_prefetch_cleanup_can_change_but_measured_constructor_cannot():
    reference = """POLICY = 'fixed'
class FooterOpener:
    def __call__(self, path):
        return native_reader(path)
def prefetched_footer_rows():
    return 'old cleanup'
"""
    observer.validate_metadata_helper_source(reference, reference.replace("old cleanup", "new cleanup"))
    for broken in (reference.replace("native_reader(path)", "None"), reference.replace("fixed", "changed"),
                   reference.replace("FooterOpener", "Other")):
        with pytest.raises(ValueError, match="serial metadata"):
            observer.validate_metadata_helper_source(reference, broken)

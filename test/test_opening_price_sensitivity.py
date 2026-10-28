import math

import pytest

from scripts.analyze_opening_price_sensitivity import (
    _execution_stage_clocks, compare_complete_runs, compare_weights, compact_signal_summary,
    completion_percentiles, signal_analysis,
)


def test_completion_uses_full_official_denominator_and_every_percent():
    result = completion_percentiles({"requested_count": 3, "symbols": [
        {"symbol": "a", "first_valid_by_provider_ms": {"broker": 200, "mis": 300}},
        {"symbol": "b", "first_valid_by_provider_ms": {"mis": 900}},
        {"symbol": "c", "first_valid_by_provider_ms": {}},
    ]})
    rows = result["merged"]["percentiles"]
    assert len(rows) == 100
    assert rows[0] == {"completion_percent": 1, "required_count": 1, "after_open_ms": 200}
    assert rows[32]["after_open_ms"] == 200
    assert rows[33]["after_open_ms"] == 900
    assert rows[65]["after_open_ms"] == 900
    assert rows[66]["after_open_ms"] is None
    assert rows[99]["after_open_ms"] is None
    assert result["merged"]["final_true_open_count"] == 2
    assert result["broker"]["percentiles"][33]["after_open_ms"] is None


@pytest.mark.parametrize("bad", [-1, True, math.nan, math.inf, "2"])
def test_completion_rejects_invalid_clocks(bad):
    with pytest.raises(ValueError, match="clock"):
        completion_percentiles({"requested_count": 1, "symbols": [
            {"symbol": "a", "first_valid_by_provider_ms": {"broker": bad}}]})


def test_completion_rejects_duplicate_official_axis():
    with pytest.raises(ValueError, match="axis"):
        completion_percentiles({"requested_count": 2, "symbols": [
            {"symbol": "a", "first_valid_by_provider_ms": {}},
            {"symbol": "a", "first_valid_by_provider_ms": {}}]})


def test_zero_result_failed_providers_remain_visible():
    result = completion_percentiles({"requested_count": 1,
        "failure_jobs": [{"provider": "denied"}], "symbols": [
            {"symbol": "a", "first_valid_by_provider_ms": {}, "deadlines": {
                "1000": {"source_states": {"old_session": "stale_or_preopen_time"}}}}]})
    assert set(result) == {"merged", "denied", "old_session"}
    assert all(source["final_true_open_count"] == 0 for source in result.values())
    assert all(row["after_open_ms"] is None for row in result["denied"]["percentiles"])


def test_sensitivity_aligns_symbols_and_counts_direction_and_l1_in_pp():
    scenario = [{"symbol": "a", "model_weight": .1}, {"symbol": "b", "model_weight": -.2},
        {"symbol": "c", "model_weight": 0}, {"symbol": "d", "model_weight": .4}]
    reference = [{"symbol": "d", "model_weight": 0}, {"symbol": "b", "model_weight": .3},
        {"symbol": "a", "model_weight": .1}, {"symbol": "c", "model_weight": -.1}]
    result = compare_weights(scenario, reference)
    assert result["changed_count"] == 3
    assert result["direction_flips"] == 1
    assert result["only_scenario_active"] == result["only_reference_active"] == 1
    assert result["active_jaccard"] == .5
    assert result["l1_difference_percentage_points"] == pytest.approx(100)
    assert result["max_difference_percentage_points"] == 50
    assert [row["symbol"] for row in result["changed_symbols"]] == ["b", "d", "c"]
    assert compare_weights(scenario, scenario)["changed_count"] == 0


def test_empty_signals_are_same_but_not_empty_axes():
    zero = [{"symbol": "a", "model_weight": 0.}]
    assert compare_weights(zero, zero)["active_jaccard"] == 1
    with pytest.raises(ValueError, match="axes"):
        compare_weights([], [])


@pytest.mark.parametrize("other", [
    [{"symbol": "b", "model_weight": .2}],
    [{"symbol": "a", "model_weight": .2}, {"symbol": "a", "model_weight": .1}],
    [{"symbol": "a", "model_weight": math.nan}],
])
def test_incompatible_signal_axes_fail_closed(other):
    with pytest.raises(ValueError):
        compare_weights([{"symbol": "a", "model_weight": .1}], other)


@pytest.mark.parametrize("report", [{}, {"status": "failed_not_sensitivity_acceptance"},
    {"status": "completed_offline_sensitivity_not_live_acceptance", "endpoint_parity": {"repeat": False}}])
def test_summary_cannot_disguise_failed_or_unverified_analysis(report):
    with pytest.raises(ValueError, match="endpoint-verified"):
        compact_signal_summary(report)


@pytest.mark.parametrize("measured", [False, True])
def test_summary_preserves_distinct_complete_stage_clocks_without_filling_legacy_gaps(measured):
    fields = ("contract", "session_date", "feature_date", "receipt_sha256", "journal_prefix",
        "runtime_profile", "mounted_code_reference", "source_roots", "opening_reference_policy",
        "boundary", "full_price_reference_is_late_information_not_available_at_open", "warmups",
        "network_requests", "production_writes", "broker_fill_claim")
    report = dict.fromkeys(fields)
    report.update(status="completed_offline_sensitivity_not_live_acceptance", endpoint_parity={"repeat": True})
    row = {"label": "0", "after_open_ms": 0, "all_models_elapsed_ms": 10., "markets": []}
    clocks = {"all_signal_calculations_complete_ms": 2., "all_execution_pointers_published_ms": 2.,
              "all_execution_verified_ms": 8., "scenario_complete_including_reports_ms": 10.}
    if measured:
        row.update(clocks)
    report["scenarios"] = [row]
    result = compact_signal_summary(report)["scenarios"][0]
    assert {key: result[key] for key in clocks} == (clocks if measured else dict.fromkeys(clocks))
    assert result["all_models_elapsed_ms"] == 10.  # Legacy alias, never inferred as GPU time.


@pytest.mark.parametrize("execution_published", [False, True])
def test_no_execution_or_reused_revision_does_not_claim_a_fast_financial_commit(execution_published):
    clocks = _execution_stage_clocks(2., 8., execution_published=execution_published)
    assert clocks["all_execution_pointers_published_ms"] == (2. if execution_published else None)
    assert clocks["all_execution_verified_ms"] == (8. if execution_published else None)
    assert clocks["all_execution_commits_ms"] == 8.  # Preserve the historical no-work span, not a commit.


def _complete_comparison_run():
    return {"status": "completed_offline_sensitivity_not_live_acceptance",
        "endpoint_parity": {"end": True}, "session_date": "2026-10-22",
        "model_identities": {"mode": {"checkpoint": "one"}},
        "scenarios": [{"label": "0", "all_models_elapsed_ms": 10., "markets": [
            {"market": "mode", "weights": [{"symbol": "a", "model_weight": .1, "target_weight": .1}],
             "execution_probe": {"filled_shares": 1000}}]}]}


def test_complete_run_comparison_separates_output_parity_from_slo_and_economics():
    run = _complete_comparison_run()
    proof = compare_complete_runs(run, run)
    assert proof["all_weights_and_execution_quantities_equal"]
    assert proof["model_identity_attested_in_both_reports"]
    assert not proof["native_opening_slo_proven"]
    assert not proof["all_ledger_economics_proven"]
    assert not proof["all_generated_ledger_values_equal"]


def _complete_financial_oracle():
    from stockagent.live.tw_day_trade_service_sync import PAPER_JOURNAL_FILENAMES
    return {"contract": "complete_generated_paper_state_and_six_journals_v1",
        "state_sha256": "a" * 64,
        "journals": {name: {"present": True, "records": 1, "physical_lines": 1, "sha256": "b" * 64}
                     for name in PAPER_JOURNAL_FILENAMES},
        "in_memory_and_disk_complete_state_equal": True,
        "excluded_state_fields": ["engine_run_id", "dashboard_content_fingerprint"],
        "path_normalization": "exact_isolated_run_root_only",
        "unobserved_full_session_economics_proven": False}


@pytest.mark.parametrize("damage", [None, "state", "fees_in_fill", "missing_journal", "missing_proof", "bad_digest"])
def test_complete_financial_comparison_cannot_hide_economics_behind_equal_quantities(damage):
    import copy
    left = _complete_comparison_run()
    left["scenarios"][0]["markets"][0]["execution_probe"]["ledger_financial_oracle"] = _complete_financial_oracle()
    right = copy.deepcopy(left)
    probe = right["scenarios"][0]["markets"][0]["execution_probe"]
    oracle = probe["ledger_financial_oracle"]
    if damage == "state":
        oracle["state_sha256"] = "c" * 64
    elif damage == "fees_in_fill":
        oracle["journals"]["fills.jsonl"]["sha256"] = "c" * 64
    elif damage == "missing_journal":
        del oracle["journals"]["events.jsonl"]
    elif damage == "missing_proof":
        del probe["ledger_financial_oracle"]
    elif damage == "bad_digest":
        oracle["state_sha256"] = "not-a-sha"
    if damage in {"missing_journal", "missing_proof", "bad_digest"}:
        with pytest.raises(ValueError, match="financial oracle"):
            compare_complete_runs(left, right)
    else:
        proof = compare_complete_runs(left, right)
        assert proof["all_generated_ledger_values_equal"] is (damage is None)
        assert bool(proof["financial_mismatches"]) is (damage is not None)
        assert proof["all_weights_and_execution_quantities_equal"] is True
        assert (proof["status"] == "offline_outputs_equal") is (damage is None)
        assert not proof["all_ledger_economics_proven"]


@pytest.mark.parametrize("damage", ["weight", "quantity", "identity", "mode", "scenario", "failed"])
def test_complete_run_comparison_retains_and_rejects_each_changed_boundary(damage):
    import copy
    left = _complete_comparison_run()
    right = copy.deepcopy(left)
    row = right["scenarios"][0]["markets"][0]
    if damage == "weight":
        row["weights"][0]["model_weight"] += 1e-15
    elif damage == "quantity":
        row["execution_probe"]["filled_shares"] += 1000
    elif damage == "identity":
        right["model_identities"]["mode"]["checkpoint"] = "two"
    elif damage == "mode":
        row["market"] = "other"
    elif damage == "scenario":
        right["scenarios"] = []
    else:
        right["status"] = "failed_not_sensitivity_acceptance"
    if damage in {"weight", "quantity"}:
        proof = compare_complete_runs(left, right)
        assert not proof["all_weights_and_execution_quantities_equal"]
        assert proof["status"] == "rejected_output_parity"
    else:
        with pytest.raises(ValueError):
            compare_complete_runs(left, right)


@pytest.mark.parametrize("options", [{"parallel_inputs": True}, {"repeat_prepare_control": True},
    {"overlap_ledger": True}, {"overlap_ledger": True, "progressive_execution": True}])
def test_signal_probe_rejects_incoherent_scheduling_before_any_source_read(options):
    with pytest.raises(ValueError, match="requires|require"):
        signal_analysis({}, feature_date="unused", runtime_profile=None, output=None, **options)

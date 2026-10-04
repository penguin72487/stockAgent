from datetime import UTC, datetime, timedelta

import pytest

from downloader.acquisition_policy import evaluate_snapshot, source_admission_class

NOW = datetime(2026, 9, 27, tzinfo=UTC)


def row(name="official:price", **kw):
    return {"id": name, "scope": "logical_source", "in_active_scope": True,
            "automation_eligible": True, "acquisition_progress": {
                "coverage_complete": True, "up_to_date": True, "state": "complete"}, **kw}


def evaluate(rows, **kw):
    return evaluate_snapshot({"sources": rows, "generated_at_utc": NOW.isoformat(),
                              "schema_version": 8, "read_only": True,
                              "summary": {"registered_items": len(rows)},
                              "integrity_checks": {"state": "pass", "violations": 0}},
                             now=NOW, finlab_core_complete=kw.get("finlab", True),
                             finmind_required=kw.get("finmind", 0))


@pytest.mark.parametrize("state", ["blocked", "deferred", "stale_complete_receipt", "pending"])
def test_waiting_required_work_never_grants_surplus(state):
    result = evaluate([row(acquisition_progress={"state": state, "coverage_complete": False})])
    assert not result["allowed"]
    assert result["blocking_counts"] == {state: 1}


def test_waiting_publication_is_idle_only_after_coverage():
    progress = {"state": "waiting_publication", "coverage_complete": True, "up_to_date": False}
    assert evaluate([row(acquisition_progress=progress)])["allowed"]
    progress["coverage_complete"] = False
    assert not evaluate([row(acquisition_progress=progress)])["allowed"]


def test_ui_complete_does_not_replace_proof():
    assert not evaluate([row(operation_state="complete", acquisition_progress=None)])["allowed"]


def test_conflicting_progress_flags_fail_closed():
    assert not evaluate([row(acquisition_progress={"state": "failed", "coverage_complete": True,
                                                   "up_to_date": True})])["allowed"]


def test_alias_planned_stream_and_pit_are_not_duplicate_acquisition_debt():
    rows = [row(), row("alias", registry_alias=True, acquisition_progress=None),
            row("planned", automation_eligible=False, acquisition_progress=None),
            row("stream", automation={"mode": "stream"}, acquisition_progress=None),
            row("tw-public:provisional-feature:twpub_cbc_m2_raw", acquisition_progress=None)]
    result = evaluate(rows)
    assert result["allowed"]
    assert result["classification_counts"]["not_automatable_not_acquired"] == 1
    assert source_admission_class(row("tw-public:cbc_money_release_vintages")) == "required_acquisition"


def test_finlab_uses_fresh_local_core_not_catalog_alias_completion():
    rows = [row(), row("finlab:catalog-acquisition")]
    assert not evaluate(rows, finlab=None)["allowed"]
    assert not evaluate(rows, finlab=False)["allowed"]
    assert evaluate(rows, finlab=True)["allowed"]


def test_mixed_finmind_endpoint_uses_primary_queue_not_secondary_tail():
    rows = [row(), row("finmind:sponsor:TaiwanStockPrice", acquisition_progress=None)]
    assert not evaluate(rows, finmind=1)["allowed"]
    assert not evaluate(rows, finmind=None)["allowed"]
    assert evaluate(rows, finmind=0)["allowed"]


@pytest.mark.parametrize("age", [timedelta(minutes=16), timedelta(minutes=-2)])
def test_stale_or_future_registry_fails_closed(age):
    result = evaluate_snapshot({"sources": [row()], "generated_at_utc": (NOW-age).isoformat()},
                              now=NOW, finlab_core_complete=True, finmind_required=0)
    assert result["reason"] == "acquisition_snapshot_stale"
    assert not result["allowed"]


def test_empty_duplicate_and_reference_only_registry_cannot_grant():
    assert not evaluate([])["allowed"]
    assert not evaluate([row(), row()])["allowed"]
    assert not evaluate([row(registry_alias=True)])["allowed"]


def test_blocker_payload_is_bounded_but_count_is_exact():
    result = evaluate([row(str(i), acquisition_progress=None) for i in range(150)])
    assert len(result["blocked_endpoints"]) == 100
    assert result["blocked_endpoint_count"] == 150


def test_truncated_registry_does_not_release_quota():
    result = evaluate_snapshot({"sources": [row()], "generated_at_utc": NOW.isoformat(),
                                "schema_version": 8, "read_only": True,
                                "summary": {"registered_items": 100},
                                "integrity_checks": {"state": "pass", "violations": 0}},
                               now=NOW, finlab_core_complete=True, finmind_required=0)
    assert result["reason"] == "acquisition_registry_incomplete_or_invalid"
    assert not result["allowed"]


def test_projection_requires_actual_owner_and_cannot_hide_its_debt():
    projection = row("binance-feature:trade_candles_1m", acquisition_progress=None)
    assert not evaluate([row(), projection])["allowed"]
    owner = row("product:binance_usdm_perpetuals:1m")
    assert evaluate([owner, projection])["allowed"]
    owner["acquisition_progress"] = None
    assert not evaluate([owner, projection])["allowed"]


def test_snapshot_needs_local_proof_not_infinite_history_denominator():
    assert not evaluate([row(), row("finmind:TaiwanStockTradingDate")])["allowed"]


@pytest.mark.parametrize('required,unknown,expected', [(0, 0, True), (1, 0, False), (0, 1, False)])
def test_finmind_opt_in_only_uses_spare_local_quota(tmp_path, monkeypatch, required, unknown, expected):
    import json
    from downloader import acquisition_policy as policy
    config = tmp_path / 'policy.json'
    config.write_text(json.dumps({'schema_version': 1, 'secondary_validation': 'provider_spare_quota'}))
    monkeypatch.setattr(policy, '_finmind_spare_work', lambda *_a: {
        'state': 'partial' if unknown else 'observed',
        'summary': {'required_requests': required, 'unknown_datasets': unknown, 'blocked_tasks': 13},
    })
    def unrelated_gate(**_kwargs):
        raise AssertionError('opt-in FinMind quota is independent of other providers')
    monkeypatch.setattr(policy, 'evaluate_secondary_admission', unrelated_gate)
    result = policy.evaluate_finmind_secondary_admission(root=tmp_path, now=NOW, config_path=config)
    assert result['allowed'] is expected
    assert result['scope'] == 'finmind_shared_account'
    assert result['blocked_tasks'] == 13  # No fake completion for terminal errors.


def test_finmind_missing_policy_preserves_global_gate_and_invalid_policy_never_grants(tmp_path, monkeypatch):
    from downloader import acquisition_policy as policy
    monkeypatch.setattr(policy, 'evaluate_secondary_admission', lambda **_kw: {
        'allowed': False, 'reason': 'global_still_required'})
    path = tmp_path / 'policy.json'
    assert policy.evaluate_finmind_secondary_admission(config_path=path)['reason'] == 'global_still_required'
    path.write_text('{"schema_version": 1, "secondary_validation": "typo"}')
    assert not policy.evaluate_finmind_secondary_admission(config_path=path)['allowed']

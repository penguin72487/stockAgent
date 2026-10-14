"""Scope an engineering fence to the actual history computation inputs."""
import json

from scripts import benchmark_tw_catalog_prewarm as benchmark


def test_heartbeat_is_observed_separately_but_consumed_state_fields_are_never_ignored(tmp_path):
    path = tmp_path / "state.json"
    state = {"product": "tw_day_trade", "enabled_markets": ["a", "b"], "updated_at": "first"}
    path.write_text(json.dumps(state))
    before = benchmark.source_inventory(tmp_path)
    metadata = benchmark.signature([path])
    state["updated_at"] = "next actual heartbeat"
    path.write_text(json.dumps(state))
    assert benchmark.source_inventory(tmp_path) == before
    assert benchmark.signature([path]) != metadata
    for field, value in (("product", "tw_overnight"), ("enabled_markets", ["a", "c"])):
        changed = {**state, field: value}
        path.write_text(json.dumps(changed))
        assert benchmark.source_inventory(tmp_path) != before


def test_source_append_still_rejects_an_index_or_curve_generation_change(tmp_path):
    before = benchmark.source_inventory(tmp_path)
    (tmp_path / "marks.jsonl").write_text('{"session_date":"2026-10-13"}\n')
    assert benchmark.source_inventory(tmp_path) != before


def test_absent_and_broken_state_follow_existing_history_default_not_fabricated_values(tmp_path):
    before = benchmark.source_inventory(tmp_path)
    (tmp_path / "state.json").write_text("{broken")
    assert benchmark.source_inventory(tmp_path) == before
    assert before["history_state_consumed_fields"] == {"product": None, "enabled_markets": None}

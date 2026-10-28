import hashlib
import json

import polars as pl
import pytest

from scripts.replay_tw_day_trade_signal_targets import replay_targets


def _event(tmp_path, signal_id, rows, *, errors=(), registered=False):
    directory = tmp_path / signal_id
    directory.mkdir()
    summary = directory / "summary.json"
    weights = directory / "target_weights.parquet"
    summary.write_text(json.dumps({"market": "m", "signal_id": signal_id}))
    pl.DataFrame(rows).write_parquet(weights)
    sha = lambda path: hashlib.sha256(path.read_bytes()).hexdigest()
    return {"signal_id": signal_id, "summary_path": str(summary), "summary_sha256": sha(summary),
            "weights_path": str(weights), "weights_sha256": sha(weights),
            "source_errors_blocking_replay": list(errors),
            "was_registered_in_original_ledger": registered}


def _plan(events):
    return {"contract": "published_revision_causal_minute_replay_plan_v1", "simulation_only": True,
            "production_order_possible": False, "ledger_modified": False,
            "session_date": "2026-10-28", "event_count": len(events),
            "modes": {"m": {"publication_count": len(events), "events": events}}}


def test_replays_each_pinned_target_without_fabricating_fills(tmp_path):
    first = _event(tmp_path, "first", {"symbol": ["a", "b"], "target_weight": [0.1, 0.2]}, registered=True)
    second = _event(tmp_path, "second", {"symbol": ["a", "b", "c"],
        "target_weight": [-0.1, 0.0, 0.3]}, errors=["source_clock_conflict"])
    report = replay_targets(_plan([first, second]))
    assert report["event_count"] == 2 and report["source_clock_blocked_event_count"] == 1
    assert report["fills_fees_curves_and_restart_accepted"] is False
    assert report["modes"]["m"]["registered_event_count"] == 1
    a, b = report["modes"]["m"]["events"]
    assert (a["changed_targets"], a["opened_targets"]) == (2, 2)
    assert (b["changed_targets"], b["opened_targets"], b["removed_targets"], b["direction_flips"]) == (3, 1, 1, 1)
    assert b["target_change_l1"] == pytest.approx(0.7)
    assert b["source_errors_blocking_execution_replay"] == ["source_clock_conflict"]


def test_refuses_mutated_pinned_source(tmp_path):
    event = _event(tmp_path, "first", {"symbol": ["a"], "target_weight": [0.1]})
    with open(event["summary_path"], "ab") as stream:
        stream.write(b" ")
    with pytest.raises(ValueError, match="hash mismatch"):
        replay_targets(_plan([event]))


@pytest.mark.parametrize("symbols,weights", [(["a", "a"], [0.1, 0.2]),
                                              (["a"], [float("nan")]),
                                              (["a"], [None])])
def test_refuses_invalid_targets(tmp_path, symbols, weights):
    event = _event(tmp_path, "first", {"symbol": symbols, "target_weight": weights})
    with pytest.raises(ValueError):
        replay_targets(_plan([event]))


def test_rejects_incomplete_or_non_paper_plan(tmp_path):
    event = _event(tmp_path, "first", {"symbol": ["a"], "target_weight": [0.1]})
    plan = _plan([event])
    plan["event_count"] = 2
    with pytest.raises(ValueError, match="count"):
        replay_targets(plan)
    plan["production_order_possible"] = True
    with pytest.raises(ValueError, match="read-only"):
        replay_targets(plan)

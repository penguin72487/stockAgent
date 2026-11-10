from datetime import date
import json
from pathlib import Path

import polars as pl
import pytest

from scripts.audit_tw_day_trade_observed_signal import audit, conservative_clock_bounds


@pytest.fixture
def observed_signal(tmp_path: Path, monkeypatch):
    monkeypatch.setenv("STOCKAGENT_DASHBOARD_INDEX_CACHE_DIR", str(tmp_path / "cache"))
    root = tmp_path / "ledger"
    root.mkdir()
    source = tmp_path / "source"
    source.mkdir()
    summary = source / "summary.json"
    summary.write_text(json.dumps({
        "signal_id": "signal-1", "market": "fixture", "asof_date": "2026-11-02",
        "opening_signal_revision": {"index": 0},
        "signal_started_at": "2026-11-02T09:00:11.8+08:00",
        "artifact_published_at": "2026-11-02T09:00:11.6+08:00",
    }))
    pl.DataFrame({"symbol": ["2330", "2317"], "target_weight": [0.2, -0.1]}).write_parquet(
        source / "target_weights.parquet")
    (root / "events.jsonl").write_text(json.dumps({
        "market": "fixture", "session_date": "2026-11-02", "event": "signal_registered",
        "signal_id": "signal-1", "recorded_at": "2026-11-02T09:00:13+08:00",
    }) + "\n")
    rows = [{"market": "fixture", "session_date": "2026-11-02", "signal_id": "signal-1",
             "simulation_replay": False, "signal_source_path": str(summary),
             "execution_pointer_observed_at": "2026-11-02T09:00:12.250112+08:00",
             "symbol": symbol, "target_weight": weight}
            for symbol, weight in (("2330", 0.2), ("2317", -0.1))]
    (root / "signals.jsonl").write_text("".join(json.dumps(row) + "\n" for row in rows))
    return root, summary, rows


def test_observed_signal_audit_preserves_anomalous_clocks_and_exact_targets(observed_signal):
    root, summary, _ = observed_signal
    result = audit(state_dir=root, session=date(2026, 11, 2), market="fixture", summaries=[summary])
    item = result["signals"][0]
    assert item["target_rows"] == 2
    assert item["executor_observed_at"] == "2026-11-02T09:00:12.250112+08:00"
    assert item["original_clocks"]["artifact_published_at"] == "2026-11-02T09:00:11.6+08:00"
    assert result["execution_replay_validated"] is False
    assert result["prices_verified"] is False and result["overall_plan_ready"] is False


@pytest.mark.parametrize("change", ["wrong_weight", "duplicate", "after_registration"])
def test_observed_signal_audit_rejects_unproved_or_inconsistent_evidence(observed_signal, change):
    root, summary, rows = observed_signal
    if change == "wrong_weight":
        rows[0]["target_weight"] = 0.2001
    elif change == "duplicate":
        rows.append(dict(rows[0]))
    else:
        for row in rows:
            row["execution_pointer_observed_at"] = "2026-11-02T09:00:14.000000+08:00"
    (root / "signals.jsonl").write_text("".join(json.dumps(row) + "\n" for row in rows))
    with pytest.raises(RuntimeError):
        audit(state_dir=root, session=date(2026, 11, 2), market="fixture", summaries=[summary])


def _clock_proof():
    return {"status": "observed_signal_evidence_only", "simulation_only": True,
            "production_order_possible": False, "execution_replay_validated": False,
            "overall_plan_ready": False, "market": "m", "session_date": "2026-11-02",
            "signals": [{"signal_id": "s", "target_rows": 2, "matching_target_weights": True,
                "summary_path": "/source/summary.json", "summary_sha256": "a" * 64,
                "weights_path": "/source/target_weights.parquet", "weights_sha256": "b" * 64,
                "executor_observed_at": "2026-11-02T09:00:12.250112+08:00",
                "registered_at_second_precision": "2026-11-02T09:00:12+08:00",
                "original_clocks": {"generated_at": "2026-11-02T09:00:11+08:00",
                    "signal_started_at": "2026-11-02T09:00:11.828675+08:00",
                    "signal_ready_at": "2026-11-02T09:00:11.579082+08:00",
                    "artifact_published_at": "2026-11-02T09:00:11.673520+08:00",
                    "artifact_completed_at": "2026-11-02T09:00:16.150364+08:00",
                    "execution_pointer_written_at": "2026-11-02T09:00:11.809466+08:00"}}]}


def test_conservative_clock_bound_preserves_conflict_and_uses_later_evidence():
    proof = _clock_proof()
    result = conservative_clock_bounds(proof)
    row = result["signals"][0]
    assert row["conservative_not_before"] == "2026-11-02T09:00:16.150364+08:00"
    assert row["original_clocks"] == proof["signals"][0]["original_clocks"]
    assert row["producer_clock_anomalies"] == ["start_after_declared_publication"]
    assert row["bound_is_actual_publication_time"] is False
    assert result["fills_or_price_evidence_verified"] is False
    assert result["publication_authorized"] is False and result["ledger_modified"] is False


def test_conservative_clock_bound_respects_second_precision_registration():
    proof = _clock_proof()
    proof["signals"][0]["original_clocks"]["artifact_completed_at"] = "2026-11-02T09:00:12.5+08:00"
    row = conservative_clock_bounds(proof)["signals"][0]
    assert row["conservative_not_before"] == "2026-11-02T09:00:13+08:00"


def test_bound_is_derived_from_fresh_audit_of_original_targets(observed_signal):
    root, summary, _ = observed_signal
    source = json.loads(summary.read_text())
    source.update({
        "generated_at": "2026-11-02T09:00:11+08:00",
        "signal_ready_at": "2026-11-02T09:00:11.5+08:00",
        "artifact_completed_at": "2026-11-02T09:00:16.15+08:00",
        "execution_pointer_written_at": "2026-11-02T09:00:11.7+08:00",
    })
    summary.write_text(json.dumps(source))
    original_bytes = (root / "signals.jsonl").read_bytes()
    proof = audit(state_dir=root, session=date(2026, 11, 2), market="fixture", summaries=[summary])
    row = conservative_clock_bounds(proof)["signals"][0]
    assert row["conservative_not_before"] == "2026-11-02T09:00:16.150000+08:00"
    assert row["producer_clock_anomalies"] == ["start_after_declared_publication"]
    assert row["source_targets_exactly_match"] is True
    assert row["summary_sha256"] == proof["signals"][0]["summary_sha256"]
    assert (root / "signals.jsonl").read_bytes() == original_bytes


@pytest.mark.parametrize("damage", ["naive", "wrong_day", "late_observation", "unmatched", "duplicate", "already_replayed"])
def test_conservative_clock_bound_rejects_unusable_evidence(damage):
    proof = _clock_proof()
    item = proof["signals"][0]
    if damage == "naive":
        item["original_clocks"]["artifact_published_at"] = "2026-11-02T09:00:11"
    elif damage == "wrong_day":
        item["original_clocks"]["artifact_completed_at"] = "2026-11-03T09:00:16+08:00"
    elif damage == "late_observation":
        item["executor_observed_at"] = "2026-11-02T09:00:13+08:00"
    elif damage == "unmatched":
        item["matching_target_weights"] = False
    elif damage == "duplicate":
        proof["signals"].append(dict(item))
    else:
        proof["execution_replay_validated"] = True
    with pytest.raises((RuntimeError, ValueError)):
        conservative_clock_bounds(proof)

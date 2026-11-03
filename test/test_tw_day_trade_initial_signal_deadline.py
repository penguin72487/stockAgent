"""Publication evidence and replay identity must not depend on consumer lag."""

from datetime import datetime
import json
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

from scripts.run_tw_day_trade_simulation import (
    _initial_opening_signal_proof,
    _missed_opening_recovery_required,
    _read_initial_opening_rows,
)


TZ = ZoneInfo("Asia/Taipei")
DAY = "2026-10-28"
MARKET = "tw_day_trade_example"
ROOT_ID = f"{MARKET}-{DAY} 09:00:21-090029-root"


def at(clock: str) -> datetime:
    return datetime.fromisoformat(f"{DAY}T{clock}").replace(tzinfo=TZ)


def original(published: str | None = "09:00:31") -> dict:
    result = {
        "market": MARKET,
        "signal_id": ROOT_ID,
        "asof_date": f"{DAY} 09:00:21",
        "artifact_published_at": f"{DAY}T09:00:29+08:00",
        "opening_signal_revision": {"index": 0, "root_signal_id": ROOT_ID},
    }
    if published:
        result["execution_pointer_written_at"] = f"{DAY}T{published}+08:00"
    return result


def setup_revision(tmp_path, published="09:00:31"):
    spec = SimpleNamespace(live_output_dir=tmp_path, market=MARKET, signal_market=None)
    directory = tmp_path / f"{DAY} 09:00:21" / ROOT_ID
    directory.mkdir(parents=True)
    root = original(published)
    (directory / "summary.json").write_text(json.dumps(root), encoding="utf-8")
    (directory / "execution_weights.json").write_text(json.dumps({
        "schema_version": 1,
        "market": MARKET,
        "signal_id": ROOT_ID,
        "rows": [{"symbol": "2330", "target_weight": 0.1}],
    }), encoding="utf-8")
    revision = {
        "market": MARKET,
        "signal_id": f"{MARKET}-{DAY}T09:02:00+08:00-revision",
        "asof_date": f"{DAY}T09:02:00+08:00",
        "execution_pointer_written_at": f"{DAY}T09:02:02+08:00",
        "opening_signal_revision": {"index": 1, "root_signal_id": ROOT_ID},
    }
    return spec, root, revision, directory


@pytest.mark.parametrize(("clock", "late"), [
    ("09:00:15", False), ("09:00:31", False),
    ("09:00:59.999999", False), ("09:01:00", True),
])
def test_replay_deadline_is_initial_publication_not_fifteen_second_slo(clock, late):
    assert _missed_opening_recovery_required(at(clock)) is late


def test_on_time_initial_signal_remains_live_after_late_consumer_and_revision(tmp_path):
    spec, root, revision, _ = setup_revision(tmp_path)
    proof = _initial_opening_signal_proof(spec, revision, at("09:02:03"))
    assert proof is not None
    initial, path, published, proof_kind = proof
    assert initial == root and path is not None
    assert published == at("09:00:31")
    assert proof_kind == "execution_pointer_written_at"
    assert not _missed_opening_recovery_required(published)


def test_late_initial_replay_reads_original_not_later_revision_weights(tmp_path):
    spec, root, revision, _ = setup_revision(tmp_path, "09:01:01")
    initial, path, published, _ = _initial_opening_signal_proof(spec, revision, at("09:02:03"))
    assert _missed_opening_recovery_required(published)
    assert initial == root
    assert _read_initial_opening_rows(spec, initial, path) == [
        {"symbol": "2330", "target_weight": 0.1}
    ]


def test_first_pointer_read_before_deadline_is_itself_timeliness_evidence(tmp_path):
    spec = SimpleNamespace(live_output_dir=tmp_path, market=MARKET, signal_market=None)
    proof = _initial_opening_signal_proof(spec, original(None), at("09:00:55"))
    assert proof is not None and proof[2:] == (at("09:00:55"), "pointer_observed_by")


@pytest.mark.parametrize("clock", ["08:59:59.999999", "08:30:00"])
def test_preopen_artifact_cannot_become_a_live_signal_by_late_pointer_read(tmp_path, clock):
    spec = SimpleNamespace(live_output_dir=tmp_path, market=MARKET, signal_market=None)
    summary = original(None)
    summary["artifact_published_at"] = f"{DAY}T{clock}+08:00"
    assert _initial_opening_signal_proof(spec, summary, at("09:00:55")) is None


def test_pointer_read_at_open_still_requires_a_valid_artifact_clock(tmp_path):
    spec = SimpleNamespace(live_output_dir=tmp_path, market=MARKET, signal_market=None)
    for clock in (None, f"{DAY}T09:00:01", "not-a-date", "2026-10-27T09:00:01+08:00"):
        summary = original(None)
        summary["artifact_published_at"] = clock
        assert _initial_opening_signal_proof(spec, summary, at("09:00:55")) is None


def test_artifact_time_is_not_pointer_publication_evidence_after_deadline(tmp_path):
    spec = SimpleNamespace(live_output_dir=tmp_path, market=MARKET, signal_market=None)
    assert _initial_opening_signal_proof(spec, original(None), at("09:01:05")) is None


def test_revision_with_missing_or_ambiguous_original_fails_closed(tmp_path):
    spec, _, revision, directory = setup_revision(tmp_path)
    (directory / "summary.json").unlink()
    assert _initial_opening_signal_proof(spec, revision, at("09:02:03")) is None
    (directory / "summary.json").write_text(json.dumps(original()), encoding="utf-8")
    duplicate = tmp_path / f"{DAY}T09:00:21" / ROOT_ID
    duplicate.mkdir(parents=True)
    (duplicate / "summary.json").write_text(json.dumps(original()), encoding="utf-8")
    assert _initial_opening_signal_proof(spec, revision, at("09:02:03")) is None


def test_replay_weights_require_original_signal_identity(tmp_path):
    spec, root, revision, directory = setup_revision(tmp_path, "09:01:01")
    initial, path, _, _ = _initial_opening_signal_proof(spec, revision, at("09:02:03"))
    (directory / "execution_weights.json").write_text(json.dumps({
        "schema_version": 1, "market": MARKET, "signal_id": "different",
        "rows": [{"symbol": "2330", "target_weight": 1.0}],
    }), encoding="utf-8")
    assert initial == root and _read_initial_opening_rows(spec, initial, path) is None


@pytest.mark.parametrize("clock", ["not-a-date", f"{DAY}T09:00:31", f"{DAY}T09:03:00+08:00"])
def test_invalid_naive_or_future_publication_time_fails_closed(tmp_path, clock):
    spec = SimpleNamespace(live_output_dir=tmp_path, market=MARKET, signal_market=None)
    summary = original()
    summary["execution_pointer_written_at"] = clock
    assert _initial_opening_signal_proof(spec, summary, at("09:02:03")) is None

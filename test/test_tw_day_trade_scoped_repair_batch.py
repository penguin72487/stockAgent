import hashlib
import json
from copy import deepcopy
from dataclasses import replace

import pytest

from scripts import prepare_tw_day_trade_scoped_repair_batch as batch
from scripts import repair_tw_day_trade_flat_opening as repair
from stockagent.live import paper_journal
from stockagent.live.tw_day_trade_simulation import TwDayTradeSimulationEngine
from test_tw_day_trade_simulation import _now, _spec


def _digest(data):
    return hashlib.sha256(data).hexdigest()


@pytest.mark.parametrize("managed", [False, True])
def test_private_append_retains_exact_prefix_and_immutable_journal(tmp_path, managed):
    source = tmp_path / "candidate" / "signals.jsonl"
    target = tmp_path / "merged" / "signals.jsonl"
    source.parent.mkdir()
    target.parent.mkdir()
    old, new = b'{"old":1}\n', b'{"new":2}\n'
    source.write_bytes(new)
    target.write_bytes(old)
    if managed:
        paper_journal.publish_snapshot(target, operation="seed")
    capacity = []
    assert batch._append_pinned(source, target, _digest(new), capacity.append) == len(new)
    with paper_journal.open_paper_journal(target) as handle:
        assert handle.read() == old + new
    assert sum(capacity) == len(new)


def test_private_append_rejects_torn_source_prefix_before_writing(tmp_path):
    source, target = tmp_path / "source", tmp_path / "signals.jsonl"
    source.write_bytes(b'{"new":2}\n')
    target.write_bytes(b'{"old":1}')
    with pytest.raises(RuntimeError, match="torn final row"):
        batch._append_pinned(source, target, _digest(source.read_bytes()), lambda _: None)
    assert target.read_bytes() == b'{"old":1}'


def test_private_append_rejects_candidate_changed_since_audit(tmp_path):
    source, target = tmp_path / "source", tmp_path / "signals.jsonl"
    source.write_bytes(b'{"new":2}\n')
    target.write_bytes(b'{"old":1}\n')
    with pytest.raises(RuntimeError, match="changed during append"):
        batch._append_pinned(source, target, _digest(b'different'), lambda _: None)
    # Failed bytes are confined to this disposable candidate, never the live ledger.


def test_inventory_rejects_symlinks_and_ignores_only_volatile_state(tmp_path):
    (tmp_path / "financial.jsonl").write_text("{}\n")
    (tmp_path / "state.json").write_text("{}")
    (tmp_path / "quote_broker").mkdir()
    (tmp_path / "quote_broker" / "request.json").write_text("{}")
    assert set(batch._inventory(tmp_path)) == {"financial.jsonl"}
    (tmp_path / "redirect").symlink_to(tmp_path / "financial.jsonl")
    with pytest.raises(RuntimeError, match="symlink"):
        batch._inventory(tmp_path)


@pytest.mark.parametrize("defect", ["position", "unknown_quantity", "divergence", "commit", "not_paper"])
def test_flat_gate_fails_closed(defect):
    state = {"production_order_possible": False,
             "modes": {"m": {"positions": {"p": {"signed_shares": 0}}}}}
    if defect == "position":
        state["modes"]["m"]["positions"]["p"]["signed_shares"] = 1000
    elif defect == "unknown_quantity":
        state["modes"]["m"]["positions"]["p"]["signed_shares"] = None
    elif defect == "divergence":
        state["modes"]["m"]["ledger_state_divergence"] = {"kind": "unknown"}
    elif defect == "commit":
        state["modes"]["m"]["research_terminal_pending_commit"] = True
    else:
        state["production_order_possible"] = True
    with pytest.raises(RuntimeError):
        batch._require_flat(state)


@pytest.fixture
def simple_batch(tmp_path):
    live, merged, source = tmp_path / "live", tmp_path / "merged", tmp_path / "source"
    merged.mkdir()
    source.mkdir()
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    spec = replace(_spec(workspace), market="target")
    engine = TwDayTradeSimulationEngine(live)
    engine.update_readiness([spec], now=_now(8, 45))
    mode = engine.state["modes"]["target"]
    mode.update(session_date="2026-08-12", signal_id="previous", positions={},
                pending_entry_orders={}, pending_reduction_orders={})
    for name in batch.JOURNALS:
        (live / name).touch(exist_ok=True)
        (source / name).write_text(json.dumps({"market": "target", "session_date": "2026-08-13"}) + "\n")
    engine._persist(_now(8, 45))
    original = batch._read_committed_state(live)
    old_mode = original["modes"]["target"]
    new_mode = deepcopy(old_mode)
    new_mode.update(session_date="2026-08-13", signal_id="new", execution_evidence_complete=False)
    manifest = {"market": "target", "sessions": [{"session_date": "2026-08-13"}],
                "anchor": repair._anchor(old_mode), "candidate_final_outcome": repair._outcome(new_mode)}
    candidate = {"manifest": manifest, "candidate": source, "mode": new_mode,
                 "expected_hashes": {name: repair._sha256(source / name) for name in batch.JOURNALS},
                 "manifest_path": "manifest", "candidate_directory": str(source),
                 "manifest_sha256": "m", "source_audit": "a", "source_audit_sha256": "s",
                 "accounting_audit": "b", "accounting_audit_sha256": "t"}
    return live, merged, candidate, original


def test_composition_creates_private_only_successor_and_preserves_original(simple_batch):
    live, merged, candidate, original = simple_batch
    old = {name: (live / name).read_bytes() for name in batch.JOURNALS}
    receipt = batch.compose(live, merged, [candidate], lambda _: None)
    assert receipt["status"] == "candidate_built_not_promoted"
    assert receipt["annual_minute_validation"] == "pending"
    assert batch._read_committed_state(live) == original
    composed = batch._read_committed_state(merged)
    assert composed["modes"]["target"]["session_date"] == "2026-08-13"
    for name in batch.JOURNALS:
        assert (live / name).read_bytes() == old[name]
        assert (merged / name).read_bytes().startswith(old[name])


def test_composition_rejects_changed_anchor_without_touching_original(simple_batch):
    live, merged, candidate, original = simple_batch
    candidate["manifest"]["anchor"]["total_equity_twd"] = -1
    with pytest.raises(RuntimeError, match="economic anchor changed"):
        batch.compose(live, merged, [candidate], lambda _: None)
    assert batch._read_committed_state(live) == original


def test_composition_rejects_a_moved_live_source(simple_batch, monkeypatch):
    live, merged, candidate, original = simple_batch
    real = batch.shutil.copytree

    def move_source(*args, **kwargs):
        result = real(*args, **kwargs)
        with (live / "events.jsonl").open("a") as handle:
            handle.write('{"market":"outside"}\n')
        return result

    monkeypatch.setattr(batch.shutil, "copytree", move_source)
    with pytest.raises(RuntimeError, match="live ledger changed"):
        batch.compose(live, merged, [candidate], lambda _: None)
    # The injected competing append remains observable; the merger never swaps.
    from stockagent.live.tw_day_trade_service_sync import PaperReadChanged
    with pytest.raises(PaperReadChanged):
        batch._read_committed_state(live)
    assert json.loads((live / "state.json").read_text())["modes"] == original["modes"]

from copy import deepcopy
import json
from pathlib import Path

import pytest

from stockagent.live import day_trade_addition as addition
from scripts import promote_tw_day_trade_replay as promoter


def write(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value))


@pytest.fixture
def staged(tmp_path, monkeypatch):
    old, new, target = [tmp_path / name for name in ("old", "new", "candidate")]
    account = {"initial_capital_twd": 10_000_000., "total_equity_twd": 10_000_012.,
               "session_date": "2026-10-12", "open_position_count": 0, "positions": {}}
    for root, market in ((old, "retained"), (new, "added")):
        root.mkdir()
        write(root / "state.json", {"modes": {market: deepcopy(account)}, "enabled_markets": [market]})
        write(root / "rebuild_receipt.json", {"sessions": [{"session_date": "2026-10-12"}]})
        for name in addition.LEDGERS:
            (root / name).write_text(json.dumps({"market": market})+"\n")
        (root / "marks.jsonl").write_text(json.dumps({"market": market})+"\n")
    monkeypatch.setattr(promoter, "_validate_rebuild", lambda root, **kw: {
        "session_dates": ["2026-10-12"], "session_count": 1, "signal_ledger_validation": {}})
    monkeypatch.setattr(promoter, "_validate_minute_curve_coverage", lambda *a, **kw: {"validated_rows": 540})
    addition.merge_addition(old, new, target, added_market="added")
    return old, new, target


def test_addition_keeps_exact_accounts_and_old_bytes(staged):
    old, new, target = staged
    before = json.loads((old / "state.json").read_text())
    result = addition.validate_addition(target, expected_markets={"retained", "added"})
    assert result["old_signal_pins_and_executions_unchanged"]
    assert json.loads((target / "state.json").read_text())["modes"]["retained"] == before["modes"]["retained"]
    for name in addition.LEDGERS:
        assert (target / name).read_bytes() == (old / name).read_bytes()+(new / name).read_bytes()
    assert not promoter._cutover_blockers(old, target, {"retained", "added"}, added_markets={"added"})


@pytest.mark.parametrize("which", ("old_prefix", "new_tail", "extra_tail", "account", "escape"))
def test_addition_rejects_every_mutation_boundary(staged, which):
    _, _, target = staged
    if which == "account":
        state = json.loads((target / "state.json").read_text())
        state["modes"]["retained"]["total_equity_twd"] += 1
        write(target / "state.json", state)
    elif which == "escape":
        receipt = json.loads((target / "rebuild_receipt.json").read_text())
        receipt["prior_state"] = "../old/state.json"
        write(target / "rebuild_receipt.json", receipt)
    else:
        path = target / "fills.jsonl"
        content = path.read_bytes()
        if which == "old_prefix":
            content = content.replace(b"retained", b"Retained")
        elif which == "new_tail":
            content = content.replace(b"added", b"Added")
        else:
            content += b"{}\n"
        path.write_bytes(content)
    with pytest.raises(ValueError):
        addition.validate_addition(target, expected_markets={"retained", "added"})


def test_addition_never_joins_open_accounts(staged, tmp_path):
    old, new, _ = staged
    state = json.loads((old / "state.json").read_text())
    state["modes"]["retained"]["positions"] = {"p": {"signed_shares": 1000}}
    write(old / "state.json", state)
    with pytest.raises(ValueError, match="inventory is open"):
        addition.merge_addition(old, new, tmp_path / "blocked", added_market="added")


def test_cutover_rechecks_original_account_snapshot(staged):
    old, _, target = staged
    state = json.loads((old / "state.json").read_text())
    state["modes"]["retained"]["total_equity_twd"] += 1
    write(old / "state.json", state)
    assert any("accounts changed" in message for message in promoter._cutover_blockers(
        old, target, {"retained", "added"}, added_markets={"added"}))


@pytest.mark.parametrize("name", addition.LEDGERS)
def test_cutover_rechecks_each_live_execution_stream(staged, name):
    old, _, target = staged
    path = old / name
    path.write_bytes(path.read_bytes()+b'{"after_snapshot":true}\n')
    assert any("execution bytes changed" in message for message in promoter._cutover_blockers(
        old, target, {"retained", "added"}, added_markets={"added"}))


def test_merge_accepts_only_flat_monotonic_mark_heartbeat(staged, tmp_path, monkeypatch):
    old, new, _ = staged
    state = json.loads((old / "state.json").read_text())
    state["modes"]["retained"]["last_mark_at"] = "2026-10-12T16:00:00+08:00"
    write(old / "state.json", state)
    original_copytree = addition.shutil.copytree

    def copy_with_heartbeat(source, target, **kwargs):
        result = original_copytree(source, target, **kwargs)
        if Path(source) == old:
            updated = deepcopy(state)
            updated["modes"]["retained"]["last_mark_at"] = "2026-10-12T16:01:00+08:00"
            write(old / "state.json", updated)
        return result

    monkeypatch.setattr(addition.shutil, "copytree", copy_with_heartbeat)
    target = tmp_path / "heartbeat_candidate"
    addition.merge_addition(old, new, target, added_market="added")
    assert json.loads((target / "state.json").read_text())["modes"]["retained"] == state["modes"]["retained"]
    assert not promoter._cutover_blockers(old, target, {"retained", "added"}, added_markets={"added"})
    assert addition.validate_addition(target, expected_markets={"retained", "added"})["old_signal_pins_and_executions_unchanged"]
    # The candidate itself must still be the exact retained snapshot.
    changed = json.loads((target / "state.json").read_text())
    changed["modes"]["retained"]["last_mark_at"] = "2026-10-12T16:02:00+08:00"
    write(target / "state.json", changed)
    with pytest.raises(ValueError, match="existing account was altered"):
        addition.validate_addition(target, expected_markets={"retained", "added"})


@pytest.mark.parametrize("mutation", ("cash", "unknown", "open", "backwards", "next_day", "naive", "absent", "malformed"))
def test_flat_heartbeat_comparison_rejects_non_proven_changes(mutation):
    before = {"retained": {"open_position_count": 0, "positions": {},
                          "cash_twd": 10_000_000., "last_mark_at": "2026-10-12T16:00:00+08:00"}}
    after = deepcopy(before)
    mode = after["retained"]
    mode["last_mark_at"] = "2026-10-12T16:01:00+08:00"
    if mutation == "cash":
        mode["cash_twd"] += 1
    elif mutation == "unknown":
        mode["unrecognised_heartbeat"] = True
    elif mutation == "open":
        mode["positions"] = {"p": {"signed_shares": 1000}}
    else:
        mode["last_mark_at"] = {"backwards": "2026-10-12T15:59:00+08:00",
                               "next_day": "2026-10-13T16:00:00+08:00",
                               "naive": "2026-10-12T16:01:00", "absent": None,
                               "malformed": "not a clock"}[mutation]
    assert not addition.retained_accounts_match(before, after)


def test_private_projection_cannot_export_weighted_raw_feature_proxies(tmp_path):
    from stockagent.live.tw_day_trade_dashboard import _read_signal_feature_drivers
    path = tmp_path / "summary.json"
    write(path, {"source_publication_contract": {"contract": "model_derived_paper_signals_only_v1",
        "feature_proxy_publication_allowed": False},
        "model_explanation": {"all_feature_drivers": [{"feature": "private", "activity_score": 123.}]}})
    assert _read_signal_feature_drivers(path) == []
    write(path, {"model_explanation": {"all_feature_drivers": [{"feature": "public", "activity_score": 123.}]}})
    assert _read_signal_feature_drivers(path)[0]["feature"] == "public"


def abort_activation(staged):
    old, _, target = staged
    state = json.loads((old / "state.json").read_text())
    state["modes"]["added"] = {"configured_enabled": False,
        "initial_capital_twd": 10_000_000., "total_equity_twd": 10_000_000.,
        "entry_fill_outcome": "no_fill", "entry_fill_count": 0, "entry_filled_shares": 0,
        "positions": {}, "open_position_count": 0, "session_date": "2026-10-12", "signal_id": "new-signal"}
    write(old / "state.json", state)
    for name, tail in {
            "signals.jsonl": {"market": "added", "status": "blocked", "filled_shares": 0},
            "events.jsonl": {"market": "added", "event": "signal_registered"},
            "latency.jsonl": {"market": "added", "simulation_only": True}}.items():
        with (old / name).open("a") as f:
            f.write(json.dumps(tail)+"\n")
    return old, target


def test_aborted_unfunded_activation_preserved_without_editing_live(staged):
    old, target = abort_activation(staged)
    prior = {name: (old / name).read_bytes() for name in (*addition.LEDGERS, "state.json")}
    proof = addition.preserve_aborted_activation(old, target)
    assert proof["superseded_account_unfunded"]
    assert all((old / name).read_bytes() == data for name, data in prior.items())
    assert addition.validate_addition(target, expected_markets={"retained", "added"})["aborted_activation_recovery"]
    assert not promoter._cutover_blockers(old, target, {"retained", "added"}, added_markets={"added"})
    assert (target / "addition_evidence/aborted_activation/signals.jsonl").read_text().count("blocked") == 1
    with (old / "events.jsonl").open("a") as f:
        f.write('{"market":"added","event":"signal_registered"}\n')
    assert promoter._cutover_blockers(old, target, {"retained", "added"}, added_markets={"added"})


@pytest.mark.parametrize("mutation", ("retained_pnl", "new_pnl", "fill_count", "inventory", "pending",
                                     "active", "fill", "order", "other_market", "unknown_event", "decision_fill", "prefix"))
def test_aborted_activation_never_relaxes_real_activity_or_scope(staged, mutation):
    old, target = abort_activation(staged)
    state = json.loads((old / "state.json").read_text())
    if mutation == "retained_pnl":
        state["modes"]["retained"]["total_equity_twd"] += 1
    elif mutation == "new_pnl":
        state["modes"]["added"]["total_equity_twd"] += 1
    elif mutation == "fill_count":
        state["modes"]["added"]["entry_fill_count"] = 1
    elif mutation == "inventory":
        state["modes"]["added"]["positions"] = {"p": {"signed_shares": 1000}}
    elif mutation == "pending":
        state["modes"]["added"]["pending_entry_orders"] = {"p": {}}
    elif mutation == "active":
        state["enabled_markets"].append("added")
    else:
        name, row = {
            "fill": ("fills.jsonl", {"market": "added"}),
            "order": ("orders.jsonl", {"market": "added"}),
            "other_market": ("events.jsonl", {"market": "retained", "event": "signal_registered"}),
            "unknown_event": ("events.jsonl", {"market": "added", "event": "financial_update"}),
            "decision_fill": ("signals.jsonl", {"market": "added", "status": "filled", "filled_shares": 1}),
            "prefix": ("signals.jsonl", {})}[mutation]
        path = old / name
        content = path.read_bytes()
        path.write_bytes(content.replace(b"retained", b"RETAINED", 1) if mutation == "prefix"
                         else content+(json.dumps(row)+"\n").encode())
    write(old / "state.json", state)
    with pytest.raises(ValueError):
        addition.preserve_aborted_activation(old, target)


def test_aborted_activation_archive_is_hash_bound(staged):
    old, target = abort_activation(staged)
    addition.preserve_aborted_activation(old, target)
    path = target / "addition_evidence/aborted_activation/events.jsonl"
    path.write_bytes(path.read_bytes()+b'{}\n')
    with pytest.raises(ValueError, match="archive differs"):
        addition.validate_addition(target, expected_markets={"retained", "added"})


def test_rebuild_after_aborted_activation_keeps_prior_streams_and_archive(staged, tmp_path):
    old, target = abort_activation(staged)
    _, new, _ = staged
    prior_streams = {name: (old / name).read_bytes() for name in addition.LEDGERS}
    fresh = tmp_path / "rebuilt_union"
    addition.merge_addition(old, new, fresh, added_market="added", aborted_activation_candidate=target)
    assert all((old / name).read_bytes() == before for name, before in prior_streams.items())
    receipt = json.loads((fresh / "rebuild_receipt.json").read_text())
    assert receipt["aborted_activation"]
    assert addition.validate_addition(fresh, expected_markets={"retained", "added"})["aborted_activation_recovery"]
    assert not promoter._cutover_blockers(old, fresh, {"retained", "added"}, added_markets={"added"})

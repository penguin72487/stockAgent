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


def test_private_projection_cannot_export_weighted_raw_feature_proxies(tmp_path):
    from stockagent.live.tw_day_trade_dashboard import _read_signal_feature_drivers
    path = tmp_path / "summary.json"
    write(path, {"source_publication_contract": {"contract": "model_derived_paper_signals_only_v1",
        "feature_proxy_publication_allowed": False},
        "model_explanation": {"all_feature_drivers": [{"feature": "private", "activity_score": 123.}]}})
    assert _read_signal_feature_drivers(path) == []
    write(path, {"model_explanation": {"all_feature_drivers": [{"feature": "public", "activity_score": 123.}]}})
    assert _read_signal_feature_drivers(path)[0]["feature"] == "public"

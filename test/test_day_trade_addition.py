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


@pytest.mark.parametrize("scope", [None, "wrong", "added"])
def test_curve_adoption_binds_first_bar_policy_before_touching_outputs(staged, scope):
    old, _new, target = staged
    output = target / "derived-first-bar"
    output.mkdir()
    points = [["added" if scope == "added" else "retained", "2026-10-12", "2330"]]
    write(output / "minute_curve_receipt.json", {
        "opening_marks_revalued_with_disclosed_first_bar_exceptions": True,
        "strategy": {"provisional_opening_first_bar_valuation": {"authorized_targets": points}},
    })
    original = (target / "marks.jsonl").read_bytes()
    authorized = (None if scope is None else
                  {("retained", "2026-10-12", "2317")} if scope == "wrong" else
                  {tuple(points[0])})
    match = "unapproved or mixed" if scope is None else "scope differs" if scope == "wrong" else "independently accepted new mode"
    with pytest.raises(ValueError, match=match):
        addition.adopt_isolated_addition_curves(target, output, live=old,
            provisional_opening_first_bar_targets=authorized)
    assert (target / "marks.jsonl").read_bytes() == original


def test_addition_keeps_exact_accounts_and_old_bytes(staged):
    old, new, target = staged
    before = json.loads((old / "state.json").read_text())
    result = addition.validate_addition(target, expected_markets={"retained", "added"})
    assert result["old_signal_pins_and_executions_unchanged"]
    assert json.loads((target / "state.json").read_text())["modes"]["retained"] == before["modes"]["retained"]
    for name in addition.LEDGERS:
        assert (target / name).read_bytes() == (old / name).read_bytes()+(new / name).read_bytes()
    assert not promoter._cutover_blockers(old, target, {"retained", "added"}, added_markets={"added"})


def test_added_mode_curve_cannot_be_silently_revalued_under_retained_economics(staged):
    _old, _new, target = staged
    rows = [json.loads(line) for line in (target / "marks.jsonl").read_text().splitlines()]
    for row in rows:
        if row.get("market") == "added": row["total_equity_twd"] = 999.
    (target / "marks.jsonl").write_text("".join(json.dumps(row) + "\n" for row in rows))
    with pytest.raises(ValueError, match="minute values differ"):
        addition.validate_addition(target, expected_markets={"retained", "added"})


@pytest.mark.parametrize("tamper", [None, "marks", "original_benchmark"])
def test_curve_adoption_recovers_only_exact_partial_outputs(staged, monkeypatch, tamper):
    from datetime import datetime, timedelta
    from stockagent.live.benchmark_history_projection import write_benchmark_projection
    from scripts import rebuild_tw_day_trade_minute_curves as curves
    old, _new, target = staged
    output = target / "derived"
    output.mkdir()
    for root in (old, target):
        write(root / "benchmark_history.json", {"marks": []})
    original = {name: addition.sha(target / name) for name in (
        "marks.jsonl", "fills.jsonl", "orders.jsonl", "events.jsonl", "state.json", "benchmark_history.json")}
    (output / "marks.before.jsonl").write_bytes((target / "marks.jsonl").read_bytes())
    (output / "marks.jsonl").write_text((target / "marks.jsonl").read_text() + '{"derived":true}\n')
    marks = [dict(benchmark_id=key, session_date="2026-10-12",
        minute=(datetime.fromisoformat(f"2026-10-12T{clock}:00+08:00") + timedelta(minutes=i)).isoformat(timespec="minutes"))
        for key, clock, count in (("benchmark_0050", "09:00", 271),
            ("benchmark_2330", "09:00", 271), ("benchmark_tx_continuous", "08:45", 300))
        for i in range(count)]
    benchmark = output / "benchmark_history.json"
    write(benchmark, {"marks": marks})
    write_benchmark_projection(state_dir=output, source_path=benchmark,
        source_sha256=addition.sha(benchmark), origins={}, marks=marks, created_at="first")
    rebuild = json.loads((target / "rebuild_receipt.json").read_text())
    write(output / "minute_curve_receipt.json", {
        "simulation_only": True, "production_order_possible": False,
        "source_ledger_hashes": original,
        "outputs": {"marks": {"sha256": addition.sha(output / "marks.jsonl")},
                    "benchmark_history": {"sha256": addition.sha(benchmark)}},
        "independently_preserved_addition_component": {
            "market": "added", "component": rebuild["new_mode_component"]},
    })
    monkeypatch.setattr(curves, "minute_curve_scope_failures", lambda *a, **kw: [])
    validate = promoter._validate_benchmarks
    monkeypatch.setattr(promoter, "_validate_benchmarks", lambda *a, **kw:
        (_ for _ in ()).throw(RuntimeError("interrupted after output adoption")))
    with pytest.raises(RuntimeError, match="interrupted"):
        addition.adopt_isolated_addition_curves(target, output, live=old)
    monkeypatch.setattr(promoter, "_validate_benchmarks", validate)
    if tamper == "marks":
        (target / "marks.jsonl").write_text('{"tampered":true}\n')
    elif tamper == "original_benchmark":
        write(old / "benchmark_history.json", {"changed": True})
    if tamper:
        with pytest.raises(ValueError, match="source changed"):
            addition.adopt_isolated_addition_curves(target, output, live=old)
    else:
        result = addition.adopt_isolated_addition_curves(target, output, live=old)
        assert result["minute_rows"] == 540 and result["financial_journals_changed"] is False
        assert result["benchmark_projection_adoption"]["projection_sessions"] == 1
    for name in ("fills.jsonl", "orders.jsonl", "events.jsonl", "state.json"):
        assert addition.sha(target / name) == original[name]


@pytest.mark.parametrize("missing_optional", [False, True])
def test_completed_entry_expiry_changes_intentions_not_recorded_economics(staged, missing_optional):
    from datetime import date, datetime
    from zoneinfo import ZoneInfo
    from stockagent.live.tw_day_trade_simulation import ENTRY_FILL_POLICY_0901_MINUTE_PRICE, REPLAY_FILL_CONTRACT_0901_MINUTE_PRICE

    live, component, _target = staged
    state = json.loads((component / "state.json").read_text())
    state.update(simulation_only=True, production_order_possible=False)
    mode = state["modes"]["added"]
    mode.update(simulation_replay=True, entry_fill_policy=ENTRY_FILL_POLICY_0901_MINUTE_PRICE,
        entry_fill_contract=REPLAY_FILL_CONTRACT_0901_MINUTE_PRICE, pending_entry_shares=5000,
        pending_entry_orders={"6971": {"status": "working", "remaining_shares": 5000}})
    write(component / "state.json", state)
    if missing_optional:
        (component / "latency.jsonl").unlink()
    before = {name: (component / name).read_bytes() if (component / name).is_file() else None
              for name in addition.LEDGERS}
    proof = addition.finalize_completed_replay_entries(component, live=live, market="added",
        completed_day=date(2026, 10, 12), recorded_at=datetime(2026, 11, 10, tzinfo=ZoneInfo("Asia/Taipei")))
    after = json.loads((component / "state.json").read_text())["modes"]["added"]
    assert after["total_equity_twd"] == mode["total_equity_twd"]
    assert after["pending_entry_orders"]["6971"]["status"] == "cancelled_exit_priority"
    assert after["pending_entry_shares"] == 0 and proof["new_fills"] == 0
    assert before == {name: (component / name).read_bytes() if (component / name).is_file() else None
                      for name in addition.LEDGERS}
    assert addition.validate_completed_replay_entry_finalization(component) == proof
    changed = json.loads((component / "state.json").read_text())
    changed["modes"]["added"]["total_equity_twd"] += 1
    write(component / "state.json", changed)
    with pytest.raises(ValueError, match="changed inventory, economics"):
        addition.validate_completed_replay_entry_finalization(component)


def test_accepted_component_preserves_only_the_exact_independent_rows(staged, monkeypatch):
    from datetime import date
    from scripts.rebuild_tw_day_trade_minute_curves import accepted_addition_curve_component
    _old, _new, target = staged
    receipt = json.loads((target / "rebuild_receipt.json").read_text())
    component = target / receipt["new_mode_component"]
    monkeypatch.setattr(promoter, "_validate_rebuild", lambda *_a, **_k: {
        "session_dates": ["2026-10-12"], "session_count": 1,
        "state_sha256": addition.sha(component / "state.json"),
        "rebuild_receipt_sha256": addition.sha(component / "rebuild_receipt.json")})
    source = [json.loads(line) for line in (target / "marks.jsonl").read_text().splitlines()]
    market, rows, proof = accepted_addition_curve_component(target, source,
        start=date(2026, 10, 12), end=date(2026, 10, 12))
    assert market == "added" and rows == [{"market": "added"}]
    assert proof["preserved_rows"] == 1
    source[-1]["total_equity_twd"] = -1.
    with pytest.raises(ValueError, match="minute rows differ"):
        accepted_addition_curve_component(target, source,
            start=date(2026, 10, 12), end=date(2026, 10, 12))


def test_curve_adoption_cannot_target_live_or_an_external_output(staged, tmp_path):
    old, _new, target = staged
    with pytest.raises(ValueError, match="private same-filesystem"):
        addition.adopt_isolated_addition_curves(old, target, live=old)
    with pytest.raises(ValueError, match="private same-filesystem"):
        addition.adopt_isolated_addition_curves(target, old, live=old)


@pytest.mark.parametrize("tamper", ["cash_equity", "retained_inventory"])
def test_cash_recovery_proof_is_recomputed_from_original_financial_prefixes(tmp_path, tamper):
    from test_tw_day_trade_minute_curves import _cash_gap_fixture
    function, source, kwargs = _cash_gap_fixture()
    kwargs["state"]["modes"]["mode"].update(open_position_count=0, positions={})
    target = tmp_path / "candidate"
    target.mkdir()
    evidence = target / "addition_evidence/retained_curves"
    evidence.mkdir(parents=True)
    write(target / "state.json", kwargs["state"])
    write(evidence / "prior_state.json", kwargs["state"])
    for name in ("orders.jsonl", "events.jsonl", "fills.jsonl"):
        (target / name).write_text("")
    before = evidence / "marks.before.jsonl"
    before.write_text("".join(json.dumps(row) + "\n" for row in source))
    derived, cash = function(source, {}, **kwargs)
    (target / "marks.jsonl").write_text("".join(json.dumps(row) + "\n" for row in derived))
    curve_path = evidence / "minute_curve_receipt.json"
    write(curve_path, {"cash_account_gap_recovery": cash,
        "source_ledger_hashes": {"marks.jsonl": addition.sha(before)},
        "strategy": {"repaired_opening_endpoints": []}})
    receipt = {"retained_markets": ["mode"], "prior_state": str((evidence / "prior_state.json").relative_to(target)),
        "sessions": [{"session_date": day} for day in kwargs["completed_sessions"]],
        "retained_execution_prefixes": {name: {"bytes": 0} for name in ("orders.jsonl", "events.jsonl", "fills.jsonl")},
        "retained_curve_recovery": {"contract": addition.CURVE_RECOVERY,
            "source_price_or_execution_invented": False,
            "curve_receipt": str(curve_path.relative_to(target)), "curve_receipt_sha256": addition.sha(curve_path),
            "marks_before": str(before.relative_to(target)), "marks_before_sha256": addition.sha(before)}}
    accepted = addition.validate_retained_curve_recovery(target, receipt)
    assert accepted["cash_rows"] == 270 and accepted["missing_signal_execution_replayed"] is False
    rows = [json.loads(line) for line in (target / "marks.jsonl").read_text().splitlines()]
    if tamper == "cash_equity":
        rows[-1]["total_equity_twd"] += 1
        failure = "cash minute values differ"
    else:
        rows[0]["open_position_count"] = 1
        failure = "adopted retained endpoint accounting differs"
    (target / "marks.jsonl").write_text("".join(json.dumps(row) + "\n" for row in rows))
    with pytest.raises(ValueError, match=failure):
        addition.validate_retained_curve_recovery(target, receipt)


def test_next_addition_preserves_existing_addition_evidence(staged, tmp_path):
    old, new, _ = staged
    prior = old / "addition_evidence" / "new_mode" / "legacy.json"
    write(prior, {"prior_addition": "preserve"})
    candidate = tmp_path / "second-addition"
    receipt = addition.merge_addition(old, new, candidate, added_market="added")
    assert json.loads((candidate / prior.relative_to(old)).read_text()) == {"prior_addition": "preserve"}
    assert receipt["new_mode_component"] != "addition_evidence/new_mode"
    assert (candidate / receipt["new_mode_component"] / "rebuild_receipt.json").is_file()
    assert addition.validate_addition(candidate, expected_markets={"retained", "added"})["added_market"] == "added"


def test_provisional_addition_records_exact_explicit_scope_and_revalidates_component(staged, tmp_path, monkeypatch):
    from contextlib import nullcontext
    old, new, _ = staged
    candidate = tmp_path / "authorized-addition"
    observed = {}
    def verify(component, **kwargs):
        observed.update(kwargs)
        return {"session_dates": ["2026-10-12"], "session_count": 1,
                "signal_ledger_validation": {},
                "provisional_research": {"market": "added", "source_gaps_may_change_results": True}}
    monkeypatch.setattr(promoter, "_validate_rebuild", verify)
    receipt = addition.merge_addition(old, new, candidate, added_market="added",
                                     allow_provisional_research_market="added")
    assert receipt["provisional_research_publication"]["market"] == "added"
    with pytest.raises(ValueError, match="managed.*journal"):
        addition.validate_addition(candidate, expected_markets={"retained", "added"})
    addition.enroll_research_execution_journals(candidate, live=old,
                                                 capacity_reservation=lambda _: nullcontext())
    result = addition.validate_addition(candidate, expected_markets={"retained", "added"})
    assert observed["allow_provisional_research_market"] == "added"
    assert result["new_component_acceptance"]["provisional_research"]["source_gaps_may_change_results"]
    tampered = json.loads((candidate / "rebuild_receipt.json").read_text())
    tampered["provisional_research_publication"]["market"] = "retained"
    write(candidate / "rebuild_receipt.json", tampered)
    with pytest.raises(ValueError, match="provisional"):
        addition.validate_addition(candidate, expected_markets={"retained", "added"})


def test_provisional_addition_cannot_authorize_a_different_market(staged, tmp_path):
    old, new, _ = staged
    with pytest.raises(ValueError, match="provisional"):
        addition.merge_addition(old, new, tmp_path / "wrong-scope", added_market="added",
                                allow_provisional_research_market="retained")


def _pinned_research_component(new, tmp_path):
    from stockagent.live.market_status import short_file_fingerprint

    fold = tmp_path / "artifact/fold_10"
    fold.mkdir(parents=True)
    checkpoint = fold / "checkpoint_best.pt"
    checkpoint.write_bytes(b"exact selected research checkpoint")
    config = tmp_path / "research.yaml"
    config.write_text("research: true\n")
    write(fold / "fold_complete.json", {"status": "complete", "fold_id": 10,
                                        "checkpoint_path": checkpoint.name})
    write(fold / "mode_artifact_contract.json", {"execution_mode": "tw_day_trade", "mode_details": {
        "short_limit_up_close_penalty": {"contract": "official_limit_up_close_source_residual_short_expense_v1",
            "assessment": "after_source_capacity_before_synthetic_terminal_liquidation",
            "base": "uncovered_short_shares_times_official_close", "research_only": True, "rate": 0.07}}})
    state = json.loads((new / "state.json").read_text())
    state["modes"]["added"].update(checkpoint_path=str(checkpoint), config_path=str(config),
        checkpoint_fingerprint=short_file_fingerprint(checkpoint),
        configured_research_short_limit_up_close_penalty_rate=0.07,
        research_short_limit_up_close_penalty_rate=0.07)
    write(new / "state.json", state)
    return checkpoint, config, fold


def test_provisional_addition_binds_exact_research_model_and_fails_if_changed(staged, tmp_path):
    from contextlib import nullcontext

    old, new, _ = staged
    checkpoint, config, _fold = _pinned_research_component(new, tmp_path)
    candidate = tmp_path / "model-pinned-addition"
    receipt = addition.merge_addition(old, new, candidate, added_market="added",
        allow_provisional_research_market="added", pin_research_model=True)
    pin = receipt["provisional_research_publication"]["research_model"]
    assert pin["checkpoint_path"] == str(checkpoint)
    assert pin["config_path"] == str(config)
    assert len(pin["checkpoint_sha256"]) == 64 and pin["research_cost_rate"] == 0.07
    addition.enroll_research_execution_journals(candidate, live=old, capacity_reservation=lambda _: nullcontext())
    checkpoint.write_bytes(b"a different checkpoint")
    with pytest.raises(ValueError, match="checkpoint|model"):
        addition.validate_addition(candidate, expected_markets={"retained", "added"})


def test_research_model_pin_rejects_unapproved_or_changed_cost(staged, tmp_path):
    _old, new, _ = staged
    _checkpoint, _config, fold = _pinned_research_component(new, tmp_path)
    mode = json.loads((new / "state.json").read_text())["modes"]["added"]
    assert addition._pin_research_model(mode)["research_cost_rate"] == 0.07
    contract = json.loads((fold / "mode_artifact_contract.json").read_text())
    contract["mode_details"]["short_limit_up_close_penalty"]["rate"] = 0
    write(fold / "mode_artifact_contract.json", contract)
    with pytest.raises(ValueError, match="cost contract"):
        addition._pin_research_model(mode)


def _promoted_research_fixture(staged, tmp_path):
    from contextlib import nullcontext

    old, new, _ = staged
    checkpoint, config, _fold = _pinned_research_component(new, tmp_path)
    candidate = tmp_path / "promoted-research"
    rebuild = addition.merge_addition(old, new, candidate, added_market="added",
        allow_provisional_research_market="added", pin_research_model=True)
    addition.enroll_research_execution_journals(candidate, live=old, capacity_reservation=lambda _: nullcontext())
    component = candidate / rebuild["new_mode_component"]
    acceptance = {"contract": addition.CONTRACT, "added_market": "added", "mode_set": ["added", "retained"],
        "rebuild_receipt_sha256": addition._stable_regular_sha(candidate / "rebuild_receipt.json"),
        "minute_curve_validation": {"validated_rows": 540, "duplicate_minute_rows": 0,
                                    "unverified_historical_interior_rows": 0},
        "new_component_acceptance": {"session_count": 1,
            "rebuild_receipt_sha256": addition._stable_regular_sha(component / "rebuild_receipt.json"),
            "state_sha256": addition._stable_regular_sha(component / "state.json"),
            "provisional_research": {"market": "added", "source_gaps_may_change_results": True,
                                     "broker_or_exchange_execution_claimed": False},
            "strict_intraday_audit": {"passed": True, "full_requested_range_passed": True, "error_count": 0}}}
    write(candidate / "promotion_receipt.json", {"simulation_only": True, "production_order_possible": False,
        "promotion_method": "same_filesystem_atomic_directory_exchange", "live_directory": str(candidate.resolve()),
        "acceptance": acceptance})
    return candidate, checkpoint, config


def test_promoted_research_admission_requires_exact_receipts_model_and_journals(staged, tmp_path):
    candidate, checkpoint, config = _promoted_research_fixture(staged, tmp_path)
    args = {"market": "added", "initial_capital": 10_000_000.0,
            "checkpoint": checkpoint, "config": config, "rate": 0.07}
    proof = addition.verify_promoted_research_admission(candidate, **args)
    assert proof["market"] == "added" and proof["research_cost_rate"] == 0.07
    assert proof["broker_fill_claimed"] is proof["native_warmup_or_data_readiness_claimed"] is False
    with pytest.raises(ValueError, match="capital or cost"):
        addition.verify_promoted_research_admission(candidate, **(args | {"rate": 0.08}))
    config.write_text("changed model config\n")
    with pytest.raises(ValueError, match="model, capital or cost"):
        addition.verify_promoted_research_admission(candidate, **args)


@pytest.mark.parametrize("defect", ["wrong_market", "audit_failed", "partial_minutes", "broker_claim", "unmanaged"])
def test_promoted_research_admission_fails_closed(staged, tmp_path, defect):
    candidate, checkpoint, config = _promoted_research_fixture(staged, tmp_path)
    receipt = candidate / "promotion_receipt.json"
    value = json.loads(receipt.read_text())
    if defect == "wrong_market": value["acceptance"]["added_market"] = "another"
    elif defect == "audit_failed": value["acceptance"]["new_component_acceptance"]["strict_intraday_audit"]["passed"] = False
    elif defect == "partial_minutes": value["acceptance"]["minute_curve_validation"]["validated_rows"] = 539
    elif defect == "broker_claim": value["production_order_possible"] = True
    else:
        from stockagent.live import paper_journal
        (candidate / paper_journal.HEADS / "orders.jsonl.json").unlink()
        (candidate / paper_journal.HEADS / "orders.jsonl.enrolled").unlink(missing_ok=True)
    write(receipt, value)
    with pytest.raises((ValueError, FileNotFoundError)):
        addition.verify_promoted_research_admission(candidate, market="added", initial_capital=10_000_000.0,
                                                     checkpoint=checkpoint, config=config, rate=0.07)


def test_private_addition_enrolls_orders_and_fills_without_changing_live_bytes(staged, tmp_path):
    from contextlib import contextmanager
    from stockagent.live import paper_journal
    old, new, _ = staged
    candidate = tmp_path / "private-provisional"
    addition.merge_addition(old, new, candidate, added_market="added",
                            allow_provisional_research_market="added")
    before = {name: (old / name).read_bytes() for name in ("orders.jsonl", "fills.jsonl")}
    candidate_before = {name: (candidate / name).read_bytes() for name in before}
    reservations = []
    @contextmanager
    def reserve(amount):
        reservations.append(amount)
        yield
    proof = addition.enroll_research_execution_journals(candidate, live=old, capacity_reservation=reserve)
    assert proof["live_ledger_mutated"] is False
    assert reservations and reservations[0] >= sum(map(len, candidate_before.values()))
    for name in before:
        assert (old / name).read_bytes() == before[name]
        assert (candidate / name).read_bytes() == candidate_before[name]
        assert paper_journal.managed(candidate / name)
        with paper_journal.open_paper_journal(candidate / name) as stream:
            assert stream.read() == candidate_before[name]
    second = addition.enroll_research_execution_journals(candidate, live=old, capacity_reservation=reserve)
    assert second["already_enrolled"] is True


def test_research_journal_enrollment_never_operates_on_live_or_unresolved_state(staged):
    from contextlib import nullcontext
    old, _, candidate = staged
    with pytest.raises(ValueError, match="private|separate"):
        addition.enroll_research_execution_journals(old, live=old, capacity_reservation=lambda _: nullcontext())
    state = json.loads((candidate / "state.json").read_text())
    state["modes"]["added"]["research_terminal_pending_commit"] = {"contract": "pending"}
    write(candidate / "state.json", state)
    with pytest.raises(ValueError, match="unresolved"):
        addition.enroll_research_execution_journals(candidate, live=old, capacity_reservation=lambda _: nullcontext())


def test_research_journal_enrollment_defers_before_writing_if_capacity_unavailable(staged, tmp_path):
    from contextlib import contextmanager
    from stockagent.live import paper_journal
    old, new, _ = staged
    candidate = tmp_path / "deferred-provisional"
    addition.merge_addition(old, new, candidate, added_market="added",
                            allow_provisional_research_market="added")
    @contextmanager
    def rejected_reservation(_amount):
        raise OSError("backing capacity unavailable")
        yield
    with pytest.raises(OSError, match="backing capacity"):
        addition.enroll_research_execution_journals(candidate, live=old, capacity_reservation=rejected_reservation)
    assert not paper_journal.managed(candidate / "orders.jsonl")
    assert not paper_journal.managed(candidate / "fills.jsonl")


def test_research_journal_enrollment_rejects_unpinned_authorization(staged, tmp_path):
    from contextlib import nullcontext
    old, new, _ = staged
    candidate = tmp_path / "tampered-provisional"
    addition.merge_addition(old, new, candidate, added_market="added",
                            allow_provisional_research_market="added")
    receipt_path = candidate / "rebuild_receipt.json"
    receipt = json.loads(receipt_path.read_text())
    receipt["provisional_research_publication"]["source_gaps_may_change_results"] = False
    write(receipt_path, receipt)
    with pytest.raises(ValueError, match="authorized"):
        addition.enroll_research_execution_journals(candidate, live=old,
                                                     capacity_reservation=lambda _: nullcontext())


def test_research_journal_enrollment_rechecks_candidate_inside_capacity_lease(staged, tmp_path):
    from contextlib import contextmanager
    from stockagent.live import paper_journal
    old, new, _ = staged
    candidate = tmp_path / "raced-provisional"
    addition.merge_addition(old, new, candidate, added_market="added",
                            allow_provisional_research_market="added")
    @contextmanager
    def changed_state(_amount):
        state = json.loads((candidate / "state.json").read_text())
        state["modes"]["added"]["total_equity_twd"] += 1
        write(candidate / "state.json", state)
        yield
    with pytest.raises(RuntimeError, match="candidate.*changed"):
        addition.enroll_research_execution_journals(candidate, live=old, capacity_reservation=changed_state)
    assert not paper_journal.managed(candidate / "orders.jsonl")
    assert not paper_journal.managed(candidate / "fills.jsonl")


@pytest.mark.parametrize("failure", ["busy_owner", "partial_enrollment"])
def test_research_journal_enrollment_quarantines_busy_or_partial_candidates(staged, tmp_path, failure):
    from contextlib import nullcontext
    import fcntl
    from stockagent.live import paper_journal
    old, new, _ = staged
    candidate = tmp_path / "blocked-provisional"
    addition.merge_addition(old, new, candidate, added_market="added",
                            allow_provisional_research_market="added")
    if failure == "partial_enrollment":
        paper_journal.publish_snapshot(candidate / "orders.jsonl", operation="seed")
    with (candidate / ".engine.lock").open("a+") as owner:
        if failure == "busy_owner":
            fcntl.flock(owner.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        with pytest.raises(ValueError, match="active engine|partial journal"):
            addition.enroll_research_execution_journals(candidate, live=old,
                                                         capacity_reservation=lambda _: nullcontext())


@pytest.mark.parametrize("mutation", ["component_receipt", "no_opt_in", "fake_complete", "broker_claim"])
def test_provisional_addition_rejects_changed_consent_or_component(staged, tmp_path, monkeypatch, mutation):
    old, new, _ = staged
    candidate = tmp_path / "provisional-guard"
    monkeypatch.setattr(promoter, "_validate_rebuild", lambda *args, **kwargs: {
        "session_dates": ["2026-10-12"], "session_count": 1,
        "signal_ledger_validation": {}, "provisional_research": {"market": "added"}})
    addition.merge_addition(old, new, candidate, added_market="added",
                            allow_provisional_research_market="added")
    receipt_path = candidate / "rebuild_receipt.json"
    receipt = json.loads(receipt_path.read_text())
    if mutation == "component_receipt":
        component = candidate / receipt["new_mode_component"] / "rebuild_receipt.json"
        write(component, {"sessions": [{"session_date": "2026-10-13"}]})
    else:
        field = {"no_opt_in": "explicit_operator_opt_in", "fake_complete": "source_gaps_may_change_results",
                 "broker_claim": "broker_or_exchange_execution_claimed"}[mutation]
        receipt["provisional_research_publication"][field] = mutation == "broker_claim"
        write(receipt_path, receipt)
    with pytest.raises(ValueError, match="provisional"):
        addition.validate_addition(candidate, expected_markets={"retained", "added"})


def test_addition_rebinds_enrolled_clones_and_preserves_full_retained_history(staged):
    from stockagent.live import paper_journal
    old, new, _ = staged
    snapshots = {}
    for root in (old, new):
        for name in paper_journal.FILENAMES:
            path = root / name
            if path.exists():
                snapshots[(root, name)] = path.read_bytes()
                paper_journal.publish_snapshot(path, operation="seed")
    target = old.parent / "managed-candidate"
    addition.merge_addition(old, new, target, added_market="added")
    proof = addition.validate_addition(target, expected_markets={"retained", "added"})
    assert proof["old_signal_pins_and_executions_unchanged"]
    for name in paper_journal.FILENAMES:
        if (old / name).exists():
            with paper_journal.open_paper_journal(target / name) as stream:
                assert stream.read() == snapshots[(old, name)] + snapshots[(new, name)]
            assert (old / name).read_bytes() == snapshots[(old, name)]


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


@pytest.mark.parametrize(("field", "value"), [
    ("entry_retry_pending_commit", {"contract": "pending"}),
    ("research_terminal_pending_commit", {"contract": "pending"}),
    ("ledger_state_divergence", {"kind": "partial"}),
    ("pending_signal_id", "not-yet-committed"),
    ("pending_entry_shares", 1_000),
    ("pending_reduction_shares", 1_000),
    ("pending_entry_source_count", 1),
    ("unresolved_exit_count", 1),
    ("pending_entry_orders", {"2330": {"status": "working"}}),
    ("pending_entry_orders", {"2330": {"status": "cancelled_unknown"}}),
    ("pending_reduction_orders", {"2330": {"status": []}}),
    ("pending_reduction_orders", []),
])
def test_addition_never_joins_unresolved_paper_work(staged, tmp_path, field, value):
    old, new, _ = staged
    state = json.loads((old / "state.json").read_text())
    state["modes"]["retained"][field] = value
    write(old / "state.json", state)
    with pytest.raises(ValueError, match="unresolved paper activity"):
        addition.merge_addition(old, new, tmp_path / "blocked", added_market="added")
    assert not addition.retained_accounts_match(state["modes"], state["modes"])


@pytest.mark.parametrize(("field", "value"), [
    ("open_position_count", 0.5),
    ("open_position_count", -1),
    ("positions", {"2330": {"signed_shares": 0.5}}),
    ("positions", {"2330": {"signed_shares": "0"}}),
    ("positions", []),
])
def test_addition_never_joins_invalid_inventory(staged, tmp_path, field, value):
    old, new, _ = staged
    state = json.loads((old / "state.json").read_text())
    state["modes"]["retained"][field] = value
    write(old / "state.json", state)
    with pytest.raises(ValueError, match="invalid"):
        addition.merge_addition(old, new, tmp_path / "blocked", added_market="added")


def test_cutover_rechecks_original_account_snapshot(staged):
    old, _, target = staged
    state = json.loads((old / "state.json").read_text())
    state["modes"]["retained"]["total_equity_twd"] += 1
    write(old / "state.json", state)
    assert any("accounts changed" in message for message in promoter._cutover_blockers(
        old, target, {"retained", "added"}, added_markets={"added"}))


def test_direct_additive_promotion_rejects_unproven_terminal_order_labels(staged):
    old, _, target = staged
    state = json.loads((old / "state.json").read_text())
    state["modes"]["retained"]["pending_entry_orders"] = {
        "2330": {"status": "cancelled_exit_priority"}}
    write(old / "state.json", state)
    blockers = promoter._cutover_blockers(old, target, {"retained", "added"}, added_markets={"added"})
    assert any("terminal paper orders are not proven" in message for message in blockers)


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

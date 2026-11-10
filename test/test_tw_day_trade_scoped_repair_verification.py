from __future__ import annotations

import hashlib
import json
from datetime import datetime, timedelta
from types import SimpleNamespace

import pytest
import polars as pl

from scripts import verify_tw_day_trade_scoped_repair as verify


def _revision_accounting_fixture():
    repaired = "source:published-minute-repair:token"
    entry_fee, exit_fee = 1000 * 100 * .000285, 1000 * 101 * .001785
    position = dict(market="m", session_date="2026-10-29", signal_id=repaired, symbol="2330",
        side="long", lot_size=1000, filled_shares=1000, signed_shares=0, entry_price=100.,
        buy_fee_rate=.001425, sell_fee_rate=.002925, commission_rebate_rate=.00114,
        take_profit_price=103., stop_trigger_price=97., realized_gross_pnl_twd=1000.,
        entry_fee_twd=entry_fee, realized_exit_fee_twd=exit_fee,
        realized_net_pnl_twd=1000. - entry_fee - exit_fee)
    mode = dict(market="m", positions={"p": position},
                cumulative_realized_net_pnl_twd=position["realized_net_pnl_twd"])
    common = dict(market="m", session_date="2026-10-29", position_id="p", quantity=1000,
                  simulation_only=True, simulation_replay=True)
    fills = [dict(common, fill_at="2026-10-29T09:01:00+08:00", purpose="entry", price=100., fee_and_tax_twd=entry_fee),
             dict(common, fill_at="2026-10-29T09:03:00+08:00", purpose="next_signal_inventory_delta_minute_sweep",
                  price=101., fee_and_tax_twd=exit_fee)]
    plan = dict(versions=[dict(signal_id="source", effective_entry_at="2026-10-29T09:01:00+08:00")])
    bars = {"2330": {f"2026-10-29T09:0{i}+08:00": dict(low=99., high=102., volume_shares=4000., vwap=price)
                      for i, price in ((1, 100.), (3, 101.))}}
    return mode, fills, plan, bars


def test_revision_fill_oracle_reconciles_quantities_prices_net_fees_and_pnl():
    mode, fills, plan, bars = _revision_accounting_fixture()
    result = verify.verify_published_fill_accounting(mode, fills, day="2026-10-29", plan=plan, bars=bars, prior_realized=0.)
    assert result["entry_shares"] == result["exit_shares"] == 1000
    assert result["session_realized_net_pnl_twd"] == pytest.approx(791.215)


@pytest.mark.parametrize("damage", [None, "fake_execution", "event", "source", "quantity", "fee", "clock"])
def test_nontrade_accounting_reconciles_without_fabricating_a_fill(tmp_path, monkeypatch, damage):
    from stockagent.live import tw_day_trade_nontrade as nontrade

    mode, fills, plan, bars = _revision_accounting_fixture()
    position = mode["positions"]["p"]
    position.update(entry_at="2026-10-29T09:01:00+08:00", status="nontrade_book_settled")
    mode.update(execution_evidence_complete=False, nontrade_settlement_count=1, nontrade_settlement_shares=1000)
    source = tmp_path / "twse_daily_ohlcv" / "2026-10-29.json"
    source.parent.mkdir()
    source.write_text(json.dumps({"date": "20261029", "stat": "OK", "tables": [
        {"fields": ["證券代號", "收盤價", "成交股數"], "data": [["2330", "101", "1000"]]}]}))
    sha = hashlib.sha256(source.read_bytes()).hexdigest()
    reference = {"symbol": "2330", "reference_price": 101., "receipt_path": "/pinned/receipt.json",
                 "reference_observed_at": "2026-10-29T12:58:00+08:00"}
    monkeypatch.setattr(nontrade, "load_pinned_finmind_ticks",
                        lambda *args, **kwargs: ({"2330": dict(reference)}, []))
    details = {"contract": nontrade.CONTRACT, "accounting_at": "2026-10-29T13:30:00+08:00",
        "reference": dict(reference), "reference_before_simulated_entry": False,
        "official_close_evidence": {"source": "twse_official_daily_close", "source_sha256": sha,
            "source_path": str(source), "price": 101., "price_basis": "official_session_close"},
        "quantity": 1000, "assumed_exit_fee_and_tax_twd": fills[1]["fee_and_tax_twd"],
        "simulation_only": True, "exchange_fill": False, "broker_fill": False,
        "simulated_order": False, "simulated_fill": False, "source_completeness_verified": False}
    position["nontrade_settlement"] = details
    event = {"event": "nontrade_last_observed_accounting_settlement", "recorded_at": "2026-10-29T18:00:00+08:00",
             "market": "m", "session_date": "2026-10-29", "position_id": "p", "symbol": "2330", **details}
    if damage == "fake_execution": mode["execution_evidence_complete"] = True
    if damage == "event": event["simulated_fill"] = True
    if damage == "source": details["reference"] = {**reference, "reference_price": 102.}
    if damage == "quantity": details["quantity"] = 2000
    if damage == "fee": details["assumed_exit_fee_and_tax_twd"] += 1
    if damage == "clock": event["recorded_at"] = "2026-10-29T13:00:00+08:00"
    kwargs = dict(day="2026-10-29", plan=plan, bars=bars, prior_realized=0., nontrade_events=[event],
                  terminal_close_sources=[{"path": str(source), "sha256": sha}])
    if damage:
        with pytest.raises(RuntimeError):
            verify.verify_published_fill_accounting(mode, fills[:1], **kwargs)
    else:
        result = verify.verify_published_fill_accounting(mode, fills[:1], **kwargs)
        assert result["entry_shares"] == 1000 and result["exit_shares"] == 0
        assert result["nontrade_settlement_shares"] == 1000
        assert result["session_bookkeeping_net_pnl_twd"] == pytest.approx(791.215)
        assert "session_realized_net_pnl_twd" not in result


@pytest.mark.parametrize("damage", ["fee", "volume", "clock", "replay"])
def test_revision_fill_oracle_rejects_financial_or_source_mismatch(damage):
    mode, fills, plan, bars = _revision_accounting_fixture()
    if damage == "fee":
        fills[0]["fee_and_tax_twd"] += 1
    elif damage == "volume":
        bars["2330"]["2026-10-29T09:01+08:00"]["volume_shares"] = 1000
    elif damage == "clock":
        plan["versions"][0]["effective_entry_at"] = "2026-10-29T09:02:00+08:00"
    else:
        fills[0]["simulation_replay"] = False
    with pytest.raises(RuntimeError):
        verify.verify_published_fill_accounting(mode, fills, day="2026-10-29", plan=plan, bars=bars, prior_realized=0.)


@pytest.mark.parametrize("damage", [None, "missing_source", "source_hash", "receipt_hash", "price", "volume", "clock"])
def test_revision_terminal_close_requires_pinned_traded_official_evidence(tmp_path, damage):
    mode, fills, plan, bars = _revision_accounting_fixture()
    path = tmp_path / "twse_daily_ohlcv" / "2026-10-29.json"
    path.parent.mkdir()
    path.write_text(json.dumps({"date": "20261029", "stat": "OK", "tables": [
        {"fields": ["證券代號", "收盤價", "成交股數"],
         "data": [["2330", "101", "0" if damage == "volume" else "1000"]]}]}))
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    sources = [{"path": str(path), "sha256": digest}]
    close = fills[1]
    close.update(fill_at="2026-10-29T13:30:00+08:00", purpose="13_30_unlimited_close_paper_settlement",
        fill_contract="source_close_full_deliverable_residual_no_capacity_paper_only_v1")
    close["terminal_close_receipt"] = dict(contract=close["fill_contract"], broker_fill=False,
        full_quantity_is_user_assumption=True, effective_at=close["fill_at"],
        source_evidence=dict(session_date="2026-10-29", price=101., source="twse_official_daily_close",
                             source_sha256=digest, price_basis="official_session_close"))
    if damage == "missing_source":
        sources = []
    elif damage == "source_hash":
        sources[0]["sha256"] = "0" * 64
    elif damage == "receipt_hash":
        close["terminal_close_receipt"]["source_evidence"]["source_sha256"] = "0" * 64
    elif damage == "price":
        close["price"] = 102.
    elif damage == "clock":
        close["fill_at"] = "2026-10-29T13:29:00+08:00"
    if damage is not None:
        with pytest.raises(RuntimeError):
            verify.verify_published_fill_accounting(mode, fills, day="2026-10-29", plan=plan, bars=bars,
                prior_realized=0., terminal_close_sources=sources)
    else:
        result = verify.verify_published_fill_accounting(mode, fills, day="2026-10-29", plan=plan, bars=bars,
            prior_realized=0., terminal_close_sources=sources)
        assert result["session_realized_net_pnl_twd"] == pytest.approx(791.215)
        with pytest.raises(RuntimeError, match="observed close-minute trade"):
            verify.verify_published_fill_accounting(mode, fills, day="2026-10-29", plan=plan, bars=bars,
                prior_realized=0., terminal_close_sources=sources, require_observed_terminal_minute=True)
        bars["2330"]["2026-10-29T13:30+08:00"] = {"close": 101., "volume_shares": 1000.}
        verify.verify_published_fill_accounting(mode, fills, day="2026-10-29", plan=plan, bars=bars,
            prior_realized=0., terminal_close_sources=sources, require_observed_terminal_minute=True)


@pytest.mark.parametrize("damage", [None, "clock", "volume", "row", "hash", "ordinary"])
def test_delayed_terminal_fill_requires_pinned_1333_source_and_official_close(tmp_path, damage):
    mode, fills, plan, bars = _revision_accounting_fixture()
    official = tmp_path / "twse_daily_ohlcv" / "2026-10-29.json"
    official.parent.mkdir()
    official.write_text(json.dumps({"date": "20261029", "stat": "OK", "tables": [
        {"fields": ["證券代號", "收盤價", "成交股數"], "data": [["2330", "101", "1000"]]}]}))
    official_sha = hashlib.sha256(official.read_bytes()).hexdigest()
    source = tmp_path / "bars.parquet"
    pl.DataFrame({"symbol": ["2330"], "ts": [datetime(2026, 10, 29, 13, 33)],
                  "Close": [100. if damage == "row" else 101.], "volume_shares": [2000.]}).write_parquet(source)
    source_sha = hashlib.sha256(source.read_bytes()).hexdigest()
    bars["2330"]["2026-10-29T13:33+08:00"] = {"close": 101., "volume_shares": 2000.}
    if damage == "ordinary":
        bars["2330"]["2026-10-29T13:30+08:00"] = {"close": 101., "volume_shares": 2000.}
    close = fills[1]
    close.update(fill_at="2026-10-29T13:33:00+08:00", purpose="13_33_delayed_close_paper_settlement",
        fill_contract="source_close_full_deliverable_residual_no_capacity_paper_only_v1")
    clock = dict(contract=verify.repair.DELAYED_AUCTION_PAPER_REPLAY_CONTRACT,
        session_date="2026-10-29", symbol="2330", source_minute_at=close["fill_at"],
        observed_close=101., volume_shares=2000., source_path=str(source), source_sha256=source_sha,
        clock_basis="historical_source_minute_label_not_broker_match")
    close["terminal_close_receipt"] = dict(contract=close["fill_contract"], broker_fill=False,
        full_quantity_is_user_assumption=True, effective_at=close["fill_at"], delayed_paper_auction=clock,
        source_evidence=dict(session_date="2026-10-29", price=101., source="twse_official_daily_close",
                             source_sha256=official_sha, price_basis="official_session_close"))
    if damage == "clock":
        clock["source_minute_at"] = "2026-10-29T13:30:00+08:00"
    elif damage == "volume":
        clock["volume_shares"] = 1000.
    elif damage == "hash":
        clock["source_sha256"] = "0" * 64
    kwargs = dict(day="2026-10-29", plan=plan, bars=bars, prior_realized=0.,
        terminal_close_sources=[{"path": str(official), "sha256": official_sha}],
        minute_source_files=[{"path": str(source), "sha256": source_sha, "symbols": ["2330"]}])
    if damage:
        with pytest.raises(RuntimeError):
            verify.verify_published_fill_accounting(mode, fills, **kwargs)
    else:
        result = verify.verify_published_fill_accounting(mode, fills, **kwargs)
        assert result["entry_shares"] == result["exit_shares"] == 1000


def test_minute_coverage_accepts_a_separate_proven_1333_event_but_not_a_fake_bar():
    start = datetime.fromisoformat("2026-10-29T09:01:00+08:00")
    marks = [{"market": "m", "session_date": "2026-10-29",
              "minute": (start + timedelta(minutes=i)).isoformat(),
              "missing_price_position_count": 0, "valuation_stale": False} for i in range(270)]
    marks.append({"market": "m", "session_date": "2026-10-29", "minute": "2026-10-29T13:33:00+08:00",
                  "valuation_source": "source_pinned_delayed_auction_event_v1", "open_position_count": 0,
                  "missing_price_position_count": 0, "valuation_stale": False})
    mode = {"positions": {}, "delayed_paper_auction_settled_at": "2026-10-29T13:33:00+08:00"}
    result = verify.verify_minute_source_coverage(mode, marks, market="m", day="2026-10-29", source_files=[])
    assert result["minute_points"] == 270
    assert result["delayed_auction_event_at"] == "2026-10-29T13:33:00+08:00"
    marks[-1]["valuation_source"] = "other"
    with pytest.raises(RuntimeError, match="delayed closing"):
        verify.verify_minute_source_coverage(mode, marks, market="m", day="2026-10-29", source_files=[])


def test_prefix_verification_accepts_append_but_not_overwrite_or_truncation(tmp_path):
    path = tmp_path / "journal"
    original = b"original\n"
    path.write_bytes(original+b"new rows\n")
    expected = hashlib.sha256(original).hexdigest()
    assert verify.prefix_sha256(path, len(original)) == expected
    path.write_bytes(b"changed!\nnew rows\n")
    assert verify.prefix_sha256(path, len(original)) != expected
    path.write_bytes(b"short")
    with pytest.raises(RuntimeError, match="truncated"):
        verify.prefix_sha256(path, len(original))


def test_suffix_reader_does_not_rescan_old_prefix_or_accept_partial_new_row(tmp_path):
    path = tmp_path / "journal"
    prefix = b"unrelated old contents\n"
    path.write_bytes(prefix+b'{"new":true}\n'+b'{"partial":')
    assert list(verify.suffix_rows(path, len(prefix))) == [{"new": True}]


@pytest.mark.parametrize("paper,orders,accepted", [(True, False, True), (False, False, False), (True, True, False)])
def test_api_verification_requires_paper_read_only_boundary(monkeypatch, paper, orders, accepted):
    monkeypatch.setattr(verify.requests, "get", lambda *a, **kw: SimpleNamespace(
        raise_for_status=lambda: None,
        json=lambda: {"simulation_only": paper, "production_order_possible": orders}))
    if accepted:
        assert verify._api("https://example.test/api", "positions", {})["simulation_only"]
    else:
        with pytest.raises(RuntimeError, match="boundary missing"):
            verify._api("https://example.test/api", "positions", {})


def test_baseline_hashes_exact_source_pinned_journals_without_replacing_them(tmp_path, monkeypatch):
    manifest = {"status": "candidate_validated_not_applied", "source_pins": [], "market": "m", "live_dir": str(tmp_path)}
    (tmp_path / "ledger_repair_manifest.json").write_text(json.dumps(manifest))
    names = ("signals.jsonl", "orders.jsonl", "fills.jsonl", "marks.jsonl", "events.jsonl")
    for name in names:
        (tmp_path / name).write_bytes(b"keep exact\n")
    result = verify.baseline(tmp_path)
    assert len(result["journals"]) == 5
    for name in names:
        assert (tmp_path / name).read_bytes() == b"keep exact\n"
        assert result["journals"][name]["sha256"] == hashlib.sha256(b"keep exact\n").hexdigest()


@pytest.mark.parametrize("symbols,missing,accepted", [({"3406"}, 0, True), (set(), 0, False), ({"3406"}, 1, False)])
def test_carried_minutes_need_explicit_no_trade_evidence_not_fabricated_freshness(symbols, missing, accepted):
    row = {"historical_minute_replay": True, "minute_valuation_contract": "right_labelled_historical_last_trade_mark_v1",
           "missing_price_position_count": missing, "last_trade_carried_position_count": 1, "stale_position_count": 1}
    if accepted:
        verify.verify_carried_minute(row, no_trade_symbols=symbols, held_symbols={"3406"})
    else:
        with pytest.raises(RuntimeError, match="zero-trade"):
            verify.verify_carried_minute(row, no_trade_symbols=symbols, held_symbols={"3406"})


def test_provisional_coverage_requires_opt_in_marker_and_prior_observed_bar():
    start = datetime.fromisoformat("2026-10-29T09:01:00+08:00")
    marks = [{"market": "m", "session_date": "2026-10-29",
              "minute": (start + timedelta(minutes=i)).isoformat(),
              "missing_price_position_count": 0, "valuation_stale": False} for i in range(270)]
    marks[1].update({"historical_minute_replay": True,
                     "provisional_minute_source_coverage": True,
                     "minute_valuation_contract": "right_labelled_historical_last_trade_mark_v1",
                     "valuation_stale": True, "stale_position_count": 1,
                     "last_trade_carried_position_count": 1})
    mode = {"positions": {"p": {"symbol": "3406", "entry_at": start.isoformat(),
                               "exit_at": "2026-10-29T09:03:00+08:00"}}}
    bars = {"3406": {"2026-10-29T09:01+08:00": {"close": 10., "volume_shares": 1000}}}
    args = dict(market="m", day="2026-10-29", source_files=[])
    with pytest.raises(RuntimeError, match="zero-trade"):
        verify.verify_minute_source_coverage(mode, marks, **args)
    result = verify.verify_minute_source_coverage(mode, marks, allow_unverified_gaps=True, bars=bars, **args)
    assert result["unknown_carried_minute_symbols"] == {"2026-10-29T09:02": ["3406"]}
    assert result["unsourced_held_price_minutes"] == 1
    marks[1]["provisional_minute_source_coverage"] = False
    with pytest.raises(RuntimeError, match="explicit replay marker"):
        verify.verify_minute_source_coverage(mode, marks, allow_unverified_gaps=True, bars=bars, **args)
    marks[1]["provisional_minute_source_coverage"] = True
    with pytest.raises(RuntimeError, match="earlier observed bar"):
        verify.verify_minute_source_coverage(mode, marks, allow_unverified_gaps=True, bars={"3406": {}}, **args)
    with pytest.raises(RuntimeError, match="observed bars"):
        verify.verify_minute_source_coverage(mode, marks, allow_unverified_gaps=True,
            bars={"3406": {**bars["3406"], "2026-10-29T09:02+08:00": {"close": 10.}}}, **args)


def _position():
    return {"market": "factorized", "session_date": "2026-10-14", "symbol": "3406", "side": "short",
            "target_weight": -0.3275, "requested_shares": 4000, "filled_shares": 4000,
            "signed_shares": 0, "entry_at": "2026-10-14T09:01:00+08:00", "entry_price": 1155.7023,
            "exit_at": "2026-10-14T09:33:00+08:00", "exit_price": 1065.0,
            "realized_net_pnl_twd": 353343.55, "total_net_pnl_twd": 353343.55, "simulation_replay": True}


@pytest.mark.parametrize("redacted", [True, False])
def test_public_position_proof_checks_exact_economics_without_exposing_private_ids(redacted):
    row = _position()
    if not redacted:
        row["signal_id"] = "repaired"
    proof = verify.verify_api_positions([row], [_position()], "repaired")
    assert proof["position_rows"] == 1
    assert proof["private_signal_ids_redacted"] is redacted


@pytest.mark.parametrize("field,value", [
    ("session_date", "2026-10-13"), ("symbol", "2330"), ("filled_shares", 3999),
    ("signed_shares", -4000), ("entry_price", 1165.0), ("exit_at", "2026-10-14T13:30:00+08:00"),
    ("total_net_pnl_twd", 0), ("target_weight", -0.5), ("simulation_replay", False),
    ("signal_id", "unrelated"),
])
def test_redacted_position_proof_rejects_wrong_session_quantities_prices_or_signal(field, value):
    row = {**_position(), field: value}
    with pytest.raises(RuntimeError, match="differ|different"):
        verify.verify_api_positions([row], [_position()], "repaired")


def test_position_proof_rejects_missing_financial_fields_and_duplicate_rows():
    row = _position()
    del row["entry_price"]
    with pytest.raises(RuntimeError, match="differ"):
        verify.verify_api_positions([row], [_position()], "repaired")
    with pytest.raises(RuntimeError, match="count differs"):
        verify.verify_api_positions([_position(), _position()], [_position()], "repaired")


@pytest.mark.parametrize("change", [None, "date", "證券代號", "證券名稱", "暫停現股賣出後現款買進當沖註記", "snapshot", "rows"])
def test_post_apply_rule_alias_refresh_requires_pinned_snapshot_and_exact_dated_rules(tmp_path, monkeypatch, change):
    raw = tmp_path / "rule.json"
    raw.write_bytes(b"new official response representation")
    values = {"date": ["2026-10-14"], "證券代號": ["3406"], "證券名稱": ["玉晶光"],
              "暫停現股賣出後現款買進當沖註記": [""]}
    original = pl.DataFrame(values)
    frozen = tmp_path / "snapshot.parquet"
    original.write_parquet(frozen)
    digest = verify.repair._sha256(frozen)
    old_raw_sha = hashlib.sha256(b"original OpenAPI representation").hexdigest()
    proof = {"raw_path": str(raw), "raw_sha256": old_raw_sha,
             "normalized_path": str(frozen), "normalized_sha256": digest, "rows": 1}
    manifest = {"status": "applied", "applied_sessions": [{"session_date": "2026-10-14",
                "eligibility_source_proof": {"twse_day_trade_eligibility": proof}}],
                "source_pins": [{"path": str(raw), "sha256": old_raw_sha},
                                {"path": str(frozen), "sha256": digest}]}
    if change in values:
        values[change] = ["different"]
    current = pl.DataFrame(values).with_columns(pl.lit("dated official report").alias("_table_title"))
    if change == "rows":
        current = pl.concat([current, current])
    if change == "snapshot":
        current.write_parquet(frozen)
    monkeypatch.setattr(verify.replay, "_retained_rule_response_kind", lambda *args: "json")
    monkeypatch.setattr(verify.replay, "_parse_historical_response_content", lambda *args: (current, ".json"))
    if change is not None:
        with pytest.raises(RuntimeError, match="snapshot changed|rule values"):
            verify.verify_applied_source_provenance(manifest)
    else:
        result = verify.verify_applied_source_provenance(manifest)
        assert result["byte_identical_source_pins"] == 1
        refresh = result["mutable_official_reference_refreshes"][0]
        assert refresh["dated_rule_values_unchanged"] is True
        assert refresh["original_raw_bytes_unchanged_claim"] is False
        assert verify.repair._sha256(frozen) == digest
        with pytest.raises(RuntimeError, match="source changed"):
            verify.repair._assert_source_pins(manifest["source_pins"])


def test_post_apply_provenance_never_exempts_prices_code_or_unapplied_candidates(tmp_path):
    source = tmp_path / "prices"
    source.write_bytes(b"changed")
    manifest = {"status": "applied", "applied_sessions": [], "source_pins": [
        {"path": str(source), "sha256": hashlib.sha256(b"original").hexdigest()}]}
    with pytest.raises(RuntimeError, match="source changed"):
        verify.verify_applied_source_provenance(manifest)
    manifest["status"] = "candidate_validated_not_applied"
    with pytest.raises(RuntimeError, match="requires applied"):
        verify.verify_applied_source_provenance(manifest)


def test_post_apply_code_archive_requires_exact_hash_and_never_weakens_preapply(tmp_path, monkeypatch):
    monkeypatch.setattr(verify, "ROOT", tmp_path)
    current = tmp_path / "scripts" / "verify_tw_day_trade_scoped_repair.py"
    current.parent.mkdir()
    current.write_bytes(b"new code")
    root = tmp_path / "operation"
    archive = root / "accepted_code_snapshots_20261104" / current.name
    archive.parent.mkdir(parents=True)
    archive.write_bytes(b"original code")
    digest = hashlib.sha256(b"original code").hexdigest()
    manifest = {"status": "applied", "operation_root": str(root), "applied_sessions": [],
                "source_pins": [{"path": str(current), "sha256": digest}]}
    result = verify.verify_applied_source_provenance(manifest)
    assert result["byte_identical_source_pins"] == 0
    assert result["archived_original_code_pins"][0]["original_path_bytes_unchanged_claim"] is False
    with pytest.raises(RuntimeError, match="source changed"):
        verify.repair._assert_source_pins(manifest["source_pins"])
    archive.write_bytes(b"not the original")
    with pytest.raises(RuntimeError, match="source changed"):
        verify.verify_applied_source_provenance(manifest)


def test_accounting_matches_only_sub_micro_twd_reduction_roundoff():
    baseline = {"position_cohorts": 19, "entry_shares": 255000,
                "net_fees_and_tax_twd": 27944.968349999992,
                "session_realized_net_pnl_twd": -88614.96834999947,
                "shared_minute_capacity_verified": True}
    actual = {**baseline, "session_realized_net_pnl_twd": -88614.9683499995}
    assert verify.accounting_matches(actual, baseline)
    assert not verify.accounting_matches({**actual, "entry_shares": 254000}, baseline)
    assert not verify.accounting_matches({**actual, "session_realized_net_pnl_twd": -88614.968}, baseline)
    assert not verify.accounting_matches({**actual, "net_fees_and_tax_twd": float("nan")}, baseline)
    assert not verify.accounting_matches({**actual, "shared_minute_capacity_verified": 1}, baseline)

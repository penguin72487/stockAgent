from __future__ import annotations

import hashlib
import json
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

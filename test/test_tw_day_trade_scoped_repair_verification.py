from __future__ import annotations

import hashlib
import json
from types import SimpleNamespace

import pytest
import polars as pl

from scripts import verify_tw_day_trade_scoped_repair as verify


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

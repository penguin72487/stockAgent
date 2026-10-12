from datetime import datetime, timedelta
import json
from pathlib import Path
import sqlite3

import polars as pl
import pytest

from stockagent.data.source_rows import SourceRows, merge_source_rows, lineage_segments
from stockagent.data.tw_minute_sources import (
    KEYS, FIELDS, SEMANTICS, normalize_shioaji, normalize_finmind, same_day_share_proofs,
    dated_market_share_proofs, resolve_share_proofs,
)
from scripts.merge_tw_minute_sources import assemble, inventory, attach_unit_evidence, _digest
from stockagent.data.tw_minute_unit_evidence import pin_official_market_inputs, read_official_market_evidence
from downloader.artifact_io import sha256_file


def _s(minutes=(1, 2, 3), prices=None):
    prices = list(prices) if prices is not None else [100.] * len(minutes)
    return pl.DataFrame({"symbol": ["2330"] * len(minutes),
        "ts": [datetime(2020, 3, 2, 9) + timedelta(minutes=n) for n in minutes],
        "Open": prices, "High": [p + 1 for p in prices], "Low": [p - 1 for p in prices],
        "Close": prices, "Volume": [2.] * len(minutes), "Amount": [p * 2000 for p in prices],
        "contract_unit": [1000.] * len(minutes)})


def _f(minutes=(0, 1, 2), *, symbol="2330", prices=None, volume=2):
    prices = list(prices) if prices is not None else [100.] * len(minutes)
    return pl.DataFrame({"stock_id": [symbol] * len(minutes), "date": ["2020-03-02"] * len(minutes),
        "minute": [(datetime(2020, 3, 2, 9) + timedelta(minutes=n)).strftime("%H:%M:%S") for n in minutes],
        "open": prices, "high": [p + 1 for p in prices], "low": [p - 1 for p in prices],
        "close": prices, "volume": [volume] * len(minutes)})


def _merge(primary, backup):
    return merge_source_rows([SourceRows("shioaji", "primary-sha", SEMANTICS, primary),
        SourceRows("finmind", "backup-sha", SEMANTICS, backup)], keys=KEYS, fields=FIELDS, semantics=SEMANTICS)


def test_left_labels_and_auction_points_are_distinct_and_causal():
    frame = normalize_finmind(_f((0, 270, 273, 330), symbol="TAIEX", volume=0))
    assert frame["ts"].to_list() == [datetime(2020, 3, 2, 9, 1), datetime(2020, 3, 2, 13, 30),
        datetime(2020, 3, 2, 13, 33), datetime(2020, 3, 2, 14, 30)]
    assert frame["phase"].to_list() == ["regular", "closing_auction", "delayed_closing_auction", "afterhours_fixed_price"]
    assert frame["volume_shares"].null_count() == frame.height
    assert frame["asset_class"].unique().to_list() == ["index"]


def test_documented_volume_scale_needs_same_day_positive_matching_peer_evidence():
    s = normalize_shioaji(_s())
    raw = _f()
    proof = same_day_share_proofs(raw, s)
    assert proof.select("multiplier", "matching_bars").rows() == [(1000., 3)]
    assert normalize_finmind(raw, share_proofs=proof)["volume_shares"].to_list() == [2000.] * 3
    assert normalize_finmind(raw)["volume_shares"].null_count() == 3
    next_day = raw.with_columns(pl.lit("2020-03-03").alias("date"))
    assert normalize_finmind(next_day, share_proofs=proof)["volume_shares"].null_count() == 3
    assert same_day_share_proofs(raw.head(2), s).is_empty()
    assert same_day_share_proofs(raw.with_columns(pl.lit(0).alias("volume")), s).is_empty()


def test_malformed_units_and_clock_are_not_executable_or_unit_evidence():
    bad_proof = pl.DataFrame({"symbol": ["2330"], "date": ["2020-03-02"], "multiplier": [-1000.]})
    with pytest.raises(ValueError, match="undocumented"):
        normalize_finmind(_f(), share_proofs=bad_proof)
    raw = _f().with_columns(pl.lit("09:00:01").alias("minute"))
    assert normalize_finmind(raw)["_invalid_reason"].unique().to_list() == ["invalid_native_clock"]
    assert same_day_share_proofs(raw, normalize_shioaji(_s())).is_empty()
    s = normalize_shioaji(_s()).with_columns(pl.lit("invalid_ohlc").alias("_invalid_reason"))
    assert same_day_share_proofs(_f(), s).is_empty()


def test_missing_primary_is_filled_without_provider_features():
    s = normalize_shioaji(_s())
    raw = _f((0, 1, 2, 3))
    f = normalize_finmind(raw, share_proofs=same_day_share_proofs(raw, s))
    result = _merge(s, f)
    assert result.values.height == 4
    assert result.values.columns == KEYS + FIELDS
    assert not set(result.values.columns) & {"_provider", "_source_ref", "selection_reason"}
    assert result.audit["selection"] == {"primary_valid": 3, "missing_primary_filled": 1}
    assert result.conflicts.is_empty()
    assert result.values.filter(pl.col("ts").dt.minute() == 4)["volume_shares"].item() == 2000.


def test_genuine_bad_primary_is_replaced_but_valid_disagreement_is_not():
    s = normalize_shioaji(_s(prices=[100., 100., 100.]))
    raw = _f(prices=[100., 102., 100.])
    f = normalize_finmind(raw)
    # A negative OHLC is objectively invalid; a different valid price is not.
    s = s.with_columns(pl.when(pl.col("ts").dt.minute() == 1).then(-1.)
        .otherwise(pl.col("Open")).alias("Open"),
        pl.when(pl.col("ts").dt.minute() == 1).then(pl.lit("invalid_ohlc"))
        .otherwise(pl.col("_invalid_reason")).alias("_invalid_reason"))
    result = _merge(s, f)
    assert result.audit["selection"]["repaired_rejected_primary"] == 1
    assert result.values["Close"].to_list() == [100., 100., 100.]
    assert result.conflicts.height == 1
    assert result.conflicts["_provider"].item() == "finmind"
    assert result.rejected.height == 1


def test_conflicting_duplicates_cannot_win_by_order_or_generate_unit_votes():
    raw = pl.concat([_f(), _f((0,), prices=[101.])])
    s = normalize_shioaji(_s())
    assert same_day_share_proofs(raw, s).is_empty()
    result = _merge(normalize_finmind(raw), s)
    assert result.audit["duplicate_rows"] == 1
    assert result.audit["selection"]["repaired_rejected_primary"] == 1
    assert result.rejected["_invalid_reason"].unique().to_list() == ["conflicting_provider_duplicates"]


def test_identical_duplicates_collapse_and_null_barrier_is_not_imputed():
    f = normalize_finmind(_f())
    result = _merge(pl.concat([f, f.head(1)]), f)
    assert result.values.height == 3 and result.audit["duplicate_rows"] == 1
    assert result.values["volume_shares"].null_count() == 3
    assert result.conflicts.is_empty()


def test_semantically_different_data_is_never_joined_by_name():
    frame = normalize_finmind(_f())
    other = {**SEMANTICS, "price": "split_adjusted"}
    with pytest.raises(ValueError, match="semantic equivalence"):
        merge_source_rows([SourceRows("provider", "sha", other, frame)], keys=KEYS, fields=FIELDS, semantics=SEMANTICS)


def test_lineage_gaps_split_and_report_exact_intervals():
    result = _merge(normalize_shioaji(_s((1, 2, 4))), normalize_finmind(_f((0, 1, 3))))
    segments = lineage_segments(result.lineage, groups=["asset_class", "symbol", "phase"])
    assert segments["rows"].to_list() == [2, 1]
    assert segments["first_ts"].to_list() == [datetime(2020, 3, 2, 9, 1), datetime(2020, 3, 2, 9, 4)]


def _sources(tmp_path):
    f_root, s_root = tmp_path / "finmind", tmp_path / "shioaji"
    f_root.mkdir(); s_root.mkdir()
    raw = _f((0, 1, 2, 3))
    f_path = f_root / "native.parquet"
    raw.write_parquet(f_path)
    receipt = {"dataset": "TaiwanStockKBar", "data_id": "", "partition": "2020-03-02",
        "status": "complete", "rows": raw.height, "parquet_path": f_path.name,
        "parquet_size_bytes": f_path.stat().st_size, "sha256": sha256_file(f_path)}
    (f_root / "receipt.json").write_text(json.dumps(receipt))
    with sqlite3.connect(f_root / "queue.sqlite3") as conn:
        conn.execute("CREATE TABLE tasks(dataset,data_id,partition,rows,receipt_path,state,first_data_date,last_data_date)")
        conn.execute("INSERT INTO tasks VALUES(?,?,?,?,?,?,?,?)", (receipt["dataset"], "", "2020-03-02",
            raw.height, "receipt.json", "complete", "2020-03-02", "2020-03-02"))
        conn.execute("INSERT INTO tasks VALUES(?,?,?,?,?,?,?,?)", (receipt["dataset"], "2330", "2020-03-02",
            raw.height, "DO_NOT_READ_OLD_RECEIPT.json", "deprecated_query_shape", "2020-03-02", "2020-03-02"))
    destination = s_root / "trade_date=2020-03-02"
    destination.mkdir()
    s = _s()
    s_path = destination / "data.parquet"
    s.write_parquet(s_path)
    summary = {"source": "shioaji_kbars_1m", "status": "ok", "trade_date": "2020-03-02",
        "rows": s.height, "output_sha256": sha256_file(s_path)}
    (destination / "summary.json").write_text(json.dumps(summary))
    manifest = {"source": "shioaji_kbars_1m", "research_ready": True, "partitions": [{
        **summary, "output": "/outside-producer/data.parquet"}]}
    (s_root / "manifest.json").write_text(json.dumps(manifest))
    return inventory(f_root, s_root), f_root, s_root


def test_pinned_inventory_uses_only_current_receipts_and_selected_root(tmp_path):
    pinned, _, s_root = _sources(tmp_path)
    assert len(pinned["records"]) == 2
    assert pinned["records"][1]["path"] == str((s_root / "trade_date=2020-03-02/data.parquet").resolve())
    assert not pinned["all_source_content_validated"]
    assert pinned["methods"]["training_ready"] is False


def test_real_assembly_retains_exact_sidecars_but_no_provider_columns(tmp_path):
    pinned, _, _ = _sources(tmp_path)
    output, report = tmp_path / "view", tmp_path / "audit"
    result = assemble(pinned, report_root=report, output_root=output, symbols=[])
    assert result["counts"]["canonical_rows"] == 4
    assert result["counts"]["rejected_input_files"] == 0
    assert result["all_source_content_validated"]
    assert not result["training_ready"]
    values = pl.read_parquet(output / "trade_date=2020-03-02/data.parquet")
    assert values.columns == KEYS + FIELDS
    assert (report / "sessions/2020-03-02/source_refs.json").is_file()
    segments = pl.read_parquet(report / "sessions/2020-03-02/lineage_segments.parquet")
    assert dict(segments.select("_provider", "rows").iter_rows()) == {"shioaji": 3, "finmind": 1}
    with pytest.raises(ValueError, match="never overwritten"):
        assemble(pinned, report_root=report, output_root=output, symbols=[])


def test_corrupt_primary_bytes_are_not_read_and_verified_backup_repairs(tmp_path):
    pinned, _, s_root = _sources(tmp_path)
    path = s_root / "trade_date=2020-03-02/data.parquet"
    path.write_bytes(b"corrupt bytes; must never be interpreted")
    output, report = tmp_path / "view", tmp_path / "audit"
    result = assemble(pinned, report_root=report, output_root=output, symbols=[])
    assert result["counts"]["rejected_input_files"] == 1
    assert result["selection"] == {"repaired_rejected_primary_input": 4}
    assert not result["all_selected_input_bytes_validated"]
    errors = json.loads((report / "sessions/2020-03-02/rejected_inputs.json").read_text())
    assert errors[0]["source"]["provider"] == "shioaji"
    assert "fingerprint mismatch" in errors[0]["error"]
    assert pl.read_parquet(output / "trade_date=2020-03-02/data.parquet")["volume_shares"].null_count() == 4


def test_inventory_rejects_wrong_session_summary_and_path_escape(tmp_path):
    _, f_root, s_root = _sources(tmp_path)
    path = s_root / "trade_date=2020-03-02/summary.json"
    summary = json.loads(path.read_text())
    summary["trade_date"] = "2020-03-03"
    path.write_text(json.dumps(summary))
    with pytest.raises(ValueError, match="summary identity"):
        inventory(f_root, s_root)
    summary["trade_date"] = "2020-03-02"
    path.write_text(json.dumps(summary))
    (f_root / "native.parquet").unlink()
    (f_root / "native.parquet").symlink_to(tmp_path / "outside.parquet")
    with pytest.raises(ValueError, match="unsafe FinMind parquet"):
        inventory(f_root, s_root)


def test_markdown_report_contains_provider_intervals_and_aliases_not_model_channels(tmp_path):
    from scripts.report_tw_raw_source_merge import render
    pinned, _, _ = _sources(tmp_path)
    inventory_path = tmp_path / "inventory.json"
    inventory_path.write_text(json.dumps(pinned))
    report = assemble(pinned, report_root=tmp_path / "audit", output_root=tmp_path / "view", symbols=["2330"])
    report_path = tmp_path / "audit" / "merge_report.json"
    report_path.write_text(json.dumps(report))
    output = tmp_path / "index.md"
    render(inventory_path, [report_path], output)
    text = output.read_text()
    assert "來源區段" in text and "2005" in text and "TAIEX" in text
    assert "TaiwanStockBalanceSheet" in text and "資產總額" in text
    assert "equity:2330:finmind" in text and "equity:2330:shioaji" in text
    assert "模型數值表不含 provider" in text
    intervals = (tmp_path / "lineage" / "audit" / "2330.md").read_text()
    assert "09:01" in intervals and "09:03" in intervals and "09:04" in intervals
    assert "missing_primary_filled" in intervals
    receipt = json.loads(output.with_suffix(".receipt.json").read_text())
    assert receipt["output_sha256"] == sha256_file(output)
    assert receipt["contract"] == "tw_raw_merge_markdown_v1"


def test_financial_null_observation_stays_a_barrier_and_clock_is_part_of_key():
    semantics = {"unit": "TWD", "clock": "published_at", "period": "quarter"}
    keys, fields = ["symbol", "period", "published_at"], ["value"]
    primary = pl.DataFrame({"symbol": ["2330"], "period": ["2020Q1"],
        "published_at": [datetime(2020, 5, 15)], "value": [None], "_invalid_reason": [""]},
        schema_overrides={"value": pl.Float64})
    candidate = primary.with_columns(pl.lit(100.).alias("value"))
    merged = merge_source_rows([SourceRows("official", "a", semantics, primary),
        SourceRows("finmind", "b", semantics, candidate)], keys=keys, fields=fields, semantics=semantics)
    assert merged.values["value"].item() is None
    late = candidate.with_columns(pl.lit(datetime(2020, 5, 16)).alias("published_at"))
    merged = merge_source_rows([SourceRows("official", "a", semantics, primary),
        SourceRows("finmind", "b", semantics, late)], keys=keys, fields=fields, semantics=semantics)
    assert merged.values.height == 2  # Never backdate the later observation.


def _market_evidence(*, venue="twse", day="2020-03-02", symbol="2330"):
    return pl.DataFrame({"symbol": [symbol], "date": [day], "venue": [venue], "source_ref": ["official-raw-sha"]})


def test_official_dated_venue_proves_lots_without_broker_overlap():
    proof = dated_market_share_proofs(_market_evidence())
    assert proof["multiplier"].item() == 1000
    assert normalize_finmind(_f(), share_proofs=proof)["volume_shares"].to_list() == [2000.] * 3
    next_day = dated_market_share_proofs(_market_evidence(day="2020-03-03"))
    assert normalize_finmind(_f(), share_proofs=next_day)["volume_shares"].null_count() == 3
    # A market proof never invents a monetary amount or index trading quantity.
    assert normalize_finmind(_f(), share_proofs=proof)["Amount"].null_count() == 3
    index_proof = dated_market_share_proofs(_market_evidence(symbol="TAIEX"))
    assert normalize_finmind(_f(symbol="TAIEX", volume=0), share_proofs=index_proof)["volume_shares"].null_count() == 3


def test_emerging_and_dated_transition_use_documented_different_units():
    evidence = pl.concat([_market_evidence(venue="emerging"), _market_evidence(venue="tpex", day="2020-03-03")])
    proof = dated_market_share_proofs(evidence)
    assert normalize_finmind(_f(), share_proofs=proof)["volume_shares"].to_list() == [2.] * 3
    next_day = _f().with_columns(pl.lit("2020-03-03").alias("date"))
    assert normalize_finmind(next_day, share_proofs=proof)["volume_shares"].to_list() == [2000.] * 3
    with pytest.raises(ValueError, match="undocumented"):
        dated_market_share_proofs(_market_evidence(venue="current_snapshot"))
    with pytest.raises(ValueError, match="missing dated"):
        dated_market_share_proofs(_market_evidence().with_columns(pl.lit("").alias("source_ref")))


def test_independent_unit_proofs_corroborate_but_conflicting_units_stay_null():
    peer = same_day_share_proofs(_f(), normalize_shioaji(_s()))
    proof, conflicts = resolve_share_proofs(peer, dated_market_share_proofs(_market_evidence()), peer_ref="peer-sha")
    assert conflicts.is_empty()
    assert proof["matching_bars"].item() == 3
    assert "documented_unit" in proof["method"].item() and "peer" in proof["method"].item()
    conflicting, conflicts = resolve_share_proofs(peer, dated_market_share_proofs(_market_evidence(venue="emerging")), peer_ref="peer-sha")
    assert conflicting.is_empty() and conflicts.height == 2
    assert normalize_finmind(_f(), share_proofs=conflicting)["volume_shares"].null_count() == 3
    # Two venues with the same documented unit are not a unit disagreement.
    dated = dated_market_share_proofs(pl.concat([_market_evidence(), _market_evidence(venue="tpex")]))
    proof, conflicts = resolve_share_proofs(peer, dated, peer_ref="peer-sha")
    assert proof["multiplier"].item() == 1000. and conflicts.is_empty()


def _official_source(tmp_path, *, declared_date="20200302", path_suffix="2020-03-02.json"):
    root = tmp_path / "official"
    raw = root / "raw/twse_daily_ohlcv" / path_suffix
    raw.parent.mkdir(parents=True, exist_ok=True)
    raw.write_text(json.dumps({"stat": "OK", "date": declared_date,
        "params": {"date": declared_date}, "tables": [{"title": "109年03月02日 每日收盤行情",
        "fields": ["證券代號", "成交股數", "開盤價", "最高價", "最低價", "收盤價"],
        "data": [["2330", "2000", "100", "101", "99", "100"]]}]}))
    journal = root / "state/journals/twse_daily_ohlcv.jsonl"
    journal.parent.mkdir(parents=True, exist_ok=True)
    receipt = {"schema_version": 1, "dataset": "twse_daily_ohlcv", "date": "2020-03-02", "status": "data",
        "url": "https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX?date=20200302&type=ALLBUT0999&response=json",
        "raw_path": str(raw.relative_to(root)), "raw_sha256": sha256_file(raw), "raw_size": raw.stat().st_size}
    journal.write_text(json.dumps(receipt) + "\n")
    return root, raw, journal, receipt


def test_official_raw_date_and_fingerprint_are_required_not_filename(tmp_path):
    root, raw, _, _ = _official_source(tmp_path)
    selected = pin_official_market_inputs(root, {"2020-03-02"})
    evidence = read_official_market_evidence(selected[0], symbols=["2330", "0050"])
    assert evidence["symbol"].to_list() == ["2330"]
    raw.write_bytes(b"tampered")
    with pytest.raises(ValueError, match="fingerprint"):
        read_official_market_evidence(selected[0], symbols=[])


def test_retitled_stale_official_body_cannot_prove_historical_market(tmp_path):
    root, _, _, _ = _official_source(tmp_path, declared_date="20200303")
    selected = pin_official_market_inputs(root, {"2020-03-02"})
    with pytest.raises(ValueError, match="date mismatch"):
        read_official_market_evidence(selected[0], symbols=[])


def test_latest_failed_journal_is_not_replaced_by_older_success(tmp_path):
    root, _, journal, receipt = _official_source(tmp_path)
    journal.write_text(journal.read_text() + json.dumps({**receipt, "status": "failed"}) + "\n")
    assert pin_official_market_inputs(root, {"2020-03-02"}) == []
    assert pin_official_market_inputs(root, {"2020-03-03"}) == []


def test_unit_evidence_source_url_and_path_cannot_escape_official_scope(tmp_path):
    root, _, journal, receipt = _official_source(tmp_path)
    journal.write_text(json.dumps({**receipt, "url": receipt["url"].replace("www.twse.com.tw", "not-official.example")}) + "\n")
    with pytest.raises(ValueError, match="URL mismatch"):
        pin_official_market_inputs(root, {"2020-03-02"})
    journal.write_text(json.dumps({**receipt, "raw_path": "../outside.json"}) + "\n")
    with pytest.raises(ValueError, match="unsafe"):
        pin_official_market_inputs(root, {"2020-03-02"})


def test_real_unit_repair_reproduces_previous_output_and_only_adds_verified_quantity(tmp_path):
    pinned, _, _ = _sources(tmp_path)
    # Historical extension: FinMind available, broker history not available.
    pinned["records"] = [r for r in pinned["records"] if r["provider"] == "finmind"]
    pinned["source_inventory_sha256"] = _digest(pinned["records"])
    baseline = assemble(pinned, report_root=tmp_path / "baseline-audit", output_root=tmp_path / "baseline", symbols=["2330"])
    assert baseline["counts"]["equity_rows_without_share_quantity"] == 4
    root, _, _, _ = _official_source(tmp_path)
    repaired = attach_unit_evidence(pinned, official_root=root)
    result = assemble(repaired, report_root=tmp_path / "repair-audit", output_root=tmp_path / "repair",
                      symbols=["2330"], previous_report=baseline)
    assert result["counts"]["equity_rows_without_share_quantity"] == 0
    assert result["repair_comparison"]["resolved_share_rows"] == 4
    assert result["repair_comparison"]["prices_amount_keys_provider_and_known_shares_unchanged"]
    assert result["partitions"][0]["previous_output_sha256_reproduced"]
    proof = pl.read_parquet(tmp_path / "repair-audit/sessions/2020-03-02/same_day_share_proofs.parquet")
    assert proof["multiplier"].item() == 1000.
    assert (tmp_path / "repair-audit/sessions/2020-03-02/unit_evidence_refs.json").exists()

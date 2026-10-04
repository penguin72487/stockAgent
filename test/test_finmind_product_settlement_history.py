from datetime import UTC, datetime
import json

import pytest

from downloader import download_finmind_complement as c


NOW = datetime(2026, 9, 27, 7, tzinfo=UTC)


def _master(root):
    task = c.Task("TaiwanFutOptDailyInfo", "", "latest", "snapshot", 0, "pending")
    return c._store(root, task, [
        {"code": "TX", "type": "TaiwanFuturesDaily"},
        {"code": "MTX", "type": "TaiwanFuturesDaily"},
        {"code": "ZZF", "type": "TaiwanFuturesDaily"},
        {"code": "TXO", "type": "TaiwanOptionDaily"},
        {"code": "ZZA", "type": "TaiwanOptionDaily"},
    ], NOW)


def test_verified_catalog_seeds_every_known_product_not_only_tx_txo(tmp_path):
    _master(tmp_path)
    with c._db(tmp_path / "queue.sqlite3") as conn:
        c._populate(conn, tmp_path, today=NOW.date())
        for dataset, expected in [("TaiwanFuturesFinalSettlementPrice", ["MTX", "TX", "ZZF"]),
                                  ("TaiwanOptionFinalSettlementPrice", ["TXO", "ZZA"])]:
            assert conn.execute("SELECT data_id,kind FROM tasks WHERE dataset=? ORDER BY data_id",
                                (dataset,)).fetchall() == [(item, "id_history") for item in expected]
        status = c._status(conn, tmp_path, state="test")
        assert status["candidate_universe"]["historical_universe_verified_complete"] is False


@pytest.mark.parametrize("damage", ["size", "hash", "missing_hash", "bytes"])
def test_derivative_catalog_requires_exact_size_and_hash(tmp_path, damage):
    receipt = _master(tmp_path)
    path = tmp_path / receipt["receipt_path"]
    assert c._derivative_ids(tmp_path, "TaiwanFuturesDaily") == ["MTX", "TX", "ZZF"]
    if damage == "size":
        receipt["parquet_size_bytes"] += 1
    elif damage == "hash":
        receipt["sha256"] = "0" * 64
    elif damage == "missing_hash":
        receipt.pop("sha256")
    else:
        parquet = tmp_path / receipt['parquet_path']
        data = bytearray(parquet.read_bytes())
        data[8] ^= 1
        parquet.write_bytes(data)
    path.write_text(json.dumps(receipt))
    assert c._derivative_ids(tmp_path, "TaiwanFuturesDaily") == []


def test_product_settlement_never_delegates_to_no_id_sponsor(tmp_path):
    root = tmp_path / "complement"
    sponsor = tmp_path / "sponsor"
    sponsor.mkdir()
    (sponsor / "status.json").write_text(json.dumps({
        "tier": "Sponsor", "state": "running", "observed_at_utc": NOW.isoformat(),
        "series": {dataset: {"target": 10, "blocked": 0} for dataset in c.PRODUCT_HISTORY_STARTS},
    }))
    assert c._sponsor_delegated(root, NOW) == frozenset()


@pytest.mark.parametrize("dataset,identifier,start,field", [
    ("TaiwanFuturesFinalSettlementPrice", "MTX", "1998-01-01", "futures_id"),
    ("TaiwanOptionFinalSettlementPrice", "ZZA", "2001-01-01", "option_id"),
])
def test_product_query_is_bounded_full_history_and_preserves_expiry_precision(
    tmp_path, monkeypatch, dataset, identifier, start, field,
):
    calls = []
    rows = [{"date": "2025-12-31", field: identifier, "contract_month": "202512",
             "settlement_price": 123.456789, "notional_value": 24691.3578}]
    monkeypatch.setattr(c, "_fetch_rows", lambda *_a, **_kw: calls.append((_a[-1], _kw)) or rows)
    task = c.Task(dataset, identifier, "history", "id_history", 3, "pending")
    result = c._request(object(), object(), tmp_path, task, "placeholder", today=NOW.date())
    assert calls == [({"dataset": dataset, "data_id": identifier, "start_date": start,
                       "end_date": NOW.date().isoformat()}, {"max_response_bytes": c.BULK_MAX_RESPONSE_BYTES})]
    receipt = c._store(tmp_path, task, result, NOW)
    assert receipt["source_last_date"] == "2025-12-31"
    assert receipt["fetched_at_utc"] == NOW.isoformat()
    assert receipt["historical_point_in_time"] is False
    import pyarrow.parquet as pq
    assert pq.read_table(tmp_path / receipt["parquet_path"]).to_pylist() == rows


def test_product_query_rejects_missing_identifier_before_fetch_and_future_rows(tmp_path, monkeypatch):
    dataset = "TaiwanFuturesFinalSettlementPrice"
    calls = []
    monkeypatch.setattr(c, "_fetch_rows", lambda *_a, **_kw: calls.append(True) or [{"date": "2027-01-01"}])
    task = c.Task(dataset, "", "2026", "year", 0, "pending")
    with pytest.raises(c.SourceError, match="provider_bad_request"):
        c._request(object(), object(), tmp_path, task, "placeholder", today=NOW.date())
    assert not calls
    task = c.Task(dataset, "TX", "history", "id_history", 3, "pending")
    with pytest.raises(c.SourceError, match="response_outside_partition"):
        c._request(object(), object(), tmp_path, task, "placeholder", today=NOW.date())


def test_successful_product_history_refreshes_on_next_publishing_day(tmp_path):
    dataset = "TaiwanFuturesFinalSettlementPrice"
    task = c.Task(dataset, "TX", "history", "id_history", 3, "pending")
    empty = c.Task(dataset, "ZZF", "history", "id_history", 3, "pending")
    with c._db(tmp_path / "queue.sqlite3") as conn:
        c._add_tasks(conn, [(t.dataset, t.data_id, t.partition, t.kind, t.priority) for t in [task, empty]])
        good = c._store(tmp_path, task, [{"date": "2025-12-31", "futures_id": "TX"}], NOW)
        c._save_result(conn, task, good, NOW)
        c._save_result(conn, empty, c._store(tmp_path, empty, [], NOW), NOW)
        assert conn.execute("SELECT priority,next_attempt_at_utc FROM tasks WHERE data_id='TX'").fetchone() == (
            0, "2026-09-27T16:00:00+00:00",  # Sunday fetch -> Monday 00:00 Taipei.
        )
        assert conn.execute("SELECT priority,next_attempt_at_utc FROM tasks WHERE data_id='ZZF'").fetchone() == (
            3, "2026-10-27T07:00:00+00:00",
        )
        assert c._next_task(conn, datetime(2026, 9, 27, 10, 1, tzinfo=UTC), incremental_only=True) is None
        assert c._next_task(conn, datetime(2026, 9, 27, 16, 0, tzinfo=UTC), incremental_only=True).data_id == "TX"

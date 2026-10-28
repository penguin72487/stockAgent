from datetime import date, datetime
import json
from pathlib import Path
import sqlite3

import polars as pl
import pytest

from stockagent.live import bar_inventory as bars
from stockagent.live.tick_inventory import _Sources


def parquet(path, days):
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({"date": days, "close": [1.] * len(days)}).write_parquet(path)


def test_date_census_uses_observations_not_span_or_symbol_days_and_warm_cache(tmp_path, monkeypatch):
    a, b = tmp_path / "a.parquet", tmp_path / "b.parquet"
    parquet(a, [date(2020, 1, 2), date(2020, 1, 6), date(2020, 1, 6)])
    parquet(b, [date(2020, 1, 6), date(2020, 1, 10)])
    census = bars._DateCensus({}, 10)
    facts = census.collect([a, b])
    assert facts["days"] == 3 and facts["known_count"] == 5
    def forbidden(*args):
        raise AssertionError("unchanged files must not be projected again")
    monkeypatch.setattr(bars, "_project_dates", forbidden)
    warm = bars._DateCensus({"dates": census.current}, 10)
    assert warm.collect([a, b])["days"] == 3 and warm.reads == 0
    parquet(b, [date(2020, 1, 7)])
    pending = bars._DateCensus({"dates": census.current}, 10)
    pending.remaining_seconds = 0
    facts = pending.collect([a, b])
    assert facts["days"] is None and facts["known_days"] == 2 and facts["date_files_pending"] == 1


def test_date_projection_timezone_null_and_corrupt_dates_are_explicit(tmp_path):
    path = tmp_path / "data.parquet"
    parquet(path, ["2026-10-23T23:59:00", "2026-10-24T00:01:00", None])
    census = bars._DateCensus({}, 10)
    facts = census.collect([path])
    assert facts["days"] is None and facts["known_days"] == 2 and facts["null_date_rows"] == 1
    parquet(path, ["invalid"])
    facts = bars._DateCensus({}, 10).collect([path])
    assert facts["days"] is None and facts["invalid_date_files"] == 1
    # Valid footer bounds must not hide an invalid interior value or let a
    # strict Polars parse error abort every provider's monitoring snapshot.
    parquet(path, ["2026-02-28", "2026-02-30", "2026-03-01"])
    facts = bars._DateCensus({}, 10).collect([path])
    assert facts["days"] is None and facts["invalid_date_files"] == 1


def test_repeated_date_sets_keep_each_file_count_and_invalidation(tmp_path):
    a, b = tmp_path / "a.parquet", tmp_path / "b.parquet"
    parquet(a, [date(2020, 1, 2)])
    parquet(b, [date(2020, 1, 2), date(2020, 1, 2)])
    cold = bars._DateCensus({}, 10)
    assert cold.collect([a, b])["known_count"] == 3
    warm = bars._DateCensus({"dates": cold.current}, 10)
    facts = warm.collect([a, b])
    assert facts["days"] == 1 and facts["known_count"] == 3
    assert warm.reads == 0
    # A shared bitmap never makes the two physical source identities aliases.
    # Replacing one source must project its new dates, while retaining the other.
    parquet(b, [date(2020, 1, 7)])
    facts = warm.collect([a, b])
    assert facts["days"] == 2 and facts["known_count"] == 2
    assert facts["known_latest_date"] == "2020-01-07"
    assert warm.reads == 1


def test_shared_file_projection_is_reused_within_census_but_replacement_is_rechecked(tmp_path, monkeypatch):
    source = tmp_path / "shared.parquet"
    parquet(source, [date(2020, 1, 2), date(2020, 1, 2)])
    project = bars._project_dates
    reads = []
    def counted(path, **kwargs):
        reads.append(path)
        return project(path, **kwargs)
    monkeypatch.setattr(bars, "_project_dates", counted)
    census = bars._DateCensus({}, 10)
    first_group = census.collect([source])
    assert census.collect([source]) == first_group
    assert reads == [source] and census.reads == 1
    assert first_group["known_count"] == 2
    parquet(source, [date(2020, 1, 7)])
    replaced = census.collect([source])
    assert reads == [source, source] and census.reads == 2
    assert replaced["known_count"] == 1
    assert replaced["known_latest_date"] == "2020-01-07"


def test_census_rejects_mutation_during_projection(tmp_path, monkeypatch):
    path = tmp_path / "data.parquet"
    parquet(path, [date(2020, 1, 1)])
    project = bars._project_dates
    def change(path, **kwargs):
        result = project(path, **kwargs)
        parquet(path, [date(2020, 1, 2)])
        return result
    monkeypatch.setattr(bars, "_project_dates", change)
    facts = bars._DateCensus({}, 10).collect([path])
    assert facts["days"] is None and facts["invalid_date_files"] == 1


def test_settlement_clock_and_warm_manifest_member_proofs(tmp_path):
    path = tmp_path / "settlement.parquet"
    pl.DataFrame({"settlement_date": [date(2026, 10, 21), date(2026, 10, 22)]}).write_parquet(path)
    assert bars._DateCensus({}, 10).collect([path])["days"] == 2
    # A changed member must invalidate a pointer-stable cached proof, without
    # reading/hashing any payload merely to decide whether the proof is reusable.
    pointer, member = tmp_path / "data.parquet.manifest.json", tmp_path / "member.parquet"
    pointer.write_text("{}")
    member.write_bytes(b"old")
    cached = {str(pointer): {"signature": bars._signature(pointer), "bitmap": bars._encode(1),
        "count": 1, "null_dates": 0, "members": [[str(member), bars._signature(member)]]}}
    assert bars._DateCensus({"dates": cached}, 10).collect([pointer])["days"] == 1
    member.write_bytes(b"new")
    census = bars._DateCensus({"dates": cached}, 10)
    census.remaining_seconds = 0
    result = census.collect([pointer])
    assert result["days"] is None and result["date_files_pending"] == 1


@pytest.fixture
def partition_dates(tmp_path, monkeypatch):
    import pyarrow as pa
    from stockagent.data import partitioned_parquet as parts
    from stockagent.data_sync import cache_capacity
    controls = tmp_path / "capacity-controls"
    controls.mkdir()
    monkeypatch.setattr(cache_capacity, "CONTROL_ROOT", controls)
    monkeypatch.setattr(cache_capacity, "_admit_backing_volume", lambda *_: {})
    path = tmp_path / "bars.parquet"
    def publish(days):
        value = pa.table({"date": pa.array(days, type=pa.date32()), "close": [1.] * len(days)})
        parts.write_partitioned_table(value, path, source_identity={"fixture": "exact dates"})
        return parts.manifest_path(path)
    return publish


def test_changed_manifest_reuses_only_unchanged_member_date_projections(partition_dates, monkeypatch):
    pointer = partition_dates([date(2024, 1, 2), date(2024, 1, 6), date(2025, 1, 3)])
    first = bars._DateCensus({}, 60)
    assert first.collect([pointer])["days"] == 3
    calls = []
    project = bars._project_member_dates
    def observed(member, *args):
        calls.append(str(member))
        return project(member, *args)
    monkeypatch.setattr(bars, "_project_member_dates", observed)
    pointer = partition_dates([date(2024, 1, 2), date(2024, 1, 6),
                               date(2025, 1, 3), date(2025, 1, 8)])
    second = bars._DateCensus({"dates": first.current}, 60)
    result = second.collect([pointer])
    assert result["days"] == 4 and result["known_count"] == 4
    assert len(calls) == 1
    calls.clear()
    pointer = partition_dates([date(2024, 1, 2), date(2024, 1, 6)])
    removed = bars._DateCensus({"dates": second.current}, 60)
    result = removed.collect([pointer])
    assert result["days"] == 2 and result["known_count"] == 2
    assert result["known_latest_date"] == "2024-01-06"
    assert calls == []


@pytest.mark.parametrize("damage", ["bitmap", "identity", "clock", "policy", "count", "missing"])
def test_changed_manifest_invalid_member_date_proof_falls_back(partition_dates, monkeypatch, damage):
    pointer = partition_dates([date(2024, 1, 2), date(2024, 1, 6)])
    first = bars._DateCensus({}, 60)
    assert first.collect([pointer])["days"] == 2
    entry = first.current[str(pointer)]
    proof = next(iter(entry["member_dates"].values()))
    if damage == "missing":
        entry["member_dates"].clear()
    else:
        proof[damage] = "invalid"
    pointer = partition_dates([date(2024, 1, 2), date(2024, 1, 6), date(2025, 1, 3)])
    calls = []
    project = bars._project_member_dates
    def observed(member, *args):
        calls.append(str(member))
        return project(member, *args)
    monkeypatch.setattr(bars, "_project_member_dates", observed)
    second = bars._DateCensus({"dates": first.current}, 60)
    assert second.collect([pointer])["days"] == 3
    assert len(calls) == 2


def test_member_date_policy_change_reprojects_unchanged_manifest(partition_dates, monkeypatch):
    pointer = partition_dates([date(2024, 1, 2), date(2024, 1, 6)])
    first = bars._DateCensus({}, 60)
    first.collect([pointer])
    monkeypatch.setattr(bars, "_date_member_policy", lambda: "changed-date-semantics")
    second = bars._DateCensus({"dates": first.current}, 60)
    assert second.collect([pointer])["days"] == 2
    assert second.reads == 1
    assert second.current[str(pointer)]["member_date_policy"] == "changed-date-semantics"


def test_date_chunk_proof_survives_other_columns_rewrite_but_reads_new_clock_bytes(tmp_path, monkeypatch):
    import pyarrow as pa
    import pyarrow.parquet as pq
    path = tmp_path / 'clock.parquet'
    days = [date(2020, 1, 2), date(2020, 1, 4), date(2020, 1, 6)]
    def write(values, price):
        pq.write_table(pa.table({'date': values, 'close': [price]*len(values)}), path,
                       compression='zstd', row_group_size=3)
    write(days, 1.)
    first = bars._DateCensus({}, 60)
    assert first.collect([path])['known_days'] == 3
    assert len(first.chunks) == 1
    before = first.current[str(path)]['signature']
    write(days, 2.)
    assert bars._signature(path) != before
    decoded = []
    convert = pl.from_arrow
    def observed(*args, **kwargs):
        decoded.append(1)
        return convert(*args, **kwargs)
    monkeypatch.setattr(pl, 'from_arrow', observed)
    second = bars._DateCensus({'dates': first.current, 'date_chunks': first.chunks}, 60)
    assert second.collect([path])['known_days'] == 3
    assert decoded == []
    # Bounds and row count stay identical; only the interior observation day
    # changes. Full compressed clock bytes, not min/max, invalidate the proof.
    days[1] = date(2020, 1, 5)
    write(days, 3.)
    third = bars._DateCensus({'dates': second.current, 'date_chunks': second.chunks}, 60)
    assert third.collect([path])['known_days'] == 3
    assert len(decoded) == 1
    assert bars._decode(third.current[str(path)]['bitmap']) == bars._date_bits(days)


@pytest.mark.parametrize('damage', ['bitmap', 'count', 'null_dates', 'sha256', 'missing'])
def test_corrupt_date_chunk_witness_reprojects_full_values(tmp_path, monkeypatch, damage):
    import pyarrow as pa
    import pyarrow.parquet as pq
    path = tmp_path / 'clock.parquet'
    days = [date(2020, 1, 2), date(2020, 1, 6), None]
    value = pa.table({'date': days, 'close': [1., 1., 1.]})
    pq.write_table(value, path)
    first = bars._DateCensus({}, 60)
    assert first.collect([path])['null_date_rows'] == 1
    chunks = first.chunks
    if damage == 'missing':
        chunks.clear()
    else:
        next(iter(chunks.values()))[damage] = 'invalid'
    pq.write_table(value.set_column(1, 'close', pa.array([2., 2., 2.])), path)
    decoded = []
    convert = pl.from_arrow
    def observed(*args, **kwargs):
        decoded.append(1)
        return convert(*args, **kwargs)
    monkeypatch.setattr(pl, 'from_arrow', observed)
    second = bars._DateCensus({'dates': first.current, 'date_chunks': chunks}, 60)
    facts = second.collect([path])
    assert facts['days'] is None and facts['known_days'] == 2 and facts['null_date_rows'] == 1
    assert len(decoded) == 1


def test_date_chunks_share_only_after_each_files_clock_bytes_are_hashed(tmp_path, monkeypatch):
    a, b = tmp_path / 'a.parquet', tmp_path / 'b.parquet'
    days = [date(2020, 1, 2), date(2020, 1, 6)]
    parquet(a, days)
    parquet(b, days)
    census = bars._DateCensus({}, 60)
    assert census.collect([a])['known_days'] == 2
    hashes = []
    hash_chunk = bars._date_chunk_key
    def observed(*args):
        hashes.append(1)
        return hash_chunk(*args)
    monkeypatch.setattr(bars, '_date_chunk_key', observed)
    def forbidden(*args, **kwargs):
        raise AssertionError('identical witnessed clock chunks need no second decode')
    monkeypatch.setattr(pl, 'from_arrow', forbidden)
    assert census.collect([b])['known_days'] == 2
    assert len(hashes) == 1


def test_date_chunk_policy_change_invalidates_unchanged_file_projection(tmp_path, monkeypatch):
    path = tmp_path / 'clock.parquet'
    parquet(path, [date(2020, 1, 2), date(2020, 1, 6)])
    first = bars._DateCensus({}, 60)
    first.collect([path])
    monkeypatch.setattr(bars, '_DATE_CHUNK_POLICY', 'changed-clock-rules')
    second = bars._DateCensus({'dates': first.current, 'date_chunks': first.chunks}, 60)
    assert second.collect([path])['known_days'] == 2
    assert second.reads == 1
    assert second.current[str(path)]['date_chunk_policy'].startswith('changed-clock-rules:')


def test_finmind_history_date_repair_uses_receipt_paths_and_never_span(tmp_path):
    base = tmp_path / "data_finmind/complement"
    paths = []
    for name, days in (("a", [date(2000, 1, 2), date(2000, 1, 6)]), ("b", [date(2000, 1, 6)])):
        parquet(base / (name + ".parquet"), days)
        receipt = base / (name + ".json")
        receipt.write_text(json.dumps({"parquet_path": name + ".parquet"}))
        paths.append(receipt.name)
    row = {"count": 3, "days": None, "known_days": 1, "basis": "收據",
           "_date_receipts": ("complement", paths)}
    bars._finmind_dates(tmp_path, [row], _Sources({}), bars._DateCensus({}, 10))
    assert row["days"] == 2 and "_date_receipts" not in row
    row = {"count": 100, "days": None, "basis": "收據", "_date_receipts": ("complement", paths)}
    bars._finmind_dates(tmp_path, [row], _Sources({}), bars._DateCensus({}, 10))
    assert row["days"] is None and row["row_count_mismatch"]


def test_date_projection_reuses_same_snapshot_footer_and_keeps_gaps(tmp_path, monkeypatch):
    import pyarrow.parquet as pq
    path = tmp_path / "wide.parquet"
    parquet(path, [date(2020, 1, 2), date(2020, 1, 6), date(2020, 1, 6)])
    real = pq.ParquetFile
    metadata_used = []
    def opened(*args, **kwargs):
        metadata_used.append(kwargs.get("metadata"))
        return real(*args, **kwargs)
    monkeypatch.setattr(pq, "ParquetFile", opened)
    result = bars._project_dates(path)
    assert result["count"] == 3 and bars._decode(result["bitmap"]).bit_count() == 2
    assert len(metadata_used) == 2 and metadata_used[0] is None and metadata_used[1] is not None


def test_date_projection_wide_schema_does_not_select_a_nested_clock(tmp_path):
    import pyarrow as pa
    import pyarrow.parquet as pq
    path = tmp_path / "wide.parquet"
    columns = {"nested": pa.array([{"date": "bad"}, {"date": "bad"}, {"date": "bad"}])}
    columns.update({f"value_{index}": pa.array([1, 2, 3]) for index in range(512)})
    columns["date"] = pa.array([date(2020, 1, 2), date(2020, 1, 6), None], type=pa.date32())
    pq.write_table(pa.table(columns), path)
    result = bars._project_dates(path)
    assert result["count"] == 3 and result["null_dates"] == 1
    assert bars._decode(result["bitmap"]) == bars._date_bits([date(2020, 1, 2), date(2020, 1, 6)])


def test_warm_receipt_paths_keep_source_stats_order_and_changed_payload(tmp_path, monkeypatch):
    base = tmp_path / "data_finmind/complement"
    relatives = ["a-b.json", "a/b.json", "./a//c.json", "../outside.json", "/outside.json"]
    for relative, day in zip(relatives[:3], (2, 6, 10)):
        receipt = base / relative
        parquet(receipt.with_suffix(".parquet"), [date(2020, 1, day)])
        receipt.write_text(json.dumps({"parquet_path": str(receipt.with_suffix(".parquet").relative_to(base))}))
    def row():
        return {"count": 3, "known_days": 0, "basis": "收據", "_date_receipts": ("complement", relatives)}
    cold_sources, cold_dates = _Sources({}), bars._DateCensus({}, 10)
    first = row()
    bars._finmind_dates(tmp_path, [first], cold_sources, cold_dates)
    previous = {"version": 1, "sources": cold_sources.current, "dates": cold_dates.current}
    signatures = []
    real_signature = bars._signature
    def counted(path):
        signatures.append(str(path))
        return real_signature(path)
    monkeypatch.setattr(bars, "_signature", counted)
    warm_sources, warm_dates = _Sources(previous), bars._DateCensus(previous, 10)
    second = row()
    bars._finmind_dates(tmp_path, [second], warm_sources, warm_dates)
    assert second == first and second["date_files_pending"] == 2
    expected = sorted({base / Path(relative).with_suffix(".parquet") for relative in relatives[:3]})
    assert signatures == list(map(str, expected))
    assert warm_sources.hits == 3 and warm_dates.reads == 0
    # Same receipt count and position do not make it immutable: a changed
    # receipt must follow its new payload and retain all prior pending states.
    parquet(base / "new.parquet", [date(2020, 2, 3)])
    (base / "a-b.json").write_text(json.dumps({"parquet_path": "new.parquet"}))
    third = row()
    changed_dates = bars._DateCensus(previous, 10)
    bars._finmind_dates(tmp_path, [third], _Sources(previous), changed_dates)
    assert changed_dates.reads == 1 and third["known_latest_date"] == "2020-02-03"
    assert third["date_files_pending"] == 2 and third["days"] is None


def test_finmind_only_nonempty_complete_dates_and_multiday_is_not_invented(tmp_path):
    path = tmp_path / "data_finmind/complement/queue.sqlite3"
    path.parent.mkdir(parents=True)
    with sqlite3.connect(path) as conn:
        conn.execute("CREATE TABLE tasks(dataset,rows,state,first_data_date,last_data_date)")
        conn.executemany("INSERT INTO tasks VALUES (?,?,?,?,?)", [
            ("USStockPriceMinute", 2, "complete", "2026-10-22", "2026-10-22"),
            ("USStockPriceMinute", 3, "complete", "2026-10-22", "2026-10-22"),
            ("USStockPriceMinute", 4, "complete", "2026-10-23", "2026-10-23"),
            ("USStockPriceMinute", 99, "failed", "2020-01-01", "2020-01-01"),
            ("USStockPrice", 100, "complete", "2000-01-01", "2026-10-23"),
            ("TaiwanStockMonthRevenue", 8, "complete", "2026-09-01", "2026-09-01"),
        ])
    rows = {r["id"]: r for r in bars._finmind(tmp_path, _Sources({}))}
    minute = rows["finmind:complement:USStockPriceMinute"]
    assert (minute["count"], minute["days"], minute["partitions"]) == (9, 2, 3)
    daily = rows["finmind:complement:USStockPrice"]
    assert daily["days"] is None and daily["known_days"] == 1
    assert rows["finmind:complement:TaiwanStockMonthRevenue"]["granularity"] == "native"


def test_finlab_wide_daily_and_native_frequency_are_distinct(tmp_path):
    root = tmp_path / "data_finlab"
    (root / "receipts").mkdir(parents=True)
    for name in ("price:收盤價", "monthly_revenue:當月營收"):
        target = root / "datasets" / (name.split(":")[0] + ".parquet")
        parquet(target, [date(2026, 9, 1), date(2026, 10, 1)])
        receipt = {"dataset": name, "status": "downloaded_unverified_for_pit", "rows": 2,
            "rows_with_values": 2, "non_null_values": 6000, "parquet_path": str(target.relative_to(root))}
        (root / "receipts" / (name.split(":")[0] + ".json")).write_text(json.dumps(receipt))
    rows = {r["id"]: r for r in bars._finlab(tmp_path, _Sources({}), bars._DateCensus({}, 10))}
    assert rows["finlab:price:收盤價"]["count"] == 2
    assert rows["finlab:price:收盤價"]["non_null_values"] == 6000
    assert rows["finlab:price:收盤價"]["days"] == 2
    assert rows["finlab:price:收盤價"]["granularity"] == "daily"
    assert rows["finlab:monthly_revenue:當月營收"]["granularity"] == "native"


def test_finlab_numeric_index_never_becomes_date_and_event_clock_is_used(tmp_path):
    base = tmp_path / "data_finlab"
    (base / "receipts").mkdir(parents=True)
    for name, event in (("inventory", True), ("reference", False)):
        parquet(base / (name + ".parquet"), [date(2016, 11, 4), date(2016, 11, 11)])
        receipt = {"dataset": name, "status": "downloaded_unverified_for_pit", "rows": 2, "rows_with_values": 2,
            "source_index_dtype": "int64", "first_non_null_source_index": "0", "last_non_null_source_index": "1",
            "parquet_path": name + ".parquet"}
        if event:
            receipt.update(event_time_column="date", first_event_at="2016-11-04T00:00:00", last_event_at="2016-11-11T00:00:00")
        (base / "receipts" / (name + ".json")).write_text(json.dumps(receipt))
    rows = {r["id"]: r for r in bars._finlab(tmp_path, _Sources({}), bars._DateCensus({}, 10))}
    assert rows["finlab:inventory"]["earliest_date"].startswith("2016-11-04")
    assert rows["finlab:inventory"]["days"] == 2
    assert rows["finlab:reference"]["earliest_date"] is None
    assert rows["finlab:reference"]["latest_date"] is None


def test_existing_discovery_is_reused_and_registered_unavailable_is_not_zero(tmp_path, monkeypatch):
    path = tmp_path / "data_binance/1m/BTCUSDT_features.parquet"
    parquet(path, [date(2026, 10, 22)])
    patterns = []
    def discover(root, pattern):
        patterns.append(pattern)
        return []
    monkeypatch.setattr(bars, "_pattern_sources", discover)
    registry = {"products": [{"id": "fx_reference_rates", "title": "外匯參考價", "provider": "Frankfurter / ECB",
        "granularities": [{"granularity": "1m", "implementation": "not_available"}]}]}
    rows = {r["id"]: r for r in bars._physical(tmp_path, {}, bars._DateCensus({}, 10),
        selected={"group:binance": [path]}, registry=registry)}
    assert "data_binance/1m/*_features.parquet" not in patterns
    assert rows["bars:group:binance"]["count"] == 1
    missing = rows["bars:product:fx_reference_rates:1m"]
    assert missing["statistics_state"] == "not_available" and missing["count"] is None


def test_warm_metadata_work_cannot_consume_the_cold_projection_budget(tmp_path, monkeypatch):
    warm, cold = tmp_path / "warm.parquet", tmp_path / "cold.parquet"
    parquet(warm, [date(2026, 10, 21)])
    parquet(cold, [date(2026, 10, 22)])
    first = bars._DateCensus({}, 10)
    first.collect([warm])
    elapsed = [0.]
    signature = bars._signature
    def slow_metadata(path):
        if path == warm:
            elapsed[0] += 100
        return signature(path)
    monkeypatch.setattr(bars, "_signature", slow_metadata)
    monkeypatch.setattr(bars.time, "monotonic", lambda: elapsed[0])
    census = bars._DateCensus({"dates": first.current}, .01)
    result = census.collect([warm, cold])
    assert result["days"] == 2 and census.reads == 1


def test_changing_hot_tail_cannot_starve_an_untouched_provider(tmp_path, monkeypatch):
    a, b = tmp_path / "a.parquet", tmp_path / "b.parquet"
    parquet(a, [date(2026, 10, 21)])
    parquet(b, [date(2026, 10, 22)])
    selected = {"group:binance": [a], "group:bybit": [b]}
    monkeypatch.setattr(bars, "_pattern_sources", lambda *args: [])
    project = bars._project_dates
    census = bars._DateCensus({}, 10)
    active = [census]
    def consume_one(path, **kwargs):
        result = project(path, **kwargs)
        active[0].remaining_seconds = 0
        return result
    monkeypatch.setattr(bars, "_project_dates", consume_one)
    bars._physical(tmp_path, {}, census, selected=selected)
    assert census.groups["group:bybit"]["pending"] == 1
    parquet(a, [date(2026, 10, 23)])
    second = bars._DateCensus({"dates": census.current, "date_groups": census.groups, "cycle": 1}, 10)
    active[0] = second
    rows = {r["id"]: r for r in bars._physical(tmp_path, {}, second, selected=selected)}
    assert rows["bars:group:bybit"]["days"] == 1
    assert rows["bars:group:binance"]["days"] is None


def test_series_projection_filters_all_grains_without_io_and_keeps_date_names(monkeypatch):
    def forbidden(*args):
        raise AssertionError("HTTP projection must not open sources")
    monkeypatch.setattr(bars, "_database", forbidden)
    rows = [bars._row("daily", "日行情", "FinLab", "taiwan_equity", "daily", count=1, days=1,
        earliest_date="2020-01-01", latest_date="2026-10-22"),
        bars._row("minute", "一分鐘", "OKX", "crypto", "1m", role="derived", count=10)]
    ticks = {"version": 1, "groups": [{**rows[0], "id": "tick", "kind": "trade"}], "revision": "t"}
    inventory = {"version": 1, "groups": rows, "revision": "b"}
    result = bars.series_page(inventory, ticks, grain="daily", provider="FinLab", limit=1)
    assert result["matching_total"] == 1 and result["rows"][0]["latest_date"] == "2026-10-22"
    assert "last" not in result["rows"][0] and result["read_only"]
    assert bars.series_page(inventory, ticks, grain="tick")["rows"][0]["id"] == "tick"
    assert bars.series_page(inventory, ticks, grain="1m", market="crypto")["rows"][0]["role"] == "derived"
    assert bars.series_page(inventory, ticks, search="不存在")["matching_total"] == 0
    before = bars.series_page(inventory, ticks, provider="FinLab")["revision"]
    inventory["revision"] = "unrelated-source-change"
    rows[1]["count"] += 1
    assert bars.series_page(inventory, ticks, provider="FinLab")["revision"] == before
    rows[0]["count"] += 1
    assert bars.series_page(inventory, ticks, provider="FinLab")["revision"] != before
    with pytest.raises(ValueError):
        bars.series_page(inventory, ticks, limit=101)


@pytest.mark.parametrize("values", [[], [date(2020, 1, 5), date(2020, 1, 2), date(2020, 1, 5)],
    [date(2200, 12, 31), date(1800, 1, 1), datetime(2000, 2, 29, 15, 0)]])
def test_date_union_preserves_original_absolute_bits(values):
    expected = 0
    for value in values:
        day = value.date() if isinstance(value, datetime) else value
        expected |= 1 << (day.toordinal() - bars._EPOCH)
    actual = bars._date_bits(values)
    assert actual == expected
    assert bars._encode(actual) == bars._encode(expected)


@pytest.mark.parametrize("value", [date(1799, 12, 31), date(2201, 1, 1), "2020-01-02", None])
def test_date_union_keeps_invalid_values_invalid(value):
    with pytest.raises(ValueError, match="invalid observation date"):
        bars._date_bits([date(2020, 1, 2), value])


def test_repeated_bitmap_memo_survives_unique_dates_without_reusing_source_proof(tmp_path, monkeypatch):
    from datetime import timedelta
    previous = {"dates": {}}
    signature = [1, 2, 3, 4, 5]
    for index in range(600):
        bitmap = bars._encode(bars._date_bits([date(2020, 1, 1) + timedelta(days=index % 300)]))
        previous["dates"][str(tmp_path / str(index))] = {"signature": signature, "bitmap": bitmap, "count": 1, "null_dates": 0}
    calls, signatures = [], []
    decode = bars._decode
    monkeypatch.setattr(bars, "_decode", lambda value: (calls.append(value), decode(value))[1])
    monkeypatch.setattr(bars, "_signature", lambda path: (signatures.append(str(path)), signature)[1])
    census = bars._DateCensus(previous, 2.)
    result = census.collect(list(previous["dates"]))
    assert result["days"] == 300 and result["known_count"] == 600
    assert len(calls) == 300 and len(signatures) == 600

from __future__ import annotations

from types import SimpleNamespace

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from downloader import download_bybit_perp_daily as bybit


START = bybit._date_to_ms("2026-01-01", end_of_day=False)
MINUTE = bybit.CANDLE_INTERVAL_MS


def row(minute: int, close: float = 101.0) -> list[str]:
    return [str(START + minute * MINUTE), "100", "200", "90", str(close), "1", "100"]


def setup_source(tmp_path, monkeypatch, *, first=4, last=40, duplicate_gap=False):
    output = tmp_path / "BTCUSDT_features.parquet"
    output.write_bytes(b"source-not-read-in-mock-test")
    rows = [row(i) for i in range(first, last + 1)]
    frame = bybit._normalize_candles(rows)
    if duplicate_gap:
        frame = pl.concat([frame.filter(pl.col("date") != bybit._ms_to_date_string(START + 20 * MINUTE)), frame.slice(0, 1)])
    reads, writes = [], []

    def read(_path):
        reads.append(_path)
        return frame

    monkeypatch.setattr(bybit, "_load_logical_existing_candle_info", lambda *_a, **_kw: bybit.ExistingCandleInfo(
        frame.height, START + last * MINUTE, True, START + first * MINUTE,
    ))
    monkeypatch.setattr(bybit, "read_logical_parquet", read)
    monkeypatch.setattr(bybit, "_write_parquet", lambda data, _path: writes.append(data))
    monkeypatch.setattr(bybit, "remove_hot_tail", lambda *_: None)
    monkeypatch.setattr(bybit, "_latest_closed_candle_start_ms", lambda: START + 42 * MINUTE)
    record = SimpleNamespace(
        code="BTCUSDT", bybit_symbol="BTCUSDT", market="bybit_linear_perp",
        category="linear", launch_time=None,
    )
    return record, frame, reads, writes


def test_split_head_tail_preserves_middle_and_fresh_boundary_prices(tmp_path, monkeypatch):
    record, old, reads, writes = setup_source(tmp_path, monkeypatch)
    calls = []

    class Client:
        def get(self, _endpoint, params):
            low, high = int(params["start"]), int(params["end"])
            calls.append((low, high))
            assert params["limit"] == "1000"
            values = [row(i, 150 + i) for i in range((low - START) // MINUTE, (high - START) // MINUTE + 1)]
            # Malformed out-of-window rows must not revise the untouched middle.
            return {"result": {"list": [*reversed(values), row(20, 999), row(43, 999)]}}

    result = bybit._download_symbol_1m(Client(), record, tmp_path, START, START + 99 * MINUTE, "incremental", False)
    assert calls == [(START, START + 4 * MINUTE), (START + 39 * MINUTE, START + 42 * MINUTE)]
    assert len(reads) == 1
    assert result.status == "updated"
    assert writes[0].height == 43
    data = dict(zip(writes[0]["date"], writes[0]["close"]))
    assert data[bybit._ms_to_date_string(START + 20 * MINUTE)] == 101
    assert data[bybit._ms_to_date_string(START + 4 * MINUTE)] == 154
    assert data[bybit._ms_to_date_string(START + 39 * MINUTE)] == 189
    assert old.height == 37


def test_empty_head_is_retried_next_run_and_empty_tail_preserves_source(tmp_path, monkeypatch):
    record, old, reads, writes = setup_source(tmp_path, monkeypatch)
    calls = []

    class Client:
        def get(self, _endpoint, params):
            calls.append((int(params["start"]), int(params["end"])))
            return {"result": {"list": []}}

    for _ in range(2):
        result = bybit._download_symbol_1m(Client(), record, tmp_path, START, START + 42 * MINUTE, "incremental", False)
        assert result.status == "skipped_up_to_date"
        assert result.rows == old.height
    assert len(calls) == 4
    assert calls[0] == calls[2] == (START, START + 4 * MINUTE)
    assert writes == []
    assert len(reads) == 2


def test_failed_tail_never_commits_partial_head(tmp_path, monkeypatch):
    record, _old, _reads, writes = setup_source(tmp_path, monkeypatch)

    class Client:
        def get(self, _endpoint, params):
            if int(params["start"]) > START:
                raise TimeoutError("bounded fake provider failure")
            return {"result": {"list": [row(0)]}}

    with pytest.raises(TimeoutError):
        bybit._download_symbol_1m(Client(), record, tmp_path, START, START + 42 * MINUTE, "incremental", False)
    assert writes == []


def test_duplicate_plus_gap_footer_cannot_skip_middle(tmp_path, monkeypatch):
    record, _old, reads, writes = setup_source(tmp_path, monkeypatch, duplicate_gap=True)
    calls = []

    class Client:
        def get(self, _endpoint, params):
            calls.append((int(params["start"]), int(params["end"])))
            return {"result": {"list": [row(i) for i in range(43)]}}

    bybit._download_symbol_1m(Client(), record, tmp_path, START, START + 42 * MINUTE, "incremental", False)
    assert calls == [(START, START + 42 * MINUTE)]
    assert len(reads) == 1
    assert writes[0].height == 43
    assert bybit._frame_matches_1m_interval(writes[0])


def test_real_parquet_footer_duplicate_gap_counterexample_rebuilds(tmp_path, monkeypatch):
    # In-memory Parquet: seven rows over seven minutes, but one duplicate and
    # one absent minute. Footer count/span is not exact continuity evidence.
    frame = bybit._normalize_candles([row(i) for i in (2, 3, 5, 6, 7, 8)])
    frame = pl.concat([frame, frame.filter(pl.col("date") == bybit._ms_to_date_string(START + 3 * MINUTE))])
    sink = pa.BufferOutputStream()
    pq.write_table(frame.to_arrow(), sink, write_statistics=True)
    encoded = sink.getvalue()
    parquet_factory = pq.ParquetFile
    monkeypatch.setattr(bybit.pq, "ParquetFile", lambda *_a, **_kw: parquet_factory(pa.BufferReader(encoded)))
    output = tmp_path / "BTCUSDT_features.parquet"
    output.write_bytes(b"file existence only; footer is supplied from RAM")
    info = bybit._load_existing_candle_info(output, require_contiguous=True)
    assert info.interval_ok  # Demonstrates the prior footer false positive.
    assert not bybit._frame_matches_1m_interval(frame)
    monkeypatch.setattr(bybit, "read_logical_parquet", lambda *_: frame)
    monkeypatch.setattr(bybit, "_latest_closed_candle_start_ms", lambda: START + 10 * MINUTE)
    writes, calls = [], []
    monkeypatch.setattr(bybit, "_write_parquet", lambda data, _path: writes.append(data))
    monkeypatch.setattr(bybit, "remove_hot_tail", lambda *_: None)

    class Client:
        def get(self, _endpoint, params):
            calls.append((int(params["start"]), int(params["end"])))
            return {"result": {"list": [row(i) for i in range(11)]}}

    record = SimpleNamespace(code="BTCUSDT", bybit_symbol="BTCUSDT", market="bybit_linear_perp", category="linear", launch_time=None)
    bybit._download_symbol_1m(Client(), record, tmp_path, START, START + 10 * MINUTE, "incremental", False)
    assert calls == [(START, START + 10 * MINUTE)]
    assert writes[0].height == 11
    assert bybit._frame_matches_1m_interval(writes[0])


def test_partial_head_empty_tail_retains_every_existing_row(tmp_path, monkeypatch):
    record, old, _reads, writes = setup_source(tmp_path, monkeypatch)

    class Client:
        def get(self, _endpoint, params):
            rows = [row(1)] if int(params["start"]) == START else []
            return {"result": {"list": rows}}

    bybit._download_symbol_1m(Client(), record, tmp_path, START, START + 42 * MINUTE, "incremental", False)
    assert writes[0].height == old.height + 1
    assert writes[0].filter(pl.col("date").is_in(old["date"].to_list())).equals(old)


@pytest.mark.parametrize("malformation", ["empty", "missing_date", "null", "invalid", "bounds", "off_grid", "subsecond", "nanosecond"])
def test_split_gate_rejects_unusable_or_mismatched_logical_dates(malformation):
    info = bybit.ExistingCandleInfo(3, START + 4 * MINUTE, True, START + 2 * MINUTE)
    frame = bybit._normalize_candles([row(i) for i in (2, 3, 4)])
    if malformation == "empty":
        frame = frame.head(0)
    elif malformation == "missing_date":
        frame = frame.drop("date")
    elif malformation in {"null", "invalid"}:
        frame = frame.with_columns(pl.Series("date", [frame["date"][0], None if malformation == "null" else "invalid", frame["date"][2]]))
    elif malformation == "bounds":
        info.earliest_ms -= MINUTE
    elif malformation in {"subsecond", "nanosecond"}:
        suffix = ".500" if malformation == "subsecond" else ".000000001"
        frame = frame.with_columns(pl.Series("date", [frame["date"][0], frame["date"][1] + suffix, frame["date"][2]]))
    else:
        frame = frame.with_columns(pl.col("date").str.replace(":00$", ":30"))
    assert not bybit._can_skip_existing_middle(frame, info)


@pytest.mark.parametrize("closed_minute", [2, 38])
def test_split_handles_end_before_existing_head_or_tail(tmp_path, monkeypatch, closed_minute):
    record, old, _reads, writes = setup_source(tmp_path, monkeypatch)
    monkeypatch.setattr(bybit, "_latest_closed_candle_start_ms", lambda: START + closed_minute * MINUTE)
    calls = []

    class Client:
        def get(self, _endpoint, params):
            low, high = int(params["start"]), int(params["end"])
            calls.append((low, high))
            return {"result": {"list": [row(i) for i in range((low - START) // MINUTE, (high - START) // MINUTE + 1)]}}

    bybit._download_symbol_1m(Client(), record, tmp_path, START, START + 99 * MINUTE, "incremental", False)
    assert calls == [(START, START + min(4, closed_minute) * MINUTE)]
    assert writes[0]["date"].max() == old["date"].max()
    assert writes[0].height == old.height + min(4, closed_minute + 1)


def test_preserve_merge_last_fresh_duplicate_wins_and_legacy_cutoff_stays():
    old = bybit._normalize_candles([row(i) for i in range(5)])
    fresh = pl.concat([bybit._normalize_candles([row(4, 150)]), bybit._normalize_candles([row(4, 175)])])
    merged, changed = bybit._merge_existing_with_fresh(old, fresh, START, preserve_existing=True)
    assert changed and merged.height == 5
    assert merged["close"].to_list() == [101, 101, 101, 101, 175]
    legacy, _ = bybit._merge_existing_with_fresh(old, fresh, START)
    assert legacy.height == 1 and legacy["close"].item() == 175


def test_split_matches_full_refetch_every_column_with_fewer_mock_requests(tmp_path, monkeypatch):
    # Fixed full truth tape spanning six API pages; neither route hits a network.
    complete_rows = [row(i, 150 if i == 5 else 175 if i == 4999 else 101) for i in range(5004)]
    expected = bybit._normalize_candles(complete_rows)
    old = bybit._normalize_candles([row(i) for i in range(5, 5001)])
    (tmp_path / "BTCUSDT_features.parquet").write_bytes(b"mock immutable source")
    monkeypatch.setattr(bybit, "_load_logical_existing_candle_info", lambda *_a, **_kw: bybit.ExistingCandleInfo(
        old.height, START + 5000 * MINUTE, True, START + 5 * MINUTE,
    ))
    reads, writes = [], []

    def read(_path):
        reads.append(_path)
        return old

    monkeypatch.setattr(bybit, "read_logical_parquet", read)
    monkeypatch.setattr(bybit, "_write_parquet", lambda data, _path: writes.append(data))
    monkeypatch.setattr(bybit, "remove_hot_tail", lambda *_: None)
    monkeypatch.setattr(bybit, "_latest_closed_candle_start_ms", lambda: START + 5003 * MINUTE)
    record = SimpleNamespace(code="BTCUSDT", bybit_symbol="BTCUSDT", market="bybit_linear_perp", category="linear", launch_time=None)

    class Client:
        def __init__(self):
            self.calls = []

        def get(self, _endpoint, params):
            low, high = int(params["start"]), int(params["end"])
            self.calls.append((low, high))
            selected = [value for value in complete_rows if low <= int(value[0]) <= high]
            assert len(selected) <= 1000
            return {"result": {"list": list(reversed(selected))}}

    full_client, split_client = Client(), Client()
    # A complete rebuild is the old missing-head request plan's output oracle.
    full = bybit._download_symbol_1m(full_client, record, tmp_path, START, START + 5003 * MINUTE, "full", True)
    split = bybit._download_symbol_1m(split_client, record, tmp_path, START, START + 5003 * MINUTE, "incremental", False)
    assert full.status == split.status == "updated"
    assert full.rows == split.rows == 5004
    assert writes[0].equals(expected)
    assert writes[1].equals(expected)
    assert writes[1].equals(writes[0])  # Every date/value/column, not only row count.
    assert len(reads) == 1
    assert len(full_client.calls) == 6
    assert len(split_client.calls) == 2
    assert split_client.calls == [
        (START, START + 5 * MINUTE),
        (START + 4999 * MINUTE, START + 5003 * MINUTE),
    ]


def test_split_folds_real_hot_tail_and_preserves_its_middle_overrides(tmp_path, monkeypatch):
    output = tmp_path / "BTCUSDT_features.parquet"
    bybit._write_parquet(bybit._normalize_candles([row(i) for i in range(4, 31)]), output)
    tail = bybit.hot_tail_path(output)
    bybit._write_parquet(bybit._normalize_candles([row(i, 130) for i in range(29, 41)]), tail)
    monkeypatch.setattr(bybit, "_latest_closed_candle_start_ms", lambda: START + 42 * MINUTE)
    record = SimpleNamespace(code="BTCUSDT", bybit_symbol="BTCUSDT", market="bybit_linear_perp", category="linear", launch_time=None)
    calls = []

    class Client:
        def get(self, _endpoint, params):
            low, high = int(params["start"]), int(params["end"])
            calls.append((low, high))
            values = [row(i, 175) for i in range((low - START) // MINUTE, (high - START) // MINUTE + 1)]
            return {"result": {"list": list(reversed(values))}}

    result = bybit._download_symbol_1m(Client(), record, tmp_path, START, START + 42 * MINUTE, "incremental", False)
    assert calls == [(START, START + 4 * MINUTE), (START + 39 * MINUTE, START + 42 * MINUTE)]
    expected = bybit._normalize_candles([
        row(i, 175 if i <= 4 or i >= 39 else 130 if i >= 29 else 101)
        for i in range(43)
    ])
    assert result.status == "updated" and result.rows == 43
    assert pl.read_parquet(output).equals(expected)
    assert not tail.exists()


def test_split_launch_between_minutes_never_accepts_prelaunch_candle(tmp_path, monkeypatch):
    record, _old, _reads, writes = setup_source(tmp_path, monkeypatch)
    record.launch_time = "2026-01-01 00:01:30"
    calls = []

    class Client:
        def get(self, _endpoint, params):
            low, high = int(params["start"]), int(params["end"])
            calls.append((low, high))
            # Include a provider row before the exact launch bound.
            values = [row(i) for i in ((1, 2, 3, 4) if low < START + 4 * MINUTE else (39, 40, 41, 42))]
            return {"result": {"list": values}}

    bybit._download_symbol_1m(Client(), record, tmp_path, START, START + 42 * MINUTE, "incremental", False)
    assert calls == [(START + 90_000, START + 4 * MINUTE), (START + 39 * MINUTE, START + 42 * MINUTE)]
    assert writes[0]["date"].min() == bybit._ms_to_date_string(START + 2 * MINUTE)
    assert writes[0].height == 41


def test_short_head_row_with_valid_tail_preserves_existing_middle(tmp_path, monkeypatch):
    record, old, _reads, writes = setup_source(tmp_path, monkeypatch)

    class Client:
        def get(self, _endpoint, params):
            values = [[str(START), "100"]] if int(params["start"]) == START else [row(i, 175) for i in range(39, 43)]
            return {"result": {"list": values}}

    bybit._download_symbol_1m(Client(), record, tmp_path, START, START + 42 * MINUTE, "incremental", False)
    assert writes[0].height == old.height + 2
    cutoff = bybit._ms_to_date_string(START + 39 * MINUTE)
    assert writes[0].filter(pl.col("date") < cutoff).equals(old.filter(pl.col("date") < cutoff))


def test_invalid_head_value_never_commits_valid_tail(tmp_path, monkeypatch):
    record, _old, _reads, writes = setup_source(tmp_path, monkeypatch)

    class Client:
        def get(self, _endpoint, params):
            invalid = row(0)
            invalid[4] = "not-a-price"
            values = [invalid] if int(params["start"]) == START else [row(i) for i in range(39, 43)]
            return {"result": {"list": values}}

    with pytest.raises(ValueError):
        bybit._download_symbol_1m(Client(), record, tmp_path, START, START + 42 * MINUTE, "incremental", False)
    assert writes == []

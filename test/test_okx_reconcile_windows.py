from __future__ import annotations

from types import SimpleNamespace

import polars as pl
import pytest

from downloader import download_okx_perp_daily as okx


START = okx._date_to_ms("2026-01-01", end_of_day=False)
MINUTE = okx.CANDLE_INTERVAL_MS
OLD_HEAD = 4
OLD_TAIL = 1000
CLOSED_END = 1003


def _timestamp(minute: int) -> int:
    return START + minute * MINUTE


def _row(minute: int, *, revision: int = 0, confirm: int = 1) -> list[str]:
    price = 100 + minute % 17 + revision
    return [
        str(_timestamp(minute)),
        str(price),
        str(price + 100),
        str(price - 10),
        str(price + 1),
        str(10 + minute + revision),
        str(20 + minute + revision),
        str(1000 + minute + revision),
        str(confirm),
    ]


def _record():
    return SimpleNamespace(
        code="BTCUSDT", okx_symbol="BTC-USDT-SWAP",
        market="okx_perpetual_swap", list_time=None,
    )


class FakeClient:
    """OKX history-candles: exclusive after, newest first, at most 300 rows."""

    def __init__(self, rows):
        self.rows = rows
        self.calls: list[int] = []

    def get(self, endpoint, params):
        assert endpoint == okx.HISTORY_CANDLES_ENDPOINT
        assert params["instId"] == "BTC-USDT-SWAP"
        assert params["bar"] == "1m"
        assert params["limit"] == okx.OKX_HISTORY_LIMIT == "300"
        after = int(params["after"])
        self.calls.append(after)
        assert len(self.calls) <= 20, "fake request bound exceeded"
        selected = sorted(
            (row for row in self.rows if int(row[0]) < after),
            key=lambda row: int(row[0]), reverse=True,
        )[:int(params["limit"])]
        return {"data": selected}


@pytest.fixture(autouse=True)
def _bounded_clock_and_no_network(monkeypatch):
    monkeypatch.setattr(
        okx, "_latest_closed_candle_start_ms", lambda: _timestamp(CLOSED_END)
    )

    def reject_network(*_args, **_kwargs):
        raise AssertionError("these regressions must never use a provider client")

    monkeypatch.setattr(okx.OkxClient, "get", reject_network)


def _source(tmp_path, *, rows=None):
    output = tmp_path / "BTCUSDT_features.parquet"
    frame = okx._normalize_candles(
        rows if rows is not None else [
            _row(i) for i in range(OLD_HEAD, OLD_TAIL + 1)
        ]
    )
    okx._write_parquet(frame, output)
    return output, frame


def _run(client, output_dir, *, refresh=False, record=None):
    return okx._download_symbol_1m(
        client, record or _record(), output_dir, START,
        _timestamp(CLOSED_END + 20), "incremental", refresh,
    )


def _assert_exact_frame(actual, expected):
    assert actual.schema == expected.schema
    assert actual.equals(expected)


def test_split_matches_full_refetch_every_column_with_two_bounded_windows(
    tmp_path, monkeypatch,
):
    truth = [
        _row(i, revision=50 if i == OLD_HEAD else 70 if i >= OLD_TAIL - 1 else 0)
        for i in range(CLOSED_END + 1)
    ]
    expected = okx._normalize_candles(truth)
    full_output, _ = _source(tmp_path / "full")
    split_output, old = _source(tmp_path / "split")
    full_client, split_client = FakeClient(truth), FakeClient(truth)
    full = _run(full_client, full_output.parent, refresh=True)

    reader = okx.read_logical_parquet
    reads = []

    def counted_read(path, *args, **kwargs):
        reads.append(path)
        return reader(path, *args, **kwargs)

    monkeypatch.setattr(okx, "read_logical_parquet", counted_read)
    split = _run(split_client, split_output.parent)

    assert full.status == split.status == "updated"
    assert full.rows == split.rows == CLOSED_END + 1
    assert split_client.calls == [
        _timestamp(OLD_HEAD) + 1, _timestamp(CLOSED_END) + 1,
    ]
    assert len(full_client.calls) == 4
    assert reads == [split_output]
    _assert_exact_frame(pl.read_parquet(full_output), expected)
    _assert_exact_frame(pl.read_parquet(split_output), expected)
    assert old.height == OLD_TAIL - OLD_HEAD + 1
    assert set(expected.columns) == set(okx.OUTPUT_COLUMNS) | {
        "okx_volume_contract", "okx_volume_base", "okx_volume_quote", "okx_confirm",
    }


def test_failed_tail_never_commits_successful_head(tmp_path, monkeypatch):
    output, _ = _source(tmp_path)
    before = output.read_bytes()
    writes, removals = [], []
    monkeypatch.setattr(okx, "_write_parquet", lambda *args: writes.append(args))
    monkeypatch.setattr(okx, "remove_hot_tail", lambda *args: removals.append(args))

    class Client(FakeClient):
        def get(self, endpoint, params):
            if int(params["after"]) == _timestamp(CLOSED_END) + 1:
                raise TimeoutError("bounded fake tail failure")
            return super().get(endpoint, params)

    client = Client([_row(i) for i in range(CLOSED_END + 1)])
    with pytest.raises(TimeoutError, match="fake tail failure"):
        _run(client, tmp_path)
    assert client.calls == [_timestamp(OLD_HEAD) + 1]
    assert writes == removals == []
    assert output.read_bytes() == before


@pytest.mark.parametrize("malformation", ["duplicate_gap", "off_grid", "subsecond"])
def test_untrusted_old_dates_force_full_sweep_with_canonical_merge(tmp_path, malformation):
    output, old = _source(tmp_path)
    middle = okx._ms_to_date_string(_timestamp(500))
    if malformation == "duplicate_gap":
        old = pl.concat([
            old.filter(pl.col("date") != middle),
            old.filter(pl.col("date") == okx._ms_to_date_string(_timestamp(499))),
        ]).sort("date")
    else:
        replacement = (
            okx._ms_to_date_string(_timestamp(500) + 30_000)
            if malformation == "off_grid" else middle + ".000000001"
        )
        old = old.with_columns(
            pl.when(pl.col("date") == middle).then(pl.lit(replacement))
            .otherwise(pl.col("date")).alias("date")
        )
    okx._write_parquet(old, output)
    info = okx._load_logical_existing_candle_info(output)
    assert info.earliest_ms == _timestamp(OLD_HEAD)
    assert info.latest_ms == _timestamp(OLD_TAIL)
    # Footer count/span (and the old median-delta heuristic) is not proof.
    assert info.interval_ok
    truth = [_row(i, revision=9) for i in range(CLOSED_END + 1)]
    client = FakeClient(truth)

    result = _run(client, tmp_path)

    assert result.status == "updated"
    assert client.calls[0] == _timestamp(CLOSED_END) + 1
    assert len(client.calls) == 4
    assert _timestamp(OLD_HEAD) + 1 not in client.calls
    # Invalid continuity forbids skipping requests; it does not authorize
    # replacing old observations or silently repairing off-grid source rows.
    expected, _ = okx._merge_existing_with_fresh(
        old, okx._normalize_candles(truth), START,
    )
    _assert_exact_frame(pl.read_parquet(output), expected)
    if malformation == "off_grid":
        assert replacement in pl.read_parquet(output)["date"].to_list()


def test_untrusted_dates_full_sweep_preserves_provider_absent_old_row_and_features(
    tmp_path,
):
    output, old = _source(tmp_path)
    missing_date = okx._ms_to_date_string(_timestamp(500))
    duplicate_date = okx._ms_to_date_string(_timestamp(499))
    old = pl.concat([
        old.filter(pl.col("date") != missing_date),
        old.filter(pl.col("date") == duplicate_date),
    ]).sort("date").with_columns(pl.lit(17.5).alias("extra_feature"))
    okx._write_parquet(old, output)
    absent_minute = 100
    absent_date = okx._ms_to_date_string(_timestamp(absent_minute))
    truth = [
        _row(i, revision=9) for i in range(CLOSED_END + 1)
        if i != absent_minute
    ]
    client = FakeClient(truth)

    result = _run(client, tmp_path)

    assert result.status == "updated"
    assert client.calls[0] == _timestamp(CLOSED_END) + 1
    assert len(client.calls) == 4
    expected, _ = okx._merge_existing_with_fresh(
        old, okx._normalize_candles(truth), START,
    )
    actual = pl.read_parquet(output)
    _assert_exact_frame(actual, expected)
    _assert_exact_frame(
        actual.filter(pl.col("date") == absent_date),
        old.filter(pl.col("date") == absent_date),
    )
    old_dates = old["date"].unique().to_list()
    assert actual.filter(pl.col("date").is_in(old_dates))["extra_feature"].to_list() == [
        17.5
    ] * len(old_dates)
    assert actual.filter(pl.col("date") == missing_date)["extra_feature"].item() is None


def test_refresh_ignores_split_candidate_and_replaces_middle(tmp_path):
    output, _ = _source(tmp_path)
    truth = [_row(i, revision=15) for i in range(CLOSED_END + 1)]
    client = FakeClient(truth)

    result = _run(client, tmp_path, refresh=True)

    assert result.status == "updated"
    assert client.calls[0] == _timestamp(CLOSED_END) + 1
    assert len(client.calls) == 4
    _assert_exact_frame(pl.read_parquet(output), okx._normalize_candles(truth))


def test_internal_gaps_without_missing_head_fetch_only_adjacent_windows(tmp_path):
    missing = {5, 501}
    output, _ = _source(tmp_path, rows=[
        _row(i) for i in range(OLD_TAIL + 1) if i not in missing
    ])
    revised = {4, 6, 500, 502, OLD_TAIL - 1, OLD_TAIL}
    truth = [
        _row(i, revision=40 if i in revised else 0)
        for i in range(CLOSED_END + 1)
    ]
    client = FakeClient(truth)

    result = _run(client, tmp_path)

    assert client.calls == [
        _timestamp(6) + 1, _timestamp(502) + 1, _timestamp(CLOSED_END) + 1,
    ]
    assert result.status == "updated" and result.rows == CLOSED_END + 1
    _assert_exact_frame(pl.read_parquet(output), okx._normalize_candles(truth))


def test_provider_absent_internal_minutes_remain_missing_and_are_retried(tmp_path):
    missing = {5, 501}
    output, _ = _source(tmp_path, rows=[
        _row(i) for i in range(OLD_TAIL + 1) if i not in missing
    ])
    truth = [
        _row(i) for i in range(CLOSED_END + 1) if i not in missing
    ]
    client = FakeClient(truth)

    first = _run(client, tmp_path)
    second = _run(client, tmp_path)

    expected_calls = [
        _timestamp(6) + 1, _timestamp(502) + 1, _timestamp(CLOSED_END) + 1,
    ]
    assert client.calls == expected_calls * 2
    # These statuses describe a file change/no-op, not proven history coverage.
    assert first.status == "updated" and second.status == "skipped_up_to_date"
    assert first.rows == second.rows == CLOSED_END + 1 - len(missing)
    actual = pl.read_parquet(output)
    _assert_exact_frame(actual, okx._normalize_candles(truth))
    assert not {
        okx._ms_to_date_string(_timestamp(i)) for i in missing
    } & set(actual["date"])


def test_historical_end_before_existing_tail_still_repairs_internal_gaps(tmp_path):
    missing = {5, 501}
    output, old = _source(tmp_path, rows=[
        _row(i) for i in range(OLD_TAIL + 1) if i not in missing
    ])
    truth = [_row(i, revision=40) for i in range(CLOSED_END + 1)]
    client = FakeClient(truth)

    result = okx._download_symbol_1m(
        client, _record(), tmp_path, START, _timestamp(600), "incremental", False,
    )

    assert client.calls == [_timestamp(6) + 1, _timestamp(502) + 1]
    assert result.status == "updated" and result.rows == OLD_TAIL + 1
    repaired = {4, 5, 6, 500, 501, 502}
    expected = okx._normalize_candles([
        _row(i, revision=40 if i in repaired else 0)
        for i in range(OLD_TAIL + 1)
    ])
    actual = pl.read_parquet(output)
    _assert_exact_frame(actual, expected)
    cutoff = okx._ms_to_date_string(_timestamp(600))
    _assert_exact_frame(
        actual.filter(pl.col("date") > cutoff), old.filter(pl.col("date") > cutoff),
    )


def test_empty_head_is_retried_after_successful_tail_update(tmp_path):
    output, _ = _source(tmp_path)

    class Client(FakeClient):
        def get(self, endpoint, params):
            if int(params["after"]) == _timestamp(OLD_HEAD) + 1:
                self.calls.append(int(params["after"]))
                return {"data": []}
            return super().get(endpoint, params)

    client = Client([_row(i) for i in range(CLOSED_END + 1)])
    first = _run(client, tmp_path)
    second = _run(client, tmp_path)

    assert first.status == "updated"
    assert second.status == "skipped_up_to_date"
    assert client.calls == [
        _timestamp(OLD_HEAD) + 1, _timestamp(CLOSED_END) + 1,
        _timestamp(OLD_HEAD) + 1, _timestamp(CLOSED_END) + 1,
    ]
    expected = okx._normalize_candles([
        _row(i) for i in range(OLD_HEAD, CLOSED_END + 1)
    ])
    _assert_exact_frame(pl.read_parquet(output), expected)


def test_partial_head_empty_tail_preserves_every_existing_column(tmp_path):
    output, old = _source(tmp_path)

    class Client(FakeClient):
        def get(self, endpoint, params):
            self.calls.append(int(params["after"]))
            return {"data": [_row(1)] if len(self.calls) == 1 else []}

    client = Client([])
    result = _run(client, tmp_path)
    actual = pl.read_parquet(output)

    assert result.status == "updated" and result.rows == old.height + 1
    assert client.calls == [
        _timestamp(OLD_HEAD) + 1, _timestamp(1), _timestamp(CLOSED_END) + 1,
    ]
    _assert_exact_frame(actual.filter(pl.col("date").is_in(old["date"].to_list())), old)
    assert actual["date"][0] == okx._ms_to_date_string(_timestamp(1))


def test_split_folds_real_hot_tail_preserving_middle_overrides(tmp_path):
    output, _ = _source(tmp_path, rows=[_row(i) for i in range(OLD_HEAD, 801)])
    tail = okx.hot_tail_path(output)
    okx._write_parquet(
        okx._normalize_candles([_row(i, revision=30) for i in range(799, OLD_TAIL + 1)]),
        tail,
    )
    truth = [_row(i, revision=70) for i in range(CLOSED_END + 1)]
    client = FakeClient(truth)

    result = _run(client, tmp_path)

    expected = okx._normalize_candles([
        _row(i, revision=70 if i <= OLD_HEAD or i >= OLD_TAIL - 1 else 30 if i >= 799 else 0)
        for i in range(CLOSED_END + 1)
    ])
    assert result.status == "updated" and result.rows == CLOSED_END + 1
    assert client.calls == [_timestamp(OLD_HEAD) + 1, _timestamp(CLOSED_END) + 1]
    _assert_exact_frame(pl.read_parquet(output), expected)
    assert not tail.exists()


def test_unconfirmed_head_boundary_and_tail_do_not_replace_confirmed_rows(tmp_path):
    output, _ = _source(tmp_path)
    truth = [
        _row(i, revision=70, confirm=0 if i in {0, OLD_HEAD, CLOSED_END} else 1)
        for i in range(CLOSED_END + 1)
    ]
    client = FakeClient(truth)

    result = _run(client, tmp_path)

    expected = okx._normalize_candles([
        _row(i, revision=70 if i < OLD_HEAD or i >= OLD_TAIL - 1 else 0)
        for i in range(1, CLOSED_END)
    ])
    assert result.status == "updated"
    assert client.calls == [_timestamp(OLD_HEAD) + 1, _timestamp(CLOSED_END) + 1]
    _assert_exact_frame(pl.read_parquet(output), expected)
    assert pl.read_parquet(output)["okx_confirm"].unique().to_list() == [1]


@pytest.mark.parametrize("stalled_window", ["head", "tail"])
def test_repeated_cursor_raises_without_committing_any_window(
    tmp_path, monkeypatch, stalled_window,
):
    output, _ = _source(tmp_path)
    before = output.read_bytes()
    writes, removals = [], []
    monkeypatch.setattr(okx, "_write_parquet", lambda *args: writes.append(args))
    monkeypatch.setattr(okx, "remove_hot_tail", lambda *args: removals.append(args))

    class Client(FakeClient):
        def get(self, endpoint, params):
            after = int(params["after"])
            if stalled_window == "head" or after >= _timestamp(OLD_TAIL - 1):
                self.calls.append(after)
                # Deliberately violate exclusive after on the second page.
                first = OLD_HEAD if stalled_window == "head" else CLOSED_END
                return {"data": [_row(first), _row(first - 1)]}
            return super().get(endpoint, params)

    client = Client([_row(i) for i in range(CLOSED_END + 1)])
    with pytest.raises(RuntimeError, match="pagination.*progress"):
        _run(client, tmp_path)
    assert len(client.calls) == (2 if stalled_window == "head" else 3)
    assert writes == removals == []
    assert output.read_bytes() == before

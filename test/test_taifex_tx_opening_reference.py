"""Run-scoped reuse must never mix source generations or publication evidence."""
from dataclasses import FrozenInstanceError
from datetime import date
import os
import sys

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from scripts import download_taifex_option_daily_history as producer
from stockagent.data import tw_index_options_daily as options
from test_tw_index_options_daily import _futures, _write, _OPTION_HEADER, _option_row


@pytest.fixture
def inputs(tmp_path):
    futures = _futures(tmp_path)
    source = tmp_path / "options.csv"
    _write(source, _OPTION_HEADER, [
        _option_row("2025/01/02", series, 20200, right, 100, 110)
        for series in ("202501", "202501W2") for right in ("買權", "賣權")
    ])
    return futures, source


def test_reference_is_immutable_and_loads_all_consumers_only_once(inputs, tmp_path, monkeypatch):
    futures, source = inputs
    original = options.load_taifex_index_futures_day_session
    calls = []
    def counted(*args, **kwargs):
        calls.append(1)
        return original(*args, **kwargs)
    monkeypatch.setattr(options, "load_taifex_index_futures_day_session", counted)
    reference = options.load_taifex_tx_opening_reference(futures)
    with pytest.raises(TypeError):
        reference.open_by_date[date(2025, 1, 2)] = 1.0
    with pytest.raises(TypeError):
        reference.contract_open_by_date[date(2025, 1, 2)] = ("wrong", 1.0)
    with pytest.raises(FrozenInstanceError):
        reference.source_path = source
    for scope in ("monthly", "weekly"):
        for builder in (options.build_taifex_opening_atm_straddles, options.build_taifex_option_full_chain):
            builder([source], futures, tmp_path / f"{scope}-{builder.__name__}.parquet",
                    series_scope=scope, futures_reference=reference)
    assert len(calls) == 1


@pytest.mark.parametrize("mode", ["append", "same_size_restored_mtime", "replace", "delete", "symlink", "different_path", "hardlink_path"])
def test_source_mutations_and_cross_path_are_rejected(inputs, tmp_path, mode):
    futures, source = inputs
    reference = options.load_taifex_tx_opening_reference(futures)
    original = futures.read_bytes()
    before = futures.stat()
    path = futures
    if mode == "append":
        with futures.open("ab") as stream:
            stream.write(b"x")
    elif mode == "same_size_restored_mtime":
        futures.write_bytes(original)
        os.utime(futures, ns=(before.st_atime_ns, before.st_mtime_ns))
    elif mode in ("replace", "symlink"):
        other = tmp_path / "other.parquet"
        other.write_bytes(original)
        if mode == "replace":
            other.replace(futures)
        else:
            futures.unlink()
            futures.symlink_to(other)
    elif mode == "delete":
        futures.unlink()
    else:
        path = tmp_path / "alias.parquet"
        if mode == "different_path":
            path.write_bytes(original)
        else:
            path.hardlink_to(futures)
    output = tmp_path / "old.parquet"
    output.write_bytes(b"old published bytes")
    with pytest.raises((ValueError, OSError)):
        options.build_taifex_option_full_chain([source], path, output,
            series_scope="monthly", futures_reference=reference)
    assert output.read_bytes() == b"old published bytes"


def test_unchanged_symlink_alias_allowed_but_retarget_rejected(inputs, tmp_path):
    futures, _source = inputs
    alias = tmp_path / "current.parquet"
    alias.symlink_to(futures)
    reference = options.load_taifex_tx_opening_reference(alias)
    reference.require_unchanged(alias)
    other = tmp_path / "other.parquet"
    other.write_bytes(futures.read_bytes())
    alias.unlink()
    alias.symlink_to(other)
    with pytest.raises(ValueError, match="source changed"):
        reference.require_unchanged(alias)


def test_mutation_during_canonical_load_never_returns_reference(inputs, monkeypatch):
    futures, _source = inputs
    original = options.load_taifex_index_futures_day_session
    def mutate(*args, **kwargs):
        result = original(*args, **kwargs)
        futures.write_bytes(futures.read_bytes())
        return result
    monkeypatch.setattr(options, "load_taifex_index_futures_day_session", mutate)
    with pytest.raises(ValueError, match="source changed"):
        options.load_taifex_tx_opening_reference(futures)


@pytest.mark.parametrize("builder", ["build_taifex_opening_atm_straddles", "build_taifex_option_full_chain"])
def test_mutation_during_option_parse_does_not_replace_published_output(inputs, tmp_path, monkeypatch, builder):
    futures, source = inputs
    reference = options.load_taifex_tx_opening_reference(futures)
    original = options._read_txo_rows
    def mutate(*args, **kwargs):
        result = original(*args, **kwargs)
        futures.write_bytes(futures.read_bytes())
        return result
    monkeypatch.setattr(options, "_read_txo_rows", mutate)
    output = tmp_path / "published.parquet"
    output.write_bytes(b"old")
    with pytest.raises(ValueError, match="source changed"):
        getattr(options, builder)([source], futures, output,
            series_scope="monthly", futures_reference=reference)
    assert output.read_bytes() == b"old"
    assert not output.with_suffix(".parquet.tmp").exists()


@pytest.mark.parametrize("change", ["open", "contract", "missing"])
def test_atm_projection_cannot_use_another_futures_reference(inputs, tmp_path, change):
    futures, source = inputs
    reference = options.load_taifex_tx_opening_reference(futures)
    wrong = dict(reference.contract_open_by_date)
    day = date(2025, 1, 2)
    if change == "missing":
        wrong.pop(day)
    else:
        contract, price = wrong[day]
        wrong[day] = ("202502", price) if change == "contract" else (contract, price + 100.0)
    projection = options.project_taifex_atm_source(source, series_scope="monthly", tx_by_date=wrong)
    with pytest.raises(ValueError, match="reference mismatch"):
        options.build_taifex_opening_atm_straddles([source], futures, tmp_path / "bad.parquet",
            series_scope="monthly", source_projections={source.resolve(): projection},
            futures_reference=reference)


def test_invalid_selected_far_tenor_still_fails_canonical_validation(inputs):
    futures, _source = inputs
    table = pq.read_table(futures)
    row = table.slice(0, 1).to_pylist()[0]
    row.update(contract_month="202502", is_front_month=False)
    pq.write_table(pa.concat_tables([table, pa.Table.from_pylist([row, row], schema=table.schema)]), futures)
    with pytest.raises(ValueError, match="duplicate tenor"):
        options.load_taifex_tx_opening_reference(futures)


def test_all_cached_shards_reject_stale_reference(inputs, tmp_path):
    futures, source = inputs
    reference = options.load_taifex_tx_opening_reference(futures)
    manifest = [{"path": str(source), "sha256": producer.sha256_path(source)}]
    kwargs = {"scope": "monthly", "builder_fingerprint": producer._builder_fingerprint()}
    cache = tmp_path / "cache"
    first, count = producer._prepare_full_chain_shards(manifest, reference, cache, **kwargs)
    assert count == 1
    again, count = producer._prepare_full_chain_shards(manifest, reference, cache, **kwargs)
    assert again == first and count == 0
    futures.write_bytes(futures.read_bytes())
    with pytest.raises(ValueError, match="source changed"):
        producer._prepare_full_chain_shards(manifest, reference, cache, **kwargs)


def test_source_change_during_merge_does_not_replace_output(inputs, tmp_path, monkeypatch):
    futures, source = inputs
    reference = options.load_taifex_tx_opening_reference(futures)
    shards, _ = producer._prepare_full_chain_shards(
        [{"path": str(source), "sha256": producer.sha256_path(source)}], reference, tmp_path / "cache",
        scope="monthly", builder_fingerprint=producer._builder_fingerprint())
    original = pq.read_table
    def mutate(*args, **kwargs):
        result = original(*args, **kwargs)
        futures.write_bytes(futures.read_bytes())
        return result
    monkeypatch.setattr(pq, "read_table", mutate)
    output = tmp_path / "merged.parquet"
    output.write_bytes(b"old")
    with pytest.raises(ValueError, match="source changed"):
        producer._merge_full_chain_shards(shards, output, scope="monthly", futures_reference=reference)
    assert output.read_bytes() == b"old"
    assert not output.with_suffix(".parquet.tmp").exists()


def test_merge_rejects_prepared_shards_from_another_reference(inputs, tmp_path):
    futures, source = inputs
    reference = options.load_taifex_tx_opening_reference(futures)
    shards, _ = producer._prepare_full_chain_shards(
        [{"path": str(source), "sha256": producer.sha256_path(source)}], reference, tmp_path / "cache",
        scope="monthly", builder_fingerprint=producer._builder_fingerprint())
    table = pq.read_table(futures)
    table = table.set_column(table.schema.get_field_index("open"), "open",
        pa.array([float(value) + 100.0 for value in table["open"].to_pylist()]))
    other = tmp_path / "other.parquet"
    pq.write_table(table, other)
    different_reference = options.load_taifex_tx_opening_reference(other)
    output = tmp_path / "merged.parquet"
    output.write_bytes(b"old")
    with pytest.raises(ValueError, match="futures reference mismatch"):
        producer._merge_full_chain_shards(shards, output, scope="monthly", futures_reference=different_reference)
    assert output.read_bytes() == b"old"
    assert not output.with_suffix(".parquet.tmp").exists()


def configure_local_main(inputs, tmp_path, monkeypatch):
    futures, source = inputs
    monkeypatch.setattr(producer, "_download", lambda *args, **kwargs: source)
    monkeypatch.setattr(producer, "ATM_SOURCE_CACHE", tmp_path / "atm-cache")
    monkeypatch.setattr(producer, "FULL_CHAIN_SHARD_CACHE", tmp_path / "chain-cache")
    output = tmp_path / "output"
    monkeypatch.setattr(sys, "argv", ["options", "--output-dir", str(output),
        "--futures-path", str(futures), "--start-year", "2025", "--end-date", "2025-01-03",
        "--series-scope", "all"])
    return output


def test_main_cold_build_loads_once_and_verified_reuse_loads_zero(inputs, tmp_path, monkeypatch):
    configure_local_main(inputs, tmp_path, monkeypatch)
    original = options.load_taifex_index_futures_day_session
    calls = []
    def counted(*args, **kwargs):
        calls.append(1)
        return original(*args, **kwargs)
    monkeypatch.setattr(options, "load_taifex_index_futures_day_session", counted)
    assert producer.main() == 0
    assert len(calls) == 1
    calls.clear()
    assert producer.main() == 0
    assert calls == []


def test_main_rejects_hash_to_reference_generation_change(inputs, tmp_path, monkeypatch):
    output = configure_local_main(inputs, tmp_path, monkeypatch)
    futures, _source = inputs
    original = producer.load_taifex_tx_opening_reference
    def mutate(*args, **kwargs):
        futures.write_bytes(futures.read_bytes())
        return original(*args, **kwargs)
    monkeypatch.setattr(producer, "load_taifex_tx_opening_reference", mutate)
    with pytest.raises(ValueError, match="source changed during normalization"):
        producer.main()
    assert not output.exists()
    assert not producer.ATM_SOURCE_CACHE.exists()
    assert not producer.FULL_CHAIN_SHARD_CACHE.exists()


def test_missing_tx_empty_shard_rebuilds_only_affected_date(inputs, tmp_path):
    futures, _source = inputs
    full = pq.read_table(futures)
    pq.write_table(full.slice(1), futures)  # Jan 2 TX is unavailable, Jan 3 stays valid.
    sources = []
    for suffix, day in (("2", "2025/01/02"), ("3", "2025/01/03")):
        path = tmp_path / f"monthly-{suffix}.csv"
        _write(path, _OPTION_HEADER, [_option_row(day, "202501", 20200, "買權", 100, 110)])
        sources.append(path)
    manifest = [{"path": str(path), "sha256": producer.sha256_path(path)} for path in sources]
    reference = options.load_taifex_tx_opening_reference(futures)
    kwargs = {"builder_fingerprint": producer._builder_fingerprint()}
    cache = tmp_path / "cache"
    monthly, count = producer._prepare_full_chain_shards(manifest, reference, cache, scope="monthly", **kwargs)
    assert count == 2
    assert monthly[0]["source_dates"] == ["2025-01-02"]
    assert pq.read_table(monthly[0]["shard_path"]).num_rows == 0
    weekly, count = producer._prepare_full_chain_shards(manifest, reference, cache, scope="weekly", **kwargs)
    assert count == 2 and all(row["source_dates"] == [] for row in weekly)
    # A repaired TX day invalidates its monthly shard, not unrelated dates or
    # genuinely absent weekly listings. No source-identity-wide cache flush.
    pq.write_table(full, futures)
    repaired = options.load_taifex_tx_opening_reference(futures)
    monthly, count = producer._prepare_full_chain_shards(manifest, repaired, cache, scope="monthly", **kwargs)
    assert count == 1 and pq.read_table(monthly[0]["shard_path"]).num_rows == 1
    again, count = producer._prepare_full_chain_shards(manifest, repaired, cache, scope="weekly", **kwargs)
    assert count == 0 and again == weekly


def test_missing_tx_does_not_hide_overlapping_empty_shard_dates(inputs, tmp_path):
    futures, source = inputs
    pq.write_table(pq.read_table(futures).slice(1), futures)
    reference = options.load_taifex_tx_opening_reference(futures)
    other = tmp_path / "overlap.csv"
    other.write_bytes(source.read_bytes())
    shards, count = producer._prepare_full_chain_shards(
        [{"path": str(path), "sha256": producer.sha256_path(path)} for path in (source, other)],
        reference, tmp_path / "cache", scope="monthly", builder_fingerprint=producer._builder_fingerprint())
    assert count == 2 and all(pq.read_table(row["shard_path"]).num_rows == 0 for row in shards)
    with pytest.raises(ValueError, match="overlapping full-chain receipts"):
        producer._merge_full_chain_shards(shards, tmp_path / "merged.parquet", scope="monthly", futures_reference=reference)

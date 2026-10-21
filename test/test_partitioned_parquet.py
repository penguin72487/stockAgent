"""Explicit immutable partitions preserve the entire canonical typed table."""
from datetime import date
import json
import os

import numpy as np
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from stockagent.data import partitioned_parquet as parts
from stockagent.data.tw_public_incremental import table_content_sha256


@pytest.fixture(autouse=True)
def isolated_capacity_controls(tmp_path, monkeypatch):
    # systemd's private /tmp may be on a different device from /var/lib.
    # Exercise the real namespace/filesystem lease on the fixture's device.
    from stockagent.data_sync import cache_capacity
    controls = tmp_path / "capacity-controls"
    controls.mkdir()
    monkeypatch.setattr(cache_capacity, "CONTROL_ROOT", controls)
    monkeypatch.setattr(cache_capacity, "_admit_backing_volume", lambda *_: {})


def table(days=None, values=None):
    days = days or [date(2024, 1, 1), date(2024, 12, 31), date(2025, 1, 1)]
    return pa.table({"date": pa.array(days, type=pa.date32()),
        "symbol": ["2330"] * len(days), "value": pa.array(
            values if values is not None else [-0.0, None, float("nan")], type=pa.float64())})


def write(path, value, **kwargs):
    return parts.write_partitioned_table(value, path, source_identity={"input": table_content_sha256(value)}, **kwargs)


def same(left, right):
    assert left.schema.equals(right.schema, check_metadata=True)
    assert table_content_sha256(left) == table_content_sha256(right)


def test_legacy_reader_keeps_values_schema_and_identity(tmp_path):
    path = tmp_path / "2330_features.parquet"
    value = table().replace_schema_metadata({b"source": b"official"})
    pq.write_table(value, path)
    same(parts.read_table(path), value)
    assert parts.source_paths([path]) == [path]
    assert parts.discover_sources(tmp_path) == [path]


def _live_source_keys(root, *, external=None):
    from types import SimpleNamespace
    from stockagent.config import DataConfig
    from stockagent.live.completed_inference_cache import source_fingerprint
    from stockagent.live.signal_engine import _live_panel_cache_key

    config = SimpleNamespace(data=DataConfig(parquet_root=str(root), benchmark_name="index",
        use_tw_public_features=external is not None, tw_public_feature_path=str(external or "")))
    kwargs = {"external_feature_path": str(external)} if external else {}
    return (source_fingerprint(config, rule_data_dir=root.parent),
            _live_panel_cache_key(config, live_tail_rows=48, panel_kwargs=kwargs))


@pytest.mark.parametrize("external", [False, True])
def test_live_source_keys_track_only_current_partition_members_and_logical_summary(tmp_path, external):
    root = tmp_path / "stocks"
    root.mkdir()
    path = tmp_path / "public.parquet" if external else root / "2330_features.parquet"
    write(path, table())
    # Explicit pointer and logical legacy spelling select the same summary.
    argument = parts.manifest_path(path) if external else None
    before = _live_source_keys(root, external=argument)
    archive = parts.manifest_path(path).with_name(parts.manifest_path(path).name + ".parts") / "unreferenced.parquet"
    archive.write_bytes(b"not an input or a valid Parquet file")
    (root / "status.json").write_text("heartbeat")
    assert _live_source_keys(root, external=argument) == before
    write(path, table([date(2025, 1, 2)], [4.]), replace_from=date(2025, 1, 2))
    after = _live_source_keys(root, external=argument)
    assert all(left != right for left, right in zip(before, after, strict=True))
    if external:
        path.with_suffix(".summary.json").write_text('{"updated": true}')
        latest = _live_source_keys(root, external=argument)
        assert all(left != right for left, right in zip(after, latest, strict=True))


@pytest.mark.parametrize("failure", ["corrupt_old_member", "missing_member", "ambiguous"])
def test_live_source_keys_fail_closed_for_partition_corruption_outside_tail(tmp_path, failure):
    from types import SimpleNamespace
    from stockagent.config import DataConfig
    from stockagent.live.completed_inference_cache import source_fingerprint
    from stockagent.live.signal_engine import _live_panel_cache_key

    root = tmp_path / "stocks"
    root.mkdir()
    path = root / "2330_features.parquet"
    write(path, table())
    _live_source_keys(root)
    with parts.ParquetSource(path) as source:
        old = source.members[0]
    if failure == "ambiguous":
        path.write_bytes(b"stale legacy")
    elif failure == "missing_member":
        old.unlink()
    else:
        before = old.stat()
        raw = old.read_bytes()
        old.chmod(0o644)
        old.write_bytes(raw[:-1] + bytes([raw[-1] ^ 1]))
        os.utime(old, ns=(before.st_atime_ns, before.st_mtime_ns))
        old.chmod(0o444)
    config = SimpleNamespace(data=DataConfig(parquet_root=str(root), benchmark_name="index"))
    for read in (lambda: source_fingerprint(config, rule_data_dir=root.parent),
                 lambda: _live_panel_cache_key(config, live_tail_rows=1, panel_kwargs={})):
        with pytest.raises((ValueError, FileNotFoundError)):
            read()


def test_live_panel_key_detects_legacy_in_place_repair_without_directory_change(tmp_path):
    root = tmp_path / "stocks"
    root.mkdir()
    path = root / "2330_features.parquet"
    path.write_bytes(b"before")
    before = _live_source_keys(root)
    directory_mtime, original = root.stat().st_mtime_ns, path.stat()
    path.write_bytes(b"repair")
    os.utime(path, ns=(original.st_atime_ns, original.st_mtime_ns))
    assert root.stat().st_mtime_ns == directory_mtime
    assert all(left != right for left, right in zip(before, _live_source_keys(root), strict=True))


def test_generation_only_legacy_discovery_avoids_per_member_path_resolution(tmp_path, monkeypatch):
    from pathlib import Path
    from stockagent.live.completed_inference_cache import _parquet_source_generations

    paths = [tmp_path / f"{symbol}_features.parquet" for symbol in ("2330", "2317")]
    for path in paths:
        path.write_bytes(b"generation proof does not decode legacy data")
    generations = {str(path): (s.st_dev, s.st_ino, s.st_size, s.st_mtime_ns, s.st_ctime_ns)
                   for path in paths for s in [path.stat()]}
    def no_resolve(*_args, **_kwargs):
        pytest.fail("legacy directory discovery must not resolve every path")
    monkeypatch.setattr(Path, "resolve", no_resolve)
    assert dict(_parquet_source_generations(feature_roots=(tmp_path,))) == generations


def test_partition_roundtrip_projection_filter_tail_metadata(tmp_path):
    path = tmp_path / "2330_features.parquet"
    value = table().replace_schema_metadata({b"source": b"official", b"raw": b"\x00\xff"})
    result = write(path, value)
    assert result["written_partitions"] == 2
    assert not path.exists()
    assert parts.source_exists(path)
    same(parts.read_table(path), value)
    same(parts.read_table(path, columns=["date", "value"], filters=[("date", ">=", date(2024, 12, 31))]),
         value.select(["date", "value"]).slice(1))
    same(parts.read_table(path, tail_rows=1), value.slice(2))
    assert parts.discover_sources(tmp_path) == [parts.manifest_path(path)]
    assert parts.logical_name(parts.manifest_path(path)) == path.name
    with parts.ParquetSource(path) as source:
        assert source.scan().collect().height == 3
        assert len(source.source_paths()) == 3
    assert np.signbit(parts.read_table(path)["value"][0].as_py())


def test_append_rewrites_only_affected_year_and_retains_original_generation(tmp_path):
    path = tmp_path / "2330_features.parquet"
    original = table()
    write(path, original)
    before = json.loads(parts.manifest_path(path).read_bytes())
    extra = table([date(2025, 1, 2)], [1.234567890123456])
    result = write(path, extra, replace_from=date(2025, 1, 2))
    same(parts.read_table(path), pa.concat_tables([original, extra]))
    after = json.loads(parts.manifest_path(path).read_bytes())
    assert before["members"][0] == after["members"][0]
    assert before["members"][1] != after["members"][1]
    assert result["written_partitions"] == 1
    assert result["reused_partitions"] == 1
    for proof in before["members"]:
        assert (path.parent / proof["path"]).exists()
    assert len(list((tmp_path / (parts.manifest_path(path).name + ".parts")).glob("generation_*.json"))) == 2


def test_correction_and_deletion_rebuild_only_changed_members(tmp_path):
    path = tmp_path / "x.parquet"
    original = table()
    write(path, original)
    changed = original.set_column(2, "value", pa.array([-0.0, 5., float("nan")]))
    result = write(path, changed)
    assert result["written_partitions"] == 1 and result["reused_partitions"] == 1
    same(parts.read_table(path), changed)
    result = write(path, changed.slice(0, 2))
    assert result["written_partitions"] == 0
    same(parts.read_table(path), changed.slice(0, 2))


@pytest.mark.parametrize("fault", ["missing", "same_size_mtime", "writable", "symlink", "schema", "checksum", "row_count", "reorder"])
def test_corrupt_or_partial_generation_is_never_silently_read(tmp_path, fault):
    path = tmp_path / "x.parquet"
    write(path, table())
    manifest_path = parts.manifest_path(path)
    manifest = json.loads(manifest_path.read_bytes())
    member = path.parent / manifest["members"][0]["path"]
    if fault == "missing":
        member.unlink()
    elif fault == "same_size_mtime":
        before = member.stat()
        member.chmod(0o644)
        with member.open("r+b") as stream:
            stream.seek(20)
            stream.write(b"X")
        member.chmod(0o444)
        os.utime(member, ns=(before.st_atime_ns, before.st_mtime_ns))
    elif fault == "writable":
        member.chmod(0o644)
    elif fault == "symlink":
        target = tmp_path / "outside.parquet"
        member.rename(target)
        member.symlink_to(target)
    else:
        if fault == "schema":
            manifest["schema"] = "bad"
        elif fault == "row_count":
            manifest["rows"] += 1
        elif fault == "reorder":
            manifest["members"].reverse()
        else:
            manifest["source_identity"] = {"wrong": True}
        if fault != "checksum":
            manifest["manifest_sha256"] = parts._digest({k: v for k, v in manifest.items() if k != "manifest_sha256"})
        manifest_path.write_text(json.dumps(manifest))
    with pytest.raises((ValueError, OSError, pa.ArrowException)):
        parts.read_table(path, filters=[("date", ">=", date(2025, 1, 1))])


def test_ambiguous_legacy_copy_cannot_hide_new_manifest(tmp_path):
    path = tmp_path / "x.parquet"
    write(path, table())
    pq.write_table(table(), path)
    with pytest.raises(ValueError, match="ambiguous"):
        parts.read_table(path)
    with pytest.raises(ValueError, match="ambiguous"):
        parts.resolve_source(parts.manifest_path(path))


def test_snapshot_fences_pointer_and_member_changes(tmp_path):
    path = tmp_path / "x.parquet"
    write(path, table())
    snapshot = parts.ParquetSource(path)
    write(path, table([date(2025, 2, 1)], [5.]), replace_from=date(2025, 2, 1))
    with pytest.raises(ValueError, match="pointer changed"):
        snapshot.table()


@pytest.mark.parametrize("fault", ["unordered", "null_date", "text_clock", "bad_boundary", "missing_base", "schema"])
def test_writer_rejects_ambiguous_input_without_overwriting_last_good(tmp_path, fault):
    path = tmp_path / "x.parquet"
    write(path, table())
    before = parts.manifest_path(path).read_bytes()
    value, kwargs = table(), {}
    if fault == "unordered":
        value = value.take(pa.array([2, 0, 1]))
    elif fault == "null_date":
        value = value.set_column(0, "date", pa.array([None] * 3, type=pa.date32()))
    elif fault == "text_clock":
        value = value.set_column(0, "date", value["date"].cast(pa.string()))
    elif fault == "bad_boundary":
        kwargs["replace_from"] = date(2025, 1, 2)
    elif fault == "missing_base":
        path = tmp_path / "missing.parquet"
        kwargs["replace_from"] = date(2024, 1, 1)
    else:
        value = value.append_column("other", pa.array([1, 2, 3]))
    with pytest.raises(ValueError):
        write(path, value, **kwargs)
    assert parts.manifest_path(tmp_path / "x.parquet").read_bytes() == before


def test_failed_publication_retains_last_good_and_new_payload_evidence(tmp_path, monkeypatch):
    path = tmp_path / "x.parquet"
    value = table()
    write(path, value)
    original = os.replace
    def fail_pointer(source, target):
        if target == parts.manifest_path(path):
            raise OSError("injected publication failure")
        return original(source, target)
    monkeypatch.setattr(os, "replace", fail_pointer)
    with pytest.raises(OSError, match="injected"):
        write(path, table([date(2025, 1, 2)], [7.]), replace_from=date(2025, 1, 2))
    same(parts.read_table(path), value)


def test_panel_and_hot_tail_readers_share_same_partition_values(tmp_path):
    from downloader.ohlcv_hot_tail import read_logical_parquet
    from stockagent.data.panel import _compute_source_hash, _load_external_feature_arrays, _symbol_name_from_path
    path = tmp_path / "2330_features.parquet"
    original = table().rename_columns(["date", "symbol", "twpub_value"])
    write(path, original)
    assert _symbol_name_from_path(parts.manifest_path(path)) == "2330"
    assert read_logical_parquet(path).height == 3
    first = _compute_source_hash([parts.manifest_path(path)])
    values = _load_external_feature_arrays(parts.manifest_path(path), date_start=date(2024, 12, 31))
    assert values.feature_names == ["twpub_value"]
    assert _compute_source_hash([parts.manifest_path(path)]) == first
    write(path, original.slice(0, 2))
    assert _compute_source_hash([parts.manifest_path(path)]) != first


@pytest.mark.parametrize("layout", ["file", "year"])
@pytest.mark.parametrize("case", ["append", "year_boundary", "episode", "revision", "delete", "last_missing", "new_missing", "reference", "zero"])
def test_base_yearly_producer_matches_full_canonical_output(tmp_path, case, layout):
    from scripts.build_tw_official_symbol_parquets import _write_symbol
    from test_tw_official_symbol_builder import _incremental_frame
    frame = _incremental_frame()
    if case == "year_boundary":
        frame = frame.with_columns(pl.Series("date", [date(2024, 12, 29), date(2024, 12, 30),
            date(2024, 12, 31), date(2025, 1, 1), date(2025, 1, 2)]))
    if case == "episode":
        frame = frame.with_columns(pl.when(pl.col("date") >= date(2026, 10, 15))
            .then(1).otherwise(0).alias("_lifecycle_episode_id"))
    if case in {"last_missing", "new_missing"}:
        frame = frame.with_columns(pl.when(pl.col("date") == frame["date"][2 if case == "last_missing" else 4])
            .then(float("nan")).otherwise(pl.col("close")).alias("close"))
    if case == "reference":
        frame = frame.with_columns(pl.when(pl.col("date") == frame["date"][4])
            .then(99.).otherwise(pl.col("reference_override")).alias("reference_override"))
    if case == "zero":
        frame = frame.with_columns(pl.when(pl.col("date") == frame["date"][3])
            .then(0.).otherwise(pl.col("open")).alias("open"))
    output, reference = tmp_path / "parts", tmp_path / "full"
    output.mkdir()
    reference.mkdir()
    states = {}
    seed = _write_symbol(output, "2330", frame.head(3), requested_end_date=str(frame["date"][2]),
        dry_run=False, state_sink=states, output_layout=layout)
    assert seed.status == "created", seed.message
    prior = json.loads(json.dumps(states["2330"]))
    if case == "revision":
        frame = frame.with_columns(pl.when(pl.col("date") == frame["date"][1])
            .then(2.).otherwise(pl.col("signed_change")).alias("signed_change"))
    elif case == "delete":
        frame = frame.filter(pl.col("date") != frame["date"][1])
    result = _write_symbol(output, "2330", frame, requested_end_date=str(frame["date"][-1]),
        dry_run=False, previous_state=prior, state_sink={}, output_layout=layout)
    full = _write_symbol(reference, "2330", frame, requested_end_date=str(frame["date"][-1]), dry_run=False)
    assert result.status == full.status == "created", (result.message, full.message)
    actual = parts.read_table(output / "2330_features.parquet")
    expected = pq.read_table(reference / "2330_features.parquet")
    assert table_content_sha256(actual) == table_content_sha256(expected)
    assert result.return_quarantined_rows == full.return_quarantined_rows
    for name in ("rows", "adjusted_rows", "missing_adjustment_rows", "first_date", "last_date", "source",
                 "fallback_rows", "fallback_adjustment_rows", "normalized_zero_ohlc_rows",
                 "previous_session_next_reference_adjustment_rows", "lifecycle_episode_quarantined_rows",
                 "listing_boundary_quarantined_rows", "unverified_extreme_quarantined_rows"):
        assert getattr(result, name) == getattr(full, name), name


def test_full_panel_all_arrays_and_persistent_rules_match_new_reader(tmp_path):
    from scripts.audit_tw_public_data_layer import _panel_kwargs
    from test_tw_public_audit_panel_projection import _fixture, _assert_panel_bits
    from stockagent.data.panel import build_panel
    stocks, public, config = _fixture(tmp_path)
    external = pl.read_parquet(public).with_columns(pl.lit(None, dtype=pl.Float64).alias("_twpub_delisted"))
    external = pl.concat([external, pl.DataFrame({"date": [date(2023, 12, 29)], "symbol": ["2330"],
        "_twpub_delisted": [1.]})], how="diagonal_relaxed").sort("date", "symbol")
    external.write_parquet(public)
    config.data.feature_include = ["open_raw", "close_raw", "next_session_open_gap_logret", "twpub_keep"]
    config.data.feature_exclude = []
    legacy = build_panel(stocks, **_panel_kwargs(config, public))
    target = tmp_path / "partitioned"
    target.mkdir()
    for path in stocks.glob("*_features.parquet"):
        write(target / path.name, pq.read_table(path))
    new_public = tmp_path / "partition_public.parquet"
    original_public = pq.read_table(public)
    import pyarrow.compute as pc
    order = pc.sort_indices(original_public, sort_keys=[("date", "ascending"), ("symbol", "ascending")])
    write(new_public, original_public.take(order), period="month")
    actual = build_panel(target, **_panel_kwargs(config, new_public))
    _assert_panel_bits(legacy, actual)
    from stockagent.data.panel import _load_external_feature_arrays
    payload = _load_external_feature_arrays(new_public, date_start=date(2024, 1, 1))
    days, values = payload.by_symbol_rules["2330"]
    row = list(days.astype("datetime64[D]")).index(np.datetime64("2023-12-29"))
    assert values[row, payload.rule_names.index("_twpub_delisted")] == 1.


def test_audit_and_stage_receipts_bind_every_partition(tmp_path):
    from scripts import audit_tw_public_data_layer as audit
    from scripts import rebuild_tw_public_data_layer as rebuild
    path = tmp_path / "stocks/2330_features.parquet"
    path.parent.mkdir()
    write(path, table())
    before = rebuild._file_receipt(path)
    assert len(before["members"]) == 2
    assert rebuild._outputs_exist([path])
    signatures = audit._audit_input_signatures(path.parent, tmp_path, tmp_path / "public.parquet")
    assert all(item["path"] in signatures for item in before["members"])
    write(path, table([date(2025, 2, 1)], [42.]), replace_from=date(2025, 2, 1))
    assert not rebuild._receipts_match({"output_receipts": [before]}, [path])


def test_unchanged_partition_stats_are_current_and_csv_report_is_scalar(tmp_path):
    from scripts import build_tw_official_symbol_parquets as builder
    from test_tw_official_symbol_builder import _incremental_frame
    frame, states = _incremental_frame(), {}
    first = builder._write_symbol(tmp_path, "2330", frame,
        requested_end_date="2026-10-16", dry_run=False, state_sink=states, output_layout="year")
    assert first.status == "created", first.message
    original_state = json.dumps(states, sort_keys=True)
    hit = builder._write_symbol(tmp_path, "2330", frame,
        requested_end_date="2026-10-16", dry_run=False, previous_state=states["2330"],
        state_sink={}, output_layout="year")
    assert hit.reused_output
    assert hit.partitioned_write["written_partitions"] == hit.partitioned_write["written_rows"] == 0
    assert hit.partitioned_write["reused_partitions"] == 1
    assert hit.partitioned_write["reused_rows"] == frame.height
    assert json.dumps(states, sort_keys=True) == original_state
    path = tmp_path / "report.csv"
    pl.DataFrame(builder._report_rows([hit])).write_csv(path)
    recorded = json.loads(pl.read_csv(path)["partitioned_write"][0])
    assert recorded == hit.partitioned_write


def test_stale_partitioned_universe_fails_before_any_other_output_is_written(tmp_path):
    from scripts import build_tw_official_symbol_parquets as builder
    from test_tw_official_symbol_builder import _incremental_frame
    path = tmp_path / "old_features.parquet"
    write(path, table())
    before = parts.manifest_path(path).read_bytes()
    with pytest.raises(ValueError, match="explicit retirement"):
        builder._write_symbol_batch(tmp_path, {"2330": _incremental_frame()},
            requested_end_date="2026-10-16", output_layout="year")
    assert not parts.manifest_path(tmp_path / "2330_features.parquet").exists()
    assert parts.manifest_path(path).read_bytes() == before
    legacy = tmp_path / "legacy_features.parquet"
    pq.write_table(table(), legacy)
    with pytest.raises(ValueError, match="explicit retirement"):
        builder._remove_stale_official_symbol_files(tmp_path, {"2330"}, dry_run=False)
    assert legacy.exists()


def test_public_table_audit_and_build_receipt_accept_explicit_manifest(tmp_path, monkeypatch):
    from dataclasses import asdict
    from scripts import audit_tw_public_data_layer as audit
    from stockagent.data.tw_public_features import FEATURE_COLUMNS, RULE_COLUMNS
    original = tmp_path / "legacy.parquet"
    value = table().rename_columns(["date", "symbol", FEATURE_COLUMNS[0]])
    for name in RULE_COLUMNS:
        value = value.append_column(name, pa.array([None] * value.num_rows, type=pa.float64()))
    pq.write_table(value, original)
    logical = tmp_path / "new.parquet"
    write(logical, value, period="month")
    expected = audit.audit_public_feature_table(original, [FEATURE_COLUMNS[0]], "__MARKET__")
    actual = audit.audit_public_feature_table(logical, [FEATURE_COLUMNS[0]], "__MARKET__")
    assert actual[:2] == expected[:2]
    # Paths are presentation only; all semantic findings must agree.
    assert [{k: v for k, v in asdict(item).items() if k != "item"} for item in actual[2]] == [
        {k: v for k, v in asdict(item).items() if k != "item"} for item in expected[2]]
    monkeypatch.setattr(audit, "_source_content_receipts", lambda *_: [])
    monkeypatch.setattr(audit, "_symbol_universe_receipt", lambda *_: {})
    manifest = parts.manifest_path(logical)
    summary = {"feature_columns": list(FEATURE_COLUMNS), "rule_columns": list(RULE_COLUMNS),
        "availability_contract_version": audit.TW_PUBLIC_FEATURE_AVAILABILITY_CONTRACT_VERSION,
        "source_receipts": [], "symbol_universe_receipt": {}, "output_receipt": audit._file_content_receipt(manifest)}
    logical.with_suffix(".summary.json").write_text(json.dumps(summary))
    for path in (logical, manifest):
        result, findings = audit.audit_feature_build_receipt(path, tmp_path, tmp_path)
        assert result["valid"] and not findings
    member = logical.parent / json.loads(manifest.read_bytes())["members"][0]["path"]
    member.chmod(0o644)
    with pytest.raises(ValueError, match="content proof"):
        audit.audit_feature_build_receipt(manifest, tmp_path, tmp_path)


def test_stock_history_resolves_manifest_and_cached_prices_still_verify_members(tmp_path):
    from stockagent.live import stock_history as history
    legacy, target = tmp_path / "legacy", tmp_path / "partitioned"
    legacy.mkdir()
    target.mkdir()
    value = table(values=[-0., 2., 3.]).rename_columns(["date", "symbol", "close"])
    value = value.append_column("open", pa.array([1., 2., 3.]))
    pq.write_table(value, legacy / "2330_features.parquet")
    write(target / "2330_features.parquet", value)
    assert history._price_history_path(target, "") is None
    before, symbol, _ = history._read_price_symbol_series(legacy, "2330.TW")
    after, actual_symbol, path = history._read_price_symbol_series(target, "2330.TW")
    assert symbol == actual_symbol == "2330"
    same(before.to_arrow(), after.to_arrow())
    expected, _ = history._read_price_history_maps(legacy, "2330")
    actual, _ = history._read_price_history_maps(target, "2330")
    assert actual == expected
    assert history._read_price_history_maps(target, "2330")[0] == expected
    member = path.parent / json.loads(path.read_bytes())["members"][0]["path"]
    member.chmod(0o644)
    with pytest.raises(ValueError, match="content proof"):
        history._read_price_history_maps(target, "2330")


def test_stock_history_cache_rejects_same_size_mtime_replacement(tmp_path):
    from stockagent.live import stock_history as history
    path = tmp_path / "2330_features.parquet"
    value = table(values=[1., 2., 3.]).rename_columns(["date", "symbol", "close"])
    pq.write_table(value, path, compression=None)
    before = path.stat()
    first, _ = history._read_price_history_maps(tmp_path, "2330", price_columns=("close",))
    pq.write_table(value.set_column(2, "close", pa.array([4., 2., 3.])), path, compression=None)
    assert path.stat().st_size == before.st_size
    os.utime(path, ns=(before.st_atime_ns, before.st_mtime_ns))
    second, _ = history._read_price_history_maps(tmp_path, "2330", price_columns=("close",))
    assert first["close"]["2024-01-01"] == 1.
    assert second["close"]["2024-01-01"] == 4.


def test_base_benchmark_isolates_suffix_from_batch_sync_and_proof_cost(tmp_path, monkeypatch):
    from scripts import benchmark_dashboard_daily_incremental as benchmark
    original = benchmark._write_symbol_batch
    observed = {}
    def record(folder, groups, **kwargs):
        observed[folder.name] = (kwargs["workers"], kwargs["batch_directory_fsync"],
                                kwargs["state_sink"] is not None)
        return original(folder, groups, **kwargs)
    monkeypatch.setattr(benchmark, "_write_symbol_batch", record)
    result = benchmark.official_symbols(tmp_path, 4, 2, workers=1)
    assert observed["symbol_full"] == (1, False, False)
    assert observed["symbol_full_batched"] == observed["symbol_incremental"] == (1, True, True)
    assert result["full_batched_directory_commits"] == result["incremental_directory_commits"] == 1
    assert result["full_batched_with_same_proofs"]["wall_ms"] >= 0
    assert result["all_columns_nulls_and_values_equal"]
    assert result["partitioned"]["all_columns_raw_bits_nulls_and_order_equal"]

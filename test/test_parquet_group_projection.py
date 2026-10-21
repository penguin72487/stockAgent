from __future__ import annotations

import json

import polars as pl
from polars.testing import assert_frame_equal
import pytest

from stockagent.data.parquet_group_projection import ParquetGroupProjection


def _raw(count):
    return pl.DataFrame({"date": [f"2026-10-{day + 1:02}" for day in range(count)],
        "value": [None if day % 4 == 0 else "-0.0" if day % 5 == 0 else str(day / 7)
                  for day in range(count)], "name": [" 測試 "] * count})


def _parse(frame):
    return frame.select("date", pl.col("value").cast(pl.Float64), pl.col("name").str.strip_chars())


@pytest.mark.parametrize("change", ["append", "revision", "delete", "schema", "regroup", "implementation"])
def test_projection_reuses_only_exact_groups_and_matches_original_parser(tmp_path, change):
    source, cache = tmp_path / "source.parquet", tmp_path / "cache"
    _raw(18).write_parquet(source, row_group_size=6)
    first = ParquetGroupProjection(cache, {"code": "v1"})
    assert_frame_equal(first.project(source, _parse, parser="test"), _parse(_raw(18)), check_exact=True)
    assert first.stats()["parsed_source_rows"] == 18
    frame = _raw(24)
    if change == "revision":
        frame = frame.with_columns(pl.when(pl.col("date") == "2026-10-09")
            .then(pl.lit("1000")).otherwise(pl.col("value")).alias("value"))
    elif change == "delete":
        frame = frame.slice(0, 13)
    elif change == "schema":
        frame = frame.with_columns(pl.lit("new field").alias("extra"))
    frame.write_parquet(source, row_group_size=8 if change == "regroup" else 6)
    second = ParquetGroupProjection(cache, {"code": "v2" if change == "implementation" else "v1"})
    assert_frame_equal(second.project(source, _parse, parser="test"), _parse(frame), check_exact=True)
    work = second.stats()
    if change == "append":
        assert work["reused_source_rows"] == 18 and work["parsed_source_rows"] == 6
    if change in {"schema", "implementation", "regroup"}:
        assert work["groups_reused"] == 0


def test_corrupt_sealed_member_is_rejected_and_preserved(tmp_path):
    source, cache = tmp_path / "source.parquet", tmp_path / "cache"
    frame = _raw(12)
    frame.write_parquet(source, row_group_size=6)
    ParquetGroupProjection(cache, {"code": "v1"}).project(source, _parse, parser="test")
    original = json.loads((cache / "group_index.json").read_text())
    victim = cache / next(iter(original["members"].values()))["path"]
    victim.chmod(0o600)
    victim.write_bytes(b"corrupt cache, never a source")
    repaired = ParquetGroupProjection(cache, {"code": "v1"})
    assert_frame_equal(repaired.project(source, _parse, parser="test"), _parse(frame), check_exact=True)
    assert repaired.stats()["rejected_members"] == 1
    assert victim.read_bytes() == b"corrupt cache, never a source"
    assert_frame_equal(pl.read_parquet(source), frame)


def test_projection_cannot_publish_row_changing_parser_or_partial_generation(tmp_path):
    source, cache = tmp_path / "source.parquet", tmp_path / "cache"
    _raw(12).write_parquet(source, row_group_size=6)
    with pytest.raises(ValueError, match="row-local"):
        ParquetGroupProjection(cache, {"code": "bad"}).project(source, lambda frame: frame.head(1), parser="test")
    assert not (cache / "group_index.json").exists()


@pytest.mark.parametrize("venue", ["twse", "tpex"])
def test_official_source_parser_uses_same_row_local_path_with_cache(tmp_path, venue):
    from scripts.build_tw_official_symbol_parquets import _tpex_frame, _twse_frame, _symbol_code_identity
    source, cache = tmp_path / "source.parquet", tmp_path / "cache"
    names = ({"證券代號": "2330", "證券名稱": " 測試 ", "開盤價": "1,000", "最高價": "1001",
        "最低價": "999", "收盤價": "--", "成交股數": "0", "漲跌(+/-)": "-", "漲跌價差": "0.0"}
        if venue == "twse" else {"代號": "6488", "名稱": " 測試 ", "開盤": "1,000", "最高": "1001",
            "最低": "999", "收盤": "--", "成交股數": "0", "漲跌": "---", "次日參考價": "1000.1"})
    pl.DataFrame({"date": ["2026-10-19"] * 12, **{key: [value] * 12 for key, value in names.items()}}).write_parquet(
        source, row_group_size=6)
    parse = _twse_frame if venue == "twse" else _tpex_frame
    expected = parse(source)
    state = ParquetGroupProjection(cache, _symbol_code_identity())
    assert_frame_equal(parse(source, source_cache=state), expected, check_exact=True)
    hit = ParquetGroupProjection(cache, _symbol_code_identity())
    assert_frame_equal(parse(source, source_cache=hit), expected, check_exact=True)
    assert hit.stats()["parsed_source_rows"] == 0

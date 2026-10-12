from __future__ import annotations

from datetime import date

from scripts.download_taifex_vix_recent import _parse_vix


def test_vix_merge_preserves_schema_last_shard_wins_and_verified_noop(tmp_path):
    import pyarrow.parquet as pq
    from scripts.download_taifex_public_history import _atomic_write_parquet
    from scripts.download_taifex_vix_recent import _merge_vix_shards
    first = _parse_vix(b"20260803 13450000 39.46 39.47\n", {})
    revised = _parse_vix(b"20260803 13450000 40.46 40.47\n", {})
    shards = [tmp_path / "first.parquet", tmp_path / "revised.parquet"]
    for path, frame in zip(shards, (first, revised), strict=True):
        _atomic_write_parquet(frame, path)
    result = _merge_vix_shards(tmp_path, shards, "calendar-v1")
    actual = pq.read_table(tmp_path / result["output_path"])
    assert actual.schema == pq.read_schema(shards[0])
    assert actual["taifex_vix"].to_pylist() == [40.46]
    assert _merge_vix_shards(tmp_path, shards, "calendar-v1")["merge_reused"]
    assert not _merge_vix_shards(tmp_path, shards, "calendar-v2")["merge_reused"]


def test_vix_parser_uses_next_verified_session() -> None:
    content = (
        "交易日期\t時間(時/分/秒/毫秒)\t臺指選擇權波動率指數\t收盤前1分鐘平均指數\r\n"
        "--------\t-------------------\t--------------------\t-------------------\r\n"
        "20260803\t13450000\t\t\t39.46\t\t39.47\r\n"
    ).encode("cp950")
    frame = _parse_vix(content, {date(2026, 8, 3): date(2026, 8, 4)})

    assert frame.loc[0, "taifex_vix"] == 39.46
    assert frame.loc[0, "preclose_1m_average_vix"] == 39.47
    assert frame.loc[0, "available_date"].date() == date(2026, 8, 4)

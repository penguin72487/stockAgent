"""Date prewarm is not a full-row cache, financial read or readiness proof."""
import hashlib
import json
from pathlib import Path

import pytest

from stockagent.live import tw_day_trade_dashboard as dashboard
from stockagent.live.benchmark_history_projection import (
    load_benchmark_projection, projection_head_path, write_benchmark_projection,
)


@pytest.fixture
def source(tmp_path):
    rows = [{"session_date": day, "benchmark_id": "benchmark_2330",
             "minute": day + "T13:30:00+08:00", "total_equity_twd": value,
             "last_mark_price": value, "unknown_source_field": [None, -0.0, 2**64 + 1]}
            for day, value in (("2026-10-12", 99.5), ("2026-10-13", 101.25))]
    path = tmp_path / "benchmark_history.json"
    path.write_text(json.dumps({"schema_version": 2, "origins": {}, "marks": rows}))
    write_benchmark_projection(state_dir=tmp_path, source_path=path,
        source_sha256=hashlib.sha256(path.read_bytes()).hexdigest(), origins={}, marks=rows,
        created_at="2026-10-13T14:00:00+08:00")
    return tmp_path, rows


def test_valid_catalog_never_eagerly_decodes_all_shards_or_publishes_empty_full_index(source, monkeypatch):
    root, rows = source
    source_path = root / "benchmark_history.json"
    original = Path.read_bytes

    def reject_shard(path):
        assert "shards" not in path.parts, "prewarm only needs accepted date metadata"
        return original(path)

    with monkeypatch.context() as scoped:
        scoped.setattr(Path, "read_bytes", reject_shard)
        scoped.setattr(dashboard, "_benchmark_history_index", lambda *_: pytest.fail("full rows are not needed"))
        assert dashboard.warm_dashboard_session_indexes(state_dir=root)["benchmark_history.json"] == 2
    assert source_path.resolve() not in dashboard._BENCHMARK_HISTORY_INDEX_CACHE
    # The actual subsequent consumer still decodes and validates real rows.
    result = dashboard._benchmark_history_index(root)
    assert tuple(result.marks) == tuple(rows)
    assert set(result.marks_by_session) == {row["session_date"] for row in rows}


@pytest.mark.parametrize("failure", ["missing", "stale", "malformed"])
def test_unavailable_catalog_preserves_the_canonical_full_fallback(source, monkeypatch, failure):
    root, rows = source
    head = projection_head_path(root)
    if failure == "missing":
        head.unlink()
    elif failure == "malformed":
        head.write_text("{invalid")
    else:
        path = root / "benchmark_history.json"
        path.write_text(json.dumps({"schema_version": 2, "origins": {}, "marks": rows}) + "\n")
    calls, original = [], dashboard._benchmark_history_index

    def observed(path):
        calls.append(path)
        return original(path)

    monkeypatch.setattr(dashboard, "_benchmark_history_index", observed)
    assert dashboard.warm_dashboard_session_indexes(state_dir=root)["benchmark_history.json"] == 2
    assert calls == [root]
    assert tuple(original(root).marks) == tuple(rows)


def test_catalog_warm_does_not_hide_corrupt_shards_from_the_actual_reader(source):
    root, rows = source
    head = json.loads(projection_head_path(root).read_bytes())
    entry = head["sessions"][rows[0]["session_date"]]
    shard = root / "benchmark_history_projection/v1" / entry["path"]
    shard.write_bytes(b"broken")
    assert dashboard.warm_dashboard_session_indexes(state_dir=root)["benchmark_history.json"] == 2
    assert load_benchmark_projection(state_dir=root, source_path=root / "benchmark_history.json",
                                    selected_sessions=[rows[0]["session_date"]]) is None
    # Same canonical raw fallback as before; no fabricated or absent rows.
    assert tuple(dashboard._benchmark_history_index(root).marks) == tuple(rows)


def test_warm_does_not_evict_an_existing_full_index_or_change_complete_curve(source):
    root, _ = source
    original = dashboard._benchmark_history_index(root)
    options = dict(state_dir=root, range_key="all", resolution="1m",
                   history_encoding="minute_columns_v2", use_memory_cache=False, use_persistent_cache=False)
    before = dashboard.build_dashboard_history_snapshot(**options)
    dashboard.warm_dashboard_session_indexes(state_dir=root)
    assert dashboard._BENCHMARK_HISTORY_INDEX_CACHE[(root / "benchmark_history.json").resolve()] is original
    assert dashboard.build_dashboard_history_snapshot(**options) == before

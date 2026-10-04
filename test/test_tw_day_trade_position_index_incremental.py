"""Position history index keeps exact duplicate semantics with delta rebuilds."""

import json
import os

from stockagent.live import tw_day_trade_dashboard as dashboard


def _archive(path, session_date, *, signed_shares, identity="shared"):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({
        "session_date": session_date,
        "positions": [{
            "position_id": identity,
            "market": "tw_day_trade_multi_basis",
            "symbol": "2330",
            "signed_shares": signed_shares,
            "target_weight": 0.1,
        }],
    }), encoding="utf-8")


def test_position_index_reuses_unchanged_sources_and_restores_shadowed_id(
    tmp_path, monkeypatch
):
    state = tmp_path / "state"
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    monkeypatch.setenv("STOCKAGENT_DASHBOARD_INDEX_CACHE_DIR", str(cache_dir))
    first = state / "position_history/2026-08-13/a.json"
    second = state / "position_history/2026-08-14/b.json"
    _archive(first, "2026-08-13", signed_shares=1000)
    _archive(second, "2026-08-14", signed_shares=2000)
    dashboard._POSITION_HISTORY_INDEX_CACHE.clear()
    initial = dashboard._position_history_index(state)
    assert len(initial.entries) == 1
    assert initial.entries[0].signed_shares == 2000
    assert len(initial.per_source_entries) == 2

    original_object = dashboard._object
    decoded = []

    def traced_object(path, *args, **kwargs):
        decoded.append(path)
        return original_object(path, *args, **kwargs)

    monkeypatch.setattr(dashboard, "_object", traced_object)
    _archive(second, "2026-08-14", signed_shares=3000)
    stat = second.stat()
    os.utime(second, ns=(stat.st_atime_ns, stat.st_mtime_ns + 1_000_000_000))
    dashboard._POSITION_HISTORY_INDEX_CACHE.clear()  # exercise durable v2 index
    changed = dashboard._position_history_index(state)
    assert decoded == [second]
    assert changed.entries[0].signed_shares == 3000

    decoded.clear()
    second.unlink()
    dashboard._POSITION_HISTORY_INDEX_CACHE.clear()
    restored = dashboard._position_history_index(state)
    assert decoded == []
    assert len(restored.entries) == 1
    assert restored.entries[0].signed_shares == 1000
    assert restored.entries[0].source_index == 0


def test_position_index_remaps_reused_locator_after_sorted_insertion(
    tmp_path, monkeypatch
):
    state = tmp_path / "state"
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    monkeypatch.setenv("STOCKAGENT_DASHBOARD_INDEX_CACHE_DIR", str(cache_dir))
    later = state / "position_history/2026-08-14/b.json"
    earlier = state / "position_history/2026-08-13/a.json"
    _archive(later, "2026-08-14", signed_shares=2000, identity="later")
    dashboard._POSITION_HISTORY_INDEX_CACHE.clear()
    dashboard._position_history_index(state)
    _archive(earlier, "2026-08-13", signed_shares=1000, identity="earlier")
    dashboard._POSITION_HISTORY_INDEX_CACHE.clear()
    index = dashboard._position_history_index(state)
    assert [(entry.identity, entry.source_index) for entry in index.entries] == [
        ("earlier", 0), ("later", 1)
    ]
    hydrated = dashboard._rehydrate_position_entries(index, list(index.entries))
    assert set(hydrated) == {"earlier", "later"}

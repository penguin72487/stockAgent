"""Position history index keeps exact duplicate semantics with delta rebuilds."""

import json
import os
import pytest

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
    dashboard._POSITION_HISTORY_INDEX_CACHE.clear()  # exercise the durable index
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


@pytest.mark.parametrize("fresh_process", [False, True])
def test_position_index_revalidates_only_ctime_changed_source(tmp_path, monkeypatch, fresh_process):
    state, cache = tmp_path / "state", tmp_path / "cache"
    cache.mkdir()
    monkeypatch.setenv("STOCKAGENT_DASHBOARD_INDEX_CACHE_DIR", str(cache))
    first, second = state / "position_history/2026-08-13/a.json", state / "position_history/2026-08-14/b.json"
    _archive(first, "2026-08-13", signed_shares=1000, identity="first")
    _archive(second, "2026-08-14", signed_shares=2000, identity="second")
    dashboard._position_history_index(state)
    before = second.stat()
    _archive(second, "2026-08-14", signed_shares=3000, identity="second")
    os.utime(second, ns=(before.st_atime_ns, before.st_mtime_ns))
    if fresh_process:
        dashboard._POSITION_HISTORY_INDEX_CACHE.clear()
    original, decoded = dashboard._object, []

    def read(path, *args, **kwargs):
        decoded.append(path)
        return original(path, *args, **kwargs)

    monkeypatch.setattr(dashboard, "_object", read)
    actual = dashboard._position_history_index(state)
    assert decoded == [second]
    assert [row.signed_shares for row in actual.entries] == [1000, 3000]


def test_archived_position_count_rejects_preserved_mtime(tmp_path, monkeypatch):
    cache = tmp_path / "cache"
    cache.mkdir()
    monkeypatch.setenv("STOCKAGENT_DASHBOARD_INDEX_CACHE_DIR", str(cache))
    source = tmp_path / "position_history/2026-08-13/a.json"
    source.parent.mkdir(parents=True)
    body = b'{"positions":[{"i":1},{"i":2}]}\n'
    source.write_bytes(body)
    assert dashboard._historical_position_count(tmp_path) == 2
    before = source.stat()
    updated = b'{"positions":[{"i":1}]}'
    source.write_bytes(updated + b' ' * (len(body)-len(updated)-1) + b'\n')
    os.utime(source, ns=(before.st_atime_ns, before.st_mtime_ns))
    assert source.stat().st_size == before.st_size
    assert dashboard._historical_position_count(tmp_path) == 1

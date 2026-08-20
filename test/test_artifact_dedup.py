from __future__ import annotations

import os
import json
import time
from pathlib import Path

from stockagent.data_sync.artifact_dedup import (
    apply_duplicate_groups,
    find_duplicate_groups,
)


def _make_old(path: Path) -> None:
    timestamp = time.time() - 48 * 3600
    os.utime(path, (timestamp, timestamp))


def test_exact_duplicates_become_one_inode_without_losing_paths(tmp_path: Path) -> None:
    root = tmp_path / "artifacts"
    first = root / "markets" / "run-a" / "result.npz"
    second = root / "markets" / "run-b" / "result.npz"
    first.parent.mkdir(parents=True)
    second.parent.mkdir(parents=True)
    first.write_bytes(b"same payload")
    second.write_bytes(b"same payload")
    _make_old(first)
    _make_old(second)

    groups, counters = find_duplicate_groups(root, min_age_hours=24)
    replaced, skipped = apply_duplicate_groups(root, groups)

    assert counters["exact_duplicate_groups"] == 1
    assert len(replaced) == 1
    assert skipped == []
    assert first.read_bytes() == b"same payload"
    assert second.read_bytes() == b"same payload"
    assert os.path.samestat(first.stat(), second.stat())

    repeated_groups, repeated_counters = find_duplicate_groups(
        root, min_age_hours=24
    )
    assert repeated_groups == []
    assert repeated_counters["duplicate_inodes"] == 0


def test_same_size_different_content_is_not_deduplicated(tmp_path: Path) -> None:
    root = tmp_path / "artifacts"
    first = root / "markets" / "a.bin"
    second = root / "markets" / "b.bin"
    first.parent.mkdir(parents=True)
    first.write_bytes(b"aaaa")
    second.write_bytes(b"bbbb")
    _make_old(first)
    _make_old(second)

    groups, counters = find_duplicate_groups(root, min_age_hours=24)

    assert groups == []
    assert counters["exact_duplicate_groups"] == 0


def test_recent_and_mutable_tree_files_are_excluded(tmp_path: Path) -> None:
    root = tmp_path / "artifacts"
    old = root / "markets" / "old.bin"
    recent = root / "markets" / "recent.bin"
    live = root / "live" / "old.bin"
    old.parent.mkdir(parents=True)
    live.parent.mkdir(parents=True)
    old.write_bytes(b"duplicate")
    recent.write_bytes(b"duplicate")
    live.write_bytes(b"duplicate")
    _make_old(old)
    _make_old(live)

    groups, counters = find_duplicate_groups(root, min_age_hours=24)

    assert groups == []
    assert counters["eligible_files"] == 1


def test_complete_runs_only_excludes_running_and_unowned_paths(tmp_path: Path) -> None:
    root = tmp_path / "artifacts"
    complete = root / "markets" / "complete"
    running = root / "markets" / "running"
    unowned = root / "diagnostics"
    for directory, state, phase in (
        (complete, "complete", "complete"),
        (running, "running", "training"),
    ):
        directory.mkdir(parents=True)
        (directory / "progress.json").write_text(
            json.dumps({"state": state, "phase": phase}), encoding="utf-8"
        )
        _make_old(directory / "progress.json")
    unowned.mkdir(parents=True)
    for path in (
        complete / "first.bin",
        complete / "second.bin",
        running / "same.bin",
        unowned / "same.bin",
    ):
        path.write_bytes(b"same stable payload")
        _make_old(path)

    groups, counters = find_duplicate_groups(
        root,
        min_age_hours=24,
        require_complete_marker=True,
    )

    assert len(groups) == 1
    assert {groups[0].canonical.relative, groups[0].duplicates[0].relative} == {
        "markets/complete/first.bin",
        "markets/complete/second.bin",
    }
    assert counters["completed_marker_roots"] == 1
    assert counters["blocked_marker_roots"] == 1


def test_nested_running_marker_blocks_completed_parent(tmp_path: Path) -> None:
    root = tmp_path / "artifacts"
    parent = root / "ablations" / "suite"
    child = parent / "active-variant"
    child.mkdir(parents=True)
    (parent / "progress.json").write_text(
        json.dumps({"state": "complete", "phase": "complete"}), encoding="utf-8"
    )
    (child / "progress.json").write_text(
        json.dumps({"state": "running", "phase": "training"}), encoding="utf-8"
    )
    for path in (parent / "stable.bin", child / "first.bin", child / "second.bin"):
        path.write_bytes(b"same payload")
        _make_old(path)
    _make_old(parent / "progress.json")
    _make_old(child / "progress.json")

    groups, counters = find_duplicate_groups(
        root,
        min_age_hours=24,
        require_complete_marker=True,
    )

    assert groups == []
    assert counters["eligible_files"] == 2

from __future__ import annotations

import os
import json
import time
from pathlib import Path

import pytest

from stockagent.data_sync import artifact_dedup
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
    assert counters["sampled_distinct_inodes"] == 2
    assert counters["hashed_distinct_inodes"] == 0


def test_same_edge_samples_still_compare_full_content(tmp_path: Path) -> None:
    root = tmp_path / "artifacts"
    first = root / "markets" / "first.bin"
    second = root / "markets" / "second.bin"
    first.parent.mkdir(parents=True)
    edge = b"x" * (64 * 1024)
    first.write_bytes(edge + b"one" + edge)
    second.write_bytes(edge + b"two" + edge)
    _make_old(first)
    _make_old(second)

    groups, counters = find_duplicate_groups(root, min_age_hours=24)

    assert groups == []
    assert counters["sampled_distinct_inodes"] == 2
    assert counters["hashed_distinct_inodes"] == 2
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


@pytest.mark.parametrize(
    "marker",
    [
        b"[]",
        b'[{"state":"complete","phase":"complete"}]',
        b"null",
        b"true",
        b"42",
        b'"complete"',
        b'{"state":"complete"}',
        b'{"state":"complete","phase":"training"}',
        b"{broken-json",
        b"\xff\xfe",
    ],
)
def test_invalid_marker_blocks_only_its_subtree(
    tmp_path: Path, marker: bytes
) -> None:
    root = tmp_path / "artifacts"
    parent = root / "suite"
    invalid = parent / "invalid-child"
    invalid.mkdir(parents=True)
    (parent / "progress.json").write_text(
        json.dumps({"state": "complete", "phase": "complete"}),
        encoding="utf-8",
    )
    (invalid / "progress.json").write_bytes(marker)
    good_paths = [parent / "first.bin", parent / "second.bin"]
    bad_paths = [invalid / "first.bin", invalid / "second.bin"]
    for path in good_paths + bad_paths:
        path.write_bytes(b"same stable payload")
        _make_old(path)

    groups, counters = find_duplicate_groups(
        root, min_age_hours=24, require_complete_marker=True
    )
    replaced, skipped = apply_duplicate_groups(root, groups)

    assert counters["completed_marker_roots"] == 1
    assert counters["blocked_marker_roots"] == 1
    assert len(groups) == len(replaced) == 1
    assert skipped == []
    assert os.path.samestat(*(path.stat() for path in good_paths))
    assert not os.path.samestat(*(path.stat() for path in bad_paths))
    assert all(path.read_bytes() == b"same stable payload" for path in bad_paths)


def test_non_object_marker_cannot_certify_an_unowned_root(tmp_path: Path) -> None:
    (tmp_path / "progress.json").write_text("[]", encoding="utf-8")
    for name in ("first.bin", "second.bin"):
        path = tmp_path / name
        path.write_bytes(b"same stable payload")
        _make_old(path)

    groups, counters = find_duplicate_groups(
        tmp_path, min_age_hours=24, require_complete_marker=True
    )

    assert groups == []
    assert counters["eligible_files"] == 0
    assert counters["completed_marker_roots"] == 0
    assert counters["blocked_marker_roots"] == 1


def test_complete_child_of_blocked_parent_remains_reachable(tmp_path: Path) -> None:
    complete = tmp_path / "suite"
    running = complete / "running"
    resumed = running / "closed-child"
    invalid = running / "invalid-child"
    for directory, marker in (
        (complete, {"state": "complete", "phase": "complete"}),
        (running, {"state": "running", "phase": "training"}),
        (resumed, {"state": "complete", "phase": "complete"}),
        (invalid, []),
    ):
        directory.mkdir(parents=True, exist_ok=True)
        (directory / "progress.json").write_text(json.dumps(marker))
        path = directory / "payload.bin"
        path.write_bytes(b"same stable payload")
        _make_old(path)

    groups, counters = find_duplicate_groups(
        tmp_path, min_age_hours=24, require_complete_marker=True
    )

    assert len(groups) == 1
    assert {groups[0].canonical.relative, groups[0].duplicates[0].relative} == {
        "suite/payload.bin", "suite/running/closed-child/payload.bin",
    }
    assert counters["eligible_files"] == 2


def test_marker_scan_and_payload_scan_skip_excluded_top_level(tmp_path, monkeypatch):
    complete, excluded = tmp_path / "complete", tmp_path / "live"
    for directory in (complete, excluded):
        directory.mkdir()
        (directory / "progress.json").write_text(
            '{"state":"complete","phase":"complete"}'
        )
        for name in ("first.bin", "second.bin"):
            path = directory / name
            path.write_bytes(b"same payload")
            _make_old(path)
    original_walk = os.walk
    visited = []

    def walk(*args, **kwargs):
        for row in original_walk(*args, **kwargs):
            visited.append(Path(row[0]))
            yield row

    monkeypatch.setattr(artifact_dedup.os, "walk", walk)
    groups, counters = find_duplicate_groups(
        tmp_path, min_age_hours=24, require_complete_marker=True
    )

    assert len(groups) == 1
    assert counters["completed_marker_roots"] == 1
    assert excluded not in visited


def test_payload_scan_prunes_unowned_branches_without_prefix_collisions(tmp_path, monkeypatch):
    for relative in ("run-1", "run-10/deep", "unowned/deep"):
        path = tmp_path / relative / "payload.bin"
        path.parent.mkdir(parents=True)
        path.write_bytes(b"same payload")
        _make_old(path)
    original_walk = os.walk
    visited = []

    def walk(*args, **kwargs):
        for row in original_walk(*args, **kwargs):
            visited.append(Path(row[0]).relative_to(tmp_path).as_posix())
            yield row

    monkeypatch.setattr(artifact_dedup.os, "walk", walk)
    files = list(artifact_dedup._iter_files(
        tmp_path, cutoff_ns=time.time_ns(), excluded_top=frozenset(),
        excluded_suffixes=frozenset(), completed_roots=(("run-1",),), blocked_roots=(),
    ))

    assert [item.relative for item in files] == ["run-1/payload.bin"]
    assert visited == [".", "run-1"]


def test_completion_census_parses_paths_only_for_markers(tmp_path, monkeypatch):
    for index in range(30):
        directory = tmp_path / f"unowned-{index}" / "nested"
        directory.mkdir(parents=True)
        (directory / "ordinary.json").write_text("{}")
    closed = tmp_path / "closed"
    closed.mkdir()
    (closed / "progress.json").write_text('{"state":"complete","phase":"complete"}')
    original_path = Path
    parsed = []

    def path(value):
        parsed.append(value)
        return original_path(value)

    def no_walk(*args, **kwargs):
        pytest.fail("completion census must not allocate os.walk filename lists")

    monkeypatch.setattr(artifact_dedup, "Path", path)
    monkeypatch.setattr(artifact_dedup.os, "walk", no_walk)

    assert artifact_dedup._artifact_completion_roots(tmp_path) == ((("closed",),), ())
    assert len(parsed) == 2
    assert all(original_path(value) == closed for value in parsed)


def test_completion_census_discovers_new_nested_markers_and_changed_state(tmp_path):
    parent = tmp_path / "closed"
    child = parent / "nested"
    child.mkdir(parents=True)
    marker = parent / "progress.json"
    marker.write_text('{"state":"complete","phase":"complete"}')
    assert artifact_dedup._artifact_completion_roots(tmp_path) == ((("closed",),), ())
    (child / "progress.json").write_text('{"state":"running","phase":"training"}')
    assert artifact_dedup._artifact_completion_roots(tmp_path) == (
        (("closed",),), (("closed", "nested"),),
    )
    before = marker.stat()
    marker.write_text('{"state":"running","phase":"training"}')
    os.utime(marker, ns=(before.st_atime_ns, before.st_mtime_ns))
    assert artifact_dedup._artifact_completion_roots(tmp_path) == (
        (), (("closed",), ("closed", "nested")),
    )


def test_completion_census_does_not_follow_directory_symlinks(tmp_path):
    root, outside = tmp_path / "artifacts", tmp_path / "outside"
    root.mkdir()
    outside.mkdir()
    (outside / "progress.json").write_text('{"state":"complete","phase":"complete"}')
    (root / "redirected").symlink_to(outside, target_is_directory=True)
    # os.walk treats a directory/symlink-to-directory named progress.json as a
    # directory, not a lifecycle marker. Keep the original rule exactly.
    (root / "progress.json").symlink_to(outside, target_is_directory=True)

    assert artifact_dedup._artifact_completion_roots(root) == ((), ())


def test_completion_census_rechecks_symlink_before_descending(tmp_path, monkeypatch):
    child, outside = tmp_path / "child", tmp_path.parent / f"{tmp_path.name}-outside"
    child.mkdir()
    outside.mkdir()
    (child / "progress.json").write_text('{"state":"complete","phase":"complete"}')
    original = os.path.islink

    def replace_before_descent(path):
        if Path(path) == child:
            child.rename(outside / "moved")
            child.symlink_to(outside / "moved", target_is_directory=True)
        return original(path)

    monkeypatch.setattr(artifact_dedup.os.path, "islink", replace_before_descent)

    assert artifact_dedup._artifact_completion_roots(tmp_path) == ((), ())


def test_completion_census_unreadable_directory_does_not_block_other_roots(tmp_path, monkeypatch):
    for name in ("closed", "unreadable"):
        directory = tmp_path / name
        directory.mkdir()
        (directory / "progress.json").write_text('{"state":"complete","phase":"complete"}')
    original = os.scandir

    def scan(path):
        if Path(path) == tmp_path / "unreadable":
            raise PermissionError(path)
        return original(path)

    monkeypatch.setattr(artifact_dedup.os, "scandir", scan)

    assert artifact_dedup._artifact_completion_roots(tmp_path) == ((("closed",),), ())


@pytest.mark.parametrize("changed", ["sha", "eligibility", "apply", "extra"])
def test_audit_benchmark_rejects_changes_outside_census(tmp_path, monkeypatch, changed):
    from scripts import benchmark_artifact_dedup_scan as benchmark

    original = ("def _artifact_completion_roots(root):\n    return (), ()\n\n"
                "def sha(): return 1\n\ndef eligibility(): return 1\n\ndef apply(): return 1\n")
    (tmp_path / "artifact_dedup.py").write_text(original)
    candidate = tmp_path / "candidate.py"
    candidate.write_text(original.replace("return (), ()", "return tuple(), tuple()"))
    monkeypatch.setattr(benchmark, "SOURCE", candidate)
    benchmark.verify_census_only(tmp_path)
    candidate.write_text(candidate.read_text() + "\nextra = 1\n" if changed == "extra" else
                         candidate.read_text().replace(f"def {changed}(): return 1", f"def {changed}(): return 2"))
    with pytest.raises(ValueError, match="outside the directory census"):
        benchmark.verify_census_only(tmp_path)

from __future__ import annotations

import os
from pathlib import Path

import pytest

from stockagent.data_sync.live_artifacts import (
    cold_ignored_artifacts,
    is_ignored_artifact,
    reconcile_artifacts,
)


def test_reconcile_adds_remote_missing_and_publishes_local_missing(
    tmp_path: Path,
) -> None:
    local = tmp_path / "local"
    sync = tmp_path / "sync"
    local.mkdir()
    sync.mkdir()
    (local / "local-only.json").write_text("local", encoding="utf-8")
    (sync / "remote-only.json").write_text("remote", encoding="utf-8")

    result = reconcile_artifacts(local, sync)

    assert result.incoming_added == 1
    assert result.local_published == 1
    assert (local / "remote-only.json").read_text(encoding="utf-8") == "remote"
    assert (sync / "local-only.json").read_text(encoding="utf-8") == "local"
    assert os.path.samestat(
        (local / "remote-only.json").stat(),
        (sync / "remote-only.json").stat(),
    )


def test_reconcile_conflict_always_keeps_local_file(tmp_path: Path) -> None:
    local = tmp_path / "local"
    sync = tmp_path / "sync"
    local.mkdir()
    sync.mkdir()
    (local / "same.json").write_text("penguin", encoding="utf-8")
    (sync / "same.json").write_text("peer", encoding="utf-8")

    result = reconcile_artifacts(local, sync)

    assert result.local_conflicts_won == 1
    assert (local / "same.json").read_text(encoding="utf-8") == "penguin"
    assert (sync / "same.json").read_text(encoding="utf-8") == "penguin"
    assert os.path.samestat(
        (local / "same.json").stat(), (sync / "same.json").stat()
    )


def test_force_publish_relinks_an_already_shared_local_file(tmp_path: Path) -> None:
    local = tmp_path / "local"
    sync = tmp_path / "sync"
    local.mkdir()
    sync.mkdir()
    (local / "shared.json").write_text("local", encoding="utf-8")
    reconcile_artifacts(local, sync)

    result = reconcile_artifacts(
        local, sync, relative="shared.json", force_local_publish=True
    )

    assert result.local_published == 1
    assert os.path.samestat(
        (local / "shared.json").stat(), (sync / "shared.json").stat()
    )


def test_reconcile_never_propagates_deletion(tmp_path: Path) -> None:
    local = tmp_path / "local"
    sync = tmp_path / "sync"
    local.mkdir()
    sync.mkdir()
    (sync / "kept.json").write_text("data", encoding="utf-8")

    reconcile_artifacts(local, sync)
    (local / "kept.json").unlink()
    result = reconcile_artifacts(local, sync)

    assert result.incoming_added == 1
    assert (local / "kept.json").read_text(encoding="utf-8") == "data"


@pytest.mark.parametrize(
    "relative",
    [
        "data_locks/job.lock",
        "markets/train.pid",
        ".stversions/old.json",
        ".stignore-cold-local",
        "markets/.syncthing.file.tmp",
        "markets/file.sync-conflict-20260818.json",
        "live/state.json.tmp.0123456789abcdef",
        "live/state.json.tmp",
        "live/.stockagent-live-sync.state.json.42.uuid.tmp",
        "live/work.tmp/partial.json",
    ],
)
def test_node_local_and_syncthing_control_files_are_ignored(
    relative: str,
) -> None:
    assert is_ignored_artifact(relative)


def test_reconcile_rejects_overlapping_roots(tmp_path: Path) -> None:
    local = tmp_path / "local"
    local.mkdir()

    with pytest.raises(ValueError, match="must not overlap"):
        reconcile_artifacts(local, local / "sync")


def test_reconcile_does_not_publish_or_restore_atomic_temp_files(
    tmp_path: Path,
) -> None:
    local = tmp_path / "local"
    sync = tmp_path / "sync"
    local.mkdir()
    sync.mkdir()
    (local / "state.json.tmp.local").write_text("local temp", encoding="utf-8")
    (sync / "state.json.tmp.remote").write_text("remote temp", encoding="utf-8")

    result = reconcile_artifacts(local, sync)

    assert result.incoming_added == 0
    assert result.local_published == 0
    assert not (local / "state.json.tmp.remote").exists()
    assert not (sync / "state.json.tmp.local").exists()


def test_reconcile_does_not_publish_activated_cold_paths(tmp_path: Path) -> None:
    local = tmp_path / "local"
    sync = tmp_path / "sync"
    cold = local / "markets" / "complete" / "report.json"
    cold.parent.mkdir(parents=True)
    sync.mkdir()
    cold.write_text("verified cold data", encoding="utf-8")
    (sync / ".stignore-cold-local").write_text(
        "// generated\n(?d)/markets/complete/report.json\n",
        encoding="utf-8",
    )

    result = reconcile_artifacts(local, sync)

    assert cold_ignored_artifacts(sync) == {
        Path("markets/complete/report.json")
    }
    assert result.local_published == 0
    assert not (sync / "markets" / "complete" / "report.json").exists()


def test_reconcile_counts_exact_cold_event_as_ignored(tmp_path: Path) -> None:
    local = tmp_path / "local"
    sync = tmp_path / "sync"
    local.mkdir()
    sync.mkdir()
    (sync / ".stignore-cold-local").write_text(
        "(?d)/markets/complete/report.json\n", encoding="utf-8"
    )

    result = reconcile_artifacts(
        local, sync, relative="markets/complete/report.json"
    )

    assert result.ignored == 1

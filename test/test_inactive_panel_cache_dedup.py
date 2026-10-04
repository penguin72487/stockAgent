import fcntl
import json
import os
import time

import numpy as np
import pytest

from scripts import deduplicate_inactive_panel_caches as cleanup


def cache_fixture(repo, name, values):
    panel = repo / "artifacts/cache" / name / "panel_cache_v2"
    generation = panel / "generations" / ("a" * 32)
    generation.mkdir(parents=True)
    (panel / "meta.json").write_text(
        json.dumps(
            {
                "arrays": {
                    "features": {"file": f"generations/{generation.name}/features.npy"}
                }
            }
        )
    )
    np.save(generation / "features.npy", values)
    old = time.time() - 10 * 86400
    os.utime(generation / "features.npy", (old, old))
    return panel, generation / "features.npy"


def allow_no_services(monkeypatch):
    monkeypatch.setattr(cleanup, "current_service_blockers", lambda selected, repo: [])
    monkeypatch.setattr(cleanup, "process_references_many", lambda selected: [])


def test_dry_run_does_not_change_paths_or_inodes(tmp_path, monkeypatch):
    allow_no_services(monkeypatch)
    names = sorted(cleanup.RESEARCH_PANELS)[:2]
    files = [cache_fixture(tmp_path, name, np.arange(20))[1] for name in names]
    before = [file.stat().st_ino for file in files]
    result = cleanup.compact(
        names=names, apply=False, receipt_dir=tmp_path / "receipts", repo=tmp_path
    )
    assert result["would_replace_files"] == 1
    assert result["reclaimed_allocated_bytes"] == 0
    assert [file.stat().st_ino for file in files] == before


def test_exact_duplicates_share_payload_but_all_logical_paths_remain(
    tmp_path, monkeypatch
):
    allow_no_services(monkeypatch)
    names = sorted(cleanup.RESEARCH_PANELS)[:2]
    files = [cache_fixture(tmp_path, name, np.arange(20))[1] for name in names]
    result = cleanup.compact(
        names=names, apply=True, receipt_dir=tmp_path / "receipts", repo=tmp_path
    )
    assert result["logical_paths_removed"] == 0
    assert len(result["replaced"]) == 1
    assert files[0].stat().st_ino == files[1].stat().st_ino
    assert np.array_equal(np.load(files[0]), np.arange(20))
    assert all(file.exists() for file in files)


@pytest.mark.parametrize(
    "name",
    ["live_signal_panels", "../data_tw_public", "tw_day_trade_v8_annual_log_cash"],
)
def test_source_service_or_unsafe_names_are_rejected(tmp_path, name):
    root = tmp_path / "artifacts/cache"
    root.mkdir(parents=True)
    with pytest.raises(ValueError, match="allowlisted"):
        cleanup.checked_panel_roots(root, [name])


def test_symlink_inside_generation_is_rejected(tmp_path):
    name = next(iter(cleanup.RESEARCH_PANELS))
    panel, file = cache_fixture(tmp_path, name, np.arange(20))
    (file.parent / "alias.npy").symlink_to(file)
    with pytest.raises(ValueError, match="symlink"):
        cleanup.checked_panel_roots(panel.parent.parent, [name])


def test_active_references_block_even_a_dry_run(tmp_path, monkeypatch):
    allow_no_services(monkeypatch)
    name = next(iter(cleanup.RESEARCH_PANELS))
    cache_fixture(tmp_path, name, np.arange(20))
    monkeypatch.setattr(
        cleanup, "process_references_many", lambda selected: ["pid=123:maps"]
    )
    with pytest.raises(ValueError, match="protected cache"):
        cleanup.compact(
            names=[name], apply=False, receipt_dir=tmp_path / "receipts", repo=tmp_path
        )


def test_held_writer_lock_blocks_cleanup_without_waiting(tmp_path, monkeypatch):
    allow_no_services(monkeypatch)
    name = next(iter(cleanup.RESEARCH_PANELS))
    panel, _ = cache_fixture(tmp_path, name, np.arange(20))
    with (panel / ".write.lock").open("a+b") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        with pytest.raises(BlockingIOError):
            cleanup.compact(
                names=[name],
                apply=False,
                receipt_dir=tmp_path / "receipts",
                repo=tmp_path,
            )


def test_non_generation_arrays_never_compact(tmp_path, monkeypatch):
    allow_no_services(monkeypatch)
    names = sorted(cleanup.RESEARCH_PANELS)[:2]
    for name in names:
        panel, _ = cache_fixture(tmp_path, name, np.arange(20))
        np.save(panel / "extra.npy", np.arange(30))
        old = time.time() - 10 * 86400
        os.utime(panel / "extra.npy", (old, old))
    result = cleanup.compact(
        names=names, apply=True, receipt_dir=tmp_path / "receipts", repo=tmp_path
    )
    assert len(result["replaced"]) == 1
    assert all("/generations/" in row["path"] for row in result["replaced"])

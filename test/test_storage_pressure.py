from __future__ import annotations

import os
from pathlib import Path

import pytest

from stockagent.data_sync.storage_pressure import (
    maintain_rebuildable_caches,
    protected_processes,
    validate_cache_roots,
)


def _age(path: Path, *, now_ns: int, days: float) -> None:
    timestamp = now_ns - int(days * 86_400 * 1_000_000_000)
    os.utime(path, ns=(timestamp, timestamp))


def test_cache_root_must_be_a_strict_allowlisted_child(tmp_path: Path) -> None:
    allowed = tmp_path / "cache"
    allowed.mkdir()
    with pytest.raises(ValueError, match="strict child"):
        validate_cache_roots([allowed], allowed_root=allowed)
    with pytest.raises(ValueError, match="strict child"):
        validate_cache_roots([tmp_path / "outside"], allowed_root=allowed)


def test_force_prune_removes_only_old_regular_cache_files(tmp_path: Path) -> None:
    now_ns = 2_000_000_000_000_000_000
    allowed = tmp_path / "cache"
    root = allowed / "torchinductor"
    root.mkdir(parents=True)
    old = root / "old.bin"
    recent = root / "recent.bin"
    partial = root / "old.partial"
    old.write_bytes(b"old" * 4096)
    recent.write_bytes(b"recent" * 4096)
    partial.write_bytes(b"partial" * 4096)
    _age(old, now_ns=now_ns, days=20)
    _age(recent, now_ns=now_ns, days=2)
    _age(partial, now_ns=now_ns, days=20)

    audit = maintain_rebuildable_caches(
        [root],
        allowed_root=allowed,
        min_age_days=14,
        apply=False,
        force=True,
        now_ns=now_ns,
    )
    assert audit["selected_files"] == 1
    assert old.exists()

    applied = maintain_rebuildable_caches(
        [root],
        allowed_root=allowed,
        min_age_days=14,
        apply=True,
        force=True,
        now_ns=now_ns,
    )
    assert applied["deleted_files"] == 1
    assert not old.exists()
    assert recent.exists()
    assert partial.exists()


def test_non_pressure_audit_selects_nothing(tmp_path: Path) -> None:
    now_ns = 2_000_000_000_000_000_000
    allowed = tmp_path / "cache"
    root = allowed / "triton"
    root.mkdir(parents=True)
    old = root / "old.bin"
    old.write_bytes(b"payload")
    _age(old, now_ns=now_ns, days=20)

    result = maintain_rebuildable_caches(
        [root],
        allowed_root=allowed,
        min_age_days=14,
        high_watermark_percent=99.999,
        target_percent=99.0,
        apply=False,
        now_ns=now_ns,
    )
    assert not result["under_pressure"]
    assert result["eligible_files"] == 1
    assert result["selected_files"] == 0
    assert old.exists()


def test_protected_process_discovery_reads_proc_cmdline(tmp_path: Path) -> None:
    process = tmp_path / "proc" / "123"
    process.mkdir(parents=True)
    (process / "cmdline").write_bytes(b"/venv/bin/python\0train.py\0--config\0x.yaml\0")

    result = protected_processes(["train.py"], proc_root=tmp_path / "proc")

    assert result == [
        {
            "pid": 123,
            "matched": "train.py",
            "command": "/venv/bin/python train.py --config x.yaml",
        }
    ]


def test_active_training_defers_automatic_cleanup(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    now_ns = 2_000_000_000_000_000_000
    allowed = tmp_path / "cache"
    root = allowed / "torchinductor"
    root.mkdir(parents=True)
    old = root / "old.bin"
    old.write_bytes(b"payload")
    _age(old, now_ns=now_ns, days=20)
    monkeypatch.setattr(
        "stockagent.data_sync.storage_pressure.protected_processes",
        lambda _patterns: [{"pid": 77, "matched": "train.py", "command": "python train.py"}],
    )

    result = maintain_rebuildable_caches(
        [root],
        allowed_root=allowed,
        min_age_days=14,
        high_watermark_percent=0.0001,
        target_percent=0.00001,
        apply=True,
        now_ns=now_ns,
    )

    assert result["under_pressure"]
    assert not result["inventory_complete"]
    assert result["scan_skipped_reason"] == "protected-process-active"
    assert result["deferred_reason"] == "protected-process-active"
    assert result["selected_files"] == 0
    assert result["deleted_files"] == 0
    assert old.exists()


def test_below_watermark_apply_skips_cache_tree_walk(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    allowed = tmp_path / "cache"
    root = allowed / "triton"
    root.mkdir(parents=True)
    monkeypatch.setattr(
        "stockagent.data_sync.storage_pressure._scan_cache_files",
        lambda *_args, **_kwargs: pytest.fail("cache tree must not be scanned"),
    )
    monkeypatch.setattr(
        "stockagent.data_sync.storage_pressure.protected_processes",
        lambda _patterns: [],
    )

    result = maintain_rebuildable_caches(
        [root],
        allowed_root=allowed,
        high_watermark_percent=99.999,
        target_percent=99.0,
        apply=True,
    )

    assert not result["under_pressure"]
    assert not result["inventory_complete"]
    assert result["scan_skipped_reason"] == "below-high-watermark"
    assert result["selected_files"] == 0

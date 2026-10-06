from __future__ import annotations

import os
from pathlib import Path

import pytest

from stockagent.data_sync.storage_pressure import (
    _filesystem_usage,
    maintain_rebuildable_caches,
    protected_processes,
    validate_cache_roots,
)


def test_pressure_uses_writer_available_space_including_reserved_blocks(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "stockagent.data_sync.storage_pressure.shutil.disk_usage",
        lambda _path: type("Usage", (), {"total": 1000, "used": 850, "free": 100})(),
    )

    usage = _filesystem_usage(tmp_path)

    assert usage["raw_filesystem_used_percent"] == 85.0
    assert usage["used_percent"] == 90.0
    assert usage["unavailable_bytes"] == 900


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


def test_tmp_compiler_scope_is_explicit_and_exclusive(tmp_path, monkeypatch):
    from scripts import maintain_storage_pressure as cli
    parent = tmp_path / "temporary"
    parent.mkdir()
    root = parent / "torchinductor_root"
    root.mkdir()
    monkeypatch.setattr(cli, "TMP_COMPILER_CACHE_ROOT", root)
    monkeypatch.setattr(cli.os, "geteuid", lambda: 0)
    args = cli.build_parser().parse_args(["--tmp-torchinductor-only"])
    assert cli.maintenance_scope(args) == (parent, [root])
    assert not cli.build_parser().parse_args([]).tmp_torchinductor_only
    args.cache_root = [parent / "unrelated"]
    with pytest.raises(ValueError, match="no custom"):
        cli.maintenance_scope(args)


def test_tmp_compiler_scope_rejects_redirected_and_non_root_use(tmp_path, monkeypatch):
    from scripts import maintain_storage_pressure as cli
    parent = tmp_path / "temporary"
    parent.mkdir()
    target = parent / "unrelated"
    target.mkdir()
    root = parent / "torchinductor_root"
    root.symlink_to(target, target_is_directory=True)
    monkeypatch.setattr(cli, "TMP_COMPILER_CACHE_ROOT", root)
    monkeypatch.setattr(cli.os, "geteuid", lambda: 0)
    args = cli.build_parser().parse_args(["--tmp-torchinductor-only"])
    with pytest.raises(ValueError, match="redirected"):
        cli.maintenance_scope(args)
    monkeypatch.setattr(cli.os, "geteuid", lambda: 1000)
    with pytest.raises(ValueError, match="requires root"):
        cli.maintenance_scope(args)


def test_default_compiler_scope_never_includes_tmp(tmp_path, monkeypatch):
    from scripts import maintain_storage_pressure as cli
    root = tmp_path / "cache"
    monkeypatch.setattr(cli, "_cache_home", lambda: root)
    monkeypatch.setattr(cli, "_default_roots", lambda parent: [parent / "torchinductor"])
    args = cli.build_parser().parse_args([])
    assert cli.maintenance_scope(args) == (root, [root / "torchinductor"])


def _local_policy(tmp_path, monkeypatch, **changes):
    import json
    from scripts import maintain_storage_pressure as cli
    parent = tmp_path / "temporary"
    parent.mkdir()
    temporary = parent / "torchinductor_root"
    temporary.mkdir()
    home = tmp_path / "cache"
    home.mkdir(mode=0o700)
    monkeypatch.setattr(cli.os, "geteuid", lambda: 0)
    monkeypatch.setattr(cli, "TMP_COMPILER_CACHE_ROOT", temporary)
    monkeypatch.setattr(cli, "_cache_home", lambda: home)
    monkeypatch.setattr(cli, "_default_roots", lambda p: [p / "torchinductor"])
    policy = tmp_path / "policy.json"
    body = {"schema_version": 1, "scope": "compiler-home-and-root-tmp", "min_age_days": 14,
            "high_watermark_percent": 89, "target_percent": 88, **changes}
    policy.write_text(json.dumps(body))
    policy.chmod(0o600)
    return cli, policy, home, temporary


def test_automatic_policy_keeps_two_exact_allowlist_roots(tmp_path, monkeypatch):
    cli, policy, home, temporary = _local_policy(tmp_path, monkeypatch)
    args = cli.build_parser().parse_args(["--policy", str(policy)])
    scopes, digest = cli.prepare_scopes(args)
    assert scopes == [("cache-home", home, [home / "torchinductor"]),
                      ("tmp-torchinductor-only", temporary.parent, [temporary])]
    assert len(digest) == 64 and args.min_age_days == 14
    assert not args.force and args.protected_process_substring is None
    assert all(allowed != Path("/") for _name, allowed, _roots in scopes)


def test_automatic_policy_rejects_environment_redirects(tmp_path, monkeypatch):
    cli, policy, home, _temporary = _local_policy(tmp_path, monkeypatch)
    monkeypatch.setattr(cli, "_default_roots", lambda p: [p / "unique-source"])
    with pytest.raises(ValueError, match="environment cache roots"):
        cli.prepare_scopes(cli.build_parser().parse_args(["--policy", str(policy)]))
    monkeypatch.setattr(cli, "_default_roots", lambda p: [p / "torchinductor"])
    home.chmod(0o777)
    with pytest.raises(ValueError, match="root-controlled"):
        cli.prepare_scopes(cli.build_parser().parse_args(["--policy", str(policy)]))


def test_automatic_policy_rejects_redirected_cache_home(tmp_path, monkeypatch):
    cli, policy, home, _temporary = _local_policy(tmp_path, monkeypatch)
    alias = tmp_path / "redirected-cache-home"
    alias.symlink_to(home, target_is_directory=True)
    monkeypatch.setattr(cli, "_cache_home", lambda: alias)
    with pytest.raises(ValueError, match="cache home must not be redirected"):
        cli.prepare_scopes(cli.build_parser().parse_args(["--policy", str(policy)]))


def test_automatic_policy_accepts_existing_root_group_cache(tmp_path, monkeypatch):
    cli, policy, home, _temporary = _local_policy(tmp_path, monkeypatch)
    assert home.stat().st_uid == home.stat().st_gid == 0
    home.chmod(0o775)
    scopes, _digest = cli.prepare_scopes(cli.build_parser().parse_args(["--policy", str(policy)]))
    assert scopes[0][1] == home and home.stat().st_mode & 0o777 == 0o775


@pytest.mark.parametrize("extra", [["--force"], ["--cache-root", "/srv/stockagent-packed"],
    ["--tmp-torchinductor-only"], ["--protected-process-substring", "ignore-everything"],
    ["--min-age-days", "0"], ["--high-watermark-percent", "1"]])
def test_automatic_policy_rejects_manual_bypasses(tmp_path, monkeypatch, extra):
    cli, policy, _home, _temporary = _local_policy(tmp_path, monkeypatch)
    args = cli.build_parser().parse_args(["--policy", str(policy), *extra])
    with pytest.raises(ValueError):
        cli.prepare_scopes(args)


@pytest.mark.parametrize("changes", [{"min_age_days": 0}, {"min_age_days": True},
    {"min_age_days": float("nan")}, {"schema_version": True}, {"scope": "all-of-tmp"},
    {"source_root": "/srv/stockagent-packed"}, {"target_percent": 90}])
def test_automatic_policy_rejects_unknown_or_unsafe_schema(tmp_path, monkeypatch, changes):
    cli, policy, _home, _temporary = _local_policy(tmp_path, monkeypatch, **changes)
    with pytest.raises(ValueError):
        cli.prepare_scopes(cli.build_parser().parse_args(["--policy", str(policy)]))


def test_automatic_policy_rejects_symlink_writable_and_duplicate_rules(tmp_path, monkeypatch):
    cli, policy, _home, _temporary = _local_policy(tmp_path, monkeypatch)
    redirected = tmp_path / "redirect.json"
    redirected.symlink_to(policy)
    with pytest.raises(ValueError, match="non-redirected"):
        cli.load_local_policy(redirected)
    policy.chmod(0o666)
    with pytest.raises(ValueError, match="non-writable"):
        cli.load_local_policy(policy)
    policy.chmod(0o600)
    policy.write_text('{"schema_version":1,"schema_version":1}')
    with pytest.raises(ValueError, match="duplicate"):
        cli.load_local_policy(policy)


def test_local_policy_enrollment_is_idempotent_and_preserves_other_policy(tmp_path, monkeypatch):
    import json
    cli, policy, _home, temporary = _local_policy(tmp_path, monkeypatch)
    destination = tmp_path / "etc/stockagent/storage-pressure.json"
    monkeypatch.setattr(cli, "LOCAL_POLICY_PATH", destination)
    _values, digest = cli.load_local_policy(policy)
    first = cli.enroll_policy(policy, digest)
    assert first["created"] and first["cache_files_deleted_by_enrollment"] == 0
    assert destination.stat().st_mode & 0o777 == 0o600
    assert cli.enroll_policy(policy, digest)["created"] is False
    original = destination.read_bytes()
    other = json.loads(policy.read_text())
    other["min_age_days"] = 21
    policy.write_text(json.dumps(other))
    _values, changed = cli.load_local_policy(policy)
    with pytest.raises(ValueError, match="preserve a different"):
        cli.enroll_policy(policy, changed)
    assert destination.read_bytes() == original and temporary.is_dir()


def test_policy_check_and_enrollment_never_scan_or_delete_cache(tmp_path, monkeypatch, capsys):
    import json
    cli, policy, _home, _temporary = _local_policy(tmp_path, monkeypatch)
    destination = tmp_path / "etc/stockagent/storage-pressure.json"
    monkeypatch.setattr(cli, "LOCAL_POLICY_PATH", destination)
    monkeypatch.setattr(cli, "maintain_rebuildable_caches", lambda *a, **k: pytest.fail("enrollment scanned/deleted data"))
    monkeypatch.setattr("sys.argv", ["cleanup", "--policy", str(policy), "--check-policy"])
    assert cli.main() == 0
    assert json.loads(capsys.readouterr().out)["state"] == "policy-valid"
    assert not destination.exists()
    monkeypatch.setattr("sys.argv", ["cleanup", "--policy", str(policy), "--enroll-policy",
        "--receipt-dir", str(tmp_path / "receipts"), "--lock-path", str(tmp_path / "owner.lock")])
    assert cli.main() == 0 and destination.exists()
    assert json.loads(capsys.readouterr().out)["cache_files_deleted_by_enrollment"] == 0


def test_automatic_combined_policy_apply_retains_young_and_source_data(tmp_path, monkeypatch, capsys):
    import json
    import stockagent.data_sync.storage_pressure as pressure
    cli, policy, home, temporary = _local_policy(tmp_path, monkeypatch)
    local = home / "torchinductor"
    local.mkdir()
    now_ns = __import__("time").time_ns()
    for root in (local, temporary):
        old = root / "old.bin"
        old.write_bytes(b"rebuildable compile result")
        _age(old, now_ns=now_ns, days=20)
        (root / "young.bin").write_bytes(b"recent compiler output")
    source = tmp_path / "source-observation.parquet"
    source.write_bytes(b"unique source unchanged")
    monkeypatch.setattr(pressure, "_filesystem_usage", lambda _p: {
        "total_bytes": 1_000_000, "used_bytes": 990_000, "free_bytes": 10_000,
        "unavailable_bytes": 990_000, "used_percent": 99., "raw_filesystem_used_percent": 99.})
    monkeypatch.setattr(pressure, "protected_processes", lambda _p: [])
    monkeypatch.setattr(pressure, "_open_cache_files", lambda: set())
    monkeypatch.setattr("sys.argv", ["cleanup", "--policy", str(policy), "--apply",
        "--receipt-dir", str(tmp_path / "receipts"), "--lock-path", str(tmp_path / "owner.lock")])
    assert cli.main() == 0
    summary = json.loads(capsys.readouterr().out)
    receipt = json.loads(Path(summary["receipt"]).read_text())
    assert summary["deleted_files"] == 2 and not summary["errors"]
    assert receipt["cache_scope"] == "compiler-home-and-root-tmp"
    assert len(receipt["scope_results"]) == 2 and receipt["enrolled_policy_sha256"]
    assert all(not (root / "old.bin").exists() and (root / "young.bin").exists() for root in (local, temporary))
    assert source.read_bytes() == b"unique source unchanged"


def test_compiler_cache_new_hard_link_after_scan_is_preserved(tmp_path, monkeypatch):
    import stockagent.data_sync.storage_pressure as pressure
    now_ns = 2_000_000_000_000_000_000
    allowed = tmp_path / "cache"
    root = allowed / "torchinductor"
    root.mkdir(parents=True)
    old = root / "old.bin"
    old.write_bytes(b"cached" * 4096)
    _age(old, now_ns=now_ns, days=20)
    calls = 0
    def open_paths():
        nonlocal calls
        calls += 1
        if calls == 2:
            os.link(old, tmp_path / "new-external-name")
        return set()
    monkeypatch.setattr(pressure, "_open_cache_files", open_paths)
    result = pressure.maintain_rebuildable_caches(
        [root], allowed_root=allowed, apply=True, force=True, now_ns=now_ns,
    )
    assert result["selected_files"] == 1
    assert result["deleted_files"] == 0 and result["skipped_changed"] == 1
    assert old.exists() and (tmp_path / "new-external-name").exists()


def test_compiler_cache_new_open_reference_before_unlink_is_preserved(tmp_path, monkeypatch):
    import stockagent.data_sync.storage_pressure as pressure
    now_ns = 2_000_000_000_000_000_000
    allowed = tmp_path / "cache"
    root = allowed / "torchinductor"
    root.mkdir(parents=True)
    old = root / "old.bin"
    old.write_bytes(b"cached")
    _age(old, now_ns=now_ns, days=20)
    calls = 0
    def open_paths():
        nonlocal calls
        calls += 1
        return {old} if calls >= 3 else set()
    monkeypatch.setattr(pressure, "_open_cache_files", open_paths)
    result = pressure.maintain_rebuildable_caches(
        [root], allowed_root=allowed, apply=True, force=True, now_ns=now_ns,
    )
    assert result["selected_files"] == 1
    assert result["deleted_files"] == 0 and result["skipped_open"] == 1
    assert old.exists()


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

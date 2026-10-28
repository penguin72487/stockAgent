"""Frozen code must isolate development without weakening real drift checks."""
import json
import os
from pathlib import Path
import subprocess

import pytest

from stockagent.live import service_code_release as release


@pytest.fixture
def workspace(tmp_path):
    root = tmp_path / "repo"
    root.mkdir()
    subprocess.run(["git", "init", "-q", str(root)], check=True)
    for name in ("stockagent/live/example.py", "downloader/example.py", "scripts/example.sh", release.ENTRYPOINT):
        path = root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("# original source\n")
    (root / ".gitignore").write_text("artifacts/\n.env\n")
    (root / ".env").write_text("SECRET=not a source member\n")
    (root / "services/discord_bot/.env").write_text("SECRET=never copied into release\n")
    for name in ("markets", "models", "staged_accounts"):
        path = root / "services/discord_bot" / name
        path.mkdir()
        (path / "live.yaml").write_text("version: original\n")
    subprocess.run(["git", "add", "."], cwd=root, check=True)
    subprocess.run(["git", "-c", "user.name=fixture", "-c", "user.email=fixture@example.invalid",
                    "commit", "-qm", "fixture"], cwd=root, check=True)
    return root


def prepare(root):
    return release.freeze_source(root, root / "artifacts/operations/service_code_releases")


def test_workspace_edits_do_not_invalidate_or_mutate_pinned_source(workspace):
    receipt = prepare(workspace)
    proof = release.verify_frozen_source(receipt, workspace)
    generation = release.frozen_generation(receipt)
    original = workspace / "stockagent/live/example.py"
    frozen = receipt.parent / "source/stockagent/live/example.py"
    assert original.stat().st_ino != frozen.stat().st_ino
    original.write_text("# changed research code\n")
    original.with_name("new_module.py").write_text("# new development import\n")
    assert release.verify_frozen_source(receipt, workspace) == proof
    assert release.frozen_generation(receipt) == generation
    assert frozen.read_text() == "# original source\n"
    assert not (receipt.parent / "source/.env").exists()
    bindings = release.mount_bindings(receipt, workspace)
    assert {target.relative_to(workspace).as_posix() for _, target, ro in bindings if ro} == {
        "stockagent", "downloader", "scripts", release.SERVICE_DIR, release.SERVICE_DIR + "/.env"}
    assert (receipt.parent / "source/services/discord_bot/.env").read_bytes() == b""
    assert all(not any(part.startswith("data_") for part in target.relative_to(workspace).parts)
               for _, target, _ in bindings)


def test_explicit_overlay_preserves_compatible_base_and_excludes_unrelated_research(workspace):
    base = prepare(workspace)
    base_proof = release.verify_frozen_source(base, workspace)
    (workspace / "stockagent/live/example.py").write_text("# reviewed output formatting\n")
    (workspace / "downloader/example.py").write_text("# unrelated incompatible research\n")
    (workspace / "stockagent/new_unselected.py").write_text("# must not silently ship\n")
    (workspace / "stockagent/live/new_selected.py").write_text("# reviewed new helper\n")
    selected = ("stockagent/live/example.py", "stockagent/live/new_selected.py")
    derived = release.freeze_source(workspace, workspace / "artifacts/operations/service_code_releases",
                                    base_receipt=base, include_sources=selected)
    release.verify_frozen_source(derived, workspace)
    assert release.verify_frozen_source(base, workspace) == base_proof
    source = derived.parent / "source"
    assert (source / selected[0]).read_text() == "# reviewed output formatting\n"
    assert (source / selected[1]).read_text() == "# reviewed new helper\n"
    assert (source / "downloader/example.py").read_text() == "# original source\n"
    assert not (source / "stockagent/new_unselected.py").exists()
    proof = json.loads(derived.read_bytes())
    assert proof["source_overlay"]["included_sources"] == sorted(selected)
    assert proof["source_overlay"]["base_receipt"] == str(base)
    assert not proof["source_overlay"]["unselected_workspace_changes_promoted"]
    base_files = json.loads(base.read_bytes())["code"]["files"]
    assert all(proof["code"]["files"][name] == digest for name, digest in base_files.items() if name not in selected)


def test_reviewed_copy_overlay_excludes_dirty_edits_inside_selected_module(workspace):
    base = prepare(workspace)
    name = "stockagent/live/example.py"
    (workspace / name).write_text("# unrelated accounting change in same module\n")
    reviewed = workspace / "artifacts/operations/reviewed"
    target = reviewed / name
    target.parent.mkdir(parents=True)
    target.write_text("# original source with reviewed exact performance change\n")
    receipt = release.freeze_source(workspace, workspace / "artifacts/operations/service_code_releases",
                                    base_receipt=base, include_sources=(name,), include_root=reviewed)
    release.verify_frozen_source(receipt, workspace)
    assert (receipt.parent / "source" / name).read_bytes() == target.read_bytes()
    assert (workspace / name).read_text() == "# unrelated accounting change in same module\n"
    proof = json.loads(receipt.read_bytes())
    assert proof["source_overlay"]["included_source_root"] == str(reviewed)
    original = json.loads(base.read_bytes())["code"]["files"]
    assert all(proof["code"]["files"][key] == value for key, value in original.items() if key != name)


@pytest.mark.parametrize("kind", ["external", "redirected", "missing-member", "symlink-member", "redirected-parent"])
def test_reviewed_source_root_refuses_outside_or_redirected_inputs(workspace, kind):
    base = prepare(workspace)
    reviewed = workspace / "artifacts/operations/reviewed"
    reviewed.mkdir(parents=True)
    name = "stockagent/live/example.py"
    if kind == "external":
        reviewed = workspace.parent
    elif kind == "redirected":
        alias = reviewed.with_name("alias")
        alias.symlink_to(reviewed, target_is_directory=True)
        reviewed = alias
    elif kind == "symlink-member":
        target = reviewed / name
        target.parent.mkdir(parents=True)
        target.symlink_to(workspace / name)
    elif kind == "redirected-parent":
        (reviewed / "stockagent").mkdir()
        (reviewed / "stockagent/live").symlink_to(workspace / "stockagent/live", target_is_directory=True)
    with pytest.raises((ValueError, FileNotFoundError, OSError)):
        release.freeze_source(workspace, workspace / "artifacts/operations/service_code_releases",
                              base_receipt=base, include_sources=(name,), include_root=reviewed)


@pytest.mark.parametrize("selected", ((), (".env",), ("configs/private.yaml",),
    ("/stockagent/live/example.py",), ("stockagent/../downloader/example.py",),
    ("stockagent/live/example.py", "stockagent/live/example.py")))
def test_overlay_rejects_implicit_private_outside_or_duplicate_members(workspace, selected):
    base = prepare(workspace)
    with pytest.raises(ValueError, match="canonical source"):
        release.freeze_source(workspace, workspace / "artifacts/operations/service_code_releases",
                              base_receipt=base, include_sources=selected)


def test_overlay_requires_base_and_refuses_changed_base(workspace):
    with pytest.raises(ValueError, match="verified base"):
        release.freeze_source(workspace, workspace / "artifacts/operations/service_code_releases",
                              include_sources=("stockagent/live/example.py",))
    base = prepare(workspace)
    target = base.parent / "source/stockagent/live/example.py"
    target.chmod(0o644)
    target.write_text("# corrupted supposedly immutable source\n")
    target.chmod(0o444)
    with pytest.raises((ValueError, RuntimeError)):
        release.freeze_source(workspace, workspace / "artifacts/operations/service_code_releases",
                              base_receipt=base, include_sources=("stockagent/live/example.py",))


def test_overlay_refuses_selected_symlink(workspace):
    base = prepare(workspace)
    target = workspace / "stockagent/live/alias.py"
    target.symlink_to(workspace / "stockagent/live/example.py")
    with pytest.raises(ValueError, match="regular"):
        release.freeze_source(workspace, workspace / "artifacts/operations/service_code_releases",
                              base_receipt=base, include_sources=("stockagent/live/alias.py",))


def test_overlay_rejects_selected_source_changed_during_copy(workspace, monkeypatch):
    base = prepare(workspace)
    target = workspace / "stockagent/live/example.py"
    original = release._read_source_bytes
    def mutate(path):
        body = original(path)
        if path == target:
            target.write_text("# changed after first byte snapshot\n")
        return body
    monkeypatch.setattr(release, "_read_source_bytes", mutate)
    with pytest.raises(RuntimeError, match="changed during freeze"):
        release.freeze_source(workspace, workspace / "artifacts/operations/service_code_releases",
                              base_receipt=base, include_sources=("stockagent/live/example.py",))


def test_freeze_retains_every_file_and_child_directory_durable_barrier(workspace, monkeypatch):
    import stat
    observed = []
    actual = os.fsync

    def record(descriptor):
        observed.append((Path(os.readlink(f"/proc/self/fd/{descriptor}")), os.fstat(descriptor)))
        return actual(descriptor)

    monkeypatch.setattr(release.os, "fsync", record)
    receipt = prepare(workspace)
    source = receipt.parent / "source"
    manifest = json.loads(receipt.read_bytes())
    for name in (*manifest["code"]["files"], "services/discord_bot/.env"):
        assert any(path == source / name and stat.S_ISREG(info.st_mode) and not info.st_mode & 0o222
                   for path, info in observed), name
    for child in (p for p in source.rglob("*") if p.is_dir()):
        child_index = next(index for index, (p, _) in enumerate(observed) if p == child)
        parent_index = next(index for index, (p, _) in enumerate(observed) if p == child.parent)
        assert child_index < parent_index
    assert any(path == receipt and not info.st_mode & 0o222 for path, info in observed)
    assert observed[-1][0] == receipt.parent.parent


def test_failed_member_fsync_never_returns_a_complete_release(workspace, monkeypatch):
    actual = os.fsync

    def fail(descriptor):
        path = Path(os.readlink(f"/proc/self/fd/{descriptor}"))
        if str(path).endswith("/source/stockagent/live/example.py"):
            raise OSError("injected member fsync failure")
        return actual(descriptor)

    monkeypatch.setattr(release.os, "fsync", fail)
    with pytest.raises(OSError, match="member fsync failure"):
        prepare(workspace)
    assert not list((workspace / "artifacts/operations/service_code_releases").rglob("release.json"))


@pytest.mark.parametrize("kind", ["oversize", "fifo", "symlink"])
def test_source_copy_rejects_unbounded_or_special_members_without_blocking(tmp_path, kind):
    path = tmp_path / "member"
    if kind == "oversize":
        with path.open("wb") as stream:
            stream.truncate(2 * 1024**2 + 1)
    elif kind == "fifo":
        os.mkfifo(path)
    else:
        path.symlink_to(tmp_path / "target")
    with pytest.raises((ValueError, OSError), match="bounded|symbolic|No such|Too many"):
        release._read_source_bytes(path)


def test_namespace_probe_uses_same_verified_absolute_paths_for_relative_inputs(workspace, monkeypatch):
    receipt = prepare(workspace)
    monkeypatch.chdir(workspace.parent)
    calls = []
    def run(command, **kwargs):
        calls.append((command, kwargs))
        return subprocess.CompletedProcess(command, 0, stdout="{}", stderr="")
    monkeypatch.setattr(release.subprocess, "run", run)
    release.run_in_namespace(receipt.relative_to(workspace.parent), Path(workspace.name), ["-c", "pass"])
    command, kwargs = calls[0]
    assert kwargs["cwd"] == workspace
    assert "--property=WorkingDirectory=" + str(workspace) in command
    assert "--property=Environment=" + release.ENVIRONMENT + "=" + str(receipt) in command
    for property_ in command:
        if property_.startswith(("--property=BindReadOnlyPaths=", "--property=BindPaths=")):
            for pair in property_.split("=", 2)[2].split():
                assert all(Path(path).is_absolute() for path in pair.split(":"))


def test_relative_namespace_receipt_still_rejects_symlinks_before_building_mounts(workspace, monkeypatch):
    receipt = prepare(workspace)
    alias = receipt.parent / "alias.json"
    receipt.parent.chmod(0o755)
    alias.symlink_to(receipt)
    receipt.parent.chmod(0o555)
    monkeypatch.chdir(workspace.parent)
    monkeypatch.setattr(release.subprocess, "run", lambda *a, **k: pytest.fail("no unverified namespace"))
    with pytest.raises(ValueError, match="escapes"):
        release.run_in_namespace(alias.relative_to(workspace.parent), Path(workspace.name), ["-c", "pass"])


def test_temporary_lakehouse_measurement_retains_runtime_path_and_native_resource_gate(workspace, monkeypatch):
    receipt = prepare(workspace)
    runtime_path = "/usr/lib/wsl/lib:/runtime tools/bin:/usr/bin"
    monkeypatch.setenv("PATH", runtime_path)
    monkeypatch.setenv("BENCHMARK_SECRET", "do-not-copy-to-probe")
    calls = []
    def run(command, **kwargs):
        calls.append(command)
        return subprocess.CompletedProcess(command, 0, stdout="{}", stderr="")
    monkeypatch.setattr(release.subprocess, "run", run)
    release.run_in_namespace(receipt, workspace, ["-c", "pass"], measurement_role="lakehouse")
    assert len(calls) == 1
    command = calls[0]
    assert "--setenv=PATH=" + runtime_path in command
    assert any(arg.startswith("--unit=stockagent-agent-lakehouse-source-wave-probe-") for arg in command)
    assert not any("BENCHMARK_SECRET" in arg or "do-not-copy-to-probe" in arg for arg in command)
    assert "--property=PrivateMounts=yes" in command
    with pytest.raises(ValueError, match="measurement role"):
        release.run_in_namespace(receipt, workspace, ["-c", "pass"], measurement_role="production")
    assert len(calls) == 1


def _probe_inputs(workspace):
    for name in ("test/test_example.py", "services/public_dashboards/app.js"):
        path = workspace / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("# verification snapshot\n")
    return release.freeze_probe_inputs(workspace, roots=("test", "services/public_dashboards"))


def test_verification_inputs_pin_tests_assets_but_never_native_mounts(workspace, monkeypatch):
    code = prepare(workspace)
    receipt = _probe_inputs(workspace)
    proof = release.verify_probe_inputs(receipt, workspace)
    original = workspace / "test/test_example.py"
    original.write_text("# concurrent next version\n")
    (original.parent / "new_test.py").write_text("# next version\n")
    assert release.verify_probe_inputs(receipt, workspace) == proof
    assert (receipt.parent / "source/test/test_example.py").stat().st_ino != original.stat().st_ino
    assert "SECRET" not in str(proof)
    assert not any(target == workspace / "test" for _, target, _ in release.mount_bindings(code, workspace))
    calls = []
    def run(command, **kwargs):
        calls.append(command)
        return subprocess.CompletedProcess(command, 0, stdout="{}", stderr="")
    monkeypatch.setattr(release.subprocess, "run", run)
    release.run_in_namespace(code, workspace, ["-c", "pass"], verification_inputs=receipt)
    readonly = next(arg for arg in calls[0] if arg.startswith("--property=BindReadOnlyPaths="))
    assert f"{receipt.parent}/source/test:{workspace}/test" in readonly
    assert f"{receipt.parent}/source/services/public_dashboards:{workspace}/services/public_dashboards" in readonly
    assert str(workspace / "configs") not in readonly


@pytest.mark.parametrize("roots", [(), ("test", "test"), ("stockagent",), ("services/discord_bot",),
                                   ("test/../configs",), (".env",)])
def test_verification_capsule_cannot_mask_code_credentials_or_live_inputs(workspace, roots):
    with pytest.raises(ValueError, match="verification input roots"):
        release.freeze_probe_inputs(workspace, roots=roots)


@pytest.mark.parametrize("change", ["contents", "extra", "missing", "writable", "symlink", "hardlink"])
def test_verification_input_changes_fail_closed(workspace, change):
    receipt = _probe_inputs(workspace)
    target = receipt.parent / "source/test/test_example.py"
    target.parent.chmod(0o755)
    if change == "contents":
        target.chmod(0o644)
        target.write_text("# corrupt\n")
        target.chmod(0o444)
    elif change == "extra":
        extra = target.with_name("unexpected.py")
        extra.write_text("# unrecorded\n")
        extra.chmod(0o444)
    elif change == "missing":
        target.unlink()
    elif change == "writable":
        target.chmod(0o644)
    elif change == "symlink":
        target.unlink()
        target.symlink_to(workspace / "test/test_example.py")
    else:
        target.with_name("alias.py").hardlink_to(target)
    target.parent.chmod(0o555)
    with pytest.raises((ValueError, FileNotFoundError)):
        release.verify_probe_inputs(receipt, workspace)


def test_verification_capsule_detects_concurrent_workspace_change(workspace, monkeypatch):
    path = workspace / "test/test_example.py"
    path.parent.mkdir()
    path.write_text("# first\n")
    read = Path.read_bytes
    def mutate(target):
        body = read(target)
        if target == path:
            target.write_text("# second\n")
        return body
    monkeypatch.setattr(Path, "read_bytes", mutate)
    with pytest.raises(RuntimeError, match="changed during freeze"):
        release.freeze_probe_inputs(workspace, roots=("test",))


def test_explicit_probe_members_ignore_unrelated_churn_not_selected_changes(workspace, monkeypatch):
    selected = workspace / "test/test_selected.py"
    other = workspace / "test/test_unrelated.py"
    selected.parent.mkdir()
    selected.write_text("# selected dependency\n")
    other.write_text("# unrelated worker\n")
    read = Path.read_bytes
    def change_unrelated(path):
        if path == other:
            raise AssertionError("unrelated file must not enter this verification read")
        body = read(path)
        if path == selected:
            other.write_text("# concurrent unrelated next revision\n")
        return body
    monkeypatch.setattr(Path, "read_bytes", change_unrelated)
    receipt = release.freeze_probe_inputs(workspace, roots=("test",),
                                         include_files=("test/test_selected.py",))
    proof = release.verify_probe_inputs(receipt, workspace)
    assert list(proof["files"]) == ["test/test_selected.py"]
    assert proof["selection"]["files"] == ["test/test_selected.py"]
    assert not (receipt.parent / "source/test/test_unrelated.py").exists()
    monkeypatch.setattr(Path, "read_bytes", read)
    def change_selected(path):
        body = read(path)
        if path == selected:
            selected.write_text("# changed selected dependency\n")
        return body
    monkeypatch.setattr(Path, "read_bytes", change_selected)
    with pytest.raises(RuntimeError, match="changed during freeze"):
        release.freeze_probe_inputs(workspace, roots=("test",), include_files=("test/test_selected.py",))


@pytest.mark.parametrize("members", [(), ("test/missing.py",), (".env",),
                                    ("test/../.env",), ("/test/x.py",),
                                    ("test/x.py", "test/x.py")])
def test_explicit_probe_members_reject_missing_private_or_aliased_scope(workspace, members):
    (workspace / "test").mkdir()
    (workspace / "test/x.py").write_text("# selected\n")
    with pytest.raises(ValueError, match="verification input"):
        release.freeze_probe_inputs(workspace, roots=("test",), include_files=members)


def test_verification_capsule_refuses_workspace_symlink(workspace):
    path = workspace / "test/test_alias.py"
    path.parent.mkdir()
    path.symlink_to(workspace / "stockagent/live/example.py")
    with pytest.raises(ValueError, match="regular bounded"):
        release.freeze_probe_inputs(workspace, roots=("test",))


def test_namespace_probe_rechecks_verification_inputs_after_execution(workspace, monkeypatch):
    code, receipt = prepare(workspace), _probe_inputs(workspace)
    target = receipt.parent / "source/test/test_example.py"
    def run(command, **kwargs):
        target.chmod(0o644)
        target.write_text("# changed during acceptance\n")
        target.chmod(0o444)
        return subprocess.CompletedProcess(command, 0, stdout="{}", stderr="")
    monkeypatch.setattr(release.subprocess, "run", run)
    with pytest.raises(ValueError, match="contents differ"):
        release.run_in_namespace(code, workspace, ["-c", "pass"], verification_inputs=receipt)


@pytest.mark.parametrize("change", ["contents", "added", "deleted", "writable", "symlink", "hardlink", "bundle"])
def test_actual_pinned_source_drift_still_fails_closed(workspace, change):
    receipt = prepare(workspace)
    source = receipt.parent / "source"
    target = source / "stockagent/live/example.py"
    before = release.frozen_generation(receipt)
    if change == "contents":
        target.chmod(0o644)
        target.write_text("# actual frozen corruption\n")
        target.chmod(0o444)
    elif change == "added":
        target.with_name("unexpected.py").write_text("# unrecorded lazy import\n")
    elif change == "deleted":
        target.unlink()
    elif change == "writable":
        target.chmod(0o644)
    elif change == "symlink":
        target.unlink()
        target.symlink_to(workspace / "stockagent/live/example.py")
    elif change == "hardlink":
        target.with_name("alias.py").hardlink_to(target)
    else:
        bundle = receipt.parent / "source.zip"
        bundle.chmod(0o644)
        bundle.write_bytes(b"broken recovery archive")
        bundle.chmod(0o444)
    assert release.frozen_generation(receipt) != before
    with pytest.raises((ValueError, FileNotFoundError)):
        release.verify_frozen_source(receipt, workspace)


def test_namespace_dropin_is_exact_and_keeps_noncode_paths_live(workspace, tmp_path):
    receipt = prepare(workspace)
    dropin = tmp_path / "code.conf"
    body = release.render_drop_in(receipt, workspace)
    dropin.write_text(body)
    assert release.configured_release(workspace, dropin) == receipt
    assert "PrivateMounts=yes" in body and "BindReadOnlyPaths=" in body
    assert "PYTHONDONTWRITEBYTECODE=1" in body
    assert "ReadWritePaths" not in body and "SECRET=" not in body
    # Quoting the complete source:target hides ':' from systemd's separator
    # parser even though systemd-analyze verify considers the syntax valid.
    assert all('="' not in line for line in body.splitlines() if line.startswith("Bind"))
    dropin.write_text(body + "BindPaths=/unsafe:/source\n")
    with pytest.raises(ValueError, match="modified"):
        release.configured_release(workspace, dropin)


@pytest.mark.parametrize("target", ["escape", "mutable", "namespace"])
def test_release_admission_rejects_unverified_paths_and_metadata(workspace, tmp_path, target):
    receipt = prepare(workspace)
    if target == "escape":
        with pytest.raises(ValueError, match="escapes"):
            release.verify_frozen_source(tmp_path / "release.json", workspace)
    elif target == "mutable":
        receipt.chmod(0o644)
        with pytest.raises(ValueError, match="mutable"):
            release.verify_frozen_source(receipt, workspace)
    else:
        value = json.loads(receipt.read_bytes())
        value["service_namespace"]["directories"].append("artifacts")
        receipt.chmod(0o644)
        receipt.write_text(json.dumps(value))
        receipt.chmod(0o444)
        with pytest.raises(ValueError, match="namespace"):
            release.verify_frozen_source(receipt, workspace)


def test_no_untracked_credentials_or_symlinked_sources_enter_release(workspace):
    (workspace / "stockagent/.env").write_text("secret\n")
    assert "stockagent/.env" not in release.source_members(workspace)
    target = workspace / "stockagent/live/unsafe.py"
    target.symlink_to(workspace / "stockagent/live/example.py")
    with pytest.raises(ValueError, match="regular"):
        prepare(workspace)


def test_cutover_namespace_flag_cannot_apply_to_execution_owner(monkeypatch):
    from scripts import reload_tw_day_trade_services_if_flat as reload
    monkeypatch.setattr("sys.argv", ["reload", "--engine-only", "--source-release", "release.json"])
    with pytest.raises(SystemExit):
        reload.main()


@pytest.mark.parametrize("existing", [False, True])
@pytest.mark.parametrize("failure", [None, "validation", "daemon_reload", "parsed_bindings"])
@pytest.mark.parametrize("role", ["discord", "engine"])
def test_install_validates_replacement_and_restores_only_own_config(workspace, tmp_path, monkeypatch, existing, failure, role):
    from types import SimpleNamespace
    receipt = prepare(workspace)
    dropin = tmp_path / "unit.service.d/stockagent-code-release.conf"
    old_body = None
    if existing:
        old = prepare(workspace)
        old_body = release.render_drop_in(old, workspace, role=role)
        dropin.parent.mkdir()
        dropin.write_text(old_body)
    external = "[Service]\nEnvironment=UNRELATED=preserved\n"
    selected = release._owner(role)[0]
    unit = f"# /etc/systemd/system/{selected}\n[Service]\nExecStart=/bin/true\n"
    if existing:
        unit += f"\n# {dropin}\n" + old_body
    unit += "\n# /etc/systemd/system/stockagent-discord-bot.service.d/other.conf\n" + external
    calls = []
    def run(command, **kwargs):
        calls.append(command)
        if command[:2] == ["systemctl", "cat"]:
            assert command[-1] == selected
            return SimpleNamespace(stdout=unit)
        if command[:2] == ["systemd-analyze", "verify"]:
            preview = Path(command[2]).read_text()
            assert external in preview
            assert preview.count("STOCKAGENT_SERVICE_CODE_RELEASE=") == 1
            assert Path(command[2]).name == selected
            assert release.render_drop_in(receipt, workspace, role=role) in preview
            if failure == "validation":
                raise subprocess.CalledProcessError(1, command)
        elif command == ["systemctl", "daemon-reload"]:
            if failure == "daemon_reload" and calls.count(command) == 1:
                raise subprocess.CalledProcessError(1, command)
        elif command[0] == "busctl":
            assert command[5] == "/org/freedesktop/systemd1/unit/" + release._owner(role)[1]
            readonly = command[-1] == "BindReadOnlyPaths"
            rows = [[str(a), str(b), False, 16384] for a, b, ro in release.mount_bindings(receipt, workspace) if ro == readonly]
            if failure == "parsed_bindings":
                rows = []
            return SimpleNamespace(stdout=json.dumps({"type": "a(ssbt)", "data": rows}))
        else:
            pytest.fail(f"unexpected service operation: {command}")
        return SimpleNamespace(returncode=0)
    monkeypatch.setattr(release.subprocess, "run", run)
    if failure:
        with pytest.raises(ValueError if failure == "parsed_bindings" else subprocess.CalledProcessError):
            release.install_drop_in(receipt, workspace, dropin, role=role)
        assert (dropin.read_text() if dropin.exists() else None) == old_body
    else:
        release.install_drop_in(receipt, workspace, dropin, role=role)
        assert dropin.read_text() == release.render_drop_in(receipt, workspace, role=role)
    assert not any("restart" in command for command in calls)


def test_unknown_dropin_is_not_overwritten(workspace, tmp_path, monkeypatch):
    receipt = prepare(workspace)
    dropin = tmp_path / "unknown.conf"
    dropin.write_text("[Service]\nEnvironment=OPERATOR=preserve\n")
    monkeypatch.setattr(release.subprocess, "run", lambda *a, **kw: pytest.fail("unknown drop-in reached systemctl"))
    with pytest.raises(ValueError, match="unknown"):
        release.install_drop_in(receipt, workspace, dropin)
    assert "OPERATOR=preserve" in dropin.read_text()


@pytest.mark.parametrize("kind", ["broken_link", "valid_link", "oversize"])
def test_namespace_control_never_follows_a_link_or_unbounded_file(workspace, tmp_path, kind):
    receipt = prepare(workspace)
    target, dropin = tmp_path / "target.conf", tmp_path / "pin.conf"
    if kind == "oversize":
        dropin.write_text("x" * (64 * 1024 + 1))
    else:
        if kind == "valid_link":
            target.write_text(release.render_drop_in(receipt, workspace))
        dropin.symlink_to(target)
    with pytest.raises(ValueError, match="unknown service"):
        release.configured_release(workspace, dropin)


@pytest.mark.parametrize("fault", [None, "quoted", "missing", "shadow", "readonly", "optional", "schema"])
def test_parsed_manager_readback_rejects_semantically_wrong_mounts(workspace, monkeypatch, fault):
    from types import SimpleNamespace
    receipt = prepare(workspace)
    def read(command, **kwargs):
        readonly = command[-1] == "BindReadOnlyPaths"
        rows = [[str(a), str(b), False, 16384] for a, b, ro in release.mount_bindings(receipt, workspace) if ro == readonly]
        if readonly:
            if fault == "quoted":
                rows[0][:2] = [rows[0][0] + ":" + rows[0][1]] * 2
            elif fault == "missing":
                rows.pop()
            elif fault == "shadow":
                rows.append(["/unsafe", str(workspace / "stockagent/live"), False, 0])
            elif fault == "readonly":
                rows.append([str(workspace / release.SERVICE_DIR / "markets"),
                             str(workspace / release.SERVICE_DIR / "markets"), False, 0])
            elif fault == "optional":
                rows[0][2] = True
        return SimpleNamespace(stdout=json.dumps({"type": "wrong" if fault == "schema" else "a(ssbt)", "data": rows}))
    monkeypatch.setattr(release.subprocess, "run", read)
    if fault:
        with pytest.raises(ValueError):
            release.verify_unit_bindings(receipt, workspace)
    else:
        assert release.verify_unit_bindings(receipt, workspace)["verified_bindings"] == 8


def test_startup_verification_requires_native_owner_and_explicit_release(workspace, monkeypatch):
    monkeypatch.delenv(release.ENVIRONMENT, raising=False)
    with pytest.raises(ValueError, match="explicit release"):
        release.verify_startup_namespace(workspace)
    receipt = prepare(workspace)
    monkeypatch.setenv(release.ENVIRONMENT, str(receipt))
    # A readable manifest alone is not a native-service acceptance proof.
    with pytest.raises(ValueError, match="native Discord"):
        release.verify_startup_namespace(workspace)


@pytest.mark.parametrize("role", ["discord", "engine"])
@pytest.mark.parametrize("wrong", [False, True])
def test_native_role_membership_is_exact_not_a_unit_name_substring(workspace, monkeypatch, role, wrong):
    unit = release._owner(role)[0]
    cgroup = f"0::/system.slice/{unit}{'.not-the-owner' if wrong else ''}\n"
    read = Path.read_text
    monkeypatch.setattr(Path, "read_text", lambda path, *a, **kw:
                        cgroup if str(path) == "/proc/123/cgroup" else read(path, *a, **kw))
    calls = []
    monkeypatch.setattr(release, "verify_mounted_source", lambda *args: calls.append(args) or {"verified": True})
    if wrong:
        with pytest.raises(ValueError, match="native"):
            release.verify_owner_namespace(Path("release.json"), workspace, 123, role=role)
        assert not calls
    else:
        assert release.verify_owner_namespace(Path("release.json"), workspace, 123, role=role)["verified"]


def test_engine_and_discord_dropins_cannot_be_confused(workspace, tmp_path):
    receipt = prepare(workspace)
    dropin = tmp_path / "code.conf"
    dropin.write_text(release.render_drop_in(receipt, workspace))
    with pytest.raises(ValueError, match="modified"):
        release.configured_release(workspace, dropin, role="engine")
    dropin.write_text(release.render_drop_in(receipt, workspace, role="engine"))
    assert release.configured_release(workspace, dropin, role="engine") == receipt
    assert release._owner("engine")[2] != release._owner("discord")[2]
    with pytest.raises(ValueError, match="unknown pinned"):
        release.render_drop_in(receipt, workspace, role="public")


@pytest.mark.parametrize("fault", [None, "relative", "duplicate", "wrong_entry", "supervisor", "gone", "overflow"])
def test_native_pid_selects_only_unique_python_entrypoint(workspace, monkeypatch, fault):
    read_text, read_bytes, readlink = Path.read_text, Path.read_bytes, Path.readlink
    unit = release._owner("engine")[0]
    inventory = f"/sys/fs/cgroup/system.slice/{unit}/cgroup.procs"
    pids = "123 124" if fault == "duplicate" else "123"
    if fault == "overflow":
        pids = "123 " * 257
    def text(path, *a, **kw):
        return pids if str(path) == inventory else read_text(path, *a, **kw)
    def binary(path):
        if str(path) in {"/proc/123/cmdline", "/proc/124/cmdline"}:
            if fault == "gone":
                raise ProcessLookupError()
            entry = release.ENGINE_CODE_FILES[0]
            if fault == "wrong_entry":
                entry = release.ENTRYPOINT
            if fault != "relative":
                entry = str(workspace / entry)
            return b"python\0" + os.fsencode(entry) + b"\0"
        return read_bytes(path)
    def link(path):
        if str(path).startswith("/proc/"):
            return workspace if path.name == "cwd" else Path("/bin/sh" if fault == "supervisor" else "/bin/python3.11")
        return readlink(path)
    monkeypatch.setattr(Path, "read_text", text)
    monkeypatch.setattr(Path, "read_bytes", binary)
    monkeypatch.setattr(Path, "readlink", link)
    if fault not in {None, "relative"}:
        with pytest.raises(ValueError, match="inventory|exactly one"):
            release.native_owner_pid(workspace, role="engine")
    else:
        assert release.native_owner_pid(workspace, role="engine") == 123


@pytest.mark.parametrize("pin", [False, True])
def test_native_verifier_cli_uses_pinned_version_not_mutable_workspace(workspace, monkeypatch, capsys, pin):
    from types import SimpleNamespace
    receipt = workspace / "release.json"
    monkeypatch.delenv(release.ENVIRONMENT, raising=False)
    monkeypatch.setattr(release, "configured_release", lambda _: receipt if pin else None)
    script = workspace / "scripts/verifier.py"
    calls = []
    def run(*args, **kwargs):
        calls.append((args, kwargs))
        return SimpleNamespace(returncode=75, stdout="pinned verifier result\n", stderr="")
    monkeypatch.setattr(release, "run_in_namespace", run)
    result = release.run_cli_against_active_source(workspace, script, ["--control", "original.json"])
    assert result == (75 if pin else None)
    if pin:
        assert calls == [((receipt, workspace, [str(script), "--control", "original.json"]), {"timeout": 600})]
        assert capsys.readouterr().out == "pinned verifier result\n"
        # An environment label alone never establishes a pinned mount view.
        monkeypatch.setenv(release.ENVIRONMENT, str(receipt))
        monkeypatch.setattr(release, "verify_mounted_source", lambda *_: (_ for _ in ()).throw(ValueError("not mounted")))
        with pytest.raises(ValueError, match="not mounted"):
            release.run_cli_against_active_source(workspace, script, [])
    else:
        assert not calls


@pytest.mark.parametrize("pinned", [False, True])
def test_launcher_does_not_start_bot_after_failed_pinned_boot_check(workspace, pinned):
    launcher = Path(release.__file__).parents[2] / "scripts/run_discord_bot.sh"
    target = workspace / "scripts/run_discord_bot.sh"
    target.write_bytes(launcher.read_bytes())
    (workspace / ".env").write_text("# fixture; no credentials\n")
    (workspace / "services/discord_bot/.env").write_text("DISCORD_BOT_TOKEN=fixture\nDISCORD_CHANNEL_ID=fixture\n")
    (workspace / "scripts/runtime_env.sh").write_text("""run_fintech_python() {
  if [[ "$1" == "-c" ]]; then return 75; fi
  echo native-bot-would-start
}
""")
    environment = {"PATH": os.environ["PATH"]}
    if pinned:
        environment[release.ENVIRONMENT] = str(workspace / "receipt.json")
    result = subprocess.run(["bash", str(target)], cwd=workspace, env=environment,
                            capture_output=True, text=True)
    assert result.returncode == (75 if pinned else 0)
    assert ("native-bot-would-start" in result.stdout) is not pinned


@pytest.mark.parametrize("unscoped", [False, True])
def test_real_private_mount_survives_workspace_edits_and_is_readonly(workspace, unscoped):
    capability = subprocess.run(["unshare", "--mount", "--propagation", "private", "true"],
                                capture_output=True)
    if capability.returncode:
        pytest.skip("native mount namespace requires CAP_SYS_ADMIN; production preparation fails closed")
    runtime = Path(release.__file__).parents[1] / "runtime_identity.py"
    (workspace / "stockagent/runtime_identity.py").write_bytes(runtime.read_bytes())
    (workspace / "stockagent/live/service_code_release.py").write_bytes(Path(release.__file__).read_bytes())
    artifact_io = Path(release.__file__).parents[2] / "downloader/artifact_io.py"
    (workspace / "downloader/artifact_io.py").write_bytes(artifact_io.read_bytes())
    frozen_module = workspace / "stockagent/live/completed_inference_cache.py"
    body = """import hashlib
from pathlib import Path
def file_identity(path):
    return {'path':str(path),'sha256':hashlib.sha256(Path(path).read_bytes()).hexdigest()}
def implementation_identity():
    root=Path(__file__).resolve().parents[2]
    return [file_identity(root/'stockagent/live/example.py')]
"""
    if unscoped:
        (workspace / "outside.py").write_text("# mutable unpinned inference dependency\n")
        body = body.replace("stockagent/live/example.py", "outside.py")
    frozen_module.write_text(body)
    receipt = prepare(workspace)
    if unscoped:
        with pytest.raises(RuntimeError, match="dependency is not pinned"):
            release.namespace_identity(receipt, workspace)
        return
    before = release.namespace_identity(receipt, workspace)
    assert before["readonly_verified"] is True
    (workspace / "stockagent/live/example.py").write_text("# edited outside namespace\n")
    replacement = workspace / "services/discord_bot/replacement.py"
    replacement.write_text("# atomically replaced outside namespace\n")
    replacement.replace(workspace / release.ENTRYPOINT)
    assert release.namespace_identity(receipt, workspace) == before
    assert (workspace / "stockagent/live/example.py").read_text() == "# edited outside namespace\n"


def test_running_namespace_survives_in_place_and_atomic_workspace_edits(workspace):
    from concurrent.futures import ThreadPoolExecutor
    import time
    capability = subprocess.run(["unshare", "--mount", "--propagation", "private", "true"],
                                capture_output=True)
    if capability.returncode:
        pytest.skip("native mount namespace requires CAP_SYS_ADMIN")
    repo = Path(release.__file__).parents[2]
    for name in ("stockagent/live/service_code_release.py", "stockagent/runtime_identity.py", "downloader/artifact_io.py"):
        (workspace / name).write_bytes((repo / name).read_bytes())
    receipt = prepare(workspace)
    control = workspace / "test_control"
    control.mkdir()
    probe = """import errno,hashlib,json,os,time
from pathlib import Path
from unittest.mock import patch
from stockagent.live.service_code_release import verify_startup_namespace
root=Path.cwd()
read_text=Path.read_text
def cgroup_only(path,*a,**kw):
    if str(path)==f'/proc/{os.getpid()}/cgroup': return '0::/system.slice/stockagent-discord-bot.service\\n'
    return read_text(path,*a,**kw)
with patch.object(Path,'read_text',cgroup_only):
    assert verify_startup_namespace(root)['readonly_mounts_verified']
names=['stockagent/live/example.py','services/discord_bot/bot.py']
def identity():
    return {p:hashlib.sha256((root/p).read_bytes()).hexdigest() for p in names}
before=identity()
(root/'test_control/ready').touch()
deadline=time.monotonic()+15
while not (root/'test_control/continue').exists():
    if time.monotonic()>deadline: raise TimeoutError('workspace edit handshake')
    time.sleep(.01)
assert before==identity(), 'resident mount changed after external edit'
for name in names:
    try: (root/name).open('ab').close()
    except OSError as exc: assert exc.errno==errno.EROFS
    else: raise RuntimeError('source is writable')
assert (root/'services/discord_bot/markets/live.yaml').read_text()=='version: updated\\n'
(root/'services/discord_bot/markets/from_service.yaml').write_text('live write preserved\\n')
print(json.dumps({'unchanged':True,'readonly':True}))
"""
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(release.run_in_namespace, receipt, workspace, ["-c", probe], timeout=20)
        deadline = time.monotonic() + 15
        while not (control / "ready").exists():
            if future.done():
                pytest.fail(f"namespace exited before edit: {future.result().stderr}")
            if time.monotonic() > deadline:
                pytest.fail("namespace did not reach workspace edit handshake")
            time.sleep(.01)
        try:
            (workspace / "stockagent/live/example.py").write_text("# in-place external edit\n")
            replacement = workspace / "services/discord_bot/replacement.py"
            replacement.write_text("# external atomic replacement\n")
            replacement.replace(workspace / release.ENTRYPOINT)
            market = workspace / "services/discord_bot/markets/live.yaml"
            replacement = market.with_name("replacement.yaml")
            replacement.write_text("version: updated\n")
            replacement.replace(market)
        finally:
            (control / "continue").touch()
        result = future.result(timeout=20)
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == {"unchanged": True, "readonly": True}
    assert (workspace / "services/discord_bot/markets/from_service.yaml").read_text() == "live write preserved\n"

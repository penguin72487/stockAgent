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
def test_install_validates_replacement_and_restores_only_own_config(workspace, tmp_path, monkeypatch, existing, failure):
    from types import SimpleNamespace
    receipt = prepare(workspace)
    dropin = tmp_path / "unit.service.d/stockagent-code-release.conf"
    old_body = None
    if existing:
        old = prepare(workspace)
        old_body = release.render_drop_in(old, workspace)
        dropin.parent.mkdir()
        dropin.write_text(old_body)
    external = "[Service]\nEnvironment=UNRELATED=preserved\n"
    unit = "# /etc/systemd/system/stockagent-discord-bot.service\n[Service]\nExecStart=/bin/true\n"
    if existing:
        unit += f"\n# {dropin}\n" + old_body
    unit += "\n# /etc/systemd/system/stockagent-discord-bot.service.d/other.conf\n" + external
    calls = []
    def run(command, **kwargs):
        calls.append(command)
        if command[:2] == ["systemctl", "cat"]:
            return SimpleNamespace(stdout=unit)
        if command[:2] == ["systemd-analyze", "verify"]:
            preview = Path(command[2]).read_text()
            assert external in preview
            assert preview.count("STOCKAGENT_SERVICE_CODE_RELEASE=") == 1
            assert release.render_drop_in(receipt, workspace) in preview
            if failure == "validation":
                raise subprocess.CalledProcessError(1, command)
        elif command == ["systemctl", "daemon-reload"]:
            if failure == "daemon_reload" and calls.count(command) == 1:
                raise subprocess.CalledProcessError(1, command)
        elif command[0] == "busctl":
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
            release.install_drop_in(receipt, workspace, dropin)
        assert (dropin.read_text() if dropin.exists() else None) == old_body
    else:
        release.install_drop_in(receipt, workspace, dropin)
        assert dropin.read_text() == release.render_drop_in(receipt, workspace)
    assert not any("restart" in command for command in calls)


def test_unknown_dropin_is_not_overwritten(workspace, tmp_path, monkeypatch):
    receipt = prepare(workspace)
    dropin = tmp_path / "unknown.conf"
    dropin.write_text("[Service]\nEnvironment=OPERATOR=preserve\n")
    monkeypatch.setattr(release.subprocess, "run", lambda *a, **kw: pytest.fail("unknown drop-in reached systemctl"))
    with pytest.raises(ValueError, match="unknown"):
        release.install_drop_in(receipt, workspace, dropin)
    assert "OPERATOR=preserve" in dropin.read_text()


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
    # This pytest process is not the native service: a readable manifest alone
    # cannot pass the cold-start gate or masquerade as deployed acceptance.
    with pytest.raises(ValueError, match="native Discord"):
        release.verify_startup_namespace(workspace)


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

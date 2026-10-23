"""Pinned source namespaces for a live owner, not a second inference pipeline.

Keep canonical absolute paths, data, credentials and configs. Only code is
copied (never hard-linked) and bind-mounted read-only by systemd. In-process
implementation admission remains unchanged and observes the pinned files.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import sys
import tempfile
import time
import uuid
import zipfile

from downloader.artifact_io import atomic_write_json, atomic_write_text
from stockagent.runtime_identity import identity_sha256, stable_source_sha256, verify_source_release

CODE_DIRS = ("stockagent", "downloader", "scripts")
ENTRYPOINT = "services/discord_bot/bot.py"
SERVICE_DIR = "services/discord_bot"
LIVE_INPUTS = ((".env", "file", True), ("markets", "directory", False),
               ("models", "directory", False), ("staged_accounts", "directory", False))
DROP_IN = Path("/etc/systemd/system/stockagent-discord-bot.service.d/stockagent-code-release.conf")
ENVIRONMENT = "STOCKAGENT_SERVICE_CODE_RELEASE"


def source_members(root: Path) -> list[str]:
    result = subprocess.run(["git", "ls-files", "--cached", "--others", "--exclude-standard", "-z",
                             "--", *CODE_DIRS, f"{SERVICE_DIR}/*.py"], cwd=root, check=True, capture_output=True)
    names = sorted(set(result.stdout.decode().split("\0")) - {""})
    # Never copy ignored credentials, caches, downloads, checkpoints or artifacts.
    for name in names:
        if (any(part.startswith(".") or part == "__pycache__" for part in Path(name).parts)
                or name.endswith((".pyc", ".pyo"))):
            raise ValueError(f"non-source member in code inventory: {name}")
        info = (root / name).lstat()
        if not stat.S_ISREG(info.st_mode) or info.st_size > 2 * 1024**2:
            raise ValueError(f"non-regular/oversized source member: {name}")
    if ENTRYPOINT not in names or not all(any(n.startswith(d + "/") for n in names) for d in CODE_DIRS):
        raise ValueError("complete canonical source roots required")
    return names


def namespace_contract(root: Path) -> dict:
    return {"contract": "canonical_path_readonly_code_namespace_v3", "workspace_root": str(root),
            "directories": [*CODE_DIRS, SERVICE_DIR], "entrypoint": ENTRYPOINT,
            "live_inputs": [list(item) for item in LIVE_INPUTS],
            "live_input_source": "pid1_namespace_canonical_paths_v1",
            "mutable_data_and_configs_preserved": True}


def freeze_source(root: Path, output: Path) -> Path:
    """Freeze one stable tree using the existing source-release verifier."""
    root, output = root.resolve(), output.resolve()
    allowed = root / "artifacts/operations/service_code_releases"
    if output != allowed:
        raise ValueError("service code releases belong in the private canonical release root")
    names = source_members(root)
    before = {name: stable_source_sha256(root, name) for name in names}
    if sum((root / name).stat().st_size for name in names) > 128 * 1024**2:
        raise ValueError("code release exceeds bounded source size")
    release = output / (time.strftime("%Y%m%dT%H%M%SZ", time.gmtime()) + "-" + uuid.uuid4().hex[:12])
    source = release / "source"
    source.mkdir(parents=True, mode=0o700)
    # Pin the entire entrypoint directory: a bind of bot.py alone can be
    # bypassed by an external atomic rename of the original target dentry.
    # Empty targets hold no credentials/config copies. Native nested mounts
    # reconnect the original live inputs after the code-directory mount.
    for name, kind, _readonly in LIVE_INPUTS:
        original = root / SERVICE_DIR / name
        if original.is_symlink() or (kind == "file" and not original.is_file()) or (kind == "directory" and not original.is_dir()):
            raise ValueError(f"regular live service input required: {name}")
        target = source / SERVICE_DIR / name
        target.parent.mkdir(parents=True, exist_ok=True)
        if kind == "file":
            target.touch(mode=0o444)
        else:
            target.mkdir(mode=0o555)
    for name in names:
        body = (root / name).read_bytes()
        if hashlib.sha256(body).hexdigest() != before[name]:
            raise RuntimeError(f"source changed during freeze: {name}")
        target = source / name
        target.parent.mkdir(parents=True, exist_ok=True)
        # O_EXCL plus a new directory prevents aliases to mutable workspace data.
        with target.open("xb") as stream:
            stream.write(body)
            stream.flush()
            os.fsync(stream.fileno())
        target.chmod(0o555 if (root / name).stat().st_mode & 0o111 else 0o444)
    if names != source_members(root) or before != {name: stable_source_sha256(root, name) for name in names}:
        raise RuntimeError("source membership/content changed during freeze; unpublished release retained")
    bundle = release / "source.zip"
    with zipfile.ZipFile(bundle, "x", compression=zipfile.ZIP_DEFLATED) as archive:
        for name in names:
            archive.write(source / name, name)
    with bundle.open("rb") as stream:
        os.fsync(stream.fileno())
    for directory in sorted((p for p in source.rglob("*") if p.is_dir()), reverse=True):
        directory.chmod(0o555)
    source.chmod(0o555)
    files = dict(before)
    head = subprocess.run(["git", "rev-parse", "HEAD"], cwd=root, check=True,
                          capture_output=True, text=True).stdout.strip()
    receipt = release / "release.json"
    atomic_write_json(receipt, {"schema_version": 1, "state": "built",
        "code": {"git_head": head, "files": files, "source_sha256": identity_sha256(files)},
        "source_bundle": {"file": bundle.name, "files": files,
                          "sha256": hashlib.sha256(bundle.read_bytes()).hexdigest()},
        "service_namespace": namespace_contract(root)})
    receipt.chmod(0o444)
    bundle.chmod(0o444)
    release.chmod(0o555)
    verify_frozen_source(receipt, root)
    return receipt


def verify_frozen_source(receipt: Path, root: Path) -> dict:
    receipt, root = Path(receipt).absolute(), root.resolve()
    if receipt.is_symlink() or not receipt.resolve().is_relative_to(root / "artifacts/operations/service_code_releases"):
        raise ValueError("code release receipt escapes canonical private root")
    value = json.loads(receipt.read_bytes())
    expected = namespace_contract(root)
    if value.get("service_namespace") != expected:
        raise ValueError("code release namespace differs from canonical owner")
    source = receipt.parent / "source"
    proof = verify_source_release(receipt, source)
    actual = {str(p.relative_to(source)) for p in source.rglob("*") if p.is_file() or p.is_symlink()}
    files = value["source_bundle"]["files"]
    placeholders = {f"{SERVICE_DIR}/{name}" for name, kind, _ in LIVE_INPUTS if kind == "file"}
    if actual != set(files) | placeholders or files != value["code"]["files"] or ENTRYPOINT not in files:
        raise ValueError("unrecorded/missing frozen source members")
    for name, kind, _ in LIVE_INPUTS:
        target = source / SERVICE_DIR / name
        if (kind == "file" and target.read_bytes() != b""
                or kind == "directory" and (not target.is_dir() or any(target.iterdir()))):
            raise ValueError("live input placeholder contains copied data")
    bundle = receipt.parent / "source.zip"
    for path in (receipt, receipt.parent, bundle, source, *source.rglob("*")):
        info = path.lstat()
        if path.is_symlink() or info.st_mode & 0o222 or (path.is_file() and info.st_nlink != 1):
            raise ValueError("frozen code is mutable, linked or escaped")
    if (value["source_bundle"].get("file") != bundle.name
            or hashlib.sha256(bundle.read_bytes()).hexdigest() != value["source_bundle"].get("sha256")):
        raise ValueError("frozen recovery bundle checksum differs")
    return {**proof, "receipt": str(receipt), "source_root": str(source)}


def mount_bindings(receipt: Path, root: Path) -> list[tuple[Path, Path, bool]]:
    # Keep the /proc/1/root magic path, without resolve(): it refers to the
    # service manager's original view even after our code-directory overlay.
    # A temporary alias mount cannot supply sources: systemd validates every
    # bind source before creating the alias and rejects its missing children.
    native_root = Path("/proc/1/root") / str(root).lstrip("/")
    return [*((receipt.parent / "source" / name, root / name, True) for name in (*CODE_DIRS, SERVICE_DIR)),
            *((native_root / SERVICE_DIR / name, root / SERVICE_DIR / name, readonly)
              for name, _, readonly in LIVE_INPUTS)]


def render_drop_in(receipt: Path, root: Path) -> str:
    for path in (receipt, root):
        if re.fullmatch(r"/[A-Za-z0-9_./-]+", str(path)) is None:
            raise ValueError("namespace requires an unambiguous absolute systemd path")
    lines = ["# Qualified Discord code release; data/config/env paths remain live.", "[Service]",
             "PrivateMounts=yes", 'Environment="PYTHONDONTWRITEBYTECODE=1"',
             f'Environment="{ENVIRONMENT}={receipt}"']
    lines += [f'{"BindReadOnlyPaths" if readonly else "BindPaths"}={source}:{target}'
              for source, target, readonly in mount_bindings(receipt, root)]
    return "\n".join(lines) + "\n"


def configured_release(root: Path, drop_in: Path = DROP_IN) -> Path | None:
    if not drop_in.exists():
        return None
    body = drop_in.read_text()
    match = re.search(r'^Environment="' + ENVIRONMENT + r'=([^"\n]+)"$', body, re.M)
    if match is None or body != render_drop_in(Path(match[1]), root):
        raise ValueError("unknown or modified service code namespace drop-in")
    return Path(match[1])


def verify_unit_bindings(receipt: Path, root: Path) -> dict:
    """Read parsed manager properties: syntax validity is not mount validity."""
    expected = {(str(source), str(target), readonly) for source, target, readonly in mount_bindings(receipt, root)}
    actual = []
    for readonly in (False, True):
        result = subprocess.run(["busctl", "--system", "--json=short", "get-property",
            "org.freedesktop.systemd1", "/org/freedesktop/systemd1/unit/stockagent_2ddiscord_2dbot_2eservice",
            "org.freedesktop.systemd1.Service", "BindReadOnlyPaths" if readonly else "BindPaths"],
            check=True, capture_output=True, text=True, timeout=10)
        value = json.loads(result.stdout)
        if not isinstance(value, dict) or value.get("type") != "a(ssbt)" or not isinstance(value.get("data"), list):
            raise ValueError("unknown parsed systemd bind-path schema")
        for row in value["data"]:
            if (not isinstance(row, list) or len(row) != 4 or not all(isinstance(p, str) for p in row[:2])
                    or type(row[2]) is not bool or type(row[3]) is not int):
                raise ValueError("malformed parsed systemd bind path")
            source, target, ignore_missing, _flags = row
            binding = (source, target, readonly)
            overlaps = any(Path(target).is_relative_to(root / name) or (root / name).is_relative_to(Path(target))
                           for name in (*CODE_DIRS, SERVICE_DIR))
            if overlaps and (binding not in expected or ignore_missing):
                raise ValueError("unapproved/optional parsed systemd code/live-input mount")
            actual.append(binding)
    if any(actual.count(binding) != 1 for binding in expected):
        raise ValueError("parsed systemd bind sources/targets differ from the verified release")
    return {"parsed_unit_bindings_verified": True, "verified_bindings": len(expected)}


def install_drop_in(receipt: Path, root: Path, drop_in: Path = DROP_IN) -> dict:
    verify_frozen_source(receipt, root)
    # Never overwrite an operator's unknown/mutated configuration. Replacing a
    # previous verified pin must not leave both old and new mounts in preview.
    configured_release(root, drop_in)
    previous = drop_in.read_text() if drop_in.exists() else None
    body = render_drop_in(receipt, root)
    # Validate the effective unit before changing its persistent configuration.
    unit = subprocess.run(["systemctl", "cat", "--no-pager", "stockagent-discord-bot.service"],
                          check=True, capture_output=True, text=True, timeout=10).stdout
    retained, own_section = [], False
    for line in unit.splitlines(keepends=True):
        if line.startswith("# /"):
            own_section = line.rstrip() == f"# {drop_in}"
        if not own_section:
            retained.append(line)
    with tempfile.TemporaryDirectory(prefix="discord-unit-check-") as temporary:
        preview = Path(temporary) / "stockagent-discord-bot.service"
        preview.write_text("".join(retained) + "\n" + body)
        subprocess.run(["systemd-analyze", "verify", str(preview)], check=True,
                       capture_output=True, text=True, timeout=30)
    drop_in.parent.mkdir(parents=True, exist_ok=True)
    atomic_write_text(drop_in, body, durable=True)
    try:
        subprocess.run(["systemctl", "daemon-reload"], check=True, timeout=30)
        proof = verify_unit_bindings(receipt, root)
    except (OSError, subprocess.SubprocessError, ValueError):
        # No restart has occurred. Restore only our exact, known drop-in;
        # never alter the unit, other drop-ins or the running owner.
        if previous is None:
            drop_in.unlink()
        else:
            atomic_write_text(drop_in, previous, durable=True)
        subprocess.run(["systemctl", "daemon-reload"], check=True, timeout=30)
        raise
    return proof


def frozen_generation(receipt: Path) -> tuple:
    """Cheap invalidation after full SHA verification, including new members."""
    source = receipt.parent / "source"
    paths = [receipt.parent, receipt, receipt.parent / "source.zip", source, *sorted(source.rglob("*"))]
    return tuple((str(path), info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns,
                  info.st_ctime_ns, info.st_mode, info.st_nlink)
                 for path in paths for info in [path.lstat()])


def run_in_namespace(receipt: Path, root: Path, arguments: list[str], *, timeout: float = 60,
                     measurement_role: str = "probe"):
    """Verify through the same systemd mount implementation as the native owner.

    Transient probes never run bot/model/quote services. Their bounded lifetime
    also prevents an interrupted verifier from becoming an orphaned owner.
    """
    # Validation accepts a relative operator path but returns the canonical
    # absolute receipt. Use that same verified identity for systemd bindings;
    # do not validate one spelling and then pass relative mount sources.
    # Keep receipt symlink rejection in the verifier (no premature resolve()).
    if measurement_role not in {"probe", "lakehouse"}:
        raise ValueError("unknown temporary namespace measurement role")
    proof = verify_frozen_source(receipt, root)
    receipt, root = Path(proof["receipt"]), root.resolve()
    # The native resource comparator admits only its dedicated engineering
    # cgroup. Preserve that gate inside an immutable-code measurement too.
    prefix = ("stockagent-agent-lakehouse-source-wave-probe-" if measurement_role == "lakehouse"
              else "stockagent-code-probe-")
    command = ["systemd-run", "--wait", "--collect", "--pipe", "--quiet",
               "--unit=" + prefix + uuid.uuid4().hex,
               "--property=PrivateMounts=yes", "--property=WorkingDirectory=" + str(root),
               "--property=RuntimeMaxSec=" + str(timeout),
               "--property=Environment=PYTHONDONTWRITEBYTECODE=1",
               "--property=Environment=" + ENVIRONMENT + "=" + str(receipt)]
    # systemd's default PATH omits caller runtime tools such as WSL's NVIDIA
    # driver/Windows adapters. Pass one structured --setenv argument, including
    # paths with spaces, without copying credentials or the whole environment.
    if "PATH" in os.environ:
        command.append("--setenv=PATH=" + os.environ["PATH"])
    bindings = mount_bindings(receipt, root)
    for readonly in (False, True):
        pairs = " ".join(f"{source}:{target}" for source, target, ro in bindings if ro == readonly)
        command += ["--property=" + ("BindReadOnlyPaths=" if readonly else "BindPaths=") + pairs]
    return subprocess.run([*command, sys.executable, *arguments], cwd=root,
                          capture_output=True, text=True, timeout=timeout + 5)


def namespace_identity(receipt: Path, root: Path) -> dict:
    """No model, provider or CUDA calls; probe the exact proposed mount view."""
    probe = """import json,errno,os
from pathlib import Path
from stockagent.live.completed_inference_cache import implementation_identity,file_identity
from stockagent.runtime_identity import verify_source_release
from stockagent.live.service_code_release import verify_mounted_source
root=Path.cwd()
receipt=Path(os.environ['STOCKAGENT_SERVICE_CODE_RELEASE'])
verify_source_release(receipt,root)
verify_mounted_source(receipt,root,os.getpid())
code=implementation_identity()
files=json.loads(receipt.read_bytes())['code']['files']
for item in code:
    name=str(Path(item['path']).relative_to(root))
    if name not in files:
        raise RuntimeError('inference dependency is not pinned: '+name)
try:
    (root/'stockagent/live/completed_inference_cache.py').open('ab').close()
except OSError as exc:
    assert exc.errno==errno.EROFS, str(exc)
else:
    raise RuntimeError('code mount is not read-only')
print(json.dumps({'code':code,'entrypoint':file_identity(root/'services/discord_bot/bot.py'),'readonly_verified':True}))
"""
    result = run_in_namespace(receipt, root, ["-c", probe])
    if result.returncode:
        raise RuntimeError(f"frozen namespace verification failed: {result.stderr[-3000:]}")
    return json.loads(result.stdout)


def run_cli_against_active_source(root: Path, script: Path, arguments: list[str]) -> int | None:
    """Native-owner verifiers must inspect that owner's source, not research."""
    receipt = configured_release(root)
    if receipt is None:
        return None
    if os.environ.get(ENVIRONMENT) == str(receipt):
        verify_mounted_source(receipt, root, os.getpid())
        return None
    result = run_in_namespace(receipt, root, [str(script), *arguments], timeout=600)
    print(result.stdout, end="", flush=True)
    print(result.stderr, end="", file=sys.stderr, flush=True)
    return result.returncode


def verify_mounted_source(receipt: Path, root: Path, python_pid: int) -> dict:
    """Verify actual code/live-input mounts without claiming Gateway readiness."""
    proof = verify_frozen_source(receipt, root)
    value = json.loads(receipt.read_bytes())
    process = Path(f"/proc/{python_pid}")
    for name, digest in value["code"]["files"].items():
        # Keep /proc/pid/root magic resolution. Path.resolve() here would jump
        # back to the observer's mount namespace and silently read the workspace.
        native = process / "root" / str(root / name).lstrip("/")
        frozen = receipt.parent / "source" / name
        a, b = native.stat(), frozen.stat()
        if ((a.st_dev, a.st_ino) != (b.st_dev, b.st_ino)
                or hashlib.sha256(native.read_bytes()).hexdigest() != digest):
            raise ValueError(f"native owner is not bound to the frozen source: {name}")
    mounts = (process / "mountinfo").read_text().splitlines()
    for _source, target, readonly in mount_bindings(receipt, root):
        if not any(row.split()[4] == str(target) and ("ro" if readonly else "rw") in row.split()[5].split(",") for row in mounts):
            raise ValueError(f"native source/live-input mount has wrong access mode: {target}")
    for name, _, _ in LIVE_INPUTS:
        source = Path("/proc/1/root") / str(root / SERVICE_DIR / name).lstrip("/")
        native = process / "root" / str(root / SERVICE_DIR / name).lstrip("/")
        a, b = source.stat(), native.stat()
        if (a.st_dev, a.st_ino) != (b.st_dev, b.st_ino):
            raise ValueError(f"native live input differs from the service manager's original view: {name}")
    return {**proof, "native_python_pid": python_pid, "readonly_mounts_verified": True,
            "native_file_count": len(value["code"]["files"]), "workspace_changes_not_auto_promoted": True}


def verify_owner_namespace(receipt: Path, root: Path, python_pid: int) -> dict:
    """Independent native-owner readback; no re-dating a service snapshot."""
    if "stockagent-discord-bot.service" not in Path(f"/proc/{python_pid}/cgroup").read_text():
        raise ValueError("namespace readback is not the native Discord process")
    return verify_mounted_source(receipt, root, python_pid)


def verify_startup_namespace(root: Path) -> dict:
    """Verify every cold start before the bot admits any resident model/code."""
    receipt = os.environ.get(ENVIRONMENT)
    if not receipt:
        raise ValueError("pinned source startup requires an explicit release receipt")
    return verify_owner_namespace(Path(receipt), root, os.getpid())

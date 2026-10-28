"""Pinned source namespaces for a live owner, not a second inference pipeline.

Keep canonical absolute paths, data, credentials and configs. Only code is
copied (never hard-linked) and bind-mounted read-only by systemd. In-process
implementation admission remains unchanged and observes the pinned files.
"""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
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
ENGINE_DROP_IN = Path("/etc/systemd/system/stockagent-tw-day-trade-simulation.service.d/stockagent-code-release.conf")
ENGINE_CODE_FILES = ("scripts/run_tw_day_trade_simulation.py", "stockagent/live/quote_provider.py",
                     "stockagent/live/tw_day_trade_simulation.py")
ENVIRONMENT = "STOCKAGENT_SERVICE_CODE_RELEASE"
PROBE_INPUT_ROOTS = ("test", "services/public_dashboards", "services/tw_day_trade_dashboard",
                     "services/data_monitor_dashboard")


def _owner(role: str) -> tuple[str, str, Path]:
    # An explicit allowlist, not arbitrary systemd unit/path interpolation.
    if role == "discord":
        return ("stockagent-discord-bot.service", "stockagent_2ddiscord_2dbot_2eservice", DROP_IN)
    if role == "engine":
        return ("stockagent-tw-day-trade-simulation.service",
                "stockagent_2dtw_2dday_2dtrade_2dsimulation_2eservice", ENGINE_DROP_IN)
    raise ValueError("unknown pinned source owner")


def _read_source_bytes(path: Path) -> tuple[bytes, int]:
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(descriptor, "rb") as stream:
        before = os.fstat(stream.fileno())
        if not stat.S_ISREG(before.st_mode) or before.st_size > 2 * 1024**2:
            raise ValueError("source copy requires a bounded regular member")
        body = stream.read(2 * 1024**2 + 1)
        generation = lambda value: (value.st_dev, value.st_ino, value.st_size,
                                    value.st_mtime_ns, value.st_ctime_ns)
        if (len(body) > 2 * 1024**2 or generation(before) != generation(os.fstat(stream.fileno()))
                or generation(before) != generation(path.lstat())):
            raise RuntimeError("source changed during freeze")
        return body, before.st_mode


def _copy_sealed_members(source: Path, source_roots: dict[str, Path], hashes: dict[str, str],
                         *, executable: bool = True) -> None:
    """Bounded parallel copies retain every file's durable, read-only barrier.

    Serial fsyncs for thousands of independent code members needlessly wait
    for thousands of filesystem commits under concurrent acquisition. At most
    eight files/16 MiB are in flight; no source link, missing fsync, unverified
    bytes or partially constructed release is admitted by this scheduling.
    """
    for parent in sorted({(source / name).parent for name in hashes}):
        parent.mkdir(parents=True, exist_ok=True)

    def copy(name):
        original = source_roots[name] / name
        body, original_mode = _read_source_bytes(original)
        if hashlib.sha256(body).hexdigest() != hashes[name]:
            raise RuntimeError(f"source changed during freeze: {name}")
        mode = 0o555 if executable and original_mode & 0o111 else 0o444
        with (source / name).open("xb") as stream:
            stream.write(body)
            stream.flush()
            os.fchmod(stream.fileno(), mode)
            os.fsync(stream.fileno())

    with ThreadPoolExecutor(max_workers=8, thread_name_prefix="sealed-source") as pool:
        # Iteration propagates a failed barrier; executor exit joins all owned
        # writes. A failure can leave unpublished files, never an accepted head.
        for _ in pool.map(copy, hashes):
            pass


def _seal_directory_tree(source: Path) -> None:
    directories = [source, *(path for path in source.rglob("*") if path.is_dir())]
    for path in sorted(directories, key=lambda item: len(item.parts), reverse=True):
        descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            os.fchmod(descriptor, 0o555)
            os.fsync(descriptor)
        finally:
            os.close(descriptor)


def _seal_control_file(path: Path) -> None:
    with path.open("rb") as stream:
        os.fchmod(stream.fileno(), 0o444)
        os.fsync(stream.fileno())


def _seal_release_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        os.fchmod(descriptor, 0o555)
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    descriptor = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


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


def freeze_source(root: Path, output: Path, *, base_receipt: Path | None = None,
                  include_sources: tuple[str, ...] = (), include_root: Path | None = None) -> Path:
    """Freeze one stable tree using the existing source-release verifier."""
    root, output = root.resolve(), output.resolve()
    allowed = root / "artifacts/operations/service_code_releases"
    if output != allowed:
        raise ValueError("service code releases belong in the private canonical release root")
    lineage = None
    if base_receipt is None:
        if include_sources or include_root is not None:
            raise ValueError("explicit source overlay requires a verified base release")
        names = source_members(root)
        source_roots = dict.fromkeys(names, root)
    else:
        proof = verify_frozen_source(base_receipt, root)
        base_receipt = Path(proof["receipt"])
        base = json.loads(base_receipt.read_bytes())
        selected = tuple(include_sources)
        if (not selected or len(set(selected)) != len(selected)
                or not set(selected).issubset(source_members(root))):
            raise ValueError("overlay requires unique explicit canonical source members")
        reviewed = root
        if include_root is not None:
            reviewed = Path(include_root).absolute()
            if (reviewed.resolve(strict=True) != reviewed or not reviewed.is_dir()
                    or not reviewed.is_relative_to(root / "artifacts/operations")):
                raise ValueError("reviewed source root must be a real private operations directory")
            if any((reviewed / name).resolve(strict=True) != reviewed / name for name in selected):
                raise ValueError("reviewed source members must not be redirected")
        source_roots = dict.fromkeys(base["code"]["files"], base_receipt.parent / "source")
        source_roots.update(dict.fromkeys(selected, reviewed))
        names = sorted(source_roots)
        lineage = {"contract": "verified_base_explicit_source_overlay_v1",
                   "base_receipt": str(base_receipt),
                   "base_source_sha256": base["code"]["source_sha256"],
                   "included_sources": sorted(selected),
                   "unselected_workspace_changes_promoted": False}
        if include_root is not None:
            lineage["included_source_root"] = str(reviewed)
    before = {name: stable_source_sha256(source_roots[name], name) for name in names}
    if sum((source_roots[name] / name).stat().st_size for name in names) > 128 * 1024**2:
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
            _seal_control_file(target)
        else:
            target.mkdir(mode=0o555)
    _copy_sealed_members(source, source_roots, before)
    if (before != {name: stable_source_sha256(source_roots[name], name) for name in names}
            or (base_receipt is None and names != source_members(root))):
        raise RuntimeError("source membership/content changed during freeze; unpublished release retained")
    if base_receipt is not None:
        verify_frozen_source(base_receipt, root)
        if not set(include_sources).issubset(source_members(root)):
            raise RuntimeError("selected source membership changed during freeze")
    bundle = release / "source.zip"
    with zipfile.ZipFile(bundle, "x", compression=zipfile.ZIP_DEFLATED) as archive:
        for name in names:
            archive.write(source / name, name)
    _seal_control_file(bundle)
    _seal_directory_tree(source)
    files = dict(before)
    head = subprocess.run(["git", "rev-parse", "HEAD"], cwd=root, check=True,
                          capture_output=True, text=True).stdout.strip()
    receipt = release / "release.json"
    atomic_write_json(receipt, {"schema_version": 1, "state": "built",
        "code": {"git_head": head, "files": files, "source_sha256": identity_sha256(files)},
        "source_bundle": {"file": bundle.name, "files": files,
                          "sha256": hashlib.sha256(bundle.read_bytes()).hexdigest()},
        "service_namespace": namespace_contract(root),
        **({"source_overlay": lineage} if lineage is not None else {})})
    _seal_control_file(receipt)
    _seal_release_directory(release)
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


def render_drop_in(receipt: Path, root: Path, *, role: str = "discord") -> str:
    _owner(role)
    for path in (receipt, root):
        if re.fullmatch(r"/[A-Za-z0-9_./-]+", str(path)) is None:
            raise ValueError("namespace requires an unambiguous absolute systemd path")
    label = "Discord" if role == "discord" else "day-trade engine"
    lines = [f"# Qualified {label} code release; data/config/env paths remain live.", "[Service]",
             "PrivateMounts=yes", 'Environment="PYTHONDONTWRITEBYTECODE=1"',
             f'Environment="{ENVIRONMENT}={receipt}"']
    lines += [f'{"BindReadOnlyPaths" if readonly else "BindPaths"}={source}:{target}'
              for source, target, readonly in mount_bindings(receipt, root)]
    return "\n".join(lines) + "\n"


def configured_release(root: Path, drop_in: Path | None = None, *, role: str = "discord") -> Path | None:
    default = _owner(role)[2]
    drop_in = default if drop_in is None else drop_in
    try:
        info = drop_in.lstat()
    except FileNotFoundError:
        return None
    if not stat.S_ISREG(info.st_mode) or info.st_size > 64 * 1024:
        raise ValueError("unknown service code namespace control file")
    body = _read_source_bytes(drop_in)[0].decode("utf-8")
    match = re.search(r'^Environment="' + ENVIRONMENT + r'=([^"\n]+)"$', body, re.M)
    if match is None or body != render_drop_in(Path(match[1]), root, role=role):
        raise ValueError("unknown or modified service code namespace drop-in")
    return Path(match[1])


def verify_unit_bindings(receipt: Path, root: Path, *, role: str = "discord") -> dict:
    """Read parsed manager properties: syntax validity is not mount validity."""
    bus_unit = _owner(role)[1]
    expected = {(str(source), str(target), readonly) for source, target, readonly in mount_bindings(receipt, root)}
    actual = []
    for readonly in (False, True):
        result = subprocess.run(["busctl", "--system", "--json=short", "get-property",
            "org.freedesktop.systemd1", "/org/freedesktop/systemd1/unit/" + bus_unit,
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


def install_drop_in(receipt: Path, root: Path, drop_in: Path | None = None, *, role: str = "discord") -> dict:
    unit_name, _, default = _owner(role)
    drop_in = default if drop_in is None else drop_in
    verify_frozen_source(receipt, root)
    # Never overwrite an operator's unknown/mutated configuration. Replacing a
    # previous verified pin must not leave both old and new mounts in preview.
    configured_release(root, drop_in, role=role)
    previous = drop_in.read_text() if drop_in.exists() else None
    body = render_drop_in(receipt, root, role=role)
    # Validate the effective unit before changing its persistent configuration.
    unit = subprocess.run(["systemctl", "cat", "--no-pager", unit_name],
                          check=True, capture_output=True, text=True, timeout=10).stdout
    retained, own_section = [], False
    for line in unit.splitlines(keepends=True):
        if line.startswith("# /"):
            own_section = line.rstrip() == f"# {drop_in}"
        if not own_section:
            retained.append(line)
    with tempfile.TemporaryDirectory(prefix=role + "-unit-check-") as temporary:
        preview = Path(temporary) / unit_name
        preview.write_text("".join(retained) + "\n" + body)
        subprocess.run(["systemd-analyze", "verify", str(preview)], check=True,
                       capture_output=True, text=True, timeout=30)
    drop_in.parent.mkdir(parents=True, exist_ok=True)
    atomic_write_text(drop_in, body, durable=True)
    try:
        subprocess.run(["systemctl", "daemon-reload"], check=True, timeout=30)
        proof = verify_unit_bindings(receipt, root, role=role)
    except (OSError, subprocess.SubprocessError, ValueError):
        # No restart has occurred. Restore only our exact, known drop-in;
        # never alter the unit, other drop-ins or the running owner.
        if previous is None:
            drop_in.unlink()
            descriptor = os.open(drop_in.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
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


def freeze_probe_inputs(root: Path, *, roots: tuple[str, ...] = PROBE_INPUT_ROOTS,
                        include_files: tuple[str, ...] | None = None) -> Path:
    """Pin verification-only tests/assets, never native code or live data.

    A source release alone cannot make a browser acceptance repeatable if its
    tests and static files are still edited concurrently. Keep this separately
    fingerprinted capsule out of native mount_bindings and code identity.
    Only Git-visible, regular, bounded members of explicit safe roots enter it.
    """
    root = root.resolve()
    if not roots or len(set(roots)) != len(roots) or not set(roots).issubset(PROBE_INPUT_ROOTS):
        raise ValueError("unsupported verification input roots")
    selected = None if include_files is None else tuple(include_files)
    if selected is not None and (not selected or len(set(selected)) != len(selected)
            or any(not isinstance(name, str) or Path(name).as_posix() != name
                   or Path(name).is_absolute() or ".." in Path(name).parts
                   or not any(name.startswith(prefix + "/") for prefix in roots)
                   for name in selected)):
        raise ValueError("invalid explicit verification input members")

    def members():
        result = subprocess.run(["git", "ls-files", "--cached", "--others", "--exclude-standard", "-z",
                                 "--", *roots], cwd=root, check=True, capture_output=True)
        names = sorted(set(result.stdout.decode().split("\0")) - {""})
        if selected is not None:
            if not set(selected).issubset(names):
                raise ValueError("explicit verification input is not Git-visible")
            # Pin only the declared dependency closure. Unrelated new tests or
            # assets cannot change this probe's identity. Missing dependencies
            # still fail inside the read-only mount; never fall through to a
            # mutable test directory for an undeclared import.
            names = sorted(selected)
        if not names or len(names) > 10_000:
            raise ValueError("verification input membership exceeds bounds")
        return names

    names = members()
    before, total = {}, 0
    for name in names:
        path = root / name
        info = path.lstat()
        if (not any(name.startswith(prefix + "/") for prefix in roots)
                or any(part.startswith(".") or part == "__pycache__" for part in Path(name).parts)
                or path.resolve() != path or not stat.S_ISREG(info.st_mode) or info.st_size > 2 * 1024**2):
            raise ValueError("verification inputs must be regular bounded public test/assets")
        total += info.st_size
        before[name] = hashlib.sha256(path.read_bytes()).hexdigest()
    if total > 128 * 1024**2:
        raise ValueError("verification input bytes exceed bounds")
    directory = root / "artifacts/operations/service_probe_inputs" / (
        time.strftime("%Y%m%dT%H%M%SZ", time.gmtime()) + "-" + uuid.uuid4().hex[:12])
    source = directory / "source"
    source.mkdir(parents=True, mode=0o700)
    for prefix in roots:
        (source / prefix).mkdir(parents=True, exist_ok=True)
    _copy_sealed_members(source, dict.fromkeys(names, root), before, executable=False)
    if names != members() or before != {name: hashlib.sha256((root / name).read_bytes()).hexdigest() for name in names}:
        raise RuntimeError("verification inputs changed during freeze")
    _seal_directory_tree(source)
    receipt = directory / "receipt.json"
    payload = {"contract": "readonly_verification_inputs_v1", "workspace_root": str(root),
        "roots": list(roots), "files": before, "sha256": identity_sha256(before),
        "native_code_or_live_data": False}
    if selected is not None:
        payload["selection"] = {"contract": "explicit_git_visible_dependency_members_v1",
                                "files": sorted(selected)}
    atomic_write_json(receipt, payload)
    _seal_control_file(receipt)
    _seal_release_directory(directory)
    verify_probe_inputs(receipt, root)
    return receipt


def verify_probe_inputs(receipt: Path, root: Path) -> dict:
    root, receipt = root.resolve(), receipt.absolute()
    allowed = root / "artifacts/operations/service_probe_inputs"
    if (receipt.resolve() != receipt or receipt.parent.parent != allowed or receipt.name != "receipt.json"
            or not stat.S_ISREG(receipt.lstat().st_mode) or receipt.stat().st_mode & 0o222):
        raise ValueError("invalid verification input receipt")
    value = json.loads(receipt.read_bytes())
    roots, files = value.get("roots"), value.get("files")
    if (value.get("contract") != "readonly_verification_inputs_v1" or value.get("workspace_root") != str(root)
            or value.get("native_code_or_live_data") is not False
            or not isinstance(roots, list) or not roots or len(set(roots)) != len(roots)
            or not set(roots).issubset(PROBE_INPUT_ROOTS) or not isinstance(files, dict)
            or not files or len(files) > 10_000 or identity_sha256(files) != value.get("sha256")):
        raise ValueError("invalid verification input identity")
    if "selection" in value and value["selection"] != {
            "contract": "explicit_git_visible_dependency_members_v1", "files": sorted(files)}:
        raise ValueError("invalid explicit verification input identity")
    source, observed = receipt.parent / "source", {}
    total = 0
    for path in [source, *sorted(source.rglob("*"))]:
        info = path.lstat()
        if path.is_symlink() or path.resolve() != path or info.st_mode & 0o222:
            raise ValueError("verification inputs are mutable or aliased")
        if stat.S_ISDIR(info.st_mode):
            continue
        name = path.relative_to(source).as_posix()
        if (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_size > 2 * 1024**2
                or not any(name.startswith(prefix + "/") for prefix in roots)):
            raise ValueError("unexpected verification input")
        total += info.st_size
        observed[name] = hashlib.sha256(path.read_bytes()).hexdigest()
    if total > 128 * 1024**2 or observed != files or not all((source / prefix).is_dir() for prefix in roots):
        raise ValueError("verification input contents differ")
    proof = {"receipt": str(receipt), "sha256": value["sha256"], "roots": roots,
             "files": files, "native_code_or_live_data": False}
    if "selection" in value:
        proof["selection"] = value["selection"]
    return proof


def run_in_namespace(receipt: Path, root: Path, arguments: list[str], *, timeout: float = 60,
                     measurement_role: str = "probe", verification_inputs: Path | None = None):
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
    inputs = verify_probe_inputs(verification_inputs, root) if verification_inputs is not None else None
    if inputs is not None:
        source = Path(inputs["receipt"]).parent / "source"
        bindings.extend((source / name, root / name, True) for name in inputs["roots"])
    for readonly in (False, True):
        pairs = " ".join(f"{source}:{target}" for source, target, ro in bindings if ro == readonly)
        command += ["--property=" + ("BindReadOnlyPaths=" if readonly else "BindPaths=") + pairs]
    result = subprocess.run([*command, sys.executable, *arguments], cwd=root,
                            capture_output=True, text=True, timeout=timeout + 5)
    if inputs is not None and verify_probe_inputs(Path(inputs["receipt"]), root) != inputs:
        raise RuntimeError("verification inputs changed during namespace probe")
    return result


def namespace_identity(receipt: Path, root: Path, *, role: str = "discord") -> dict:
    """No model, provider or CUDA calls; probe the exact proposed mount view."""
    _owner(role)
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
result={'code':code,'entrypoint':file_identity(root/'services/discord_bot/bot.py'),'readonly_verified':True}
"""
    if role == "engine":
        probe += "\nengine_files=" + repr(ENGINE_CODE_FILES) + "\n"
        probe += """from stockagent.live.service_code_release import _owner, ENGINE_CODE_FILES
if _owner('engine')[0]!='stockagent-tw-day-trade-simulation.service' or ENGINE_CODE_FILES!=engine_files:
    raise RuntimeError('source release does not implement the engine owner contract')
if any(name not in files for name in engine_files):
    raise RuntimeError('engine dependency is not pinned')
result['engine_code']=[file_identity(root/name) for name in engine_files]
"""
    probe += "\nprint(json.dumps(result))\n"
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


def verify_owner_namespace(receipt: Path, root: Path, python_pid: int, *, role: str = "discord") -> dict:
    """Independent native-owner readback; no re-dating a service snapshot."""
    unit = _owner(role)[0]
    groups = Path(f"/proc/{python_pid}/cgroup").read_text().splitlines()
    if not any(unit in row.split(":", 2)[-1].split("/") for row in groups):
        label = "Discord" if role == "discord" else "day-trade engine"
        raise ValueError(f"namespace readback is not the native {label} process")
    return verify_mounted_source(receipt, root, python_pid)


def native_owner_pid(root: Path, *, role: str) -> int:
    """Find the unique Python owner in its allowlisted native service cgroup.

    MainPID can name the existing shell supervisor, not its Python child. Do
    not validate that supervisor's different mount/code view as the executor.
    """
    unit = _owner(role)[0]
    entrypoint = ENTRYPOINT if role == "discord" else ENGINE_CODE_FILES[0]
    raw = (Path("/sys/fs/cgroup/system.slice") / unit / "cgroup.procs").read_text()
    tokens = raw.split()
    if len(raw) > 64 * 1024 or not 0 < len(tokens) <= 256 or not all(t.isdecimal() for t in tokens):
        raise ValueError("invalid native owner process inventory")
    matches = []
    for token in tokens:
        process = Path("/proc") / token
        try:
            command = process.joinpath("cmdline").read_bytes()
            if len(command) > 64 * 1024:
                raise ValueError("unbounded native owner command")
            arguments = command.rstrip(b"\0").split(b"\0")
            absolute = os.fsencode(root / entrypoint) in arguments
            relative = os.fsencode(entrypoint) in arguments and process.joinpath("cwd").readlink() == root
            if (absolute or relative) and process.joinpath("exe").readlink().name.startswith("python"):
                matches.append(int(token))
        except (FileNotFoundError, ProcessLookupError):
            continue  # A disappearing owner is not a valid acceptance candidate.
    if len(matches) != 1:
        raise ValueError("native service requires exactly one Python entrypoint owner")
    return matches[0]


def verify_startup_namespace(root: Path, *, role: str = "discord") -> dict:
    """Verify every cold start before the bot admits any resident model/code."""
    receipt = os.environ.get(ENVIRONMENT)
    if not receipt:
        raise ValueError("pinned source startup requires an explicit release receipt")
    return verify_owner_namespace(Path(receipt), root, os.getpid(), role=role)

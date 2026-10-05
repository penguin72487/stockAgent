"""Small, reversible skill-file exchange; usable by a stdlib-only SSH peer.

Only custom skill directories are inputs. Credentials, sessions, memory, system
skills and package caches are outside the exchange. Deletions never propagate.
"""
from __future__ import annotations

import base64
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import stat
import subprocess
import sys
import tempfile
import uuid


CONTRACT = "codex-custom-skills-three-way-v1"
MAX_FILE_BYTES = 16 * 1024 * 1024
MAX_TOTAL_BYTES = 64 * 1024 * 1024
NAME = re.compile(r"[a-z0-9][a-z0-9-]{0,63}")
PRIVATE_NAME = re.compile(r"(?:^|[._-])(?:credentials?|passwords?|secrets?|id_rsa|id_ed25519)(?:$|[._-])", re.I)


class SkillSyncError(RuntimeError):
    pass


def content(record: dict) -> bytes:
    data = base64.b64decode(record["data"], validate=True)
    if len(data) > MAX_FILE_BYTES or hashlib.sha256(data).hexdigest() != record["sha256"]:
        raise SkillSyncError("invalid skill member hash or size")
    return data


def record(data: bytes, mode: int = 0o644) -> dict:
    return {"data": base64.b64encode(data).decode(),
            "sha256": hashlib.sha256(data).hexdigest(), "mode": mode & 0o777}


def public_member(member: str, data: bytes) -> None:
    if (PRIVATE_NAME.search(PurePosixPath(member).name)
            or member.lower().endswith((".pem", ".p12", ".pfx", ".key"))
            or re.search(rb"-----BEGIN [A-Z ]*PRIVATE KEY-----", data)):
        raise SkillSyncError(f"private resource is outside skill synchronization: {member}")


def same(left: dict | None, right: dict | None) -> bool:
    if left is None or right is None:
        return left is right
    return left["sha256"] == right["sha256"] and left["mode"] == right["mode"]


def key_parts(key: str) -> tuple[str, str, PurePosixPath]:
    parts = PurePosixPath(key).parts
    if len(parts) < 3 or parts[0] not in {"user", "repo"} or not NAME.fullmatch(parts[1]):
        raise SkillSyncError(f"unsafe skill key: {key!r}")
    member = PurePosixPath(*parts[2:])
    if str(member) != "/".join(parts[2:]) or any(p in {".", ".."} or p.startswith(".") for p in member.parts):
        raise SkillSyncError(f"unsafe skill member: {key!r}")
    if str(PurePosixPath(key)) != key:
        raise SkillSyncError(f"noncanonical skill key: {key!r}")
    return parts[0], parts[1], member


def checked_root(value: str) -> Path:
    root = Path(value)
    if not root.is_absolute() or root.name != "skills" or ".." in root.parts:
        raise SkillSyncError(f"expected an absolute skills root: {value!r}")
    if any(p.is_symlink() for p in [root, *root.parents]):
        raise SkillSyncError(f"symlinked skill root: {root}")
    return root


def inventory(roots: dict[str, str]) -> dict[str, dict]:
    files = {}
    total = 0
    for scope, value in roots.items():
        if scope not in {"user", "repo"}:
            raise SkillSyncError(f"unknown scope: {scope}")
        root = checked_root(value)
        if not root.exists():
            continue
        for skill in sorted(root.iterdir()):
            if skill.name.startswith("."):
                continue
            if skill.is_symlink():
                raise SkillSyncError(f"skill symlink requires explicit handling: {skill}")
            if not skill.is_dir() or not (skill / "SKILL.md").is_file():
                continue
            if not NAME.fullmatch(skill.name):
                raise SkillSyncError(f"invalid skill directory: {skill.name}")
            for parent, dirs, names in os.walk(skill, followlinks=False):
                dirs[:] = sorted(d for d in dirs if not d.startswith(".") and d != "__pycache__")
                if any((Path(parent) / d).is_symlink() for d in dirs):
                    raise SkillSyncError(f"symlinked resource directory: {parent}")
                for name in sorted(names):
                    if name.startswith(".") or name.endswith((".pyc", ".pyo")):
                        continue
                    p = Path(parent) / name
                    if p.is_symlink() or not stat.S_ISREG(p.stat().st_mode):
                        raise SkillSyncError(f"nonregular skill resource: {p}")
                    before = p.stat()
                    if before.st_size > MAX_FILE_BYTES:
                        raise SkillSyncError(f"skill member exceeds size limit: {p}")
                    data = p.read_bytes()
                    after = p.stat()
                    if (before.st_ino, before.st_size, before.st_mtime_ns, before.st_mode) != (
                            after.st_ino, after.st_size, after.st_mtime_ns, after.st_mode):
                        raise SkillSyncError(f"skill changed during inventory: {p}")
                    total += len(data)
                    if total > MAX_TOTAL_BYTES:
                        raise SkillSyncError("skill inventory exceeds size limit")
                    key = f"{scope}/{skill.name}/{p.relative_to(skill).as_posix()}"
                    key_parts(key)
                    public_member(key, data)
                    files[key] = record(data, stat.S_IMODE(after.st_mode))
    return files


def merge_files(local: dict, remote: dict, base: dict) -> tuple[dict, list[dict]]:
    """Union additions; use Git's three-way text merge only with a common base."""
    merged, conflicts = {}, []
    for key in sorted(local.keys() | remote.keys() | base.keys()):
        key_parts(key)
        left, right, old = local.get(key), remote.get(key), base.get(key)
        if left is None and right is None:
            # Both copies vanished: retain the recorded member rather than infer deletion consent.
            chosen = old
        elif left is None:
            chosen = right
        elif right is None or same(left, right):
            chosen = left
        elif same(left, old):
            chosen = right
        elif same(right, old):
            chosen = left
        else:
            reason = "different members without a common base"
            if old is not None:
                values = [content(x) for x in (left, old, right)]
                modes = [x["mode"] for x in (left, old, right)]
                mode = modes[0] if modes[0] == modes[2] or modes[2] == modes[1] else modes[2] if modes[0] == modes[1] else None
                try:
                    for value in values:
                        value.decode("utf-8")
                        if b"\0" in value:
                            raise ValueError("binary")
                    if mode is None:
                        raise ValueError("conflicting file modes")
                    with tempfile.TemporaryDirectory(prefix="skill-merge-") as tmp:
                        paths = [Path(tmp) / n for n in ("local", "base", "remote")]
                        for p, value in zip(paths, values):
                            p.write_bytes(value)
                        r = subprocess.run(["git", "merge-file", "-p", *map(str, paths)],
                                           capture_output=True, timeout=15)
                    if r.returncode == 0:
                        merged[key] = record(r.stdout, mode)
                        continue
                    reason = "overlapping edits" if r.returncode > 0 else "merge tool failed"
                except (UnicodeError, ValueError, OSError, subprocess.TimeoutExpired) as exc:
                    reason = str(exc)
            conflicts.append({"key": key, "reason": reason})
            continue
        if chosen is not None:
            content(chosen)
            merged[key] = chosen
    return merged, conflicts


def install(roots: dict[str, str], expected: dict, desired: dict, run_id: str) -> dict:
    """Stage complete directories; retain previous versions on the same filesystem."""
    if not re.fullmatch(r"[A-Za-z0-9_-]+", run_id):
        raise SkillSyncError("unsafe run ID")
    current = inventory(roots)
    if current != expected:
        raise SkillSyncError("skills changed since planning; re-inventory before applying")
    if not expected.keys() <= desired.keys():
        raise SkillSyncError("sync cannot delete existing skill members")
    groups = {}
    for key, item in desired.items():
        scope, skill, member = key_parts(key)
        if scope not in roots:
            raise SkillSyncError(f"missing target scope: {scope}")
        public_member(str(member), content(item))
        if item["mode"] not in {0o600, 0o644, 0o700, 0o755, 0o664, 0o775}:
            raise SkillSyncError(f"unsupported resource permissions: {key}")
        groups.setdefault((scope, skill), {})[str(member)] = item
    changed, backups = [], []
    for (scope, name), members in sorted(groups.items()):
        prefix = f"{scope}/{name}/"
        old = {key[len(prefix):]: value for key, value in expected.items() if key.startswith(prefix)}
        if old == members:
            continue
        if "SKILL.md" not in members:
            raise SkillSyncError(f"missing entrypoint: {name}")
        root = checked_root(roots[scope])
        root.mkdir(parents=True, exist_ok=True)
        stage = Path(tempfile.mkdtemp(prefix=".skill-sync-stage-", dir=root))
        dest = root / name
        backup = root / ".skill-sync-backups" / run_id / name
        try:
            for member, item in sorted(members.items()):
                p = stage / member
                p.parent.mkdir(parents=True, exist_ok=True)
                with p.open("xb") as f:
                    f.write(content(item))
                    f.flush()
                    os.fsync(f.fileno())
                p.chmod(item["mode"])
            current = inventory(roots)
            observed = {key[len(prefix):]: value for key, value in current.items() if key.startswith(prefix)}
            if observed != old or (dest.exists() and not old):
                raise SkillSyncError(f"skill changed before installation: {dest}")
            if dest.exists():
                if any(p.is_symlink() for p in [backup, *backup.parents]):
                    raise SkillSyncError(f"symlinked backup path: {backup}")
                backup.parent.mkdir(parents=True, exist_ok=True)
                if backup.exists():
                    raise SkillSyncError(f"backup already exists: {backup}")
                os.rename(dest, backup)
                backups.append(str(backup))
            try:
                os.rename(stage, dest)
            except BaseException:
                if backup.exists() and not dest.exists():
                    os.rename(backup, dest)
                raise
            changed.append(f"{scope}/{name}")
        finally:
            if stage.exists():
                shutil.rmtree(stage)
    return {"changed_skills": changed, "backups": backups, "files": inventory(roots)}


def rpc() -> None:
    request = json.load(sys.stdin)
    if request["contract"] != CONTRACT:
        raise SkillSyncError("unsupported skill exchange contract")
    if request["action"] == "inventory":
        result = {"files": inventory(request["roots"])}
    elif request["action"] == "install":
        result = install(request["roots"], request["expected"], request["desired"], request["run_id"])
    else:
        raise SkillSyncError("unsupported skill action")
    print(json.dumps({"contract": CONTRACT, **result}, ensure_ascii=False))

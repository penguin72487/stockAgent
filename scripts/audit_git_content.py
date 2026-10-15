#!/usr/bin/env python3
"""Read-only Git content inventory and staged-upload guard; never deletes files."""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
import json
import os
from pathlib import Path, PurePosixPath
import subprocess


ROOT = Path(__file__).resolve().parents[1]
PAYLOAD_SUFFIXES = {".pt", ".pth", ".ckpt", ".onnx", ".parquet", ".feather",
                    ".npy", ".npz", ".pkl", ".joblib", ".sqlite", ".sqlite3",
                    ".sqlite-wal", ".sqlite-shm", ".log", ".pid", ".7z", ".zip", ".gz", ".zst"}
FIXTURE_SUFFIXES = {".pt", ".parquet", ".npy", ".npz", ".zip"}
CACHE_PARTS = {"__pycache__", ".pytest_cache", ".ruff_cache", ".mypy_cache",
               ".venv", "node_modules", ".skill-sync-backups", ".ipynb_checkpoints"}
SOURCE_ROOTS = {"stockagent", "scripts", "test", "configs", "docs", "deploy",
                "services", "downloader", "schemas", ".agents", ".vscode"}
PRIVATE_SUFFIXES = {".pem", ".key", ".pfx", ".p12"}


def git(repo: Path, *args: str, data: bytes | None = None) -> bytes:
    return subprocess.run(["git", "-C", str(repo), *args], input=data,
                          stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                          check=True).stdout


def decision(path: str, size: int = 0, *, large_bytes: int = 5 * 1024**2) -> dict:
    """A location/role policy, not proof that every source file is necessary."""
    p = PurePosixPath(path)
    parts, name = p.parts, p.name
    if not parts or p.is_absolute() or ".." in parts:
        raise ValueError("expected a repository-relative path")
    first = parts[0]
    private = (name.startswith(".env") and not name.endswith((".example", ".sample", ".template"))
               or p.suffix.lower() in PRIVATE_SUFFIXES
               or name in {"id_rsa", "id_ed25519"} or ".ssh" in parts)
    if private:
        category, reason = "exclude", "private credentials or machine environment"
    elif set(parts) & CACHE_PARTS:
        category, reason = "exclude", "rebuildable cache or duplicate skill backup"
    elif (first in {"data", "artifacts", "reports", ".codex", "codex", "build", ".build", "dist"}
          or (first.startswith(("data_", "artifacts_"))
              and (len(parts) > 1 or not p.suffix))):
        category, reason = "exclude", "node-local data, generated output or tool state"
    elif path in {".stignore", ".stfolder", "json"} or name in {".DS_Store", "Thumbs.db"}:
        category, reason = "exclude", "machine-local state or accidental scratch"
    elif name.endswith((".swp", ".swo", ".tmp")):
        category, reason = "exclude", "editor or temporary output"
    elif parts[:2] == ("test", "fixtures") and p.suffix.lower() in FIXTURE_SUFFIXES:
        category, reason = "keep", "reviewed fixture input; retain minimal non-private bytes"
    elif p.suffix.lower() in PAYLOAD_SUFFIXES:
        category, reason = "exclude", "generated binary, archive, runtime log or database"
    elif first in SOURCE_ROOTS or len(parts) == 1:
        category, reason = "keep", "source/config/test/evidence candidate; inspect purpose and callers"
    else:
        category, reason = "review", "unrecognized role; inspect before staging or deleting"
    if category == "keep" and size > large_bytes:
        category, reason = "review", "large source/fixture; size alone never authorizes deletion"
    return {"decision": category, "reason": reason}


def index_entries(repo: Path) -> list[dict]:
    result = []
    for record in git(repo, "ls-files", "--stage", "-z").split(b"\0"):
        if not record:
            continue
        meta, path = record.split(b"\t", 1)
        mode, oid, stage = meta.decode().split()
        result.append({"path": path.decode("utf-8", "surrogateescape"),
                       "mode": mode, "oid": oid, "stage": int(stage)})
    return result


def object_sizes(repo: Path, oids: set[str]) -> dict[str, int]:
    if not oids:
        return {}
    raw = git(repo, "cat-file", "--batch-check=%(objectname) %(objecttype) %(objectsize)",
              data=("\n".join(sorted(oids)) + "\n").encode())
    sizes = {}
    for line in raw.decode().splitlines():
        fields = line.split()
        if len(fields) != 3:
            raise ValueError(f"missing or invalid Git object metadata: {line}")
        oid, kind, size = fields
        if kind == "blob":
            sizes[oid] = int(size)
    return sizes


def history_inventory(repo: Path, *, reflogs: bool = False) -> dict:
    scope = ["--all", *(["--reflog"] if reflogs else [])]
    raw = git(repo, "log", *scope, "--root", "-m", "--full-history", "--raw",
              "--no-abbrev", "--no-renames", "--format=", "-z")
    fields = iter(raw.split(b"\0"))
    versions: dict[str, set[str]] = defaultdict(set)
    for field in fields:
        header = field.lstrip(b"\n")
        if not header:
            continue
        if not header.startswith(b":"):
            raise ValueError("unexpected Git raw history record")
        old_mode, new_mode, old, new, _status = header[1:].split()
        path = next(fields).decode("utf-8", "surrogateescape")
        versions[path].update(oid.decode() for mode, oid in ((old_mode, old), (new_mode, new))
                              if mode != b"160000" and set(oid) != {48})
    # IDE checkpoints can reference trees directly, outside every commit graph.
    # git log --all alone does not enumerate their paths.
    non_commit_refs, blob_refs = [], set()
    for value in git(repo, "for-each-ref", "--format=%(refname) %(objecttype) %(objectname)").decode().splitlines():
        ref, kind, oid = value.split()
        if kind == "tag":
            oid = git(repo, "rev-parse", f"{ref}^{{}}").decode().strip()
            kind = git(repo, "cat-file", "-t", oid).decode().strip()
        if kind == "commit":
            continue
        row = {"ref": ref, "type": kind, "oid": oid}
        if kind == "tree":
            row.update(decision="inspect_paths", reason="standalone tree paths included in inventory")
            for record in git(repo, "ls-tree", "-rz", "--full-tree", oid).split(b"\0"):
                if not record:
                    continue
                meta, path = record.split(b"\t", 1)
                mode, _kind, blob = meta.split()
                name = path.decode("utf-8", "surrogateescape")
                versions[name]  # Retain a path-only entry for foreign gitlinks.
                if mode != b"160000":
                    versions[name].add(blob.decode())
        else:
            row.update(decision="review", reason="ref has no repository path; explicit review required")
            if kind == "blob":
                blob_refs.add(oid)
        non_commit_refs.append(row)
    sizes = object_sizes(repo, (set().union(*versions.values()) if versions else set()) | blob_refs)
    rows, excluded_oids = [], set()
    for path, ids in sorted(versions.items()):
        blobs = ids & sizes.keys()
        size = max((sizes[oid] for oid in blobs), default=0)
        row = {"path": path, "versions": len(blobs), "max_bytes": size,
               "version_bytes": sum(sizes[oid] for oid in blobs), **decision(path, size)}
        rows.append(row)
        if row["decision"] == "exclude":
            excluded_oids.update(blobs)
    return {"scope": "all refs and reflogs" if reflogs else "all local refs",
            "commits": int(git(repo, "rev-list", "--count", *scope)),
            "unique_blob_bytes": sum(sizes.values()),
            "excluded_unique_blob_bytes": sum(sizes[oid] for oid in excluded_oids),
            "note": "Logical blob bytes, not disk savings; path totals may share blobs.",
            "non_commit_refs": non_commit_refs,
            "paths": rows}


def audit(repo: Path, *, staged: bool = False, history: bool = False,
          reflogs: bool = False) -> dict:
    repo = Path(git(repo, "rev-parse", "--show-toplevel").decode().strip())
    entries = index_entries(repo)
    if any(row["stage"] for row in entries):
        raise ValueError("resolve unmerged index entries before auditing uploads")
    if staged:
        changed = set(git(repo, "diff", "--cached", "--name-only", "--diff-filter=ACMRT", "-z").split(b"\0"))
        entries = [r for r in entries if r["path"].encode("utf-8", "surrogateescape") in changed]
    sizes = object_sizes(repo, {row["oid"] for row in entries if row["mode"] != "160000"})
    tracked = [{**row, "bytes": sizes.get(row["oid"], 0),
                **decision(row["path"], sizes.get(row["oid"], 0))} for row in entries]
    untracked = []
    if not staged:
        for value in git(repo, "ls-files", "--others", "--exclude-standard", "-z").split(b"\0"):
            if value:
                path = value.decode("utf-8", "surrogateescape")
                size = (repo / path).lstat().st_size
                untracked.append({"path": path, "bytes": size, **decision(path, size)})
    report = {"contract": "git_content_role_audit_v1", "repo": str(repo),
              "head": git(repo, "rev-parse", "HEAD").decode().strip(),
              "scope": "staged content" if staged else "index and visible untracked files",
              "tracked": tracked, "untracked": untracked,
              "counts": dict(Counter(r["decision"] for r in [*tracked, *untracked])),
              "boundaries": "No deletion, secret-content scan, remote verification or proof of source necessity."}
    if history:
        report["history"] = history_inventory(repo, reflogs=reflogs)
    return report


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--repo", type=Path, default=ROOT)
    p.add_argument("--staged", action="store_true", help="Inspect index bytes, not unstaged working files.")
    p.add_argument("--history", action="store_true", help="Inventory all local refs, including deleted/renamed paths.")
    p.add_argument("--reflogs", action="store_true", help="Also inspect locally retained reflog history.")
    p.add_argument("--check", action="store_true", help="Fail if selected index entries contain excluded content.")
    p.add_argument("--output", type=Path, help="Optional private, ignored JSON evidence file.")
    args = p.parse_args(argv)
    if args.output:
        target = args.output.absolute()
        root = Path(git(args.repo, "rev-parse", "--show-toplevel").decode().strip()).resolve()
        common = Path(git(root, "rev-parse", "--path-format=absolute", "--git-common-dir").decode().strip())
        if any(part.is_symlink() for part in (target, *target.parents)):
            p.error("audit output cannot use symlinked paths")
        target = target.resolve()
        if target == root or target.is_relative_to(root / ".git") or target.is_relative_to(common):
            p.error("audit output cannot overwrite repository metadata")
        if target.exists():
            p.error("audit output already exists; choose a fresh evidence path")
        if target.is_relative_to(root):
            check = subprocess.run(["git", "-C", str(root), "check-ignore", "--quiet", "--", str(target)])
            if check.returncode:
                p.error("audit output inside checkout must be ignored")
    report = audit(args.repo, staged=args.staged, history=args.history, reflogs=args.reflogs)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        with os.fdopen(os.open(args.output, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600),
                       "w", encoding="utf-8") as stream:
            json.dump(report, stream, ensure_ascii=True, indent=2)
            stream.write("\n")
    print(json.dumps({"counts": report["counts"], "history": {
        key: value for key, value in report.get("history", {}).items() if key not in {"paths", "non_commit_refs"}
    }, "output": str(args.output) if args.output else None}, ensure_ascii=True))
    return int(args.check and any(r["decision"] == "exclude" for r in report["tracked"]))


if __name__ == "__main__":
    raise SystemExit(main())

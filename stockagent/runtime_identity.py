"""Portable runtime locks and source observations, independent of torch import.

Runtime versions are provenance. Numerical resume compatibility remains owned
by the canonical checkpoint contract; observing a Git checkout does not prove
which revision a previously started service loaded.
"""

from __future__ import annotations

from functools import lru_cache
import hashlib
from importlib import metadata
import json
import os
from pathlib import Path
import platform
import stat
import subprocess
import sys
from typing import Any, Mapping


RUNTIME_IDENTITY_VERSION = 1


def identity_sha256(value: Any) -> str:
    body = json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"), allow_nan=False).encode("utf-8")
    return hashlib.sha256(body).hexdigest()


def runtime_identity() -> dict[str, Any]:
    """Capture installed distribution versions without importing GPU libraries."""
    packages: dict[str, str] = {}
    observed_versions: dict[str, set[str]] = {}
    metadata_errors: list[dict[str, str]] = []
    non_distribution_entries: list[dict[str, str]] = []
    for distribution in metadata.distributions():
        try:
            package_metadata = distribution.metadata
            if "Name" not in package_metadata or "Version" not in package_metadata:
                raise ValueError("distribution has no name/version metadata")
            name, version = package_metadata["Name"], package_metadata["Version"]
            if not isinstance(name, str) or not name or not isinstance(version, str) or not version:
                raise ValueError("distribution has no name/version metadata")
            normalized = name.lower().replace("_", "-").replace(".", "-")
            observed_versions.setdefault(normalized, set()).add(version)
            packages.setdefault(normalized, version)
        except (OSError, ValueError, DeprecationWarning) as exc:
            # A cached third-party DistributionFinder can retain removed
            # temporary metadata. Preserve the incomplete observation rather
            # than aborting unrelated training or silently claiming a lock.
            path = Path(str(getattr(distribution, "_path", "unknown")))
            reason = None
            if path.name.endswith((".dist-info", ".egg-info")):
                try:
                    if not path.exists():
                        reason = "stale_finder_entry"
                    elif not path.is_symlink() and path.is_dir() and not any(path.iterdir()):
                        reason = "empty_directory"
                except OSError:
                    pass
            if reason is not None:
                non_distribution_entries.append({"entry": path.name, "reason": reason})
            else:
                metadata_errors.append({"entry": path.name, "error_type": type(exc).__name__})
    duplicates = {name: sorted(versions) for name, versions in observed_versions.items()
                  if len(versions) > 1}
    for name in duplicates:
        try:
            packages[name] = metadata.version(name)
        except (OSError, ValueError, DeprecationWarning) as exc:
            metadata_errors.append({"entry": name, "error_type": type(exc).__name__})
    contract = {
        "schema_version": RUNTIME_IDENTITY_VERSION,
        "python": platform.python_version(),
        "implementation": platform.python_implementation(),
        "machine": platform.machine(),
        "system": platform.system(),
        "packages": dict(sorted(packages.items())),
        "duplicate_distribution_versions": dict(sorted(duplicates.items())),
    }
    if metadata_errors:
        contract["metadata_errors"] = sorted(
            metadata_errors, key=lambda item: (item["entry"], item["error_type"])
        )
    if non_distribution_entries:
        contract["non_distribution_metadata_entries"] = sorted(
            non_distribution_entries, key=lambda item: (item["entry"], item["reason"])
        )
    return {**contract, "sha256": identity_sha256(contract)}


def validate_runtime_lock(expected: Mapping[str, Any], actual: Mapping[str, Any]) -> list[str]:
    """Require the exact recorded runtime; produce names, never credentials."""
    body = {key: value for key, value in expected.items() if key != "sha256"}
    if expected.get("schema_version") != RUNTIME_IDENTITY_VERSION:
        return ["unsupported runtime identity schema"]
    if not isinstance(expected.get("sha256"), str) or expected["sha256"] != identity_sha256(body):
        return ["runtime lock checksum mismatch"]
    differences = []
    if expected.get("metadata_errors") or actual.get("metadata_errors"):
        differences.append("runtime distribution metadata observation is incomplete")
    if expected.get("non_distribution_metadata_entries", []) != actual.get("non_distribution_metadata_entries", []):
        differences.append("non-distribution metadata artifact inventory differs")
    if expected.get("duplicate_distribution_versions", {}) != actual.get("duplicate_distribution_versions", {}):
        differences.append("duplicate distribution inventory differs")
    for key in ("python", "implementation", "machine", "system"):
        if expected.get(key) != actual.get(key):
            differences.append(f"{key}: expected {expected.get(key)!r}, observed {actual.get(key)!r}")
    left, right = expected.get("packages"), actual.get("packages")
    if not isinstance(left, Mapping) or not isinstance(right, Mapping):
        return differences + ["runtime packages must be objects"]
    for name in sorted(set(left) | set(right)):
        if left.get(name) != right.get(name):
            differences.append(f"package {name}: expected {left.get(name)!r}, observed {right.get(name)!r}")
    return differences


def source_identity(root: Path, *, allow_tracked_deletions: bool = False) -> dict[str, Any]:
    """Hash code and deployment inputs; refuse changing or external files."""
    root = root.resolve()
    patterns = ["stockagent/*.py", "downloader/*.py", "scripts/*.py", "scripts/*.sh",
                "scripts/*.ps1", "services/*.py", "services/public_dashboards/*",
                "services/*.html", "services/*.css", "services/*.js", "services/*.mjs", "services/*.ts",
                "deploy/systemd/*.in",
                "schemas/public/*.json", "package.json", "package-lock.json",
                "train.py", "plot_epoch_curves.py", "coda_runner.sh", "pyproject.toml", "uv.lock", "requirements*.txt", "README.md"]
    result = subprocess.run(
        ["git", "ls-files", "--cached", "--others", "--exclude-standard", "-z", "--", *patterns],
        cwd=root, capture_output=True, check=True,
    )
    paths = sorted(set(name for name in result.stdout.decode().split("\0") if name))
    deleted = set()
    if allow_tracked_deletions:
        deleted = set(subprocess.run(["git", "ls-files", "--deleted", "-z", "--", *patterns],
            cwd=root, capture_output=True, check=True).stdout.decode().split("\0")) - {""}
    files: dict[str, str] = {}
    for name in paths:
        if name in deleted and not (root / name).is_symlink():
            continue
        files[name] = stable_source_sha256(root, name)
    head = subprocess.run(["git", "rev-parse", "HEAD"], cwd=root,
                          capture_output=True, check=True, text=True).stdout.strip()
    return {"schema_version": 1, "git_head": head, "source_sha256": identity_sha256(files),
            "files": files, "scope": "repository code, public frontend assets, systemd templates and packaging metadata",
            "loaded_service_revision_verified": False,
            **({"deleted_tracked_paths": sorted(deleted)} if allow_tracked_deletions else {})}


def stable_source_sha256(root: Path, name: str) -> str:
    """Verify a regular source file without accepting path escape or mutation."""
    from pathlib import PurePosixPath

    relative = PurePosixPath(name)
    if (not name or relative.is_absolute() or ".." in relative.parts
            or "\\" in name or str(relative) != name):
        raise ValueError(f"source escapes repository: {name}")
    path = root / name
    if not path.resolve().is_relative_to(root):
        raise ValueError(f"source escapes repository: {name}")
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(descriptor, "rb") as stream:
        before = os.fstat(stream.fileno())
        if not stat.S_ISREG(before.st_mode):
            raise ValueError(f"source is not a regular file: {name}")
        digest = hashlib.file_digest(stream, "sha256").hexdigest()
        after = path.stat()
    if (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns) != (
        after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns
    ):
        raise RuntimeError(f"source changed during observation: {name}")
    return digest


def verify_source_release(receipt: Path, root: Path) -> dict[str, Any]:
    """Verify an existing frozen code tree without Git or data publication."""
    root = root.resolve()
    raw = receipt.read_bytes()
    value = json.loads(raw)
    if value.get("schema_version") != 1 or value.get("state") != "built":
        raise ValueError("unsupported code release receipt")
    code = value.get("code", {})
    files = code.get("files")
    bundled = value.get("source_bundle", {}).get("files")
    if not isinstance(files, dict) or not files or not isinstance(bundled, dict):
        raise ValueError("code release has no source inventory")
    if code.get("source_sha256") != identity_sha256(files):
        raise ValueError("code source inventory checksum mismatch")
    if any(bundled.get(name) != digest for name, digest in files.items()):
        raise ValueError("source bundle differs from code inventory")
    for name, digest in bundled.items():
        if stable_source_sha256(root, name) != digest:
            raise ValueError(f"code release source mismatch: {name}")
    return {
        "schema_version": 1,
        "receipt_sha256": hashlib.sha256(raw).hexdigest(),
        "source_sha256": code["source_sha256"],
        "git_head": code.get("git_head"),
        "source_bundle_sha256": value["source_bundle"]["sha256"],
        "selected_config": value.get("selected_config"),
        "verified_file_count": len(bundled),
        "source_files_verified": True,
        "loaded_service_revision_verified": False,
        "data_publication": False,
        "scope": "frozen files observed at startup; imported-code coverage is not proven",
    }


def verify_release_bundles(receipt: Path) -> dict[str, bool]:
    """Check retained wheel/source ZIP identity before rebuilding or extracting."""
    value = json.loads(receipt.read_bytes())
    if value.get("schema_version") != 1 or value.get("state") != "built":
        raise ValueError("unsupported code release receipt")
    for kind in ("wheel", "source_bundle"):
        artifact = value[kind]
        name = artifact["file"]
        if stable_source_sha256(receipt.parent.resolve(), name) != artifact["sha256"]:
            raise ValueError(f"code release {kind} checksum mismatch")
        if (receipt.parent / name).stat().st_size != artifact["bytes"]:
            raise ValueError(f"code release {kind} size mismatch")
    return {"bundles_verified": True}


@lru_cache(maxsize=1)
def training_runtime_provenance() -> dict[str, Any]:
    """One immutable runtime observation per training process, not per epoch."""
    runtime = runtime_identity()
    torch = sys.modules.get("torch")
    cuda = getattr(getattr(torch, "version", None), "cuda", None)
    receipt = os.environ.get("STOCKAGENT_CODE_RELEASE_RECEIPT")
    source = (verify_source_release(Path(receipt), Path(__file__).resolve().parents[1])
              if receipt else None)
    return {"schema_version": 1, "runtime": runtime,
            "source_release": source, "torch_cuda_build": cuda,
            "resume_policy": "canonical_checkpoint_contract"}

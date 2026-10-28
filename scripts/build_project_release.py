#!/usr/bin/env python3
"""Build a private wheel plus exact code/runtime receipt without changing runtime.

Deployments retain the canonical checkout/config/CLI contracts. This is a code
artifact, not a data publication, source release, CUDA image or loaded-service
revision claim.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time
import zipfile

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from downloader.artifact_io import atomic_write_json  # noqa: E402
from stockagent.runtime_identity import (  # noqa: E402
    identity_sha256, runtime_identity, source_identity, stable_source_sha256,
    verify_release_bundles, verify_source_release,
)
from stockagent.storage_layout import admit_output  # noqa: E402
from stockagent.config import _CONFIG_INHERITANCE_KEYS, _normalize_base_config_refs, _UniqueKeySafeLoader, load_config  # noqa: E402
import yaml  # noqa: E402


def _config_files(root: Path, entry: Path) -> dict[str, bytes]:
    load_config(entry if entry.is_absolute() else root / entry)
    files = {}
    pending = [entry if entry.is_absolute() else root / entry]
    while pending:
        path = pending.pop().resolve()
        if not path.is_relative_to(root / "configs"):
            raise ValueError("release config dependencies must remain in repository configs")
        name = str(path.relative_to(root))
        if name in files:
            continue
        body = path.read_bytes()
        files[name] = body
        payload = yaml.load(body, Loader=_UniqueKeySafeLoader)
        if not isinstance(payload, dict):
            raise ValueError(f"invalid release config: {name}")
        for key in _CONFIG_INHERITANCE_KEYS:
            pending.extend(path.parent / ref for ref in _normalize_base_config_refs(payload.get(key)))
    return files


def build_release(root: Path, output: Path, *, uv: str, config: Path | None = None) -> dict:
    output = admit_output(output, "code")
    os.umask(0o077)
    root = root.resolve()
    started = time.perf_counter()
    before = source_identity(root)
    source_date_epoch = int(subprocess.run(
        ["git", "show", "-s", "--format=%ct", "HEAD"], cwd=root,
        capture_output=True, text=True, check=True,
    ).stdout.strip())
    build_env = {**os.environ, "SOURCE_DATE_EPOCH": str(source_date_epoch)}
    configs = _config_files(root, config) if config is not None else {}
    runtime = runtime_identity()
    if runtime.get("metadata_errors"):
        raise ValueError("runtime distribution metadata observation is incomplete")
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    destination = output / f"{timestamp}-{before['source_sha256'][:12]}"
    destination.mkdir(parents=True, mode=0o700, exist_ok=False)
    # Build only the receipt-owned files in a clean tree. Existing build/
    # permissions, egg-info inventories and ignored sources are not inputs.
    build_source = destination / "build-source"
    build_source.mkdir(mode=0o700)
    frozen = {}
    for name, digest in before["files"].items():
        body = (root / name).read_bytes()
        if hashlib.sha256(body).hexdigest() != digest:
            raise RuntimeError(f"source changed while staging: {name}")
        frozen[name] = body
    frozen.update(configs)
    for name, body in frozen.items():
        path = build_source / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(body)
    with (destination / "build.log").open("w", encoding="utf-8") as log:
        subprocess.run([uv, "build", "--wheel", "--offline", "--no-build-isolation", "--python", sys.executable,
                        "--out-dir", str(destination.resolve())],
                       cwd=build_source, env=build_env, stdout=log, stderr=subprocess.STDOUT, check=True)
    after = source_identity(root)
    if before != after:
        raise RuntimeError("repository sources changed during wheel build; release rejected")
    wheels = list(destination.glob("*.whl"))
    if len(wheels) != 1:
        raise RuntimeError("release must contain exactly one wheel")
    wheel = wheels[0]
    with zipfile.ZipFile(wheel) as archive:
        names = archive.namelist()
        if archive.testzip() is not None or "stockagent/training/lifecycle.py" not in names:
            raise RuntimeError("wheel is incomplete or corrupt")
        forbidden = ("data_", "artifacts/", "configs/", ".env", ".git/")
        if any(name.startswith(forbidden) for name in names):
            raise RuntimeError("wheel contains private/runtime data")
        expected_python = {name for name in before["files"]
                           if name.endswith(".py") and (name in {"train.py", "plot_epoch_curves.py"} or name.startswith(("stockagent/", "downloader/", "scripts/")))}
        if {name for name in names if name.endswith(".py")} != expected_python:
            raise RuntimeError("wheel contains missing or unrecorded Python sources")
        for name, digest in before["files"].items():
            if name.endswith(".py") and (name in {"train.py", "plot_epoch_curves.py"} or name.startswith(("stockagent/", "downloader/", "scripts/"))):
                if name not in names or hashlib.sha256(archive.read(name)).hexdigest() != digest:
                    raise RuntimeError(f"wheel source mismatch: {name}")
    if configs != (_config_files(root, config) if config is not None else {}):
        raise RuntimeError("configuration changed during build; release rejected")
    bundle = destination / "source.zip"
    bundle_hashes = dict(before["files"])
    bundle_hashes.update({name: hashlib.sha256(body).hexdigest() for name, body in configs.items()})
    with zipfile.ZipFile(bundle, "x", compression=zipfile.ZIP_DEFLATED) as archive:
        for name, digest in sorted(bundle_hashes.items()):
            body = frozen[name]
            if hashlib.sha256(body).hexdigest() != digest:
                raise RuntimeError(f"source changed while freezing: {name}")
            info = zipfile.ZipInfo(name, date_time=datetime.fromtimestamp(
                max(315532800, source_date_epoch), timezone.utc).timetuple()[:6])
            info.compress_type = zipfile.ZIP_DEFLATED
            info.external_attr = 0o100644 << 16
            archive.writestr(info, body)
    receipt = {
        "schema_version": 1, "state": "built", "created_at_utc": timestamp,
        "code": before, "runtime": runtime,
        "wheel": {"file": wheel.name, "bytes": wheel.stat().st_size,
                  "sha256": hashlib.sha256(wheel.read_bytes()).hexdigest()},
        "source_bundle": {"file": bundle.name, "bytes": bundle.stat().st_size,
                          "sha256": hashlib.sha256(bundle.read_bytes()).hexdigest(), "files": bundle_hashes},
        "selected_config": str(config) if config is not None else None,
        "build_environment": {"SOURCE_DATE_EPOCH": source_date_epoch, "umask": "0077"},
        "build_wall_s": round(time.perf_counter()-started, 3),
        "scope": "Python wheel, frozen code/config dependencies and runtime observation; data excluded",
        "data_publication": False, "deployment_verified": False,
    }
    atomic_write_json(destination / "release.json", receipt)
    return {"receipt": str(destination / "release.json"), "wheel": str(wheel),
            "source_sha256": before["source_sha256"], "runtime_sha256": runtime["sha256"]}


def build_overlay_release(base_receipt: Path, override_root: Path, output: Path, *,
                          overrides: tuple[str, ...], uv: str,
                          additions: tuple[str, ...] = ()) -> dict:
    """Build an explicitly scoped source overlay on a verified frozen release.

    Replacements must already exist and additions must be explicitly named new
    Python modules. All other bytes are copied from the verified base; unrelated
    checkout changes are never promoted. This is not a deployment.
    """
    base_receipt = base_receipt.resolve(strict=True)
    base_root = base_receipt.parent / "build-source"
    override_root = override_root.resolve(strict=True)
    output = admit_output(output, "code")
    selected = tuple(overrides)
    base_proof = verify_source_release(base_receipt, base_root)
    verify_release_bundles(base_receipt)
    base = json.loads(base_receipt.read_bytes())
    code_files = dict(base["code"]["files"])
    bundle_files = dict(base["source_bundle"]["files"])
    added = tuple(additions)
    if (not selected and not added) or len(set(selected)) != len(selected) or not set(selected) <= code_files.keys():
        raise ValueError("overlay must name unique existing frozen code members")
    if (len(set(added)) != len(added) or set(added) & bundle_files.keys()
            or set(added) & set(selected) or any(
                not name.startswith(("stockagent/", "downloader/", "scripts/"))
                or not name.endswith(".py") or any(part.startswith(".") for part in Path(name).parts)
                for name in added)):
        raise ValueError("overlay additions must name unique new canonical Python modules")
    selected_hashes = {name: stable_source_sha256(override_root, name) for name in (*selected, *added)}
    override_hashes = {name: selected_hashes[name] for name in selected}
    addition_hashes = {name: selected_hashes[name] for name in added}
    code_files.update(selected_hashes)
    bundle_files.update(selected_hashes)
    source_hash = identity_sha256(code_files)
    runtime = runtime_identity()
    if runtime.get("metadata_errors"):
        raise ValueError("runtime distribution metadata observation is incomplete")
    epoch = int(base.get("build_environment", {}).get("SOURCE_DATE_EPOCH", 315532800))
    epoch = max(315532800, epoch)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    destination = output / f"{timestamp}-{source_hash[:12]}"
    os.umask(0o077)
    destination.mkdir(parents=True, mode=0o700, exist_ok=False)
    build_source = destination / "build-source"
    build_source.mkdir(mode=0o700)
    started = time.perf_counter()
    for name, digest in sorted(bundle_files.items()):
        source = override_root if name in selected_hashes else base_root
        if stable_source_sha256(source, name) != digest:
            raise RuntimeError(f"overlay input changed while staging: {name}")
        target = build_source / name
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source / name, target)
        if stable_source_sha256(build_source, name) != digest:
            raise RuntimeError(f"overlay copy differs from input: {name}")
    with (destination / "build.log").open("w", encoding="utf-8") as log:
        subprocess.run([uv, "build", "--wheel", "--offline", "--no-build-isolation", "--python", sys.executable,
                        "--out-dir", str(destination.resolve())], cwd=build_source,
                       env={**os.environ, "SOURCE_DATE_EPOCH": str(epoch)},
                       stdout=log, stderr=subprocess.STDOUT, check=True)
    wheels = list(destination.glob("*.whl"))
    if len(wheels) != 1:
        raise RuntimeError("overlay must contain exactly one wheel")
    wheel = wheels[0]
    with zipfile.ZipFile(wheel) as archive:
        names = archive.namelist()
        expected_python = {name for name in code_files if name.endswith(".py") and
            (name in {"train.py", "plot_epoch_curves.py"} or name.startswith(("stockagent/", "downloader/", "scripts/")))}
        if (archive.testzip() is not None or "stockagent/training/lifecycle.py" not in names
                or {name for name in names if name.endswith(".py")} != expected_python):
            raise RuntimeError("overlay wheel is corrupt or has an unexpected Python inventory")
        for name in expected_python:
            if hashlib.sha256(archive.read(name)).hexdigest() != code_files[name]:
                raise RuntimeError(f"overlay wheel source mismatch: {name}")
    bundle = destination / "source.zip"
    with zipfile.ZipFile(bundle, "x", compression=zipfile.ZIP_DEFLATED) as archive:
        for name, digest in sorted(bundle_files.items()):
            body = (build_source / name).read_bytes()
            if hashlib.sha256(body).hexdigest() != digest:
                raise RuntimeError(f"staged overlay changed before bundling: {name}")
            info = zipfile.ZipInfo(name, date_time=datetime.fromtimestamp(epoch, timezone.utc).timetuple()[:6])
            info.compress_type = zipfile.ZIP_DEFLATED
            info.external_attr = 0o100644 << 16
            archive.writestr(info, body)
    if (verify_source_release(base_receipt, base_root) != base_proof
            or {name: stable_source_sha256(override_root, name) for name in (*selected, *added)} != selected_hashes):
        raise RuntimeError("overlay input changed during build; release rejected")
    verify_release_bundles(base_receipt)
    code = dict(base["code"], files=code_files, source_sha256=source_hash)
    receipt = {
        "schema_version": 1, "state": "built", "created_at_utc": timestamp,
        "code": code, "runtime": runtime,
        "wheel": {"file": wheel.name, "bytes": wheel.stat().st_size,
                  "sha256": hashlib.sha256(wheel.read_bytes()).hexdigest()},
        "source_bundle": {"file": bundle.name, "bytes": bundle.stat().st_size,
                          "sha256": hashlib.sha256(bundle.read_bytes()).hexdigest(), "files": bundle_files},
        "selected_config": base.get("selected_config"),
        "build_environment": {"SOURCE_DATE_EPOCH": epoch, "umask": "0077"},
        "source_overlay": {"contract": "verified_base_explicit_source_overlay_v1",
            "base_receipt": str(base_receipt), "base_receipt_sha256": base_proof["receipt_sha256"],
            "base_source_sha256": base_proof["source_sha256"], "overrides": override_hashes,
            "additions": addition_hashes,
            "unselected_workspace_changes_promoted": False},
        "builder_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "build_wall_s": round(time.perf_counter() - started, 3),
        "scope": "explicit frozen code overlay and runtime observation; data excluded",
        "data_publication": False, "deployment_verified": False,
    }
    receipt_path = destination / "release.json"
    atomic_write_json(receipt_path, receipt)
    verify_source_release(receipt_path, build_source)
    verify_release_bundles(receipt_path)
    return {"receipt": str(receipt_path), "wheel": str(wheel),
            "source_sha256": source_hash, "runtime_sha256": runtime["sha256"]}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--uv", default=shutil.which("uv"))
    parser.add_argument("--config", type=Path, help="Freeze the selected repository YAML dependency chain")
    parser.add_argument("--base-receipt", type=Path, help="Verified frozen source release for an explicit overlay")
    parser.add_argument("--overlay-root", type=Path, help="Root containing the selected replacement files")
    parser.add_argument("--overlay", action="append", default=[], help="Existing code member to replace; repeat as needed")
    parser.add_argument("--add-source", action="append", default=[], help="Explicit new Python module; repeat as needed")
    args = parser.parse_args()
    if not args.uv:
        parser.error("uv is required; pass --uv with the installed tool executable")
    if args.base_receipt is not None:
        if args.overlay_root is None or not (args.overlay or args.add_source) or args.config is not None:
            parser.error("overlay builds require --overlay-root and selected --overlay/--add-source, without --config")
        result = build_overlay_release(args.base_receipt, args.overlay_root, args.output_dir,
                                       overrides=tuple(args.overlay), additions=tuple(args.add_source), uv=args.uv)
    else:
        if args.overlay_root is not None or args.overlay or args.add_source:
            parser.error("--overlay-root/--overlay/--add-source require --base-receipt")
        result = build_release(REPO_ROOT, args.output_dir, uv=args.uv, config=args.config)
    print(json.dumps(result, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

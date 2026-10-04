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
from stockagent.runtime_identity import runtime_identity, source_identity  # noqa: E402
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


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--uv", default=shutil.which("uv"))
    parser.add_argument("--config", type=Path, help="Freeze the selected repository YAML dependency chain")
    args = parser.parse_args()
    if not args.uv:
        parser.error("uv is required; pass --uv with the installed tool executable")
    print(json.dumps(build_release(REPO_ROOT, args.output_dir, uv=args.uv, config=args.config), ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

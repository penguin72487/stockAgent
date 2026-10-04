#!/usr/bin/env python3
"""Rebuild and install one recorded release outside its original checkout.

This uses the selected existing Python, never installs into its environment,
and retains every diagnostic file. It validates code artifacts, not a running
service, data publication or a CUDA image.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import shutil
import stat
import subprocess
import sys
import time
import zipfile

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from downloader.artifact_io import atomic_write_json  # noqa: E402
from stockagent.runtime_identity import verify_release_bundles, verify_source_release  # noqa: E402


def extract_recorded_sources(receipt: Path, destination: Path) -> None:
    """Only extract the unique regular files listed by the verified receipt."""
    release = json.loads(receipt.read_bytes())
    expected = release["source_bundle"]["files"]
    verify_release_bundles(receipt)
    with zipfile.ZipFile(receipt.parent / release["source_bundle"]["file"]) as archive:
        infos = archive.infolist()
        if len(infos) != len(expected) or {entry.filename for entry in infos} != set(expected):
            raise ValueError("source ZIP differs from receipt inventory")
        # Admit all names before the first write. A rejected ZIP leaves no
        # partially extracted file outside the isolated work directory.
        for entry in infos:
            name = entry.filename
            relative = PurePosixPath(name)
            if (relative.is_absolute() or ".." in relative.parts or "\\" in name
                    or str(relative) != name or not name
                    or entry.is_dir() or stat.S_ISLNK(entry.external_attr >> 16)):
                raise ValueError(f"invalid source ZIP entry: {name}")
        destination.mkdir(mode=0o700, parents=True, exist_ok=False)
        for entry in infos:
            body = archive.read(entry)
            if hashlib.sha256(body).hexdigest() != expected[entry.filename]:
                raise ValueError(f"source ZIP file checksum mismatch: {entry.filename}")
            path = destination / entry.filename
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(body)
    verify_source_release(receipt, destination)


def accept_release(receipt: Path, work: Path, *, uv: str) -> dict:
    started = time.perf_counter()
    receipt = receipt.resolve(strict=True)
    release = json.loads(receipt.read_bytes())
    os.umask(int(release["build_environment"]["umask"], 8))
    work = work.resolve()
    work.mkdir(mode=0o700, parents=True, exist_ok=False)
    source, rebuilt, site, outside = (work / name for name in (
        "source", "rebuilt-wheel", "wheel-site", "outside",
    ))
    outside.mkdir(mode=0o700)
    extract_recorded_sources(receipt, source)
    env = {**os.environ,
           "SOURCE_DATE_EPOCH": str(release["build_environment"]["SOURCE_DATE_EPOCH"])}
    with (work / "acceptance.log").open("w", encoding="utf-8") as log:
        def run(args: list[str], *, cwd: Path, environment: dict) -> None:
            subprocess.run(args, cwd=cwd, env=environment, stdout=log,
                           stderr=subprocess.STDOUT, check=True, timeout=180)

        run([uv, "build", "--wheel", "--offline", "--no-build-isolation", "--python", sys.executable,
             "--out-dir", str(rebuilt)], cwd=source, environment=env)
        wheel = rebuilt / release["wheel"]["file"]
        if hashlib.sha256(wheel.read_bytes()).hexdigest() != release["wheel"]["sha256"]:
            raise ValueError("rebuilt wheel differs from recorded wheel")
        run([sys.executable, "-m", "pip", "install", "--no-index", "--no-deps", "--target",
             str(site), str(wheel)], cwd=outside, environment=env)
        wheel_env = {**env, "PYTHONPATH": str(site), "PYTHONNOUSERSITE": "1"}
        wheel_env.pop("STOCKAGENT_CODE_RELEASE_RECEIPT", None)
        probe = (
            "from pathlib import Path; import sys,stockagent,downloader,scripts,train,plot_epoch_curves; "
            "root=Path(sys.argv[1]).resolve(); "
            "assert all(Path(m.__file__).resolve().is_relative_to(root) "
            "for m in (stockagent,downloader,scripts,train,plot_epoch_curves))"
        )
        run([sys.executable, "-c", probe, str(site)], cwd=outside, environment=wheel_env)
        run([sys.executable, "-m", "train", "--help"], cwd=outside, environment=wheel_env)
        source_env = {**env, "PYTHONPATH": str(source),
                      "STOCKAGENT_CODE_RELEASE_RECEIPT": str(receipt)}
        source_probe = (
            "from stockagent.runtime_identity import training_runtime_provenance; "
            "p=training_runtime_provenance(); assert p['source_release']['source_files_verified']"
        )
        run([sys.executable, "-c", source_probe], cwd=source, environment=source_env)
    result = {
        "schema_version": 1, "state": "accepted", "release_receipt": str(receipt),
        "release_receipt_sha256": hashlib.sha256(receipt.read_bytes()).hexdigest(),
        "source_sha256": release["code"]["source_sha256"],
        "verified_files": len(release["source_bundle"]["files"]),
        "wheel_sha256": release["wheel"]["sha256"], "wheel_rebuilt_exactly": True,
        "wheel_imports_outside_checkout": True, "train_help_exit": 0,
        "startup_source_provenance_verified": True,
        "wall_s": round(time.perf_counter() - started, 3),
        "scope": "frozen source, exact rebuilt wheel, target installation/imports and CLI; not service/data/CUDA-image acceptance",
    }
    atomic_write_json(work / "packaging-acceptance.json", result)
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("receipt", type=Path)
    parser.add_argument("--work-dir", type=Path)
    parser.add_argument("--uv", default=shutil.which("uv"))
    args = parser.parse_args()
    if not args.uv:
        parser.error("uv is required")
    work = args.work_dir or args.receipt.parent / (
        "acceptance-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    )
    result = accept_release(args.receipt, work, uv=args.uv)
    print(json.dumps({key: result[key] for key in (
        "state", "wheel_rebuilt_exactly", "verified_files", "train_help_exit", "wall_s",
    )}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

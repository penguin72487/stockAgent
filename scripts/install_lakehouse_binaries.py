#!/usr/bin/env python3
"""Install exact official Linux x86_64 binaries into a fresh private prefix."""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import hashlib
import io
import json
from pathlib import Path
import platform
import subprocess
import sys
import tarfile
import urllib.request
import zipfile

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from downloader.artifact_io import atomic_write_bytes, atomic_write_json

RELEASES = {
    "temporal": {
        "version": "1.32.0",
        "url": "https://github.com/temporalio/temporal/releases/download/v1.32.0/temporal_1.32.0_linux_amd64.tar.gz",
        "sha256": "ca1ccbb1d1545b68eb4523de463c51ffcd80f7e0bccd14a9b2c56fc7e389e792",
    },
    "rclone": {
        "version": "1.75.1",
        "url": "https://github.com/rclone/rclone/releases/download/v1.75.1/rclone-v1.75.1-linux-amd64.zip",
        "sha256": "982b5aa772841168f8e380f139e9e787b2a105403e32b94da8676a0e1c0a13ab",
    },
}


def install_one(name: str, output: Path) -> dict:
    release = RELEASES[name]
    request = urllib.request.Request(release["url"], headers={"User-Agent": "StockAgent-lakehouse-deployment"})
    with urllib.request.urlopen(request, timeout=60) as response:
        raw = response.read(256 * 1024**2 + 1)
    if len(raw) > 256 * 1024**2 or hashlib.sha256(raw).hexdigest() != release["sha256"]:
        raise ValueError("official binary archive differs from the recorded release SHA-256")
    atomic_write_bytes(output / release["url"].rsplit("/", 1)[1], raw)
    members = {}
    if name == "rclone":
        with zipfile.ZipFile(io.BytesIO(raw)) as archive:
            paths = [p for p in archive.namelist() if p.endswith("/rclone")]
            if len(paths) != 1:
                raise ValueError("unexpected official rclone executable set")
            members["rclone"] = archive.read(paths[0])
    else:
        with tarfile.open(fileobj=io.BytesIO(raw), mode="r:gz") as archive:
            for member in archive.getmembers():
                if member.isfile() and Path(member.name).name in {"temporal-server", "temporal-sql-tool", "tctl"}:
                    executable = Path(member.name).name
                    if executable in members:
                        raise ValueError("duplicate official executable")
                    members[executable] = archive.extractfile(member).read()
        if not {"temporal-server", "temporal-sql-tool"}.issubset(members):
            raise ValueError("production Temporal server/schema executables missing")
    receipts = {}
    for executable, body in members.items():
        path = output / "bin" / executable
        atomic_write_bytes(path, body)
        path.chmod(0o755)
        receipts[executable] = {"sha256": hashlib.sha256(body).hexdigest(), "bytes": len(body)}
    return {**release, "executables": receipts}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if platform.system() != "Linux" or platform.machine() != "x86_64":
        raise ValueError("binary pins apply only to Linux x86_64")
    output = args.output.absolute()
    output.mkdir(parents=True, mode=0o700, exist_ok=False)
    (output / "bin").mkdir(mode=0o755)
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = {name: pool.submit(install_one, name, output) for name in RELEASES}
        receipts = {name: future.result() for name, future in futures.items()}
    versions = {}
    for name, argv in {"rclone": ["version"], "temporal-server": ["--version"]}.items():
        versions[name] = subprocess.run([str(output / "bin" / name), *argv], check=True,
                                       capture_output=True, text=True, timeout=10).stdout.strip()
    result = {"state": "accepted", "observed_at_utc": datetime.now(timezone.utc).isoformat(),
              "platform": "linux-amd64", "releases": receipts, "versions": versions}
    atomic_write_json(output / "acceptance.json", result)
    print(json.dumps(result))


if __name__ == "__main__":
    main()

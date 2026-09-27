"""Metadata identity for the canonical Bybit daily-materialization inputs.

This guards ordinary replacement and in-place writes across a build/publication
transaction. It does not replace the packed publisher's complete content hashes.
"""

from __future__ import annotations

import hashlib
from pathlib import Path
import stat


def materialization_input_signature(root: Path) -> dict[str, object]:
    digest = hashlib.sha256()
    paths = [
        *(root / "data_bybit/1m").glob("*_features.parquet"),
        *(root / "data_bybit/1m/_hot_tail").glob("*_features.parquet"),
        *(root / "data_bybit/funding").glob("*_funding.parquet"),
        root / "data_bybit/funding/instruments.csv",
        root / "data_bybit/funding/funding_coverage.csv",
    ]
    for path in sorted(paths):
        try:
            info = path.stat(follow_symlinks=False)
        except FileNotFoundError:
            raise RuntimeError(f"Bybit materialization input disappeared: {path}") from None
        if stat.S_ISLNK(info.st_mode):
            raise RuntimeError(f"Bybit materialization input is a symlink: {path}")
        if not stat.S_ISREG(info.st_mode):
            raise RuntimeError(f"Bybit materialization input is not a regular file: {path}")
        identity = (
            path.relative_to(root), info.st_dev, info.st_ino, info.st_size,
            info.st_mtime_ns, info.st_ctime_ns,
        )
        digest.update(repr(identity).encode("utf-8"))
        digest.update(b"\n")
    return {"files": len(paths), "metadata_sha256": digest.hexdigest()}

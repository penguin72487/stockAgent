#!/usr/bin/env python3
"""Verify a frozen code tree and its release files without modifying it."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from downloader.artifact_io import atomic_write_json  # noqa: E402
from stockagent.runtime_identity import verify_release_bundles, verify_source_release  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("receipt", type=Path)
    parser.add_argument("--root", type=Path, default=ROOT)
    parser.add_argument("--verify-bundles", action="store_true")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    result = verify_source_release(args.receipt, args.root)
    if args.verify_bundles:
        result.update(verify_release_bundles(args.receipt))
    if args.output:
        atomic_write_json(args.output, result)
    print(json.dumps(result, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Prepare/verify a Discord code release; never reload services or change data."""
from __future__ import annotations
import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from stockagent.live.service_code_release import freeze_source, namespace_identity, verify_frozen_source
from stockagent.runtime_identity import identity_sha256
from downloader.artifact_io import atomic_write_json


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--verify", type=Path)
    parser.add_argument("--base-release", type=Path,
                        help="Derive from this verified compatible release, not the entire development tree")
    parser.add_argument("--include-source", action="append", default=[],
                        help="Explicit source member to overlay from the current workspace; repeat for each reviewed file")
    parser.add_argument("--include-root", type=Path,
                        help="Use reviewed copies below private artifacts/operations for the explicit members")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.verify and (args.base_release or args.include_source or args.include_root):
        parser.error("verify and source overlays are separate operations")
    if bool(args.base_release) != bool(args.include_source):
        parser.error("base-release and at least one include-source are required together")
    if args.include_root and not args.base_release:
        parser.error("include-root requires a verified base-release and explicit include-source members")
    if args.output.exists():
        raise FileExistsError("preserve source release preparation evidence")
    receipt = args.verify or freeze_source(ROOT, ROOT / "artifacts/operations/service_code_releases",
        base_receipt=args.base_release, include_sources=tuple(args.include_source), include_root=args.include_root)
    proof = verify_frozen_source(receipt, ROOT)
    native = namespace_identity(receipt, ROOT)
    result = {"status": "prepared_not_adopted", "release": proof, "namespace_identity": native,
              "code_sha256": identity_sha256({"code": native["code"]}), "provider_requests": 0,
              "service_restarts": 0, "ledger_writes": 0}
    atomic_write_json(args.output, result)
    print(json.dumps({k: v for k, v in result.items() if k != "namespace_identity"}))


if __name__ == "__main__":
    main()

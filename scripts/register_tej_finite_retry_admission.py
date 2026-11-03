#!/usr/bin/env python3
"""Enroll a source-bound finite TEJ plan without issuing a provider query."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

from downloader.artifact_io import atomic_write_json
from downloader.tej_finite_retry import enroll


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=REPO / 'data_tej')
    parser.add_argument('--plan', type=Path, required=True)
    parser.add_argument('--source-receipt', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        raise FileExistsError('Retain the existing admission readback')
    result = enroll(args.root, args.plan, args.source_receipt)
    atomic_write_json(args.output, result, durable=True)
    print(json.dumps(result, ensure_ascii=False))


if __name__ == '__main__':
    main()

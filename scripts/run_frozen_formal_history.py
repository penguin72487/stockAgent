#!/usr/bin/env python3
"""Run the canonical inference entrypoint from a verified service release.

Only the code bytes are frozen.  The canonical repository path remains
``__file__`` so existing config, data, output and source-lease resolution is
unchanged.  This launcher is intentionally limited to single-process inference;
training or a self-relaunch would otherwise reopen the mutable root train.py.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import sys
from types import ModuleType

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from stockagent.live.service_code_release import (
    FORMAL_HISTORY_ENTRYPOINT,
    _read_source_bytes,
    verify_frozen_source,
)


def _validate_inference_arguments(arguments: list[str]) -> None:
    for option, expected in (("--mode", "infer"), ("--multi-gpu-strategy", "none")):
        if (arguments.count(option) != 1 or any(arg.startswith(option + "=") for arg in arguments)
                or arguments.index(option) + 1 >= len(arguments)
                or arguments[arguments.index(option) + 1] != expected):
            raise ValueError(f"frozen formal history requires exactly {option} {expected}")
    if os.environ.get("WORLD_SIZE", "1") != "1":
        raise ValueError("frozen formal history requires a single-process owner")


def _verified_entrypoint(receipt: Path, root: Path) -> tuple[bytes, str]:
    proof = verify_frozen_source(receipt, root)
    actual_receipt = Path(proof["receipt"])
    manifest = json.loads(actual_receipt.read_bytes())
    expected = manifest["code"]["files"].get(FORMAL_HISTORY_ENTRYPOINT)
    if not isinstance(expected, str):
        raise ValueError("service release lacks a frozen train.py; build and verify a compatible release")
    source = Path(proof["source_root"]) / FORMAL_HISTORY_ENTRYPOINT
    body, _ = _read_source_bytes(source)
    if hashlib.sha256(body).hexdigest() != expected:
        raise ValueError("frozen formal-history entrypoint differs from its verified release")
    return body, expected


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--release", required=True, type=Path)
    parser.add_argument("train_arguments", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    arguments = args.train_arguments
    if arguments[:1] == ["--"]:
        arguments = arguments[1:]
    _validate_inference_arguments(arguments)
    body, digest = _verified_entrypoint(args.release, ROOT)
    canonical = str(ROOT / FORMAL_HISTORY_ENTRYPOINT)
    print(f"[formal-history] verified frozen train.py sha256={digest}", flush=True)
    sys.argv = [canonical, *arguments]
    sys.path[0] = str(ROOT)
    # Libraries may resolve/pickle objects through sys.modules['__main__']; a
    # detached dict would give the right __name__ but the wrong actual module.
    previous_main = sys.modules.get("__main__")
    trainer_main = ModuleType("__main__")
    trainer_main.__file__ = canonical
    trainer_main.__package__ = None
    trainer_main.__cached__ = None
    sys.modules["__main__"] = trainer_main
    try:
        exec(compile(body, canonical, "exec"), trainer_main.__dict__)
    finally:
        if previous_main is None:
            del sys.modules["__main__"]
        else:
            sys.modules["__main__"] = previous_main


if __name__ == "__main__":
    main()

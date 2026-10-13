#!/usr/bin/env python3
"""Run serialized full-workflow tracing and canonical financial kernel checks."""
from __future__ import annotations
import argparse
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--trace-spec", type=Path, required=True)
    parser.add_argument("--baseline-config", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--test-source-root", type=Path, required=True)
    args = parser.parse_args()
    commands = [
        [sys.executable, str(ROOT / "scripts/run_frontier_fold_queue.py"),
            "--spec", str(args.trace_spec), "--baseline-config", str(args.baseline_config),
            "--output", str(args.output_root / "full-fold-timeline")],
        [sys.executable, str(ROOT / "scripts/run_frontier_financial_probes.py"),
            "--output", str(args.output_root / "financial-compiler-probes"),
            "--test-source-root", str(args.test_source_root)],
    ]
    returncodes = [subprocess.run(command, cwd=ROOT).returncode for command in commands]
    return next((rc for rc in returncodes if rc), 0)


if __name__ == "__main__":
    raise SystemExit(main())

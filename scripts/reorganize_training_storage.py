#!/usr/bin/env python3
"""Apply the enrolled fixed local layout plan; busy paths remain independently queued."""
from __future__ import annotations
import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from stockagent.data_sync.training_layout import apply_plan


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--plan', type=Path, required=True)
    p.add_argument('--state', type=Path, required=True)
    p.add_argument('--maximum-moves', type=int, default=32)
    a = p.parse_args()
    if not 1 <= a.maximum_moves <= 128:
        p.error('maximum moves must be between 1 and 128')
    for path in (a.plan, *a.plan.parents):
        if path.is_symlink():
            raise ValueError('layout plan is redirected')
    value = json.loads(a.plan.read_text())
    print(json.dumps(apply_plan(value, a.state, maximum_moves=a.maximum_moves)))


if __name__ == '__main__':
    main()

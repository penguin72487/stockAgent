#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from stockagent.models.temporal_basis_fit import (
    temporal_basis_effective_rank_profile,
)
from stockagent.models.transformer_base_portfolio import (
    FIXED_GRID_TEMPORAL_BASIS_FAMILIES,
    ONLINE_SAFE_TEMPORAL_BASIS_FAMILIES,
    TRAINING_ONLY_TEMPORAL_BASIS_FAMILIES,
)


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Profile unlimited non-DC temporal-basis candidate pools and "
            "novelty-threshold effective ranks."
        )
    )
    parser.add_argument("--lookback", type=int, default=32)
    parser.add_argument("--novelty-threshold", type=float, default=1e-4)
    args = parser.parse_args()
    families = (
        *ONLINE_SAFE_TEMPORAL_BASIS_FAMILIES,
        *FIXED_GRID_TEMPORAL_BASIS_FAMILIES,
        *TRAINING_ONLY_TEMPORAL_BASIS_FAMILIES,
    )
    profile = temporal_basis_effective_rank_profile(
        families,
        lookback=args.lookback,
        novelty_threshold=args.novelty_threshold,
    )
    profile["components_by_family"] = {
        family: values["effective_rank"]
        for family, values in profile["families"].items()
    }
    print(json.dumps(profile, indent=2, sort_keys=False))


if __name__ == "__main__":
    main()

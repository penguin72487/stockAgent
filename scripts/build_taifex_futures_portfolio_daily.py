#!/usr/bin/env python3
"""Build the all-listed TAIFEX equity/ETF/index futures daily dataset."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from stockagent.data.tw_futures_portfolio_daily import (  # noqa: E402
    DEFAULT_OUTPUT_ROOT,
    DEFAULT_OFFICIAL_PRODUCT_CODE_PATH,
    DEFAULT_PRODUCT_MASTER_PATH,
    DEFAULT_PUBLIC_FEATURE_PATH,
    DEFAULT_SOURCE_PATH,
    DEFAULT_STOCK_MASTER_PATH,
    build_dataset,
)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", default=DEFAULT_SOURCE_PATH)
    parser.add_argument("--product-master", default=DEFAULT_PRODUCT_MASTER_PATH)
    parser.add_argument(
        "--official-product-codes", default=DEFAULT_OFFICIAL_PRODUCT_CODE_PATH
    )
    parser.add_argument("--stock-master", default=DEFAULT_STOCK_MASTER_PATH)
    parser.add_argument("--public-features", default=DEFAULT_PUBLIC_FEATURE_PATH)
    parser.add_argument("--output-root", default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args()
    manifest = build_dataset(
        source_path=args.source,
        product_master_path=args.product_master,
        official_product_code_path=args.official_product_codes,
        stock_master_path=args.stock_master,
        public_feature_path=args.public_features,
        output_root=args.output_root,
    )
    print(json.dumps(manifest, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

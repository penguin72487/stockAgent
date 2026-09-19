#!/usr/bin/env python3
"""Prepare the configured overnight-decision panel and persist its acceptance gate."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path
import sys
import time

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from downloader.artifact_io import atomic_write_json  # noqa: E402
from stockagent.config import load_config  # noqa: E402
from stockagent.data.panel import build_panel  # noqa: E402
from stockagent.training.checkpoint_contract import build_checkpoint_manifest  # noqa: E402
from stockagent.training.trainer import _training_dataset_identity  # noqa: E402
from train import _build_panel_kwargs  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', default='configs/markets/tw_overnight_1325_multi_basis_22_capital10m.yaml')
    parser.add_argument('--output', required=True, type=Path)
    args = parser.parse_args()
    config = load_config(args.config)
    if not config.trading.tw_overnight_fixed_close_to_open:
        parser.error('this audit requires tw_overnight_fixed_close_to_open')
    started = time.monotonic()
    receipt = {
        'checked_at': datetime.now(timezone.utc).isoformat(),
        'config': args.config, 'requested_panel_start': config.data.panel_start_date,
        'status': 'blocked', 'research_panel_ready': False,
        'auction_fill_verified': False, 'production_order_possible': False,
    }
    try:
        panel = build_panel(config.data.parquet_root, **_build_panel_kwargs(config))
        receipt.update(status='research_panel_ready', research_panel_ready=True,
                       data_summary=_training_dataset_identity(panel),
                       checkpoint_manifest=build_checkpoint_manifest(panel, config))
    except Exception as exc:
        receipt.update(error_type=type(exc).__name__, error=str(exc))
    receipt['elapsed_seconds'] = time.monotonic() - started
    atomic_write_json(args.output, receipt)
    print(f"{receipt['status']}: {args.output}", flush=True)
    if 'error' in receipt:
        print(receipt['error'], file=sys.stderr, flush=True)
    return 0 if receipt['research_panel_ready'] else 1


if __name__ == '__main__':
    raise SystemExit(main())

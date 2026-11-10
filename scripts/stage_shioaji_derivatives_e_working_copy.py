#!/usr/bin/env python3
"""Resume the indexed Shioaji derivatives source with a measured E budget."""
from pathlib import Path

from scripts import stage_futures_e_working_copy as stage


if __name__ == '__main__':
    stage.main(
        source=stage.ROOT / 'data_tw_shioaji_history',
        name='derivatives-public-data_tw_shioaji_history-e-20261103-v1',
        marker=Path('summary.json'),
        report=(stage.ROOT / 'artifacts/markets/tw_futures_full_history_v22_official10_20261030_v1'
                / 'derivatives-public-data_tw_shioaji_history-e-20261103-v1.json'),
        budget=32 * 1024**3,
        seed_hardlinks=True,
    )

#!/usr/bin/env python3
"""Stage the existing Shioaji continuous futures history on E as a snapshot."""
from pathlib import Path

from scripts import stage_futures_e_working_copy as stage


if __name__ == '__main__':
    stage.main(
        source=stage.ROOT / 'data_tw_futures/shioaji_history',
        name='futures-shioaji-continuous-e-20261103-v1',
        marker=Path('BRFR1/manifest.json'),
        report=(stage.ROOT / 'artifacts/markets/tw_futures_full_history_v22_official10_20261030_v1'
                / 'futures-shioaji-continuous-e-20261103-v1.json'),
        budget=80 * 1024**3,
        seed_hardlinks=True,
    )

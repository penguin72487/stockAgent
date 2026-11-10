#!/usr/bin/env python3
"""Stage the retained 765-code futures repair evidence through the shared E owner."""
from pathlib import Path

from scripts import stage_futures_e_working_copy as stage


if __name__ == '__main__':
    name = 'futures-universe765-e-temporary-20261103-v1'
    stage.main(
        source=stage.ROOT/'data_tw_index_futures/preparation_sources/universe765_repair_20261027_v1',
        name=name,
        marker=Path('association_holiday_bulletins_feb_20261102_v1/monthly_201102_100.pdf.acquisition.json'),
        report=stage.ROOT/'artifacts/markets/tw_futures_full_history_v22_official10_20261030_v1'/f'{name}.json',
        budget=64*1024**3,
        seed_hardlinks=True,
    )

#!/usr/bin/env python3
"""Stage retained 725-code evidence referenced by the 765-code source bundle."""
from pathlib import Path

from scripts import stage_futures_e_working_copy as stage


if __name__ == '__main__':
    name = 'futures-universe725-e-temporary-20261103-v1'
    stage.main(
        source=stage.ROOT/'data_tw_index_futures/preparation_sources/universe725_repair_20261025_v3',
        name=name,
        marker=Path('native_quote_quality_v482/source_price_contradictions.parquet'),
        report=stage.ROOT/'artifacts/markets/tw_futures_full_history_v22_official10_20261030_v1'/f'{name}.json',
        budget=96*1024**3,
        seed_hardlinks=True,
    )

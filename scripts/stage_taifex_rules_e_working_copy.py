#!/usr/bin/env python3
"""Stage retained exchange-rule originals through the shared E copy owner."""
from pathlib import Path

from scripts import stage_futures_e_working_copy as stage


if __name__ == '__main__':
    name = 'taifex-rules-e-temporary-20261103-v1'
    stage.main(
        source=stage.ROOT/'data_taifex_public_history/rules',
        name=name,
        marker=Path('raw/6d/6d22d15520a7456e14845f7e493b2c19026c8828bcc58ce0897657894bd1c2fa.body.gz'),
        report=stage.ROOT/'artifacts/markets/tw_futures_full_history_v22_official10_20261030_v1'/f'{name}.json',
        budget=20*1024**3,
        seed_hardlinks=True,
    )

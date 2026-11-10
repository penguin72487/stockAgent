import json
from datetime import date

import polars as pl
import pytest

from scripts.plan_shioaji_positive_empty_finlab import build


def test_positive_empty_maps_only_exact_monthly_rank_and_preserves_unmatched(tmp_path):
    root = tmp_path / 'history'
    root.mkdir()
    (root / 'batch.lock').touch()
    for alias in ('AAR1', 'AAR2', 'ZZR1'):
        folder = root / alias
        folder.mkdir()
        (folder / 'manifest.json').write_text(json.dumps({'contract': alias,
            'coverage_state': 'source_gaps', 'positive_activity_empty_dates': ['2025-01-02']}))
    daily = tmp_path / 'official.parquet'
    pl.DataFrame({'date': [date(2025, 1, 2)] * 3, 'product': ['AAA'] * 3,
                  'contract': ['202501', '202501W1', '202502'],
                  'shioaji_roots': ['AA'] * 3, 'volume': [0, 50, 10]}).write_parquet(daily)
    plan = build(root, daily)
    assert plan['source_gap_alias_dates'] == 3
    assert plan['candidates'] == [{'alias': 'AAR2', 'dataset': 'tw_futures_minute:AAA_202502',
                                   'partition': '2025-01-02'}]
    assert {x['alias'] for x in plan['unresolved']} == {'AAR1', 'ZZR1'}
    assert len(plan['manifest_pins']) == 3
    assert plan['provider_calls'] == 0 and not plan['data_or_training_admission']


def test_positive_empty_rejects_mismatched_manifest_alias(tmp_path):
    root = tmp_path / 'history'
    folder = root / 'AAR1'
    folder.mkdir(parents=True)
    (folder / 'manifest.json').write_text(json.dumps({'contract': 'OTHER',
        'positive_activity_empty_dates': ['2025-01-02']}))
    with pytest.raises(ValueError, match='manifest and continuous alias differ'):
        build(root, tmp_path / 'unused.parquet')

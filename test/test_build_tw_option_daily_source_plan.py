import json
from pathlib import Path

import pytest

from downloader.artifact_io import sha256_file
from scripts.build_tw_option_daily_source_plan import build


def test_plan_binds_annual_and_monthly_sources_and_rejects_changed_evidence(tmp_path):
    manifest = tmp_path / 'manifest.json'
    manifest.write_text('{}')
    annual_sources = []
    for year in range(2011, 2026):
        archive = tmp_path / f'{year}_opt.zip'
        archive.write_bytes(str(year).encode())
        annual_sources.append({'path': str(archive), 'sha256': sha256_file(archive)})
    annual = tmp_path / 'annual.json'
    annual.write_text(json.dumps({'contract': 'taifex_option_annual_positive_days_finmind_receipts_v1',
                                  'source_manifest': str(manifest), 'source_manifest_sha256': sha256_file(manifest),
                                  'annual_sources': annual_sources}))
    source = tmp_path / 'january.csv'
    source.write_bytes(b'original option source')
    receipt = tmp_path / 'january.receipt.json'
    receipt_data = {'contract': 'taifex_option_all_products_daily_raw_v1', 'range_start': '2026-01-01',
                    'range_end': '2026-01-31', 'point_in_time_snapshot': False, 'path': str(source),
                    'sha256': sha256_file(source), 'bytes': source.stat().st_size}
    receipt.write_text(json.dumps(receipt_data))
    monthly = tmp_path / 'monthly.json'
    monthly.write_text(json.dumps({'contract': 'tw_option_all_product_monthly_range_readback_v1',
                                   'reader_sha256': sha256_file(Path('stockagent/data/tw_option_product_daily.py')),
                                   'start': '2026-01-01', 'end': '2026-01-31', 'months_verified': 1,
                                   'sources': [{'start': '2026-01-01', 'end': '2026-01-31',
                                                'path': str(source), 'receipt': str(receipt),
                                                'sha256': sha256_file(source), 'bytes': source.stat().st_size}]}))
    plan = build(annual, monthly)
    assert len(plan['sources']) == 16
    assert plan['sources'][0]['start'] == '2011-01-01'
    assert plan['sources'][-1]['end'] == '2026-01-31'
    receipt.write_text(json.dumps({**receipt_data, 'point_in_time_snapshot': True}))
    with pytest.raises(ValueError, match='monthly source evidence changed'):
        build(annual, monthly)
    receipt.write_text(json.dumps(receipt_data))
    source.write_bytes(b'changed')
    with pytest.raises(ValueError, match='monthly source evidence changed'):
        build(annual, monthly)

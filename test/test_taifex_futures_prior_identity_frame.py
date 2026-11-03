from datetime import date
import json

import polars as pl
import pytest

from downloader.artifact_io import sha256_file
from scripts.build_taifex_futures_prior_trade_window import earlier_frame_identities, pin_identity_frame


def test_blocked_history_never_extends_target_or_changes_generation():
    def row(day, product, physical):
        return {'date': date.fromisoformat(day), 'product': product, 'contract': '202611',
                'physical_contract': physical, '_start': 31500, '_end': 49500}
    released = pl.DataFrame([row('2026-09-17', 'AAA', 'old'),
                             row('2026-09-18', 'AAA', 'new'),
                             row('2026-09-18', 'BBB', 'same')])
    frame = pl.DataFrame([row('2026-09-17', 'AAA', 'new'),
                          row('2026-09-17', 'BBB', 'same'),
                          row('2026-09-18', 'CCC', 'not-released'),
                          row('2026-09-19', 'BBB', 'same')])
    combined, added = earlier_frame_identities(released, frame, date(2026, 9, 18))
    assert added == 1
    assert combined.filter((pl.col('date') == date(2026, 9, 17)) & (pl.col('product') == 'AAA'))['physical_contract'].to_list() == ['old']
    assert combined.filter((pl.col('date') == date(2026, 9, 17)) & (pl.col('product') == 'BBB')).height == 1
    assert combined.filter((pl.col('date') == date(2026, 9, 18)) & (pl.col('product') == 'CCC')).height == 0
    assert combined.filter(pl.col('date') == date(2026, 9, 19)).height == 0


def test_identity_frame_requires_same_pinned_financial_owner(tmp_path):
    owner = tmp_path / 'owner'
    daily = owner / 'release' / 'daily' / 'continuous_daily.parquet'
    daily.parent.mkdir(parents=True)
    daily.write_bytes(b'daily')
    frame = owner / 'frame.parquet'
    frame.write_bytes(b'frame')
    (owner / 'accounting_checkpoint.json').write_text(json.dumps({
        'inputs': {'source_manifest_sha256': 'same-source'},
        'outputs': {'frame.parquet': sha256_file(frame)}}))
    (daily.parent / 'manifest.json').write_text(json.dumps({
        'status': 'complete', 'source_evidence_sha256': 'same-source',
        'outputs': {'continuous_daily': {'sha256': sha256_file(daily)}}}))
    result = pin_identity_frame(frame, daily, sha256_file(daily))
    assert result['use'] == 'prior_identity_context_only_not_tradability'
    frame.write_bytes(b'changed')
    with pytest.raises(ValueError, match='pinned source'):
        pin_identity_frame(frame, daily, sha256_file(daily))

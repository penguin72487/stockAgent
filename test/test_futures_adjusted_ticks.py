"""Actual dated CL1 source acceptance, including independently checked TAIFEX facts."""
import hashlib
import json
from pathlib import Path
import shutil

import polars as pl
import pytest

from stockagent.data.tw_stock_futures_transition import load_transition_bundle


@pytest.fixture
def bundle(tmp_path):
    source = Path('artifacts/data_preparation/futures_corporate_transitions_v3_20260910')
    if not source.exists():
        pytest.skip('receipt-backed remote fixture unavailable')
    dest = tmp_path / 'bundle'
    shutil.copytree(source, dest)
    return dest


def test_exact_ticks_cover_all_nine_days_with_correct_final_value(bundle):
    result = load_transition_bundle(bundle / 'manifest.json')
    covered = result['coverage'].filter(pl.col('physical_contract') == 'CL1:202312')
    assert covered.height == 9
    assert (covered['tick_volume'] == covered['outright_volume']).all()
    assert covered['tick_rows'].sum() == 141
    assert result['finals'].filter(pl.col('physical_contract') == 'CL1:202312')['final_settlement_value'].item() == 77759.


@pytest.mark.parametrize('damage,match', [
    ('missing_hash', 'omits required source hashes'),
    ('date', 'source receipt invalid'),
    ('identity', 'code differs from physical delivery month'),
    ('volume', 'differ from official outright volume or OHLC'),
    ('price', 'differ from official outright volume or OHLC'),
    ('tape', 'differs from exact source KBars'),
])
def test_rehashed_bundle_still_rejects_source_or_official_contradictions(bundle, damage, match):
    path = bundle / 'manifest.json'
    manifest = json.loads(path.read_text())
    source = manifest['tick_sources'][0]
    if damage == 'missing_hash':
        manifest['files'].pop(source['receipt_file'])
    elif damage == 'date':
        source['date'] = '2023-12-11'
    elif damage == 'identity':
        source['code'] = 'CL1K3'
    elif damage in ('volume', 'price'):
        rel = 'official/official_evidence.parquet'
        frame = pl.read_parquet(bundle / rel)
        field = 'outright_volume' if damage == 'volume' else 'official_high'
        value = pl.col(field) + 1 if damage == 'volume' else pl.lit('999')
        frame.with_columns(pl.when(pl.col('physical_contract') == 'CL1:202312')
            .then(value).otherwise(pl.col(field)).alias(field)).write_parquet(bundle / rel)
        digest = hashlib.sha256((bundle / rel).read_bytes()).hexdigest()
        manifest['files'][rel]['sha256'] = digest
        proof_path = bundle / 'official/official_evidence_manifest.json'
        proof = json.loads(proof_path.read_text())
        proof['sha256'] = digest
        proof_path.write_text(json.dumps(proof))
        manifest['files']['official/official_evidence_manifest.json']['sha256'] = hashlib.sha256(proof_path.read_bytes()).hexdigest()
    else:
        rel = 'minutes.parquet'
        frame = pl.read_parquet(bundle / rel)
        frame.with_columns((pl.col('volume') + 1).alias('volume')).write_parquet(bundle / rel)
        manifest['files'][rel]['sha256'] = hashlib.sha256((bundle / rel).read_bytes()).hexdigest()
    path.write_text(json.dumps(manifest))
    with pytest.raises(ValueError, match=match):
        load_transition_bundle(path)

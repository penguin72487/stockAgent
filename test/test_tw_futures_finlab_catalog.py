import json
from datetime import date

import polars as pl
import pytest

from downloader.artifact_io import sha256_file
from scripts.audit_tw_futures_finlab_catalog import audit


def _source(tmp_path):
    root = tmp_path / 'finlab'
    (root / 'datasets').mkdir(parents=True)
    payload = root / 'datasets' / 'volume.parquet'
    pl.DataFrame({
        'source_index': ['2011-01-03 00:00:00', '2026-10-19 00:00:00'],
        'OLD一般': [1., None], 'NEW一般': [0., 3.],
        'NEW盤後': [None, 2.], 'FOREIGN一般': [1., None],
    }).write_parquet(payload)
    receipt = root / 'receipt.json'
    receipt.write_text(json.dumps(dict(dataset='futures_price:成交量',
        status='downloaded_unverified_for_pit', parquet_path='datasets/volume.parquet',
        parquet_size_bytes=payload.stat().st_size, sha256=sha256_file(payload),
        rows=2, field_columns=4)))
    catalog = tmp_path / 'catalog.csv'
    catalog.write_text('product\nOLD\nABSENT\n')
    return root, receipt, catalog, payload


def _audit(root, receipt, catalog):
    return audit(root, receipt, catalog, start=date(2011, 1, 3), cutoff=date(2026, 9, 24))


def test_source_axis_union_distinguishes_sessions_and_dates(tmp_path):
    root, receipt, catalog, _ = _source(tmp_path)
    result = _audit(root, receipt, catalog)
    assert (result['catalog_codes'], result['source_axis_products'], result['common_codes']) == (2, 3, 1)
    assert result['catalog_codes_absent_from_source_axis'] == ['ABSENT']
    added = {item['product']: item['sessions'] for item in result['source_codes_outside_catalog']}
    assert added['NEW']['一般']['positive_source_rows'] == 1
    assert added['NEW']['一般']['positive_rows_in_requested_interval'] == 0
    assert added['NEW']['盤後']['first_positive_source_date'] == '2026-10-19'
    assert added['FOREIGN']['一般']['positive_rows_in_requested_interval'] == 1
    assert not result['source_axis_is_complete_catalog'] and not result['training_admitted']
    assert result['provider_calls'] == 0


def test_refuses_payload_hash_and_catalog_duplicates(tmp_path):
    root, receipt, catalog, payload = _source(tmp_path)
    with payload.open('ab') as stream:
        stream.write(b'changed')
    with pytest.raises(ValueError, match='payload hash or size'):
        _audit(root, receipt, catalog)
    root, receipt, catalog, _ = _source(tmp_path / 'second')
    catalog.write_text('product\nOLD\nOLD\n')
    with pytest.raises(ValueError, match='unique explicit'):
        _audit(root, receipt, catalog)


def test_unknown_source_axis_cannot_be_silently_reclassified(tmp_path):
    root, receipt, catalog, payload = _source(tmp_path)
    frame = pl.read_parquet(payload).rename({'NEW盤後': 'NEW_unknown'})
    frame.write_parquet(payload)
    body = json.loads(receipt.read_text())
    body.update(sha256=sha256_file(payload), parquet_size_bytes=payload.stat().st_size)
    receipt.write_text(json.dumps(body))
    with pytest.raises(ValueError, match='unknown FinLab futures axis'):
        _audit(root, receipt, catalog)

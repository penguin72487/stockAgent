"""Inventory rows retain their source and version boundaries."""
import json

import pytest

from scripts import report_storage_migration_inventory as report


def _catalog(tmp_path):
    sha1, sha2 = '1'*64, '2'*64
    key1, key2 = 'objects/one@'+sha1, 'objects/two@'+sha2
    body = {'contract': 'retained_packed_backup_catalog_v1',
        'releases': [
            {'dataset':'tw-public', 'current_head':True, 'required_file_keys':[key1, key2]},
            {'dataset':'tw-public', 'current_head':False, 'required_file_keys':[key1]},
            {'dataset':'binance', 'current_head':True, 'required_file_keys':[key1]},
        ],
        'files': [
            {'relative':'objects/one', 'sha256':sha1, 'bytes':10},
            {'relative':'objects/two', 'sha256':sha2, 'bytes':20},
        ], 'identity_sha256':'a'*64}
    catalog, status = tmp_path/'catalog.json', tmp_path/'status.json'
    catalog.write_text(json.dumps(body))
    catalog.chmod(0o600)
    status.write_text(json.dumps({'contract':'stockagent.backup_catalog_observation.v1',
        'catalog_identity_sha256':'a'*64, 'catalog_generation':report._private_catalog_generation(catalog),
        'persisted_observed_at_utc':'2026-11-06T00:00:00+00:00',
        'available_file_count':2, 'available_bytes':30}))
    return catalog, status, body


def test_catalog_counts_shared_objects_once_per_dataset_and_retains_age(tmp_path):
    catalog, status, _ = _catalog(tmp_path)
    rows, info = report.catalog_inventory(catalog, status)
    assert rows['tw-public']['catalog_referenced_bytes'] == 30
    assert rows['tw-public']['catalog_current_bytes'] == 30
    assert rows['tw-public']['catalog_versions'] == 2
    assert rows['binance']['catalog_referenced_bytes'] == 10
    assert info['catalog_total_bytes'] == 30
    assert info['catalog_observed_at_utc'] == '2026-11-06T00:00:00+00:00'
    assert info['unresolved_references'] == 0


def test_catalog_rejects_identity_mismatch_and_duplicate_objects(tmp_path):
    catalog, status, body = _catalog(tmp_path)
    status.write_text(json.dumps({'contract':'stockagent.backup_catalog_observation.v1',
        'catalog_identity_sha256':'b'*64, 'catalog_generation':report._private_catalog_generation(catalog),
        'available_file_count':2, 'available_bytes':30}))
    with pytest.raises(ValueError, match='identity'):
        report.catalog_inventory(catalog, status)
    catalog, status, body = _catalog(tmp_path)
    body['files'].append(body['files'][0])
    catalog.write_text(json.dumps(body))
    marker = json.loads(status.read_text())
    marker['catalog_generation'] = report._private_catalog_generation(catalog)
    status.write_text(json.dumps(marker))
    with pytest.raises(ValueError, match='duplicate'):
        report.catalog_inventory(catalog, status)


def test_fixed_receipts_require_dataset_and_manifest_identity(tmp_path):
    path = tmp_path/'binance'
    path.mkdir()
    receipt = {'contract':'stockagent.source_local_exact_repack.v1',
        'state':'new_D_head_committed_from_exact_E_source',
        'dataset':'binance', 'new_manifest_sha256':'c'*64}
    (path/('c'*64+'.json')).write_text(json.dumps(receipt))
    (path/('d'*64+'.json')).write_text(json.dumps(receipt))
    rows, errors = report.repack_receipts(tmp_path)
    assert rows == [receipt]
    assert errors == [{'dataset':'binance', 'error_type':'ValueError'}]


def test_local_dataset_category_matches_its_source_category():
    assert report.category('local-tw-index-futures') == 'derivatives'
    assert report.category('local-bybit') == 'crypto'


def test_localized_tables_do_not_treat_missing_receipt_as_failed_migration(tmp_path):
    fields = ['dataset', 'category', 'queue_state', 'fixed_repack_receipt',
        'repack_source_logical_bytes', 'repack_files', 'catalog_current_bytes',
        'catalog_referenced_bytes', 'catalog_observed_at_utc', 'queue_logical_bytes',
        'queue_reason', 'direct_retirement_state', 'lifecycle_state', 'repack_new_manifest_sha256']
    values = {k:'' for k in fields}
    rows = [{**values, 'dataset':'one', 'category':'taiwan', 'fixed_repack_receipt':'False'},
            {**values, 'dataset':'two', 'category':'crypto', 'fixed_repack_receipt':'True', 'queue_state':'waiting'}]
    report.write_csv(tmp_path/'datasets.csv', fields, rows)
    report.write_localized_tables(tmp_path)
    import csv
    with (tmp_path/'尚未確認新封裝.csv').open(encoding='utf-8-sig', newline='') as stream:
        missing = list(csv.DictReader(stream))
    assert len(missing) == 1 and missing[0]['新封裝狀態'] == '尚未確認新封裝'
    with (tmp_path/'待處理佇列.csv').open(encoding='utf-8-sig', newline='') as stream:
        waiting = list(csv.DictReader(stream))
    assert len(waiting) == 1 and waiting[0]['有固定發布收據'] == '有'

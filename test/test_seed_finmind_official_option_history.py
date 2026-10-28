import hashlib
import json
import sqlite3

import pytest

from scripts.seed_finmind_official_option_history import seed, verified_official_candidates


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def evidence(tmp_path):
    source = tmp_path / '2011_opt.zip'
    source.write_bytes(b'official-retained-archive')
    inventory = tmp_path / 'inventory.csv'
    inventory.write_text('product,source_rows\nMSO,2\nTXO,10\n')
    manifest = tmp_path / 'manifest.json'
    manifest.write_text(json.dumps({'receipts': [{'path': str(source), 'sha256': sha(source)}]}))
    summary = tmp_path / 'summary.json'
    summary.write_text(json.dumps({'contract': 'taifex_option_archive_product_day_inventory_v1',
        'product_inventory': str(inventory), 'product_inventory_sha256': sha(inventory),
        'source_manifest': str(manifest), 'source_manifest_sha256': sha(manifest),
        'distinct_observed_products': 2,
        'annual_inputs': [{'path': str(source), 'sha256': sha(source)}]}))
    readback = tmp_path / 'readback.json'
    readback.write_text(json.dumps({'contract': 'tw_option_all_product_daily_full_inventory_readback_v1',
        'reference_summary_path': str(summary), 'reference_summary_sha256': sha(summary),
        'reference_inventory_sha256': sha(inventory), 'product_count': 2, 'per_product_counts_equal': True,
        'years': [{'year': 2011, 'source_path': str(source), 'source_sha256': sha(source)}]}))
    return source, inventory, manifest, summary, readback


def queue(path):
    with sqlite3.connect(path) as con:
        con.execute('CREATE TABLE tasks(dataset TEXT, data_id TEXT, partition TEXT, kind TEXT, '
                    'priority INTEGER, state TEXT, PRIMARY KEY(dataset,data_id,partition))')
        con.execute("INSERT INTO tasks VALUES ('TaiwanOptionDaily','TXO','history','id_history',3,'complete')")


def test_receipt_bound_candidates_seed_native_history_without_reopening_existing(tmp_path):
    _, _, _, summary, readback = evidence(tmp_path)
    products, proof = verified_official_candidates(summary, readback)
    assert products == ['MSO', 'TXO'] and proof['annual_years'] == [2011]
    db = tmp_path / 'queue.sqlite3'
    queue(db)
    result = seed(db, products)
    assert result['seeded_identifiers'] == ['MSO'] and result['provider_calls'] == 0
    assert seed(db, products)['seeded_identifiers'] == []
    with sqlite3.connect(db) as con:
        assert con.execute('SELECT data_id,state FROM tasks ORDER BY data_id').fetchall() == [
            ('MSO', 'pending'), ('TXO', 'complete')]


@pytest.mark.parametrize('altered', ['source', 'inventory', 'manifest', 'readback'])
def test_rejects_changed_official_source_or_inconsistent_independent_readback(tmp_path, altered):
    source, inventory, manifest, summary, readback = evidence(tmp_path)
    if altered == 'readback':
        value = json.loads(readback.read_text())
        value['per_product_counts_equal'] = False
        readback.write_text(json.dumps(value))
    else:
        {'source': source, 'inventory': inventory, 'manifest': manifest}[altered].write_text('changed')
    with pytest.raises(ValueError):
        verified_official_candidates(summary, readback)


def test_conflicting_existing_task_does_not_seed_partial_scope(tmp_path):
    db = tmp_path / 'queue.sqlite3'
    queue(db)
    with sqlite3.connect(db) as con:
        con.execute("INSERT INTO tasks VALUES ('TaiwanOptionDaily','MSO','latest','snapshot',3,'pending')")
    with pytest.raises(ValueError, match='conflicting_existing_native_tasks'):
        seed(db, ['MSO', 'TFO'])
    with sqlite3.connect(db) as con:
        assert con.execute("SELECT COUNT(*) FROM tasks WHERE data_id='TFO'").fetchone()[0] == 0

import hashlib
import json

import pytest

from scripts import integrate_frankfurter_fixed_generations as fx
from stockagent.data_sync.desync_snapshots import SnapshotError


def audit_values():
    plan = {'public_manifest_sha256': 'a'*64, 'private_manifest_sha256': 'b'*64,
            'worklist_sha256': 'c'*64,
            'counters': {'conflict_categories': {'parquet': 2, 'json': 3, 'other': 1},
                         'public_only': 0, 'private_only': 0, 'unexpected_public_excluded': 0}}
    identity = {key: plan[key] for key in ('public_manifest_sha256', 'private_manifest_sha256', 'worklist_sha256')}
    rows = {**identity, 'contract': 'stockagent.frankfurter_generation_row_audit.v1',
            'pairs': 2, 'pair_compared': 2, 'live_matches_private': 2, 'live_changed_from_private': 0,
            'public_only_rows': 0, 'conflicting_overlap_rows': 0, 'duplicate_dates': 0, 'null_dates': 0,
            'errors': [], 'source_mutated': False}
    meta = {**identity, 'contract': 'stockagent.frankfurter_generation_metadata_audit.v1',
            'files': 4, 'live_matches_private': 4, 'bad_sha_binding': 0,
            'equal_except_source_sha': 2, 'errors': [], 'source_mutated': False,
            'different_fields': {'non_json': 1, 'applied_end_date': 1, 'requested_end_date': 1,
                                 'row_count': 1, 'provider_end_date': 1}}
    return rows, meta, plan


def test_audited_superset_requires_exact_fixed_generations_and_complete_rows():
    rows, meta, plan = audit_values()
    fx.validate_audits(rows, meta, plan)
    for key, bad in [('public_only_rows', 1), ('conflicting_overlap_rows', 1),
                     ('pair_compared', 1), ('live_changed_from_private', 1)]:
        with pytest.raises(SnapshotError):
            fx.validate_audits({**rows, key: bad}, meta, plan)
    with pytest.raises(SnapshotError):
        fx.validate_audits(rows, {**meta, 'private_manifest_sha256': 'd'*64}, plan)
    with pytest.raises(SnapshotError):
        fx.validate_audits(rows, {**meta, 'different_fields': {'unknown': 1}}, plan)


def descriptor(name, value):
    return {'path': name, 'kind': 'file', 'mode': 0o644, 'size': len(value),
            'sha256': hashlib.sha256(value).hexdigest()}


def test_live_source_check_binds_head_and_never_follows_replacement(tmp_path):
    pair = 'AUDBRL_features.parquet'
    head = 'AUDBRL_features.head.json'
    value = b'fixed newer price data'
    proof = json.dumps({'source_sha256': hashlib.sha256(value).hexdigest()}).encode()
    (tmp_path/pair).write_bytes(value)
    (tmp_path/head).write_bytes(proof)
    rows = {pair: descriptor(pair, value), head: descriptor(head, proof)}
    assert fx.check_live_source(tmp_path, rows, rows) == 2
    (tmp_path/pair).write_bytes(b'changed')
    with pytest.raises(SnapshotError, match='live source'):
        fx.check_live_source(tmp_path, rows, rows)
    (tmp_path/pair).unlink()
    (tmp_path/pair).symlink_to(tmp_path/head)
    with pytest.raises(SnapshotError, match='redirected'):
        fx.check_live_source(tmp_path, rows, rows)


def test_head_metadata_must_bind_the_corresponding_new_price_file(tmp_path):
    pair, head = 'AUDBRL_features.parquet', 'AUDBRL_features.head.json'
    value, proof = b'data', b'{"source_sha256":"wrong"}'
    (tmp_path/pair).write_bytes(value)
    (tmp_path/head).write_bytes(proof)
    rows = {pair: descriptor(pair, value), head: descriptor(head, proof)}
    with pytest.raises(SnapshotError, match='does not bind'):
        fx.check_live_source(tmp_path, rows, rows)

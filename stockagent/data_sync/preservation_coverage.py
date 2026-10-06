"""Bind received legacy carriers to canonical recovery and exact NAS files."""
from __future__ import annotations

import json
import re

from stockagent.data_sync.bulk_archive import (
    INCOMING, verify_retained_preservation,
)
from stockagent.data_sync.desync_snapshots import SnapshotError
from stockagent.data_sync.windows_cold_io import metadata_path
from stockagent.runtime_identity import identity_sha256

CONTRACT = 'received_bulk_preservation_cohort_v1'


def received_publications(cohort):
    if (cohort.get('contract') != CONTRACT or not isinstance(cohort.get('carriers'), list)
            or not cohort['carriers']):
        raise ValueError('invalid received carrier cohort')
    pending, publications = [], []
    known = set()
    for row in cohort['carriers']:
        batch, scope = row['batch'], row['scope']
        if (not re.fullmatch(r'20\d{6}T\d{6}-[0-9a-f]{12}', batch)
                or scope not in {'cache', 'markets', 'ablations'} or (batch, scope) in known
                or not re.fullmatch('[0-9a-f]{64}', row['compressed_sha256'])
                or type(row['compressed_bytes']) is not int or row['compressed_bytes'] <= 0):
            raise ValueError('received carrier identity/scope differs')
        known.add((batch, scope))
        directory = metadata_path(INCOMING) / batch
        proof_path = directory / (scope + '.cold-proof.json')
        if not proof_path.exists():
            pending.append({'batch': batch, 'scope': scope, 'state': 'waiting_canonical_original_recovery'})
            continue
        if proof_path.is_symlink() or directory.resolve() != directory:
            raise ValueError('received preservation proof is redirected')
        proof = json.loads(proof_path.read_bytes())
        if proof.get('compressed_sha256') != row['compressed_sha256'] or proof.get('source_scope') != scope:
            raise ValueError('canonical preservation does not match the frozen received carrier')
        try:
            files = verify_retained_preservation(proof)
        except SnapshotError as error:
            pending.append({'batch': batch, 'scope': scope, 'state': 'canonical_recovery_needs_audit',
                            'error': str(error)})
            continue
        original = metadata_path(files['member_inventory.json'])
        index = json.loads(original.read_bytes())
        if (index.get('compressed_bytes') != row['compressed_bytes']
                or index.get('compressed_sha256') != row['compressed_sha256']
                or index.get('member_fingerprint_sha256') != row['member_fingerprint_sha256']):
            raise ValueError('canonical original member set differs from received cohort')
        publications.append({k: proof[k] for k in ('dataset', 'snapshot_id', 'manifest_sha256')})
    return {'pending': pending, 'publications': publications,
            'received_carriers': len(cohort['carriers']),
            'received_compressed_bytes': sum(r['compressed_bytes'] for r in cohort['carriers']),
            'all_received_originals_verified': not pending and len(publications) == len(cohort['carriers'])}


def publication_cohort(catalog, publications, *, upstream_sha256):
    """Freeze the five received releases, leaving future collectors incrementing."""
    wanted = {(p['dataset'], p['snapshot_id'], p['manifest_sha256']) for p in publications}
    releases = [r for r in catalog['releases'] if (r['dataset'], r['snapshot_id'], r['manifest_sha256']) in wanted]
    if not wanted or len(releases) != len(wanted) or any(r['missing_objects'] for r in releases):
        return None
    keys = {key for release in releases for key in release['required_file_keys']}
    files = []
    for row in catalog['files']:
        key = row['relative'] + '@' + row['sha256']
        if row['role'] == 'cold_metadata' and row.get('captured_bytes_utf8'):
            head = json.loads(row['captured_bytes_utf8'])
            if (head.get('dataset'), head.get('snapshot_id'), head.get('manifest_sha256')) in wanted:
                keys.add(key)
        if key in keys:
            files.append(row)
    if {r['relative'] + '@' + r['sha256'] for r in files} != keys:
        return None
    result = {'contract': 'retained_packed_backup_catalog_v1',
              'scope': 'exact canonical releases of the frozen received carrier cohort',
              'observed_at_utc': catalog['observed_at_utc'], 'files': files, 'releases': releases,
              'missing_objects': [], 'metadata_errors': [], 'upstream_cohort_sha256': upstream_sha256}
    result['identity_sha256'] = identity_sha256(result)
    return result


def merge_cohorts(first, second):
    files = {r['relative'] + '@' + r['sha256']: r for c in (first, second) for r in c['files']}
    result = {'contract': 'retained_packed_backup_catalog_v1', 'files': list(files.values()),
              'missing_objects': first.get('missing_objects', []) + second.get('missing_objects', []),
              'metadata_errors': first.get('metadata_errors', []) + second.get('metadata_errors', []),
              'observed_at_utc': second['observed_at_utc'],
              'component_catalogs': [first['identity_sha256'], second['identity_sha256']]}
    result['identity_sha256'] = identity_sha256(result)
    return result

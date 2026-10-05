#!/usr/bin/env python3
"""Compose exact NAS file-restore receipts with independent packed rebuilding."""
import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from downloader.artifact_io import atomic_write_json
from stockagent.control.lakehouse import acceptance, configuration, restore_lake_delivery
from stockagent.control.lakehouse_recovery import reconstruct_registered_release
from stockagent.data_sync.immutable_replication import replicate, safe, verify


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--delivery-id', required=True)
    parser.add_argument('--dataset', required=True)
    parser.add_argument('--snapshot-id', required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        raise ValueError('preserve prior recovery evidence')
    c = configuration()
    catalog_proof = acceptance(c, args.delivery_id)
    if catalog_proof.get('catalog_semantic_restore_verified') is not True:
        raise ValueError('fixed catalog must have exact paired NAS semantic acceptance')
    root = Path(c['lake_root']) / 'releases' / ('lake-' + args.delivery_id)
    scratch = Path(tempfile.mkdtemp(prefix='sa-lake-packed-recovery-'))
    copied = scratch / 'independent-catalog'
    replication = replicate(root, copied, Path(c['binaries']) / 'bin/rclone')
    ledger = json.loads((Path(c['state_root']) / 'source-replication-ledger.json').read_bytes())
    candidates = {key: identity for identity, row in ledger['deliveries'].items()
                  if row.get('nas_acceptance', {}).get('state') == 'nas_archive_file_recovery_verified'
                  for key in row['file_keys']}
    archive_proofs, archived_manifests = {}, {}

    def object_source(item):
        identity = candidates.get(item['relpath'] + '@' + item['sha256'])
        if not identity:
            raise ValueError('selected source object has no exact NAS file-restore proof')
        if identity not in archive_proofs:
            archive_proofs[identity] = acceptance(c, identity)
            batch = Path(c['transport_root']) / ('lake-' + identity)
            if batch.exists():
                manifest = verify(batch)
            else:
                journal = json.loads((Path(c['state_root']) / ('cache-retirement-' + identity + '.json')).read_bytes())
                manifest = json.loads(journal['manifest_bytes_utf8'])
            archived_manifests[identity] = manifest
        manifest = archived_manifests[identity]
        mapping = manifest['context'].get('source_object_paths', {})
        expected = [row for relative, row in manifest['files'].items()
                    if mapping.get(relative, relative) == item['relpath']]
        if expected != [{'sha256': item['sha256'], 'bytes': item['bytes']}]:
            raise ValueError('NAS accepted manifest does not bind this exact original object')
        return safe(Path(c['cold_root']), item['relpath'])

    def validate(con):
        return {'packed_recovery': reconstruct_registered_release(con, scratch / 'independent-packed',
                dataset=args.dataset, snapshot_id=args.snapshot_id, object_source=object_source)}

    pid = subprocess.check_output(['systemctl', 'show', 'postgresql@18-main', '-p', 'MainPID', '--value'], text=True).strip()
    cluster = Path(tempfile.mkdtemp(prefix='sa-lake-packed-private-pg-'))
    cluster.rmdir()
    restored = restore_lake_delivery(copied, cluster, extensions=Path(c['extensions']),
                    pg_bin=Path('/proc/' + pid + '/exe').resolve().parent, validate_restored=validate)
    unchanged = subprocess.check_output(['systemctl', 'show', 'postgresql@18-main', '-p', 'MainPID', '--value'], text=True).strip() == pid
    if not unchanged:
        raise ValueError('authority PostgreSQL unexpectedly restarted')
    receipt = {'state': 'accepted', 'observed_at_utc': datetime.now(timezone.utc).isoformat(),
               'catalog_delivery_identity_sha256': args.delivery_id, 'catalog_nas_acceptance': catalog_proof,
               'archive_nas_acceptances': archive_proofs, 'independent_catalog_copy': replication,
               'independent_logical_and_packed_restore': restored, 'production_postgres_main_pid_unchanged': True,
               'proof_scope': 'paired NAS independent file restores plus canonical rebuilding of hash-identical independent copies',
               'direct_nas_read_by_this_tool': False, 'scratch': str(scratch), 'private_pg_scratch': str(cluster),
               'full_history_reconstruction_verified': False}
    atomic_write_json(args.output, receipt)
    print(json.dumps({'state': receipt['state'], 'packed_recovery': restored['packed_recovery'],
                      'archive_delivery_count': len(archive_proofs), 'direct_nas_read_by_this_tool': False}))


if __name__ == '__main__':
    main()

#!/usr/bin/env python3
"""Measure complete closed delivery creation and verification on D aliases."""
from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import resource
import subprocess
import sys
import time
import uuid

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scripts.verify_backup_delivery import verify  # noqa: E402
from stockagent.data_sync.offhost_backup import export_incremental_delivery, private_json  # noqa: E402
from stockagent.data_sync.packed_backup import signature  # noqa: E402
from stockagent.data_sync.desync_snapshots import SnapshotError, atomic_write_json, sha256_file  # noqa: E402


def retire_probe_copies(queue, receipts, output: Path, *, apply=False, wait_for_owner=False):
    """Retire only fixed benchmark duplicates after fresh exact D readback.

    This never enrolls failed exports, deliveries or arbitrary staging names.
    The original stream owner fences the ignored staging namespace. An original
    receipt, complete envelope, unchanged full-SHA cold originals and negative
    process references are required before quarantining any exact probe.
    """
    from scripts.verify_backup_delivery import regular
    from stockagent.data_sync.backup_transport_cache import process_roots
    from stockagent.data_sync.materialized_cache import process_references_many
    from stockagent.data_sync.remote_legacy_return import metadata_tree
    allowed = {Path('/srv/stockagent-d-backup-volume/stockagent-backup-ingress-lab203/.staging'),
               Path('/srv/stockagent-backup-ingress-lab203/.staging'),
               Path('/srv/stockagent-d-volume/stockagent-backup-ingress-lab203/.staging')}
    queue.storage_guard()
    trials = {}
    for receipt in receipts:
        raw = receipt.read_bytes()
        if len(raw) > 1024**2 or receipt.is_symlink():
            raise SnapshotError('probe receipt is redirected or oversized')
        document = json.loads(raw)
        if (document.get('contract') != 'source_complete_closed_delivery_drvfs_comparison_v1'
                or document.get('probe_files_retained_in_ignored_staging') is not True
                or not isinstance(document.get('trials'), list) or not 1 <= len(document['trials']) <= 16):
            raise SnapshotError('only the original bounded D comparison receipts enroll probes')
        for trial in document['trials']:
            recorded = Path(trial['staging_path'])
            if (recorded.parent not in allowed or not re.fullmatch(r'drvfs-benchmark-[a-f0-9]{32}', recorded.name)
                    or trial.get('closed_envelope_and_READY_verified') is not True
                    or trial.get('every_file_sha256_verified') is not True
                    or trial.get('complete_file_set_verified') is not True):
                raise SnapshotError('probe is outside its frozen benchmark scope')
            key = recorded.name
            value = {'trial': trial, 'receipt_sha256': hashlib.sha256(raw).hexdigest()}
            if key in trials and trials[key] != value:
                raise SnapshotError('conflicting benchmark probe enrollment')
            trials[key] = value
    if not 1 <= len(trials) <= 32:
        raise SnapshotError('probe retirement must be bounded')
    report = {'contract': 'closed_drvfs_benchmark_probe_retirement_v1', 'apply': apply,
              'source_files_deleted': 0, 'NAS_snapshots_deleted': 0, 'items': []}
    with (queue.state / 'owner.lock').open('a') as owner:
        fcntl.flock(owner, fcntl.LOCK_EX | (0 if wait_for_owner else fcntl.LOCK_NB))
        queue.storage_guard()
        ledger = queue.load_ledger()
        enrolled = {row.get('staging_relative') for row in ledger['deliveries'].values()}
        pending = ledger.get('auxiliary_pending')
        if isinstance(pending, dict):
            enrolled.add(pending.get('staging_relative'))
        for name, binding in trials.items():
            trial = binding['trial']
            root = queue.transport / '.staging' / name
            if '.staging/' + name in enrolled or root.resolve() != root:
                raise SnapshotError('probe is registered to a real delivery or redirected')
            checked = verify(root, trial['envelope_identity_sha256'], include_file_signatures=True)
            if checked['files_verified'] != trial['files'] or checked['bytes_verified'] != trial['bytes']:
                raise SnapshotError('probe counters differ from the frozen trial')
            envelope = json.loads(regular(root, 'backup-envelope.json').read_bytes())
            originals = {}
            for row in envelope['files']:
                if not row['relative'].startswith('cold/'):
                    raise SnapshotError('benchmark member is not a cold duplicate')
                source = regular(queue.cold, row['relative'][len('cold/'):])
                before = signature(source)
                if (before[2] != row['bytes'] or sha256_file(source) != row['sha256']
                        or signature(source) != before):
                    raise SnapshotError('original cold bytes changed; preserve benchmark copy')
                originals[source] = before
            tree = metadata_tree(root)
            if any(row['kind'] == 'unsupported' or row['cross_filesystem']
                   or row['kind'] == 'file' and row['signature'][6] != 1 for row in tree['rows']):
                raise SnapshotError('benchmark tree contains unknown/shared members')
            roots = process_roots(queue.transport, root)
            keys = {(p.stat().st_dev, p.stat().st_ino) for p in root.rglob('*') if p.is_file()}
            if process_references_many(roots, inode_keys=keys):
                raise SnapshotError('benchmark copy is referenced by an active process')
            item = {**binding, 'source': str(root), 'state': 'verified_dry_run',
                    'complete_cold_original_sha_verified': True,
                    'tree_identity_sha256': hashlib.sha256(json.dumps(tree, sort_keys=True).encode()).hexdigest(),
                    'allocated_bytes_before': sum(p.stat().st_blocks * 512 for p in root.rglob('*') if p.is_file())}
            report['items'].append(item)
            atomic_write_json(output, report)
            if not apply:
                continue
            queue.storage_guard()
            if metadata_tree(root) != tree or any(signature(p) != sig for p, sig in originals.items()):
                raise SnapshotError('benchmark/source generation changed before quarantine')
            quarantine = root.with_name('retired-probe-' + name)
            if quarantine.exists() or quarantine.is_symlink():
                raise SnapshotError('retain existing benchmark retirement quarantine')
            item.update(state='quarantine_prepared', quarantine=str(quarantine))
            atomic_write_json(output, report)
            os.rename(root, quarantine)
            item['state'] = 'quarantined'
            atomic_write_json(output, report)
            if (metadata_tree(quarantine)['rows'] != tree['rows']
                    or any(signature(p) != sig for p, sig in originals.items())
                    or process_references_many(process_roots(queue.transport, quarantine), inode_keys=keys)):
                raise SnapshotError('post-quarantine verification changed; preserve quarantine')
            for row in tree['rows']:
                if row['kind'] != 'file':
                    continue
                path = regular(quarantine, row['path'])
                if list(signature(path)) != row['signature'][:5]:
                    raise SnapshotError('benchmark member changed immediately before unlink')
                path.unlink()
            directories = [Path(parent) for parent, _, _ in os.walk(quarantine, followlinks=False)]
            for directory in sorted(directories, key=lambda p: len(p.parts), reverse=True):
                directory.rmdir()
            item.update(state='retired', allocated_file_bytes_unlinked=item['allocated_bytes_before'])
            atomic_write_json(output, report)
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", type=Path)
    parser.add_argument("--candidate-volume", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument('--retire-receipt', type=Path, action='append')
    parser.add_argument('--apply', action='store_true')
    parser.add_argument('--wait-for-owner', action='store_true')
    args = parser.parse_args()
    if args.output.exists():
        parser.error("retain a fresh benchmark receipt")
    if args.retire_receipt:
        if args.catalog or args.candidate_volume:
            parser.error('retirement uses only the original fixed trial receipts')
        from stockagent.data_sync.backup_stream import BackupStream
        configuration = json.loads((ROOT / 'configs/data_sync/backup_stream.json').read_bytes())
        result = retire_probe_copies(BackupStream(configuration), args.retire_receipt, args.output,
                                    apply=args.apply, wait_for_owner=args.wait_for_owner)
        print(json.dumps({'items': len(result['items']), 'apply': args.apply,
                          'retired': sum(row['state'] == 'retired' for row in result['items'])}), flush=True)
        return
    if not args.catalog or not args.candidate_volume or args.apply:
        parser.error('a new comparison requires --catalog and --candidate-volume')
    cold = Path("/srv/stockagent-packed")
    transport = Path("/srv/stockagent-d-volume/stockagent-backup-ingress-lab203")
    candidate_cold = args.candidate_volume / "stockagent-cold-primary/packed"
    candidate_transport = args.candidate_volume / "stockagent-backup-ingress-lab203"
    marker = "stockagent-cold-primary/volume.json"
    if ((args.candidate_volume / marker).read_bytes()
            != Path("/srv/stockagent-d-volume", marker).read_bytes()
            or (candidate_cold / ".stockagent-d-primary").read_bytes() != (cold / ".stockagent-d-primary").read_bytes()
            or (candidate_transport / ".transport-owner.json").read_bytes() != (transport / ".transport-owner.json").read_bytes()):
        parser.error("candidate is not the enrolled D namespace")
    catalog = json.loads(args.catalog.read_bytes())
    blob = next(r for r in catalog["files"] if r["relative"].startswith("objects/blobs/") and 60 * 1024**2 < r["bytes"] < 68 * 1024**2)
    rows = [*([r for r in catalog["files"] if r["role"] == "cold_metadata" and r["bytes"] < 16384][:12]), blob]
    for row in rows:
        row["signature"] = list(signature(cold / row["relative"]))
    results = []
    sequence = [("candidate", candidate_cold, candidate_transport), ("baseline", cold, transport)] * 2
    for label, reader, destination_root in sequence:
        stage = destination_root / ".staging" / ("drvfs-benchmark-" + uuid.uuid4().hex)
        started = time.perf_counter()
        result = export_incremental_delivery(cold, rows, stage,
                                            catalog_identity=catalog["identity_sha256"], read_root=reader)
        # The source queue checks a closed batch again before journaling and publishing.
        checked = verify(stage, result["envelope_identity_sha256"])
        elapsed = time.perf_counter() - started
        row = {"backend": label, "seconds": elapsed, "files": len(rows),
               "bytes": checked["bytes_verified"], "MiB_per_second": checked["bytes_verified"] / 1024**2 / elapsed,
               "closed_envelope_and_READY_verified": True, "every_file_sha256_verified": True,
               "complete_file_set_verified": True, "staging_path": str(stage),
               "envelope_identity_sha256": result["envelope_identity_sha256"]}
        results.append(row)
        print(json.dumps(row), flush=True)
    result = {"contract": "source_complete_closed_delivery_drvfs_comparison_v1", "trials": results,
              "peak_process_rss_kib": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
              "same_real_members_interleaved": True, "operating_system_cache_cleared": False,
              "primary_mount_or_data_changed": False, "transport_or_NAS_backup_claimed": False,
              "probe_files_retained_in_ignored_staging": True}
    for label, root in (("baseline", Path("/srv/stockagent-d-volume")), ("candidate", args.candidate_volume)):
        options = subprocess.check_output(["findmnt", "-n", "-o", "OPTIONS", "-T", str(root)], text=True)
        result[label + "_request_bytes"] = next(int(part.split("=", 1)[1]) for part in options.strip().split(",") if part.startswith("msize="))
    private_json(args.output, result)


if __name__ == "__main__":
    main()

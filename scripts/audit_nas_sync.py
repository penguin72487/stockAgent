#!/usr/bin/env python3
"""Observe exact NAS file coverage of a frozen, retained source inventory."""
import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from downloader.artifact_io import atomic_write_json
from stockagent.control.lakehouse import configuration, guard
from stockagent.data_sync.nas_coverage import combined_coverage, read_ledger, transport_converged


def observe_transport(receiver_device_id):
    """A transient API outage cannot end the catch-up observer or prove success."""
    from scripts.configure_artifact_ingress_syncthing import credentials, request
    folders = ('stockagent-backup-ingress-lab203', 'stockagent-backup-receipts-lab203')
    try:
        base, key = credentials()
        connection = request(base, key, '/rest/system/connections')['connections'].get(receiver_device_id, {})
        system_errors = request(base, key, '/rest/system/error').get('errors')
        transport = []
        for folder in folders:
            local = request(base, key, '/rest/db/status', {'folder': folder})
            peer = request(base, key, '/rest/db/completion', {'folder': folder, 'device': receiver_device_id})
            errors = request(base, key, '/rest/folder/errors', {'folder': folder}).get('errors')
            ready = transport_converged(local, peer, connected=connection.get('connected'),
                                        folder_errors=errors, system_errors=system_errors)
            transport.append({'folder': folder, 'converged': ready, 'local_state': local.get('state'),
                              'peer_connected': connection.get('connected'), 'system_errors_present': bool(system_errors),
                              'peer_need_bytes': peer.get('needBytes'), 'peer_need_items': peer.get('needItems')})
        return transport
    except (OSError, RuntimeError, ValueError, KeyError, TypeError) as error:
        return [{'folder': folder, 'converged': False, 'state': 'observation_unavailable',
                 'error_type': type(error).__name__} for folder in folders]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--capture-cohort', type=Path)
    parser.add_argument('--cohort', type=Path)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--watch-seconds', type=int, default=0)
    parser.add_argument('--interval-seconds', type=int, default=30)
    parser.add_argument('--upstream-cohort', type=Path)
    parser.add_argument('--upstream-sha256')
    parser.add_argument('--require-transport-convergence', action='store_true')
    args = parser.parse_args()
    if (bool(args.capture_cohort) == bool(args.cohort) or args.output.exists()
            or not 0 <= args.watch_seconds <= 7 * 24 * 3600 or not 5 <= args.interval_seconds <= 60
            or bool(args.upstream_cohort) != bool(args.upstream_sha256)):
        parser.error('use one fixed cohort, fresh receipt, and bounded observation interval')
    c = configuration()
    guard(c)
    source = Path('/var/lib/stockagent/backup-stream/catalog.json')
    cohort_path = args.capture_cohort or args.cohort
    if args.capture_cohort:
        if cohort_path.exists() or cohort_path.is_symlink():
            raise ValueError('preserve the existing fixed cohort')
        raw = source.read_bytes()
        catalog = json.loads(raw)
        if catalog.get('missing_objects') or catalog.get('metadata_errors'):
            raise ValueError('source errors must be repaired before freezing the available cohort')
        cohort_path.parent.mkdir(parents=True, exist_ok=True)
        with cohort_path.open('xb') as stream:
            cohort_path.chmod(0o600)
            stream.write(raw)
            stream.flush()
            __import__('os').fsync(stream.fileno())
    else:
        if cohort_path.is_symlink() or not cohort_path.is_file():
            raise ValueError('fixed source inventory is redirected or missing')
        raw = cohort_path.read_bytes()
        catalog = json.loads(raw)
    if catalog.get('contract') != 'retained_packed_backup_catalog_v1':
        raise ValueError('unexpected frozen source catalog')
    cohort_sha = hashlib.sha256(raw).hexdigest()
    upstream = None
    expanded = None
    expanded_path = None
    if args.upstream_cohort:
        if (not args.upstream_sha256 or args.upstream_cohort.is_symlink()
                or hashlib.sha256(args.upstream_cohort.read_bytes()).hexdigest() != args.upstream_sha256):
            raise ValueError('frozen upstream source cohort changed')
        upstream = json.loads(args.upstream_cohort.read_bytes())
        expanded_path = args.output.with_name(args.output.stem + '-upstream-canonical.json')
        if expanded_path.exists():
            raise ValueError('preserve the existing expanded source cohort')
    deadline = time.monotonic() + args.watch_seconds
    previous = None
    while True:
        restic = read_ledger(Path('/var/lib/stockagent/backup-stream/ledger.json'))
        archive = read_ledger(Path(c['state_root']) / 'source-replication-ledger.json')
        source_ready = True
        source_status = None
        if upstream:
            from stockagent.data_sync.preservation_coverage import received_publications, publication_cohort, merge_cohorts
            source_status = received_publications(upstream)
            source_ready = source_status['all_received_originals_verified']
            if source_ready and expanded is None:
                current_catalog = json.loads(source.read_bytes())
                expanded = publication_cohort(current_catalog, source_status['publications'], upstream_sha256=args.upstream_sha256)
                if expanded:
                    atomic_write_json(expanded_path, expanded, durable=True)
            source_ready = source_ready and expanded is not None
        effective = merge_cohorts(catalog, expanded) if expanded else catalog
        result = combined_coverage(effective, restic, archive)
        if upstream:
            result.update(upstream_preservation=source_status, upstream_canonical_cohort=str(expanded_path),
                          unpublished_sources_included=True,
                          all_received_originals_verified=source_ready)
        transport_ready = True
        if args.require_transport_convergence:
            transport = observe_transport(c['receiver_device_id'])
            transport_ready = all(row['converged'] for row in transport)
            result['transport'] = transport
        result['all_available_cold_files_verified'] &= source_ready and transport_ready
        result.update(cohort=str(cohort_path), cohort_file_sha256=cohort_sha,
                      observed_at_utc=datetime.now(timezone.utc).isoformat(),
                      state='accepted' if result['all_available_cold_files_verified'] else 'backfill_in_progress')
        atomic_write_json(args.output, result, durable=True)
        current = (result['machine_pending_file_count'], result['machine_pending_bytes'], source_ready, transport_ready)
        if current != previous:
            print(json.dumps(result, ensure_ascii=False), flush=True)
            previous = current
        if result['all_available_cold_files_verified'] or time.monotonic() >= deadline:
            break
        time.sleep(min(args.interval_seconds, max(0, deadline - time.monotonic())))
    if args.watch_seconds and not result['all_available_cold_files_verified']:
        raise SystemExit(75)


if __name__ == '__main__':
    main()

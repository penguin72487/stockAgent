#!/usr/bin/env python3
"""Bounded real hydration through the existing Vast edge and transport owners."""
import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import shlex
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from downloader.artifact_io import atomic_write_json
from stockagent.data_sync.rclone_edge_transport import configuration
from stockagent.remote_ssh import ssh_base, validate_ssh_target

INSPECT = '''import json,sys
from pathlib import Path
from stockagent.data_sync.packed_snapshots import resolve_packed_snapshot_id
from stockagent.data_sync.packed_edge_cache import release_payload_relpaths
p=json.load(sys.stdin);root=Path('/srv/stockagent-packed')
state=json.loads(Path('/var/lib/stockagent-packed-edge/state.json').read_bytes())
release=resolve_packed_snapshot_id(root,p['dataset'],p['snapshot_id'],require_objects=False)
paths=release_payload_relpaths(release)
print(json.dumps({'dataset':p['dataset'],'snapshot_id':p['snapshot_id'],
 'hydrating':state.get('hydrating',{}).get(p['dataset']),
 'retained':state.get('retained_payloads',{}).get(p['dataset']),
 'materialized_exists':(Path('/srv/stockagent-packed-materialized')/p['dataset']/p['snapshot_id']).exists(),
 'required_count':len(paths),'present_count':sum((root/q).is_file() for q in paths),
 'payload_paths':sorted(paths),'manifest_sha256':release.manifest_sha256,
 'source_fingerprint_sha256':release.manifest['source']['portable_fingerprint_sha256']}))'''


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--dataset', required=True)
    parser.add_argument('--snapshot-id', required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        raise ValueError('preserve a fresh acceptance receipt')
    c = configuration()
    if not c:
        raise ValueError('production immutable edge transport is not enrolled')
    values = {}
    for line in Path('/etc/stockagent/remote-cold-artifact-ingress.env').read_text().splitlines():
        if line and not line.startswith('#') and '=' in line:
            key, raw = line.split('=', 1)
            tokens = shlex.split(raw)
            values[key] = tokens[0] if tokens else ''
    ssh = [*ssh_base(Path(values['COLD_ARTIFACT_INGRESS_IDENTITY_FILE']),
                    int(values['COLD_ARTIFACT_INGRESS_SSH_PORT'])),
           '-o', 'StrictHostKeyChecking=yes', validate_ssh_target(values['COLD_ARTIFACT_INGRESS_SSH_TARGET'])]
    prefix = 'cd ' + shlex.quote(c['remote_repo_root']) + ' && '
    identity = {'dataset': args.dataset, 'snapshot_id': args.snapshot_id}
    before = subprocess.run([*ssh, prefix + 'source scripts/runtime_env.sh && run_fintech_python -c ' + shlex.quote(INSPECT)],
                            input=json.dumps(identity), text=True, capture_output=True, check=True, timeout=45)
    observed = json.loads(before.stdout)
    if (observed['hydrating'] or observed['retained'] or observed['materialized_exists']
            or observed['present_count'] == observed['required_count']):
        raise ValueError('probe must use a new, unused release with genuinely missing objects')
    source = Path('/srv/stockagent-packed')
    size = sum((source / p).stat().st_size for p in observed['payload_paths'])
    if size > 64 * 1024**2:
        raise ValueError('engineering hydration must remain bounded to 64 MiB')
    atomic_write_json(args.output.with_name(args.output.stem + '-before.json'), observed)
    started = time.perf_counter()
    # A short lease on this new release uses the normal canonical fetch/GC
    # contract. Existing training and retained releases are never edited here.
    command = ['bash', 'scripts/run_data_cache.sh', 'use', args.dataset, '--snapshot-id', args.snapshot_id,
               '--ttl-days', '0.001', '--timeout-seconds', '600']
    result = subprocess.run([*ssh, prefix + shlex.join(command)], capture_output=True, text=True, timeout=660)
    if result.returncode:
        atomic_write_json(args.output, {'state': 'failed', **identity, 'exit_code': result.returncode,
                                       'remote_diagnostics_not_public': True})
        raise RuntimeError('canonical edge hydration failed; retain its owner receipts')
    proof = json.loads(result.stdout)
    receipt = {'state': 'accepted', **identity, 'before': observed, 'payload_bytes': size,
               'canonical_remote_use_exit_code': result.returncode, 'canonical_remote_use': proof,
               'complete_workflow_seconds': time.perf_counter() - started,
               'observed_at_utc': datetime.now(timezone.utc).isoformat(),
               'source_owner': 'stockagent-packed-transport', 'remote_owner': 'canonical packed edge use',
               'broker_orders_or_gpu_training_started': False, 'preexisting_sources_deleted': False}
    atomic_write_json(args.output, receipt)
    print(json.dumps({k: v for k, v in receipt.items() if k not in ('before', 'canonical_remote_use')}))


if __name__ == '__main__':
    main()

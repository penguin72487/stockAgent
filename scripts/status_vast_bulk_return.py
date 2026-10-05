#!/usr/bin/env python3
"""Read-only compressed transport/cold proof/retirement progress, no activation."""
import json
from pathlib import Path
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from stockagent.data_sync.bulk_archive import INCOMING
from stockagent.data_sync.windows_cold_io import metadata_path
from scripts.organize_vast_bulk_archives import received_batches


def transport_phase(receipt, stored):
    state=receipt['state']
    prefix=receipt.get('resumed_prefix_bytes',0)
    if state!='receiving' or not prefix:
        return state
    return ('verified_tail_append' if stored is not None and stored>prefix
            else 'exact_prefix_verification')


def main():
    rows=[]
    for batch in received_batches():
        if batch.resolve()!=batch or batch.is_symlink():
            continue
        for path in sorted(batch.glob('*.receipt.json')):
            receipt=json.loads(path.read_text())
            scope=receipt['scope']
            payload=metadata_path(receipt['payload'])
            if payload.parent!=batch or payload.is_symlink():
                continue
            organization=batch/(scope+'.organization.json')
            state=json.loads(organization.read_text()) if organization.exists() else {}
            verification=batch/(scope+'.verification.json')
            decoded=json.loads(verification.read_text()) if verification.exists() else {}
            progress=decoded or (state if state.get('state')=='decoding_all_originals' else {})
            proof=batch/(scope+'.cold-proof.json')
            stored=payload.stat().st_size if payload.exists() else None
            prefix=receipt.get('resumed_prefix_bytes',0)
            average=receipt.get('average_bytes_per_second')
            if prefix and not receipt.get('append_started_at_epoch'):
                average=(max(0,receipt.get('received_bytes',0)-prefix)/receipt['elapsed_seconds']
                         if receipt.get('elapsed_seconds') else None)
            rows.append({'batch':batch.name,'scope':scope,'transport_state':receipt['state'],
                         'stored_compressed_bytes':stored,'resumed_prefix_bytes':prefix,
                         'transport_phase':transport_phase(receipt,stored),
                         'receipt_bytes':receipt.get('received_bytes',0),
                         'average_bytes_per_second':average,
                         'organization_state':state.get('state','not_started'),
                         'received_original_verification_state':progress.get('state','not_started'),
                         'decoded_compressed_bytes':progress.get('compressed_read_bytes'),
                         'decode_compressed_total_bytes':progress.get('compressed_total_bytes'),
                         'cold_proof_present':proof.is_file(),
                         'source_roots_retired':sum(r.get('deleted') is True for r in state.get('roots',{}).values()),
                         'reclaimed_allocated_bytes':state.get('reclaimed_allocated_bytes',0),
                         'all_sources_retired':state.get('all_sources_retired',False)})
    path=ROOT/'artifacts/operations/vast_bulk_return_20261004/cache-compaction-independent-verification.json'
    compaction=json.loads(path.read_text()) if path.exists() else None
    print(json.dumps({'observed_at_epoch':time.time(),'batches':rows,
                      'exact_cache_compaction':compaction,
                      'reclaimed_allocated_bytes':sum(r['reclaimed_allocated_bytes'] for r in rows),
                      'transport_is_not_cold_acceptance':True},ensure_ascii=False,indent=2))
    return 0


if __name__=='__main__':
    raise SystemExit(main())

#!/usr/bin/env python3
"""Traditional SQL backup through the existing paired encrypted Restic relay."""
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import uuid

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from stockagent.control.lakehouse import configuration, guard, sql_text
from stockagent.control.backup import logical_database_state
from stockagent.data_sync.backup_auxiliary import CONTROL
from stockagent.data_sync.immutable_replication import digest
from stockagent.runtime_identity import identity_sha256
from scripts.verify_backup_delivery import CONTRACT, verify
from scripts.backup_delivery_receipt import signed, atomic_public, validate_ack
from downloader.artifact_io import atomic_write_json, atomic_write_text

c = configuration()
guard(c)
state = Path(c['state_root'])
with (state / 'control-backup-owner.lock').open('a') as owner:
    fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
    previous_path = state / 'traditional-backup-status.json'
    previous = json.loads(previous_path.read_bytes()) if previous_path.exists() else None
    backup_config = json.loads(Path('configs/data_sync/backup_stream.json').read_bytes())
    receipt_root = Path(backup_config['receipt_root'])
    if previous:
        dispatch = previous['dispatch']
        ack_path = receipt_root / ('acceptance-' + dispatch['envelope_identity_sha256'] + '.json')
        if previous['state'] == 'publication_prepared':
            staging=Path(previous['staging_root'])
            target=physical=Path('/srv/stockagent-d-volume/stockagent-backup-ingress-lab203') / dispatch['batch_relative']
            if not target.exists():
                verify(staging,dispatch['envelope_identity_sha256'])
                staging.rename(target)
            verify(target,dispatch['envelope_identity_sha256'])
            atomic_public(target.parent/'tools/dispatch'/(dispatch['envelope_identity_sha256']+'.json'),dispatch)
            previous['state']='waiting_encrypted_nas_backup'
            atomic_write_json(previous_path,previous)
        if not ack_path.exists():
            previous.update(state='waiting_encrypted_nas_backup', observed_at_utc=datetime.now(timezone.utc).isoformat())
            atomic_write_json(previous_path, previous)
            print(json.dumps({'state':previous['state'],'delivery_identity_sha256':dispatch['envelope_identity_sha256']}))
            raise SystemExit(0)
        accepted = json.loads(ack_path.read_bytes())
        validate_ack(accepted, dispatch)
        previous['last_nas_acceptance'] = {k: accepted[k] for k in
            ('envelope_identity_sha256', 'snapshot_id', 'accepted_at_utc', 'complete_files', 'complete_bytes',
             'independent_restore_verified', 'all_files_sha256_verified')}
        atomic_write_json(previous_path, previous)
    physical = Path('/srv/stockagent-d-volume/stockagent-backup-ingress-lab203')
    if not physical.samefile(Path(backup_config['transport_root'])) or shutil.disk_usage(physical).free < 64*1024**3:
        raise ValueError('traditional SQL backup needs its enrolled D transport reserve')
    staging = physical / '.staging' / ('lifecycle-controls-' + uuid.uuid4().hex)
    staging.mkdir(parents=True)
    # PostgreSQL OS account has loopback peer access; exported snapshots and
    # all-table fingerprints are generated in the SAME SQL transaction.
    import psycopg
    for db in ('stockagent_ducklake','stockagent_temporal','stockagent_temporal_visibility'):
        with psycopg.connect(host='127.0.0.1', dbname=db, user=c['catalog_role'] if db=='stockagent_ducklake' else 'stockagent_temporal',
            password=c['catalog_password'] if db=='stockagent_ducklake' else __import__('yaml').safe_load(Path('/etc/stockagent/temporal/stockagent.yaml').read_text())['persistence']['datastores']['default']['sql']['password']) as connection:
            connection.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
            snapshot=connection.execute('SELECT pg_export_snapshot()').fetchone()[0]
            logical=logical_database_state(connection)
            dump=subprocess.run(['runuser','-u','postgres','--','pg_dump','-Fc','--no-owner','--no-privileges','--snapshot',snapshot,db],capture_output=True,timeout=180)
            if dump.returncode:raise RuntimeError('dedicated lifecycle SQL backup failed')
        root=staging / db
        root.mkdir()
        (root/'archive.backup').write_bytes(dump.stdout)
        atomic_write_json(root/'logical-state.json',logical)
        atomic_write_json(root/'receipt.json',{'contract':CONTROL,'archive':'archive.backup','logical_state_file':'logical-state.json',
            'same_mvcc_snapshot_as_dump':True,'sha256':digest(root/'archive.backup'),'logical_state_file_sha256':digest(root/'logical-state.json'),
            'logical_state_identity_sha256':logical['identity_sha256'],'table_count':logical['table_count'],'row_count':logical['row_count'],
            'source_observed_at_utc':datetime.now(timezone.utc).isoformat(),'bytes':len(dump.stdout),'control_database_restore_verified':False})
    files=[{'relative':p.relative_to(staging).as_posix(),'sha256':digest(p),'bytes':p.stat().st_size} for p in sorted(staging.rglob('*')) if p.is_file()]
    body={'contract':CONTRACT,'source_plan_identity_sha256':identity_sha256(files),'files':files,'producer_node_id':'penguin',
          'source_scope':'same-MVCC individual lakehouse/Temporal SQL backups; no role passwords',
          'selection':'lifecycle_controls_backup','all_history_verified':False,'unpublished_sources_included':False,'complete_release_in_this_delivery':False}
    envelope={**body,'identity_sha256':identity_sha256(body)}
    atomic_write_json(staging/'backup-envelope.json',envelope)
    atomic_write_text(staging/'READY',envelope['identity_sha256']+'\n')
    verified=verify(staging,envelope['identity_sha256'])
    target=physical / ('batch-'+envelope['identity_sha256'])
    dispatch=signed({'contract':'nas_backup_dispatch_v1','batch_relative':target.name,
        'producer_device_id':backup_config['producer_device_id'],'receiver_device_id':backup_config['receiver_device_id'],
        'repository_id':backup_config['repository_id'],'envelope_identity_sha256':envelope['identity_sha256'],
        'envelope_file_sha256':digest(staging/'backup-envelope.json'),'complete_files':len(files)+2,
        'complete_bytes':sum(p.stat().st_size for p in staging.rglob('*') if p.is_file()),'delivery_kind':'lifecycle_controls_backup'})
    # Unique SQL backup namespace: original source ledger/data owner is not
    # modified. Existing relay performs its unchanged fixed snapshot restore.
    result={'state':'publication_prepared','dispatch':dispatch,'staging_root':str(staging),
        'delivery_identity_sha256':envelope['identity_sha256'],
        'databases':['stockagent_ducklake','stockagent_temporal','stockagent_temporal_visibility'],
        'last_nas_acceptance':previous.get('last_nas_acceptance') if previous else None,
        'observed_at_utc':datetime.now(timezone.utc).isoformat()}
    atomic_write_json(previous_path,result)
    staging.rename(target)
    atomic_public(physical/'tools/dispatch'/(envelope['identity_sha256']+'.json'),dispatch)
    result['state']='waiting_encrypted_nas_backup'
    atomic_write_json(previous_path,result)
    from scripts.configure_artifact_ingress_syncthing import credentials,request
    base,key=credentials()
    for sub in ('tools/dispatch',target.name):
        request(base,key,'/rest/db/scan',{'folder':'stockagent-backup-ingress-lab203','sub':sub},method='POST',timeout=30)
    print(json.dumps({'state':result['state'],'delivery_identity_sha256':envelope['identity_sha256'],'complete_files':len(files)+2}))

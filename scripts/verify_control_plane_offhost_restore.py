#!/usr/bin/env python3
"""Restore the exact private control backup in an isolated off-host PG cluster.

No formal service is migrated. Existing remote code, Python, DB and GPU owners
remain untouched. Mamba creates isolated roles; only this run's socket-only
cluster is started/stopped. All receipts and the transferred backup are private.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import shlex
import sys
import zipfile

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from downloader.artifact_io import atomic_write_json
from scripts.verify_control_plane_pilot import Pilot
from stockagent.runtime_identity import identity_sha256, stable_source_sha256

FILES = (
    'stockagent/__init__.py', 'stockagent/runtime_identity.py',
    'stockagent/control/__init__.py', 'stockagent/control/backup.py',
    'downloader/__init__.py', 'downloader/artifact_io.py',
    'scripts/__init__.py', 'scripts/manage_runtime_environments.py',
    'configs/environments/control-recovery.yml',
)


def recorded_backup(path: Path) -> tuple[dict, Path, Path]:
    receipt = json.loads(path.read_bytes())
    if receipt.get('state') != 'backup_written' or not receipt.get('same_mvcc_snapshot_as_dump'):
        raise ValueError('an exact exported-snapshot backup is required')
    archive = Path(receipt['archive']).resolve(strict=True)
    state = Path(receipt['logical_state_file']).resolve(strict=True)
    if archive.parent != path.resolve().parent or state.parent != archive.parent:
        raise ValueError('backup objects must belong to the receipt directory')
    for file, key in ((archive, 'sha256'), (state, 'logical_state_file_sha256')):
        if file.stat().st_mode & 0o077:
            raise ValueError('backup objects must remain private')
        if hashlib.sha256(file.read_bytes()).hexdigest() != receipt[key]:
            raise ValueError('backup object differs from receipt')
    value = json.loads(state.read_bytes())
    if value['identity_sha256'] != receipt['logical_state_identity_sha256']:
        raise ValueError('backup logical identity differs')
    body = {key:value[key] for key in ('contract', 'tables')}
    if identity_sha256(body) != value['identity_sha256']:
        raise ValueError('backup state is not internally consistent')
    return receipt, archive, state


def bundle(output: Path) -> dict:
    files = {name: stable_source_sha256(ROOT, name) for name in FILES}
    archive = output/'recovery-code.zip'
    with zipfile.ZipFile(archive, 'x', compression=zipfile.ZIP_DEFLATED) as target:
        for name, digest in files.items():
            body = (ROOT/name).read_bytes()
            if hashlib.sha256(body).hexdigest() != digest:
                raise ValueError('recovery code changed during capture')
            target.writestr(name, body)
    result = {'files':files, 'identity_sha256':identity_sha256(files),
              'archive_sha256':hashlib.sha256(archive.read_bytes()).hexdigest()}
    atomic_write_json(output/'recovery-code.json', result)
    return result


class Recovery(Pilot):
    def execute(self):
        self.remote_root = self.remote_root.replace('control-pilot', 'control-recovery')
        remote = self.remote_root
        suffix = remote.rsplit('-',1)[1]
        # Existing stockagent parents can intentionally be private to root.
        # New public binaries/private PG state have separate owned parents;
        # never loosen an existing role owner's directory permissions.
        prefix_root = '/opt/stockagent-control-recovery-'+suffix
        cluster = '/var/lib/stockagent-control-recovery-'+suffix
        backup, archive, state = recorded_backup(self.args.backup_receipt)
        atomic_write_json(self.output/'source-backup.json', backup)
        delivery = bundle(self.output)
        self.remote('set -eu\numask 077\nmkdir '+shlex.quote(remote)+'\n')
        for path in (self.output/'recovery-code.zip', self.output/'recovery-code.json', archive, state):
            self.run([*self.scp, str(path), self.target+':'+remote+'/'])
        atomic_write_json(self.output/'remote-paths.json', {'code_root':remote,
                          'prefix_root':prefix_root,'owned_cluster':cluster})
        unpack = '''import hashlib,json,sys,zipfile
from pathlib import Path,PurePosixPath
r=Path(sys.argv[1]); p=json.loads((r/'recovery-code.json').read_bytes())
assert hashlib.sha256((r/'recovery-code.zip').read_bytes()).hexdigest()==p['archive_sha256']
with zipfile.ZipFile(r/'recovery-code.zip') as z:
 assert set(z.namelist())==set(p['files']) and len(z.infolist())==len(p['files'])
 for name,digest in p['files'].items():
  part=PurePosixPath(name); assert not part.is_absolute() and '..' not in part.parts
  body=z.read(name); assert hashlib.sha256(body).hexdigest()==digest
  dest=r/'code'/name;dest.parent.mkdir(parents=True,exist_ok=True);dest.write_bytes(body)
print(json.dumps({'state':'exact_private_code_delivered','files':len(p['files'])}))
'''
        self.event('delivery',**json.loads(self.native(unpack,remote).stdout))
        preparation = '''import json,os,shutil,sys,time
from pathlib import Path
from scripts.manage_runtime_environments import create_role
from stockagent.runtime_identity import runtime_identity,validate_runtime_lock
r=Path(sys.argv[1]); prefixes=Path(sys.argv[2]); prefixes.mkdir(mode=0o755)
# Role binaries must be executable by the existing unprivileged PG identity.
# Change only this newly created directory, never shared package-cache files.
prefixes.chmod(0o755)
before=runtime_identity(); (r/'native-before.json').write_text(json.dumps(before))
os.environ['FINTECH_MAMBA_BIN']='/opt/stockagent/miniforge3/condabin/mamba'
spec=r/'code/configs/environments/control-recovery.yml'
# Prime package download/solver separately from the repeated warm comparisons.
primed=create_role(prefixes/'priming',spec,r/'role-priming')
lock=r/'role-priming/conda-explicit.txt'; samples=[]
reference=json.loads((r/'role-priming/conda-packages.json').read_bytes())
for round_number,order in enumerate((('yaml','explicit'),('explicit','yaml'),('yaml','explicit'))):
 for method in order:
  out=r/f'role-{method}-{round_number}'; pre=prefixes/f'{method}-{round_number}'
  proof=create_role(pre,lock if method=='explicit' else spec,out,explicit=method=='explicit')
  inventory=json.loads((out/'conda-packages.json').read_bytes())
  assert inventory==reference
  samples.append({'method':method,'round':round_number,'complete_wall_seconds':proof['complete_wall_seconds'],
                  'prefix':str(pre),'receipt':str(out/'acceptance.json')})
means={method:sum(v['complete_wall_seconds'] for v in samples if v['method']==method)/3 for method in ('yaml','explicit')}
selected=min(means,key=means.get); chosen=next(v for v in reversed(samples) if v['method']==selected)
after=runtime_identity(); assert not validate_runtime_lock(before,after)
for directory,_,_ in os.walk(chosen['prefix']):
 path=Path(directory)
 assert path.resolve().is_relative_to(Path(chosen['prefix']).resolve())
 path.chmod(path.stat().st_mode | 0o055)
result={'state':'accepted','samples':samples,'mean_seconds':means,'selected_method':selected,
 'selected_prefix':chosen['prefix'],'native_runtime_unchanged':True,'priming_wall_seconds':primed['complete_wall_seconds'],
 'scope':'three interleaved fresh-role creations per method after separate cache priming; complete observed wall times'}
(r/'role-build-comparison.json').write_text(json.dumps(result,indent=2))
(r/'native-after-role.json').write_text(json.dumps(after))
print(json.dumps(result))
'''
        self.event('role_preparation_started',scope='private PostgreSQL role; no native environment update')
        prepared=json.loads(self.native(preparation,remote,prefix_root,code=True,timeout=1200).stdout)
        atomic_write_json(self.output/'role-build-comparison.json',prepared)
        self.event('role_ready',selected=prepared['selected_method'],means=prepared['mean_seconds'])
        prefix=prepared['selected_prefix']
        cluster_started=False
        try:
            # The existing nobody identity owns only a newly created private
            # directory. Trust auth is restricted to its 0700 UNIX socket;
            # PostgreSQL has no TCP listener and retains fsync.
            setup=f'''set -euo pipefail
umask 077
cd /var/lib
test ! -e {shlex.quote(cluster)}
install -d -m 0700 -o nobody -g nogroup {shlex.quote(cluster)}
install -d -m 0700 -o nobody -g nogroup {shlex.quote(cluster+'/socket')}
runuser -u nobody -- {shlex.quote(prefix+'/bin/initdb')} -D {shlex.quote(cluster+'/data')} -U stockagent_recovery -A trust --locale=C --encoding=UTF8 >{shlex.quote(remote+'/initdb.private.log')} 2>&1
runuser -u nobody -- {shlex.quote(prefix+'/bin/pg_ctl')} -D {shlex.quote(cluster+'/data')} -l {shlex.quote(cluster+'/postgres.private.log')} -o {shlex.quote('-p 55436 -k '+cluster+'/socket -c listen_addresses= -c shared_buffers=16MB -c max_connections=12 -c work_mem=2MB -c maintenance_work_mem=16MB')} -w start
'''
            self.remote(setup)
            cluster_started=True
            evaluation=REMOTE_RESTORE
            result=json.loads(self.role(evaluation,prefix,remote,cluster,archive.name,state.name,
                          backup['sha256'],backup['logical_state_file_sha256'],code=True,timeout=300).stdout)
            atomic_write_json(self.output/'restore-comparison.json',result)
            self.event('all_restore_states_match',tables=result['table_count'],rows=result['row_count'],
                       selected_workers=result['selected_workers'])
        finally:
            if cluster_started:
                self.remote('set -eu\ncd /var/lib\nrunuser -u nobody -- '+shlex.quote(prefix+'/bin/pg_ctl')+
                            ' -D '+shlex.quote(cluster+'/data')+' -m fast -w stop\n')
                self.event('owned_cluster_stopped')
        verification='''import hashlib,json,subprocess,sys
from pathlib import Path
from stockagent.runtime_identity import runtime_identity,validate_runtime_lock
r=Path(sys.argv[1]);before=json.loads((r/'native-before.json').read_bytes());after=runtime_identity()
assert not validate_runtime_lock(before,after)
p=json.loads((r/'recovery-code.json').read_bytes())
assert all(hashlib.sha256((r/'code'/n).read_bytes()).hexdigest()==v for n,v in p['files'].items())
cap=subprocess.run(['vast-capabilities'],capture_output=True,text=True)
assert cap.returncode==0
volume=json.loads(cap.stdout).get('instance',{}).get('workspace_is_volume')
proof={'native_runtime_unchanged':True,'exact_delivered_code_unchanged':True,'workspace_is_volume':volume}
(r/'native-after.json').write_text(json.dumps(after)); print(json.dumps(proof))
'''
        preserved=json.loads(self.native(verification,remote,code=True).stdout)
        # Copy receipts and the exact platform lock; exclude logs and database
        # rows. The source backup remains private at both independently hashed
        # locations and the original backup receipt remains historical.
        for name in ('role-priming/conda-explicit.txt','role-priming/conda-packages.json','native-before.json','native-after.json'):
            self.run([*self.scp,self.target+':'+remote+'/'+name,str(self.output/Path(name).name)])
        proof={'schema_version':1,'state':'accepted','observed_at_utc':datetime.now(timezone.utc).isoformat(),
               'source_backup_sha256':backup['sha256'],'source_logical_identity_sha256':backup['logical_state_identity_sha256'],
               'delivery':delivery,'off_host_full_database_restore_verified':True,
               'same_snapshot_table_columns_and_rows_verified':True,
               'table_count':result['table_count'],'row_count':result['row_count'],
               'restore_samples':len(result['samples']),'selected_restore_workers':result['selected_workers'],
               'selected_role_build_method':prepared['selected_method'], 'cluster_stopped':True,
               'public_database_listener':False,'fsync_enabled':True,'role_credentials_transferred':False,
               'durable_off_host_backup_verified':False,
               **preserved,'scope':'private engineering DB recovery rehearsal; no HA/PITR/fleet migration or source/model recovery proof'}
        atomic_write_json(self.output/'acceptance.json',proof)
        return proof

    def native(self,script_text,*arguments,code=False,timeout=180):
        return self.role(script_text,None,*arguments,code=code,timeout=timeout)

    def role(self,script_text,prefix,*arguments,code=False,timeout=180):
        script='set -euo pipefail\numask 077\ncd /root/stockAgent\n'
        if prefix:
            script+='export FINTECH_ENV_PATH='+shlex.quote(prefix)+'\nunset PYTHON_BIN\n'
        script+='source scripts/runtime_env.sh\n'
        if code:
            script_text="import sys\nsys.path.insert(0,"+repr(self.remote_root+'/code')+")\n"+script_text
        script+='run_fintech_python - '+shlex.join(arguments)+" <<'RECOVERY_PY'\n"+script_text+'\nRECOVERY_PY\n'
        return self.remote(script,timeout=timeout)


REMOTE_RESTORE = '''import hashlib,json,os,subprocess,sys,time
from pathlib import Path
import psycopg
from psycopg import sql
from stockagent.control.backup import logical_database_state
r=Path(sys.argv[1]);cluster=Path(sys.argv[2]);archive=r/sys.argv[3];statefile=r/sys.argv[4]
assert hashlib.sha256(archive.read_bytes()).hexdigest()==sys.argv[5]
assert hashlib.sha256(statefile.read_bytes()).hexdigest()==sys.argv[6]
expected=json.loads(statefile.read_bytes());socket=str(cluster/'socket')
prefix=Path(sys.prefix); samples=[]
with psycopg.connect(host=socket,port=55436,user='stockagent_recovery',dbname='postgres',autocommit=True) as admin:
 assert admin.execute('SHOW listen_addresses').fetchone()[0]==''
 assert admin.execute('SHOW fsync').fetchone()[0]=='on'
 admin.execute('CREATE ROLE stockagent_control LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE')
 for round_number,order in enumerate(((1,2,4),(4,2,1),(2,1,4))):
  for workers in order:
   db=f'recovery_{round_number}_{workers}'; started=time.perf_counter()
   admin.execute(sql.SQL('CREATE DATABASE {} OWNER stockagent_control TEMPLATE template0').format(sql.Identifier(db)))
   try:
    command=[str(prefix/'bin/pg_restore'),'--exit-on-error','--no-owner','--no-privileges','--role','stockagent_control',
             '--host',socket,'--port','55436','--username','stockagent_recovery','--dbname',db,'--jobs',str(workers),str(archive)]
    restored=time.perf_counter(); result=subprocess.run(command,capture_output=True,timeout=90)
    (r/f'restore-{round_number}-{workers}.private.log').write_bytes(result.stdout+result.stderr)
    assert result.returncode==0, 'pg_restore failed; private diagnostic retained'
    restore_seconds=time.perf_counter()-restored
    with psycopg.connect(host=socket,port=55436,user='stockagent_control',dbname=db,autocommit=True) as peer:
     with peer.transaction():
      peer.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
      actual=logical_database_state(peer)
      assert actual==expected, 'restored user tables/columns/rows differ'
    samples.append({'round':round_number,'workers':workers,'restore_seconds':restore_seconds,
                    'create_restore_verify_seconds':time.perf_counter()-started,
                    'logical_identity_sha256':actual['identity_sha256']})
   finally:
    admin.execute(sql.SQL('DROP DATABASE {}').format(sql.Identifier(db)))
means={w:sum(v['create_restore_verify_seconds'] for v in samples if v['workers']==w)/3 for w in (1,2,4)}
proof={'state':'accepted','samples':samples,'mean_create_restore_verify_seconds':means,
       'selected_workers':min(means,key=means.get),'table_count':expected['table_count'],'row_count':expected['row_count'],
       'exact_state_verified':True,'scope':'nine complete DB creation/restore/state checks; disposable DB teardown excluded from measured time'}
(r/'restore-comparison.json').write_text(json.dumps(proof,indent=2));print(json.dumps(proof))
'''


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--backup-receipt',type=Path,default=Path('/var/lib/stockagent/control-plane/backups/latest.json'))
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--remote-env',type=Path,default=Path('/etc/stockagent/remote-cold-artifact-ingress.env'))
    parser.add_argument('--control-env',type=Path,default=Path('/etc/stockagent/control-plane.env'))
    args=parser.parse_args()
    old=os.umask(0o077)
    try:
        proof=Recovery(args).execute()
        print(json.dumps({key:proof[key] for key in ('state','table_count','row_count','restore_samples','durable_off_host_backup_verified')}))
    finally:
        os.umask(old)


if __name__=='__main__':
    main()

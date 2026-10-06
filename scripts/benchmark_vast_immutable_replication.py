#!/usr/bin/env python3
"""Actual SFTP rclone replication and independent remote SHA on fresh owned probes."""
import argparse
import json
from pathlib import Path
import shlex
import subprocess
import sys
import time
import uuid

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from stockagent.remote_ssh import ssh_base as _ssh_base, validate_ssh_target as _validate_ssh_target
from stockagent.control.lakehouse import configuration
from stockagent.data_sync.immutable_replication import inventory, verify
from downloader.artifact_io import atomic_write_json, atomic_write_text

parser=argparse.ArgumentParser(description=__doc__)
parser.add_argument('--delivery-id',required=True)
parser.add_argument('--output',type=Path,required=True)
args=parser.parse_args()
args.output.mkdir(parents=True,mode=0o700,exist_ok=False)
c=configuration()
source=Path(c['lake_root'])/'releases'/('lake-'+args.delivery_id)
verify(source)
expected=inventory(source)
env=Path('/etc/stockagent/remote-cold-artifact-ingress.env')
if env.is_symlink() or env.stat().st_mode & 0o077:raise ValueError('existing SSH environment must be private')
values={}
for line in env.read_text().splitlines():
    if line.startswith('COLD_ARTIFACT_INGRESS_') and '=' in line:
        k,body=line.split('=',1);parts=shlex.split(body,comments=True)
        if len(parts)==1:values[k]=parts[0]
target=_validate_ssh_target(values['COLD_ARTIFACT_INGRESS_SSH_TARGET'])
key=Path(values['COLD_ARTIFACT_INGRESS_IDENTITY_FILE'])
port=int(values['COLD_ARTIFACT_INGRESS_SSH_PORT'])
ssh=_ssh_base(key,port)
resolved=subprocess.run([*ssh,'-G',target],text=True,capture_output=True,check=True).stdout
options=dict(line.split(' ',1) for line in resolved.splitlines() if ' ' in line)
known=Path(options['userknownhostsfile'].split()[0]).expanduser()
config=Path(c['state_root'])/'rclone-vast.private.conf'
atomic_write_text(config,'[vast-lake]\ntype = sftp\nhost = '+options['hostname']+'\nuser = '+options['user']+'\nport = '+str(port)+'\nkey_file = '+str(key)+'\nknown_hosts_file = '+str(known)+'\n')
config.chmod(0o600)
repo=values['COLD_ARTIFACT_INGRESS_REMOTE_REPO_ROOT']
probe='/workspace/.stockagent-immutable-probes-'+uuid.uuid4().hex
def remote(body):
    result=subprocess.run([*ssh,'-o','StrictHostKeyChecking=yes',target,'cd '+shlex.quote(repo)+' && source scripts/runtime_env.sh && run_fintech_python -'],
                          input=body,text=True,capture_output=True,timeout=60)
    if result.returncode:raise RuntimeError('owned remote replication verifier failed')
    return json.loads(result.stdout)
before=remote("import json,shutil,subprocess\nprint(json.dumps({'free_bytes':shutil.disk_usage('/workspace').free,'rclone_version':subprocess.check_output(['rclone','version'],text=True).splitlines()[0]}))")
if before['free_bytes']<4*1024**3:raise ValueError('Vast probe requires real free capacity')
samples=[]
for workers in (1,2,4,1,2,4):
    path=probe+'/round-'+str(len(samples))
    started=time.perf_counter()
    result=subprocess.run([str(Path(c['binaries'])/'bin/rclone'),'copy',str(source),'vast-lake:'+path,'--config',str(config),
        '--immutable','--checksum','--transfers',str(workers),'--checkers',str(workers),'--buffer-size','8Mi','--retries','1','--stats','0'],capture_output=True,timeout=180)
    if result.returncode:raise RuntimeError('real immutable SFTP replication failed: '+str(result.returncode))
    body="from pathlib import Path\nimport hashlib,json\nroot=Path("+repr(path)+")\nfiles={}\nfor p in sorted(root.rglob('*')):\n if p.is_symlink():raise ValueError('redirected remote probe')\n if p.is_file():\n  with p.open('rb') as f: h=hashlib.file_digest(f,'sha256').hexdigest()\n  files[p.relative_to(root).as_posix()]={'sha256':h,'bytes':p.stat().st_size}\nprint(json.dumps(files))"
    actual=remote(body)
    if actual!=expected:raise ValueError('remote exact file set/full SHA differs')
    samples.append({'workers':workers,'complete_workflow_seconds':time.perf_counter()-started,'complete_files':len(actual),
                    'complete_bytes':sum(v['bytes'] for v in actual.values()),'command_exit_codes':[0,0],'all_files_sha256_verified':True})
    atomic_write_json(args.output/'samples.json',samples)
# These new probes are not training inputs, caches, checkpoints or source
# releases. Revalidate the complete known set, then remove only our own probes.
cleanup="from pathlib import Path\nimport json,hashlib,shutil\nroot=Path("+repr(probe)+")\nexpected="+repr(expected)+"\nif set(p.name for p in root.iterdir())!=set('round-'+str(i) for i in range(6)):raise ValueError('unknown probe child')\nfor folder in root.iterdir():\n actual={}\n for p in folder.rglob('*'):\n  if p.is_symlink():raise ValueError('redirected probe')\n  if p.is_file():\n   with p.open('rb') as f:h=hashlib.file_digest(f,'sha256').hexdigest()\n   actual[p.relative_to(folder).as_posix()]={'sha256':h,'bytes':p.stat().st_size}\n if actual!=expected:raise ValueError('probe changed before exact cleanup')\nshutil.rmtree(root)\nprint(json.dumps({'only_owned_probe_removed':True,'source_or_training_data_deleted':False}))"
cleaned=remote(cleanup)
means={n:sum(r['complete_workflow_seconds'] for r in samples if r['workers']==n)/2 for n in (1,2,4)}
proof={'state':'accepted','before':before,'samples':samples,'means_seconds':means,'selected_workers':min(means,key=means.get),
       'cleanup':cleaned,'delivery_identity_sha256':args.delivery_id,'scope':'real fixed catalog/data SFTP copy plus independent SHA; not a full GPU training or large-payload benchmark'}
atomic_write_json(args.output/'acceptance.json',proof)
print(json.dumps(proof))

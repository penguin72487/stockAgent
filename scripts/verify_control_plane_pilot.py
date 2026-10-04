#!/usr/bin/env python3
"""Verify the opt-in work controller on two real nodes and a restored database.

The action is the existing frozen-source verifier. This acceptance never starts
training, consumes provider quota, publishes data, or sends orders. SSH connects
with an existing trusted key; the database has no public listener.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import shlex
import subprocess
import sys
import time
from urllib.parse import urlsplit, urlunsplit
import uuid
import zipfile

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from downloader.artifact_io import atomic_write_bytes, atomic_write_json  # noqa: E402
from downloader.common import load_env_file  # noqa: E402
from stockagent.control.contracts import WorkSpec  # noqa: E402
from stockagent.runtime_identity import (  # noqa: E402
    identity_sha256, stable_source_sha256, validate_runtime_lock, verify_source_release,
)

CONTROLLER_FILES = (
    'stockagent/__init__.py', 'stockagent/runtime_identity.py', 'stockagent/remote_build.py',
    'stockagent/control/__init__.py', 'stockagent/control/contracts.py',
    'stockagent/control/_schema.py', 'stockagent/control/postgres.py',
    'stockagent/control/worker.py', 'downloader/__init__.py', 'downloader/artifact_io.py',
    'scripts/__init__.py', 'scripts/accept_project_release.py',
    'scripts/manage_control_work.py', 'scripts/stockagent-control.sh', 'scripts/runtime_env.sh',
    'scripts/manage_runtime_environments.py', 'configs/environments/control.yml',
    'scripts/download_pinned_tool.py',
)


def controller_bundle(output: Path) -> dict:
    files = {name: stable_source_sha256(ROOT, name) for name in CONTROLLER_FILES}
    archive = output / 'controller.zip'
    with zipfile.ZipFile(archive, 'x', compression=zipfile.ZIP_DEFLATED) as target:
        for name in files:
            body = (ROOT / name).read_bytes()
            if hashlib.sha256(body).hexdigest() != files[name]:
                raise RuntimeError('controller changed during delivery')
            target.writestr(name, body)
    proof = {'schema_version': 1, 'files': files, 'identity_sha256': identity_sha256(files),
             'archive_sha256': hashlib.sha256(archive.read_bytes()).hexdigest(),
             'scope': 'control-role delivery only; separate from the verified code release'}
    atomic_write_json(output / 'controller-delivery.json', proof)
    return proof


def private_env(path: Path, dsn: str, environment: str) -> None:
    body = ('CONTROL_PLANE_DSN=' + shlex.quote(dsn) + '\nCONTROL_PLANE_ENV_PATH=' +
            shlex.quote(environment) + '\n').encode()
    old = os.umask(0o077)
    try:
        atomic_write_bytes(path, body, durable=True)
        path.chmod(0o600)
    finally:
        os.umask(old)


def accepted_snapshot(snapshot: dict, source: dict) -> dict:
    """Admit the real node, dependency, failed attempt and exact source proofs."""
    nodes = {row['node_id'] for row in snapshot['nodes']}
    jobs = {row['key']: row for row in snapshot['jobs']}
    if nodes != {'penguin-control-pilot', 'vast-control-pilot'} or set(jobs) != {
            'local-release', 'remote-release', 'recovered-release'}:
        raise ValueError('pilot must contain exactly the expected nodes and work')
    if snapshot['dependencies'] != [{'job_key': 'remote-release', 'dependency_key': 'local-release'}]:
        raise ValueError('dependency proof differs')
    for row in jobs.values():
        result = row.get('result') or {}
        if row['state'] != 'succeeded' or any(result.get(k) != source[k] for k in (
                'receipt_sha256', 'source_sha256', 'verified_file_count', 'source_files_verified')):
            raise ValueError('pilot completion is not an exact canonical source verification')
        if result.get('data_publication') or result.get('loaded_service_revision_verified'):
            raise ValueError('code proof cannot establish data or loaded-service readiness')
    actual = {(r['job_key'], r['attempt'], r['node_id'], r['state']) for r in snapshot['attempts']}
    expected = {('local-release', 1, 'penguin-control-pilot', 'succeeded'),
                ('remote-release', 1, 'vast-control-pilot', 'succeeded'),
                ('recovered-release', 1, 'vast-control-pilot', 'expired'),
                ('recovered-release', 2, 'penguin-control-pilot', 'succeeded')}
    if actual != expected or len(snapshot['attempts']) != 4:
        raise ValueError('real crash/recovery attempt history differs')
    return {'nodes': sorted(nodes), 'completed_work': len(jobs), 'attempts': len(actual),
            'remote_crash_recovered': True, 'matching_source_verified': True}


class Pilot:
    def __init__(self, args):
        self.args = args
        self.output = args.output.resolve()
        self.output.mkdir(mode=0o700, parents=True, exist_ok=False)
        suffix = uuid.uuid4().hex[:12]
        self.schema = 'control_pilot_' + suffix
        self.restore_db = 'control_restore_' + suffix
        self.remote_root = '/root/stockagent-control-pilot-' + suffix
        self.unit = 'stockagent-control-pilot-tunnel-' + suffix
        self.restore_created = False
        self.tunnel_started = False
        self.events = []
        load_env_file(args.remote_env, allowed_names=[
            'COLD_ARTIFACT_INGRESS_SSH_TARGET', 'COLD_ARTIFACT_INGRESS_SSH_PORT',
            'COLD_ARTIFACT_INGRESS_IDENTITY_FILE'], override=True)
        self.target = os.environ['COLD_ARTIFACT_INGRESS_SSH_TARGET']
        port = os.environ['COLD_ARTIFACT_INGRESS_SSH_PORT']
        key = os.environ['COLD_ARTIFACT_INGRESS_IDENTITY_FILE']
        if not port.isdecimal() or not Path(key).is_file():
            raise ValueError('trusted remote SSH configuration is incomplete')
        self.ssh = ['ssh', '-oBatchMode=yes', '-oStrictHostKeyChecking=yes',
                    '-oConnectTimeout=10', '-i', key, '-p', port]
        self.scp = ['scp', '-q', '-oBatchMode=yes', '-oStrictHostKeyChecking=yes',
                    '-oConnectTimeout=10', '-i', key, '-P', port]
        load_env_file(args.control_env, allowed_names=['CONTROL_PLANE_DSN', 'CONTROL_PLANE_ENV_PATH'], override=True)
        self.dsn = os.environ['CONTROL_PLANE_DSN']

    def event(self, name: str, **values):
        self.events.append({'event': name, 'observed_at_utc': datetime.now(timezone.utc).isoformat(), **values})
        atomic_write_json(self.output / 'events.json', self.events)
        print(json.dumps(self.events[-1]), flush=True)

    def run(self, argv, *, body=None, accepted=(0,), timeout=180):
        result = subprocess.run(argv, input=body, capture_output=True, timeout=timeout, cwd=ROOT)
        if result.returncode not in accepted:
            path=self.output/'subprocess-failure.private.log'
            atomic_write_bytes(path,result.stdout+result.stderr)
            path.chmod(0o600)
            # Process errors can contain connection strings. Keep raw private
            # env values out of receipts, argv, exceptions and tool output.
            raise RuntimeError(f'pilot subprocess exit {result.returncode}: {Path(argv[0]).name}')
        return result

    def remote(self, script, *, accepted=(0,), timeout=180):
        return self.run([*self.ssh, self.target, 'bash -s'], body=script.encode(),
                        accepted=accepted, timeout=timeout)

    def work(self, *arguments, env_file=None):
        environment = {**os.environ, 'STOCKAGENT_CONTROL_ENV_FILE': str(env_file or self.args.control_env)}
        result = subprocess.run(['bash', str(ROOT / 'scripts/stockagent-control.sh'),
                                 '--schema', self.schema, *map(str, arguments)],
                                capture_output=True, timeout=60, env=environment, cwd=ROOT)
        if result.returncode:
            raise RuntimeError(f'local control operation failed: {arguments[0]}')
        return json.loads(result.stdout)

    def admin(self, sql, *, database='postgres'):
        return self.run(['runuser', '-u', 'postgres', '--', 'psql', '-X', '-v', 'ON_ERROR_STOP=1',
                         '-d', database, '-At'], body=sql.encode())

    def submit(self, key, inputs, dependencies=()):
        path = self.output / (key + '.spec.json')
        atomic_write_json(path, WorkSpec(key, inputs, dependencies=dependencies).as_dict())
        self.work('submit', path)

    def prepare_remote(self, release, delivery):
        r = self.remote_root
        self.remote(f'set -eu\numask 077\nmkdir {shlex.quote(r)}\n')
        for file in ['controller.zip', 'controller-delivery.json']:
            self.run([*self.scp, str(self.output / file), self.target + ':' + r + '/'])
        for file in ['release.json', release['source_bundle']['file'], release['wheel']['file']]:
            self.run([*self.scp, str(self.args.receipt.parent / file), self.target + ':' + r + '/'])
        # Use native Python discovery to unpack only our exact controller files.
        # The accepted code release uses the repository's canonical extractor.
        script = f'''set -euo pipefail
cd /root/stockAgent
source scripts/runtime_env.sh
run_fintech_python - {shlex.quote(r)} <<'PY'
import hashlib,json,sys,zipfile
from pathlib import Path, PurePosixPath
root=Path(sys.argv[1]); proof=json.loads((root/'controller-delivery.json').read_text())
archive=root/'controller.zip'
assert hashlib.sha256(archive.read_bytes()).hexdigest()==proof['archive_sha256']
with zipfile.ZipFile(archive) as z:
    assert len(z.infolist())==len(proof['files']) and set(z.namelist())==set(proof['files'])
    for name in proof['files']:
        p=PurePosixPath(name)
        assert not p.is_absolute() and '..' not in p.parts and str(p)==name and '\\\\' not in name
        body=z.read(name); assert hashlib.sha256(body).hexdigest()==proof['files'][name]
        dest=root/'controller'/name;dest.parent.mkdir(parents=True,exist_ok=True);dest.write_bytes(body)
sys.path.insert(0,str(root/'controller'))
from scripts.accept_project_release import extract_recorded_sources
extract_recorded_sources(root/'release.json',root/'source')
print(json.dumps({{'state':'delivered','controller_files':len(proof['files'])}}))
PY
'''
        self.event('remote_delivery', **json.loads(self.remote(script).stdout))
        probe = f'''set -euo pipefail
cd {shlex.quote(r + '/controller')}
source /root/stockAgent/scripts/runtime_env.sh
run_fintech_python - <<'PY'
import json,os,sys,shutil
from pathlib import Path
from stockagent.runtime_identity import runtime_identity
from stockagent.remote_build import observe_node
prefix=Path(sys.prefix)
candidates=[shutil.which('mamba'),str(prefix.parent.parent/'condabin/mamba'),str(prefix.parent.parent/'bin/mamba'),'/opt/stockagent/miniforge3/condabin/mamba']
manager=next((p for p in candidates if p and Path(p).is_file()),None)
print(json.dumps({{'native_runtime':runtime_identity(),'native_python':sys.executable,
 'mamba':manager,'node_profile':observe_node(Path('.'),Path('.')),
 'cpu_affinity_count':len(os.sched_getaffinity(0)),
 'free_bytes':shutil.disk_usage('.').free}}))
PY
'''
        before = json.loads(self.remote(probe).stdout)
        atomic_write_json(self.output / 'remote-native-before.json', before)
        if before['free_bytes'] < 1024 * 1024**2 or before['node_profile']['limits']['memory_headroom_bytes'] < 256*1024**2:
            raise ValueError('remote control role capacity/preflight failed')
        if not before['mamba']:
            if not self.args.install_remote_miniforge:
                raise ValueError('remote Miniforge/mamba missing; select --install-remote-miniforge for a fresh owned base')
            bootstrap = f'''set -euo pipefail
umask 077
cd {shlex.quote(r + '/controller')}
source /root/stockAgent/scripts/runtime_env.sh
run_fintech_python - {shlex.quote(r)} <<'PY'
import hashlib,json,subprocess,sys,time
from pathlib import Path
from downloader.artifact_io import atomic_write_json
from scripts.download_pinned_tool import download
root=Path(sys.argv[1]);prefix=Path('/opt/stockagent/miniforge3')
assert not prefix.exists(), 'preserve an existing unknown manager'
started=time.perf_counter()
url='https://github.com/conda-forge/miniforge/releases/download/26.7.2-0/Miniforge3-26.7.2-0-Linux-x86_64.sh'
expected='281b0ac7d550802efc81af633225a5e6116d29ae72f3ab4eae7168c3931a4c05'
archive=root/'miniforge-installer.sh'
with (root/'miniforge-transfer.log').open('w') as log:
    from contextlib import redirect_stdout
    with redirect_stdout(log):transfer=download(url,expected,archive)
prefix.parent.mkdir(parents=True,exist_ok=True)
result=subprocess.run(['bash',str(archive),'-b','-p',str(prefix)],capture_output=True,timeout=600)
(root/'miniforge-installation.log').write_bytes(result.stdout+result.stderr)
assert result.returncode==0, 'Miniforge installation failed; retain its private log'
manager=prefix/'condabin/mamba'
version=subprocess.run([str(manager),'--version'],capture_output=True,text=True,check=True).stdout.strip()
receipt={{'state':'accepted','url':url,'installer_sha256':expected,'prefix':str(prefix),
         'mamba':str(manager),'mamba_version':version,'complete_wall_seconds':time.perf_counter()-started,
         'transport':transfer,
         'shell_initialization_changed':False,'scope':'fresh isolated manager; original GPU venv not replaced'}}
atomic_write_json(prefix/'.stockagent-installation.json',receipt)
atomic_write_json(root/'miniforge-installation.json',receipt)
print(json.dumps(receipt))
PY
'''
            installed = json.loads(self.remote(bootstrap,timeout=900).stdout)
            atomic_write_json(self.output/'remote-miniforge-installation.json',installed)
            before['mamba'] = installed['mamba']
            self.event('remote_miniforge_installed',mamba_version=installed['mamba_version'],
                       complete_wall_seconds=installed['complete_wall_seconds'])
        self.run([*self.scp, str(self.args.role_lock), self.target+':'+r+'/control-conda-explicit.txt'])
        install = f'''set -euo pipefail
umask 077
cd {shlex.quote(r + '/controller')}
export FINTECH_MAMBA_BIN={shlex.quote(before['mamba'])}
source /root/stockAgent/scripts/runtime_env.sh
run_fintech_python - {shlex.quote(r)} <<'PY'
import json,sys,statistics
from pathlib import Path
from scripts.manage_runtime_environments import create_role,conda_inventory
from downloader.artifact_io import atomic_write_json
from stockagent.runtime_identity import runtime_identity,validate_runtime_lock
root=Path(sys.argv[1]); runs=[]; expected=None
for iteration in range(3):
    for method in ('declaration','explicit'):
        prefix=root/('mamba-'+method+'-'+str(iteration))
        specification=Path('configs/environments/control.yml') if method=='declaration' else root/'control-conda-explicit.txt'
        proof=create_role(prefix,specification,root/('role-'+method+'-'+str(iteration)),explicit=method=='explicit')
        identity=[(p['name'],p['version'],p['build']) for p in conda_inventory(prefix)]
        if expected is None: expected=identity
        assert identity==expected, 'candidate role package builds differ'
        runs.append({{'method':method,'iteration':iteration,'prefix':str(prefix),**proof}})
means={{method:statistics.mean(p['complete_wall_seconds'] for p in runs if p['method']==method)
        for method in ('declaration','explicit')}}
winner=min(means,key=means.get)
selected=next(p['prefix'] for p in reversed(runs) if p['method']==winner)
result={{'state':'accepted','runs':runs,'mean_complete_seconds':means,'selected_method':winner,
         'selected_prefix':selected,'identical_conda_builds':True,
         'scope':'three interleaved builds per method on this node; first declaration includes cache warmup'}}
atomic_write_json(root/'remote-mamba-build-comparison.json',result)
print(json.dumps(result))
PY
'''
        result = self.remote(install,timeout=900)
        atomic_write_bytes(self.output / 'remote-role-install.log', result.stdout + result.stderr)
        comparison = json.loads(result.stdout)
        atomic_write_json(self.output/'remote-mamba-build-comparison.json',comparison)
        self.remote_role = comparison['selected_prefix']
        after = json.loads(self.remote(probe).stdout)
        atomic_write_json(self.output / 'remote-native-after.json', after)
        if validate_runtime_lock(before['native_runtime'], after['native_runtime']):
            raise ValueError('remote native runtime changed during role installation')
        self.event('remote_role_installed', native_runtime_unchanged=True,
                   manager='mamba',selected_method=comparison['selected_method'],
                   mean_complete_seconds=comparison['mean_complete_seconds'])
        parsed = urlsplit(self.dsn)
        if parsed.hostname not in {'127.0.0.1', 'localhost'}:
            raise ValueError('pilot database must be loopback')
        authority = parsed.netloc.rsplit('@', 1)[0] + '@127.0.0.1:' + str(self.args.forward_port)
        remote_dsn = urlunsplit(parsed._replace(netloc=authority))
        env_path = self.output / '.remote-control.env'
        private_env(env_path, remote_dsn, self.remote_role)
        self.run([*self.scp, str(env_path), self.target + ':' + r + '/control.env'])
        # Local temp holds only this test-role credential and is removed after transfer.
        env_path.unlink()
        self.remote(f'chmod 600 {shlex.quote(r + "/control.env")}\n')
        if self.remote(f"ss -H -lnt 'sport = :{self.args.forward_port}'\n").stdout.strip():
            raise ValueError('requested private forward port is already owned by another listener')
        tunnel = [*self.ssh, '-oExitOnForwardFailure=yes', '-oServerAliveInterval=15',
                  '-oServerAliveCountMax=2', '-N', '-T', '-R',
                  f'127.0.0.1:{self.args.forward_port}:127.0.0.1:{parsed.port or 5432}', self.target]
        self.run(['systemd-run', '--unit', self.unit, '--property=Slice=stockagent-control.slice',
                  '--property=RuntimeMaxSec=600', '--property=Nice=10', '--', *tunnel])
        self.tunnel_started = True
        for _ in range(30):
            listener = self.remote(f"ss -H -lnt 'sport = :{self.args.forward_port}'\n").stdout.decode()
            if listener:
                break
            time.sleep(.2)
        if not listener or any(row.split()[3] != f'127.0.0.1:{self.args.forward_port}' for row in listener.splitlines()):
            raise ValueError('reverse SSH database listener is not exclusively loopback')
        atomic_write_bytes(self.output / 'remote-loopback-listener.txt', listener.encode())
        self.event('private_tunnel_ready', public_database_listener=False)

    def remote_worker(self, *, crash=False):
        r = self.remote_root
        prefix = f'''set -euo pipefail
set -a
source {shlex.quote(r + '/control.env')}
set +a
export FINTECH_ENV_PATH="$CONTROL_PLANE_ENV_PATH"
unset PYTHON_BIN
cd {shlex.quote(r + '/controller')}
source scripts/runtime_env.sh
'''
        if crash:
            script = prefix + f'''run_fintech_python - {shlex.quote(self.schema)} {shlex.quote(r + '/source')} <<'PY'
import json,os,sys
from pathlib import Path
from stockagent.control.postgres import ControlStore
from stockagent.control.worker import node_observation
with ControlStore(os.environ['CONTROL_PLANE_DSN'],schema=sys.argv[1]) as store:
    store.register_node('vast-control-pilot',**node_observation(Path(sys.argv[2])))
    claim=store.claim('vast-control-pilot','intentional-crash',lease_seconds=2)
    assert claim and claim.spec.key=='recovered-release'
    print(json.dumps({{'work_key':claim.spec.key,'attempt':claim.attempt,'node_id':claim.node_id,
                      'lease_token':claim.token,'fault':'exit_before_handler'}}),flush=True)
raise SystemExit(23)
PY
'''
            return json.loads(self.remote(script, accepted=(23,)).stdout)
        script = prefix + shlex.join(['bash', 'scripts/stockagent-control.sh', '--schema', self.schema,
                                    'worker-once', '--node', 'vast-control-pilot', '--worker', 'remote-verifier',
                                    '--receipt', r + '/release.json', '--root', r + '/source',
                                    '--output', r + '/attempt-receipts']) + '\n'
        # Select the transferred private configuration; wrapper keeps canonical role discovery.
        script = script.replace('bash scripts/stockagent-control.sh',
                                'STOCKAGENT_CONTROL_ENV_FILE=' + shlex.quote(r + '/control.env') +
                                ' bash scripts/stockagent-control.sh')
        return json.loads(self.remote(script).stdout)

    def backup_restore(self, snapshot):
        dump = self.run(['runuser', '-u', 'postgres', '--', 'pg_dump', '-Fc', '--no-owner',
                         '--no-privileges', '-n', self.schema, '-d', 'stockagent_control']).stdout
        path = self.output / 'control-schema.backup'
        atomic_write_bytes(path, dump, durable=True); path.chmod(0o600)
        self.admin(f'CREATE DATABASE {self.restore_db} OWNER stockagent_control;')
        self.restore_created = True
        self.run(['runuser', '-u', 'postgres', '--', 'pg_restore', '--no-owner', '--no-privileges',
                  '--exit-on-error', '--role=stockagent_control', '-d', self.restore_db], body=dump)
        parsed = urlsplit(self.dsn)
        env_path = self.output / '.restore-control.env'
        private_env(env_path, urlunsplit(parsed._replace(path='/' + self.restore_db)),
                    os.environ['CONTROL_PLANE_ENV_PATH'])
        try:
            restored = self.work('status', env_file=env_path)
        finally:
            env_path.unlink()
        atomic_write_json(self.output / 'restored-state.json', restored)
        if restored != snapshot:
            raise ValueError('restored nodes/jobs/dependencies/attempts differ from the backup')
        proof = {'backup_sha256': hashlib.sha256(dump).hexdigest(), 'backup_bytes': len(dump),
                 'schema': self.schema, 'state_identity_sha256': identity_sha256(snapshot),
                 'exact_restore': True, 'scope': 'isolated pilot schema, not PostgreSQL HA or host disaster recovery'}
        atomic_write_json(self.output / 'backup-restore.json', proof)
        self.event('backup_restored', exact_restore=True, backup_bytes=len(dump))
        return proof

    def execute(self):
        started = time.perf_counter()
        source = verify_source_release(self.args.receipt, self.args.source_root)
        release = json.loads(self.args.receipt.read_bytes())
        delivery = controller_bundle(self.output)
        self.work('init')
        self.event('started', schema=self.schema, remote_root=self.remote_root,
                   source_sha256=source['source_sha256'], verified_file_count=source['verified_file_count'])
        try:
            self.prepare_remote(release, delivery)
            inputs = {key: source[key] for key in ('receipt_sha256', 'source_sha256')}
            self.submit('local-release', inputs)
            self.submit('remote-release', inputs, ('local-release',))
            local = self.work('worker-once', '--node', 'penguin-control-pilot', '--worker', 'local-verifier',
                              '--receipt', self.args.receipt, '--root', self.args.source_root,
                              '--output', self.output / 'attempt-receipts')
            atomic_write_json(self.output / 'local-worker.json', local)
            self.event('local_completed', state=local['state'])
            remote = self.remote_worker()
            atomic_write_json(self.output / 'remote-worker.json', remote)
            self.event('remote_completed', state=remote['state'])
            self.submit('recovered-release', inputs)
            fault = self.remote_worker(crash=True)
            atomic_write_json(self.output / 'remote-crash.json', fault)
            self.event('remote_crashed_before_handler', attempt=fault['attempt'])
            time.sleep(3)
            expired = self.work('expire-leases')
            if expired != {'expired_attempts': 1}:
                raise ValueError('real server-clock lease expiry was not observed')
            recovered = self.work('worker-once', '--node', 'penguin-control-pilot', '--worker', 'recovery-verifier',
                                  '--receipt', self.args.receipt, '--root', self.args.source_root,
                                  '--output', self.output / 'attempt-receipts')
            atomic_write_json(self.output / 'recovered-worker.json', recovered)
            snapshot = self.work('status')
            atomic_write_json(self.output / 'final-state.json', snapshot)
            proof = accepted_snapshot(snapshot, source)
            backup = self.backup_restore(snapshot)
            current = {name: stable_source_sha256(ROOT, name) for name in CONTROLLER_FILES}
            if current != delivery['files'] or verify_source_release(self.args.receipt, self.args.source_root) != source:
                raise ValueError('local controller or accepted source changed during the pilot')
            remote_check = f'''set -eu
cd {shlex.quote(self.remote_root + '/controller')}
{shlex.quote(self.remote_role + '/bin/python')} - <<'PY'
import hashlib,json
from pathlib import Path
p=json.loads(Path('../controller-delivery.json').read_text())
assert all(hashlib.sha256(Path(n).read_bytes()).hexdigest()==h for n,h in p['files'].items())
from stockagent.runtime_identity import verify_source_release
verify_source_release(Path('../release.json'),Path('../source'))
print(json.dumps({{'controller_unchanged':True,'source_unchanged':True}}))
PY
'''
            unchanged = json.loads(self.remote(remote_check).stdout)
            result = {'schema_version': 1, 'state': 'accepted', 'observed_at_utc': datetime.now(timezone.utc).isoformat(),
                      **proof, 'schema': self.schema, 'source_proof': source, 'backup_restore': backup,
                      'controller_identity_sha256': delivery['identity_sha256'], 'remote_unchanged': unchanged,
                      'wall_seconds': time.perf_counter()-started,
                      'scope': 'two-node read-only canonical code verification, dependency, crash retry and exact DB restore',
                      'gpu_or_provider_quota_admission': False, 'external_side_effect_exactly_once': False,
                      'production_workflows_migrated': False, 'remote_storage_persistence_verified': False}
            atomic_write_json(self.output / 'acceptance.json', result)
            self.event('accepted', **proof)
            return result
        finally:
            cleanup = {}
            if self.tunnel_started:
                cleanup['tunnel_stop_exit'] = self.run(['systemctl', 'stop', self.unit], accepted=(0, 5)).returncode
            # Remove only this disposable worker's credential. The source,
            # attempt receipts and controller remain available for diagnosis.
            cleanup['remote_private_env_removal_exit'] = self.remote(
                f'rm -f -- {shlex.quote(self.remote_root + "/control.env")}\n', accepted=(0, 255)).returncode
            if self.restore_created:
                self.admin(f'DROP DATABASE {self.restore_db};')
                cleanup['isolated_restore_database_removed'] = True
            atomic_write_json(self.output / 'cleanup.json', cleanup)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--receipt', required=True, type=Path)
    parser.add_argument('--source-root', required=True, type=Path)
    parser.add_argument('--output', required=True, type=Path)
    parser.add_argument('--remote-env', type=Path, default=Path('/etc/stockagent/remote-cold-artifact-ingress.env'))
    parser.add_argument('--control-env', type=Path, default=Path('/etc/stockagent/control-plane.env'))
    parser.add_argument('--forward-port', type=int, default=55433)
    parser.add_argument('--role-lock',type=Path,required=True,
                        help='hash-pinned Linux Conda explicit control-role lock')
    parser.add_argument('--install-remote-miniforge',action='store_true',
                        help='bootstrap a missing remote manager into a fresh isolated owned prefix')
    args = parser.parse_args()
    if not 1024 <= args.forward_port <= 65535:
        parser.error('forward port must be 1024..65535')
    args.receipt = args.receipt.resolve(strict=True)
    args.source_root = args.source_root.resolve(strict=True)
    args.role_lock = args.role_lock.resolve(strict=True)
    try:
        result = Pilot(args).execute()
    except Exception as error:
        atomic_write_json(args.output / 'failure.json', {'state': 'failed', 'error_type': type(error).__name__,
                                                       'reason': str(error)[:512]})
        print(json.dumps({'state': 'failed', 'error_type': type(error).__name__}), flush=True)
        return 1
    print(json.dumps({key: result[key] for key in ('state', 'nodes', 'completed_work', 'attempts', 'wall_seconds')}))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())

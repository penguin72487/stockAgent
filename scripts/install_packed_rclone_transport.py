#!/usr/bin/env python3
"""Enroll immutable SFTP under the existing exact edge transport timer."""
import argparse
import fcntl
import json
import os
from pathlib import Path
import shlex
import subprocess
import sys
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from downloader.artifact_io import atomic_write_json, atomic_write_text
from scripts.configure_artifact_ingress_syncthing import credentials, request
from scripts.manage_packed_transport import observe_remote, requested_payloads
from stockagent.data_sync.immutable_replication import digest
from stockagent.data_sync.rclone_edge_transport import configuration, POLICY


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--apply', action='store_true')
    args = parser.parse_args()
    if os.geteuid() != 0:
        raise ValueError('enroll through the current local root owner')
    if os.readlink('/proc/self/ns/mnt') != os.readlink('/proc/1/ns/mnt'):
        os.execvp('nsenter', ['nsenter', '--mount=/proc/1/ns/mnt', '--', sys.executable, str(Path(__file__).resolve()), *sys.argv[1:]])
    os.chdir(ROOT)
    p = Path('/etc/stockagent/remote-cold-artifact-ingress.env')
    if p.is_symlink() or p.stat().st_mode & 0o077:
        raise ValueError('reuse the private authorized SSH connection')
    env = {}
    for line in p.read_text().splitlines():
        if line.startswith('COLD_ARTIFACT_INGRESS_') and '=' in line:
            k, raw = line.split('=', 1);values = shlex.split(raw, comments=True)
            if len(values) == 1:env[k] = values[0]
    options = SimpleNamespace(ssh_target=env['COLD_ARTIFACT_INGRESS_SSH_TARGET'], ssh_port=int(env['COLD_ARTIFACT_INGRESS_SSH_PORT']),
        identity_file=Path(env['COLD_ARTIFACT_INGRESS_IDENTITY_FILE']), sync_root=Path('/srv/stockagent-packed'),
        state_root=Path('/var/lib/stockagent-packed-transport'), apply=args.apply)
    options.state_root.mkdir(exist_ok=True, mode=0o700)
    with (options.state_root / 'owner.lock').open('a') as owner:
        fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
        subprocess.run(['bash', str(ROOT / 'scripts/mount_packed_d_cold.sh'), '--check'], capture_output=True, check=True, timeout=30)
        private = Path('/etc/stockagent/lakehouse-control.json')
        if private.is_symlink() or private.stat().st_mode & 0o077:
            raise ValueError('use the accepted private lakehouse binary role')
        lake = json.loads(private.read_bytes())
        b,k = credentials()
        identity = request(b,k,'/rest/system/status')['myID']
        devices = request(b,k,'/rest/config/devices')
        peers = [d['deviceID'] for d in devices if d['name'] == 'vastai1T']
        paired = {d['deviceID'] for d in request(b,k,'/rest/config/folders/stockagent-packed')['devices']}
        if len(peers) != 1 or paired != {identity,peers[0]}:
            raise ValueError('preserve the sole enrolled index-only edge')
        demand = observe_remote(options)
        allowed = requested_payloads(options.sync_root,demand)
        binary = Path(lake['binaries']) / 'bin/rclone'
        config = Path(lake['state_root']) / 'rclone-vast.private.conf'
        if not config.is_file() or config.stat().st_mode & 0o077:
            raise ValueError('complete the real fixed-version SFTP benchmark first')
        policy = {'schema_version':1,'backend':'rclone-sftp','producer_device_id':identity,'receiver_device_id':peers[0],
            'rclone':str(binary),'rclone_sha256':digest(binary),'rclone_config':str(config),'remote_name':'vast-lake',
            'remote_root':'/srv/stockagent-packed','remote_state_root':'/var/lib/stockagent-packed-edge',
            'remote_repo_root':env['COLD_ARTIFACT_INGRESS_REMOTE_REPO_ROOT'],
            'workers':2,'maximum_files':64,'maximum_bytes':1024**3,'maximum_single_object_bytes':8*1024**3,'reserve_bytes':8*1024**3}
        result = {'applied':False,'existing_owner':'stockagent-packed-transport','requested_objects':len(allowed),
                  'source_payload_transport':'rclone-sftp','source_index_transport':'paired Syncthing','workers':policy['workers'],
                  'existing_training_or_data_deleted':False,'active_owner_restarted':False}
        if args.apply:
            previous = configuration()
            if previous and any(previous[x] != policy[x] for x in ('producer_device_id','receiver_device_id','remote_root','remote_name')):
                raise ValueError('preserve an existing foreign immutable transport enrollment')
            atomic_write_json(POLICY,policy,durable=True);POLICY.chmod(0o600)
            drop = Path('/etc/systemd/system/stockagent-packed-transport.service.d/immutable-rclone.conf')
            drop.parent.mkdir(exist_ok=True)
            body = (ROOT / 'deploy/systemd/stockagent-packed-rclone-transport.conf.in').read_text()
            if drop.exists() and drop.read_text() != body:
                raise ValueError('preserve an edited existing transport resource override')
            atomic_write_text(drop,body);drop.chmod(0o644)
            subprocess.run(['systemd-analyze','verify','--man=no','/etc/systemd/system/stockagent-packed-transport.service'],capture_output=True,check=True,timeout=30)
            subprocess.run(['systemctl','daemon-reload'],check=True)
            subprocess.run(['systemctl','enable','--now','stockagent-packed-transport.timer'],check=True)
            result['applied'] = True
            atomic_write_json(options.state_root/'rclone-enrollment.json',result)
        print(json.dumps(result))


if __name__ == '__main__':
    main()

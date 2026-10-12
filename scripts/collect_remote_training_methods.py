"""Read only run/config provenance from an explicitly selected training node.

No data/checkpoint transfer, environment mutation, model load or GPU job occurs.
The resulting small method inventory is not a remote training acceptance proof.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import shlex
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from downloader.artifact_io import atomic_write_json

REMOTE_READ = '''import hashlib, json, os, socket
from pathlib import Path
root=Path.cwd()
rows, errors=[], []
for name in ("markets", "ablations", "benchmarks"):
    for directory, _, files in os.walk(root/"artifacts"/name, followlinks=False):
        if "run_manifest.json" not in files: continue
        path=Path(directory)/"run_manifest.json"
        try:
            before=path.stat()
            content=path.read_bytes()
            raw=json.loads(content)
            after=path.stat()
            if (before.st_ino,before.st_size,before.st_mtime_ns,before.st_ctime_ns)!=(after.st_ino,after.st_size,after.st_mtime_ns,after.st_ctime_ns):
                raise RuntimeError("manifest changed during inventory")
            cfg=raw.get("configuration",raw.get("resolved_config",{}))
            rows.append({"path":str(path),"sha256":hashlib.sha256(content).hexdigest(),
                "configuration":{k:cfg.get(k,{}) for k in ("data","trading","walk_forward")},
                **{k:raw.get(k) for k in ("dataset_fingerprint","selected_fold_ids","source_releases","execution_clock","decision_clock")}})
        except Exception as exc: errors.append({"path":str(path),"error":str(exc)})
print(json.dumps({"node":"vastai1T", "hostname":socket.gethostname(),
    "scope":"current run manifests only; no archive payload opened; no data/checkpoint copied",
    "training_manifests":rows,"errors":errors},ensure_ascii=False))
'''


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--host', required=True)
    parser.add_argument('--port', type=int, required=True)
    parser.add_argument('--key', type=Path, required=True)
    parser.add_argument('--remote-repo', default='/root/stockAgent')
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    command = f'cd {shlex.quote(args.remote_repo)} && source scripts/runtime_env.sh && run_fintech_python -'
    response = subprocess.run(['ssh', '-i', str(args.key), '-p', str(args.port),
        '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=10', '--', args.host, command],
        input=REMOTE_READ, text=True, capture_output=True, check=True, timeout=60)
    payload = json.loads(response.stdout.strip().splitlines()[-1])
    atomic_write_json(args.output, payload)
    print(json.dumps({'node': payload['node'], 'training_manifests':len(payload['training_manifests']),
                      'errors':len(payload['errors']), 'output':str(args.output)}))


if __name__ == '__main__':
    main()

#!/usr/bin/env python3
"""Read-only full cache inventory, provenance and current consumer evidence."""
from __future__ import annotations
import argparse
from datetime import UTC, datetime
import json
from pathlib import Path
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts.ingest_remote_cold_artifacts import _ssh_base, _validate_ssh_target
from stockagent.data_sync.desync_snapshots import atomic_write_json


def observe(args):
    code = "import sys,types\n"
    for name, relative in (
        ("stockagent.data_sync.node_roles", "stockagent/data_sync/node_roles.py"),
        ("stockagent.data_sync.artifact_consumers", "stockagent/data_sync/artifact_consumers.py"),
        ("stockagent.data_sync.legacy_artifact_archive", "stockagent/data_sync/legacy_artifact_archive.py"),
        ("stockagent.data_sync.remote_legacy_return", "stockagent/data_sync/remote_legacy_return.py"),
        ("scripts.deduplicate_inactive_panel_caches", "scripts/deduplicate_inactive_panel_caches.py"),
    ):
        code += f"m=types.ModuleType({name!r});m.__file__={'/root/stockAgent/' + relative!r};sys.modules[{name!r}]=m\n"
        code += f"exec({(ROOT / relative).read_text()!r},m.__dict__)\n"
    code += r'''
import json,os,time,zipfile
from pathlib import Path
from stockagent.data_sync.materialized_cache import process_references_many
from stockagent.data_sync.remote_legacy_return import artifact_process_references,active_configuration_references,metadata_tree
from scripts.deduplicate_inactive_panel_caches import current_service_blockers
from stockagent.data_sync.node_roles import training_only_node
from stockagent.remote_build import observe_node
repo=Path('/root/stockAgent');cache=repo/'artifacts/cache'
paths=sorted(p for p in cache.iterdir() if p.is_dir() and not p.is_symlink())
direct=process_references_many(paths);rows=[];inodes={}
for path in paths:
 observed=metadata_tree(path);large=[];metadata=[]
 for row in observed['rows']:
  if row['kind']!='file': continue
  p=path/row['path'];s=p.lstat();inodes.setdefault((s.st_dev,s.st_ino),s.st_blocks*512)
  if p.name in ('meta.json','panel_cache.meta.json','manifest.json') or p.name.endswith('_manifest.json'):
   if s.st_size<1024*1024:
    try:
     v=json.loads(p.read_bytes())
     if isinstance(v,dict):
      metadata.append({'path':row['path'],'fields':sorted(v),'source_hash':v.get('source_hash'),'backend_key':v.get('backend_key'),'version':v.get('version'),'generation':v.get('generation'),'arrays':v.get('arrays')})
    except (ValueError,OSError): pass
  if s.st_size>1024*1024*1024:
   item={'path':row['path'],'bytes':s.st_size,'nlink':s.st_nlink}
   if p.suffix=='.npz':
    try:
     with zipfile.ZipFile(p) as z:
      item['npz_members']=[{'name':m.filename,'logical_bytes':m.file_size,'encoded_bytes':m.compress_size,'compression':m.compress_type} for m in z.infolist()]
    except (ValueError,OSError,zipfile.BadZipFile): item['format_error']=True
   large.append(item)
 try: services=current_service_blockers([path],repo);service_error=None
 except Exception as error: services=[];service_error=type(error).__name__
 rows.append({'name':path.name,'path':str(path),'files':observed['files'],'logical_bytes':observed['logical_bytes'],'allocated_unique_file_bytes':observed['allocated_unique_file_bytes'],'reclaimable_allocated_file_bytes':observed['reclaimable_allocated_file_bytes'],'fingerprint':observed['fingerprint'],'newest_mtime_ns':max((r['signature'][3] for r in observed['rows'] if r['kind']=='file'),default=0),'process_references':artifact_process_references(path,repo/'artifacts')+active_configuration_references(path,repo),'service_references':services,'service_error':service_error,'metadata':metadata,'large_files':large,'rows':observed['rows']})
fs=os.statvfs(repo)
print(json.dumps({'schema_version':1,'observed_at_epoch':time.time(),'available_bytes':fs.f_bavail*fs.f_frsize,'allocated_unique_cache_file_bytes':sum(inodes.values()),'direct_process_references':direct,'caches':rows,'training_only_role_verified':training_only_node(),'node_profile':observe_node(repo,repo)},ensure_ascii=False),flush=True)
'''
    command = [*_ssh_base(args.identity_file, args.ssh_port), _validate_ssh_target(args.ssh_target),
               "cd /root/stockAgent && source scripts/runtime_env.sh && run_fintech_python -"]
    result = subprocess.run(command, input=code, text=True, capture_output=True, timeout=1800)
    if result.returncode:
        atomic_write_json(args.output.with_suffix(".error.json"),
                          {"state": "inventory_failed", "stderr": result.stderr,
                           "returncode": result.returncode, "observed_at_epoch": time.time()})
        raise RuntimeError("private cache inventory failed; failure receipt retained")
    value = json.loads(result.stdout.splitlines()[-1])
    return value


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--output", type=Path, required=True)
    p.add_argument("--ssh-target", default="root@114.32.64.6")
    p.add_argument("--ssh-port", type=int, default=40032)
    p.add_argument("--identity-file", type=Path, default=Path("/root/.ssh/stockagent_vastai1t_ed25519"))
    args = p.parse_args()
    value = observe(args)
    if args.output.exists():
        old = json.loads(args.output.read_text())
        prior = args.output.with_name(args.output.stem + "-" + str(int(old["observed_at_epoch"])) + ".json")
        if not prior.exists():
            atomic_write_json(prior, old)
    atomic_write_json(args.output, value)
    print(json.dumps({"state": "current_inventory_captured", "receipt": str(args.output),
                      "cache_roots": len(value["caches"]), "available_bytes": value["available_bytes"],
                      "allocated_unique_cache_file_bytes": value["allocated_unique_cache_file_bytes"],
                      "protected_roots": [r["name"] for r in value["caches"] if r["service_error"]
                                          or r["process_references"] or r["service_references"]]}), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Discard the user's exact unwanted Vast testing outputs via private SSH.

Keeps the conflicted remote checkout and frozen training code unchanged. It
uses the shared process/configuration/inode gates and records each dry run.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts.return_remote_legacy_archives import private_control_prefix
from stockagent.data_sync.artifact_discard import DEFAULT_POLICY, load_policy
from stockagent.data_sync.desync_snapshots import atomic_write_json
from stockagent.remote_ssh import ssh_base, validate_ssh_target


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--policy", type=Path, default=DEFAULT_POLICY)
    p.add_argument("--receipt", type=Path, required=True)
    p.add_argument("--apply", action="store_true")
    p.add_argument("--only-relative", action="append", default=[], help="retry exact targets already in the disposition")
    p.add_argument("--ssh-target", default="root@114.32.64.6")
    p.add_argument("--ssh-port", type=int, default=40032)
    p.add_argument("--identity-file", type=Path, default=Path("/root/.ssh/stockagent_vastai1t_ed25519"))
    args = p.parse_args()
    if args.receipt.exists():
        p.error("retain a fresh receipt")
    policy = load_policy(args.policy)
    if args.only_relative and (len(set(args.only_relative)) != len(args.only_relative)
            or not set(args.only_relative) <= {r["relative"] for r in policy["targets"]}):
        p.error("retry selection must name unique targets already reviewed")
    code = private_control_prefix()
    module = "stockagent.data_sync.artifact_discard"
    code += f"\nm=types.ModuleType({module!r});m.__file__='/root/stockAgent/stockagent/data_sync/artifact_discard.py';sys.modules[{module!r}]=m\n"
    code += f"exec({(ROOT / 'stockagent/data_sync/artifact_discard.py').read_text()!r},m.__dict__)\n"
    code += "from stockagent.data_sync.artifact_discard import load_policy, discard_target, restore_unchanged_discard_quarantine\n"
    code += f"policy=json.loads({json.dumps(policy, ensure_ascii=True)!r}); apply={args.apply!r}\n"
    code += f"selection={args.only_relative!r}\n"
    code += '''
from pathlib import Path
import fcntl, os, time
os.umask(0o077)
state=real(Path('/var/lib/stockagent-testing-artifact-discard-20261007'))
state.mkdir(mode=0o700,parents=True,exist_ok=True)
atomic_write_json(state/'policy.json',policy)
policy=load_policy(state/'policy.json')
targets=[t for t in policy['targets'] if not selection or t['relative'] in selection]
options=dict(repo_root=Path('/root/stockAgent'),engineering_root=Path('/root'),state_root=state/'transactions',
             materialized_root=Path('/srv/stockagent-packed-materialized'),cold_root=Path('/srv/stockagent-packed'))
with real(state/'owner.lock').open('a') as owner:
 fcntl.flock(owner,fcntl.LOCK_EX|fcntl.LOCK_NB)
 if apply:
  for t in targets:
   restore_unchanged_discard_quarantine(policy,t,**options)
 initial=[discard_target(policy,t,**options) for t in targets]
 atomic_write_json(state/('initial-dry-'+str(time.time_ns())+'.json'),{'policy_identity_sha256':policy['identity_sha256'],'targets':initial})
 print(json.dumps({'phase':'initial-dry-complete','targets':len(initial),'protected':sum(bool(x.get('blockers')) for x in initial)}),flush=True)
 results=[]
 for t in targets:
  try:
   plan=discard_target(policy,t,**options)
   result=discard_target(policy,t,**options,apply=True,expected_plan_identity=plan.get('identity_sha256')) if apply else plan
  except Exception as error:
   result={'state':'protected-or-changed','target':t,'deleted':False,'error':str(error),'reclaimed_allocated_bytes':0}
  results.append(result)
  atomic_write_json(state/'progress.json',{'policy_identity_sha256':policy['identity_sha256'],'results':results,'updated_at_epoch':time.time()})
  print(json.dumps({'phase':'target-complete','target':t['relative'],'state':result['state'],'bytes':result.get('reclaimed_allocated_bytes',0)}),flush=True)
 result={'policy_identity_sha256':policy['identity_sha256'],'results':results,'targets':len(results),
         'reclaimed_allocated_bytes':sum(x.get('reclaimed_allocated_bytes',0) for x in results),
         'deleted_targets':sum(x.get('deleted',False) for x in results),'observed_at_epoch':time.time(),
         'failed_or_protected_targets':sum(x['state']=='protected-or-changed' for x in results),'cold_deletion_claimed':False}
 atomic_write_json(state/'result.json',result)
 print(json.dumps(result),flush=True)
'''
    command = "cd /root/stockAgent && source scripts/runtime_env.sh && run_fintech_python -"
    log = args.receipt.with_suffix(".log")
    log.parent.mkdir(parents=True, exist_ok=True)
    with log.open("x") as output:
        result = subprocess.run([*ssh_base(args.identity_file, args.ssh_port),
                                 validate_ssh_target(args.ssh_target), command], input=code,
                                text=True, stdout=output, stderr=subprocess.PIPE, timeout=1800)
    if result.returncode:
        raise RuntimeError("fixed private testing discard failed: " + result.stderr[-1500:])
    final = json.loads(log.read_text().splitlines()[-1])
    atomic_write_json(args.receipt, final)
    print(json.dumps({k: v for k, v in final.items() if k != "results"}))


if __name__ == "__main__":
    main()

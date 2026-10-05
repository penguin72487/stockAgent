#!/usr/bin/env python3
"""One-shot compressed preservation transport, separate from cold acceptance.

Stream directly to penguin D; do not allocate full archives on Vast, expose raw
artifacts to Syncthing, restart workers, publish heads or delete source data.
"""
from __future__ import annotations

import argparse
import ast
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import socket
import subprocess
import sys
import time
import uuid

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts.ingest_remote_cold_artifacts import _ssh_base, _validate_ssh_target
from stockagent.data_sync.cold_primary import _check_d_primary_mount
from stockagent.data_sync.desync_snapshots import SnapshotError
from stockagent.data_sync.bulk_archive import load_policy, atomic_write_json
from stockagent.data_sync.windows_cold_io import BinaryWriter, hash_file, metadata_path

MAGIC = b"STOCKAGENT-BULK-ZSTD-V1\n"
SCOPES = ("markets", "ablations", "cache")
DEFAULT_SCOPES = ("markets", "ablations")
D_LANDING = Path("/mnt/d/stockagent-cold-primary/remote-artifact-incoming")
TRANSPORT_LOCK_ROOT = Path("/run/lock")


def producer_code(scope: str, threads: int, cache_inventory=None) -> str:
    if scope not in SCOPES or not 1 <= threads <= 16:
        raise SnapshotError("fixed artifact scopes and bounded compression threads required")
    if scope == "cache":
        return cache_producer_code(threads, cache_inventory)
    return '''
import os,subprocess,sys
from pathlib import Path
scope=SCOPE
root=Path('/root/stockAgent/artifacts')
path=root/scope
if path.is_symlink() or path.resolve()!=path or not path.is_dir():
 raise RuntimeError('artifact scope is missing or redirected')
sys.stdout.buffer.write(MAGIC)
sys.stdout.buffer.flush()
tar=subprocess.Popen(['/usr/bin/tar','--format=pax','--pax-option=delete=atime,delete=ctime','--one-file-system','--numeric-owner','-cf','-',scope],cwd=root,stdout=subprocess.PIPE,stderr=sys.stderr)
zstd=subprocess.Popen(['/usr/bin/zstd','-1','-T'+str(THREADS),'--check','-q','-c'],stdin=tar.stdout,stdout=sys.stdout.buffer,stderr=sys.stderr)
tar.stdout.close()
compression_exit=zstd.wait()
tar_exit=tar.wait()
if tar_exit or compression_exit:
 sys.stderr.write('STOCKAGENT_BULK_STATUS='+str({'scope':scope,'tar_exit':tar_exit,'compression_exit':compression_exit})+'\\n')
 sys.exit(tar_exit or compression_exit)
'''.replace("SCOPE", repr(scope)).replace("MAGIC", repr(MAGIC)).replace("THREADS", str(threads))


def cache_producer_code(threads, inventory):
    if (not isinstance(inventory, dict) or inventory.get("schema_version") != 1
        or not 0 <= time.time() - inventory.get("observed_at_epoch", 0) <= 7200):
        raise SnapshotError("fresh complete cache inventory required for preservation")
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
    code += f"inventory={inventory!r}\nthreads={threads!r}\nmagic={MAGIC!r}\n"
    code += '''
import fcntl,os,subprocess,time
from pathlib import Path
from contextlib import ExitStack
from stockagent.data_sync.remote_legacy_return import metadata_tree,real,artifact_process_references,active_configuration_references
from scripts.deduplicate_inactive_panel_caches import current_service_blockers
repo=Path('/root/stockAgent')
root=repo/'artifacts'
selected=[]
observations={}
with ExitStack() as locks:
 for row in inventory['caches']:
  if row['service_error'] is not None: raise RuntimeError('cache consumer inventory incomplete')
  if row['service_references'] or row['process_references']: continue
  name=row['name']
  if inventory.get('requested_roots') and name not in inventory['requested_roots']: continue
  if name in inventory.get('explicit_protected_roots',[]): continue
  if Path(name).name!=name or name in ('.','..'): raise RuntimeError('unsafe cache root')
  path=real(root/'cache'/name)
  refs=artifact_process_references(path,root)+active_configuration_references(path,repo)+current_service_blockers([path],repo)
  if refs: continue
  observed=metadata_tree(path)
  if observed['fingerprint']!=row['fingerprint']: raise RuntimeError('cache changed; recapture inventory')
  if any(r['kind']=='unsupported' or r['cross_filesystem'] for r in observed['rows']): continue
  if any(time.time_ns()-r['signature'][3]<12*3600*1000000000 for r in observed['rows'] if r['kind']=='file'): continue
  for r in observed['rows']:
   if r['kind']=='file' and r['path'].endswith('panel_cache_v2/.write.lock'):
    handle=locks.enter_context((path/r['path']).open('r+b'))
    fcntl.flock(handle,fcntl.LOCK_EX|fcntl.LOCK_NB)
  selected.append('cache/'+name)
  observations[str(path)]=observed
 if not selected: raise RuntimeError('no stable inactive cache roots')
 sys.stdout.buffer.write(magic);sys.stdout.buffer.flush()
 tar=subprocess.Popen(['/usr/bin/tar','--format=pax','--pax-option=delete=atime,delete=ctime','--one-file-system','--numeric-owner','-cf','-',*selected],cwd=root,stdout=subprocess.PIPE,stderr=sys.stderr)
 zstd=subprocess.Popen(['/usr/bin/zstd','-1','-T'+str(threads),'--check','-q','-c'],stdin=tar.stdout,stdout=sys.stdout.buffer,stderr=sys.stderr)
 tar.stdout.close()
 compression_exit=zstd.wait();tar_exit=tar.wait()
 if any(metadata_tree(Path(path))!=before for path,before in observations.items()):
  sys.stderr.write('CACHE_SOURCE_MUTATED_AFTER_STREAM\\n');sys.exit(3)
 sys.stderr.write('CACHE_PRESERVATION_SELECTED='+str(selected)+'\\n')
 sys.exit(tar_exit or compression_exit)
'''
    return code


def resume_producer(body, offset, prefix_sha256):
    """Regenerate/hash the exact compressed prefix remotely BEFORE any append.

    Compression/tar input changes are not guessed around: a mismatch emits no
    stream marker, retains the old partial unchanged, and requires a fresh batch.
    """
    if not offset:
        return body
    if offset < 0 or len(prefix_sha256) != 64:
        raise SnapshotError('Exact retained prefix required for resume')
    helper = f'''
import hashlib
def forward_resume(stream,tar,zstd):
 try:
  left={offset!r};value=hashlib.sha256()
  while left:
   block=stream.read(min(left,4*1024*1024))
   if not block: raise RuntimeError('compressed source is shorter than retained prefix')
   value.update(block);left-=len(block)
  if value.hexdigest()!={prefix_sha256!r}:
   raise RuntimeError('compressed prefix differs; retained partial is untouched')
  sys.stdout.buffer.write({MAGIC!r});sys.stdout.buffer.flush()
  while block:=stream.read(4*1024*1024): sys.stdout.buffer.write(block)
  sys.stdout.buffer.flush()
 except BaseException:
  zstd.terminate();tar.terminate();zstd.wait();tar.wait();raise
'''
    body = body.replace('sys.stdout.buffer.write(MAGIC)\nsys.stdout.buffer.flush()\n'.replace('MAGIC', repr(MAGIC)), '')
    body = body.replace(' sys.stdout.buffer.write(magic);sys.stdout.buffer.flush()\n', '')
    body = body.replace('stdout=sys.stdout.buffer,stderr=', 'stdout=subprocess.PIPE,stderr=')
    lines = body.splitlines()
    for number, line in enumerate(lines):
        if line.strip() == 'tar.stdout.close()':
            indent = line[:len(line) - len(line.lstrip())]
            lines.insert(number + 1, indent + 'forward_resume(zstd.stdout,tar,zstd)')
            break
    else:
        raise SnapshotError('Resume producer pipeline is missing')
    return helper + '\n' + '\n'.join(lines) + '\n'


def retained_prefix_sha(payload, receipt, offset, scope):
    """A retained SHA is an expectation, NEVER a substitute for verification.

    The remote producer hashes its regenerated prefix AND the native writer
    hashes the entire current stored prefix before readiness/append. Reusing
    this expectation avoids a third redundant local whole-prefix read.
    """
    if receipt.exists():
        prior=json.loads(metadata_path(receipt).read_text())
        value=prior.get('resumed_prefix_sha256')
        if (prior.get('authority_node_id')=='penguin' and prior.get('origin_node_id')=='vastai1T'
            and prior.get('scope')==scope and prior.get('resumed_prefix_bytes')==offset
            and isinstance(value,str) and re.fullmatch('[0-9a-f]{64}',value)):
            return value
    print(json.dumps({'scope':scope,'state':'hashing_retained_prefix_expectation','bytes':offset}),flush=True)
    return hash_file(payload)


def receive(args, scope: str, directory: Path) -> dict:
    payload = directory / (scope + ".tar.zst.partial")
    receipt = directory / (scope + ".receipt.json")
    errors = directory / (scope + ".transport-" + str(time.time_ns()) + ".log")
    cache_inventory = json.loads(args.cache_inventory.read_text()) if scope == "cache" else None
    if cache_inventory:
        requested = getattr(args, "include_cache_root", [])
        for name in requested:
            if Path(name).name != name or name in {".", ".."}:
                raise SnapshotError("unsafe selected cache namespace")
        if requested:
            known = {row["name"] for row in cache_inventory["caches"]}
            if not set(requested) <= known:
                raise SnapshotError("selected cache root missing from the complete inventory")
            cache_inventory["requested_roots"] = list(dict.fromkeys(requested))
        for name in args.exclude_cache_root:
            if Path(name).name != name or name in {".", ".."}:
                raise SnapshotError("unsafe protected cache namespace")
        cache_inventory["explicit_protected_roots"] = list(dict.fromkeys(
            load_policy()["protected_cache_roots"] + args.exclude_cache_root))
    offset = payload.lstat().st_size if args.resume_batch and payload.exists() else 0
    if args.resume_batch and not offset:
        raise SnapshotError('Resume requires a retained nonempty exact partial')
    prefix_sha256 = retained_prefix_sha(payload,receipt,offset,scope) if offset else None
    body = resume_producer(producer_code(scope, args.threads, cache_inventory), offset, prefix_sha256)
    command = [*_ssh_base(args.identity_file, args.ssh_port), _validate_ssh_target(args.ssh_target),
               "cd /root/stockAgent && source scripts/runtime_env.sh && run_fintech_python -"]
    digest = hashlib.sha256()
    total = offset
    started = time.monotonic()
    last_status = 0.0
    control = {"schema_version": 1, "contract": "one_shot_compressed_artifact_transport_v1",
               "scope": scope, "origin_node_id": "vastai1T", "authority_node_id": "penguin",
               "state": "verifying_compressed_prefix" if offset else "receiving", "started_at_epoch": time.time(),
               "compression": "zstd-1", "compression_threads": args.threads,
               "payload": str(payload), "transport_only": True,
               "cold_accepted": False, "source_deletion_authorized": False,
               "producer_code_sha256": hashlib.sha256(body.encode()).hexdigest()}
    control.update(io='windows-filestream-v1', resumed_prefix_bytes=offset,
                   resumed_prefix_sha256=prefix_sha256,received_bytes=offset,
                   transferred_this_attempt_bytes=0)
    if receipt.exists():
        previous = directory / (scope + '.receipt-history-' + str(time.time_ns()) + '.json')
        atomic_write_json(previous, json.loads(receipt.read_text()))
    if cache_inventory:
        control["approved_cache_roots"] = ["cache/" + row["name"] for row in cache_inventory["caches"]
                                           if row["service_error"] is None and not row["service_references"] and not row["process_references"]
                                           and row["name"] not in cache_inventory["explicit_protected_roots"]
                                           and (not cache_inventory.get("requested_roots")
                                                or row["name"] in cache_inventory["requested_roots"])]
    atomic_write_json(receipt, control)
    process, target, durable = None, None, None
    try:
      with errors.open("xb") as error_log:
        process = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=error_log)
        process.stdin.write(body.encode())
        process.stdin.close()
        # A host may put a harmless SSH/MOTD banner on stdout. Bounded marker
        # framing ensures it can never become archive payload.
        banner_bytes = 0
        while True:
            line = process.stdout.readline(4096)
            banner_bytes += len(line)
            if line == MAGIC:
                break
            if not line or banner_bytes > 65536:
                process.wait(timeout=30)
                raise SnapshotError("remote compressed stream marker missing")
        # Do not open/append the retained file until the remote prefix matched.
        target = BinaryWriter(payload, offset=offset, prefix_sha256=prefix_sha256,
                              reserve_bytes=args.reserve_bytes)
        append_started=time.monotonic()
        control.update(state='receiving',append_started_at_epoch=time.time())
        while chunk := process.stdout.read(4 * 1024 * 1024):
            target.write(chunk)
            digest.update(chunk)
            total += len(chunk)
            now = time.monotonic()
            if now - last_status > 15:
                control.update(received_bytes=total, elapsed_seconds=now-started,
                               transferred_this_attempt_bytes=total-offset,
                               average_bytes_per_second=(total-offset)/max(now-append_started, 0.001), observed_at_epoch=time.time())
                atomic_write_json(receipt, control)
                print(json.dumps({k: control[k] for k in ("scope", "state", "received_bytes", "elapsed_seconds", "average_bytes_per_second")}), flush=True)
                last_status = now
        code = process.wait(timeout=30)
        durable = target.finish()
        if not offset and durable['sha256'] != digest.hexdigest():
            raise SnapshotError('Native Windows durable bytes differ from authenticated stream')
    except BaseException as error:
        if process is not None and process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=30)
            except subprocess.TimeoutExpired:
                process.kill();process.wait(timeout=30)
        control.update(state='transport_failed_partial_retained', error=str(error),
                       received_bytes=payload.stat().st_size if payload.exists() else 0,
                       completed_at_epoch=time.time(), producer_exit_code=-1)
        atomic_write_json(receipt, control)
        print(json.dumps({k: control[k] for k in ('scope','state','received_bytes','error')}), flush=True)
        return control
    finally:
        if target is not None:
            target.close()
        if process is not None:
            if process.stdout: process.stdout.close()
    final = directory / (scope + ".tar.zst")
    # Exit 1 can be a changing source; retain the decodable archive as evidence
    # but never upgrade it to cold/source retirement proof.
    if code == 0:
        payload.rename(final)
        control.update(state="compressed_transport_received", payload=str(final))
    else:
        control.update(state="transport_incomplete_source_preserved", payload=str(payload))
    control.update(received_bytes=total, compressed_sha256=durable['sha256'], producer_exit_code=code,
                   elapsed_seconds=time.monotonic()-started, completed_at_epoch=time.time())
    if cache_inventory and code == 0:
        selected_lines = [line.partition("=")[2] for line in errors.read_text().splitlines()
                          if line.startswith("CACHE_PRESERVATION_SELECTED=")]
        if len(selected_lines) == 1:
            selected = ast.literal_eval(selected_lines[0])
            if not isinstance(selected, list) or not set(selected) <= set(control["approved_cache_roots"]):
                raise SnapshotError("private producer selection escapes the approved cache cohort")
            control["selected_cache_roots"] = selected
            control["capture_source_fingerprints"] = {
                "cache/" + r["name"]: r["fingerprint"] for r in cache_inventory["caches"]
                if "cache/" + r["name"] in selected}
    atomic_write_json(receipt, control)
    print(json.dumps({k: control[k] for k in ("scope", "state", "received_bytes", "compressed_sha256", "producer_exit_code")}), flush=True)
    return control


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--ssh-target", default="root@114.32.64.6")
    p.add_argument("--ssh-port", type=int, default=40032)
    p.add_argument("--identity-file", type=Path, default=Path("/root/.ssh/stockagent_vastai1t_ed25519"))
    p.add_argument("--scope", action="append", choices=SCOPES)
    p.add_argument("--cache-inventory", type=Path)
    p.add_argument("--exclude-cache-root", action="append", default=[])
    p.add_argument("--include-cache-root", action="append", default=[],
                   help="Preserve only these exact inventoried inactive caches")
    p.add_argument("--threads", type=int, default=8)
    p.add_argument("--reserve-bytes", type=int, default=64 * 1024**3)
    p.add_argument("--landing", type=Path, default=D_LANDING)
    p.add_argument('--resume-batch', type=Path,
                   help='Exact retained batch; compressed prefix SHA must match before append')
    p.add_argument("--apply", action="store_true")
    args = p.parse_args()
    if socket.gethostname() != "penguin" or not 1 <= args.threads <= 16 or args.reserve_bytes < 32 * 1024**3:
        p.error("penguin only, bounded threads and at least 32 GiB reserve")
    if args.landing != D_LANDING or args.landing.is_symlink() or args.landing.resolve(strict=False) != args.landing:
        p.error("fixed, non-redirected D incoming path required")
    _check_d_primary_mount(Path("/srv/stockagent-packed"))
    scopes = list(dict.fromkeys(args.scope or DEFAULT_SCOPES))
    if "cache" in scopes and (not args.cache_inventory or not args.cache_inventory.is_file()):
        p.error("cache preservation requires a fresh explicit inventory")
    if args.include_cache_root and scopes != ["cache"]:
        p.error("selected cache roots require a cache-only sealed batch")
    if not args.apply:
        print(json.dumps({"state": "read_only_transport_plan", "scopes": scopes, "landing": str(args.landing),
                          "remote_full_archives_created": False, "cold_publish": False,
                          "source_cleanup": False, "threads_per_scope": args.threads}), flush=True)
        return 0
    os.umask(0o077)
    lock_name = ('stockagent-vast-bulk-' + scopes[0] + '-transport.lock'
                 if len(scopes) == 1 else 'stockagent-vast-bulk-return-transport.lock')
    with (TRANSPORT_LOCK_ROOT / lock_name).open("a+b") as owner:
        fcntl.flock(owner.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S")
        directory = args.resume_batch or args.landing / (stamp + "-" + uuid.uuid4().hex[:12])
        if args.resume_batch:
            if directory.parent != args.landing or directory.resolve() != directory:
                p.error('Resume requires exact existing non-redirected D batch')
            old = json.loads((directory / 'intent.json').read_text())
            if old.get('scopes') != scopes or old.get('authority_node_id') != 'penguin' or old.get('origin_node_id') != 'vastai1T':
                p.error('Resume batch authority/origin/scopes differ')
            atomic_write_json(directory / ('intent-history-' + str(time.time_ns()) + '.json'), old)
        else:
            directory.mkdir(parents=True, mode=0o700)
        intent = {"state": "bulk_transport_running", "scopes": scopes, "directory": str(directory),
                  "origin_node_id": "vastai1T", "authority_node_id": "penguin", "cold_accepted": False,
                  "source_cleanup": False, "created_at_epoch": time.time()}
        atomic_write_json(directory / "intent.json", intent)
        with ThreadPoolExecutor(max_workers=2) as workers:
            results = list(workers.map(lambda scope: receive(args, scope, directory), scopes))
        intent.update(state="bulk_transport_complete" if all(row["producer_exit_code"] == 0 for row in results) else "bulk_transport_requires_reconciliation",
                      receipts=[str(directory / (scope + ".receipt.json")) for scope in scopes],
                      received_bytes=sum(row["received_bytes"] for row in results), completed_at_epoch=time.time())
        atomic_write_json(directory / "intent.json", intent)
        print(json.dumps(intent), flush=True)
        return 0 if intent["state"] == "bulk_transport_complete" else 2


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Measure the full inactive-cache SSH -> D -> original-decode workflow.

Reuses the preservation producer and native D byte I/O. Only fresh private
benchmark payloads are created; no remote archive, source unlink or publication.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import statistics
import subprocess
import sys
import tempfile
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts.receive_vast_bulk_archives import MAGIC, cache_producer_code
from scripts.ingest_remote_cold_artifacts import _ssh_base, _validate_ssh_target
from stockagent.data_sync.bulk_archive import native_guard, index_zstd, signature, atomic_write_json, load_policy
from stockagent.data_sync.desync_snapshots import SnapshotError, _fsync_directory
from stockagent.data_sync.windows_cold_io import BinaryWriter, metadata_path

PROOF_MARKER = "STOCKAGENT_TRANSPORT_BENCHMARK_PROOF="


def measured_producer(threads, inventory):
    code = cache_producer_code(threads, inventory)
    end = " sys.exit(tar_exit or compression_exit)"
    if code.count(end) != 1:
        raise SnapshotError("canonical producer terminal boundary changed")
    proof = '''
 if not tar_exit and not compression_exit:
  import hashlib,resource,json,stat
  original=[]
  for name in selected:
   path=root/name
   for local in [path,*[path/r['path'] for r in observations[str(path)]['rows']]]:
    info=local.lstat()
    value={'path':local.relative_to(root).as_posix(),'kind':'directory' if stat.S_ISDIR(info.st_mode) else 'file',
           'mode':stat.S_IMODE(info.st_mode),'mtime_ns':info.st_mtime_ns,'uid':info.st_uid,'gid':info.st_gid}
    if value['kind']=='file':
     digest=hashlib.sha256()
     with local.open('rb') as reader:
      while block:=reader.read(4*1024*1024): digest.update(block)
     value.update(size=info.st_size,sha256=digest.hexdigest())
    original.append(value)
  if any(metadata_tree(Path(path))!=before for path,before in observations.items()):
   raise RuntimeError('source changed during independent original hashing')
  usage=resource.getrusage(resource.RUSAGE_CHILDREN)
  record={'source_rows':sorted(original,key=lambda r:r['path']),
          'remote_complete_seconds':time.monotonic()-benchmark_started,
          'remote_cpu_seconds':time.process_time()+usage.ru_utime+usage.ru_stime,
          'remote_max_child_rss_bytes':usage.ru_maxrss*1024,
          'remote_self_peak_rss_bytes':resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*1024}
  sys.stderr.write(PROOF_MARKER+json.dumps(record)+'\\n');sys.stderr.flush()
 sys.exit(tar_exit or compression_exit)
'''.replace("PROOF_MARKER", repr(PROOF_MARKER))
    return "import time\nbenchmark_started=time.monotonic()\n" + code.replace(end, proof)


def compare_originals(index, proof):
    actual = [{k: v for k, v in row.items() if k != "hardlink_to"} for row in index["rows"]]
    if actual != proof["source_rows"]:
        raise SnapshotError("independent D decoded originals differ from remote whole-file SHA/metadata")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cache-inventory", required=True, type=Path)
    parser.add_argument("--cache-root", required=True)
    parser.add_argument("--threads", nargs="+", type=int, default=[4, 8, 16])
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--ssh-target", default="root@114.32.64.6")
    parser.add_argument("--ssh-port", type=int, default=40032)
    parser.add_argument("--identity-file", type=Path, default=Path("/root/.ssh/stockagent_vastai1t_ed25519"))
    args = parser.parse_args()
    if (args.output.exists() or not 1 <= args.rounds <= 3 or not args.threads
            or len(set(args.threads)) != len(args.threads) or any(not 1 <= n <= 16 for n in args.threads)):
        parser.error("fresh receipt, 1-3 rounds and unique bounded thread candidates required")
    native_guard()
    os.umask(0o077)
    inventory = json.loads(args.cache_inventory.read_text())
    row = next((r for r in inventory["caches"] if r["name"] == args.cache_root), None)
    if not row or row["process_references"] or row["service_references"] or row["service_error"]:
        raise SnapshotError("benchmark requires one explicitly inventoried inactive cache")
    if row["logical_bytes"] > 2 * 1024**3:
        raise SnapshotError("bounded benchmark sample exceeds two GiB")
    inventory["requested_roots"] = [args.cache_root]
    inventory["explicit_protected_roots"] = load_policy()["protected_cache_roots"]
    # The same canonical producer repeats current references and exact metadata.
    measured_producer(args.threads[0], inventory)
    scratch = Path(tempfile.mkdtemp(prefix=".stockagent-transport-benchmark-",
                                    dir=metadata_path("/mnt/d/stockagent-cold-primary")))
    records = []
    for turn in range(args.rounds):
        order = args.threads if turn % 2 == 0 else list(reversed(args.threads))
        for threads in order:
            native_guard()
            trial = f"round-{turn+1}-threads-{threads}"
            payload = scratch / (trial + ".tar.zst")
            error_path = args.output.parent / (args.output.stem + "-" + trial + ".log")
            args.output.parent.mkdir(parents=True, exist_ok=True)
            command = [*_ssh_base(args.identity_file, args.ssh_port), _validate_ssh_target(args.ssh_target),
                       "cd /root/stockAgent && source scripts/runtime_env.sh && run_fintech_python -"]
            body = measured_producer(threads, inventory)
            started = time.perf_counter()
            transferred, expected = 0, hashlib.sha256()
            with error_path.open("xb") as errors:
                process = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=errors)
                process.stdin.write(body.encode()); process.stdin.close()
                target = None
                try:
                    framing = 0
                    while True:
                        line = process.stdout.readline(4096)
                        framing += len(line)
                        if line == MAGIC:
                            break
                        if not line or framing > 65536:
                            raise SnapshotError("benchmark authenticated stream marker missing")
                    target = BinaryWriter(payload, reserve_bytes=64 * 1024**3)
                    while block := process.stdout.read(4 * 1024 * 1024):
                        target.write(block); expected.update(block); transferred += len(block)
                    if process.wait(timeout=60):
                        raise SnapshotError("remote measured producer failed; source and scratch retained")
                    durable = target.finish()
                finally:
                    if process.poll() is None:
                        process.terminate(); process.wait(timeout=30)
                    if target is not None:
                        target.close()
            received = time.perf_counter()
            if durable["sha256"] != expected.hexdigest() or durable["bytes"] != transferred:
                raise SnapshotError("durable D benchmark payload differs from stream")
            values = [line[len(PROOF_MARKER):] for line in error_path.read_text().splitlines() if line.startswith(PROOF_MARKER)]
            if len(values) != 1:
                raise SnapshotError("remote independent source proof missing")
            proof = json.loads(values[0])
            before = signature(payload)
            index = index_zstd(payload, expected_sha256=durable["sha256"], scopes={"cache"})
            compare_originals(index, proof)
            if signature(payload) != before or payload.stat().st_nlink != 1:
                raise SnapshotError("private benchmark payload changed or acquired an external link")
            payload.unlink(); _fsync_directory(scratch)
            elapsed = time.perf_counter() - started
            record = {"threads": threads, "round": turn + 1, "state": "accepted",
                      "complete_wall_seconds": elapsed, "transport_fsync_seconds": received - started,
                      "decode_original_compare_cleanup_seconds": time.perf_counter() - received,
                      "original_bytes": row["logical_bytes"], "compressed_bytes": transferred,
                      "complete_original_mib_per_second": row["logical_bytes"] / 1024**2 / elapsed,
                      "all_original_sha_metadata_verified": True, "private_payload_removed": True,
                      "producer_code_sha256": hashlib.sha256(body.encode()).hexdigest(),
                      **{k: v for k, v in proof.items() if k != "source_rows"}}
            records.append(record)
            atomic_write_json(args.output.with_suffix(".progress.json"), {"state": "running", "runs": records})
            print(json.dumps(record), flush=True)
    if list(scratch.iterdir()):
        raise SnapshotError("unexpected benchmark scratch retained")
    scratch.rmdir(); _fsync_directory(scratch.parent)
    rankings = sorted([{"threads": n, "samples": args.rounds,
                         "mean_complete_seconds": statistics.mean(r["complete_wall_seconds"] for r in records if r["threads"] == n),
                         "median_complete_seconds": statistics.median(r["complete_wall_seconds"] for r in records if r["threads"] == n)}
                        for n in args.threads], key=lambda r: (r["mean_complete_seconds"], r["threads"]))
    result = {"state": "measured_remote_transport_accepted", "observed_at_epoch": time.time(),
              "cache_root": args.cache_root, "source_fingerprint": row["fingerprint"], "runs": records,
              "ranked": rankings, "selected_threads": rankings[0]["threads"],
              "os_caches_flushed": False, "source_deleted": False, "cold_published": False,
              "scope": "actual node/load, one complete inactive cache, SSH/native D/fsync/decode/original parity; no training-build claim"}
    atomic_write_json(args.output, result)
    print(json.dumps({k: result[k] for k in ("state", "selected_threads", "ranked")}))


if __name__ == "__main__":
    main()

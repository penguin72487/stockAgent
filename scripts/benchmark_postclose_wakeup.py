"""Fresh-process full observation versus its unchanged generation plan (ABBA).

Actual configured source metadata, full decision parity, no worker dispatch,
network, source writes, cache acceptance or model/CUDA imports. This measures
the entire engineering probe process, not inference or cold filesystem I/O.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import statistics
import subprocess
import sys
import tempfile
import time

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

WORKER = """
import json,resource,sys,time
from pathlib import Path
t=time.perf_counter(); c=time.process_time()
from scripts import discord_postclose_wakeup as w
path=Path(sys.argv[2]); previous=json.loads(path.read_bytes())
snapshot=w.observe(Path.cwd(),previous=previous if sys.argv[1]=='unchanged_plan' else None)
forbidden=sorted({'torch','pandas','requests'} & sys.modules.keys())
assert not forbidden
if sys.argv[1]=='unchanged_plan':
 assert not {'numpy','pyarrow','stockagent.config'} & sys.modules.keys()
print(json.dumps({'snapshot':snapshot,'wall_seconds':time.perf_counter()-t,
 'cpu_seconds':time.process_time()-c,
 'post_exec_vm_hwm_bytes':int(next(line.split()[1] for line in Path('/proc/self/status').read_text().splitlines() if line.startswith('VmHWM:')))*1024,
 'rusage_maxrss_bytes_includes_fork_pre_exec':resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*1024,
 'forbidden_imports':forbidden},sort_keys=True))
"""


def benchmark(*, rounds: int = 1) -> dict:
    from scripts import discord_postclose_wakeup as w

    if not 1 <= rounds <= 3:
        raise ValueError("rounds must be 1..3")
    code = {str(path): hashlib.sha256(path.read_bytes()).hexdigest() for path in (
        Path(__file__), ROOT / "scripts/discord_postclose_wakeup.py",
        ROOT / "scripts/run_discord_artifact_maintenance.py")}
    snapshot = w.observe(ROOT)
    if not snapshot["fingerprint"]:
        raise ValueError("accepted completed session and configured markets required")
    trials = []
    with tempfile.TemporaryDirectory(prefix="stockagent-postclose-probe-") as directory:
        path = Path(directory) / "plan.json"
        path.write_text(json.dumps(snapshot, sort_keys=True))
        for variant in ("full_observation", "unchanged_plan", "unchanged_plan", "full_observation") * rounds:
            started = time.perf_counter()
            child = subprocess.run([sys.executable, "-c", WORKER, variant, str(path)],
                                   cwd=ROOT, capture_output=True, text=True, check=True, timeout=20)
            wall = time.perf_counter() - started
            row = json.loads(child.stdout)
            if row.pop("snapshot") != snapshot:
                raise ValueError("source/code/config generation changed or decision parity failed")
            trials.append({"variant": variant, "complete_process_wall_seconds": wall, **row})
    if w.observe(ROOT) != snapshot:
        raise ValueError("source/code/config changed during benchmark")
    if code != {path: hashlib.sha256(Path(path).read_bytes()).hexdigest() for path in code}:
        raise ValueError("benchmark implementation changed during measurement")
    means = {variant: {key: statistics.mean(row[key] for row in trials if row["variant"] == variant)
                       for key in ("complete_process_wall_seconds", "cpu_seconds", "post_exec_vm_hwm_bytes")}
             for variant in ("full_observation", "unchanged_plan")}
    return {"contract": "postclose_cpu_wakeup_complete_probe_abba_v1", "state": "verified",
            "observed_at_utc": datetime.now(timezone.utc).isoformat(), "code_sha256": code,
            "scope": "fresh_process_configured_metadata_probe_not_inference_or_service_SLA",
            "host_exclusive": False, "filesystem_cache_cleared": False,
            "provider_calls": 0, "model_forward_calls": 0, "dispatch_requests": 0,
            "source_writes": 0, "acceptance_writes": 0, "complete_decision_parity": True,
            "snapshot": snapshot, "samples": trials, "means": means}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rounds", type=int, default=1)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    if args.output.exists():
        raise FileExistsError("do not replace previous measurements")
    result = benchmark(rounds=args.rounds)
    from downloader.artifact_io import atomic_write_json

    atomic_write_json(args.output, result)
    print(json.dumps({"output": str(args.output), "state": result["state"], "means": result["means"]}))


if __name__ == "__main__":
    main()

"""Small fleet observations for the existing minute storage observer.

These are process/host observations, never recovery, CUDA or training acceptance.
No payload scans, model imports, wakeups, new worker or HTTP-side probes.
"""
from __future__ import annotations

import math
import os
from pathlib import Path
import subprocess
import time

from stockagent.data_sync.storage_progress import iso, timestamp

CONTRACT = "stockagent.storage_runtime.v1"
HOST_RUNTIME_CONTRACT = "stockagent.storage_host_runtime.v1"
HOST_METRICS = frozenset({"cpu_percent", "cpu_count", "memory_used_bytes", "memory_total_bytes"})
NODES = {"penguin": "Penguin", "lab203": "lab203／NAS", "vastai1T": "Vast"}
SERVICES = {
    "stockagent-tw-day-trade-simulation.service": ("day_trade", "當沖服務"),
    "stockagent-tw-day-trade-priority-source.service": ("day_trade_source", "當沖優先資料更新"),
    "stockagent-finlab-daytrade-projection.service": ("day_trade_projection", "當沖資料來源投影"),
    "stockagent-tw-overnight-simulation.service": ("overnight", "隔夜服務"),
    "stockagent-shioaji-taifex-bidask.service": ("taifex", "期貨行情服務"),
    "stockagent-storage-lifecycle.service": ("lakehouse", "儲存工作執行者"),
    "stockagent-backup-stream.service": ("backup", "冷庫備份"),
    "stockagent-private-cold-backup.service": ("private_backup", "私有冷庫備份"),
    "stockagent-wsl-retirement-queue.service": ("retirement", "WSL 回收排程"),
    "syncthing@root.service": ("transport", "同步傳輸"),
}
SERVICE_STATES = {"active", "activating", "deactivating", "inactive", "failed", "waiting", "unknown"}
METRICS = {"cpu_percent", "cpu_count", "memory_used_bytes", "memory_total_bytes",
           "gpu_count", "gpu_percent", "gpu_memory_used_mb", "gpu_memory_total_mb",
           "supervised_running", "supervised_waiting", "supervised_failed"}
INTEGER_METRICS = {"cpu_count", "memory_used_bytes", "memory_total_bytes", "gpu_count",
                   "supervised_running", "supervised_waiting", "supervised_failed"}

# Executed inside the SAME enrolled SSH/statvfs request. Portal is a local
# read-only metrics cache; supervisor is observed, never started or restarted.
REMOTE_RUNTIME = """
import urllib.request,subprocess
runtime={'state':'measured','observed_at_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'source':'ssh_observer','metrics':{}}
try:
 with urllib.request.urlopen('http://localhost:11111/system-metrics',timeout=2) as r:
  raw=r.read(65537)
 if len(raw)>65536: raise ValueError('metrics_too_large')
 m=json.loads(raw); metrics=runtime['metrics']
 for section,key,target in [('cpu','percent','cpu_percent'),('cpu','count','cpu_count'),('ram','used','memory_used_bytes'),('ram','total','memory_total_bytes'),('gpu','count','gpu_count'),('gpu','avg_load_percent','gpu_percent')]:
  metrics[target]=m.get(section,{}).get(key)
 if m.get('gpu',{}).get('memory_unit')=='MB':
  metrics['gpu_memory_used_mb']=m['gpu'].get('memory_used'); metrics['gpu_memory_total_mb']=m['gpu'].get('memory_total')
except (OSError,ValueError,TypeError,KeyError):
 pass
try:
 p=subprocess.run(['supervisorctl','status'],capture_output=True,text=True,timeout=2)
 if len(p.stdout)>65536: raise ValueError('supervisor_too_large')
 programs=[line.split() for line in p.stdout.splitlines() if line.startswith('stockagent-')]
 if programs and all(len(row)>1 and row[1] in {'RUNNING','STARTING','STOPPING','STOPPED','EXITED','FATAL','BACKOFF','UNKNOWN'} for row in programs):
  runtime['metrics'].update(supervised_running=sum(row[1]=='RUNNING' for row in programs),supervised_waiting=sum(row[1] in {'STARTING','STOPPING'} for row in programs),supervised_failed=sum(row[1] in {'FATAL','BACKOFF','UNKNOWN'} for row in programs))
except (OSError,ValueError,subprocess.SubprocessError):
 pass
"""


def metric(value, *, integer=False, percent=False):
    if (type(value) not in ((int,) if integer else (int, float))
            or not math.isfinite(value) or not 0 <= value <= (100 if percent else 2**53 - 1)):
        return None
    return value


def local_observation(previous: dict, *, now: float, include_services: bool = True) -> dict:
    previous = previous if isinstance(previous, dict) else {}
    result = {"state": "measured", "observed_at_utc": iso(now), "source": "local_proc",
              "metrics": {}, "services": []}
    metrics = result["metrics"]
    try:
        fields = Path("/proc/stat").read_text().splitlines()[0].split()[1:9]
        counts = list(map(int, fields))
        sample = {"total": sum(counts), "idle": counts[3] + counts[4], "clock": time.monotonic(),
                  "boot": Path("/proc/sys/kernel/random/boot_id").read_text().strip()}
        old = previous.get("cpu_sample")
        old = old if isinstance(old, dict) else {}
        if (all(metric(old.get(key)) is not None for key in ("clock", "total", "idle"))
                and old.get("boot") == sample["boot"] and 0 < sample["clock"] - old["clock"] <= 180
                and sample["total"] > old["total"]):
            delta = sample["total"] - old["total"]
            idle = sample["idle"] - old["idle"]
            if 0 <= idle <= delta:
                metrics["cpu_percent"] = round(100 * (delta - idle) / delta, 2)
        result["cpu_sample"] = sample  # Local sampler state; public projection removes it.
        metrics["cpu_count"] = len(os.sched_getaffinity(0))
        memory = dict(line.split(":", 1) for line in Path("/proc/meminfo").read_text().splitlines())
        total, available = (int(memory[key].split()[0]) * 1024 for key in ("MemTotal", "MemAvailable"))
        if 0 <= available <= total:
            metrics.update(memory_total_bytes=total, memory_used_bytes=total - available)
    except (OSError, ValueError, IndexError, KeyError):
        pass
    if not include_services:
        return result
    try:
        process = subprocess.run(["systemctl", "show", *SERVICES, "-p", "Id", "-p", "ActiveState",
                                  "-p", "LoadState", "-p", "ExecMainStatus", "--no-pager"],
                                 capture_output=True, text=True, timeout=3)
        if len(process.stdout) > 65536:
            raise ValueError("service_observation_too_large")
        for block in process.stdout.strip().split("\n\n"):
            row = dict(line.split("=", 1) for line in block.splitlines() if "=" in line)
            if row.get("Id") in SERVICES and row.get("LoadState") == "loaded":
                state = row.get("ActiveState")
                if (SERVICES[row["Id"]][0] in {"day_trade_source", "day_trade_projection"}
                        and state in {"failed", "inactive"} and row.get("ExecMainStatus") == "75"):
                    state = "waiting"
                result["services"].append({"id": SERVICES[row["Id"]][0], "state": state})
    except (OSError, ValueError, subprocess.SubprocessError):
        pass
    return result


def receiver_host_observation(previous: dict, *, now: float) -> tuple[dict, dict]:
    """Sample receiver CPU/RAM without leaking sampler state into its signed receipt."""
    sample = local_observation(previous, now=now, include_services=False)
    exported = {"contract": HOST_RUNTIME_CONTRACT, "observed_at_utc": sample["observed_at_utc"],
                "metrics": {key: sample["metrics"][key] for key in HOST_METRICS
                            if key in sample["metrics"]}}
    return sample, exported


def collect(previous, remote, *, receiver=None, now=None) -> dict:
    """One minute turn; only Penguin/SSH observations and paired relay metadata."""
    now = time.time() if now is None else now
    old = previous if isinstance(previous, dict) and previous.get("contract") == CONTRACT else {}
    old_nodes = old.get("nodes") if isinstance(old.get("nodes"), dict) else {}
    nodes = {"penguin": local_observation(old_nodes.get("penguin", {}), now=now)}
    candidate = remote.get("runtime") if isinstance(remote, dict) else None
    nodes["vastai1T"] = candidate if isinstance(candidate, dict) else {"state": "unavailable"}
    # The capacity observer's already paired/signed receipt is reused. No
    # expensive NAS call or second management connection is introduced.
    if isinstance(receiver, dict):
        nodes["lab203"] = {"state": "measured", "source": "paired_storage_receipt",
                            "observed_at_utc": receiver.get("generated_at_utc"), "metrics": {}}
        host = receiver.get("host_runtime")
        receipt_clock = timestamp(receiver.get("generated_at_utc"))
        host_clock = timestamp(host.get("observed_at_utc")) if isinstance(host, dict) else None
        if (isinstance(host, dict) and host.get("contract") == HOST_RUNTIME_CONTRACT
                and receipt_clock is not None and host_clock is not None
                and 0 < host_clock <= receipt_clock + 120 and isinstance(host.get("metrics"), dict)):
            nodes["lab203"] = {"state": "measured", "source": "paired_host_runtime",
                "observed_at_utc": host["observed_at_utc"],
                "metrics": {key: host["metrics"][key] for key in HOST_METRICS if key in host["metrics"]}}
    else:
        nodes["lab203"] = {"state": "unavailable"}
    for name in ("lab203", "vastai1T"):
        row = nodes[name]
        prior = old_nodes.get(name) if isinstance(old_nodes.get(name), dict) else {}
        if row.get("state") == "unavailable" and timestamp(prior.get("observed_at_utc")) is not None:
            nodes[name] = {**prior, "state": "unavailable", "attempted_at_utc": iso(now)}
    return {"contract": CONTRACT, "observed_at_utc": iso(now), "nodes": nodes}


def public_status(value, *, now=None) -> dict:
    now = time.time() if now is None else now
    value = value if isinstance(value, dict) and value.get("contract") == CONTRACT else {}
    source = value.get("nodes") if isinstance(value.get("nodes"), dict) else {}
    nodes = []
    labels = {key: label for key, label in SERVICES.values()}
    for name, label in NODES.items():
        raw = source.get(name) if isinstance(source.get(name), dict) else {}
        observed = timestamp(raw.get("observed_at_utc"))
        valid_clock = observed is not None and 0 < observed <= now + 120
        age = max(0, now - observed) if valid_clock else None
        state = ("unknown" if not valid_clock else "stale" if age > 180 else
                 "measured" if raw.get("state") == "measured" else "unavailable")
        metrics = raw.get("metrics") if isinstance(raw.get("metrics"), dict) else {}
        metrics = {key: metric(metrics.get(key), integer=key in INTEGER_METRICS, percent=key.endswith("percent"))
                   for key in sorted(METRICS)}
        total, used = metrics["memory_total_bytes"], metrics["memory_used_bytes"]
        if used is not None and (total is None or used > total):
            metrics["memory_used_bytes"] = None
        gpu_used, gpu_total = metrics["gpu_memory_used_mb"], metrics["gpu_memory_total_mb"]
        if gpu_used is not None and (gpu_total is None or gpu_used > gpu_total):
            metrics["gpu_memory_used_mb"] = None
        services = raw.get("services") if isinstance(raw.get("services"), list) else []
        indexed = {r.get("id"): r for r in services if isinstance(r, dict) and isinstance(r.get("id"), str)}
        nodes.append({"id": name, "label": label, "state": state,
            "observed_at_utc": iso(observed) if valid_clock else None,
            "age_seconds": round(age) if age is not None else None,
            "metrics": metrics,
            "services": [{"id": key, "label": title, "state": indexed.get(key, {}).get("state")
                          if isinstance(indexed.get(key, {}).get("state"), str)
                          and indexed[key]["state"] in SERVICE_STATES else "unknown"}
                         for key, title in labels.items()] if name == "penguin" else [],
            "scope": {"penguin": "WSL 整體使用量與已登錄服務；GPU 未取樣，避免喚醒本機 GPU。",
                      "lab203": ("CPU／RAM 由接收端配對回執取樣；備份驗收與工作量仍須看各自收據。"
                                 if raw.get("source") == "paired_host_runtime" else
                                 "沿既有配對回執確認在線；接收端尚未提供 CPU／RAM 與逐階段工作量。"),
                      "vastai1T": "Vast 本機監控快取與 Supervisor StockAgent 程序；可能與其他工作共享硬體。"}[name]})
    return {"contract": CONTRACT, "read_only": True, "interval_seconds": 60, "nodes": nodes,
            "acceptance": "在線、GPU 使用率與程序數均不代表資料或訓練已驗收。"}

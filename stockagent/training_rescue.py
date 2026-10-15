"""Penguin-owned, confirmation-gated recovery of ephemeral training workers.

Cold payloads use guarded D storage; small journals/keys live on SSD. SSH
reachability, NVML visibility, complete checkpoint preservation, bootstrap,
resume and useful training progress are deliberately separate receipts.
"""
from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import uuid
import urllib.error
import urllib.request
import zipfile

from stockagent.remote_ssh import ssh_base, validate_ssh_target
from stockagent.runtime_identity import validate_runtime_lock
from stockagent.training_rescue_worker import (
    CONTRACT, HASH, absolute, atomic_json, digest, file_hash, identifier,
    relative, validate_spec,
)

ROOT = Path(__file__).resolve().parents[1]
WORKER = ROOT / "stockagent/training_rescue_worker.py"
REMOTE_ADAPTER = "/opt/stockagent-training-rescue/worker.py"
REMOTE_STATE = "/var/lib/stockagent-training-rescue"


class RescueError(RuntimeError):
    pass


def private_file(path: Path) -> Path:
    absolute(str(path))
    if not path.is_file() or path.stat().st_mode & 0o077:
        raise RescueError("rescue private file must be regular and 0600")
    return path


def load_policy(path: Path) -> dict:
    policy = json.loads(path.read_bytes())
    if policy.get("contract") != CONTRACT or policy.get("schema_version") != 1:
        raise RescueError("unsupported rescue policy")
    if (policy.get("automatic_destination_selection") is not False
            or policy.get("automatic_provider_mutations") is not False
            or policy.get("delete_source_files") is not False):
        raise RescueError("rescue never selects/rents/destroys a host or deletes source data")
    for k in ("state_root", "cold_root"):
        absolute(policy[k])
    if policy.get("cold_guard") != "penguin-d-primary":
        raise RescueError("production rescue requires the enrolled D primary guard")
    if not re.fullmatch(r"[A-Za-z0-9_.-]{1,80}", policy.get("windows_distribution", "")):
        raise RescueError("rescue requires the enrolled WSL distribution identity")
    for k in ("poll_seconds", "lease_seconds", "reserve_bytes", "maximum_member_bytes", "minimum_destination_free_bytes"):
        if type(policy.get(k)) is not int or policy[k] <= 0:
            raise RescueError("rescue capacity/lease controls must be positive integers")
    if not 30 <= policy["lease_seconds"] <= 180:
        raise RescueError("worker fencing lease must be bounded to 30..180 seconds")
    return policy


class SSH:
    def __init__(self, base: list[str], target: str):
        self.base, self.target = base, validate_ssh_target(target)

    @classmethod
    def profile(cls, config: Path, alias: str):
        private_file(config)
        return cls(["ssh", "-F", str(config), "-o", "BatchMode=yes", "-o",
                    "StrictHostKeyChecking=yes", "-o", "ForwardAgent=no", "-o",
                    "ConnectTimeout=8", "-o", "ServerAliveInterval=5", "-o", "ServerAliveCountMax=3"], alias)

    @classmethod
    def target(cls, target: dict, key_path: Path, known_hosts: Path):
        private_file(known_hosts)
        base = ssh_base(private_file(key_path), target["port"])
        # No inherited config, multiplexed session, ssh-agent or host-key bypass.
        base[1:1] = ["-F", "/dev/null"]
        base += ["-o", "StrictHostKeyChecking=yes", "-o", f"UserKnownHostsFile={known_hosts}",
                 "-o", "ForwardAgent=no", "-o", "ControlMaster=no", "-o", "ServerAliveInterval=5",
                 "-o", "ServerAliveCountMax=3"]
        return cls(base, target["user"] + "@" + target["host"])

    def command(self, argv: list[str]) -> list[str]:
        return [*self.base, self.target, shlex.join([str(v) for v in argv])]

    def run(self, argv: list[str], *, input: bytes | None = None, timeout: int = 60) -> bytes:
        result = subprocess.run(self.command(argv), input=input, capture_output=True, timeout=timeout)
        if result.returncode:
            # No arbitrary SSH stderr, env or remote exceptions in public logs.
            raise RescueError(f"SSH operation failed (exit={result.returncode})")
        if len(result.stdout) > 64 * 1024**2:
            raise RescueError("remote control response exceeded its bound")
        return result.stdout

    def json(self, argv: list[str], **kwargs) -> dict:
        return json.loads(self.run(argv, **kwargs))

    def adapter(self, action: str, *args: str, python="python3", timeout=120) -> dict:
        return self.json([python, REMOTE_ADAPTER, action, *args], timeout=timeout)

    def upload(self, path: Path, data: bytes) -> dict:
        absolute(str(path))
        sha = hashlib.sha256(data).hexdigest()
        return self.json(["python3", REMOTE_ADAPTER, "receive", "--path", str(path),
                          "--sha256", sha, "--bytes", str(len(data))], input=data)

    def install_adapter(self) -> None:
        # Bootstrap has no account credentials; only explicit new SSH targets
        # after approval can execute it. Source hosts already have Python 3.
        script = (
            "import hashlib,os,pathlib,sys; p=pathlib.Path(sys.argv[1]); "
            "assert all(not q.is_symlink() for q in (p,*p.parents)); "
            "b=sys.stdin.buffer.read(); assert hashlib.sha256(b).hexdigest()==sys.argv[2]; "
            "p.parent.mkdir(parents=True,exist_ok=True,mode=0o700); "
            "n=p.with_name('.worker-'+sys.argv[2]+'.tmp'); "
            "f=os.open(n,os.O_WRONLY|os.O_CREAT|os.O_TRUNC|os.O_NOFOLLOW,0o600); "
            "os.write(f,b); os.fsync(f); os.close(f); os.replace(n,p)"
        )
        data = WORKER.read_bytes()
        self.run(["python3", "-c", script, REMOTE_ADAPTER, hashlib.sha256(data).hexdigest()], input=data)

    def open_read(self, root: Path, row: dict, *, python: str, errors):
        return subprocess.Popen(self.command([python, REMOTE_ADAPTER, "read", "--path", str(root),
            "--row", json.dumps(row, separators=(",", ":"))]), stdout=subprocess.PIPE, stderr=errors)


class ControlSSH(SSH):
    """Run-scoped forced-command transport, deliberately incapable of a shell."""
    def rpc(self, request: dict, *, timeout=180) -> dict:
        return json.loads(super().run(["stockagent-control"], input=json.dumps(request).encode() + b"\n", timeout=timeout))

    def adapter(self, action: str, *args: str, python="python3", timeout=120) -> dict:
        if action not in {"inspect", "health", "status", "stop", "seal"}:
            raise RescueError("ongoing control key cannot perform bootstrap/arbitrary commands")
        return self.rpc({"action": action}, timeout=timeout)

    def open_read(self, root: Path, row: dict, *, python: str, errors):
        process = subprocess.Popen(super().command(["stockagent-control"]), stdin=subprocess.PIPE,
                                   stdout=subprocess.PIPE, stderr=errors)
        request = {"action": "read", "row": row}
        if root.parent.name == "sealed":
            request["seal"] = root.name
        process.stdin.write(json.dumps(request).encode() + b"\n")
        process.stdin.close()
        return process


class Rescue:
    def __init__(self, policy: dict):
        self.policy = policy
        self.state = absolute(policy["state_root"])
        self.cold = absolute(policy["cold_root"])
        self.state.mkdir(parents=True, mode=0o700, exist_ok=True)

    @contextmanager
    def lock(self, *, wait_seconds: int = 0):
        if type(wait_seconds) is not int or not 0 <= wait_seconds <= 1200:
            raise RescueError("owner admission wait must be bounded to 0..1200 seconds")
        with (self.state / "owner.lock").open("a") as f:
            until = time.monotonic() + wait_seconds
            while True:
                try:
                    fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except BlockingIOError:
                    if time.monotonic() >= until:
                        raise
                    time.sleep(0.2)
            # WAN bootstrap/capture may take longer than a fencing lease. Keep
            # live already-owned generations supervised while this exclusive
            # transaction runs; never renew a stopped/awaiting generation.
            stop = threading.Event()
            def pulse():
                while not stop.wait(min(10, self.policy["lease_seconds"] // 3)):
                    try:
                        self.guard()
                    except Exception:
                        continue  # No cold durability -> no owner renewal.
                    for path in sorted((self.state / "runs").glob("*/state.json")):
                        try:
                            state = self.journal(path.parent.name)
                            if state.get("active_target") and state["state"] in {"recovering", "resume_progress_verified"}:
                                self.renew_target(path.parent.name, state)
                        except Exception:
                            pass  # Main monitor records fault; pulse never launches.
            worker = threading.Thread(target=pulse, daemon=True)
            worker.start()
            try:
                yield
            finally:
                stop.set(); worker.join(timeout=25)

    def guard(self):
        from stockagent.data_sync.cold_primary import _check_d_primary_mount
        _check_d_primary_mount(Path("/srv/stockagent-packed"))
        if not self.cold.is_relative_to(Path("/srv/stockagent-packed/.local-state/training-rescue")):
            raise RescueError("rescue cold payloads must stay in the excluded private D namespace")
        self.cold.mkdir(parents=True, mode=0o700, exist_ok=True)
        if shutil.disk_usage(self.cold).free < self.policy["reserve_bytes"]:
            raise RescueError("D reserve reached; never fall back to C")

    def path(self, run: str) -> Path:
        return self.state / "runs" / identifier(run)

    def spec(self, run: str) -> dict:
        return validate_spec(json.loads((self.path(run) / "spec.json").read_bytes()))

    def journal(self, run: str) -> dict:
        path = self.path(run) / "state.json"
        return json.loads(path.read_bytes()) if path.exists() else {
            "contract": CONTRACT, "run_id": run, "generation": 1,
            "state": "registered_not_preserved", "lease_sequence": 0}

    def record(self, run: str, state: dict) -> None:
        state["updated_at"] = time.time()
        atomic_json(self.path(run) / "state.json", state)

    def source(self, spec: dict) -> SSH:
        endpoint = spec["source_ssh"]
        return SSH.profile(Path(endpoint["config"]), endpoint["alias"])

    def register(self, spec: dict) -> dict:
        validate_spec(spec)
        private_file(Path(spec["source_ssh"]["config"]))
        validate_ssh_target(spec["source_ssh"]["alias"])
        run = spec["run_id"]
        path = self.path(run) / "spec.json"
        if path.exists() and json.loads(path.read_bytes()) != spec:
            raise RescueError("preserve the enrolled run contract; use a new ID for changed semantics")
        atomic_json(path, spec)
        state = self.journal(run)
        self.record(run, state)
        ssh = self.source(spec)
        ssh.install_adapter()
        ssh.upload(Path(REMOTE_STATE) / run / "spec.json", json.dumps(spec).encode())
        observed = ssh.adapter("health", timeout=20)
        identity = observed.get("node_identity")
        if identity:
            state["source_identity"] = identity
            state["health"] = {**observed, "observed_at": time.time()}
            self.record(run, state)
        return {"run_id": run, "state": state["state"], "training_started": False}

    def object_path(self, sha: str) -> Path:
        if not HASH.fullmatch(sha):
            raise RescueError("invalid rescue object digest")
        return self.cold / "objects" / sha[:2] / sha

    def object_hash(self, path: Path) -> str:
        from stockagent.data_sync.windows_cold_io import hash_file
        return hash_file(path)

    def put_stream(self, stream, row: dict, *, process=None) -> dict:
        from stockagent.data_sync.windows_cold_io import BinaryWriter, windows_path
        self.guard()
        sha, count = row["sha256"], row["bytes"]
        if type(count) is not int or not 0 <= count <= self.policy["maximum_member_bytes"]:
            raise RescueError("rescue member exceeds the admitted bound")
        destination = self.object_path(sha)
        if destination.exists():
            if destination.stat().st_size != count or self.object_hash(destination) != sha:
                raise RescueError("existing D rescue object is corrupt; preserve and reject")
            return {"sha256": sha, "bytes": count, "reused": True}
        destination.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
        partial = destination.with_name(sha + ".partial." + uuid.uuid4().hex)
        h, size, writer = hashlib.sha256(), 0, None
        try:
            if windows_path(partial) is not None:
                writer = BinaryWriter(partial, reserve_bytes=self.policy["reserve_bytes"])
            else:
                writer = partial.open("xb")
            for block in iter(lambda: stream.read(4 * 1024**2), b""):
                size += len(block)
                if size > count:
                    raise RescueError("incoming rescue object exceeds its sealed size")
                writer.write(block)
                h.update(block)
            if process is not None and process.wait(timeout=30):
                raise RescueError("remote source changed/failed during object transfer")
            if size != count or h.hexdigest() != sha:
                raise RescueError("partial/corrupt rescue object is not acknowledged")
            if isinstance(writer, BinaryWriter):
                proof = writer.finish()
                if proof["sha256"] != sha:
                    raise RescueError("native D flush readback differs")
            else:
                writer.flush(); os.fsync(writer.fileno())
            writer.close(); writer = None
            # Independent full-byte D readback BEFORE any commit/head ACK.
            if self.object_hash(partial) != sha:
                raise RescueError("independent D recovery readback failed")
            if destination.exists():
                raise RescueError("another publisher owns this D object; retry")
            os.rename(partial, destination)
            return {"sha256": sha, "bytes": count, "D_readback_verified": True}
        finally:
            if writer is not None:
                writer.close()
            # Failed partials are retained, never promoted or removed silently.

    def pull(self, ssh: SSH, root: Path, row: dict, *, python="python3") -> dict:
        if self.object_path(row["sha256"]).exists():
            from io import BytesIO
            return self.put_stream(BytesIO(), row)
        with tempfile.TemporaryFile() as errors:
            process = ssh.open_read(root, row, python=python, errors=errors)
            try:
                return self.put_stream(process.stdout, row, process=process)
            finally:
                if process.poll() is None:
                    process.terminate(); process.wait(timeout=20)
                process.stdout.close()

    def capture(self, run: str, *, transport: SSH | None = None) -> dict:
        self.guard()
        spec, state = self.spec(run), self.journal(run)
        if transport is not None:
            ssh = transport
        elif state.get("active_target"):
            plan = json.loads((self.state / "plans" / (state["active_target"]["plan_id"] + ".json")).read_bytes())
            ssh = self.control_ssh(plan)
        else:
            ssh = self.source(spec)
        remote_spec = str(Path(REMOTE_STATE) / run / "spec.json")
        inventory = ssh.adapter("seal", "--spec", remote_spec,
                                python=spec["runtime_prefix"] + "/bin/python", timeout=180)
        if inventory.get("spec_sha256") != digest(spec):
            raise RescueError("source inspected a different enrolled run")
        if (not HASH.fullmatch(inventory.get("context_sha256", ""))
                or digest(inventory["files"]) != inventory["context_sha256"]):
            raise RescueError("source seal inventory identity differs")
        state["source_lifecycle_state"] = inventory.get("source_lifecycle_state", "unknown")
        for name, proof in inventory["checkpoints"].items():
            previous = state.get("latest", {}).get("checkpoints", {}).get(name)
            if previous and proof["epoch"] < previous["epoch"]:
                raise RescueError("checkpoint regressed; never replace a later preserved recovery point")
        if state.get("latest", {}).get("context_sha256") == inventory["context_sha256"]:
            # An old SSD ACK is not proof that D remains recoverable today.
            for row in inventory["files"]:
                self.pull(ssh, Path(inventory["sealed_root"]), row)
            self.acknowledge_seal(ssh, remote_spec, inventory, state["latest"]["commit_id"])
            self.record(run, state)
            return {"state": "unchanged_committed_recovery_point", "run_id": run,
                    "checkpoint_epochs": {k: v["epoch"] for k, v in inventory["checkpoints"].items()}}
        rows = inventory["files"]
        if len(rows) > 2048 or sum(r["bytes"] for r in rows) > 4 * 1024**3:
            raise RescueError("formal recovery context exceeds enrolled bounds")
        expected_sealed_root = Path(REMOTE_STATE) / run / "sealed" / inventory["context_sha256"]
        if inventory.get("sealed_root") != str(expected_sealed_root):
            raise RescueError("source returned a seal outside its enrolled private namespace")
        for row in rows:
            self.pull(ssh, expected_sealed_root, row)
        commit = {"contract": CONTRACT, "run_id": run, "spec_sha256": digest(spec),
                  "generation": state["generation"], "inventory": inventory,
                  "D_original_readback_verified": True, "committed_at": time.time(),
                  "all_history_backup_claim": False, "NAS_backup_verified": False}
        commit_id = digest(commit)
        atomic_json(self.cold / "commits" / run / (commit_id + ".json"), commit)
        # The manifest lives on D too; an SSD journal alone is never a payload proof.
        if digest(json.loads((self.cold / "commits" / run / (commit_id + ".json")).read_bytes())) != commit_id:
            raise RescueError("D commit manifest readback differs")
        state["latest"] = {"commit_id": commit_id, "context_sha256": inventory["context_sha256"],
                           "committed_at": commit["committed_at"], "bytes": sum(r["bytes"] for r in rows),
                           "checkpoints": inventory["checkpoints"]}
        if not state.get("active_target"):
            state["state"] = "preserved_waiting_user_target"
        self.record(run, state)
        self.acknowledge_seal(ssh, remote_spec, inventory, commit_id)
        return {"run_id": run, "state": state["state"], "commit_id": commit_id,
                "files": len(rows), "bytes": state["latest"]["bytes"],
                "checkpoint_epochs": {k: v["epoch"] for k, v in inventory["checkpoints"].items()},
                "D_original_readback_verified": True, "training_started": False}

    def acknowledge_seal(self, ssh: SSH, remote_spec: str, inventory: dict, commit_id: str):
        if isinstance(ssh, ControlSSH):
            return ssh.rpc({"action": "ack-seal", "context_sha256": inventory["context_sha256"], "commit_id": commit_id})
        # The selected interpreter is already fixed by the enrolled run.
        run = Path(remote_spec).parent.name
        python = self.spec(run)["runtime_prefix"] + "/bin/python"
        return ssh.adapter("ack-seal", "--spec", remote_spec, "--sha256", inventory["context_sha256"],
                           "--commit-id", commit_id, python=python, timeout=180)

    def put_file(self, path: Path) -> dict:
        row = {"sha256": file_hash(path), "bytes": path.stat().st_size}
        with path.open("rb") as stream:
            self.put_stream(stream, row)
        return row

    def seal_bootstrap(self, run: str) -> dict:
        """Pin fixed source/runtime, never a Git moving HEAD or environment.yml."""
        self.guard()
        spec, state = self.spec(run), self.journal(run)
        ssh = self.source(spec)
        files = spec["bootstrap_files"]
        runtime_path = Path(spec["runtime_capsule_path"])
        runtime_stability = ssh.adapter("verify-runtime", "--path", spec["runtime_prefix"],
            "--proof", str(runtime_path.with_suffix(runtime_path.suffix + ".json")), timeout=600)
        for row in files:
            self.pull(ssh, Path(row["source_path"]).parent,
                      {**row, "relative": Path(row["source_path"]).name})
        proof = ssh.adapter("metadata", "--path", str(runtime_path.with_suffix(runtime_path.suffix + ".json")))
        raw_proof = ssh.run(["python3", REMOTE_ADAPTER, "read", "--path", str(runtime_path.parent), "--row",
            json.dumps({**proof, "relative": runtime_path.name + ".json"})], timeout=180)
        capsule_proof = json.loads(raw_proof)
        if capsule_proof["prefix"] != spec["runtime_prefix"] or capsule_proof.get("kind") != "fixed_prefix_runtime":
            raise RescueError("runtime capsule belongs to a different training prefix")
        runtime = {"source_path": str(runtime_path), "sha256": capsule_proof["archive_sha256"],
                   "bytes": capsule_proof["archive_bytes"]}
        self.pull(ssh, Path(runtime["source_path"]).parent,
                  {**runtime, "relative": Path(runtime["source_path"]).name})
        self.pull(ssh, Path(proof["source_path"]).parent,
                  {**proof, "relative": Path(proof["source_path"]).name})
        payload = {"contract": CONTRACT, "run_id": run, "spec_sha256": digest(spec),
                   "files": files, "runtime_archive": runtime, "runtime_archive_proof": proof,
                   "runtime_platform": {k: capsule_proof[k] for k in ("system", "machine", "libc", "uncompressed_bytes")},
                   "source_runtime_stability": runtime_stability,
                   "sealed_at": time.time(), "D_original_readback_verified": True}
        bootstrap_id = digest(payload)
        atomic_json(self.cold / "bootstrap" / run / (bootstrap_id + ".json"), payload)
        state["bootstrap"] = {"id": bootstrap_id, "sealed_at": payload["sealed_at"]}
        self.record(run, state)
        return {"run_id": run, "state": "bootstrap_preserved", "bootstrap_id": bootstrap_id,
                "compressed_runtime_bytes": runtime["bytes"], "source_inputs_rebuilt_on_remote": True}

    def verify_sources(self, run: str) -> dict:
        """Decode and SHA-check pinned D inputs, without materializing on C."""
        from stockagent.data_sync.packed_snapshots import resolve_packed_snapshot_id, verify_packed_snapshot, _load_inventory
        self.guard()
        spec, state = self.spec(run), self.journal(run)
        rows = []
        for binding in spec.get("data_sources", []):
            root = Path("/srv/stockagent-packed")
            resolved = resolve_packed_snapshot_id(root, binding["dataset"], binding["snapshot_id"])
            if resolved.manifest_sha256 != binding["manifest_sha256"]:
                raise RescueError("pinned source manifest changed before recovery readback")
            paths = [r["path"] for r in _load_inventory(root, resolved.manifest) if r["kind"] == "file"]
            print(json.dumps({"phase": "source_readback_started", "dataset": binding["dataset"],
                              "original_files": len(paths)}), flush=True)
            proof = verify_packed_snapshot(root, resolved, reconstruct_paths=paths, d_primary_native_blob_reads=True,
                                          d_primary_native_pack_reads=True)
            if proof.get("independently_reconstructed_files") != len(paths):
                raise RescueError("not all original source file bytes were independently reconstructed")
            print(json.dumps({"phase": "source_readback_complete", "dataset": binding["dataset"],
                              "original_file_SHA_verified": len(paths)}), flush=True)
            rows.append({"binding": binding, "verification": proof})
        body = {"contract": CONTRACT, "run_id": run, "spec_sha256": digest(spec), "sources": rows,
                "verified_at": time.time(), "hot_materialization_created": False,
                "fresh_GPU_or_PIT_acceptance_claim": False}
        proof_id = digest(body)
        path = self.cold / "source-verification" / run / (proof_id + ".json")
        atomic_json(path, body)
        if digest(json.loads(path.read_bytes())) != proof_id:
            raise RescueError("D source verification receipt readback differs")
        state["input_verification"] = {"id": proof_id, "spec_sha256": digest(spec), "verified_at": body["verified_at"],
                                       "level": "all_original_file_SHA_decoded"}
        self.record(run, state)
        return {"run_id": run, "state": "pinned_sources_full_decode_and_SHA_verified", "source_count": len(rows),
                "receipt": str(path), "hot_materialization_created": False}

    def cycle(self) -> dict:
        self.guard()
        results = []
        for path in sorted((self.state / "runs").glob("*/spec.json")):
            run = path.parent.name
            state, spec = self.journal(run), self.spec(run)
            if state.get("active_target"):
                # A recovery owns its own state machine. Monitor may only renew
                # the currently committed destination's lease, never old source.
                if state["state"] in {"recovering", "resume_progress_verified"}:
                    try:
                        self.renew_target(run, state)
                        results.append(self.watch_target(run))
                    except (OSError, ValueError, RescueError, subprocess.TimeoutExpired) as exc:
                        state.update(state="awaiting_user_confirmation", health={"state": "target_unreachable", "error_type": type(exc).__name__})
                        self.record(run, state)
                continue
            try:
                ssh = self.source(spec)
                observed = ssh.adapter("health", timeout=20)
                identity = observed.get("node_identity")
                if identity and state.get("source_identity") and identity != state["source_identity"]:
                    raise RescueError("source SSH profile now points to another boot/instance; explicit re-enrollment required")
                if identity:
                    state["source_identity"] = identity
                if len(observed.get("devices", [])) < spec["world_size"] and observed["state"] != "gpu_fault":
                    observed = {"state": "gpu_fault", "reason": "device_count_reduced"}
                state["health"] = {**observed, "observed_at": time.time()}
                if observed["state"] == "gpu_fault" or state.get("source_lifecycle_state") == "failed":
                    state["state"] = "awaiting_user_confirmation"
                self.record(run, state)
                # Preserve CPU-readable committed progress even during GPU EIO.
                result = self.capture(run)
                state = self.journal(run)
                state["health"] = {**observed, "observed_at": time.time()}
                if observed["state"] == "gpu_fault" or state.get("source_lifecycle_state") == "failed":
                    state["state"] = "awaiting_user_confirmation"
                self.record(run, state)
                results.append(result)
            except (OSError, ValueError, RescueError, subprocess.TimeoutExpired) as exc:
                state = self.journal(run)
                state.update(state="awaiting_user_confirmation", last_failure={"type": type(exc).__name__, "at": time.time()})
                self.record(run, state)
                results.append({"run_id": run, "state": state["state"], "failure_type": type(exc).__name__,
                                "latest_checkpoint_retained": bool(state.get("latest")), "training_started": False})
        return {"contract": CONTRACT, "observed_at": time.time(), "runs": results, "automatic_migration": False}

    def issue_key(self, *, hours: int = 24) -> dict:
        if type(hours) is not int or not 1 <= hours <= 72:
            raise RescueError("temporary rescue keys last 1..72 hours")
        key_id = "rescue-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:8]
        directory = self.state / "keys" / key_id
        directory.mkdir(parents=True, mode=0o700)
        key = directory / "id_ed25519"
        subprocess.run(["ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-C", key_id, "-f", str(key)], check=True)
        public = (directory / "id_ed25519.pub").read_text().strip()
        expiry = time.time() + hours * 3600
        openssh_expiry = datetime.fromtimestamp(expiry, timezone.utc).strftime("%Y%m%d%H%M%SZ")
        record = {"contract": CONTRACT, "key_id": key_id, "public_key": public,
                  "expires_at": expiry, "state": "issued_not_authorized", "bound_plan": None}
        atomic_json(directory / "key.json", record)
        return {"key_id": key_id, "public_key": public, "expires_at_utc": openssh_expiry,
                "authorized_keys_line": f'restrict,expiry-time="{openssh_expiry}" {public}',
                "private_key_exported": False, "remote_authorization_created": False}

    def key(self, key_id: str) -> tuple[dict, Path]:
        directory = self.state / "keys" / identifier(key_id)
        value = json.loads((directory / "key.json").read_bytes())
        if value["expires_at"] <= time.time() or value["state"].startswith("revoked"):
            raise RescueError("rescue key expired/revoked; issue a new incident key")
        return value, private_file(directory / "id_ed25519")

    def plan(self, run: str, *, host: str, port: int, user: str, key_id: str, host_key_line: str) -> dict:
        self.guard()
        spec, state = self.spec(run), self.journal(run)
        key, _ = self.key(key_id)
        if not state.get("latest") or not state.get("bootstrap"):
            raise RescueError("checkpoint and source/runtime bootstrap must both be committed to D first")
        if spec.get("data_sources") and (state.get("input_verification", {}).get("spec_sha256") != digest(spec)
                or state.get("input_verification", {}).get("level") != "all_original_file_SHA_decoded"):
            raise RescueError("all pinned cold source bytes must be independently decoded/verified first")
        source_identity = state.get("target_identity") if state.get("active_target") else state.get("source_identity")
        if not source_identity:
            raise RescueError("old worker identity must be observed before planning a migration")
        if (not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9.-]*", host) or not 1 <= port <= 65535
                or not re.fullmatch(r"[a-z_][a-z0-9_-]*", user)):
            raise RescueError("invalid explicit destination SSH endpoint")
        fields = host_key_line.strip().split()
        if len(fields) != 3 or fields[1] != "ssh-ed25519":
            raise RescueError("supply the selected host's verified ed25519 ssh-keyscan line")
        expected_name = host if port == 22 else f"[{host}]:{port}"
        if fields[0] != expected_name:
            raise RescueError("host key belongs to a different destination endpoint")
        target = {"host": host, "port": port, "user": user, "host_key_line": " ".join(fields)}
        payload = {"contract": CONTRACT, "run_id": run, "spec_sha256": digest(spec),
                   "commit_id": state["latest"]["commit_id"], "bootstrap_id": state["bootstrap"]["id"],
                   "source_generation": state["generation"], "destination_generation": state["generation"] + 1,
                   "source_identity": source_identity,
                   "target": target, "key_id": key_id, "expires_at": min(time.time() + 3600, key["expires_at"]),
                   "created_at": time.time(), "automatic_provider_mutation": False}
        plan_id = digest(payload)
        if key.get("bound_plan"):
            raise RescueError("temporary key already belongs to another plan; issue a fresh key")
        atomic_json(self.state / "plans" / (plan_id + ".json"), payload)
        key["bound_plan"] = plan_id
        atomic_json(self.state / "keys" / key_id / "key.json", key)
        return {"plan_id": plan_id, "run_id": run, "target": {k: target[k] for k in ("host", "port", "user")},
                "checkpoint_epochs": {k: v["epoch"] for k, v in state["latest"]["checkpoints"].items()},
                "state": "plan_ready_requires_explicit_confirmation", "target_contacted": False,
                "old_node_fence_required": True, "expires_at": payload["expires_at"]}

    def confirmed_plan(self, plan_id: str, confirm: str) -> dict:
        if not HASH.fullmatch(plan_id) or confirm != plan_id:
            raise RescueError("migration requires the exact plan ID as explicit confirmation")
        plan = json.loads((self.state / "plans" / (plan_id + ".json")).read_bytes())
        if digest(plan) != plan_id or plan["expires_at"] <= time.time():
            raise RescueError("approval plan corrupted/expired; create another plan")
        key, _ = self.key(plan["key_id"])
        state = self.journal(plan["run_id"])
        if key.get("bound_plan") != plan_id or digest(self.spec(plan["run_id"])) != plan["spec_sha256"]:
            raise RescueError("approval key/spec no longer matches its plan")
        if state["generation"] not in {plan["source_generation"], plan["destination_generation"]}:
            raise RescueError("another migration already fenced this source generation")
        if state.get("migration_plan") == plan_id and state.get("launch"):
            raise RescueError("this recovery has already launched; use status/cycle, never replay its bootstrap")
        return plan

    def target_ssh(self, plan: dict) -> SSH:
        _, key_path = self.key(plan["key_id"])
        known = self.state / "keys" / plan["key_id"] / "known_hosts"
        data = (plan["target"]["host_key_line"] + "\n").encode()
        if known.exists() and known.read_bytes() != data:
            raise RescueError("preserve a conflicting enrolled SSH host key")
        from stockagent.data_sync.desync_snapshots import atomic_write_bytes
        atomic_write_bytes(known, data, mode=0o600)
        return SSH.target(plan["target"], key_path, known)

    def control_ssh(self, plan: dict) -> ControlSSH:
        directory = self.state / "keys" / plan["key_id"]
        ssh = SSH.target(plan["target"], directory / "id_control", directory / "known_hosts")
        return ControlSSH(ssh.base, ssh.target)

    def install_control_key(self, plan: dict, ssh: SSH, remote_spec: Path, status: Path) -> dict:
        directory = self.state / "keys" / plan["key_id"]
        private = directory / "id_control"
        if not private.exists():
            subprocess.run(["ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-C",
                "stockagent-control-" + plan["run_id"] + "-g" + str(plan["destination_generation"]), "-f", str(private)], check=True)
        public = private.with_suffix(".pub").read_text().strip()
        key, _ = self.key(plan["key_id"])
        expiry = datetime.fromtimestamp(key["expires_at"], timezone.utc).strftime("%Y%m%d%H%M%SZ")
        command = shlex.join([self.spec(plan["run_id"])["runtime_prefix"] + "/bin/python", REMOTE_ADAPTER,
                             "control", "--spec", str(remote_spec), "--output", str(status),
                             "--generation", str(plan["destination_generation"])])
        request = {"rescue_public_key": key["public_key"], "control_public_key": public,
                   "expiry": expiry, "forced_command": command}
        result = ssh.json(["python3", REMOTE_ADAPTER, "install-keys"], input=json.dumps(request).encode())
        ssh.upload(status.parent / "key-enrollment.json", json.dumps({"rescue_public_key": key["public_key"],
            "generation": plan["destination_generation"]}).encode())
        atomic_json(directory / "control-key.json", {"contract": CONTRACT, "run_id": plan["run_id"],
            "generation": plan["destination_generation"], "target": plan["target"],
            "public_key": public, "scope": "forced_command_one_run_lease_read_status_owned_stop", "shell_access": False})
        return result

    def push_object(self, ssh: SSH, row: dict, destination: Path) -> dict:
        from stockagent.data_sync.windows_cold_io import binary_reader
        source = self.object_path(row["sha256"])
        if source.stat().st_size != row["bytes"] or self.object_hash(source) != row["sha256"]:
            raise RescueError("D rescue object is not independently recoverable")
        with tempfile.TemporaryFile() as errors, binary_reader(source) as stream:
            command = ["python3", REMOTE_ADAPTER, "receive", "--path", str(destination),
                       "--sha256", row["sha256"], "--bytes", str(row["bytes"])]
            if "mode" in row:
                command += ["--mode", str(row["mode"])]
            process = subprocess.Popen(ssh.command(command), stdin=subprocess.PIPE,
                stdout=subprocess.PIPE, stderr=errors)
            try:
                try:
                    shutil.copyfileobj(stream, process.stdin, 4 * 1024**2)
                    process.stdin.close()
                except BrokenPipeError:
                    pass  # An idempotent target can acknowledge an already verified file.
                output = process.stdout.read(16384)
                if process.wait(timeout=60):
                    raise RescueError("target rejected rescue object")
                proof = json.loads(output)
                if proof.get("sha256") != row["sha256"] or proof.get("bytes") != row["bytes"]:
                    raise RescueError("target object acknowledgement differs")
                return proof
            finally:
                if process.poll() is None:
                    process.terminate(); process.wait(timeout=20)
                process.stdout.close()

    def fence_provider(self, run: str, *, api_key_file: Path | None = None,
                       identity: dict | None = None, generation: int | None = None) -> dict:
        """READ ONLY Vast proof, never Stop/Destroy/rent or dump provider env."""
        state = self.journal(run)
        if identity is None:
            identity = state.get("target_identity") if state.get("active_target") else state.get("source_identity")
        if not identity or not re.fullmatch(r"[0-9]+", identity.get("instance_id") or ""):
            raise RescueError("no enrolled Vast instance ID; provider proof cannot be guessed")
        token = private_file(api_key_file).read_text().strip() if api_key_file else os.environ.get("VAST_API_KEY", "")
        if not token or any(c.isspace() for c in token):
            raise RescueError("supply private --api-key-file or VAST_API_KEY; never paste it into a command")
        instance = identity["instance_id"]
        request = urllib.request.Request("https://console.vast.ai/api/v0/instances/" + instance + "/?owner=me",
                                         headers={"Authorization": "Bearer " + token}, method="GET")
        try:
            with urllib.request.urlopen(request, timeout=20) as response:
                raw = response.read(2 * 1024**2 + 1)
            if len(raw) > 2 * 1024**2:
                raise RescueError("provider readback exceeds response bound")
            row = json.loads(raw)["instances"]
        except (OSError, ValueError, KeyError) as exc:
            raise RescueError("provider read-only query failed: " + type(exc).__name__) from None
        if (not isinstance(row, dict) or str(row.get("id")) != instance
                or row.get("actual_status") != "stopped"):
            raise RescueError("exact old Vast instance has NOT been verified stopped; no destination launched")
        return self.write_fence(run, {"evidence_kind": "provider_stopped_readback", "source_identity": identity,
            "instance_id": instance, "actual_status": "stopped", "provider_write_called": False,
            "readback_api_key_file": str(api_key_file) if api_key_file else None}, generation=generation)

    def fence_guardian(self, run: str) -> dict:
        """Stop ONLY this run's enrolled managed guardian, then prove it is gone."""
        state = self.journal(run)
        if not state.get("active_target"):
            raise RescueError("legacy source is not owned by this guardian; use provider stopped readback")
        plan = json.loads((self.state / "plans" / (state["active_target"]["plan_id"] + ".json")).read_bytes())
        state["state"] = "awaiting_user_confirmation"
        self.record(run, state)  # Stop renewing this generation before stopping it.
        ssh = self.control_ssh(plan)
        ssh.adapter("stop")
        until = time.monotonic() + 45
        while time.monotonic() < until:
            status = ssh.adapter("status", timeout=10)
            if (status.get("generation") == state["generation"]
                    and status.get("node_identity") == state.get("target_identity")
                    and status.get("owned_processes_gone_verified") is True
                    and status.get("owned_guardian_gone_verified") is True):
                return self.write_fence(run, {"evidence_kind": "guarded_owned_processes_gone",
                    "source_identity": state["target_identity"], "owned_processes_gone_verified": True,
                    "guardian_status": status, "provider_write_called": False})
            time.sleep(1)
        raise RescueError("owned stop requested but descendants/guardian have NOT been proven gone")

    def write_fence(self, run: str, proof: dict, *, generation: int | None = None) -> dict:
        state = self.journal(run)
        body = {"contract": CONTRACT, "run_id": run,
                "source_generation": state["generation"] if generation is None else generation,
                "old_node_stopped_verified": True, "observed_at": time.time(), **proof}
        path = self.state / "fences" / (digest(body) + ".json")
        atomic_json(path, body)
        return {"run_id": run, "evidence_kind": proof["evidence_kind"], "receipt": str(path),
                "old_node_stopped_verified": True, "provider_write_called": False}

    def recover(self, plan_id: str, *, confirm: str, old_node_stopped_receipt: Path) -> dict:
        try:
            return self._recover(plan_id, confirm=confirm, old_node_stopped_receipt=old_node_stopped_receipt)
        except Exception as exc:
            if HASH.fullmatch(plan_id) and confirm == plan_id:
                path = self.state / "plans" / (plan_id + ".json")
                if path.exists():
                    saved = json.loads(path.read_bytes())
                    if digest(saved) == plan_id:
                        state = self.journal(saved["run_id"])
                        if state.get("migration_plan") == plan_id and state["state"] == "recovering":
                            state.update(state="awaiting_user_confirmation",
                                         recovery_failure={"type": type(exc).__name__, "at": time.time()})
                            self.record(saved["run_id"], state)
            raise

    def _recover(self, plan_id: str, *, confirm: str, old_node_stopped_receipt: Path) -> dict:
        """One confirmed, restartable transaction. No provider API mutations.

        Legacy jobs have no cross-node guardian. An independently obtained
        provider Stopped/readback receipt is mandatory for them, not a guessed
        consequence of CUDA failure or SSH loss.
        """
        self.guard()
        plan = self.confirmed_plan(plan_id, confirm)
        run, spec = plan["run_id"], self.spec(plan["run_id"])
        fence = json.loads(private_file(old_node_stopped_receipt).read_bytes())
        if (fence.get("run_id") != run or fence.get("source_generation") != plan["source_generation"]
                or fence.get("old_node_stopped_verified") is not True
                or fence.get("evidence_kind") not in {"provider_stopped_readback", "guarded_owned_processes_gone"}
                or fence.get("source_identity") != plan["source_identity"] or not fence.get("observed_at")
                or not 0 <= time.time() - float(fence["observed_at"]) <= 300):
            raise RescueError("fresh old-worker fencing proof required; GPU loss is not Stop proof")
        if (fence["evidence_kind"] == "provider_stopped_readback"
                and (not plan["source_identity"].get("instance_id")
                     or fence.get("actual_status") != "stopped"
                     or str(fence.get("instance_id")) != plan["source_identity"]["instance_id"])):
            raise RescueError("provider readback must prove the exact old instance is stopped")
        if (fence["evidence_kind"] == "guarded_owned_processes_gone"
                and (not self.journal(run).get("active_target")
                     or fence.get("owned_processes_gone_verified") is not True)):
            raise RescueError("legacy unsupervised jobs need a provider stopped readback")
        state = self.journal(run)
        previous_plan = state.get("active_target", {}).get("plan_id")
        if state.get("migration_plan") not in (None, plan_id, previous_plan):
            raise RescueError("another migration owns this run")
        state.update(state="recovering", generation=plan["destination_generation"], migration_plan=plan_id,
                     old_source_fence_sha256=file_hash(old_node_stopped_receipt))
        if previous_plan and previous_plan != plan_id:
            state["previous_target"] = state.pop("active_target")
        self.record(run, state)  # Fence durable before any destination launch.
        ssh = self.target_ssh(plan)
        ssh.run(["bash", "-c", 'command -v python3 >/dev/null || { test "$(id -u)" = 0 && apt-get update && apt-get install -y python3; }'], timeout=300)
        ssh.install_adapter()
        bootstrap_path = self.cold / "bootstrap" / run / (plan["bootstrap_id"] + ".json")
        bootstrap = json.loads(bootstrap_path.read_bytes())
        if digest(bootstrap) != plan["bootstrap_id"]:
            raise RescueError("bootstrap manifest no longer matches approval")
        request = {"runtime_platform": bootstrap["runtime_platform"], "runtime_prefix": spec["runtime_prefix"],
                   "archive_sha256": bootstrap["runtime_archive"]["sha256"],
                   "minimum_free_bytes": self.policy["minimum_destination_free_bytes"]}
        ssh.json(["python3", REMOTE_ADAPTER, "target-preflight"], input=json.dumps(request).encode())
        # Compiled training requires userspace C++ tooling, not a host-driver
        # reinstall. Only install absent standard tools in this approved target.
        ssh.run(["bash", "-c", "command -v g++ >/dev/null && command -v make >/dev/null || "
                 "{ test \"$(id -u)\" = 0 && apt-get update && apt-get install -y --no-install-recommends build-essential; }"], timeout=600)
        incoming = Path(REMOTE_STATE) / run / str(state["generation"])
        # Fixed-prefix runtime restores into a temporary sibling, then atomically
        # activates only after complete file/hash verification. No solver/download.
        runtime_row = bootstrap["runtime_archive"]
        runtime_in = incoming / "runtime.tar.gz"
        self.push_object(ssh, runtime_row, runtime_in)
        self.push_object(ssh, bootstrap["runtime_archive_proof"], incoming / "runtime-proof.json")
        ssh.adapter("restore-runtime", "--path", str(runtime_in), "--proof", str(incoming / "runtime-proof.json"),
                    "--output", spec["runtime_prefix"], timeout=1800)
        for row in bootstrap["files"]:
            self.push_object(ssh, row, Path(row["target_path"]))
        # Source ZIP is the canonical frozen release. Extract only its declared
        # code members; never git-reset a target or overwrite its working tree.
        for release in spec.get("source_releases", [{"zip_role": "source_zip", "receipt": spec["source_receipt"], "root": spec["source_root"]}]):
            source_zip = next(r for r in bootstrap["files"] if r.get("role") == release["zip_role"])
            ssh.adapter("restore-source", "--path", source_zip["target_path"], "--proof", release["receipt"],
                        "--output", release["root"], timeout=180)
        runtime_probe = (
            "import sys,json; sys.path.insert(0,sys.argv[1]); from stockagent.runtime_identity import runtime_identity; "
            "print(json.dumps(runtime_identity()))"
        )
        restored_runtime = ssh.json([spec["runtime_prefix"] + "/bin/python", "-c", runtime_probe, spec["source_root"]])
        differences = validate_runtime_lock(spec["runtime_lock"], restored_runtime)
        if differences:
            raise RescueError("restored selected runtime lock differs: " + "; ".join(differences[:5]))
        remote_spec = Path(REMOTE_STATE) / run / "spec.json"
        ssh.upload(remote_spec, json.dumps(spec).encode())
        # Reject a broken new host before downloading/rebuilding large inputs.
        check = ["bash", "-c", 'cd "$1"; source scripts/runtime_env.sh; exec_fintech_python scripts/check_environment.py --require-cuda --minimum-cuda-devices "$2" --strict',
                 "rescue-cuda-preflight", spec["source_root"], str(spec["world_size"])]
        ssh.run(["env", "PYTHON_BIN=" + spec["runtime_prefix"] + "/bin/python",
                 "FINTECH_ENV_PATH=" + spec["runtime_prefix"], "CUDA_VISIBLE_DEVICES=" + ",".join(map(str, range(spec["world_size"]))), *check], timeout=180)
        self.restore_inputs(ssh, spec, incoming)
        commit_path = self.cold / "commits" / run / (plan["commit_id"] + ".json")
        commit = json.loads(commit_path.read_bytes())
        if digest(commit) != plan["commit_id"]:
            raise RescueError("checkpoint commit manifest differs from the confirmed recovery point")
        for row in commit["inventory"]["files"]:
            self.push_object(ssh, row, Path(spec["output_root"]) / relative(row["relative"]))
        restored = ssh.adapter("inspect", "--spec", str(remote_spec), python=spec["runtime_prefix"] + "/bin/python", timeout=180)
        if restored["context_sha256"] != commit["inventory"]["context_sha256"]:
            raise RescueError("restored canonical checkpoint/context is not exact")
        state["target_boot_id"] = restored["source_boot_id"]
        state["target_identity"] = ssh.adapter("health")["node_identity"]
        state["active_target"] = {"plan_id": plan_id, "baseline_epochs": {
            k: v["epoch"] for k, v in restored["checkpoints"].items()}}
        self.record(run, state)
        generation_path = incoming / "supervisor.json"
        self.install_control_key(plan, ssh, remote_spec, generation_path)
        # Restore can take minutes/hours. Do not reuse the initial fresh receipt
        # as a current pre-launch Stop claim after a long bootstrap.
        if fence["evidence_kind"] == "provider_stopped_readback":
            key_file = fence.get("readback_api_key_file")
            fresh = self.fence_provider(run, api_key_file=Path(key_file) if key_file else None,
                                       identity=plan["source_identity"], generation=plan["source_generation"])
            state["prelaunch_old_node_fence"] = fresh["receipt"]
        else:
            old_plan = json.loads((self.state / "plans" / (state["previous_target"]["plan_id"] + ".json")).read_bytes())
            old_status = self.control_ssh(old_plan).adapter("status")
            if (old_status.get("node_identity") != plan["source_identity"]
                    or old_status.get("owned_processes_gone_verified") is not True
                    or old_status.get("owned_guardian_gone_verified") is not True):
                raise RescueError("old guarded worker fencing no longer verifies before launch")
        self.record(run, state)
        self.renew_target(run, state)
        # Detached only through a small owned guardian; canonical training still
        # runs foreground below it. The generation lock prevents double launch.
        launch_proof = ssh.adapter("launch", "--spec", str(remote_spec), "--output", str(incoming),
            "--generation", str(state["generation"]), timeout=120)
        state["state"] = "recovering"
        state["launch"] = launch_proof
        state["launch"]["launched_at"] = time.time()
        self.record(run, state)
        return {"run_id": run, "state": "target_restored_guardian_launched", "plan_id": plan_id,
                "resume_progress_verified": False, "old_node_fenced": True, "provider_destroy_called": False}

    def restore_inputs(self, ssh: SSH, spec: dict, incoming: Path) -> None:
        """Transfer only pinned source objects already in D; build views remotely."""
        from stockagent.data_sync.packed_snapshots import resolve_packed_snapshot_id
        from stockagent.data_sync.windows_cold_io import binary_reader
        cold = Path("/srv/stockagent-packed")
        for binding in spec.get("data_sources", []):
            resolved = resolve_packed_snapshot_id(cold, binding["dataset"], binding["snapshot_id"])
            if resolved.manifest_sha256 != binding["manifest_sha256"]:
                raise RescueError("training source release changed; do not follow latest")
            inventory = resolved.manifest["archive"]["inventory"]
            members = [*resolved.manifest["archive"]["objects"], inventory]
            for row in members:
                path = cold / relative(row["relpath"])
                destination = Path("/srv/stockagent-packed") / row["relpath"]
                self.push_cold_source(ssh, path, row, destination)
            self.push_cold_source(ssh, resolved.manifest_path,
                {"sha256": resolved.manifest_sha256, "bytes": resolved.manifest_path.stat().st_size},
                Path("/srv/stockagent-packed") / resolved.manifest_path.relative_to(cold))
            script = (
                "import sys,pathlib,json; sys.path.insert(0,sys.argv[1]); "
                "from stockagent.data_sync.packed_snapshots import resolve_packed_snapshot_id,fetch_packed_snapshot; "
                "root=pathlib.Path('/srv/stockagent-packed'); r=resolve_packed_snapshot_id(root,sys.argv[2],sys.argv[3]); "
                "assert r.manifest_sha256==sys.argv[4]; "
                "p=fetch_packed_snapshot(root,pathlib.Path(sys.argv[5]),r); print(json.dumps({'materialized':str(p)}))"
            )
            ssh.json([spec["runtime_prefix"] + "/bin/python", "-c", script, spec["source_root"],
                      binding["dataset"], binding["snapshot_id"], binding["manifest_sha256"],
                      binding["materialized_root"]], timeout=3600)
        # CPU-only original builders. This is not permission to change the data
        # contract or permanently store prepared training views on penguin.
        for index, step in enumerate(spec.get("rebuild_steps", [])):
            remote_spec = str(Path(REMOTE_STATE) / spec["run_id"] / "spec.json")
            admission = ssh.adapter("prepare-begin", "--spec", remote_spec, "--index", str(index),
                python=spec["runtime_prefix"] + "/bin/python", timeout=300)
            if admission.get("skip_builder"):
                continue
            source_root = spec.get("rebuild_source_roots", [spec["source_root"]] * len(spec["rebuild_steps"]))[index]
            command = ["bash", "-c", 'cd "$1"; shift; source scripts/runtime_env.sh; exec_fintech_python "$@"',
                       "stockagent-rescue-prepare", source_root, *step]
            prefix = spec["runtime_prefix"]
            ssh.run(["env", "PYTHON_BIN=" + prefix + "/bin/python", "FINTECH_ENV_PATH=" + prefix, *command], timeout=7200)
            ssh.adapter("prepare-finish", "--spec", remote_spec, "--index", str(index),
                        python=prefix + "/bin/python", timeout=300)

    def push_cold_source(self, ssh: SSH, source: Path, row: dict, destination: Path) -> None:
        """Same SHA/flush boundary, without making a duplicate rescue CAS object."""
        from stockagent.data_sync.windows_cold_io import binary_reader, hash_file
        count = row.get("size", row.get("bytes"))
        if source.stat().st_size != count or hash_file(source) != row["sha256"]:
            raise RescueError("pinned cold source failed original-byte readback")
        with binary_reader(source) as stream, tempfile.TemporaryFile() as errors:
            process = subprocess.Popen(ssh.command(["python3", REMOTE_ADAPTER, "receive", "--path", str(destination),
                "--sha256", row["sha256"], "--bytes", str(count)]), stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=errors)
            try:
                try:
                    shutil.copyfileobj(stream, process.stdin, 4 * 1024**2); process.stdin.close()
                except BrokenPipeError:
                    pass
                result = process.stdout.read(16384)
                if process.wait(timeout=60):
                    raise RescueError("target cold-source transfer failed")
                value = json.loads(result)
                if value.get("sha256") != row["sha256"] or value.get("bytes") != count:
                    raise RescueError("target cold-source readback differs")
            finally:
                if process.poll() is None:
                    process.terminate(); process.wait(timeout=20)
                process.stdout.close()

    def renew_target(self, run: str, state: dict) -> None:
        with (self.path(run) / "lease-send.lock").open("a") as stream:
            fcntl.flock(stream, fcntl.LOCK_EX)
            fresh = self.journal(run)
            if (fresh["generation"] != state["generation"] or fresh["state"] not in {"recovering", "resume_progress_verified"}
                    or fresh.get("active_target") != state.get("active_target")):
                raise RescueError("generation stopped/fenced while preparing heartbeat")
            self._renew_target(run, state)

    def _renew_target(self, run: str, state: dict) -> None:
        plan = json.loads((self.state / "plans" / (state["active_target"]["plan_id"] + ".json")).read_bytes())
        spec = self.spec(run)
        ssh = self.control_ssh(plan)
        ttl = self.policy["lease_seconds"]
        lease = {"contract": CONTRACT, "run_id": run, "spec_sha256": digest(spec),
                 "generation": state["generation"], "sequence": time.time_ns(),
                 "boot_id": state["target_boot_id"], "issued_at": time.time(),
                 "expires_at": time.time() + ttl, "ttl_seconds": ttl}
        # Lease overwrites are the one explicitly mutable worker control file.
        ssh.rpc({"action": "lease", "lease": lease}, timeout=20)

    def watch_target(self, run: str) -> dict:
        state = self.journal(run)
        plan_id = state["active_target"]["plan_id"]
        plan = json.loads((self.state / "plans" / (plan_id + ".json")).read_bytes())
        ssh = self.control_ssh(plan)
        health = ssh.adapter("health", timeout=20)
        state["health"] = {**health, "observed_at": time.time()}
        fault = (health.get("state") == "gpu_fault"
                 or len(health.get("devices", [])) < self.spec(run)["world_size"]
                 or health.get("node_identity") != state.get("target_identity"))
        if fault:
            state["state"] = "awaiting_user_confirmation"
            self.record(run, state)
            # CPU-readable recovery points may still be preserved. Lease renewal
            # stops immediately; the guardian fences its own process group.
            result = self.capture(run, transport=ssh)
            return {**result, "state": "awaiting_user_confirmation", "resume_progress_verified": False,
                    "health": health, "automatic_migration": False}
        self.record(run, state)
        status = ssh.adapter("status")
        if (status.get("state") == "fenced_or_failed"
                or (status.get("state") == "child_exited" and status.get("exit_code"))
                or (status.get("owned_guardian_gone_verified") and status.get("state") != "child_exited")
                or (status.get("state") == "not_started"
                    and time.time() - state.get("launch", {}).get("launched_at", time.time()) > 190)):
            state.update(state="awaiting_user_confirmation", target_status=status)
            self.record(run, state)
            return {"run_id": run, "state": state["state"], "target_status": status}
        errors, stop = [], threading.Event()

        def heartbeat():
            while not stop.wait(min(20, self.policy["lease_seconds"] // 3)):
                try:
                    self.renew_target(run, state)
                except Exception as exc:
                    errors.append(type(exc).__name__); return

        worker = threading.Thread(target=heartbeat, daemon=True)
        worker.start()
        try:
            result = self.capture(run, transport=ssh)
        finally:
            stop.set(); worker.join(timeout=25)
        if errors:
            raise RescueError("destination owner lease renewal failed")
        state = self.journal(run)
        baseline = state["active_target"]["baseline_epochs"]
        progressed = any(v["epoch"] > baseline.get(k, v["epoch"])
                         for k, v in state.get("latest", {}).get("checkpoints", {}).items())
        if progressed:
            state["state"] = "resume_progress_verified"
            state["resume_acceptance"] = {"new_full_checkpoint_in_D": True,
                "after_original_epochs": baseline, "verified_at": time.time(), "cross_hardware_bitwise_claim": False}
            self.record(run, state)
            key_path = self.state / "keys" / plan["key_id"] / "key.json"
            key = json.loads(key_path.read_bytes())
            if key["state"] != "revoked":
                ssh.rpc({"action": "revoke-bootstrap"})
                key["state"] = "revoked"
                key["revoked_after_verified_progress_at"] = time.time()
                atomic_json(key_path, key)
        return {**result, "resume_progress_verified": progressed, "target_status": status}

    def revoke_key(self, key_id: str) -> dict:
        path = self.state / "keys" / identifier(key_id) / "key.json"
        key = json.loads(path.read_bytes())
        remote_revoked = False
        if key.get("bound_plan") and (path.parent / "id_control").exists():
            plan = json.loads((self.state / "plans" / (key["bound_plan"] + ".json")).read_bytes())
            try:
                self.control_ssh(plan).rpc({"action": "revoke-bootstrap"})
                remote_revoked = True
            except (RescueError, OSError, ValueError, subprocess.TimeoutExpired):
                pass  # Still revoke LOCAL use; do not fake a remote acceptance.
        key["state"] = "revoked" if remote_revoked else "revoked_locally_remote_authorization_unverified"
        key["revoked_at"] = time.time()
        atomic_json(path, key)
        return {"key_id": key_id, "state": key["state"], "remote_key_revoked": remote_revoked,
                "remove_unbootstrapped_bare_key_in_Vast_UI": not remote_revoked,
                "ongoing_control_key_unchanged": True,
                "private_key_exported": False}

    def status(self) -> dict:
        rows = []
        for p in sorted((self.state / "runs").glob("*/state.json")):
            state = json.loads(p.read_bytes())
            latest = state.get("latest", {})
            rows.append({"run_id": state["run_id"], "state": state["state"], "generation": state["generation"],
                "checkpoint_epochs": {k: v["epoch"] for k, v in latest.get("checkpoints", {}).items()},
                "preserved_bytes": latest.get("bytes"), "latest_ack_at": latest.get("committed_at"),
                "ack_age_seconds": time.time() - latest["committed_at"] if latest else None,
                "bootstrap_preserved": bool(state.get("bootstrap")), "health": state.get("health"),
                "pinned_sources_readback_verified": state.get("input_verification", {}).get("level") == "all_original_file_SHA_decoded",
                "source_lifecycle_state": state.get("source_lifecycle_state", "unknown"),
                "recovery_prepared": bool(latest and state.get("bootstrap") and state.get("source_identity")
                    and (not self.spec(state["run_id"]).get("data_sources")
                         or state.get("input_verification", {}).get("level") == "all_original_file_SHA_decoded")),
                "last_failure": state.get("last_failure"), "resume_progress_verified": state["state"] == "resume_progress_verified"})
        return {"contract": CONTRACT, "runs": rows, "automatic_migration": False,
                "destination_selected_by": "user", "source_files_deleted": False}

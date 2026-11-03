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
import io
import json
import os
from pathlib import Path
import re
import shlex
import shutil
import stat
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
from stockagent.training_rescue_bootstrap import (
    BOOTSTRAP_BINDING_V2, MAX_BOOTSTRAP_CONTROL_BYTES, attachment_request,
    bootstrap_runtime_lock, decode_runtime_capsule, validate_bootstrap_binding,
)
from stockagent.training_rescue_worker import (
    CONTRACT, HASH, absolute, atomic_json, digest, file_hash, identifier,
    relative, validate_spec, preparation_references, preparation_resource_bindings, signature, MAX_PREPARED_REFERENCE_BYTES,
    MAX_PRESERVED_AUXILIARY_BYTES, MAX_PRESERVED_FILE_SET_BYTES,
    input_capsule_proof, decode_input_capsule,
    source_aliases,
    input_control_bindings,
    _prepared_lock,
)

ROOT = Path(__file__).resolve().parents[1]
WORKER = ROOT / "stockagent/training_rescue_worker.py"
CACHE_CONTROLLER = ROOT / "stockagent/data_sync/materialized_cache.py"
REMOTE_ADAPTER = "/opt/stockagent-training-rescue/worker.py"
REMOTE_STATE = "/var/lib/stockagent-training-rescue"
SOURCE_IDENTITY_REASON = "source_identity_changed_requires_reenrollment"
SOURCE_POLL_GATE_CONTRACT = "stockagent.source_enrollment_poll_gate.v1"


class RescueError(RuntimeError):
    pass


class RescueCapacityWait(RescueError):
    """Expected D admission wait; never a failed checkpoint/runtime proof."""


class SourceIdentityChanged(RescueError):
    def __init__(self):
        super().__init__("source SSH profile now points to another boot/instance; explicit re-enrollment required")


def source_poll_gate_active(spec: dict, state: dict) -> bool:
    """A verified wrong node needs an enrollment event, not timed SSH retries.

    Bind this negative polling decision to the exact original enrollment and
    generation. It never proves data health or fences/launches a destination.
    Legacy unbound failures still receive one fresh identity check.
    """
    failure = state.get("last_failure") or {}
    return bool(not state.get("active_target") and state.get("source_identity")
        and failure.get("type") == "SourceIdentityChanged"
        and failure.get("reason") == SOURCE_IDENTITY_REASON
        and state.get("source_poll_gate") == {
            "contract": SOURCE_POLL_GATE_CONTRACT,
            "spec_sha256": digest(spec), "source_identity": state["source_identity"],
            "generation": state["generation"], "reason": SOURCE_IDENTITY_REASON})


def _clear_source_poll_gate(state: dict) -> None:
    state.pop("source_poll_gate", None)
    failure = state.get("last_failure") or {}
    if failure.get("type") == "SourceIdentityChanged" and failure.get("reason") == SOURCE_IDENTITY_REASON:
        state.pop("last_failure", None)


class SSHOperationError(RescueError):
    """Private bounded diagnostics, never included in public exception text."""
    def __init__(self, returncode: int, stderr: bytes):
        self.returncode = returncode
        self.private_stderr = stderr[-16384:]
        super().__init__(f"SSH operation failed (exit={returncode})")


def input_source_readback_ready(spec: dict, state: dict) -> bool:
    """A checkpoint-only enrollment is not a prepared input recovery plan."""
    proof = state.get("input_verification", {})
    if (not (spec.get("data_sources") or spec.get("data_capsules")) or proof.get("spec_sha256") != digest(spec)
            or proof.get("level") != "all_original_file_SHA_decoded"):
        return False
    try:
        resources = preparation_resource_bindings(spec)
    except (KeyError, TypeError, ValueError):
        return False
    expected = {row["path"]: row["sha256"] for row in resources}
    capsules = {row["archive_sha256"]: row["proof_sha256"] for row in spec.get("data_capsules", [])}
    return ((not expected or proof.get("prepared_reference_sha256") == expected)
            and (not capsules or proof.get("capsule_proof_sha256") == capsules))


def bootstrap_declaration_gaps(spec: dict, *, preservation_only: bool = False) -> list[str]:
    """Describe missing enrollment operands without IO or readiness claims.

    Checkpoint-only enrollment remains valid. These declarations are necessary
    for bootstrap preservation/recovery; they are not full-byte D proofs.
    """
    gaps = []
    capsule = spec.get("runtime_capsule_path")
    if not isinstance(capsule, str) or "\x00" in capsule or not Path(capsule).is_absolute():
        gaps.append("runtime_capsule_path")
    files = spec.get("bootstrap_files")
    if not isinstance(files, list) or not files:
        gaps.append("bootstrap_files")
        files = []
    elif any(not isinstance(row, dict)
             or not all(isinstance(row.get(k), str) and row[k] and "\x00" not in row[k]
                        for k in ("source_path", "target_path", "role", "sha256"))
             or not HASH.fullmatch(row["sha256"])
             or type(row.get("bytes")) is not int or row["bytes"] < 0
             for row in files):
        gaps.append("bootstrap_file_metadata")
        files = []
    if preservation_only:
        return gaps
    if not isinstance(spec.get("runtime_lock"), dict) or not spec["runtime_lock"]:
        gaps.append("runtime_lock")
    if not any(row["target_path"] == spec.get("config_file") for row in files):
        gaps.append("training_config_binding")
    releases = spec.get("source_releases", [{"zip_role": "source_zip", "receipt": spec.get("source_receipt")}])
    if (not isinstance(releases, list) or not releases
            or any(not isinstance(release, dict)
                   or not isinstance(release.get("zip_role"), str)
                   or not isinstance(release.get("receipt"), str)
                   or not any(row["role"] == release["zip_role"] for row in files)
                   or not any(row["target_path"] == release["receipt"] for row in files)
                   for release in releases)):
        gaps.append("source_release_bindings")
    return gaps


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
                    "ControlMaster=no", "-o", "ControlPath=none", "-o", "ControlPersist=no", "-o",
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
            raise SSHOperationError(result.returncode, result.stderr)
        if len(result.stdout) > 64 * 1024**2:
            raise RescueError("remote control response exceeded its bound")
        return result.stdout

    def json(self, argv: list[str], **kwargs) -> dict:
        return json.loads(self.run(argv, **kwargs))

    def node_identity(self) -> dict:
        """Read the host identity before calling a possibly absent old adapter."""
        script = (
            "import json,os,pathlib,re; i=os.environ.get('CONTAINER_ID',''); "
            "print(json.dumps({'boot_id':pathlib.Path('/proc/sys/kernel/random/boot_id').read_text().strip(),"
            "'instance_id':i if re.fullmatch(r'[0-9]+',i) else None}))"
        )
        value = self.json(["python3", "-c", script], timeout=20)
        if (not isinstance(value, dict) or set(value) != {"boot_id", "instance_id"}
                or not re.fullmatch(r"[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}", str(value.get("boot_id", "")))
                or (value["instance_id"] is not None
                    and (not isinstance(value["instance_id"], str) or not re.fullmatch(r"[0-9]+", value["instance_id"])))):
            raise RescueError("source host identity response is invalid")
        return value

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
            raise RescueCapacityWait("D reserve reached; never fall back to C")

    def path(self, run: str) -> Path:
        return self.state / "runs" / identifier(run)

    def spec(self, run: str) -> dict:
        return validate_spec(json.loads((self.path(run) / "spec.json").read_bytes()))

    def _recipe_pointer(self, run: str) -> dict | None:
        from stockagent.training_rescue_worker import _prepared_control
        path = absolute(str(self.path(run) / "input-recipe.json"))
        if not path.exists():
            return None
        row = _prepared_control(path)
        if (set(row) != {"contract", "revision_sha256", "base_spec_sha256"}
                or row["contract"] != "stockagent.training_input_recipe_pointer.v1"
                or not isinstance(row["revision_sha256"], str) or not HASH.fullmatch(row["revision_sha256"])
                or row["base_spec_sha256"] != digest(self.spec(run))):
            raise RescueError("input recipe pointer differs from original enrollment")
        return row

    def recovery_spec(self, run: str, *, revision_sha256: str | None = None) -> dict:
        """The target's pinned input revision; source capture keeps its base spec."""
        from stockagent.training_rescue_recipes import apply_input_recipe
        from stockagent.training_rescue_worker import _prepared_control
        base = self.spec(run)
        if revision_sha256 is None:
            pointer = self._recipe_pointer(run)
            if pointer is None:
                return base
            revision_sha256 = pointer["revision_sha256"]
        if not isinstance(revision_sha256, str) or not HASH.fullmatch(revision_sha256):
            raise RescueError("invalid pinned input recipe revision")
        path = self.cold / "input-recipes" / identifier(run) / (revision_sha256 + ".json")
        return apply_input_recipe(base, _prepared_control(path), revision_sha256)

    def execution_spec(self, run: str) -> dict:
        """A running target follows its approved revision, never a moving pointer."""
        target = self.journal(run).get("active_target")
        if not target:
            return self.spec(run)
        plan_id = target["plan_id"]
        if not HASH.fullmatch(plan_id):
            raise RescueError("active target plan identity is invalid")
        plan = json.loads((self.state / "plans" / (plan_id + ".json")).read_bytes())
        if digest(plan) != plan_id or plan["run_id"] != run:
            raise RescueError("active target plan changed")
        revision = plan.get("input_recipe_revision_sha256")
        spec = self.recovery_spec(run, revision_sha256=revision) if revision else self.spec(run)
        if digest(spec) != plan["spec_sha256"]:
            raise RescueError("active target effective spec differs from its approved plan")
        return spec

    def register_input_recipe(self, candidate: dict) -> dict:
        """Attach immutable recovery dependencies through this same owner only.

        No source login/upload, original enrollment rewrite, target contact,
        READY declaration or GPU launch occurs. Full D decode remains required.
        """
        from stockagent.training_rescue_recipes import input_recipe, apply_input_recipe
        from stockagent.training_rescue_worker import _prepared_control
        run = identifier(candidate["run_id"])
        original_path = absolute(str(self.path(run) / "spec.json"))
        original_signature = signature(original_path)
        base = self.spec(run)
        revision = input_recipe(base, candidate)
        state = self.journal(run)
        if state.get("active_target") or state.get("migration_plan") or state["state"] in {"recovering", "resume_progress_verified"}:
            raise RescueError("an active target/migration pins its existing input recipe")
        self.guard()
        self.prepared_reference_objects(candidate)
        self.input_capsule_objects(candidate)
        revision_sha = digest(revision)
        effective = apply_input_recipe(base, revision, revision_sha)
        pointer = {"contract": "stockagent.training_input_recipe_pointer.v1", "revision_sha256": revision_sha,
                   "base_spec_sha256": digest(base)}
        previous = self._recipe_pointer(run)
        record = absolute(str(self.cold / "input-recipes" / run / (revision_sha + ".json")))
        if record.exists():
            if _prepared_control(record) != revision:
                raise RescueError("preserve a conflicting immutable D input recipe")
        else:
            atomic_json(record, revision)
        if digest(_prepared_control(record)) != revision_sha:
            raise RescueError("D input recipe independent readback differs")
        if signature(original_path) != original_signature or digest(self.spec(run)) != digest(base):
            raise RescueError("original enrollment changed while attaching input recipe")
        if previous != pointer:
            atomic_json(self.path(run) / "input-recipe.json", pointer)
        if self.recovery_spec(run) != effective:
            raise RescueError("input recipe pointer readback differs")
        return {"run_id": run, "state": "input_recipe_registered_requires_full_source_verification",
                "input_recipe_revision_sha256": revision_sha, "effective_spec_sha256": digest(effective),
                "original_spec_sha256": digest(base), "reused": previous == pointer,
                "original_enrollment_modified": False, "source_contacted": False, "training_started": False}

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

    @staticmethod
    def check_source_identity(ssh: SSH, state: dict) -> None:
        expected = state.get("source_identity")
        if expected and ssh.node_identity() != expected:
            raise SourceIdentityChanged()

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
        self.check_source_identity(ssh, state)
        ssh.install_adapter()
        ssh.upload(Path(REMOTE_STATE) / run / "spec.json", json.dumps(spec).encode())
        observed = ssh.adapter("health", timeout=20)
        identity = observed.get("node_identity")
        if identity:
            if state.get("source_identity") and identity != state["source_identity"]:
                raise SourceIdentityChanged()
            state["source_identity"] = identity
            state["health"] = {**observed, "observed_at": time.time()}
            _clear_source_poll_gate(state)
            self.record(run, state)
        return {"run_id": run, "state": state["state"], "training_started": False}

    def object_path(self, sha: str) -> Path:
        if not HASH.fullmatch(sha):
            raise RescueError("invalid rescue object digest")
        return self.cold / "objects" / sha[:2] / sha

    def object_hash(self, path: Path) -> str:
        from stockagent.data_sync.windows_cold_io import hash_file
        return hash_file(path)

    def validate_member(self, row: dict) -> Path:
        count = row["bytes"]
        if type(count) is not int or not 0 <= count <= self.policy["maximum_member_bytes"]:
            raise RescueError("rescue member exceeds the admitted bound")
        return self.object_path(row["sha256"])

    @contextmanager
    def object_lock(self, sha: str):
        self.object_path(sha)
        root = absolute(str(self.state / "transfers"))
        root.mkdir(mode=0o700, exist_ok=True)
        fd = os.open(root / (sha + ".lock"), os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        with os.fdopen(fd, "a+b") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            yield

    def put_stream(self, stream, row: dict, *, process=None) -> dict:
        self.validate_member(row)
        with self.object_lock(row["sha256"]):
            return self._write_stream(stream, row, process=process)

    def _write_stream(self, stream, row: dict, *, process=None, errors=None,
                      partial: Path | None = None, offset: int = 0,
                      prefix_sha256: str | None = None) -> dict:
        from stockagent.data_sync.windows_cold_io import BinaryWriter, windows_path
        self.guard()
        sha, count = row["sha256"], row["bytes"]
        destination = self.validate_member(row)
        if destination.exists():
            if destination.stat().st_size != count or self.object_hash(destination) != sha:
                raise RescueError("existing D rescue object is corrupt; preserve and reject")
            return {"sha256": sha, "bytes": count, "reused": True}
        destination.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
        partial = partial or destination.with_name(sha + ".partial." + uuid.uuid4().hex)
        absolute(str(partial))
        h, size, writer = hashlib.sha256(), offset, None
        try:
            if windows_path(partial) is not None:
                writer = BinaryWriter(partial, offset=offset, prefix_sha256=prefix_sha256,
                                      reserve_bytes=self.policy["reserve_bytes"])
            elif prefix_sha256 is not None:
                fd = os.open(partial, os.O_RDWR | os.O_NOFOLLOW)
                writer = os.fdopen(fd, "r+b")
                if (not stat.S_ISREG(os.fstat(fd).st_mode) or os.fstat(fd).st_size != offset
                        or self.object_hash(partial) != prefix_sha256):
                    raise RescueError("retained D prefix changed; no append permitted")
                writer.seek(offset)
            else:
                writer = partial.open("xb")
            for block in iter(lambda: stream.read(4 * 1024**2), b""):
                size += len(block)
                if size > count:
                    raise RescueError("incoming rescue object exceeds its sealed size")
                writer.write(block)
                h.update(block)
            if process is not None:
                code = process.wait(timeout=30)
                if code:
                    if errors is not None:
                        errors.seek(0, os.SEEK_END)
                        errors.seek(max(0, errors.tell() - 16384))
                        raise SSHOperationError(code, errors.read(16384))
                    raise RescueError("remote source changed/failed during object transfer")
            if size != count or (not offset and h.hexdigest() != sha):
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
            return {"sha256": sha, "bytes": count, "D_readback_verified": True,
                    "resumed_bytes": offset, "transferred_bytes": count - offset}
        finally:
            if writer is not None:
                writer.close()
            # Failed partials are retained, never promoted or removed silently.

    def pull(self, ssh: SSH, root: Path, row: dict, *, python="python3",
             retained_partial: Path | None = None) -> dict:
        """Resume only an enrolled exact prefix; full D readback still commits.

        Unrecorded legacy partials require an explicit exact-name adoption. No
        directory discovery, source deletion, truncation or blind append occurs.
        A failed transfer retains one durable intent, so retries reuse its bytes.
        """
        from io import BytesIO

        destination = self.validate_member(row)
        self.guard()
        with self.object_lock(row["sha256"]):
            if destination.exists():
                return self._write_stream(BytesIO(), row)
            destination.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
            intent_path = absolute(str(self.cold / "incoming" / (row["sha256"] + ".json")))
            if intent_path.exists():
                if intent_path.stat().st_size > 32768:
                    raise RescueError("rescue transfer intent exceeds its metadata bound")
                intent = json.loads(intent_path.read_bytes())
                if (intent.get("contract") != "stockagent.rescue_transfer.v1"
                        or intent.get("sha256") != row["sha256"] or intent.get("bytes") != row["bytes"]):
                    raise RescueError("immutable transfer intent identity differs")
                partial = destination.with_name(intent["partial"])
                if retained_partial is not None and retained_partial != partial:
                    raise RescueError("another retained partial already owns this transfer")
            else:
                partial = retained_partial or destination.with_name(row["sha256"] + ".partial." + uuid.uuid4().hex)
                intent = {"contract": "stockagent.rescue_transfer.v1", "sha256": row["sha256"],
                          "bytes": row["bytes"], "partial": partial.name, "created_at": time.time()}
            if (partial.parent != destination.parent
                    or not re.fullmatch(re.escape(row["sha256"]) + r"\.partial\.[a-f0-9]{32}", partial.name)):
                raise RescueError("retained transfer must be the exact owned D partial")
            absolute(str(partial))
            offset, prefix = 0, None
            try:
                info = partial.lstat()
            except FileNotFoundError:
                if retained_partial is not None:
                    raise RescueError("explicit retained partial is absent")
            else:
                if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_size > row["bytes"]:
                    raise RescueError("retained transfer is not a bounded single-owner regular file")
                if retained_partial is not None:
                    from stockagent.data_sync.materialized_cache import process_references_many
                    if process_references_many((partial,), include_current_process=True,
                                               inode_keys={(info.st_dev, info.st_ino)}):
                        raise RescueError("legacy partial still has process references; no adoption permitted")
                offset, prefix = info.st_size, self.object_hash(partial)
                after = partial.lstat()
                fields = ("st_dev", "st_ino", "st_size", "st_mtime_ns", "st_ctime_ns")
                if any(getattr(info, key) != getattr(after, key) for key in fields):
                    raise RescueError("retained D prefix changed during verification")
            # Persist ownership before opening any producer or new D payload.
            atomic_json(intent_path, {**intent, "resume_offset": offset, "prefix_sha256": prefix})
            request = {**row, "resume_offset": offset, "prefix_sha256": prefix}
            with tempfile.TemporaryFile() as errors:
                process = ssh.open_read(root, request, python=python, errors=errors)
                try:
                    result = self._write_stream(process.stdout, row, process=process, errors=errors,
                                                partial=partial, offset=offset, prefix_sha256=prefix)
                    atomic_json(intent_path, {**intent, **result, "state": "D_original_readback_verified",
                                              "committed_at": time.time()})
                    return result
                except SSHOperationError as exc:
                    atomic_json(intent_path, {**intent, "state": "retained_for_retry", "failed_at": time.time(),
                                              "returncode": exc.returncode,
                                              "private_stderr": exc.private_stderr.decode(errors="replace")[-4096:]})
                    raise
                finally:
                    if process.poll() is None:
                        process.terminate()
                        try:
                            process.wait(timeout=20)
                        except subprocess.TimeoutExpired:
                            process.kill(); process.wait(timeout=20)
                    process.stdout.close()

    def pull_context(self, ssh: SSH, root: Path, rows: list[dict]) -> None:
        """Reuse exact objects, batching fresh full-byte D checks, never old ACKs.

        Native process startup dominated small unchanged context members. The
        existing guarded batch reader verifies at most 16 immutable files per
        invocation; new payloads retain their original transfer/flush/readback.
        """
        from stockagent.data_sync.windows_cold_io import hash_many, windows_path

        self.guard()
        if (not isinstance(rows, list) or not 1 <= len(rows) <= 2048
                or any(not isinstance(row, dict) or type(row.get("bytes")) is not int
                       or not 0 <= row["bytes"] <= self.policy["maximum_member_bytes"] for row in rows)
                or sum(row["bytes"] for row in rows) > 4 * 1024**3):
            raise RescueError("formal recovery context exceeds enrolled bounds")
        objects, names = {}, set()
        for row in rows:
            name = relative(row["relative"])
            if name in names:
                raise RescueError("recovery context repeats a logical member")
            names.add(name)
            path = self.object_path(row["sha256"])
            if path in objects and objects[path]["bytes"] != row["bytes"]:
                raise RescueError("recovery context contradicts an immutable object size")
            objects[path] = row
        native, remaining = [], []
        for path, row in objects.items():
            try:
                info = path.lstat()
            except FileNotFoundError:
                remaining.append(row)
                continue
            if not stat.S_ISREG(info.st_mode) or info.st_size != row["bytes"]:
                raise RescueError("existing D rescue object is corrupt; preserve and reject")
            if windows_path(path) is not None:
                native.append(path)
            else:
                remaining.append(row)
        for offset in range(0, len(native), 16):
            paths = native[offset:offset + 16]
            hashes = hash_many(paths)
            if set(hashes) != set(paths) or any(hashes[path] != objects[path]["sha256"] for path in paths):
                raise RescueError("existing D rescue object is corrupt; preserve and reject")
        for row in remaining:
            self.pull(ssh, root, row)

    def capture(self, run: str, *, transport: SSH | None = None) -> dict:
        self.guard()
        spec, state = self.execution_spec(run), self.journal(run)
        if transport is not None:
            ssh = transport
        elif state.get("active_target"):
            plan = json.loads((self.state / "plans" / (state["active_target"]["plan_id"] + ".json")).read_bytes())
            ssh = self.control_ssh(plan)
        else:
            ssh = self.source(spec)
        if not state.get("active_target"):
            self.check_source_identity(ssh, state)
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
        expected_sealed_root = Path(REMOTE_STATE) / run / "sealed" / inventory["context_sha256"]
        if inventory.get("sealed_root") != str(expected_sealed_root):
            raise RescueError("source returned a seal outside its enrolled private namespace")
        if state.get("latest", {}).get("context_sha256") == inventory["context_sha256"]:
            # An old SSD ACK is not proof that D remains recoverable today.
            self.pull_context(ssh, Path(inventory["sealed_root"]), inventory["files"])
            self.acknowledge_seal(ssh, remote_spec, inventory, state["latest"]["commit_id"])
            _clear_source_poll_gate(state)
            self.record(run, state)
            return {"state": "unchanged_committed_recovery_point", "run_id": run,
                    "checkpoint_epochs": {k: v["epoch"] for k, v in inventory["checkpoints"].items()}}
        rows = inventory["files"]
        if len(rows) > 2048 or sum(r["bytes"] for r in rows) > 4 * 1024**3:
            raise RescueError("formal recovery context exceeds enrolled bounds")
        self.pull_context(ssh, expected_sealed_root, rows)
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
        _clear_source_poll_gate(state)
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
        missing = bootstrap_declaration_gaps(spec, preservation_only=True)
        if missing:
            raise RescueError("bootstrap preservation enrollment is incomplete: " + ", ".join(missing))
        ssh = self.source(spec)
        self.check_source_identity(ssh, state)
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
        self.check_source_identity(ssh, state)
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
                "bootstrap_report_contract": "fixed_source_runtime_preservation_v2",
                "compressed_runtime_bytes": runtime["bytes"], "source_inputs_rebuilt_on_remote": False}

    def _bootstrap_object(self, row: dict) -> bytes:
        """Fresh bounded original-object readback, never a persisted hash cache."""
        from stockagent.data_sync.windows_cold_io import binary_reader
        from stockagent.training_rescue_bootstrap import object_binding
        object_binding(row)
        path = absolute(str(self.object_path(row["sha256"])))
        before = signature(path)
        if (not stat.S_ISREG(before[-1]) or before[2] != row["bytes"]
                or row["bytes"] > MAX_BOOTSTRAP_CONTROL_BYTES):
            raise RescueError("bootstrap control exceeds its bound regular-object size")
        with binary_reader(path, expected_bytes=row["bytes"]) as source:
            raw = source.read(row["bytes"] + 1)
        if (len(raw) != row["bytes"] or hashlib.sha256(raw).hexdigest() != row["sha256"]
                or signature(path) != before):
            raise RescueError("bootstrap original object failed full D SHA/size readback")
        return raw

    def bootstrap(self, run: str, bootstrap_id: str) -> dict:
        from stockagent.training_rescue_worker import _prepared_control
        if not isinstance(bootstrap_id, str) or not HASH.fullmatch(bootstrap_id):
            raise RescueError("invalid bootstrap identity")
        value = _prepared_control(self.cold / "bootstrap" / identifier(run) / (bootstrap_id + ".json"))
        if digest(value) != bootstrap_id:
            raise RescueError("bootstrap manifest differs from its pinned identity")
        return validate_bootstrap_binding(self.spec(run), value, read_object=self._bootstrap_object,
                                          source_identity=self.journal(run).get("source_identity"))

    def bind_bootstrap(self, request: dict, *, owner_wait_seconds: int = 0) -> dict:
        return self._bind_bootstrap(request, owner_wait_seconds=owner_wait_seconds, runtime_readbacks={})

    def bind_bootstrap_batch(self, requests: list[dict], *, owner_wait_seconds: int = 0) -> dict:
        """Share one full D runtime read across an explicit bounded cohort only."""
        if (not isinstance(requests, list) or not 0 < len(requests) <= 16
                or any(not isinstance(r, dict) or not isinstance(r.get("run_id"), str) for r in requests)
                or len({r["run_id"] for r in requests}) != len(requests)):
            raise RescueError("bootstrap batch needs 1..16 distinct explicit run requests")
        runtime_readbacks = {}
        results = [self._bind_bootstrap(r, owner_wait_seconds=owner_wait_seconds, runtime_readbacks=runtime_readbacks)
                   for r in requests]
        return {"state": "bootstrap_cohort_preserved", "runs": results,
                "unique_D_runtime_decodes": len(runtime_readbacks), "GPU_jobs_started": 0}

    def _bind_bootstrap(self, request: dict, *, owner_wait_seconds: int, runtime_readbacks: dict) -> dict:
        """Bind already-preserved runtime/code without rewriting source enrollment.

        The per-run claim spans bounded object reads and source checks. Only the
        initial pin and final latest-journal merge use the capture owner lock.
        This operation never starts, stops or selects a GPU worker.
        """
        from stockagent.data_sync.windows_cold_io import binary_reader
        from stockagent.training_rescue_worker import _prepared_control
        run = identifier(request["run_id"])
        claim = absolute(str(self.state / "bootstrap-binding-locks" / (run + ".lock")))
        with _prepared_lock(claim) as (_, check_claim):
            with self.lock(wait_seconds=owner_wait_seconds):
                self.guard()
                spec, state = self.spec(run), self.journal(run)
                attachment_request(spec, request)
                if (state.get("active_target") or state.get("state") == "recovering"
                        or state.get("source_identity") != request["source_identity"]
                        or not state.get("latest")):
                    raise RescueError("bootstrap attachment requires the preserved original source/checkpoint")
                previous = state.get("bootstrap")
            commit = _prepared_control(self.cold / "commits" / run / (request["checkpoint_commit_id"] + ".json"))
            manifest = [row for row in commit["inventory"]["files"] if row.get("relative") == "run_manifest.json"]
            if len(manifest) != 1:
                raise RescueError("original checkpoint context has no unique runtime provenance manifest")
            rows = [*request["files"], request["runtime_archive"], request["runtime_archive_proof"], manifest[0]]
            generations = {}
            for row in rows:
                path = absolute(str(self.object_path(row["sha256"])))
                before = signature(path)
                if not stat.S_ISREG(before[-1]) or before[2] != row["bytes"]:
                    raise RescueError("bootstrap requires its preserved regular D objects")
                generations[path] = before
            cached = {}

            def read(row):
                key = (row["sha256"], row["bytes"])
                if key not in cached:
                    if sum(size for _, size in cached) + row["bytes"] > 128 * 1024**2:
                        raise RescueError("bootstrap bounded control working set exceeded")
                    cached[key] = self._bootstrap_object(row)
                return cached[key]

            provenance = json.loads(read(manifest[0]))["runtime_provenance"]
            proof = json.loads(read(request["runtime_archive_proof"]))
            body = {"contract": CONTRACT, "bootstrap_contract": BOOTSTRAP_BINDING_V2,
                    "run_id": run, "spec_sha256": digest(spec), "attachment": request,
                    "attachment_sha256": digest(request), "files": request["files"],
                    "runtime_archive": request["runtime_archive"], "runtime_archive_proof": request["runtime_archive_proof"],
                    "runtime_platform": {k: proof[k] for k in ("system", "machine", "libc", "uncompressed_bytes")},
                    "runtime_lock": provenance["runtime"], "runtime_provenance_file": manifest[0],
                    "provenance_commit": commit,
                    "source_runtime_stability": {"state": "source_runtime_still_matches_closed_capsule"},
                    "D_original_readback_verified": True}
            # Check the complete provenance and allowed supplemental-file set
            # before WAN IO or a large runtime decode.
            validate_bootstrap_binding(spec, body, read_object=read, source_identity=state.get("source_identity"))
            for row in request["files"]:
                read(row)
            receipt = json.loads(read(next(r for r in request["files"] if r["role"] == "source_receipt")))
            bundle = read(next(r for r in request["files"] if r["role"] == "source_zip"))
            with zipfile.ZipFile(io.BytesIO(bundle)) as archive:
                expected = receipt["source_bundle"]["files"]
                if (not 0 < len(expected) <= 10000 or len(archive.namelist()) != len(expected)
                        or set(archive.namelist()) != set(expected)
                        or sum(row.file_size for row in archive.infolist()) > 2 * 1024**3):
                    raise RescueError("frozen source ZIP has another original member set")
                for name, sha in expected.items():
                    relative(name)
                    entry = archive.getinfo(name)
                    if (stat.S_ISLNK(entry.external_attr >> 16) or entry.file_size > MAX_BOOTSTRAP_CONTROL_BYTES
                            or hashlib.sha256(archive.read(name)).hexdigest() != sha):
                        raise RescueError("frozen source ZIP original member differs")
            runtime = request["runtime_archive"]
            runtime_key = (runtime["sha256"], request["runtime_archive_proof"]["sha256"])
            runtime_generation = {self.object_path(row["sha256"]): generations[self.object_path(row["sha256"])]
                                  for row in (runtime, request["runtime_archive_proof"])}
            reused_decode = runtime_key in runtime_readbacks
            if reused_decode:
                prior = runtime_readbacks[runtime_key]
                if prior["generations"] != runtime_generation:
                    raise RescueError("runtime object generation changed within this explicit bootstrap cohort")
                decoded = prior["decoded"]
            else:
                with binary_reader(self.object_path(runtime["sha256"]), expected_bytes=runtime["bytes"]) as source:
                    decoded = decode_runtime_capsule(source, proof)
                if any(signature(path) != before for path, before in runtime_generation.items()):
                    raise RescueError("runtime object changed during full bootstrap decoding")
                runtime_readbacks[runtime_key] = {"generations": runtime_generation, "decoded": decoded}
            ssh = self.source(spec)
            self.check_source_identity(ssh, state)
            for row in [*request["files"], request["runtime_archive_proof"]]:
                actual = ssh.adapter("metadata", "--path", row["source_path"])
                if any(actual.get(k) != row[k] for k in ("sha256", "bytes")):
                    raise RescueError("source bootstrap object no longer matches its preserved original")
            probe = ("import sys,json;sys.path.insert(0,sys.argv[1]);"
                     "from stockagent.runtime_identity import runtime_identity,verify_source_release;"
                     "from pathlib import Path;print(json.dumps({'runtime':runtime_identity(),"
                     "'source_release':verify_source_release(Path(sys.argv[2]),Path(sys.argv[1]))}))")
            observed = ssh.json(["env", "PYTHONDONTWRITEBYTECODE=1", "nice", "-n", "19", "ionice", "-c", "3",
                spec["runtime_prefix"] + "/bin/python", "-c", probe, spec["source_root"], spec["source_receipt"]], timeout=180)
            if (validate_runtime_lock(body["runtime_lock"], observed["runtime"])
                    or observed["source_release"] != spec["source_release"]):
                raise RescueError("source runtime/code differs from original training provenance")
            stability = ssh.adapter("verify-runtime", "--path", spec["runtime_prefix"],
                "--proof", request["runtime_archive_proof"]["source_path"], timeout=600)
            self.check_source_identity(ssh, state)
            body.update(source_runtime_stability=stability, runtime_full_decode=decoded, sealed_at=time.time())
            validate_bootstrap_binding(spec, body, read_object=read, source_identity=state.get("source_identity"))
            with self.lock(wait_seconds=max(120, owner_wait_seconds)):
                self.guard()
                check_claim()
                current = self.journal(run)
                if (digest(self.spec(run)) != digest(spec) or current.get("source_identity") != request["source_identity"]
                        or current.get("bootstrap") != previous or current.get("active_target")
                        or current.get("state") == "recovering"
                        or any(signature(path) != before for path, before in generations.items())):
                    raise RescueError("bootstrap enrollment/source/object generation changed during verification")
                reused = bool(previous and previous.get("attachment_sha256") == digest(request))
                if reused:
                    bootstrap_id = previous["id"]
                    self.bootstrap(run, bootstrap_id)
                else:
                    bootstrap_id = digest(body)
                    path = self.cold / "bootstrap" / run / (bootstrap_id + ".json")
                    if path.exists():
                        if _prepared_control(path) != body:
                            raise RescueError("preserve a conflicting immutable runtime bootstrap")
                    else:
                        atomic_json(path, body)
                    self.bootstrap(run, bootstrap_id)  # Fresh D readback before journal admission.
                    current["bootstrap"] = {"id": bootstrap_id, "sealed_at": body["sealed_at"],
                        "binding_contract": BOOTSTRAP_BINDING_V2, "attachment_sha256": digest(request)}
                    self.record(run, current)  # Merge the newest checkpoint/fence, never the old snapshot.
            return {"run_id": run, "state": "bootstrap_preserved", "bootstrap_id": bootstrap_id,
                    "bootstrap_binding_contract": BOOTSTRAP_BINDING_V2, "reused": reused,
                    "runtime_full_decode": decoded, "original_enrollment_unchanged": True,
                    "runtime_decode_reused_within_this_cohort": reused_decode,
                    "owner_lock_scope": "initial_pin_and_latest_journal_merge",
                    "source_inputs_rebuilt_on_remote": False, "GPU_jobs_started": 0}

    def prepared_reference_objects(self, spec: dict) -> list[dict]:
        """Preflight pinned original metadata in the existing private D CAS.

        The target path belongs to the enrolled spec, while its immutable bytes
        are identified solely by SHA. Legacy untyped factorized references keep
        their original bootstrap contract; newly pinned references must exist
        before any source payload transfer or readiness receipt.
        """
        from stockagent.data_sync.windows_cold_io import hash_many, windows_path
        resources = preparation_resource_bindings(spec)
        selected = {row["path"]: row["sha256"] for row in resources}
        sizes = {row["path"]: row["bytes"] for row in resources if "bytes" in row}
        preserved = {row["preserved_auxiliary_bundle"]["path"]
                     for rows in preparation_references(spec).values() for row in rows
                     if "preserved_auxiliary_bundle" in row}
        file_sets = {row["original_file_set_bundle"]["path"]
                     for rows in preparation_references(spec).values() for row in rows
                     if "original_file_set_bundle" in row}
        if selected:
            self.guard()
        members, native, hashes, seen = [], [], {}, set()
        for destination, sha in sorted(selected.items()):
            path = absolute(str(self.object_path(sha)))
            before = signature(path)
            limit = (MAX_PRESERVED_AUXILIARY_BYTES if destination in preserved else
                     MAX_PRESERVED_FILE_SET_BYTES if destination in file_sets else MAX_PREPARED_REFERENCE_BYTES)
            if not stat.S_ISREG(before[-1]) or not 0 < before[2] <= limit:
                raise RescueError("prepared original reference exceeds its closed metadata bound")
            if destination in sizes and before[2] != sizes[destination]:
                raise RescueError("original input control differs from its exact enrolled size")
            members.append((path, before, {"reference_path": destination, "sha256": sha, "bytes": before[2]}))
            if path not in seen:
                seen.add(path)
                if windows_path(path) is None:
                    hashes[path] = self.object_hash(path)
                else:
                    native.append(path)
        for offset in range(0, len(native), 16):
            batch = native[offset:offset + 16]
            proof = hash_many(batch)
            if set(proof) != set(batch):
                raise RescueError("prepared reference native hash census differs")
            hashes.update(proof)
        # One transaction-local full-byte observation, not a persisted digest
        # cache. Recheck every original generation after all bounded batches.
        for path, before, row in members:
            if hashes[path] != row["sha256"] or signature(path) != before:
                raise RescueError("prepared original reference failed independent D SHA readback")
        return [row for _, _, row in members]

    def verify_sources(self, run: str, *, owner_wait_seconds: int = 0) -> dict:
        """Decode pinned D inputs while checkpoint preservation keeps its owner.

        A per-run claim prevents duplicate long readbacks. Only pinning the
        input version and merging its proof require the global control lock;
        always refresh the journal after IO so a newer checkpoint/fence wins.
        """
        run = identifier(run)
        claim = absolute(str(self.state / "source-verification-locks" / (run + ".lock")))
        with _prepared_lock(claim) as (_, check_claim):
            with self.lock(wait_seconds=owner_wait_seconds):
                self.guard()
                enrollment_sha = digest(self.spec(run))
                spec = self.recovery_spec(run)
                if not (spec.get("data_sources") or spec.get("data_capsules")):
                    raise RescueError("checkpoint-only enrollment has no pinned cold source bindings to verify")
                validate_spec(spec)
                spec_sha = digest(spec)
            # These hashes/decodes can take hours. Do not serialize checkpoint
            # capture, target fencing or lease supervision behind their IO.
            objects = {self.object_path(row["sha256"]) for row in preparation_resource_bindings(spec)}
            objects.update(self.object_path(row[key]) for row in spec.get("data_capsules", [])
                           for key in ("proof_sha256", "archive_sha256"))
            generations = {path: signature(path) for path in objects}
            body = self._decode_source_objects(spec)
            proof_id = digest(body)
            path = self.cold / "source-verification" / run / (proof_id + ".json")
            # Admission may skip a busy owner before expensive IO. At commit,
            # tolerate a short in-flight capture instead of discarding a full
            # decode because it overlaps one normal preservation transaction.
            with self.lock(wait_seconds=max(120, owner_wait_seconds)):
                self.guard()
                check_claim()
                if digest(self.spec(run)) != enrollment_sha or digest(self.recovery_spec(run)) != spec_sha:
                    raise RescueError("input version changed during source verification; retry its new version")
                if any(signature(obj) != before for obj, before in generations.items()):
                    raise RescueError("cold input control changed during source verification")
                state = self.journal(run)
                atomic_json(path, body)
                if digest(json.loads(path.read_bytes())) != proof_id:
                    raise RescueError("D source verification receipt readback differs")
                check_claim()
                state["input_verification"] = {"id": proof_id, "spec_sha256": spec_sha,
                    "verified_at": body["verified_at"], "level": "all_original_file_SHA_decoded",
                    "prepared_reference_sha256": {row["reference_path"]: row["sha256"]
                                                   for row in body["prepared_references"]},
                    "capsule_proof_sha256": {row["binding"]["archive_sha256"]: row["binding"]["proof_sha256"]
                                             for row in body["capsules"]}}
                self.record(run, state)
            return {"run_id": run, "state": "pinned_sources_full_decode_and_SHA_verified",
                    "source_count": len(body["sources"]), "receipt": str(path),
                    "prepared_reference_count": len(body["prepared_references"]),
                    "capsule_count": len(body["capsules"]), "hot_materialization_created": False,
                    "owner_lock_scope": "input_pin_and_latest_journal_merge"}

    def _decode_source_objects(self, spec: dict) -> dict:
        """Full original-byte verification; caller pins and commits its version."""
        from stockagent.data_sync.packed_snapshots import resolve_packed_snapshot_id, verify_packed_snapshot
        self.guard()
        references = self.prepared_reference_objects(spec)
        capsules = self.input_capsule_objects(spec)
        rows = []
        for binding in spec.get("data_sources", []):
            root = Path("/srv/stockagent-packed")
            resolved = resolve_packed_snapshot_id(root, binding["dataset"], binding["snapshot_id"])
            if resolved.manifest_sha256 != binding["manifest_sha256"]:
                raise RescueError("pinned source manifest changed before recovery readback")
            original_files = resolved.manifest["source"]["files"]
            print(json.dumps({"phase": "source_readback_started", "dataset": binding["dataset"],
                              "original_files": original_files}), flush=True)
            # The request covers the whole release. Reuse the canonical fused
            # CRC/original-SHA pass and native chunk reader; spelling every path
            # forces redundant inventory loads, pack decompression and chunk IO.
            proof = verify_packed_snapshot(root, resolved, reconstruct_all=True,
                d_primary_native_blob_reads=True, d_primary_native_pack_reads=True,
                d_primary_native_chunk_reads=True)
            if proof.get("independently_reconstructed_files") != original_files:
                raise RescueError("not all original source file bytes were independently reconstructed")
            print(json.dumps({"phase": "source_readback_complete", "dataset": binding["dataset"],
                              "original_file_SHA_verified": original_files}), flush=True)
            rows.append({"binding": binding, "verification": proof})
        capsule_readbacks = []
        if capsules:
            from stockagent.data_sync.windows_cold_io import binary_reader
            for binding, original, _, archive in capsules:
                path = self.object_path(archive["sha256"])
                before = signature(path)
                with binary_reader(path) as stream:
                    verification = decode_input_capsule(stream, binding, original)
                if signature(path) != before:
                    raise RescueError("input capsule changed during independent D decode")
                capsule_readbacks.append({"binding": binding, "verification": verification})
        return {"contract": CONTRACT, "run_id": spec["run_id"], "spec_sha256": digest(spec), "sources": rows,
                "prepared_references": references, "capsules": capsule_readbacks,
                "verified_at": time.time(), "hot_materialization_created": False,
                "fresh_GPU_or_PIT_acceptance_claim": False}

    def input_capsule_objects(self, spec: dict) -> list[tuple]:
        """Preflight small exact proofs, reusing preserved CAS objects as-is."""
        from stockagent.data_sync.windows_cold_io import binary_reader
        result = []
        for binding in spec.get("data_capsules", []):
            proof_row = {"sha256": binding["proof_sha256"], "bytes": binding["proof_bytes"]}
            archive = {"sha256": binding["archive_sha256"], "bytes": binding["archive_bytes"]}
            path = absolute(str(self.object_path(proof_row["sha256"])))
            before = signature(path)
            if not stat.S_ISREG(before[-1]) or before[2] != proof_row["bytes"]:
                raise RescueError("input capsule proof differs from its bounded regular D object")
            with binary_reader(path) as stream:
                original = input_capsule_proof(binding, stream.read(proof_row["bytes"] + 1))
            if signature(path) != before:
                raise RescueError("input capsule proof changed during independent D readback")
            path = absolute(str(self.object_path(archive["sha256"])))
            before = signature(path)
            if not stat.S_ISREG(before[-1]) or before[2] != archive["bytes"]:
                raise RescueError("input capsule archive differs from its pinned regular D object")
            result.append((binding, original, proof_row, archive))
        return result

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
            if source_poll_gate_active(spec, state):
                results.append({"run_id": run, "state": "waiting_source_reenrollment",
                    "reason": SOURCE_IDENTITY_REASON, "automatic_retry": False,
                    "source_contacted": False,
                    "latest_checkpoint_retained": bool(state.get("latest")), "training_started": False})
                continue
            retry_at = state.get("last_failure", {}).get("retry_after_epoch", 0)
            if type(retry_at) in (int, float) and time.time() < retry_at:
                results.append({"run_id": run, "state": "waiting_source_retry",
                                "retry_after_epoch": retry_at, "latest_checkpoint_retained": bool(state.get("latest")),
                                "training_started": False})
                continue
            try:
                ssh = self.source(spec)
                # The SSH alias can be repointed after a rental replacement.
                # Verify its read-only identity before asking an old adapter
                # to inspect/seal a run, or misclassifying its absence as EIO.
                self.check_source_identity(ssh, state)
                observed = ssh.adapter("health", timeout=20)
                identity = observed.get("node_identity")
                if identity and state.get("source_identity") and identity != state["source_identity"]:
                    raise SourceIdentityChanged()
                if identity:
                    state["source_identity"] = identity
                if len(observed.get("devices", [])) < spec["world_size"] and observed["state"] != "gpu_fault":
                    observed = {"state": "gpu_fault", "reason": "device_count_reduced"}
                state["health"] = {**observed, "observed_at": time.time()}
                if observed["state"] == "gpu_fault" or state.get("source_lifecycle_state") == "failed":
                    state["state"] = "awaiting_user_confirmation"
                self.record(run, state)
                # Preserve CPU-readable committed progress even during GPU EIO.
                result = self.capture(run, transport=ssh)
                state = self.journal(run)
                state.pop("last_failure", None)
                state["health"] = {**observed, "observed_at": time.time()}
                if observed["state"] == "gpu_fault" or state.get("source_lifecycle_state") == "failed":
                    state["state"] = "awaiting_user_confirmation"
                self.record(run, state)
                results.append(result)
            except (OSError, ValueError, RescueError, subprocess.TimeoutExpired) as exc:
                state = self.journal(run)
                previous = state.get("last_failure", {})
                failures = min(10, int(previous.get("consecutive_failures", 0)) + 1)
                delay = min(300, self.policy["poll_seconds"] * 2**(failures-1))
                failure = {"type": type(exc).__name__, "at": time.time(),
                           "consecutive_failures": failures, "retry_after_epoch": time.time()+delay}
                if isinstance(exc, SSHOperationError):
                    diagnostic = self.path(run) / "last-failure.private.json"
                    atomic_json(diagnostic, {"at": failure["at"], "exit_code": exc.returncode,
                                "stderr": exc.private_stderr.decode(errors="replace")})
                    failure["private_diagnostics_available"] = True
                    failure["ssh_exit_code"] = exc.returncode
                # Journal is private, but status only projects this fixed
                # finite set. No arbitrary remote error/command is published.
                if isinstance(exc, SourceIdentityChanged):
                    failure.update(reason=SOURCE_IDENTITY_REASON, automatic_retry=False)
                    failure.pop("retry_after_epoch", None)
                    state["source_poll_gate"] = {"contract": SOURCE_POLL_GATE_CONTRACT,
                        "spec_sha256": digest(spec), "source_identity": state.get("source_identity"),
                        "generation": state["generation"], "reason": SOURCE_IDENTITY_REASON}
                else:
                    state.pop("source_poll_gate", None)
                state.update(state="awaiting_user_confirmation", last_failure=failure)
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
        spec, state = self.recovery_spec(run), self.journal(run)
        key, _ = self.key(key_id)
        if not state.get("latest") or not state.get("bootstrap"):
            raise RescueError("checkpoint and source/runtime bootstrap must both be committed to D first")
        if not input_source_readback_ready(spec, state):
            raise RescueError("non-empty pinned cold source bindings must be independently decoded/verified for this spec first")
        self.bootstrap(run, state["bootstrap"]["id"])
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
        if "input_recipe_revision_sha256" in spec:
            payload.update(input_recipe_revision_sha256=spec["input_recipe_revision_sha256"],
                           original_spec_sha256=digest(self.spec(run)))
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
        spec = self.recovery_spec(plan["run_id"])
        if key.get("bound_plan") != plan_id or digest(spec) != plan["spec_sha256"]:
            raise RescueError("approval key/spec no longer matches its plan")
        if (plan.get("input_recipe_revision_sha256") != spec.get("input_recipe_revision_sha256")
                or ("input_recipe_revision_sha256" in spec
                    and plan.get("original_spec_sha256") != digest(self.spec(plan["run_id"])))):
            raise RescueError("approval input recipe/original enrollment no longer matches its plan")
        if not input_source_readback_ready(spec, state):
            raise RescueError("pinned source verification no longer matches this recovery plan")
        self.bootstrap(plan["run_id"], plan["bootstrap_id"])
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
        source = self.object_path(row["sha256"])
        if source.stat().st_size != row["bytes"] or self.object_hash(source) != row["sha256"]:
            raise RescueError("D rescue object is not independently recoverable")
        return self._push_verified_source(ssh, source, row, destination)

    def _push_verified_source(self, ssh: SSH, source: Path, row: dict, destination: Path) -> dict:
        """One receiver/ACK/native-producer lifecycle for already verified D files."""
        from stockagent.data_sync.windows_cold_io import binary_reader
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
                    # The receiver may acknowledge an existing exact object
                    # without reading stdin. Finish the owned native D producer
                    # before leaving binary_reader: its stdout pipe otherwise
                    # fills and masks a successful ACK with a 120-second wait.
                    try:
                        process.stdin.close()
                    except BrokenPipeError:
                        pass
                    for _ in iter(lambda: stream.read(4 * 1024**2), b""):
                        pass
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
        run, spec = plan["run_id"], self.recovery_spec(plan["run_id"])
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
        bootstrap = self.bootstrap(run, plan["bootstrap_id"])
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
        differences = validate_runtime_lock(bootstrap_runtime_lock(spec, bootstrap), restored_runtime)
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
        if not (spec.get("data_sources") or spec.get("data_capsules")):
            raise RescueError("checkpoint-only enrollment cannot restore unbound training inputs")
        validate_spec(spec)
        references = self.prepared_reference_objects(spec)
        capsules = self.input_capsule_objects(spec)
        # Deliver original oracles before multi-GB raw inputs. Reuse the same
        # SHA-verified, no-overwrite transfer used by checkpoint/bootstrap data;
        # never assume the source worker's control files exist on a fresh host.
        for row in references:
            self.push_object(ssh, row, Path(row["reference_path"]))
        for _, _, proof_row, _ in capsules:
            self.push_object(ssh, proof_row, incoming / proof_row["sha256"])
        from stockagent.data_sync.packed_snapshots import resolve_packed_snapshot_id
        cold = Path("/srv/stockagent-packed")
        selected = []
        for binding in spec.get("data_sources", []):
            resolved = resolve_packed_snapshot_id(cold, binding["dataset"], binding["snapshot_id"])
            if resolved.manifest_sha256 != binding["manifest_sha256"]:
                raise RescueError("training source release changed; do not follow latest")
            selected.append((binding, resolved))
        # The controller is distinct from frozen model/builder code. Deliver
        # one existing canonical cache module, SHA-addressed and no-overwrite,
        # rather than copying its alias/lease/GC algorithms into the adapter.
        absolute(str(CACHE_CONTROLLER))
        before = signature(CACHE_CONTROLLER)
        if not stat.S_ISREG(before[-1]):
            raise RescueError("canonical cache controller must be a regular source resource")
        data = CACHE_CONTROLLER.read_bytes()
        controller_sha = hashlib.sha256(data).hexdigest()
        if not 0 < len(data) <= 512 * 1024 or signature(CACHE_CONTROLLER) != before:
            raise RescueError("canonical cache controller changed/exceeded its resource bound")
        controller = Path("/opt/stockagent-training-rescue/resources") / controller_sha / "materialized_cache.py"
        uploaded = ssh.upload(controller, data)
        if uploaded.get("sha256") != controller_sha or uploaded.get("bytes") != len(data):
            raise RescueError("target canonical cache controller readback differs")
        remote_spec = str(Path(REMOTE_STATE) / spec["run_id"] / "spec.json")
        arguments = ["--spec", remote_spec, "--path", str(controller), "--sha256", controller_sha]
        prepared_links = [row for row in source_aliases(spec) if "step_index" in row]
        if prepared_links:
            proof = ssh.adapter("prepared-preflight", *arguments,
                python=spec["runtime_prefix"] + "/bin/python", timeout=120)
            if (proof.get("spec_sha256") != digest(spec) or proof.get("preflight_only") is not True
                    or proof.get("cache_controller_sha256") != controller_sha
                    or proof.get("prepared_link_targets") != {row["path"]: row["target"] for row in prepared_links}):
                raise RescueError("target original prepared alias preflight differs")
        # Check every original alias before the first large transfer. The
        # canonical cache owner repeats no-overwrite admission under its lock.
        for index, (binding, _) in enumerate(selected):
            proof = ssh.adapter("input-preflight", *arguments, "--index", str(index),
                python=spec["runtime_prefix"] + "/bin/python", timeout=120)
            if (proof.get("spec_sha256") != digest(spec) or proof.get("preflight_only") is not True
                    or type(proof.get("source_index")) is not int or proof["source_index"] != index
                    or proof.get("dataset") != binding["dataset"] or proof.get("snapshot_id") != binding["snapshot_id"]
                    or proof.get("cache_controller_sha256") != controller_sha):
                raise RescueError("target input alias preflight differs from the pinned spec")
        def check_capsule_reply(proof, binding, index, *, preflight):
            if (proof.get("spec_sha256") != digest(spec) or proof.get("preflight_only") is not preflight
                    or type(proof.get("capsule_index")) is not int or proof["capsule_index"] != index
                    or any(proof.get(key) != binding[key] for key in ("archive_sha256", "proof_sha256", "source_plan_sha256"))
                    or proof.get("cache_controller_sha256") != controller_sha
                    or (not preflight and proof.get("original_source_paths_ready") is not True)):
                raise RescueError("target input capsule preflight/materialization proof differs")
        for index, (binding, _, proof_row, _) in enumerate(capsules):
            proof = ssh.adapter("capsule-preflight", *arguments, "--index", str(index),
                "--proof", str(incoming / proof_row["sha256"]),
                python=spec["runtime_prefix"] + "/bin/python", timeout=120)
            check_capsule_reply(proof, binding, index, preflight=True)
        for index, (binding, resolved) in enumerate(selected):
            inventory = resolved.manifest["archive"]["inventory"]
            members = [*resolved.manifest["archive"]["objects"], inventory]
            for row in members:
                path = cold / relative(row["relpath"])
                destination = Path("/srv/stockagent-packed") / row["relpath"]
                self.push_cold_source(ssh, path, row, destination)
            self.push_cold_source(ssh, resolved.manifest_path,
                {"sha256": resolved.manifest_sha256, "bytes": resolved.manifest_path.stat().st_size},
                Path("/srv/stockagent-packed") / resolved.manifest_path.relative_to(cold))
            proof = ssh.adapter("materialize-input", *arguments, "--index", str(index),
                python=spec["runtime_prefix"] + "/bin/python", timeout=3600)
            if (proof.get("spec_sha256") != digest(spec)
                    or proof.get("manifest_sha256") != binding["manifest_sha256"]
                    or type(proof.get("source_index")) is not int or proof["source_index"] != index
                    or proof.get("dataset") != binding["dataset"] or proof.get("snapshot_id") != binding["snapshot_id"]
                    or proof.get("cache_controller_sha256") != controller_sha
                    or proof.get("original_source_paths_ready") is not True):
                raise RescueError("target pinned source materialization/lease proof differs")
        for index, (binding, _, proof_row, archive) in enumerate(capsules):
            self.push_object(ssh, archive, incoming / archive["sha256"])
            proof = ssh.adapter("materialize-capsule", *arguments, "--index", str(index),
                "--proof", str(incoming / proof_row["sha256"]), "--archive", str(incoming / archive["sha256"]),
                python=spec["runtime_prefix"] + "/bin/python", timeout=3600)
            check_capsule_reply(proof, binding, index, preflight=False)
        # CPU-only original builders. This is not permission to change the data
        # contract or permanently store prepared training views on penguin.
        def check_prepared_links(proof, index):
            expected = {row["path"]: row["target"] for row in prepared_links if row["step_index"] == index}
            if expected and (proof.get("prepared_link_targets") != expected
                             or proof.get("cache_controller_sha256") != controller_sha):
                raise RescueError("target verified prepared compatibility aliases differ")
        for index, step in enumerate(spec.get("rebuild_steps", [])):
            if spec.get("prepared_build_policy") == "shared_atomic_v1":
                proof = ssh.adapter("prepare-run", *arguments, "--index", str(index),
                    python=spec["runtime_prefix"] + "/bin/python", timeout=9000)
                from stockagent.training_rescue_worker import prepared_build_identity
                if (proof.get("spec_sha256") != digest(spec) or type(proof.get("step_index")) is not int
                        or proof["step_index"] != index or proof.get("skip_builder") is not True
                        or proof.get("fresh_full_original_readback") is not True
                        or proof.get("artifact_sha256") != digest(prepared_build_identity(spec, index))):
                    raise RescueError("target shared prepared build identity/readback differs")
                check_prepared_links(proof, index)
                continue
            admission = ssh.adapter("prepare-begin", *arguments, "--index", str(index),
                python=spec["runtime_prefix"] + "/bin/python", timeout=300)
            if admission.get("skip_builder"):
                check_prepared_links(admission, index)
                continue
            source_root = spec.get("rebuild_source_roots", [spec["source_root"]] * len(spec["rebuild_steps"]))[index]
            command = ["bash", "-c", 'cd "$1"; shift; source scripts/runtime_env.sh; exec_fintech_python "$@"',
                       "stockagent-rescue-prepare", source_root, *step]
            prefix = spec["runtime_prefix"]
            ssh.run(["env", "PYTHON_BIN=" + prefix + "/bin/python", "FINTECH_ENV_PATH=" + prefix, *command], timeout=7200)
            proof = ssh.adapter("prepare-finish", *arguments, "--index", str(index),
                                python=prefix + "/bin/python", timeout=300)
            check_prepared_links(proof, index)

    def push_cold_source(self, ssh: SSH, source: Path, row: dict, destination: Path) -> None:
        """Same SHA/flush boundary, without making a duplicate rescue CAS object."""
        from stockagent.data_sync.windows_cold_io import hash_file
        count = row.get("size", row.get("bytes"))
        if source.stat().st_size != count or hash_file(source) != row["sha256"]:
            raise RescueError("pinned cold source failed original-byte readback")
        self._push_verified_source(ssh, source, {"sha256": row["sha256"], "bytes": count}, destination)

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
        spec = self.execution_spec(run)
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
            selected_spec = self.execution_spec(state["run_id"]) if state.get("active_target") else self.recovery_spec(state["run_id"])
            sources_ready = input_source_readback_ready(selected_spec, state)
            bootstrap_gaps = bootstrap_declaration_gaps(selected_spec)
            enrollment_gate = source_poll_gate_active(self.spec(state["run_id"]), state)
            target_watched = bool(state.get("active_target") and state["state"] in
                                  {"recovering", "resume_progress_verified"})
            rows.append({"run_id": state["run_id"], "state": state["state"], "generation": state["generation"],
                "checkpoint_epochs": {k: v["epoch"] for k, v in latest.get("checkpoints", {}).items()},
                "preserved_bytes": latest.get("bytes"), "latest_ack_at": latest.get("committed_at"),
                "ack_age_seconds": time.time() - latest["committed_at"] if latest else None,
                "bootstrap_preserved": bool(state.get("bootstrap")), "health": state.get("health"),
                "bootstrap_binding_contract": (state.get("bootstrap") or {}).get("binding_contract"),
                "bootstrap_declaration": {"contract": "bootstrap_enrollment_declaration_v1",
                    "complete": not bootstrap_gaps, "missing": bootstrap_gaps,
                    "metadata_only": True, "fresh_D_readback_verified": False},
                "input_recipe_revision_sha256": selected_spec.get("input_recipe_revision_sha256"),
                "pinned_sources_readback_verified": sources_ready,
                "source_lifecycle_state": state.get("source_lifecycle_state", "unknown"),
                "source_polling": {"state": "waiting_source_reenrollment" if enrollment_gate else
                    (("target_owned" if target_watched else "waiting_target_confirmation")
                     if state.get("active_target") else "scheduled"),
                    "automatic_retry": not enrollment_gate and (not state.get("active_target") or target_watched),
                    "reason": SOURCE_IDENTITY_REASON if enrollment_gate else None},
                "recovery_prepared": bool(latest and state.get("bootstrap") and state.get("source_identity")
                    and sources_ready),
                "recovery_preparation_contract": "pinned_sources_and_runtime_v2",
                "last_failure": state.get("last_failure"), "resume_progress_verified": state["state"] == "resume_progress_verified"})
        return {"contract": CONTRACT, "runs": rows, "automatic_migration": False,
                "destination_selected_by": "user", "source_files_deleted": False}

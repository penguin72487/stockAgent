#!/usr/bin/env python3
"""Small local workflow around existing inventories, artifacts and supervisors.

Task/run receipts describe engineering work. They never replace a training
manifest, source receipt, publication gate or production service definition.
"""

from __future__ import annotations

import argparse
from collections import Counter
from contextlib import contextmanager
from datetime import UTC, datetime
import fcntl
import hashlib
from importlib.metadata import PackageNotFoundError, version
import json
import os
from pathlib import Path
import re
import shlex
import shutil
import signal
import socket
import subprocess
import sys
import uuid

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
from downloader.artifact_io import atomic_write_json  # noqa: E402

SCHEMA_VERSION = 1
TMUX_SOCKET = "stockagent-agent"
ID_PATTERN = re.compile(r"[a-zA-Z0-9][a-zA-Z0-9_-]{0,95}\Z")
TOOLS = (
    "git",
    "rg",
    "tmux",
    "jq",
    "sqlite3",
    "ssh",
    "rsync",
    "flock",
    "timeout",
    "curl",
    "node",
    "systemctl",
    "journalctl",
    "systemd-run",
)
RUNTIME_ENV_KEYS = (
    "PATH",
    "CONDA_PREFIX",
    "FINTECH_ENV_PATH",
    "PYTHON_BIN",
    "CUDA_VISIBLE_DEVICES",
    "STOCKAGENT_REPO_ROOT",
)


def now() -> str:
    return datetime.now(UTC).isoformat()


def identifier(value: str) -> str:
    if not ID_PATTERN.fullmatch(value):
        raise ValueError(
            "id must contain 1..96 letters, digits, underscores or hyphens"
        )
    return value


def command(argv: list[str], *, root: Path = REPO_ROOT, timeout: int = 15) -> str:
    environment = {**os.environ, "GIT_OPTIONAL_LOCKS": "0"}
    result = subprocess.run(
        argv, cwd=root, env=environment, capture_output=True, text=True, timeout=timeout
    )
    if result.returncode:
        raise RuntimeError(
            f"{argv[0]} exited {result.returncode}: {result.stderr.strip()}"
        )
    return result.stdout.rstrip("\n")


def save(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    atomic_write_json(path, data)
    path.chmod(0o600)


@contextmanager
def lock(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    with path.open("a") as handle:
        path.chmod(0o600)
        fcntl.flock(handle, fcntl.LOCK_EX)
        yield


def dirty_paths(raw: str) -> list[dict]:
    """Parse porcelain v1 -z, including spaces, newlines and rename sources."""
    entries, fields, index = [], raw.split("\0"), 0
    while index < len(fields) and fields[index]:
        field = fields[index]
        entry = {"status": field[:2], "path": field[3:]}
        index += 1
        if "R" in entry["status"] or "C" in entry["status"]:
            entry["original_path"] = fields[index]
            index += 1
        entries.append(entry)
    return entries


def git_snapshot(root: Path, *, fingerprint: bool = False) -> dict:
    snapshot = {
        "head": command(["git", "rev-parse", "HEAD"], root=root),
        "branch": command(["git", "branch", "--show-current"], root=root),
        "dirty_paths": dirty_paths(
            command(["git", "status", "--porcelain=v1", "-z"], root=root)
        ),
        "worktrees": command(
            ["git", "worktree", "list", "--porcelain"], root=root
        ).splitlines(),
        "remote_refs_live_verified": False,
    }
    if fingerprint:
        patch = subprocess.run(
            ["git", "diff", "--no-ext-diff", "--binary", "HEAD"],
            cwd=root,
            capture_output=True,
            check=True,
        ).stdout
        snapshot["tracked_diff_sha256"] = hashlib.sha256(patch).hexdigest()
    return snapshot


def file_evidence(path: Path, *, allow_empty: bool = False) -> dict:
    path = path.resolve()
    before = path.stat()
    if not path.is_file() or (before.st_size == 0 and not allow_empty):
        raise ValueError(f"evidence must be a non-empty file: {path}")
    if before.st_size > 16 * 1024 * 1024:
        raise ValueError(
            f"use a small manifest/receipt as evidence for large artifacts: {path}"
        )
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    after = path.stat()
    if (before.st_ino, before.st_size, before.st_mtime_ns) != (
        after.st_ino,
        after.st_size,
        after.st_mtime_ns,
    ):
        raise ValueError(f"evidence changed while being read: {path}")
    return {
        "path": str(path),
        "size_bytes": after.st_size,
        "mtime_ns": after.st_mtime_ns,
        "sha256": digest,
    }


def process_identity(pid: int) -> dict | None:
    try:
        stat = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
        if stat[0] in {"Z", "X"}:
            return None
        return {
            "pid": pid,
            "start_ticks": stat[19],
            "boot_id": Path("/proc/sys/kernel/random/boot_id").read_text().strip(),
        }
    except (OSError, IndexError):
        return None


def run_view(record: dict) -> dict:
    result = dict(record)
    if (
        record["state"] == "queued"
        and (
            datetime.now(UTC) - datetime.fromisoformat(record["queued_at_utc"])
        ).total_seconds()
        > 30
    ):
        result.update(
            state="launch_unverified",
            interruption="runner has not published a start receipt within 30 seconds",
        )
    if (
        record["state"] == "running"
        and process_identity(record["process"]["pid"]) != record["process"]
    ):
        result.update(
            state="interrupted",
            interruption="recorded process no longer matches this boot/PID/start time",
        )
    log = Path(record["log"])
    try:
        stat = log.stat()
        result["log_size_bytes"] = stat.st_size
        result["log_age_seconds"] = max(
            0, datetime.now(UTC).timestamp() - stat.st_mtime
        )
    except FileNotFoundError:
        result["log_size_bytes"] = None
    return result


class Workflow:
    def __init__(self, root: Path = REPO_ROOT, state_dir: Path | None = None):
        self.root = root.resolve()
        self.state_dir = (
            state_dir or self.root / "artifacts/operations/agent-workflow"
        ).resolve()

    def task_path(self, task_id: str) -> Path:
        return self.state_dir / "tasks" / identifier(task_id) / "task.json"

    def run_path(self, run_id: str) -> Path:
        return self.state_dir / "runs" / identifier(run_id) / "run.json"

    def read(self, path: Path) -> dict:
        data = json.loads(path.read_text())
        if (
            not isinstance(data, dict)
            or data.get("schema_version") != SCHEMA_VERSION
            or data.get("repo_root") != str(self.root)
            or data.get("host") != socket.gethostname()
        ):
            raise ValueError(f"incompatible workflow receipt: {path}")
        return data

    def start_task(
        self, task_id: str, objective: str, next_action: str, config: Path | None = None
    ) -> dict:
        if not objective.strip() or not next_action.strip():
            raise ValueError("objective and next action must be non-empty")
        path = self.task_path(task_id)
        baseline = git_snapshot(self.root, fingerprint=True)
        self.state_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.state_dir.chmod(0o700)
        with lock(path.with_suffix(".lock")):
            if path.exists():
                raise ValueError(
                    f"task already exists; use task show/update: {task_id}"
                )
            config_evidence = (
                file_evidence(config if config.is_absolute() else self.root / config)
                if config
                else None
            )
            task = {
                "schema_version": SCHEMA_VERSION,
                "task_id": task_id,
                "repo_root": str(self.root),
                "host": socket.gethostname(),
                "objective": objective,
                "next_action": next_action,
                "state": "active",
                "created_at_utc": now(),
                "updated_at_utc": now(),
                "git_baseline": baseline,
                "selected_config": config_evidence,
                "evidence": [],
                "history": [],
            }
            save(path, task)
            return task

    def update_task(
        self,
        task_id: str,
        *,
        state: str | None = None,
        next_action: str | None = None,
        note: str | None = None,
        evidence: list[Path] = (),
    ) -> dict:
        path = self.task_path(task_id)
        with lock(path.with_suffix(".lock")):
            task = self.read(path)
            proofs = [
                file_evidence(p if p.is_absolute() else self.root / p) for p in evidence
            ]
            if state == "complete":
                if not proofs:
                    raise ValueError("completion requires current non-empty evidence")
                latest = {}
                for record in self.runs(task_id):
                    latest[record["name"]] = record
                resolutions = {
                    resolution["run_id"]: resolution
                    for resolution in task.get("run_resolutions", [])
                }
                for record in latest.values():
                    if record["state"] == "succeeded":
                        continue
                    resolution = resolutions.get(record["run_id"])
                    if resolution is None:
                        raise ValueError(
                            "latest run of each name must have succeeded or have "
                            "a verified failure resolution before completion"
                        )
                    self._verify_resolution(task_id, resolution)
            if next_action is not None:
                if not next_action.strip():
                    raise ValueError("next action must be non-empty")
                task["next_action"] = next_action
            if state is not None:
                task["state"] = state
            task["evidence"].extend(proofs)
            task["updated_at_utc"] = now()
            task["history"].append(
                {
                    "at_utc": now(),
                    "state": task["state"],
                    "next_action": task["next_action"],
                    "note": note,
                    "evidence": proofs,
                }
            )
            save(path, task)
            return task

    def _verify_resolution(self, task_id: str, resolution: dict) -> None:
        """A disposition preserves failure and requires a later successful proof."""
        if resolution["outcome"] not in {"superseded", "expected-rejection"}:
            raise ValueError("unsupported failure resolution")
        failed = self.read(self.run_path(resolution["run_id"]))
        verified = self.read(self.run_path(resolution["verified_by"]))
        if (
            failed["task_id"] != task_id
            or verified["task_id"] != task_id
            or failed["state"] != "failed"
            or type(failed.get("exit_code")) is not int
            or failed["exit_code"] == 0
            or not failed.get("finished_at_utc")
            or verified["state"] != "succeeded"
            or verified.get("exit_code") != 0
            or not verified.get("finished_at_utc")
            or datetime.fromisoformat(verified["queued_at_utc"])
            <= datetime.fromisoformat(failed["queued_at_utc"])
            or datetime.fromisoformat(verified["finished_at_utc"])
            < datetime.fromisoformat(failed["finished_at_utc"])
        ):
            raise ValueError(
                "resolution requires a terminal failed run and a later successful "
                "verification in the same task"
            )
        if not resolution["note"].strip() or not resolution["evidence"]:
            raise ValueError("resolution requires a note and current evidence")
        for proof in [
            resolution["failed_receipt"],
            resolution["verification_receipt"],
            *resolution["evidence"],
            *([resolution["failed_log"]] if resolution.get("failed_log") else []),
        ]:
            current = file_evidence(
                Path(proof["path"]), allow_empty=proof is resolution.get("failed_log")
            )
            if (current["sha256"], current["size_bytes"]) != (
                proof["sha256"], proof["size_bytes"]
            ):
                raise ValueError("failure resolution evidence changed")
        if resolution["outcome"] == "expected-rejection":
            declared = False
            for proof in resolution["evidence"]:
                body = Path(proof["path"]).read_bytes()
                if hashlib.sha256(body).hexdigest() != proof["sha256"]:
                    raise ValueError("failure resolution evidence changed")
                try:
                    receipt = json.loads(body)
                except (ValueError, UnicodeError):
                    continue
                if (
                    isinstance(receipt, dict)
                    and receipt.get("state") == "accepted_expected_rejection"
                    and receipt.get("task_id") == task_id
                    and receipt.get("run_id") == resolution["run_id"]
                    and receipt.get("verified_by") == resolution["verified_by"]
                    and isinstance(receipt.get("criterion"), str)
                    and receipt["criterion"].strip()
                ):
                    declared = True
            if not declared:
                raise ValueError("expected rejection requires a bound criterion receipt")

    def resolve_run(
        self,
        task_id: str,
        run_id: str,
        *,
        outcome: str,
        verified_by: str,
        note: str,
        evidence: list[Path],
    ) -> dict:
        """Append an engineering disposition; never rewrite a run as successful."""
        path = self.task_path(task_id)
        with lock(path.with_suffix(".lock")):
            task = self.read(path)
            if task["state"] != "active":
                raise ValueError("failure resolution requires an active task")
            if any(r["run_id"] == run_id for r in task.get("run_resolutions", [])):
                raise ValueError("run already has a failure resolution")
            failed = self.read(self.run_path(run_id))
            log = Path(failed["log"])
            resolution = {
                "at_utc": now(),
                "run_id": run_id,
                "outcome": outcome,
                "verified_by": verified_by,
                "note": note,
                "failed_receipt": file_evidence(self.run_path(run_id)),
                "verification_receipt": file_evidence(self.run_path(verified_by)),
                "failed_log": file_evidence(log, allow_empty=True) if log.exists() else None,
                "evidence": [
                    file_evidence(p if p.is_absolute() else self.root / p)
                    for p in evidence
                ],
            }
            self._verify_resolution(task_id, resolution)
            task.setdefault("run_resolutions", []).append(resolution)
            task["updated_at_utc"] = now()
            task["history"].append(
                {
                    "at_utc": now(),
                    "state": task["state"],
                    "next_action": task["next_action"],
                    "note": note,
                    "run_resolution": resolution,
                }
            )
            save(path, task)
            return resolution

    def tasks(self) -> list[dict]:
        return [
            self.read(path)
            for path in sorted((self.state_dir / "tasks").glob("*/task.json"))
        ]

    def runs(self, task_id: str | None = None) -> list[dict]:
        records = [
            run_view(self.read(path))
            for path in sorted((self.state_dir / "runs").glob("*/run.json"))
        ]
        return sorted(
            (
                record
                for record in records
                if task_id is None or record["task_id"] == task_id
            ),
            key=lambda record: (record["queued_at_utc"], record["run_id"]),
        )

    def recovery_snapshot(self, record: dict) -> dict:
        """Verify the old process/session and supervisor before a manual retry."""
        for key in ("process", "child_process"):
            anchor = record.get(key)
            if anchor and process_identity(anchor["pid"]) == anchor:
                raise ValueError("recorded runner or child is still alive")
        child = record.get("child_process")
        if (
            child
            and child["boot_id"]
            == Path("/proc/sys/kernel/random/boot_id").read_text().strip()
        ):
            # Children inherit the foreground command's new session even if its
            # leader exits. ps exits 1 with empty stdout when that SID is gone.
            processes = subprocess.run(
                ["ps", "-s", str(child["pid"]), "-o", "pid="],
                capture_output=True,
                text=True,
                timeout=10,
            )
            if (
                processes.returncode not in (0, 1)
                or processes.stderr.strip()
                or processes.stdout.strip()
            ):
                raise ValueError(
                    "old command session still exists or could not be verified"
                )
        if record["backend"] == "systemd":
            argv = ["systemctl"] + ([] if os.geteuid() == 0 else ["--user"])
            observed = command(
                [
                    *argv,
                    "show",
                    record["unit"],
                    "--property=ActiveState,LoadState,MainPID",
                ],
                root=self.root,
            )
            properties = dict(
                line.split("=", 1) for line in observed.splitlines() if "=" in line
            )
            if (
                properties.get("ActiveState") not in {"inactive", "failed"}
                or properties.get("MainPID") != "0"
            ):
                raise ValueError("old systemd unit has not been verified stopped")
            return properties
        prefix = ["tmux", "-L", TMUX_SOCKET]
        windows = command(
            [*prefix, "list-windows", "-t", record["session"], "-F", "#{window_name}"],
            root=self.root,
        ).splitlines()
        window = record.get("window_name", record["name"])
        if window in windows:
            panes = command(
                [
                    *prefix,
                    "list-panes",
                    "-t",
                    record["session"] + ":" + window,
                    "-F",
                    "#{pane_dead}",
                ],
                root=self.root,
            ).splitlines()
            if not panes or any(pane != "1" for pane in panes):
                raise ValueError("old tmux command window is still alive")
        return {
            "session": record["session"],
            "window": window,
            "command_window_stopped": True,
        }

    def reconcile(self, run_id: str, note: str, evidence: list[Path]) -> dict:
        if not note.strip() or not evidence:
            raise ValueError("reconciliation requires a note and current evidence")
        path = self.run_path(run_id)
        with lock(path.with_suffix(".lock")):
            record = self.read(path)
            if run_view(record)["state"] not in {"interrupted", "launch_unverified"}:
                raise ValueError(
                    "only interrupted or unverified runs need reconciliation"
                )
            proofs = [
                file_evidence(p if p.is_absolute() else self.root / p) for p in evidence
            ]
            stopped = self.recovery_snapshot(record)
            record.update(
                state="interrupted",
                recovery_verified=True,
                recovery={
                    "at_utc": now(),
                    "note": note,
                    "evidence": proofs,
                    "stopped_supervisor": stopped,
                },
            )
            save(path, record)
            return record

    def session(self, task_id: str) -> dict:
        self.read(self.task_path(task_id))
        name = (
            "sa-"
            + task_id
            + "-"
            + hashlib.sha256(str(self.root).encode()).hexdigest()[:6]
        )
        prefix = ["tmux", "-L", TMUX_SOCKET]
        exists = (
            subprocess.run(
                [*prefix, "has-session", "-t", "=" + name], capture_output=True
            ).returncode
            == 0
        )
        shell = shlex.join(
            [
                "/bin/bash",
                "--noprofile",
                "--rcfile",
                str(self.root / "scripts/agent_tmux_rc.sh"),
                "-i",
            ]
        )
        if not exists:
            session_id = command(
                [
                    *prefix,
                    "new-session",
                    "-d",
                    "-P",
                    "-F",
                    "#{session_id}",
                    "-s",
                    name,
                    "-n",
                    "work",
                    "-c",
                    str(self.root),
                    "-e",
                    "STOCKAGENT_REPO_ROOT=" + str(self.root),
                    shell,
                ],
                root=self.root,
            )
        else:
            session_id = command(
                [*prefix, "display-message", "-p", "-t", name + ":", "#{session_id}"],
                root=self.root,
            )
        command(
            [*prefix, "set-option", "-t", session_id, "history-limit", "20000"],
            root=self.root,
        )
        command(
            [*prefix, "set-option", "-t", session_id, "remain-on-exit", "on"],
            root=self.root,
        )
        windows = command(
            [*prefix, "list-windows", "-t", session_id, "-F", "#{window_name}"],
            root=self.root,
        ).splitlines()
        for window in ("work", "logs", "status"):
            if window not in windows:
                command(
                    [
                        *prefix,
                        "new-window",
                        "-d",
                        "-t",
                        name + ":",
                        "-n",
                        window,
                        "-c",
                        str(self.root),
                        shell,
                    ],
                    root=self.root,
                )
        return {
            "socket": TMUX_SOCKET,
            "session": name,
            "created": not exists,
            "attach": shlex.join([*prefix, "attach-session", "-t", name]),
        }

    def launch(self, task_id: str, argv: list[str], backend: str, name: str) -> dict:
        if self.read(self.task_path(task_id))["state"] != "active":
            raise ValueError("launch requires an active task")
        if not argv:
            raise ValueError("supply a foreground command after --")
        identifier(name)
        run_id = (
            name[:32]
            + "-"
            + datetime.now(UTC).strftime("%Y%m%dT%H%M%S")
            + "-"
            + uuid.uuid4().hex[:8]
        )
        path = self.run_path(run_id)
        record = {
            "schema_version": SCHEMA_VERSION,
            "run_id": run_id,
            "task_id": task_id,
            "name": name,
            "repo_root": str(self.root),
            "host": socket.gethostname(),
            "backend": backend,
            "argv": argv,
            "python": sys.executable,
            "state": "queued",
            "queued_at_utc": now(),
            "exit_code": None,
            "log": str(path.with_name("run.log")),
        }
        record["workflow_code_sha256"] = hashlib.sha256(
            Path(__file__).read_bytes()
        ).hexdigest()
        execute = [
            "/bin/bash",
            str(self.root / "scripts/stockagent-agent.sh"),
            "--state-dir",
            str(self.state_dir),
            "_execute",
            run_id,
        ]
        environment = {
            key: os.environ[key] for key in RUNTIME_ENV_KEYS if key in os.environ
        }
        environment["STOCKAGENT_REPO_ROOT"] = str(self.root)
        environment["PYTHON_BIN"] = sys.executable
        if backend == "systemd":
            record["unit"] = "stockagent-agent-" + run_id + ".service"
            launch = ["systemd-run"] + ([] if os.geteuid() == 0 else ["--user"])
            launch += [
                "--quiet",
                "--collect",
                "--unit=" + record["unit"],
                "--property=Type=exec",
                "--property=Restart=no",
                "--property=WorkingDirectory=" + str(self.root),
            ]
            launch += [
                "--setenv=" + key + "=" + value for key, value in environment.items()
            ]
            launch += ["--", *execute]
        else:
            record["session"] = self.session(task_id)["session"]
            record["window_name"] = run_id
            launch = [
                "tmux",
                "-L",
                TMUX_SOCKET,
                "new-window",
                "-d",
                "-P",
                "-F",
                "#{pane_id}",
                "-t",
                record["session"] + ":",
                "-n",
                record["window_name"],
                "-c",
                str(self.root),
            ]
            for key, value in environment.items():
                launch.extend(["-e", key + "=" + value])
            launch.append(shlex.join(execute))
        with lock(self.task_path(task_id).with_suffix(".lock")):
            task = self.read(self.task_path(task_id))
            if task["state"] != "active":
                raise ValueError("launch requires an active task")
            previous = [r for r in self.runs(task_id) if r["name"] == name]
            if previous and previous[-1]["state"] in {
                "queued",
                "running",
                "interrupted",
                "launch_unverified",
            }:
                if not previous[-1].get("recovery_verified"):
                    raise ValueError(
                        "same-name command is still active or its outcome is unverified"
                    )
                self.recovery_snapshot(previous[-1])
            save(path, record)
        try:
            command(launch, root=self.root)
        except (OSError, RuntimeError, subprocess.TimeoutExpired) as exc:
            # A timed-out launcher may already have handed off the command.
            # Preserve an actual runner receipt if it has taken ownership.
            with lock(path.with_suffix(".lock")):
                current = self.read(path)
                if current["state"] == "queued":
                    current.update(
                        state="launch_unverified"
                        if isinstance(exc, subprocess.TimeoutExpired)
                        else "launch_failed",
                        launch_error=str(exc),
                        finished_at_utc=now(),
                    )
                    save(path, current)
            raise
        return self.read(path)

    def execute(self, run_id: str) -> int:
        path = self.run_path(run_id)
        with lock(path.with_suffix(".execution.lock")):
            with lock(path.with_suffix(".lock")):
                record = self.read(path)
                if record["state"] not in {"queued", "launch_unverified"}:
                    raise ValueError("run already executed or launch failed")
                if (
                    record.get("workflow_code_sha256")
                    != hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
                ):
                    record.update(
                        state="failed",
                        exit_code=78,
                        finished_at_utc=now(),
                        error="workflow code changed after launch; command was not executed",
                    )
                    save(path, record)
                    return 78
                anchor = process_identity(os.getpid())
                if anchor is None:
                    raise RuntimeError("cannot establish runner process identity")
                record.update(state="running", started_at_utc=now(), process=anchor)
                save(path, record)
            child, received = None, []

            def forward(signum, _frame):
                received.append(signum)
                if child is not None:
                    try:
                        os.killpg(child.pid, signum)
                    except ProcessLookupError:
                        pass

            old_handlers = {
                sig: signal.signal(sig, forward)
                for sig in (signal.SIGTERM, signal.SIGINT)
            }
            log = Path(record["log"])
            try:
                with log.open("ab", buffering=0) as output:
                    log.chmod(0o600)
                    argv = list(record["argv"])
                    if argv[0] == "run_fintech_python":
                        argv[0] = record["python"]
                    try:
                        child = subprocess.Popen(
                            argv,
                            cwd=self.root,
                            stdin=subprocess.DEVNULL,
                            stdout=output,
                            stderr=subprocess.STDOUT,
                            start_new_session=True,
                        )
                        record["child_process"] = process_identity(child.pid)
                        with lock(path.with_suffix(".lock")):
                            save(path, record)
                        if received:
                            forward(received[-1], None)
                        code = child.wait()
                    except OSError as exc:
                        output.write((f"{type(exc).__name__}: {exc}\n").encode())
                        code = 127
                    record.update(
                        state="cancelled"
                        if received
                        else ("succeeded" if code == 0 else "failed"),
                        exit_code=code,
                        received_signals=received,
                        finished_at_utc=now(),
                    )
                    with lock(path.with_suffix(".lock")):
                        save(path, record)
            finally:
                for sig, handler in old_handlers.items():
                    signal.signal(sig, handler)
            return code if code >= 0 else 128 - code


def status(workflow: Workflow, *, architecture: bool = False) -> dict:
    result = {
        "schema_version": SCHEMA_VERSION,
        "observed_at_utc": now(),
        "host": socket.gethostname(),
        "repo_root": str(workflow.root),
        "scope": "local checkout and local processes; no provider requests",
        "git": git_snapshot(workflow.root),
        "tools": {tool: shutil.which(tool) for tool in TOOLS},
        "tasks": [
            {
                key: task[key]
                for key in (
                    "task_id",
                    "state",
                    "objective",
                    "next_action",
                    "updated_at_utc",
                )
            }
            for task in workflow.tasks()
        ],
        "runs": workflow.runs(),
        "evidence_limits": [
            "Service activity is process liveness, not data completeness or deployed revision.",
            "Run success is command exit status, not training/source acceptance.",
            "Remote peers and refs have not been live-verified.",
        ],
    }
    try:
        lines = command(
            [
                "systemctl",
                "list-units",
                "--all",
                "stockagent-*.service",
                "--no-pager",
                "--plain",
                "--no-legend",
            ],
            root=workflow.root,
        ).splitlines()
        units = [
            dict(zip(("unit", "load", "active", "sub"), line.split(maxsplit=4)[:4]))
            for line in lines
            if line.strip()
        ]
        result["services"] = {
            "count": len(units),
            "states": dict(Counter(unit["active"] for unit in units)),
            "failed": [unit["unit"] for unit in units if unit["active"] == "failed"],
        }
    except (OSError, RuntimeError, subprocess.TimeoutExpired) as exc:
        result["services"] = {"observation_error": str(exc)}
    heads = {}
    for name in ("refresh_services", "public_summary"):
        path = workflow.root / "artifacts/live/data_monitor" / (name + ".json")
        try:
            stat = path.stat()
            heads[name] = {
                "path": str(path),
                "size_bytes": stat.st_size,
                "file_age_seconds": max(
                    0, datetime.now(UTC).timestamp() - stat.st_mtime
                ),
                "content_freshness_verified": False,
            }
        except FileNotFoundError:
            heads[name] = {"path": str(path), "available": False}
    result["existing_status_files"] = heads
    if architecture:
        from scripts.audit_project_architecture import build_inventory

        inventory = build_inventory()
        result["architecture"] = {
            "training_modes": len(inventory["training_modes"]),
            "market_configs": len(inventory["market_configs"]),
            "config_errors": inventory["market_config_errors"],
            "dataset_catalog_entries": len(inventory["dataset_catalog"]),
            "systemd_templates": len(inventory["systemd_templates"]),
        }
    return result


def doctor(workflow: Workflow) -> dict:
    tools = {tool: {"path": shutil.which(tool)} for tool in TOOLS}
    for tool, flag in (
        ("tmux", "-V"),
        ("git", "--version"),
        ("jq", "--version"),
        ("sqlite3", "--version"),
        ("rg", "--version"),
    ):
        if tools[tool]["path"]:
            tools[tool]["version"] = command(
                [tools[tool]["path"], flag], root=workflow.root
            ).splitlines()[0]
    for tool in tools:
        tools[tool]["system_path"] = shutil.which(
            tool, path="/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
        )
    packages = {}
    for package in ("pytest", "ruff", "playwright", "torch"):
        try:
            packages[package] = version(package)
        except PackageNotFoundError:
            packages[package] = None
    missing = [tool for tool, info in tools.items() if not info["path"]]
    if not packages["pytest"]:
        missing.append("Python package pytest")
    return {
        "schema_version": SCHEMA_VERSION,
        "observed_at_utc": now(),
        "repo_root": str(workflow.root),
        "host": socket.gethostname(),
        "accepted": not missing,
        "missing": missing,
        "tools": tools,
        "python": {
            "executable": sys.executable,
            "version": sys.version.split()[0],
            "prefix": sys.prefix,
            "packages": packages,
        },
        "limits": [
            "Tool availability does not prove browser interaction, CUDA readiness or remote installation."
        ],
    }


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--state-dir", type=Path)
    commands = result.add_subparsers(dest="command", required=True)
    for kind in ("status", "doctor"):
        sub = commands.add_parser(kind)
        sub.add_argument("--output", type=Path)
        if kind == "status":
            sub.add_argument("--architecture", action="store_true")
    tasks = commands.add_parser("task").add_subparsers(
        dest="task_command", required=True
    )
    sub = tasks.add_parser("start")
    sub.add_argument("id")
    sub.add_argument("--objective", required=True)
    sub.add_argument("--next", required=True)
    sub.add_argument("--config", type=Path)
    sub = tasks.add_parser("update")
    sub.add_argument("id")
    sub.add_argument(
        "--state", choices=("active", "paused", "blocked", "failed", "complete")
    )
    sub.add_argument("--next")
    sub.add_argument("--note")
    sub.add_argument("--evidence", type=Path, action="append", default=[])
    sub = tasks.add_parser("resolve-run")
    sub.add_argument("id")
    sub.add_argument("run_id")
    sub.add_argument("--outcome", choices=("superseded", "expected-rejection"), required=True)
    sub.add_argument("--verified-by", required=True)
    sub.add_argument("--note", required=True)
    sub.add_argument("--evidence", type=Path, action="append", required=True)
    tasks.add_parser("list")
    tasks.add_parser("show").add_argument("id")
    commands.add_parser("session").add_argument("id")
    sub = commands.add_parser("run")
    sub.add_argument("--backend", choices=("systemd", "tmux"), default="systemd")
    sub.add_argument("--name", default="command")
    sub.add_argument("id")
    sub.add_argument("argv", nargs=argparse.REMAINDER)
    commands.add_parser("run-status").add_argument("id")
    sub = commands.add_parser("run-reconcile")
    sub.add_argument("id")
    sub.add_argument("--note", required=True)
    sub.add_argument("--evidence", type=Path, action="append", required=True)
    sub = commands.add_parser("logs")
    sub.add_argument("id")
    sub.add_argument("--tail", type=int, default=80)
    commands.add_parser("_execute", help=argparse.SUPPRESS).add_argument("id")
    return result


def main() -> int:
    args = parser().parse_args()
    workflow = Workflow(state_dir=args.state_dir)
    if args.command == "_execute":
        return workflow.execute(args.id)
    if args.command in {"status", "doctor"}:
        data = (
            status(workflow, architecture=args.architecture)
            if args.command == "status"
            else doctor(workflow)
        )
        if args.output:
            save(args.output, data)
    elif args.command == "task":
        if args.task_command == "start":
            data = workflow.start_task(args.id, args.objective, args.next, args.config)
        elif args.task_command == "update":
            data = workflow.update_task(
                args.id,
                state=args.state,
                next_action=args.next,
                note=args.note,
                evidence=args.evidence,
            )
        elif args.task_command == "show":
            data = workflow.read(workflow.task_path(args.id))
        elif args.task_command == "resolve-run":
            data = workflow.resolve_run(
                args.id,
                args.run_id,
                outcome=args.outcome,
                verified_by=args.verified_by,
                note=args.note,
                evidence=args.evidence,
            )
        else:
            data = workflow.tasks()
    elif args.command == "session":
        data = workflow.session(args.id)
    elif args.command == "run":
        argv = args.argv[1:] if args.argv[:1] == ["--"] else args.argv
        data = workflow.launch(args.id, argv, args.backend, args.name)
    elif args.command == "run-status":
        data = run_view(workflow.read(workflow.run_path(args.id)))
    elif args.command == "run-reconcile":
        data = workflow.reconcile(args.id, args.note, args.evidence)
    else:
        if args.tail < 1 or args.tail > 1000:
            raise ValueError("tail must be between 1 and 1000")
        record = workflow.read(workflow.run_path(args.id))
        subprocess.run(["tail", "-n", str(args.tail), "--", record["log"]], check=True)
        return 0
    print(json.dumps(data, ensure_ascii=False, indent=2))
    return 1 if args.command == "doctor" and not data["accepted"] else 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as error:
        print(json.dumps({"error": str(error)}, ensure_ascii=False), file=sys.stderr)
        raise SystemExit(1)

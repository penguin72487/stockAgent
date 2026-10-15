#!/usr/bin/env python3
"""Measure owned terminal trials; the Herdr worker runs in a real managed pane."""
from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path
import pty
import select
import shlex
import statistics
import subprocess
import struct
import sys
import tempfile
import termios
import time
import uuid

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from downloader.artifact_io import atomic_write_json


def invoke(argv, *, environment=None, timeout=15):
    result = subprocess.run(argv, cwd=ROOT, env=environment, capture_output=True,
                            text=True, timeout=timeout, check=True)
    return result.stdout


def worker(args):
    if os.environ.get("HERDR_ENV") != "1" or not os.environ.get("HERDR_PANE_ID"):
        raise ValueError("terminal trial worker must execute in a real Herdr pane")
    output = args.output.resolve()
    parent = output.parent
    herdr = [str(args.herdr), "--session", args.session]
    created = json.loads(invoke([*herdr, "tab", "create", "--workspace",
        os.environ["HERDR_WORKSPACE_ID"], "--cwd", str(ROOT), "--label", "owned-probe", "--no-focus"]))
    pane = created["result"]["root_pane"]["pane_id"]
    tab = created["result"]["tab"]["tab_id"]
    socket = "sa-tool-probe-" + uuid.uuid4().hex[:12]
    tmux = ["tmux", "-L", socket]
    invoke([*tmux, "new-session", "-d", "-s", "probe", "-x", "120", "-y", "40",
            "-c", str(ROOT), "/bin/bash --noprofile --norc"])
    python_code = (
        "from pathlib import Path; from hashlib import sha256; "
        "print(sha256(Path('scripts/runtime_env.sh').read_bytes()+"
        "Path('docs/agents/runtime.md').read_bytes()).hexdigest())"
    )
    workload = "source scripts/runtime_env.sh\nrun_fintech_python -c " + shlex.quote(python_code)
    expected = hashlib.sha256((ROOT / "scripts/runtime_env.sh").read_bytes() +
                              (ROOT / "docs/agents/runtime.md").read_bytes()).hexdigest()

    def submit(mode, command, marker):
        if mode == "tmux":
            invoke([*tmux, "send-keys", "-t", "probe:0.0", "-l", "--", command])
            invoke([*tmux, "send-keys", "-t", "probe:0.0", "Enter"])
        else:
            invoke([*herdr, "pane", "run", pane, command])

    def collect(mode, marker):
        if mode == "herdr":
            invoke([*herdr, "pane", "wait-output", pane, "--match", marker,
                    "--source", "recent-unwrapped", "--timeout", "10000"])
            return invoke([*herdr, "pane", "read", pane, "--source", "recent-unwrapped", "--lines", "120"])
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            text = invoke([*tmux, "capture-pane", "-p", "-J", "-t", "probe:0.0", "-S", "-120"])
            if marker in text:
                return text
            time.sleep(0.01)
        raise TimeoutError("owned tmux command did not complete")

    samples = []
    background = []
    try:
        time.sleep(0.15)
        for turn in range(args.repetitions):
            modes = ("bash", "zsh_bash_delegate", "tmux", "herdr")
            if turn % 2:
                modes = tuple(reversed(modes))
            for mode in modes:
                token = uuid.uuid4().hex[:12]
                marker = "DONE_" + token + "=0"
                started = time.perf_counter()
                if mode == "bash":
                    text = invoke(["/bin/bash", "--noprofile", "--norc", "-c", workload])
                elif mode == "zsh_bash_delegate":
                    text = invoke(["zsh", "-f", "-c", shlex.join(
                        ["/bin/bash", "--noprofile", "--norc", "-c", workload])])
                else:
                    body = workload + "\ntrial_exit=$?\nprintf '\\nDONE_" + token + "=%s\\n' \"$trial_exit\""
                    submit(mode, shlex.join(["/bin/bash", "--noprofile", "--norc", "-c", body]), marker)
                    text = collect(mode, marker)
                assert expected in text, "the complete runtime/read/hash output must match"
                samples.append({"mode": mode, "iteration": turn + 1,
                                "seconds": time.perf_counter() - started, "complete_output_verified": True})
        for mode in ("tmux", "herdr"):
            token = uuid.uuid4().hex[:12]
            marker = "PERSIST_" + token + "=0"
            receipt = parent / (mode + "-owned-counter.json")
            code = (
                "import json,time; from pathlib import Path; "
                "[time.sleep(0.2) for _ in range(6)]; "
                "Path(" + repr(str(receipt)) + ").write_text(json.dumps({'ticks':6,'token':" + repr(token) + "}))"
            )
            body = "source scripts/runtime_env.sh\nrun_fintech_python -c " + shlex.quote(code)
            body += "\ntrial_exit=$?\nprintf '\\nPERSIST_" + token + "=%s\\n' \"$trial_exit\""
            started = time.perf_counter()
            submit(mode, shlex.join(["/bin/bash", "--noprofile", "--norc", "-c", body]), marker)
            dispatch = time.perf_counter() - started
            # The submitting CLI has exited; the pane owner must keep the work alive.
            text = collect(mode, marker)
            value = json.loads(receipt.read_bytes())
            assert value == {"ticks": 6, "token": token} and marker in text
            background.append({"mode": mode, "submitting_cli_exited_before_completion": True,
                "cli_dispatch_seconds": dispatch, "completion_seconds": time.perf_counter() - started,
                "all_owned_counter_output_verified": True, "interactive_disconnect_tested": False,
                "physical_host_reboot_tested": False})
        current = hashlib.sha256((ROOT / "scripts/runtime_env.sh").read_bytes() +
                                 (ROOT / "docs/agents/runtime.md").read_bytes()).hexdigest()
        assert current == expected, "compared project inputs changed"
        result = {"state": "accepted", "samples": samples, "background_processes": background,
            "median_seconds": {mode: statistics.median(row["seconds"] for row in samples if row["mode"] == mode)
                               for mode in ("bash", "zsh_bash_delegate", "tmux", "herdr")},
            "expected_complete_output_sha256": expected, "real_herdr_managed_context": True,
            "caller_pane": os.environ["HERDR_PANE_ID"], "session": args.session,
            "additional_ai_agents_started": 0, "production_services_changed": 0,
            "scope": "same canonical runtime discovery, actual project contract reads, full hashes, dispatch, wait and output verification"}
        atomic_write_json(output, result, durable=True)
    except Exception as error:
        atomic_write_json(output, {"state": "failed", "error_type": type(error).__name__, "error": str(error),
                                   "samples": samples, "background_processes": background}, durable=True)
        raise
    finally:
        invoke([*tmux, "kill-server"])
        invoke([*herdr, "tab", "close", tab])


def launch(args):
    parent = args.output.resolve().parent
    parent.mkdir(parents=True, mode=0o700, exist_ok=True)
    # Unix domain socket paths are bounded by sockaddr_un, even when the
    # evidence path itself is a valid filesystem path. Keep runtime paths short.
    config_dir = Path(tempfile.mkdtemp(prefix="sa-tools-"))
    bootstrap = config_dir / "shell.sh"
    config = config_dir / "config.toml"
    claim = config_dir / "root-started"
    command = shlex.join([str(ROOT / "scripts/benchmark_agent_terminal_tools.py"),
                          "--worker", "--output", str(args.output.resolve()), "--herdr", str(args.herdr),
                          "--session", args.session, "--repetitions", str(args.repetitions)])
    bootstrap.write_text("#!/bin/bash\n" +
        "if mkdir " + shlex.quote(str(claim)) + " 2>/dev/null; then\n" +
        "  cd " + shlex.quote(str(ROOT)) + "\n  source scripts/runtime_env.sh\n" +
        "  run_fintech_python " + command + " > " + shlex.quote(str(config_dir / "worker.log")) + " 2>&1\n" +
        "  " + shlex.join([str(args.herdr), "--session", args.session, "server", "stop"]) + "\n" +
        "fi\nexec /bin/bash --noprofile --norc\n")
    bootstrap.chmod(0o700)
    config.write_text("[terminal]\ndefault_shell = " + json.dumps(str(bootstrap)) +
                      "\nshell_mode = 'non_login'\n[update]\nversion_check = false\nmanifest_check = false\n"
                      "[session]\nresume_agents_on_restore = false\n")
    config.chmod(0o600)
    environment = {**os.environ, "HERDR_CONFIG_PATH": str(config), "XDG_CONFIG_HOME": str(config_dir),
                   "XDG_DATA_HOME": str(config_dir / "data"), "XDG_STATE_HOME": str(config_dir / "state")}
    master, slave = pty.openpty()
    fcntl.ioctl(slave, termios.TIOCSWINSZ, struct.pack("HHHH", 40, 120, 0, 0))
    environment["TERM"] = "xterm-256color"
    # A new TUI creates the first managed shell. A headless server can exist
    # without any workspace, so it cannot bootstrap an in-pane worker alone.
    with (config_dir / "server.log").open("wb") as log:
        server = subprocess.Popen([str(args.herdr), "--session", args.session], cwd=ROOT,
                                  env=environment, stdin=slave, stdout=slave, stderr=slave, start_new_session=True)
        os.close(slave)
        try:
            deadline = time.monotonic() + 90
            while not args.output.exists() and server.poll() is None and time.monotonic() < deadline:
                readable, _, _ = select.select([master], [], [], 0.1)
                if readable:
                    try:
                        log.write(os.read(master, 65536))
                        log.flush()
                    except OSError:
                        break
            if not args.output.exists():
                raise RuntimeError("owned Herdr worker did not produce a receipt: " +
                                   (config_dir / "server.log").read_text(errors="replace")[-1500:])
            result = json.loads(args.output.read_bytes())
            server.wait(timeout=15)
            if result["state"] != "accepted":
                raise RuntimeError(json.dumps(result))
            print(json.dumps({"state": result["state"], "median_seconds": result["median_seconds"],
                              "real_herdr_managed_context": True, "own_test_server_stopped": True}))
        finally:
            if server.poll() is None:
                server.terminate()
                server.wait(timeout=10)
            os.close(master)
            evidence_dir = parent / ("herdr-runtime-" + config_dir.name)
            evidence_dir.mkdir(mode=0o700)
            for name in ("config.toml", "server.log", "worker.log"):
                source = config_dir / name
                if source.exists():
                    destination = evidence_dir / name
                    destination.write_bytes(source.read_bytes())
                    destination.chmod(0o600)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--herdr", type=Path, required=True)
    parser.add_argument("--repetitions", type=int, default=5)
    parser.add_argument("--session", default="stockagent-owned-tool-trial-" + uuid.uuid4().hex[:8])
    parser.add_argument("--worker", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.output.exists() or not 1 <= args.repetitions <= 10:
        parser.error("use a fresh receipt and one to ten bounded repetitions")
    (worker if args.worker else launch)(args)


if __name__ == "__main__":
    main()

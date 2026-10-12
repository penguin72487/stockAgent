#!/usr/bin/env python3
"""Bounded encrypted loopback probe; no keys, public listeners or remote changes."""
import argparse
import fcntl
import json
import os
from pathlib import Path
import pty
import re
import select
import signal
import socket
import struct
import subprocess
import termios
import time
import uuid


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        parser.error("preserve prior probe receipts")
    nonce = uuid.uuid4().hex[:12]
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as reserve:
        reserve.bind(("127.0.0.1", 0))
        port = reserve.getsockname()[1]
    command = "printf 'READY_agent_trial\\n'\nIFS= read -r received\nprintf 'ACK_%s\\n' \"$received\"\nsleep 0.3"
    startup = subprocess.run(["mosh-server", "new", "-i", "127.0.0.1", "-p", str(port),
        "--", "/bin/bash", "--noprofile", "--norc", "-c", command],
        capture_output=True, text=True, timeout=10, check=True)
    handshake = re.search(r"MOSH CONNECT (\d+) (\S+)", startup.stdout)
    owner = re.search(r"pid\s*[=:]\s*(\d+)", startup.stderr)
    if not handshake or not owner or int(handshake[1]) != port:
        raise RuntimeError("loopback probe did not obtain its owned server identity")
    server_pid = int(owner[1])
    master, slave = pty.openpty()
    fcntl.ioctl(slave, termios.TIOCSWINSZ, struct.pack("HHHH", 40, 120, 0, 0))
    environment = {**os.environ, "MOSH_KEY": handshake[2], "TERM": "xterm-256color", "LC_ALL": "C.UTF-8"}
    started = time.perf_counter()
    client = subprocess.Popen(["mosh-client", "127.0.0.1", str(port)], env=environment,
                              stdin=slave, stdout=slave, stderr=slave, start_new_session=True)
    os.close(slave)
    screen = bytearray()

    def wait_text(text):
        deadline = time.monotonic() + 8
        while text.encode() not in screen and time.monotonic() < deadline:
            ready, _, _ = select.select([master], [], [], 0.1)
            if ready:
                try:
                    screen.extend(os.read(master, 65536))
                except OSError:
                    break
        if text.encode() not in screen:
            raise TimeoutError("encrypted loopback probe output did not match")

    try:
        wait_text("READY_agent_trial")
        handshake_seconds = time.perf_counter() - started
        os.kill(client.pid, signal.SIGSTOP)
        time.sleep(0.35)
        os.kill(client.pid, signal.SIGCONT)
        os.write(master, (nonce + "\n").encode())
        wait_text("ACK_" + nonce)
        result = {"state": "accepted", "bind_address": "127.0.0.1", "port": port,
                  "encrypted_client_server_exchange_verified": True,
                  "same_server_after_client_pause_resume": True, "client_pause_seconds": 0.35,
                  "first_visible_output_seconds": handshake_seconds,
                  "full_pause_resume_exchange_seconds": time.perf_counter() - started,
                  "remote_vast_udp_verified": False, "network_roaming_tested": False,
                  "key_saved_or_printed": False, "public_listener_created": False}
        args.output.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        args.output.write_text(json.dumps(result, indent=2))
        args.output.chmod(0o600)
        print(json.dumps(result))
    finally:
        if client.poll() is None:
            client.terminate()
            client.wait(timeout=5)
        os.close(master)
        command_line = Path(f"/proc/{server_pid}/cmdline")
        if command_line.exists():
            observed = command_line.read_bytes()
            if b"mosh-server" in observed and str(port).encode() in observed:
                os.kill(server_pid, signal.SIGTERM)


if __name__ == "__main__":
    main()

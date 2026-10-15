#!/usr/bin/env python3
"""Derive optional agent SSH multiplexing from the enrolled Vast endpoint."""
import argparse
import configparser
import hashlib
import json
import os
from pathlib import Path
import re
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from downloader.artifact_io import atomic_write_text

HEADER = "# Managed StockAgent agent SSH profile; derived from enrolled payload endpoint.\n"


def prepare(policy_path: Path, output: Path, control_dir: Path) -> dict:
    for path in (policy_path, output):
        if path.is_symlink():
            raise ValueError("SSH policy/profile is redirected")
    if output.exists() and not output.read_text().startswith(HEADER):
        raise ValueError("preserve the unrelated SSH profile")
    policy = json.loads(policy_path.read_bytes())
    source = Path(policy["rclone_config"])
    if source.is_symlink() or source.stat().st_mode & 0o077:
        raise ValueError("enrolled credential configuration is not private")
    parser = configparser.ConfigParser(interpolation=None)
    parser.read(source)
    remote = parser[policy["remote_name"]]
    if remote.get("type") != "sftp":
        raise ValueError("agent SSH needs the enrolled SFTP endpoint")
    fields = {key: remote[key] for key in ("host", "port", "user", "key_file", "known_hosts_file")}
    if (any("\n" in value or "\r" in value for value in fields.values())
            or not re.fullmatch(r"[A-Za-z0-9_.:-]+", fields["host"])
            or not re.fullmatch(r"[A-Za-z0-9_-]+", fields["user"])
            or not fields["port"].isdigit() or not 1 <= int(fields["port"]) <= 65535):
        raise ValueError("invalid enrolled SSH endpoint fields")
    for key in ("key_file", "known_hosts_file"):
        if not Path(fields[key]).is_absolute() or not Path(fields[key]).is_file():
            raise ValueError("SSH needs existing enrolled identity and host verification files")
    if control_dir.is_symlink():
        raise ValueError("SSH control directory is redirected")
    control_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    if control_dir.stat().st_uid != os.geteuid() or control_dir.stat().st_mode & 0o077:
        raise ValueError("SSH control directory is not owned privately")
    epoch = hashlib.sha256(json.dumps(fields, sort_keys=True).encode()).hexdigest()[:12]
    socket_template = str(control_dir / (epoch + "-%C"))
    if len(socket_template.replace("%C", "0" * 40).encode()) >= 104:
        raise ValueError("SSH control socket path is too long")
    body = HEADER + "Host stockagent-vast\n"
    for key, source_key in (("HostName", "host"), ("Port", "port"), ("User", "user"),
                            ("IdentityFile", "key_file"), ("UserKnownHostsFile", "known_hosts_file")):
        body += "  " + key + " " + json.dumps(fields[source_key], ensure_ascii=False) + "\n"
    body += ("  BatchMode yes\n  IdentitiesOnly yes\n  StrictHostKeyChecking yes\n"
             "  ConnectTimeout 8\n  ControlMaster auto\n  ControlPersist 60\n"
             "  ControlPath " + json.dumps(socket_template, ensure_ascii=False) +
             "\n  ServerAliveInterval 15\n  ServerAliveCountMax 3\n")
    atomic_write_text(output, body, durable=True)
    output.chmod(0o600)
    return {"state": "profile_prepared", "private_profile": str(output), "profile_sha256":
            hashlib.sha256(output.read_bytes()).hexdigest(), "control_persist_seconds": 60,
            "remote_configuration_changed": False}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--policy", type=Path, default=Path("/etc/stockagent/packed-rclone-transport.json"))
    parser.add_argument("--output", type=Path, default=Path("/etc/stockagent/agent-ssh.conf"))
    parser.add_argument("--control-dir", type=Path, default=Path("/run/stockagent-agent-ssh"))
    args = parser.parse_args()
    print(json.dumps(prepare(args.policy, args.output, args.control_dir)))


if __name__ == "__main__":
    main()

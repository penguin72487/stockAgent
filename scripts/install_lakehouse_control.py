#!/usr/bin/env python3
"""Prepare isolated PG catalogs and an official, PostgreSQL-backed Temporal server."""
from __future__ import annotations
import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import pwd
import secrets
import subprocess
import sys
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from downloader.artifact_io import atomic_write_json, atomic_write_text
from stockagent.runtime_identity import runtime_identity

COMMIT = "d94e34a1ebba5410a2e7d07119a76896909591aa"


def command(argv, *, body=None, env=None):
    result = subprocess.run(argv, input=body, text=True, capture_output=True, env=env, timeout=180)
    if result.returncode:
        raise RuntimeError("owned deployment operation failed: " + Path(argv[0]).name)
    return result.stdout


def admin(sql):
    return command(["runuser", "-u", "postgres", "--", "psql", "-X", "-At", "-v", "ON_ERROR_STOP=1", "-d", "postgres"], body=sql)


def download_schemas(output: Path):
    headers = {"User-Agent": "StockAgent-lakehouse-deployment"}
    url = "https://api.github.com/repos/temporalio/temporal/git/trees/" + COMMIT + "?recursive=1"
    tree = json.load(urllib.request.urlopen(urllib.request.Request(url, headers=headers), timeout=30))
    if tree["sha"] != COMMIT or tree.get("truncated"):
        raise ValueError("official pinned schema tree differs")
    members = [r for r in tree["tree"] if r["type"] == "blob" and r["path"].startswith("schema/postgresql/v12/")
               and "/versioned/" in r["path"]]
    def fetch(row):
        raw_url = "https://raw.githubusercontent.com/temporalio/temporal/" + COMMIT + "/" + row["path"]
        body = urllib.request.urlopen(urllib.request.Request(raw_url, headers=headers), timeout=30).read()
        if hashlib.sha1(b"blob " + str(len(body)).encode() + b"\0" + body).hexdigest() != row["sha"]:
            raise ValueError("official schema content differs from pinned Git blob")
        path = output / row["path"]
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(body)
        return {"path": row["path"], "git_blob_sha1": row["sha"], "sha256": hashlib.sha256(body).hexdigest()}
    with ThreadPoolExecutor(max_workers=8) as pool:
        receipts = list(pool.map(fetch, members))
    atomic_write_json(output / "schema-receipt.json", {"commit": COMMIT, "files": receipts})


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--role-prefix", type=Path, required=True)
    parser.add_argument("--binaries", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if os.geteuid() != 0 or Path(sys.prefix).resolve() != args.role_prefix.resolve():
        raise ValueError("use root with the selected independent Mamba lakehouse role")
    output = args.output.absolute()
    output.mkdir(parents=True, mode=0o700, exist_ok=False)
    config_path = Path("/etc/stockagent/lakehouse-control.json")
    if config_path.exists():
        raise ValueError("preserve an existing lakehouse control deployment")
    binaries = args.binaries.resolve(strict=True)
    pin = json.loads((binaries / "acceptance.json").read_bytes())
    if pin["state"] != "accepted":
        raise ValueError("official binary installation not accepted")
    for release in pin["releases"].values():
        for name, row in release["executables"].items():
            if hashlib.sha256((binaries / "bin" / name).read_bytes()).hexdigest() != row["sha256"]:
                raise ValueError("official executable changed")
    binaries.chmod(0o755)
    before = command(["systemctl", "show", "postgresql@18-main", "-p", "MainPID"])
    names = {"stockagent_lake", "stockagent_temporal"}
    dbs = {"stockagent_ducklake", "stockagent_temporal", "stockagent_temporal_visibility"}
    existing = set(admin("SELECT rolname FROM pg_roles;\n").splitlines())
    existing_dbs = set(admin("SELECT datname FROM pg_database;\n").splitlines())
    if names & existing or dbs & existing_dbs:
        raise ValueError("unowned target role/database already exists; no overwrite")
    passwords = {name: secrets.token_hex(32) for name in names}
    for name in sorted(names):
        admin("CREATE ROLE " + name + " LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE CONNECTION LIMIT 40 PASSWORD '" + passwords[name] + "';\n")
    for name in sorted(dbs):
        role = "stockagent_lake" if name == "stockagent_ducklake" else "stockagent_temporal"
        admin("CREATE DATABASE " + name + " OWNER " + role + ";\nREVOKE CONNECT ON DATABASE " + name + " FROM PUBLIC;\nGRANT CONNECT ON DATABASE " + name + " TO " + role + ";\n")
    download_schemas(output)
    for db, kind in (("stockagent_temporal", "temporal"), ("stockagent_temporal_visibility", "visibility")):
        environment = {**os.environ, "SQL_HOST": "127.0.0.1", "SQL_PORT": "5432", "SQL_PLUGIN": "postgres12",
                       "SQL_USER": "stockagent_temporal", "SQL_PASSWORD": passwords["stockagent_temporal"], "SQL_DATABASE": db}
        tool = str(binaries / "bin" / "temporal-sql-tool")
        command([tool, "setup-schema", "-v", "0.0"], env=environment)
        command([tool, "update-schema", "-d", str(output / "schema/postgresql/v12" / kind / "versioned")], env=environment)
    try:
        user = pwd.getpwnam("stockagent-temporal")
    except KeyError:
        command(["useradd", "--system", "--home-dir", "/var/lib/stockagent/temporal", "--shell", "/usr/sbin/nologin", "stockagent-temporal"])
        user = pwd.getpwnam("stockagent-temporal")
    server_root = Path("/var/lib/stockagent/temporal")
    server_root.mkdir(parents=True, mode=0o750, exist_ok=True)
    os.chown(server_root, user.pw_uid, user.pw_gid)
    server_config = Path("/etc/stockagent/temporal/stockagent.yaml")
    server_config.parent.mkdir(parents=True, mode=0o750, exist_ok=True)
    os.chown(server_config.parent, 0, user.pw_gid)
    import yaml
    stores = {}
    for kind, db in (("default", "stockagent_temporal"), ("visibility", "stockagent_temporal_visibility")):
        stores[kind] = {"sql": {"pluginName": "postgres12", "databaseName": db, "connectAddr": "127.0.0.1:5432",
                               "connectProtocol": "tcp", "user": "stockagent_temporal", "password": passwords["stockagent_temporal"],
                               "maxConns": 4, "maxIdleConns": 2, "maxConnLifetime": "1h"}}
    server = {"log": {"stdout": True, "level": "warn"},
              "persistence": {"defaultStore": "default", "visibilityStore": "visibility", "numHistoryShards": 4, "datastores": stores},
              "global": {"membership": {"maxJoinDuration": "30s", "broadcastAddress": "127.0.0.1"},
                         "metrics": {"prometheus": {"framework": "tally", "listenAddress": "127.0.0.1:9723"}}},
              "services": {name: {"rpc": {"grpcPort": port, "membershipPort": port - 300, "bindOnLocalHost": True}}
                           for name, port in (("frontend", 7233), ("history", 7234), ("matching", 7235), ("worker", 7239))},
              "clusterMetadata": {"enableGlobalNamespace": False, "failoverVersionIncrement": 10, "masterClusterName": "stockagent",
                                  "currentClusterName": "stockagent", "clusterInformation": {"stockagent": {"enabled": True,
                                  "initialFailoverVersion": 1, "rpcName": "frontend", "rpcAddress": "127.0.0.1:7233"}}},
              "dcRedirectionPolicy": {"policy": "noop"}, "archival": {"history": {"state": "disabled"}, "visibility": {"state": "disabled"}}}
    atomic_write_text(server_config, yaml.safe_dump(server, sort_keys=False))
    server_config.chmod(0o640)
    os.chown(server_config, 0, user.pw_gid)
    state = Path("/var/lib/stockagent/lakehouse-control")
    state.mkdir(parents=True, mode=0o700, exist_ok=True)
    runtime = state / "runtime-lock.json"
    atomic_write_json(runtime, runtime_identity())
    configuration = {"schema_version": 1, "role_prefix": str(args.role_prefix.resolve()), "runtime_lock": str(runtime),
                     "binaries": str(binaries), "extensions": str(state / "extensions"), "state_root": str(state),
                     "catalog_database": "stockagent_ducklake", "catalog_role": "stockagent_lake", "catalog_password": passwords["stockagent_lake"],
                     "lake_root": "/srv/stockagent-d-volume/stockagent-immutable-lake", "cold_root": "/srv/stockagent-packed",
                     "transport_root": "/srv/stockagent-backup-ingress-lab203/lakehouse", "receipt_root": "/srv/stockagent-backup-receipts-lab203/lakehouse",
                     "temporal_endpoint": "127.0.0.1:7233", "temporal_namespace": "stockagent-storage", "task_queue": "stockagent-storage-v1",
                     "producer_device_id": "QZTXXEL-YBCBYK7-ZK2ZSMS-DQKVSCE-IFC7MIX-4CWG6LE-KHPDLJI-7DSE6QW",
                     "receiver_device_id": "TLOI2HH-6EZOSBC-H6YMGVW-APQOGFW-P2A7FZT-EKX6IUP-2HMRKS6-LBDHOQM"}
    atomic_write_json(config_path, configuration)
    config_path.chmod(0o600)
    unit = """[Unit]
Description=StockAgent PostgreSQL-backed Temporal lifecycle server
Requires=postgresql@18-main.service
After=postgresql@18-main.service network.target
StartLimitIntervalSec=0
[Service]
Type=simple
User=stockagent-temporal
Group=stockagent-temporal
WorkingDirectory=/var/lib/stockagent/temporal
ExecStart=__BINARY__ --config-file /etc/stockagent/temporal/stockagent.yaml --allow-no-auth start
Restart=on-failure
RestartSec=3
TimeoutStopSec=60
UMask=0077
NoNewPrivileges=true
ProtectSystem=strict
ProtectHome=true
PrivateTmp=true
ReadWritePaths=/var/lib/stockagent/temporal
CPUQuota=150%
MemoryHigh=768M
MemoryMax=1536M
TasksMax=256
Environment=GOMAXPROCS=2
[Install]
WantedBy=multi-user.target
""".replace("__BINARY__", str(binaries / "bin" / "temporal-server"))
    unit_path = Path("/etc/systemd/system/stockagent-temporal.service")
    if unit_path.exists():
        raise ValueError("preserve unknown Temporal service definition")
    atomic_write_text(unit_path, unit)
    command(["systemctl", "daemon-reload"])
    command(["systemctl", "enable", "--now", "stockagent-temporal.service"])
    after = command(["systemctl", "show", "postgresql@18-main", "-p", "MainPID"])
    if before != after:
        raise ValueError("existing control database unexpectedly restarted")
    receipt = {"state": "installed", "observed_at_utc": datetime.now(timezone.utc).isoformat(),
               "databases": sorted(dbs), "roles": sorted(names), "postgresql_restarted": False,
               "production_server": True, "development_server": False, "loopback_only": True,
               "runtime_sha256": runtime_identity()["sha256"], "service": "stockagent-temporal.service"}
    atomic_write_json(output / "installation.json", receipt)
    print(json.dumps(receipt))


if __name__ == "__main__":
    main()

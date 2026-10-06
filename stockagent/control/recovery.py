"""Verify a portable control dump in a new private, socket-only PG cluster."""
from __future__ import annotations

import json
import os
from pathlib import Path
import pwd
import shlex
import shutil
import subprocess
import tempfile
import time

from stockagent.control.backup import logical_database_state
from stockagent.data_sync.backup_auxiliary import verified_control
from stockagent.data_sync.offhost_backup import private_json


def private_socket_directory(scratch: Path, port: int) -> tuple[Path, bool]:
    """Keep long artifact identities out of the Unix socket's path budget."""
    candidate = scratch / "socket"
    if len(os.fsencode(candidate / f".s.PGSQL.{port}")) <= 100:
        candidate.mkdir(mode=0o700)
        return candidate, False
    short_root = Path("/tmp")
    if any(p.is_symlink() for p in (short_root, *short_root.parents)) or not short_root.is_dir():
        raise ValueError("private short socket parent must be an existing unredirected directory")
    socket = Path(tempfile.mkdtemp(prefix="sa-pg-", dir=short_root))
    socket.chmod(0o700)
    return socket, True


def restore_control(control_root: Path, scratch: Path, *, pg_bin: Path | None = None, validator=None) -> dict:
    """No TCP, production DB connection, production role or existing cluster."""
    receipt, archive, expected = verified_control(control_root)
    if scratch.exists() or any(p.is_symlink() for p in (scratch, *scratch.parents)):
        raise ValueError("logical restore requires a fresh independent private cluster")
    if not pg_bin:
        config = shutil.which("pg_config")
        if not config:
            raise ValueError("use the isolated accepted Mamba control-recovery role")
        pg_bin = Path(subprocess.check_output([config, "--bindir"], text=True).strip())
    for binary in ("initdb", "pg_ctl", "pg_restore"):
        if not (pg_bin / binary).is_file():
            raise ValueError("control-recovery PostgreSQL binaries are incomplete")
    scratch.mkdir(mode=0o700, parents=True)
    port = 55479  # Private socket directory, no network listener.
    data = scratch / "data"
    socket, external_socket = private_socket_directory(scratch, port)
    private_json(scratch / "socket-location.private.json", {"path": str(socket), "external_short_directory": external_socket})
    account = pwd.getpwnam("nobody") if os.geteuid() == 0 else None
    if account:
        # Only these new directories change ownership. Existing parent trees
        # and the source backup stay private and untouched.
        os.chown(scratch, account.pw_uid, account.pw_gid)
        os.chown(socket, account.pw_uid, account.pw_gid)
    prefix = ["runuser", "-u", "nobody", "--"] if account else []
    logfile = scratch / "postgres.log"
    diagnostics = scratch / "restore-diagnostics.private.log"
    def execute(argv, *, server=False):
        result = subprocess.run([*(prefix if server else []), *map(str, argv)], capture_output=True, timeout=180)
        if result.returncode:
            with diagnostics.open("ab") as log:
                diagnostics.chmod(0o600)
                log.write(result.stdout + result.stderr)
            raise RuntimeError("isolated PostgreSQL restore failed; diagnostics remain private")
        return result
    started = time.perf_counter()
    running = False
    try:
        execute([pg_bin / "initdb", "-D", data, "-A", "trust", "-U", "stockagent_restore", "--no-locale", "-E", "UTF8"], server=True)
        options = shlex.join(["-k", str(socket), "-p", str(port), "-c", "listen_addresses=", "-c", "unix_socket_permissions=0700"])
        execute([pg_bin / "pg_ctl", "-D", data, "-l", logfile, "-w", "start", "-o", options], server=True)
        running = True
        import psycopg
        connection_args = {"host": str(socket), "port": port, "user": "stockagent_restore"}
        with psycopg.connect(dbname="postgres", autocommit=True, **connection_args) as admin:
            admin.execute("CREATE DATABASE stockagent_restore")
        execute([pg_bin / "pg_restore", "--exit-on-error", "--no-owner", "--no-privileges",
                 "--host", socket, "--port", port, "--username", "stockagent_restore",
                 "--dbname", "stockagent_restore", archive])
        with psycopg.connect(dbname="stockagent_restore", **connection_args) as database:
            with database.transaction():
                database.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
                actual = logical_database_state(database)
        if actual != expected:
            raise ValueError("restored control columns/rows differ from the same-MVCC source state")
        # A fixed locally installed validator can read another catalog's data
        # before this private cluster stops. Never deserialize synced callbacks.
        additional = validator({**connection_args, "dbname": "stockagent_restore"}) if validator else {}
        protected = {"state", "source_archive_sha256", "logical_state_identity_sha256", "table_count", "row_count",
                     "no_tcp_listener", "private_new_cluster", "production_database_modified", "complete_restore_seconds",
                     "nas_provenance_verified_by_this_tool"}
        if not isinstance(additional, dict) or protected.intersection(additional):
            raise ValueError("restore validator cannot replace canonical recovery proof")
        result = {"state": "isolated_control_logical_restore_verified", "source_archive_sha256": receipt["sha256"],
            "logical_state_identity_sha256": actual["identity_sha256"], "table_count": actual["table_count"],
            "row_count": actual["row_count"], "no_tcp_listener": True, "private_new_cluster": True,
            "production_database_modified": False, "complete_restore_seconds": time.perf_counter() - started,
            "nas_provenance_verified_by_this_tool": False, **additional}
        private_json(scratch / "logical-restore.json", result)
        return result
    finally:
        if running:
            execute([pg_bin / "pg_ctl", "-D", data, "-w", "stop", "-m", "fast"], server=True)
        if external_socket:
            try:
                socket.rmdir()  # Remove only the now-empty private socket directory.
            except OSError as error:
                private_json(scratch / "socket-cleanup.private.json", {"state": "retained_nonempty_socket_directory",
                    "path": str(socket), "error_type": type(error).__name__})
        # Retain this run's receipts/cluster for inspection. Never delete a
        # caller's source, backup, private key, existing cluster or NAS snapshot.

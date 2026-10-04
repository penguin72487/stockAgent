"""Recovery admission must bind private archive bytes to one logical snapshot."""
import hashlib
import json
import os
from pathlib import Path
import socket

import pytest

from scripts.verify_control_plane_offhost_restore import recorded_backup
from stockagent.runtime_identity import identity_sha256
from stockagent.control.recovery import private_socket_directory


@pytest.fixture
def backup_receipt(tmp_path):
    archive=tmp_path/'source.backup';archive.write_bytes(b'example private archive')
    state=tmp_path/'source.state.json'
    body={'contract':'example','tables':[]}
    state.write_text(json.dumps({**body,'identity_sha256':identity_sha256(body),'table_count':0,'row_count':0}))
    for file in (archive,state):file.chmod(0o600)
    receipt=tmp_path/'source.receipt.json'
    receipt.write_text(json.dumps({'state':'backup_written','same_mvcc_snapshot_as_dump':True,
       'archive':str(archive),'logical_state_file':str(state),
       'sha256':hashlib.sha256(archive.read_bytes()).hexdigest(),
       'logical_state_file_sha256':hashlib.sha256(state.read_bytes()).hexdigest(),
       'logical_state_identity_sha256':identity_sha256(body)}))
    return receipt,archive,state


@pytest.mark.parametrize('alteration',['archive_corrupt','state_corrupt','public_archive','unbound_snapshot','foreign_archive'])
def test_recovery_rejects_unbound_or_unprotected_backup(backup_receipt,alteration):
    receipt,archive,state=backup_receipt
    recorded_backup(receipt)
    if alteration=='archive_corrupt':archive.write_bytes(b'changed')
    elif alteration=='state_corrupt':state.write_bytes(b'changed')
    elif alteration=='public_archive':archive.chmod(0o644)
    else:
        body=json.loads(receipt.read_bytes())
        if alteration=='unbound_snapshot':body['same_mvcc_snapshot_as_dump']=False
        else:
            foreign=receipt.parent/'other';foreign.mkdir()
            file=foreign/'source.backup';file.write_bytes(archive.read_bytes());file.chmod(0o600)
            body['archive']=str(file)
        receipt.write_text(json.dumps(body))
    with pytest.raises(ValueError):recorded_backup(receipt)


def test_long_recovery_identity_gets_a_short_private_working_unix_socket(tmp_path):
    scratch = tmp_path / ("a" * 80) / ("b" * 80)
    scratch.mkdir(parents=True)
    directory, external = private_socket_directory(scratch, 55479)
    endpoint = directory / ".s.PGSQL.55479"
    try:
        assert external and directory.stat().st_mode & 0o777 == 0o700
        assert len(os.fsencode(endpoint)) <= 100
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as server:
            server.bind(str(endpoint))
    finally:
        endpoint.unlink(missing_ok=True)
        directory.rmdir()
    assert scratch.exists()


def test_long_recovery_runs_do_not_share_a_socket_directory(tmp_path):
    scratch = tmp_path / ("same-identity-" + "a" * 80)
    scratch.mkdir()
    first, _ = private_socket_directory(scratch, 55479)
    second, _ = private_socket_directory(scratch, 55479)
    try:
        assert first != second
    finally:
        first.rmdir(); second.rmdir()

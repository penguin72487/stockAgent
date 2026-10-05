import json
import os
from pathlib import Path
import sys
import time

import pytest

from scripts import receive_vast_bulk_archives as receiver
from stockagent.data_sync.desync_snapshots import SnapshotError


@pytest.fixture(autouse=True)
def preserve_process_umask():
    original = os.umask(0o022)
    os.umask(original)
    yield
    os.umask(original)


def test_apply_cache_transport_uses_separate_safe_owner(tmp_path, monkeypatch):
    landing = tmp_path / "incoming"
    locks = tmp_path / "locks"
    locks.mkdir()
    inventory = tmp_path / "inventory.json"
    inventory.write_text(json.dumps({"schema_version": 1, "observed_at_epoch": time.time(), "caches": []}))
    monkeypatch.setattr(receiver, "D_LANDING", landing)
    monkeypatch.setattr(receiver, "TRANSPORT_LOCK_ROOT", locks)
    monkeypatch.setattr(receiver.socket, "gethostname", lambda: "penguin")
    monkeypatch.setattr(receiver, "_check_d_primary_mount", lambda *_: None)
    calls = []
    def received(args, scope, directory):
        calls.append(scope)
        return {"producer_exit_code": 0, "received_bytes": 10}
    monkeypatch.setattr(receiver, "receive", received)
    monkeypatch.setattr(sys, "argv", ["receiver", "--scope", "cache", "--cache-inventory", str(inventory), "--apply"])
    assert receiver.main() == 0
    assert calls == ["cache"]
    assert (locks / "stockagent-vast-bulk-cache-transport.lock").is_file()
    assert not (locks / "stockagent-vast-bulk-return-transport.lock").exists()
    intent = json.loads(next(landing.glob("*/intent.json")).read_text())
    assert intent["state"] == "bulk_transport_complete" and intent["source_cleanup"] is False


def test_default_dry_run_only_plans_original_artifact_scopes(tmp_path, monkeypatch):
    monkeypatch.setattr(receiver, "D_LANDING", tmp_path / "incoming")
    monkeypatch.setattr(receiver.socket, "gethostname", lambda: "penguin")
    monkeypatch.setattr(receiver, "_check_d_primary_mount", lambda *_: None)
    monkeypatch.setattr(sys, "argv", ["receiver"])
    assert receiver.main() == 0
    assert not (tmp_path / "incoming").exists()
    assert receiver.DEFAULT_SCOPES == ("markets", "ablations")


def test_cache_code_requires_current_full_inventory():
    with pytest.raises(SnapshotError):
        receiver.cache_producer_code(8, None)
    with pytest.raises(SnapshotError):
        receiver.cache_producer_code(8, {"schema_version": 1, "observed_at_epoch": time.time() - 7201})
    code = receiver.cache_producer_code(8, {"schema_version": 1, "observed_at_epoch": time.time(), "caches": []})
    compile(code, "private-cache-producer", "exec")
    assert "active_configuration_references" in code
    assert "metadata_tree(Path(path))!=before" in code
    assert "delete=atime,delete=ctime" in code
    assert "explicit_protected_roots" in code
    assert "requested_roots" in code


@pytest.mark.parametrize("scope", ["markets", "ablations"])
def test_producer_never_creates_full_archive_remote(scope):
    code = receiver.producer_code(scope, 8)
    compile(code, "private-artifact-producer", "exec")
    assert "'-cf','-'" in code
    assert "--one-file-system" in code and "delete=atime,delete=ctime" in code
    assert "tar.zst" not in code


@pytest.mark.parametrize('scope', ['markets','ablations','cache'])
def test_resume_requires_compressed_prefix_sha_before_stream_marker(scope):
    inventory = {'schema_version':1,'observed_at_epoch':time.time(),'caches':[]}
    body = receiver.producer_code(scope, 8, inventory)
    code = receiver.resume_producer(body, 7170292944, 'a'*64)
    compile(code, 'private-prefix-verified-resume', 'exec')
    assert code.count('forward_resume(zstd.stdout,tar,zstd)') == 1
    assert 'stdout=sys.stdout.buffer' not in code
    assert 'compressed prefix differs; retained partial is untouched' in code
    assert code.count('sys.stdout.buffer.write(') == 2


def test_resume_does_not_accept_unverified_prefix():
    with pytest.raises(SnapshotError):
        receiver.resume_producer(receiver.producer_code('markets',8),1,'invalid')


def test_reuse_prefix_expectation_still_keeps_both_fresh_actual_byte_gates(tmp_path,monkeypatch):
    receipt=tmp_path/'cache.receipt.json'
    receipt.write_text(json.dumps({'authority_node_id':'penguin','origin_node_id':'vastai1T',
                                  'scope':'cache','resumed_prefix_bytes':7170292944,'resumed_prefix_sha256':'a'*64}))
    monkeypatch.setattr(receiver,'hash_file',lambda *_: (_ for _ in ()).throw(AssertionError('redundant third read')))
    expected=receiver.retained_prefix_sha(tmp_path/'cache.tar.zst.partial',receipt,7170292944,'cache')
    assert expected=='a'*64
    body=receiver.resume_producer(receiver.producer_code('markets',8),7170292944,expected)
    assert "value.hexdigest()!=" in body and 'retained partial is untouched' in body
    from stockagent.data_sync.windows_cold_io import SCRIPT
    code=SCRIPT.read_text()
    assert "prefixHex -ne [string]$config.prefix_sha256" in code
    assert code.index('Retained prefix SHA differs')<code.index('native_writer_ready')<code.index('$inputBytes.Read')


def test_failed_native_writer_retains_receipt_and_owned_child_is_stopped(tmp_path, monkeypatch):
    import io
    from types import SimpleNamespace
    class Process:
        def __init__(self, *args, **kwargs):
            self.stdin=io.BytesIO();self.stdout=io.BytesIO(receiver.MAGIC+b'payload')
            self.code=None
        def poll(self): return self.code
        def terminate(self): self.code=-15
        def wait(self, **kwargs): return self.code or 0
    class Writer:
        def __init__(self,path,**kwargs):
            self.path=path
            path.write_bytes(b'part')
        def write(self,block): raise SnapshotError('injected native I/O failure')
        def close(self): pass
    args=SimpleNamespace(cache_inventory=None,resume_batch=None,threads=8,ssh_target='root@host',
                         ssh_port=40032,identity_file=Path('/private-key'),reserve_bytes=32*1024**3)
    monkeypatch.setattr(receiver.subprocess,'Popen',Process)
    monkeypatch.setattr(receiver,'BinaryWriter',Writer)
    monkeypatch.setattr(receiver,'_ssh_base',lambda *_: ['ssh'])
    value=receiver.receive(args,'markets',tmp_path)
    assert value['state']=='transport_failed_partial_retained'
    assert value['cold_accepted'] is False and value['source_deletion_authorized'] is False
    assert (tmp_path/'markets.tar.zst.partial').read_bytes()==b'part'
    assert json.loads((tmp_path/'markets.receipt.json').read_text())==value

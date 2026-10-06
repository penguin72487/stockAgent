import hashlib
import io
import json
from pathlib import Path

import pytest

from stockagent.data_sync import windows_cold_io as native
from stockagent.data_sync.desync_snapshots import SnapshotError


def test_non_d_read_and_hash_use_exact_regular_bytes(tmp_path):
    path = tmp_path / 'bytes.bin'
    value = bytes(range(256)) * 4097
    path.write_bytes(value)
    assert native.windows_path(path) is None
    assert native.hash_file(path) == hashlib.sha256(value).hexdigest()


@pytest.mark.parametrize('failure',['startup-once','startup-exhausted','nonempty-failure','other-error','empty-file'])
def test_read_retry_only_accepts_empty_known_interop_startup_and_preserves_first_byte(monkeypatch,failure):
    value=b'\x00\xffevery byte matters'
    launched=[]
    class Process:
        def __init__(self,data,code):
            self.stdin=io.BytesIO();self.stdout=io.BufferedReader(io.BytesIO(data));self.code=code
        def wait(self,**kwargs):return self.code
        def poll(self):return self.code
    def launch(*args,**kwargs):
        index=len(launched)
        fail=failure=='startup-exhausted' or index==0 and failure not in {'empty-file'}
        data=(value if failure=='nonempty-failure' else b'') if fail else (b'' if failure=='empty-file' else value)
        error=b'<3>WSL (123) ERROR: UtilAcceptVsock:273: accept4 failed 110' if failure!='other-error' else b'file is unavailable'
        process=Process(data,1 if fail else 0);launched.append(process)
        return process,io.BytesIO(error if fail else b'')
    monkeypatch.setattr(native,'windows_path',lambda p:'D:\\stockagent-cold-primary\\fixed')
    monkeypatch.setattr(native,'_process',launch)
    monkeypatch.setattr(native.time,'sleep',lambda n:None)
    if failure in {'startup-exhausted','nonempty-failure','other-error'}:
        with pytest.raises(SnapshotError):
            with native.binary_reader(Path('/srv/stockagent-packed/fixed')) as stream:stream.read()
        assert len(launched)==(3 if failure=='startup-exhausted' else 1)
    else:
        with native.binary_reader(Path('/srv/stockagent-packed/fixed')) as stream:
            assert stream.read()==(b'' if failure=='empty-file' else value)
        assert len(launched)==(2 if failure=='startup-once' else 1)
    assert all(p.stdout.closed for p in launched)


def test_d_mapping_cannot_silently_normalize_parent_traversal(monkeypatch):
    monkeypatch.setattr(Path, 'resolve', lambda self: self)
    monkeypatch.setattr(Path, 'is_symlink', lambda self: False)
    assert native.windows_path(Path('/mnt/d/stockagent-cold-primary/packed/objects/a.blob')) == (
        'D:\\stockagent-cold-primary\\packed\\objects\\a.blob')
    with pytest.raises(SnapshotError, match='Unsafe'):
        native.windows_path(Path('/mnt/d/stockagent-cold-primary/packed/../outside.bin'))


def test_d_redirect_rejected_before_native_process_launch(monkeypatch):
    monkeypatch.setattr(Path, 'resolve', lambda self: Path('/redirected'))
    with pytest.raises(SnapshotError, match='Redirected'):
        native.windows_path(Path('/srv/stockagent-packed/objects/a.blob'))


def test_partial_append_needs_exact_sha_before_native_process():
    with pytest.raises(SnapshotError, match='prefix'):
        native.BinaryWriter(Path('/mnt/d/stockagent-cold-primary/incoming/a'), offset=1)


@pytest.mark.parametrize('failure', ['startup-once', 'startup-exhausted', 'created-partial',
                                    'nonempty', 'existing', 'append', 'other-error'])
def test_writer_retry_requires_empty_startup_no_input_and_absent_fresh_destination(tmp_path, monkeypatch, failure):
    path=tmp_path/'fresh.partial';launched=[]
    if failure in ('existing','append'):path.write_bytes(b'fixed-prefix')
    offset=12 if failure=='append' else 0
    class Process:
        def __init__(self, output, code):
            self.stdin=io.BytesIO();self.stdout=io.BytesIO(output);self.code=code
        def wait(self,**kwargs):return self.code
        def poll(self):return self.code
    def launch(*args,**kwargs):
        fail=failure!='startup-once' or not launched
        ready=(b'nonempty invalid readiness' if failure=='nonempty' else b'') if fail else (
            json.dumps({'state':'native_writer_ready','offset':offset}).encode()+b'\n')
        if failure=='created-partial':path.write_bytes(b'partial to retain')
        p=Process(ready,1 if fail else 0);launched.append(p)
        stderr=(b'file access denied' if failure=='other-error' else
                b'<3>WSL ERROR: UtilAcceptVsock:273: accept4 failed 110')
        return p,io.BytesIO(stderr if fail else b'')
    monkeypatch.setattr(native,'_process',launch)
    monkeypatch.setattr(native.time,'sleep',lambda *a:None)
    if failure=='startup-once':
        writer=native.BinaryWriter(path);assert len(launched)==2 and writer.appended==0
        assert writer.process.stdin.getvalue()==b''
        writer.close();writer.process.stdin.close()
    else:
        with pytest.raises(SnapshotError,match='writer not ready'):
            native.BinaryWriter(path,offset=offset,prefix_sha256=('a'*64 if offset else None))
        assert len(launched)==(3 if failure=='startup-exhausted' else 1)
    assert all(p.stdout.closed for p in launched)
    if failure in ('existing','append'):assert path.read_bytes()==b'fixed-prefix'
    if failure=='created-partial':assert path.read_bytes()==b'partial to retain'


def test_native_io_uses_binary_filestream_not_9p_copy():
    code = native.SCRIPT.read_text()
    assert 'OpenStandardInput' in code and 'OpenStandardOutput' in code
    assert '[IO.FileMode]::CreateNew' in code and '$stream.Flush($true)' in code
    assert 'Retained prefix SHA differs' in code and 'AvailableFreeSpace' in code
    assert 'Copy-Item' not in code and 'Set-Content' not in code
    # Prefixes exceed 2 GiB: avoid PowerShell choosing Math.Min(Int32,Int32).
    assert '[Math]::Min([long]$buffer.Length, [long]($offset - $total))' in code


def test_only_enrolled_backup_transport_maps_to_windows(monkeypatch):
    monkeypatch.setattr(Path, 'resolve', lambda self: self)
    monkeypatch.setattr(Path, 'is_symlink', lambda self: False)
    assert native.windows_path(Path('/srv/stockagent-backup-ingress-lab203/.staging/fixed/member')) == (
        'D:\\stockagent-backup-ingress-lab203\\.staging\\fixed\\member')
    assert native.windows_path(Path('/srv/unrelated-backup/member')) is None


@pytest.mark.parametrize('damage', ['changed_source', 'wrong_sha', 'copy_failure'])
def test_native_copy_retains_partial_and_rejects_changed_source_or_wrong_proof(tmp_path, monkeypatch, damage):
    source, destination = tmp_path / 'source', tmp_path / 'staging/member.partial'
    value = b'fixed authoritative input'
    source.write_bytes(value)
    sha = hashlib.sha256(value).hexdigest()
    class Process:
        def __init__(self):
            self.stdin = io.BytesIO()
            self.stdout = io.BytesIO(json.dumps({'bytes': len(value), 'sha256': sha if damage != 'wrong_sha' else '0' * 64,
                                                'flushed': True}).encode())
        def wait(self, **kwargs): return 2 if damage == 'copy_failure' else 0
        def poll(self): return self.wait()
    def run(*args, **kwargs):
        destination.write_bytes(value)
        if damage == 'changed_source':source.write_bytes(b'changed primary')
        return Process(), io.BytesIO()
    monkeypatch.setattr(native, '_process', run)
    with pytest.raises(SnapshotError):
        native.copy_verified(source, destination, expected_sha256=sha, expected_bytes=len(value))
    assert destination.exists()


def test_native_copy_requires_regular_exact_source_before_process(tmp_path, monkeypatch):
    source = tmp_path / 'source';source.write_bytes(b'real bytes')
    monkeypatch.setattr(native, '_process', lambda *a, **k: pytest.fail('launched without an exact source'))
    with pytest.raises(SnapshotError, match='exact regular'):
        native.copy_verified(source, tmp_path / 'target', expected_sha256='a' * 64, expected_bytes=99)


def test_native_copy_is_exclusive_flushes_and_verifies_exact_source():
    code = native.SCRIPT.read_text()
    assert "mode -eq 'copy'" in code
    assert 'expected_sha256' in code and 'expected_bytes' in code and '.staging\\' in code
    assert '[IO.FileMode]::CreateNew' in code and '$stream.Flush($true)' in code


@pytest.mark.parametrize('damage', ['source_change', 'wrong_size'])
def test_native_hash_batch_cannot_accept_mutation_or_truncated_member(tmp_path, monkeypatch, damage):
    paths = [tmp_path / str(i) for i in range(2)]
    for p in paths:p.write_bytes(b'original exact bytes')
    monkeypatch.setattr(native, '_configuration', lambda p, **k: k)
    def batch(configs):
        if damage == 'source_change':paths[1].write_bytes(b'mutated bytes')
        return [{'bytes': c['expected_bytes'] - (damage == 'wrong_size'), 'sha256': 'a' * 64} for c in configs]
    monkeypatch.setattr(native, '_batch', batch)
    with pytest.raises(SnapshotError, match='changed'):
        native.hash_many(paths)


def test_native_copy_batch_failure_keeps_every_partial(tmp_path, monkeypatch):
    members = []
    for i in range(2):
        source, dest = tmp_path / f'source-{i}', tmp_path / f'partial-{i}'
        source.write_bytes(b'preserved immutable bytes')
        members.append((source, dest, hashlib.sha256(source.read_bytes()).hexdigest(), source.stat().st_size))
    monkeypatch.setattr(native, '_configuration', lambda p, **k: k)
    def batch(configs):
        for source, dest, _, _ in members:dest.write_bytes(source.read_bytes())
        raise SnapshotError('batch stopped after writing partials')
    monkeypatch.setattr(native, '_batch', batch)
    with pytest.raises(SnapshotError):native.copy_many(members)
    assert all(source.exists() and dest.exists() for source, dest, _, _ in members)


def test_native_batch_requires_one_result_for_every_member(monkeypatch):
    class Process:
        returncode = 0
        stdout = io.BytesIO()
        def communicate(self, *args, **kwargs):return b'{"bytes": 1, "sha256": "a"}\n', None
        def poll(self):return 0
    monkeypatch.setattr(native, '_launch', lambda *a, **k: (Process(), io.BytesIO()))
    with pytest.raises(SnapshotError, match='omitted'):
        native._batch([{'mode': 'hash', 'expected_bytes': 1}] * 2)


@pytest.mark.parametrize('failure',['startup-once','startup-exhausted','nonempty-failure','copy-startup','other-error'])
def test_native_batch_retries_only_an_empty_read_only_startup(monkeypatch,failure):
    launched=[]
    row=json.dumps({'bytes':5,'sha256':'a'*64}).encode()+b'\n'
    class Process:
        def __init__(self,output,code):
            self.stdout=io.BytesIO();self.output=output;self.returncode=code
        def communicate(self,*args,**kwargs):return self.output,None
        def poll(self):return self.returncode
    def launch(*args,**kwargs):
        fail=failure=='startup-exhausted' or not launched
        output=(row if failure=='nonempty-failure' else b'') if fail else row*2
        process=Process(output,1 if fail else 0);launched.append(process)
        error=(b'file unavailable' if failure=='other-error' else
               b'<3>WSL ERROR: UtilAcceptVsock:273: accept4 failed 110')
        return process,io.BytesIO(error if fail else b'')
    monkeypatch.setattr(native,'_launch',launch)
    monkeypatch.setattr(native.time,'sleep',lambda n:None)
    configs=[{'mode':'copy' if failure=='copy-startup' else 'hash','expected_bytes':5}]*2
    if failure=='startup-once':
        assert native._batch(configs)==[json.loads(row),json.loads(row)]
        assert len(launched)==2
    else:
        with pytest.raises(SnapshotError,match='Native batch failed'):
            native._batch(configs)
        assert len(launched)==(3 if failure=='startup-exhausted' else 1)
    assert all(p.stdout.closed for p in launched)

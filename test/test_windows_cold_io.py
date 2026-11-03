import hashlib
import io
import json
from pathlib import Path
import subprocess
import sys
import threading
from types import SimpleNamespace

import pytest

from stockagent.data_sync import windows_cold_io as native
from stockagent.data_sync.desync_snapshots import SnapshotError


@pytest.mark.parametrize('linux_free,physical_free,required,accepted', [
    (293 * 1024**3, 30 * 1024**3, 32 * 1024**3, False),
    (293 * 1024**3, 32 * 1024**3, 32 * 1024**3, True),
    (30 * 1024**3, 293 * 1024**3, 32 * 1024**3, False),
])
def test_workspace_admission_distinguishes_verified_capacity_refusal(
        tmp_path, monkeypatch, linux_free, physical_free, required, accepted):
    from stockagent.data_sync import recovery_queue
    original_read_text = Path.read_text
    monkeypatch.setattr(Path, 'read_text', lambda path, *a, **kw:
                        'fixture-microsoft' if str(path) == '/proc/sys/kernel/osrelease'
                        else original_read_text(path, *a, **kw))
    monkeypatch.setattr(native.subprocess, 'run', lambda *_a, **_kw: SimpleNamespace(stdout='ext4\n'))
    monkeypatch.setattr(native.os, 'statvfs', lambda *_: SimpleNamespace(f_bavail=linux_free, f_frsize=1))
    probes = []
    def physical(config):
        probes.append(config)
        return physical_free
    monkeypatch.setattr(recovery_queue, 'physical_free_bytes', physical)
    if accepted:
        result = native.admit_workspace(tmp_path, required, windows_distribution='fixture')
        assert result == {'filesystem': 'ext4', 'physical_free_bytes': min(linux_free, physical_free),
                          'required_bytes': required}
    else:
        with pytest.raises(native.WorkspaceCapacityDeferred, match='real physical free space') as caught:
            native.admit_workspace(tmp_path, required, windows_distribution='fixture')
        assert isinstance(caught.value, SnapshotError)  # Existing callers remain compatible.
        assert caught.value.physical_free_bytes == min(linux_free, physical_free)
        assert caught.value.required_bytes == required
    assert probes == [{'windows_distribution': 'fixture', 'powershell': native.POWERSHELL}]


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


@pytest.mark.parametrize('offset,prefix', [(True, 'a'*64), (0, 'g'*64), (0, ''), (-1, 'a'*64)])
def test_native_resume_identity_rejected_before_process(offset, prefix):
    with pytest.raises(SnapshotError, match='prefix'):
        native.BinaryWriter(Path('/mnt/d/stockagent-cold-primary/incoming/a'),
                            offset=offset, prefix_sha256=prefix)


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


def test_nested_batch_metadata_does_not_pass_through_windows_environment():
    # A legitimate 160-extent original needs over 32,767 characters when its
    # metadata is base64 encoded. Pass the parsed object into a fresh scope;
    # command length stays fixed and no nested fields are serialized again.
    chunks = [{'path': 'D:\\stockagent-cold-primary\\' + 'a' * 150,
               'sha256': 'b' * 64, 'bytes': 1} for _ in range(160)]
    configs = [{'path': chunks[0]['path'], 'mode': 'hash-chunks',
                'expected_bytes': 160, 'chunks': chunks}]
    import base64
    assert len(base64.b64encode(json.dumps(configs[0]).encode())) > 32767
    command = native._native_command(configs, batched=True)
    assert command == native._native_command([], batched=True)
    assert '& { param($stockagentBinaryBatchConfig)' in command
    assert '$config = $stockagentBinaryBatchConfig' in command
    assert '$env:STOCKAGENT_BINARY_IO_CONFIG =' not in command
    assert 'ConvertTo-Json -Compress -Depth' not in command
    standalone = native._native_command([{'path': 'D:\\stockagent-cold-primary\\one', 'mode': 'hash'}])
    assert "$env:STOCKAGENT_BINARY_IO_CONFIG='" in standalone


@pytest.mark.parametrize('on_e', [False, True])
def test_only_enrolled_backup_transport_maps_to_windows(monkeypatch, on_e):
    from stockagent.data_sync import transport_placement
    monkeypatch.setattr(Path, 'resolve', lambda self: self)
    monkeypatch.setattr(Path, 'is_symlink', lambda self: False)
    monkeypatch.setattr(transport_placement, 'on_e', lambda: on_e)
    expected = None if on_e else 'D:\\stockagent-backup-ingress-lab203\\.staging\\fixed\\member'
    assert native.windows_path(Path('/srv/stockagent-backup-ingress-lab203/.staging/fixed/member')) == expected
    assert native.windows_path(Path('/srv/unrelated-backup/member')) is None


@pytest.mark.parametrize('damage',['none','overlay','write'])
def test_enrolled_lake_allows_only_native_hash_on_its_actual_d_volume(monkeypatch,damage):
    from stockagent.data_sync import cold_primary,packed_backup
    path=Path('/srv/stockagent-d-volume/stockagent-immutable-lake/releases/fixed/data.parquet')
    monkeypatch.setattr(Path,'resolve',lambda self:self)
    monkeypatch.setattr(Path,'is_symlink',lambda self:False)
    monkeypatch.setattr(Path,'stat',lambda self:SimpleNamespace(st_dev=9 if self==path and damage=='overlay' else 7))
    monkeypatch.setattr(cold_primary,'_check_d_primary_mount',lambda root:None)
    monkeypatch.setattr(packed_backup,'mounted_volume',lambda root:(None,None,'D:'))
    assert native.windows_path(path)=='D:\\stockagent-immutable-lake\\releases\\fixed\\data.parquet'
    if damage=='none':
        assert native._configuration(path,mode='hash',expected_bytes=9*1024**2)['mode']=='hash'
    else:
        with pytest.raises(SnapshotError,match='read-only'):
            native._configuration(path,mode='write' if damage=='write' else 'hash',expected_bytes=9*1024**2)


@pytest.mark.parametrize('namespace', ['stockagent-completed-benchmark-scratch', 'stockagent-completed-offline-scratch'])
@pytest.mark.parametrize('damage', ['none', 'overlay', 'wrong-drive', 'write', 'copy', 'hash'])
def test_completed_encoding_native_access_is_read_only_on_its_actual_d_volume(monkeypatch, namespace, damage):
    from stockagent.data_sync import cold_primary, packed_backup
    path = Path('/srv/stockagent-d-volume')/namespace/'fixed/archive/payload/aa/encoding.zst'
    monkeypatch.setattr(Path, 'resolve', lambda self:self)
    monkeypatch.setattr(Path, 'is_symlink', lambda self:False)
    monkeypatch.setattr(Path, 'stat', lambda self:SimpleNamespace(st_dev=9 if self==path and damage=='overlay' else 7))
    checked=[]
    monkeypatch.setattr(cold_primary, '_check_d_primary_mount', lambda root:checked.append(root))
    monkeypatch.setattr(packed_backup, 'mounted_volume', lambda root:(None, None, 'C:' if damage=='wrong-drive' else 'D:'))
    mode = damage if damage in {'write', 'copy', 'hash'} else 'read'
    if damage == 'none':
        config = native._configuration(path, mode=mode, expected_bytes=9*1024**2)
        assert config['path'] == str(native.PureWindowsPath('D:/')/namespace/'fixed/archive/payload/aa/encoding.zst')
        assert config['mode'] == 'read' and config['expected_bytes'] == 9*1024**2
    else:
        with pytest.raises(SnapshotError, match='actual D volume and read-only'):
            native._configuration(path, mode=mode, expected_bytes=9*1024**2)
    assert checked == [Path('/srv/stockagent-packed')]


def test_windows_stage_namespace_guard_also_requires_read_mode():
    code = native.SCRIPT.read_text()
    guard = code.split('$stream = $null', 1)[0]
    assert "$completedStageRead = $config.mode -eq 'read' -and" in guard
    assert "D:\\stockagent-completed-benchmark-scratch\\" in guard
    assert "D:\\stockagent-completed-offline-scratch\\" in guard
    assert "D:\\stockagent-legacy-archive-stage\\" in guard
    assert '!$completedStageRead' in guard and '!$retiredEncodingHash' in guard
    assert "$retiredEncodingHash = $config.mode -eq 'hash' -and" in guard
    assert '$path -ne [string]$config.path' in guard


@pytest.mark.parametrize('admitted', [False, True])
@pytest.mark.parametrize('mode', ['read', 'write', 'hash'])
def test_native_legacy_encoding_requires_reviewed_dataset_and_read_only_mode(monkeypatch, admitted, mode):
    from stockagent.data_sync import cold_primary, packed_backup, legacy_artifact_archive as archive
    path = Path('/srv/stockagent-d-volume/stockagent-legacy-archive-stage/fixed/archive/payload/test.gz')
    monkeypatch.setattr(Path, 'resolve', lambda self: self)
    monkeypatch.setattr(Path, 'is_symlink', lambda self: False)
    monkeypatch.setattr(Path, 'stat', lambda self: SimpleNamespace(st_dev=7))
    monkeypatch.setattr(cold_primary, '_check_d_primary_mount', lambda root: None)
    monkeypatch.setattr(packed_backup, 'mounted_volume', lambda root: (None, None, 'D:'))
    monkeypatch.setattr(archive, 'reviewed_retired_stage_path', lambda p: admitted)
    if admitted and mode in {'read', 'hash'}:
        assert native._configuration(path, mode=mode, expected_bytes=5)['mode'] == mode
    else:
        with pytest.raises(SnapshotError, match='read-only|exact reviewed'):
            native._configuration(path, mode=mode, expected_bytes=5)


@pytest.mark.parametrize('count',[1,2])
def test_measured_mid_size_copy_uses_native_batch_or_single_tail_without_changing_bytes(tmp_path,monkeypatch,count):
    from stockagent.data_sync import offhost_backup
    data=b'x'*(9*1024**2);sha=hashlib.sha256(data).hexdigest();sources=[];members=[];calls=[]
    for i in range(count):
        p=tmp_path/('source'+str(i));p.write_bytes(data);sources.append(p)
        members.append((p,tmp_path/'target'/str(i),sha,len(data)))
    monkeypatch.setattr(native,'windows_path',lambda p: 'D:\\stockagent-cold-primary\\fixed' if p in sources
                        else 'D:\\stockagent-backup-ingress-lab203\\.staging\\fixed')
    def single(p,d,**options):
        calls.append(('single',1));d.parent.mkdir(parents=True,exist_ok=True);d.write_bytes(p.read_bytes())
    def batch(values):
        calls.append(('batch',len(values)))
        for p,d,s,n in values:d.parent.mkdir(parents=True,exist_ok=True);d.write_bytes(p.read_bytes())
    monkeypatch.setattr(native,'copy_verified',single);monkeypatch.setattr(native,'copy_many',batch)
    offhost_backup.copy_verified_many(members)
    assert calls==[('single',1)] if count==1 else calls==[('batch',2)]
    for p,d,s,n in members:
        assert p.read_bytes()==d.read_bytes()==data and hashlib.sha256(d.read_bytes()).hexdigest()==sha


@pytest.mark.parametrize('reserve', [True, '34359738368', 0, 32*1024**3-1, 65*1024**3])
def test_native_backup_copy_rejects_invalid_reserve_before_any_io(monkeypatch, reserve):
    monkeypatch.setattr(native, '_batch', lambda *a, **k: pytest.fail('must not start native process'))
    with pytest.raises(SnapshotError, match='32 GiB writer floor'):
        native.copy_many([], reserve_bytes=reserve)
    with pytest.raises(SnapshotError, match='32 GiB writer floor'):
        native.copy_verified(Path('/missing'), Path('/target'), expected_sha256='a'*64,
                             expected_bytes=1, reserve_bytes=reserve)


@pytest.mark.parametrize('reserve', [None, 32*1024**3])
def test_closed_private_export_forwards_exact_copy_reserve_without_changing_bytes(tmp_path, monkeypatch, reserve):
    from stockagent.data_sync import offhost_backup
    source, destination = tmp_path/'source', tmp_path/'target'
    value = b'original cold bytes -3.5'
    source.write_bytes(value)
    sha = hashlib.sha256(value).hexdigest()
    monkeypatch.setattr(native, 'windows_path', lambda p: 'D:\\stockagent-cold-primary\\fixed'
        if p == source else 'D:\\stockagent-backup-ingress-lab203\\.staging\\fixed')
    calls = []
    def copy(members, **kwargs):
        calls.append(kwargs)
        for s, d, digest, count in members:
            assert hashlib.sha256(s.read_bytes()).hexdigest() == digest and s.stat().st_size == count
            d.write_bytes(s.read_bytes())
    monkeypatch.setattr(native, 'copy_many', copy)
    offhost_backup.copy_verified_many([(source, destination, sha, len(value))],
        native_windows_io=True, **({} if reserve is None else {'native_reserve_bytes': reserve}))
    assert calls == ([{}] if reserve is None else [{'reserve_bytes': reserve}])
    assert source.read_bytes() == destination.read_bytes() == value


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
    from stockagent.data_sync import cold_primary
    paths = [tmp_path / str(i) for i in range(2)]
    for p in paths:p.write_bytes(b'original exact bytes')
    monkeypatch.setattr(cold_primary, '_check_d_primary_mount', lambda root: None)
    monkeypatch.setattr(native, '_configuration_on_checked_mount', lambda p, **k: k)
    def batch(configs):
        if damage == 'source_change':paths[1].write_bytes(b'mutated bytes')
        return [{'bytes': c['expected_bytes'] - (damage == 'wrong_size'), 'sha256': 'a' * 64} for c in configs]
    monkeypatch.setattr(native, '_batch', batch)
    with pytest.raises(SnapshotError, match='changed'):
        native.hash_many(paths)


@pytest.mark.parametrize('failure', [None, 'before', 'after'])
def test_native_hash_batch_checks_authority_before_and_after_not_per_member(tmp_path, monkeypatch, failure):
    from stockagent.data_sync import cold_primary
    paths = [tmp_path/str(i) for i in range(16)]
    for path in paths:
        path.write_bytes(b'exact immutable member')
    events = []
    def guard(root):
        assert root == Path('/srv/stockagent-packed')
        events.append('guard')
        if failure == ('before' if len(events) == 1 else 'after'):
            raise SnapshotError('enrolled authority unavailable')
    def config(path, **options):
        events.append('member')
        return options
    def batch(configs):
        events.append('batch')
        return [{'bytes': c['expected_bytes'], 'sha256': 'a'*64} for c in configs]
    monkeypatch.setattr(cold_primary, '_check_d_primary_mount', guard)
    monkeypatch.setattr(native, '_configuration_on_checked_mount', config)
    monkeypatch.setattr(native, '_batch', batch)
    if failure:
        with pytest.raises(SnapshotError, match='authority unavailable'):
            native.hash_many(paths)
        if failure == 'before':
            assert events == ['guard']
    else:
        assert native.hash_many(paths) == {path: 'a'*64 for path in paths}
        assert events == ['guard']+['member']*16+['batch', 'guard']
    assert all(path.read_bytes() == b'exact immutable member' for path in paths)


@pytest.mark.parametrize('reject_member', [False, True])
def test_hash_metadata_can_overlap_but_every_path_must_join_before_payload(tmp_path, monkeypatch, reject_member):
    import threading
    from stockagent.data_sync import cold_primary
    paths = [tmp_path / str(index) for index in range(3)]
    for path in paths:
        path.write_bytes(b'exact source')
    barrier = threading.Barrier(3, timeout=5)
    checked = []
    def config(path, **options):
        barrier.wait()
        if reject_member and path == paths[1]:
            raise SnapshotError('redirected original member')
        checked.append(path)
        return options
    def batch(configs):
        assert set(checked) == set(paths)
        assert not reject_member
        return [{'bytes': item['expected_bytes'], 'sha256': 'a' * 64} for item in configs]
    monkeypatch.setattr(cold_primary, '_check_d_primary_mount', lambda *_: None)
    monkeypatch.setattr(native, '_configuration_on_checked_mount', config)
    monkeypatch.setattr(native, '_batch', batch)
    if reject_member:
        with pytest.raises(SnapshotError, match='redirected original member'):
            native.hash_many(paths)
    else:
        assert native.hash_many(paths) == {path: 'a' * 64 for path in paths}


@pytest.mark.parametrize('failure', [None, 'before', 'after', 'changed', 'short', 'excess',
                                    'unconsumed', 'partial_failure', 'startup-once',
                                    'startup-exhausted', 'other-error', 'consumer-error'])
def test_exact_native_stream_batch_keeps_order_bounds_and_all_lifecycle_guards(tmp_path, monkeypatch, failure):
    from stockagent.data_sync import cold_primary
    paths = [tmp_path / str(i) for i in range(2)]
    values = [b'\x00first', b'\xffsecond']
    for path, value in zip(paths, values, strict=True):
        path.write_bytes(value)
    # The same object can occur twice in one original, and must not deduplicate.
    members = [(paths[0], len(values[0])), (paths[1], len(values[1])), (paths[0], len(values[0]))]
    expected = values[0] + values[1] + values[0]
    launches, events = [], []
    def guard(root):
        events.append('guard')
        if failure == 'before' and not launches or failure == 'after' and launches:
            raise SnapshotError('authority unavailable')
    class Process:
        def __init__(self, output, code):
            self.stdin = io.BytesIO()
            self.stdout = io.BufferedReader(io.BytesIO(output))
            self.code = code
        def wait(self, **kwargs):return self.code
        def poll(self):return self.code
    def launch(configs, **options):
        assert options == {'batched': True}
        assert [c['expected_bytes'] for c in configs] == [n for _, n in members]
        startup = failure in {'startup-exhausted', 'other-error'} or failure == 'startup-once' and not launches
        output = b'' if startup else expected[:-1] if failure == 'short' else expected
        if failure == 'excess':output += b'!'
        p = Process(output, 1 if startup or failure == 'partial_failure' else 0)
        launches.append(p)
        stderr = (b'permission denied' if failure == 'other-error' else
                  b'<3>WSL ERROR: UtilAcceptVsock:273: accept4 failed 110')
        return p, io.BytesIO(stderr)
    monkeypatch.setattr(cold_primary, '_check_d_primary_mount', guard)
    monkeypatch.setattr(native, '_configuration_on_checked_mount', lambda path, **k: k)
    monkeypatch.setattr(native, '_launch', launch)
    monkeypatch.setattr(native.time, 'sleep', lambda n: None)
    def consume():
        with native.binary_reader_many(members) as stream:
            if failure == 'consumer-error':raise SnapshotError('consumer SHA mismatch')
            if failure == 'unconsumed':return
            actual = stream.read(len(expected))
            if actual != expected:raise SnapshotError('truncated exact member')
            if failure == 'changed':paths[1].write_bytes(b'changed')
    if failure in {None, 'startup-once'}:
        consume()
        assert len(launches) == (2 if failure else 1)
        assert events == ['guard'] * (3 if failure else 2)
    else:
        with pytest.raises(SnapshotError):consume()
        assert len(launches) == (0 if failure == 'before' else 3 if failure == 'startup-exhausted' else 1)
    assert all(p.stdin.closed and p.stdout.closed for p in launches)


@pytest.mark.parametrize('members', [[], [(Path('/fixed'), True)], [(Path('/fixed'), -1)],
                                    [(Path('/fixed'), 8 * 1024**3 + 1)],
                                    [(Path('/fixed'), 1)] * 17,
                                    [(Path('/fixed'), 8 * 1024**3)] * 3])
def test_native_stream_batch_rejects_unbounded_inputs_before_authority_or_process(members, monkeypatch):
    monkeypatch.setattr(native, '_launch', lambda *a, **k: pytest.fail('unbounded process'))
    with pytest.raises(SnapshotError, match='bounds'):
        with native.binary_reader_many(members):pass


def test_native_exact_stream_checks_each_filestream_size_before_and_after():
    script = native.SCRIPT.read_text()
    assert "Exact read member size differs" in script
    assert "Truncated exact read member" in script


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
        stdin = io.BytesIO()
        def communicate(self, *args, **kwargs):return b'{"bytes": 1, "sha256": "a"}\n', None
        def poll(self):return 0
    monkeypatch.setattr(native, '_launch', lambda *a, **k: (Process(), io.BytesIO()))
    monkeypatch.setattr(native, '_await_batch_ready', lambda *a, **k: None)
    with pytest.raises(SnapshotError, match='omitted'):
        native._batch([{'mode': 'hash', 'expected_bytes': 1}] * 2)


@pytest.mark.parametrize('mode,count,total_excess,allowed', [
    ('hash', 128, False, True), ('hash', 129, False, False),
    ('copy', 17, False, False), ('copy', 16, False, True),
    ('hash', 128, True, False),
])
def test_larger_read_only_batches_keep_write_and_total_byte_bounds(monkeypatch, mode, count, total_excess, allowed):
    class ReachedProcess(Exception):
        pass
    def launch(*_args, **_kwargs):
        raise ReachedProcess
    monkeypatch.setattr(native, '_launch', launch)
    configs = [{'mode': mode, 'expected_bytes': 1024**3 if total_excess else 1}] * count
    with pytest.raises(ReachedProcess if allowed else SnapshotError):
        native._batch(configs)


def test_full_128_member_result_set_survives_exact_output_count_gate(monkeypatch):
    row = {'bytes': 1, 'sha256': 'a' * 64, 'io': 'windows-filestream-v1'}
    class Process:
        def __init__(self):
            self.stdout, self.stdin, self.returncode = io.BytesIO(), io.BytesIO(), 0
        def communicate(self, payload, **_kwargs):
            assert len(json.loads(payload)) == 128
            return (json.dumps(row).encode() + b'\n') * 128, None
        def poll(self):
            return self.returncode
    monkeypatch.setattr(native, '_launch', lambda *_args, **_kwargs: (Process(), io.BytesIO()))
    monkeypatch.setattr(native, '_await_batch_ready', lambda *_args, **_kwargs: None)
    assert native._batch([{'mode': 'hash', 'expected_bytes': 1}] * 128) == [row] * 128


def test_read_only_native_batch_stops_when_failed_peer_cancels(monkeypatch):
    cancel = threading.Event()
    class Process:
        def __init__(self):
            self.stdout = io.BytesIO()
            self.stdin = io.BytesIO()
            self.returncode = None
            self.terminated = False
            self.inputs = []
        def communicate(self, payload, *, timeout):
            self.inputs.append(payload)
            cancel.set()
            raise subprocess.TimeoutExpired('hash-process', timeout)
        def poll(self):
            return self.returncode
        def terminate(self):
            self.terminated = True
            self.returncode = -15
        def wait(self, **kwargs):
            return self.returncode
    process = Process()
    monkeypatch.setattr(native, '_launch', lambda *a, **k: (process, io.BytesIO()))
    monkeypatch.setattr(native, '_await_batch_ready', lambda *a, **k: None)
    with pytest.raises(SnapshotError, match='cancelled after peer'):
        native._batch([{'mode': 'hash', 'expected_bytes': 1}], cancel_event=cancel)
    assert process.terminated and len(process.inputs) == 1
    assert process.stdin.closed and process.stdout.closed


def test_cancellable_hash_retries_communicate_without_resending_input():
    process = subprocess.Popen(
        [sys.executable, '-c',
         'import sys,time; data=sys.stdin.buffer.readline(); time.sleep(1.1); sys.stdout.buffer.write(data)'],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    try:
        output, _ = native._communicate_cancellable_hash(process, b'one payload\n', threading.Event())
        assert output == b'one payload\n'
        assert process.returncode == 0
    finally:
        if process.poll() is None:
            process.kill()
            process.wait()
        process.stdin.close()
        process.stdout.close()
        process.stderr.close()


def test_cancellation_is_never_applied_to_native_writes(monkeypatch):
    monkeypatch.setattr(native, '_launch', lambda *a, **k: pytest.fail('write must not launch'))
    with pytest.raises(SnapshotError, match='only for read-only'):
        native._batch([{'mode': 'copy', 'expected_bytes': 1}], cancel_event=threading.Event())


@pytest.mark.parametrize('failure',['startup-once','startup-exhausted','nonempty-failure','copy-startup','other-error'])
def test_native_batch_retries_only_an_empty_read_only_startup(monkeypatch,failure):
    launched=[]
    row=json.dumps({'bytes':5,'sha256':'a'*64}).encode()+b'\n'
    class Process:
        def __init__(self,output,code):
            self.stdout=io.BytesIO();self.stdin=io.BytesIO();self.output=output;self.returncode=code
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
    monkeypatch.setattr(native, '_await_batch_ready', lambda *a, **k: None)
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

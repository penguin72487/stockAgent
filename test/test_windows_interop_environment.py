"""Native byte I/O must validate its bridge without replaying a file operation."""
from pathlib import Path
import base64
import os
import socket
import stat
from types import SimpleNamespace

import pytest

from stockagent.data_sync import windows_cold_io as native
from stockagent.data_sync.desync_snapshots import SnapshotError


def endpoints(monkeypatch, tmp_path, names):
    monkeypatch.setattr(native, '_INTEROP_DIRECTORY', tmp_path)
    monkeypatch.setattr(native, '_INTEROP_CACHE', None)
    signatures = {}
    for index, name in enumerate(names):
        path = tmp_path / name
        path.touch()
        signatures[str(path)] = (str(path), 1, index + 10)
    monkeypatch.setattr(native, '_interop_socket', lambda p: signatures.get(str(p)))
    return signatures


def test_stale_existing_endpoint_selects_verified_bridge_then_reuses_it(monkeypatch, tmp_path):
    signatures = endpoints(monkeypatch, tmp_path, ['1_interop', '20_interop'])
    preferred = str(tmp_path / '1_interop')
    good = str(tmp_path / '20_interop')
    monkeypatch.setenv('WSL_INTEROP', preferred)
    calls = []

    def probe(argv, **kwargs):
        calls.append(kwargs['env']['WSL_INTEROP'])
        assert argv[-2] == '-EncodedCommand'
        command = base64.b64decode(argv[-1]).decode('utf-16le')
        assert command == native._native_command([], batched=True) + "\n[Console]::Write('stockagent-cold-interop-v1')"
        assert native.SCRIPT.read_text() in command
        assert kwargs['input'] == b'[]\n'  # Execute the actual wrapper, with no files.
        assert kwargs['timeout'] == 5
        ok = calls[-1] == good
        return SimpleNamespace(returncode=0 if ok else 1,
                               stdout=native._INTEROP_MARKER if ok else b'')

    monkeypatch.setattr(native.subprocess, 'run', probe)
    assert native._windows_process_environment()['WSL_INTEROP'] == good
    assert calls == [preferred, good]
    assert native._windows_process_environment()['WSL_INTEROP'] == good
    assert calls == [preferred, good]
    assert os.environ['WSL_INTEROP'] == preferred  # Other activities keep their env.
    signatures[good] = (good, 1, 999)  # Recreated socket must be checked again.
    assert native._windows_process_environment()['WSL_INTEROP'] == good
    assert calls == [preferred, good, preferred, good]


def test_success_exit_with_wrong_marker_is_not_a_usable_bridge(monkeypatch, tmp_path):
    endpoints(monkeypatch, tmp_path, ['1_interop'])
    monkeypatch.setenv('WSL_INTEROP', str(tmp_path / '1_interop'))
    monkeypatch.setattr(native.subprocess, 'run', lambda *a, **k:
                        SimpleNamespace(returncode=0, stdout=b'other process output'))
    with pytest.raises(SnapshotError, match='not started'):
        native._windows_process_environment()


def test_probe_and_file_launch_do_not_depend_on_login_windows_path(monkeypatch, tmp_path):
    endpoints(monkeypatch, tmp_path, ['20_interop'])
    monkeypatch.setenv('PATH', '/private-no-Windows-executables')
    monkeypatch.setenv('WSL_INTEROP', str(tmp_path / '20_interop'))
    calls = []

    def probe(argv, **kwargs):
        assert argv[0] == native.POWERSHELL and Path(argv[0]).is_absolute()
        assert kwargs['env']['PATH'] == '/private-no-Windows-executables'
        assert kwargs['input'] == b'[]\n'
        calls.append('probe')
        return SimpleNamespace(returncode=0, stdout=native._INTEROP_MARKER)

    process = object()
    def launch(argv, **kwargs):
        assert argv[0] == native.POWERSHELL
        assert kwargs['env']['WSL_INTEROP'] == str(tmp_path / '20_interop')
        calls.append('file-launch')
        return process

    monkeypatch.setattr(native.subprocess, 'run', probe)
    monkeypatch.setattr(native.subprocess, 'Popen', launch)
    returned, errors = native._launch([], batched=True)
    errors.close()
    assert returned is process and calls == ['probe', 'file-launch']
    assert os.environ['PATH'] == '/private-no-Windows-executables'


def test_failed_probe_is_bounded_and_never_launches_file_operation(monkeypatch, tmp_path):
    endpoints(monkeypatch, tmp_path, [str(i) + '_interop' for i in range(1, 15)])
    monkeypatch.setenv('WSL_INTEROP', str(tmp_path / '1_interop'))
    calls = []

    def timeout(argv, **kwargs):
        calls.append(kwargs['env']['WSL_INTEROP'])
        raise native.subprocess.TimeoutExpired(argv, kwargs['timeout'])

    monkeypatch.setattr(native.subprocess, 'run', timeout)
    monkeypatch.setattr(native.subprocess, 'Popen', lambda *a, **k:
                        pytest.fail('File operation launched before bridge admission'))
    monkeypatch.setattr(native.tempfile, 'TemporaryFile', lambda:
                        pytest.fail('Descriptor allocated before bridge admission'))
    with pytest.raises(SnapshotError, match='No verified'):
        native._launch([], batched=True)
    assert len(calls) == len(set(calls)) == 8
    assert native._INTEROP_CACHE is None


def test_one_transient_empty_admission_timeout_retries_without_any_file_configuration(monkeypatch, tmp_path):
    endpoints(monkeypatch, tmp_path, ['20_interop'])
    good = str(tmp_path / '20_interop')
    monkeypatch.setenv('WSL_INTEROP', good)
    calls, waits = [], []
    monkeypatch.setattr(native.time, 'sleep', waits.append)
    def probe(argv, **options):
        calls.append(options)
        assert options['input'] == b'[]\n'
        assert options['env']['WSL_INTEROP'] == good
        if len(calls) == 1:
            raise native.subprocess.TimeoutExpired(argv, options['timeout'])
        return SimpleNamespace(returncode=0, stdout=native._INTEROP_MARKER)
    monkeypatch.setattr(native.subprocess, 'run', probe)
    assert native._windows_process_environment()['WSL_INTEROP'] == good
    assert len(calls) == 2 and waits == [.25]
    assert native._windows_process_environment()['WSL_INTEROP'] == good
    assert len(calls) == 2


def test_single_unavailable_bridge_has_three_attempts_and_never_starts_file_IO(monkeypatch, tmp_path):
    endpoints(monkeypatch, tmp_path, ['20_interop'])
    calls, waits = [], []
    monkeypatch.setattr(native.time, 'sleep', waits.append)
    def probe(argv, **options):
        calls.append(options['input'])
        raise native.subprocess.TimeoutExpired(argv, options['timeout'])
    monkeypatch.setattr(native.subprocess, 'run', probe)
    monkeypatch.setattr(native.subprocess, 'Popen', lambda *_a, **_k: pytest.fail('file I/O started'))
    with pytest.raises(SnapshotError, match='not started'):
        native._windows_process_environment()
    assert calls == [b'[]\n'] * 3 and waits == [.25, 1.0]


@pytest.mark.parametrize('failure', ['partial-output-timeout', 'wrong-marker', 'script-error', 'permission-error'])
def test_ambiguous_or_permanent_admission_failure_is_not_retried(monkeypatch, tmp_path, failure):
    endpoints(monkeypatch, tmp_path, ['20_interop'])
    calls = []
    monkeypatch.setattr(native.time, 'sleep', lambda *_: pytest.fail('permanent failure retried'))
    def probe(argv, **options):
        calls.append(options['input'])
        if failure == 'partial-output-timeout':
            raise native.subprocess.TimeoutExpired(argv, options['timeout'], output=b'partial marker')
        if failure == 'permission-error':
            raise PermissionError('fixture permission rejection')
        return SimpleNamespace(returncode=1 if failure == 'script-error' else 0, stdout=b'wrong marker')
    monkeypatch.setattr(native.subprocess, 'run', probe)
    with pytest.raises(SnapshotError, match='not started'):
        native._windows_process_environment()
    assert calls == [b'[]\n']


def test_replaced_socket_is_never_retried_or_admitted_after_a_probe(monkeypatch, tmp_path):
    signatures = endpoints(monkeypatch, tmp_path, ['20_interop'])
    good = str(tmp_path / '20_interop')
    calls = []
    def probe(argv, **options):
        calls.append(options['input'])
        signatures[good] = (good, 1, 999)
        return SimpleNamespace(returncode=0, stdout=native._INTEROP_MARKER)
    monkeypatch.setattr(native.subprocess, 'run', probe)
    with pytest.raises(SnapshotError, match='not started'):
        native._windows_process_environment()
    assert calls == [b'[]\n'] and native._INTEROP_CACHE is None


def test_cached_bridge_is_revalidated_after_timeout(monkeypatch, tmp_path):
    signatures = endpoints(monkeypatch, tmp_path, ['20_interop'])
    good = str(tmp_path / '20_interop')
    monkeypatch.setenv('WSL_INTEROP', good)
    clock = [159.0]
    monkeypatch.setattr(native.time, 'monotonic', lambda: clock[0])
    monkeypatch.setattr(native, '_INTEROP_CACHE', (signatures[good], 100.0))
    calls = []
    monkeypatch.setattr(native.subprocess, 'run', lambda *a, **k:
                        calls.append(k['env']['WSL_INTEROP']) or
                        SimpleNamespace(returncode=0, stdout=native._INTEROP_MARKER))
    assert native._windows_process_environment()['WSL_INTEROP'] == good
    assert calls == []
    clock[0] = 161.0
    assert native._windows_process_environment()['WSL_INTEROP'] == good
    assert calls == [good]


def test_non_wsl_does_not_probe_or_change_process_environment(monkeypatch, tmp_path):
    monkeypatch.setattr(native, '_INTEROP_DIRECTORY', tmp_path / 'absent')
    monkeypatch.setattr(native.subprocess, 'run', lambda *a, **k: pytest.fail('Not WSL'))
    assert native._windows_process_environment() is None


@pytest.mark.parametrize('case', ['direct', 'init-alias', 'foreign-alias', 'regular', 'wrong-init', 'wrong-owner'])
def test_only_owned_init_sockets_and_fixed_alias_are_admitted(monkeypatch, tmp_path, case):
    monkeypatch.setattr(native, '_INTEROP_DIRECTORY', tmp_path)
    endpoint = tmp_path / '20_interop'
    server = socket.socket(socket.AF_UNIX)
    server.bind(str(endpoint))
    original_lstat = Path.lstat
    original_resolve = Path.resolve

    def lstat(path):
        value = original_lstat(path)
        if path == tmp_path:
            return SimpleNamespace(st_mode=stat.S_IFDIR | 0o755, st_uid=0)
        if path == endpoint and case == 'wrong-owner':
            return SimpleNamespace(st_mode=value.st_mode, st_uid=os.geteuid() + 1)
        return value

    def resolve(path, *args, **kwargs):
        if str(path) == '/proc/20/exe':
            return Path('/other') if case == 'wrong-init' else Path('/init')
        if str(path) == '/proc/2/exe':
            return Path('/init')
        return original_resolve(path, *args, **kwargs)

    monkeypatch.setattr(Path, 'lstat', lstat)
    monkeypatch.setattr(Path, 'resolve', resolve)
    try:
        if case == 'init-alias':
            server.close()
            endpoint.unlink()
            endpoint = tmp_path / '2_interop'
            server = socket.socket(socket.AF_UNIX)
            server.bind(str(endpoint))
            path = tmp_path / '1_interop'
            path.symlink_to(endpoint)
        elif case == 'foreign-alias':
            path = tmp_path / '1_interop'
            path.symlink_to(endpoint)
        elif case == 'regular':
            path = tmp_path / '30_interop'
            path.touch()
        else:
            path = endpoint
        signature = native._interop_socket(path)
        assert (signature is not None) == (case in {'direct', 'init-alias'})
        if signature:
            assert signature[0] == str(endpoint)
    finally:
        server.close()


def test_expired_verified_generation_gets_fresh_probe_before_eight_newer_endpoints(monkeypatch, tmp_path):
    signatures = endpoints(monkeypatch, tmp_path, ['2_interop'] + [f'{i}_interop' for i in range(20, 40)])
    good = str(tmp_path / '2_interop')
    bad = str(tmp_path / '39_interop')
    monkeypatch.setenv('WSL_INTEROP', bad)
    monkeypatch.setattr(native.time, 'monotonic', lambda: 100.0)
    monkeypatch.setattr(native, '_INTEROP_CACHE', (signatures[good], 1.0))
    calls = []

    def probe(*args, **kwargs):
        calls.append(kwargs['env']['WSL_INTEROP'])
        assert kwargs['input'] == b'[]\n'
        return SimpleNamespace(returncode=0, stdout=native._INTEROP_MARKER if calls[-1] == good else b'')

    monkeypatch.setattr(native.subprocess, 'run', probe)
    assert native._windows_process_environment()['WSL_INTEROP'] == good
    assert calls == [good]
    assert native._INTEROP_CACHE == (signatures[good], 100.0)
    assert os.environ['WSL_INTEROP'] == bad


@pytest.mark.parametrize('failure', ['wrong-marker', 'script-error', 'partial-timeout'])
def test_previous_success_never_bypasses_new_failed_empty_admission(monkeypatch, tmp_path, failure):
    signatures = endpoints(monkeypatch, tmp_path, ['2_interop'])
    good = str(tmp_path / '2_interop')
    monkeypatch.setenv('WSL_INTEROP', good)
    monkeypatch.setattr(native.time, 'monotonic', lambda: 100.0)
    monkeypatch.setattr(native, '_INTEROP_CACHE', (signatures[good], 1.0))
    calls = []

    def probe(argv, **kwargs):
        calls.append(kwargs['input'])
        if failure == 'partial-timeout':
            raise native.subprocess.TimeoutExpired(argv, kwargs['timeout'], output=b'partial')
        return SimpleNamespace(returncode=1 if failure == 'script-error' else 0, stdout=b'wrong')

    monkeypatch.setattr(native.subprocess, 'run', probe)
    with pytest.raises(SnapshotError, match='not started'):
        native._windows_process_environment()
    assert calls == [b'[]\n'] and native._INTEROP_CACHE is None


def test_changed_previous_generation_does_not_displace_original_preferred_probe(monkeypatch, tmp_path):
    signatures = endpoints(monkeypatch, tmp_path, ['2_interop', '30_interop'])
    old = str(tmp_path / '2_interop')
    preferred = str(tmp_path / '30_interop')
    monkeypatch.setenv('WSL_INTEROP', preferred)
    monkeypatch.setattr(native, '_INTEROP_CACHE', ((old, 1, 999999), 0.0))
    calls = []
    monkeypatch.setattr(native.subprocess, 'run', lambda *a, **kw: calls.append(kw['env']['WSL_INTEROP']) or
        SimpleNamespace(returncode=0, stdout=native._INTEROP_MARKER))
    assert native._windows_process_environment()['WSL_INTEROP'] == preferred
    assert calls == [preferred]

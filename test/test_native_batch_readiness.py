"""Native batch startup is bounded separately from complete file I/O."""
import io
import json
import subprocess
import sys

import pytest

from stockagent.data_sync import windows_cold_io as native
from stockagent.data_sync.desync_snapshots import SnapshotError


TOKEN = 'a1' * 16


@pytest.mark.parametrize('mode', ['split-ready', 'wrong-nonce', 'eof', 'silent', 'partial'])
def test_real_pipe_startup_has_exact_nonce_and_bounded_wait(mode):
    expected = native._batch_ready_marker(TOKEN) + b'\n'
    script = "import os,sys,time\n"
    if mode == 'split-ready':
        script += f"os.write(1,{expected[:7]!r}); time.sleep(.01); os.write(1,{expected[7:]!r})\n"
        script += "line=sys.stdin.buffer.readline(); os.write(1,line)\n"
    elif mode == 'wrong-nonce':
        script += f"os.write(1,{native._batch_ready_marker('b2' * 16) + bytes([10])!r})\n"
    elif mode == 'partial':
        script += f"os.write(1,{expected[:9]!r}); time.sleep(10)\n"
    elif mode == 'silent':
        script += "time.sleep(10)\n"
    process = subprocess.Popen([sys.executable, '-c', script], stdin=subprocess.PIPE,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    try:
        if mode == 'split-ready':
            native._await_batch_ready(process, TOKEN, timeout=2)
            output, error = process.communicate(b'{"metadata":"after-ready"}\n', timeout=2)
            assert output == b'{"metadata":"after-ready"}\n' and not error
            assert process.returncode == 0
        else:
            with pytest.raises(native._BatchNotReady) as failure:
                native._await_batch_ready(process, TOKEN, timeout=.3 if mode in {'silent', 'partial'} else 2)
            assert failure.value.reason == ('invalid-marker' if mode == 'wrong-nonce' else
                'eof' if mode == 'eof' else 'timeout')
            assert bool(failure.value.received) == (mode in {'wrong-nonce', 'partial'})
    finally:
        if process.poll() is None:
            process.kill()
        process.wait(timeout=2)
        for stream in (process.stdin, process.stdout, process.stderr):
            stream.close()


@pytest.mark.parametrize('failure', ['empty-once', 'empty-exhausted', 'eof-once',
    'partial', 'wrong-marker', 'copy-empty'])
def test_batch_sends_metadata_only_after_exact_ready_and_bounds_read_only_retry(monkeypatch, failure):
    launched, sleeps = [], []
    row = {'bytes': 5, 'sha256': 'a' * 64}

    class Process:
        def __init__(self, token):
            self.token = token
            self.returncode = None
            self.stdin, self.stdout = io.BytesIO(), io.BytesIO()
            self.communicated = False
            self.terminated = False

        def communicate(self, data, **kwargs):
            assert self.ready and kwargs == {'timeout': 3600}
            assert json.loads(data) == configs
            self.communicated = True
            self.returncode = 0
            return (json.dumps(row) + '\n').encode(), None

        def poll(self):
            return self.returncode

        def terminate(self):
            self.terminated = True
            self.returncode = -15

        def wait(self, **kwargs):
            return self.returncode

    def launch(values, **kwargs):
        assert kwargs['batched'] is True and len(kwargs['startup_token']) == 32
        process = Process(kwargs['startup_token'])
        launched.append(process)
        return process, io.BytesIO()

    def ready(process, token):
        assert process.token == token and not process.communicated
        if failure in {'partial', 'wrong-marker'}:
            raise native._BatchNotReady('timeout' if failure == 'partial' else 'invalid-marker', b'x')
        if failure in {'empty-exhausted', 'copy-empty'} or len(launched) == 1:
            raise native._BatchNotReady('eof' if failure == 'eof-once' else 'timeout', b'')
        process.ready = True

    monkeypatch.setattr(native, '_launch', launch)
    monkeypatch.setattr(native, '_await_batch_ready', ready)
    monkeypatch.setattr(native.time, 'sleep', sleeps.append)
    configs = [{'mode': 'copy' if failure == 'copy-empty' else 'hash', 'expected_bytes': 5}]
    if failure in {'empty-once', 'eof-once'}:
        assert native._batch(configs) == [row]
        assert len(launched) == 2 and sleeps == [.2]
        assert not launched[0].communicated and launched[0].terminated
        assert launched[1].communicated
    else:
        with pytest.raises(SnapshotError, match='Native batch failed'):
            native._batch(configs)
        assert len(launched) == (3 if failure == 'empty-exhausted' else 1)
        assert all(not process.communicated and process.terminated for process in launched)
    assert len({process.token for process in launched}) == len(launched)
    assert all(process.stdin.closed and process.stdout.closed for process in launched)


def test_native_nonce_is_flushed_before_metadata_and_original_probe_protocol_is_unchanged():
    original = native._native_command([], batched=True)
    command = native._native_command([], batched=True, startup_token=TOKEN)
    assert 'stockagent-native-batch-ready' not in original
    assert command.index(native._batch_ready_marker(TOKEN).decode()) < command.index('[Console]::Out.Flush()')
    assert command.index('[Console]::Out.Flush()') < command.index('[Console]::In.ReadLine()')
    assert original[original.index('$batch ='):] == command[command.index('$batch ='):]


@pytest.mark.parametrize('token', ['', "'; injected", 'a' * 31, 'G' * 32, 1])
def test_invalid_nonce_is_rejected_before_a_command_is_built(token):
    with pytest.raises(SnapshotError, match='token'):
        native._native_command([], batched=True, startup_token=token)

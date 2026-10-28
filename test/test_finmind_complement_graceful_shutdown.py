from __future__ import annotations

import fcntl
import os
from pathlib import Path
import signal
import subprocess
import sys
import time


def test_sigterm_closes_idle_process_pool_and_releases_worker_lock(tmp_path: Path):
    root = tmp_path / 'complement'
    ready = tmp_path / 'ready'
    program = r'''
import sys, time
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from downloader import download_finmind_complement as worker

worker.object_pipeline.initialize_cpu = lambda _threads: None
def idle(*_args, **_kwargs):
    with ProcessPoolExecutor(max_workers=1):
        Path(sys.argv[2]).write_text('ready')
        while True: time.sleep(.1)
worker.run_once = idle
sys.exit(worker.main(['--root', sys.argv[1], '--loop']))
'''
    process = subprocess.Popen([sys.executable, '-c', program, str(root), str(ready)],
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    try:
        deadline = time.monotonic() + 15
        while not ready.exists() and process.poll() is None and time.monotonic() < deadline:
            time.sleep(.05)
        assert ready.exists(), 'test worker never entered its idle pool'
        process.send_signal(signal.SIGTERM)
        _out, error = process.communicate(timeout=15)
        assert process.returncode == 130
        assert 'leaked semaphore' not in error
        with (root / 'worker.lock').open('a+') as handle:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
    finally:
        if process.poll() is None:
            process.kill()
            process.communicate(timeout=10)

#!/usr/bin/env python3
"""Launch the owned Windows fixture or an exact no-query, same-result benchmark."""
from __future__ import annotations

import argparse
from contextlib import closing, nullcontext
import hashlib
import json
from pathlib import Path
import re
import sqlite3
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from downloader.artifact_io import atomic_write_bytes
from downloader.dataset_lock import exclusive_dataset_lock
from downloader.tej_history import DesktopBridge, SOURCE_SCOPE_CONTRACT


def completed_source(root: Path, task_id: str) -> tuple[Path, Path]:
    """Read and bind the original attempt, receipt and saved result; no adoption."""
    if not re.fullmatch('[0-9a-f]{24}', task_id):
        raise ValueError('Exact canonical completed task ID required')
    with closing(sqlite3.connect(f'file:{root / "queue.sqlite3"}?mode=ro', uri=True, timeout=2)) as con:
        con.row_factory = sqlite3.Row
        task = con.execute('SELECT * FROM tasks WHERE task_id=?', (task_id,)).fetchone()
        if (task is None or task['kind'] != 'download' or task['state'] != 'complete'
                or task['scope_contract'] != SOURCE_SCOPE_CONTRACT or not task['actual_rows']):
            raise ValueError('Completed nonempty source receipt required')
        if con.execute("SELECT 1 FROM tasks WHERE scope_contract=? AND (state='running' "
                       "OR (state='blocked' AND last_error_code='unknown_outcome_no_auto_retry')) LIMIT 1",
                       (SOURCE_SCOPE_CONTRACT,)).fetchone():
            raise ValueError('Unresolved acquisition owner must be settled before source benchmarking')
        attempt = task['active_attempt_id']
        if not isinstance(attempt, str) or not re.fullmatch(task_id+'-[0-9a-f]{32}', attempt):
            raise ValueError('Modern exact-attempt source evidence required')
        prepared = root/'requests'/(attempt+'.json')
        raw = root/'raw'/(attempt+'.json')
        receipt_path = root/'receipts'/(task_id+'.json')
        if (task['output_path'] != str(raw.relative_to(root))
                or task['receipt_path'] != str(receipt_path.relative_to(root))
                or not prepared.is_file() or prepared.stat().st_size > 2*1024**2
                or not raw.is_file() or raw.stat().st_size > 64*1024**2
                or not receipt_path.is_file() or receipt_path.stat().st_size > 2*1024**2):
            raise ValueError('Exact bounded private source artifacts required')
        receipt = json.loads(receipt_path.read_text())
        if (receipt.get('task_id') != task_id
                or receipt.get('raw_sha256') != hashlib.sha256(raw.read_bytes()).hexdigest()):
            raise ValueError('Original source result digest differs from its receipt')
    return prepared, raw


def run(root: Path, baseline: Path, output: Path, *, fixture: bool = False,
        task_id: str | None = None, rounds: int = 2) -> dict:
    root, baseline, output = root.resolve(), baseline.resolve(), output.resolve()
    if output.exists():
        raise FileExistsError('Retain original benchmark evidence')
    if not baseline.is_file() or baseline.stat().st_size > 1024**2:
        raise ValueError('Bounded original bridge snapshot required')
    if type(rounds) is not int or not 1 <= rounds <= 3:
        raise ValueError('Benchmark rounds must be bounded to 1..3')
    if fixture == (task_id is not None):
        raise ValueError('Select either an owned fixture or one exact completed source task')
    lock = nullcontext() if fixture else exclusive_dataset_lock(
        root/'.download.lock', provider='tej_no_query_readback_benchmark', timeout_seconds=0)
    with lock:
        source_args = ''
        if not fixture:
            prepared, raw = completed_source(root, task_id)
            session = root/'desktop_session.json'
            session_doc = json.loads(session.read_text())
            if set(session_doc) != {'TejProcessId','ExpectedWindow','ExpectedTitle','ExpectedWorkbook','ExpectedExcelWindow'}:
                raise ValueError('Exact pinned canonical desktop session required')
            source_args = (' -Session '+DesktopBridge.quote(DesktopBridge.windows_path(session))
                           +' -Prepared '+DesktopBridge.quote(DesktopBridge.windows_path(prepared))
                           +' -SourceResult '+DesktopBridge.quote(DesktopBridge.windows_path(raw)))
        output.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        script = DesktopBridge.windows_path(ROOT/'scripts/benchmark_tej_preview_readback.ps1')
        command = ("$ErrorActionPreference='Stop';$s="+DesktopBridge.quote(script)+";"
                   "$d=Join-Path ([Environment]::GetFolderPath('LocalApplicationData')) ('StockAgent\\TEJSmartWizard\\benchmark-'+[Guid]::NewGuid().ToString('N'));"
                   "[void](New-Item -ItemType Directory -Path $d);$p=Join-Path $d 'benchmark.ps1';Copy-Item -LiteralPath $s -Destination $p;"
                   "if((Get-FileHash -LiteralPath $s).Hash -cne (Get-FileHash -LiteralPath $p).Hash){throw 'Script copy mismatch'};"
                   "& $p -BridgeScript "+DesktopBridge.quote(DesktopBridge.windows_path(ROOT/'scripts/tej_smart_wizard_bridge.ps1'))
                   +' -BaselineScript '+DesktopBridge.quote(DesktopBridge.windows_path(baseline))
                   +' -Output '+DesktopBridge.quote(DesktopBridge.windows_path(output))
                   +' -Rounds '+str(rounds)+(' -Fixture' if fixture else source_args))
        result = subprocess.run(['/mnt/c/WINDOWS/System32/WindowsPowerShell/v1.0/powershell.exe',
                                 '-NoProfile','-NonInteractive','-Command',command],
                                capture_output=True, timeout=900)
        if result.returncode or not output.is_file():
            diagnostic = root/'diagnostics'/('readback-benchmark-'+output.stem+'.txt')
            atomic_write_bytes(diagnostic, result.stderr[:32768])
            diagnostic.chmod(0o600)
            raise RuntimeError('Windows readback comparison failed; inspect private diagnostic, no query sent')
        receipt = json.loads(output.read_text(encoding='utf-8-sig'))
        if (receipt.get('accepted') is not True or receipt.get('provider_queries_sent') != 0
                or receipt.get('source_values_exposed') is not False or receipt.get('queue_modified') is not False
                or (not fixture and receipt.get('exact_saved_source_result_matches') is not True)):
            raise ValueError('Incomplete readback benchmark acceptance')
        return receipt


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=ROOT/'data_tej')
    parser.add_argument('--baseline', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--fixture', action='store_true')
    parser.add_argument('--task-id')
    parser.add_argument('--rounds', type=int, default=2)
    args = parser.parse_args(argv)
    receipt = run(args.root, args.baseline, args.output, fixture=args.fixture,
                  task_id=args.task_id, rounds=args.rounds)
    print(json.dumps(receipt, ensure_ascii=False))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())

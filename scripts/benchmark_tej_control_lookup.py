#!/usr/bin/env python3
"""Bounded exact-function control lookup ABBA, never a source data query."""
from __future__ import annotations

import argparse
from contextlib import nullcontext
import hashlib
import json
from pathlib import Path
import sys
import uuid

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from downloader.artifact_io import atomic_write_bytes, atomic_write_json
from downloader.dataset_lock import exclusive_dataset_lock
from downloader.tej_history import DesktopBridge
from downloader.tej_startup import interactive_transport, valid_session
from downloader.tej_windows_transport import run_guarded_windows


def run(root: Path, baseline: Path, output: Path, *, fixture=False, iterations=10,
        candidate: Path | None = None) -> dict:
    root, baseline, output = root.resolve(), baseline.resolve(), output.resolve()
    candidate = (candidate or ROOT/'scripts/tej_smart_wizard_bridge.ps1').resolve()
    if output.exists():
        raise FileExistsError('Retain the original comparison')
    if type(iterations) is not int or not 1 <= iterations <= 30 or type(fixture) is not bool:
        raise ValueError('Bounded control lookup comparison required')
    for code in (baseline, candidate):
        if not code.is_file() or not 1 <= code.stat().st_size <= 256*1024:
            raise ValueError('Exact bounded comparator scripts required')
    lock = nullcontext() if fixture else exclusive_dataset_lock(root/'.download.lock',
        provider='tej_read_only_control_benchmark', timeout_seconds=10)
    with lock:
        session = root/'desktop_session.json'
        if not fixture and not valid_session(json.loads(session.read_text())):
            raise ValueError('Exact canonical desktop session pins required')
        output.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        hashes = {key: hashlib.sha256(code.read_bytes()).hexdigest() for key, code in
                  (('baseline', baseline), ('candidate', candidate))}
        request = output.parent/'requests'/('f'*24+'-'+uuid.uuid4().hex+'.json')
        atomic_write_json(request, {'action':'read_only_control_lookup_benchmark',
                                   'fixture':fixture, 'source_hashes':hashes})
        relay = interactive_transport(root)
        w, quote = DesktopBridge.windows_path, DesktopBridge.quote
        command = ('& '+quote(w(ROOT/'scripts/benchmark_tej_control_lookup.ps1'))
            +' -BridgeScript '+quote(w(candidate))+' -BaselineScript '+quote(w(baseline))
            +' -Output '+quote(w(output))+' -Iterations '+str(iterations)
            +(' -Fixture' if fixture else ' -Session '+quote(w(session))))
        result = run_guarded_windows(command, request=request, windows_path=w, timeout=180,
            windows_session_id=relay['windows_session_id'], interop_socket=relay['interop_socket'])
        if result.returncode or not output.is_file():
            atomic_write_bytes(output.with_suffix('.stderr.txt'), result.stderr[:32768], mode=0o600)
            raise RuntimeError('Control benchmark failed; retained private diagnostic, no query sent')
        receipt = json.loads(output.read_text(encoding='utf-8-sig'))
        if (receipt.get('contract') != 'tej_exact_control_lookup_abba_v1'
                or receipt.get('accepted') is not True
                or receipt.get('provider_queries_sent') != 0 or receipt.get('desktop_actions_sent') != 0
                or receipt.get('queue_modified') is not False or receipt.get('source_values_exposed') is not False
                or receipt.get('source_selection_unchanged') is not True or receipt.get('exact_handle_parity') is not True
                or any(receipt.get(key+'_sha256') != digest for key, digest in hashes.items())):
            raise ValueError('Exact source/control comparison was not accepted')
        return receipt


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=ROOT/'data_tej')
    parser.add_argument('--baseline', type=Path, required=True)
    parser.add_argument('--candidate', type=Path)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--fixture', action='store_true')
    parser.add_argument('--iterations', type=int, default=10)
    args = parser.parse_args(argv)
    receipt = run(args.root, args.baseline, args.output, fixture=args.fixture,
                  iterations=args.iterations, candidate=args.candidate)
    print(json.dumps({key:receipt[key] for key in ('contract','accepted','samples','checks')}, ensure_ascii=False))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())

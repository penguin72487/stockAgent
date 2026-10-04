#!/usr/bin/env python3
"""Check responsibility extraction against a recorded pre-change source file."""
from __future__ import annotations

import argparse
import ast
import base64
from dataclasses import asdict
from datetime import date, datetime, timezone
from decimal import Decimal
import hashlib
import importlib.util
import json
from pathlib import Path
import pickle
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from downloader.artifact_io import atomic_write_json  # noqa: E402
from downloader import download_openbb_archive as current  # noqa: E402

MOVED = {
    'downloader/openbb_archive_types.py': ('AssetRecord', 'DownloadTask', 'TaskResult',
                                          'ColumnarTaskPayload', 'CoverageDecision', 'PlannerContext'),
    'downloader/openbb_archive_serialization.py': ('_json_default', '_canonical_json', '_write_json_atomic'),
    'downloader/openbb_request_checkpoints.py': ('_request_checkpoint_path', '_load_request_checkpoint',
                                               '_save_request_checkpoint', '_clear_request_checkpoints'),
}


def logic(tree):
    moved = {name for names in MOVED.values() for name in names}
    for node in tree.body:
        if isinstance(node, (ast.Import, ast.ImportFrom)) or getattr(node, 'name', None) in moved:
            continue
        if isinstance(node, ast.Try) and all(isinstance(n, (ast.Import, ast.ImportFrom)) for n in node.body):
            continue
        yield ast.dump(node, include_attributes=False)


def verify(baseline: Path, legacy: Path, output: Path) -> dict:
    before = ast.parse(baseline.read_bytes())
    after = ast.parse((ROOT / 'downloader/download_openbb_archive.py').read_bytes())
    before_definitions = {n.name: ast.dump(n, include_attributes=False) for n in before.body if hasattr(n, 'name')}
    count = 0
    for file, names in MOVED.items():
        definitions = {n.name: ast.dump(n, include_attributes=False)
                       for n in ast.parse((ROOT / file).read_bytes()).body if hasattr(n, 'name')}
        if any(before_definitions[n] != definitions[n] for n in names):
            raise ValueError('extracted definitions differ from the recorded implementation')
        count += len(names)
    if list(logic(before)) != list(logic(after)):
        raise ValueError('archive orchestration/provider/manifest logic changed during extraction')
    # These are explicitly engineering-only bytes captured before extraction,
    # not an arbitrary remote pickle or a market-data deserialization route.
    recorded = json.loads(legacy.read_text())
    values = [pickle.loads(base64.b64decode(value)) for value in recorded['pickles']]
    if [type(v).__name__ for v in values] != ['AssetRecord', 'DownloadTask', 'TaskResult', 'CoverageDecision']:
        raise ValueError('legacy process payloads no longer resolve the compatibility aliases')
    task = values[1]
    if task.attempts != 3 or task.provider_outcomes != {'sec': 'empty'} or task.task_id != 'engineering-id':
        raise ValueError('legacy task observation/identity changed')
    spec = importlib.util.spec_from_file_location('architecture_openbb_before', baseline)
    original = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = original
    spec.loader.exec_module(original)
    payload = {'decimal': Decimal('1.250'), 'date': date(2020, 1, 1),
               'clock': datetime(2020, 1, 1, tzinfo=timezone.utc), 'path': Path('relative'),
               'integer': 2**63+29, 'unicode': '來源', 'unknown': None}
    if original._canonical_json(payload) != current._canonical_json(payload):
        raise ValueError('canonical JSON bytes changed')
    raw = asdict(task)
    previous = original.DownloadTask(**raw)
    if asdict(previous) != asdict(task):
        raise ValueError('task dataclass envelope changed')
    result = {'schema_version': 1, 'state': 'accepted', 'moved_definitions': count,
              'definitions_ast_identical': True, 'remaining_orchestration_ast_identical': True,
              'legacy_process_payloads_compatible': len(values), 'task_envelope_equal': True,
              'canonical_json_bytes_equal': True,
              'baseline_sha256': hashlib.sha256(baseline.read_bytes()).hexdigest(),
              'source_files': {p: hashlib.sha256((ROOT / p).read_bytes()).hexdigest()
                               for p in ['downloader/download_openbb_archive.py', *MOVED]},
              'scope': 'structural extraction and engineering compatibility; no provider/API throughput or source completeness claim'}
    atomic_write_json(output, result)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--baseline', required=True, type=Path)
    parser.add_argument('--legacy-fixture', required=True, type=Path)
    parser.add_argument('--output', required=True, type=Path)
    args = parser.parse_args()
    result = verify(args.baseline, args.legacy_fixture, args.output)
    print(json.dumps({k: result[k] for k in ('state', 'moved_definitions', 'legacy_process_payloads_compatible')}))


if __name__ == '__main__':
    main()

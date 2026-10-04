#!/usr/bin/env python3
"""Bounded, read-only TEJ end-to-end timing profile; never queries a provider."""
from __future__ import annotations

import argparse
from contextlib import closing
from datetime import UTC, datetime
import hashlib
import json
import math
from pathlib import Path
import sqlite3
import statistics
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from downloader.artifact_io import atomic_write_json
from downloader.tej_history import SOURCE_SCOPE_CONTRACT

PROFILE_CONTRACT = 'tej_bounded_acquisition_timing_profile_v2_scope_stages'
SCOPE_TIMING_PHASES = frozenset({
    'context_and_source_binding', 'field_selection',
    'company_universe_and_lookup', 'date_model_and_lookup',
    'company_selection_and_verification', 'date_selection_and_verification',
    'final_source_and_preview_guards',
})


def profile(root: Path, output: Path, *, limit: int = 60) -> dict:
    if output.exists():
        raise FileExistsError('Retain the original timing profile')
    if type(limit) is not int or not 1 <= limit <= 256:
        raise ValueError('Timing sample must be bounded to 1..256 tasks')
    root = root.resolve()
    query = ('SELECT task_id,table_id,active_attempt_id,attempted_at_utc,'
             'completed_at_utc,seconds,actual_rows FROM tasks '
             'WHERE scope_contract=? AND state=? AND kind=? AND timing_basis=? '
             'ORDER BY completed_at_utc DESC LIMIT ?')
    args = (SOURCE_SCOPE_CONTRACT, 'complete', 'download', 'fresh_end_to_end', limit)
    started = time.monotonic()
    with closing(sqlite3.connect(f'file:{root / "queue.sqlite3"}?mode=ro', uri=True, timeout=2)) as con:
        con.row_factory = sqlite3.Row
        con.execute('BEGIN')
        observed = datetime.now(UTC).isoformat()
        plan = [dict(r) for r in con.execute('EXPLAIN QUERY PLAN '+query, args)]
        rows = [dict(r) for r in con.execute(query, args)]
        names = {r['table_id']:r['name'] for r in con.execute('SELECT table_id,name FROM tables')}
        con.rollback()
    query_seconds = time.monotonic()-started
    samples = []
    for row in rows:
        attempt = row['active_attempt_id']
        sample = {k:row[k] for k in ('task_id','seconds','actual_rows','completed_at_utc')}
        sample['table'] = names[row['table_id']]
        prepared = root/'requests'/(attempt+'.json') if attempt else None
        stage = root/'raw'/(attempt+'.json.stage.json') if attempt else None
        if prepared and prepared.is_file() and prepared.stat().st_size <= 2*1024**2:
            request = json.loads(prepared.read_text(encoding='utf-8-sig'))
            sample.update(fields=len(request.get('fields', [])),
                          companies=len(request.get('company_labels', [])),
                          dates=len(request.get('date_labels', [])))
            sample['prepared_sha256'] = hashlib.sha256(prepared.read_bytes()).hexdigest()
        if stage and stage.is_file() and stage.stat().st_size <= 2*1024**2:
            evidence = json.loads(stage.read_text(encoding='utf-8-sig'))
            if evidence.get('task_id') == row['task_id'] and evidence.get('stage') == 'prepreview_verified':
                delta = (datetime.fromisoformat(evidence['observed_at_utc'])
                         -datetime.fromisoformat(row['attempted_at_utc'])).total_seconds()
                if row['seconds'] is not None and 0 <= delta <= row['seconds']:
                    sample['before_submission_seconds'] = delta
                    sample['remaining_including_query_readback_validation_seconds'] = row['seconds']-delta
                if (evidence.get('scope_preparation_timings_contract') == 'monotonic_complete_scope_stages_v1'
                        and isinstance(evidence.get('scope_preparation_seconds'),dict)):
                    sample['scope_preparation_seconds'] = {
                        name: seconds for name, seconds in evidence['scope_preparation_seconds'].items()
                        if name in SCOPE_TIMING_PHASES and type(seconds) in (int, float)
                        and math.isfinite(seconds) and 0 <= seconds <= 900
                    }
        samples.append(sample)

    def summary(selected):
        durations = [r['seconds'] for r in selected if r['seconds'] is not None]
        return {'tasks':len(selected),'exported_rows':sum(r['actual_rows'] or 0 for r in selected),
                'sum_end_to_end_seconds':sum(durations),
                'median_end_to_end_seconds':statistics.median(durations) if durations else None}

    preparations = [r['before_submission_seconds'] for r in samples if 'before_submission_seconds' in r]
    result = {'contract':PROFILE_CONTRACT,'sql_snapshot_at_utc':observed,
              'provider_queries_sent':0,'queue_modified':False,'raw_values_exposed':False,
              'timing_basis':'fresh_end_to_end','selection_seconds':query_seconds,
              'selection_plan':plan,'sample_limit':limit,'samples':samples,
              'all':summary(samples),'nonempty':summary([r for r in samples if r['actual_rows']]),
              'empty':summary([r for r in samples if not r['actual_rows']]),
              'median_before_submission_seconds':statistics.median(preparations) if preparations else None,
              'interpretation':'Retrospective complete-task timings; not a same-scope speed comparison, API rate limit or full-history completeness proof'}
    atomic_write_json(output, result)
    return result


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=ROOT/'data_tej')
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--limit', type=int, default=60)
    args = parser.parse_args(argv)
    result = profile(args.root, args.output, limit=args.limit)
    print(json.dumps({k:v for k,v in result.items() if k not in ('samples','selection_plan')}, ensure_ascii=False))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())

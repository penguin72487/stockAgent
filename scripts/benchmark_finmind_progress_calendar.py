"""Fixed-input, local-only ETA/calendar acceptance; never fetch provider data."""
from __future__ import annotations

import argparse
from contextlib import closing
from datetime import UTC, date, datetime, timedelta
from functools import lru_cache
import hashlib
import json
from pathlib import Path
import resource
import sqlite3
import statistics
import tempfile
import time

from downloader.artifact_io import atomic_write_json
from downloader import download_finmind_complement as worker
from downloader import finmind_eta_stages as stages
from downloader import finmind_supplemental as supplemental
from downloader.finmind_history_calendar import load_closures, reconcile_closures
from downloader.finmind_scheduling import TAIPEI
from stockagent.live.market_status import tw_stock_day_decision


def nonempty_digest(conn):
    digest = hashlib.sha256()
    rows = 0
    for row in conn.execute('SELECT dataset,data_id,partition,rows,bytes,receipt_path FROM tasks '
                            'WHERE rows>0 ORDER BY dataset,data_id,partition'):
        digest.update(json.dumps(row, separators=(',', ':')).encode())
        rows += 1
    return {'nonempty_tasks': rows, 'metadata_sha256': digest.hexdigest()}


def plan(conn, now):
    frontiers = supplemental.frontier_status(conn, now)
    queued = dict(conn.execute("SELECT dataset,count(*) FROM tasks WHERE state IN ('pending','failed','inflight') "
                               'GROUP BY dataset'))
    return {name: value['unseeded_partition_candidates'] + queued.get(name, 0)
            for name, value in frontiers.items()}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path('data_finmind'))
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--repeat', type=int, default=3)
    args = parser.parse_args()
    if not 1 <= args.repeat <= 10:
        parser.error('--repeat must be 1..10')
    root = args.root.resolve()
    snapshot = json.loads((root / 'eta_status.json').read_bytes())
    now = datetime.fromisoformat(snapshot['estimate']['observed_at_utc'])
    public_root = root.parent / 'data_tw_public'

    @lru_cache(maxsize=4096)
    def decision(day):
        return tw_stock_day_decision(day, parquet_root=public_root, observed=now)

    def protected(day):
        if day > now.astimezone(TAIPEI).date() + timedelta(days=366):
            return day.weekday() < 5
        item = decision(day)
        return item.status != 'closed' and (item.is_session or day.weekday() < 5)

    # Only the copy is mutable. A transactionally consistent backup, rather
    # than a bare copy of an active WAL, keeps the candidate comparison fixed.
    with tempfile.TemporaryDirectory(prefix='finmind-calendar-proof-') as folder:
        with closing(sqlite3.connect((root / 'complement' / 'queue.sqlite3').as_uri() + '?mode=ro', uri=True)) as source:
            with closing(sqlite3.connect(Path(folder) / 'queue.sqlite3')) as conn:
                source.backup(conn)
                old_closures = load_closures(conn)
                before_plan, before_rows = plan(conn, now), nonempty_digest(conn)
                from downloader.download_finmind_sponsor import _official_session_calendar
                proof = _official_session_calendar()
                assert proof is not None, 'verified_actual_calendar_required_for_benchmark'
                new_closures = reconcile_closures(conn, supplemental.SOURCES, proof, now, day_decision=decision)
                after_plan, after_rows = plan(conn, now), nonempty_digest(conn)
                assert before_rows == after_rows, 'nonempty_receipt_metadata_changed'
                newly_closed = sorted(set(new_closures.days) - set(old_closures.days))
                calendar_report = {'before_candidate_requests': sum(before_plan.values()),
                                   'after_candidate_requests': sum(after_plan.values()),
                                   'before_by_dataset': before_plan, 'after_by_dataset': after_plan,
                                   'new_compact_closed_days': newly_closed, 'nonempty_receipts': after_rows}

    # Isolate duplicated date walking, not unrelated HEAD/dirty changes:
    # identical new seven-contract ETA model and frozen inputs, alternating
    # the redundant independent calendar projection versus its removal.
    estimator = stages.estimate_completion
    outputs = {}
    timings = {'redundant_calendar_baseline': [], 'single_ordered_calendar': []}
    cpu = {key: [] for key in timings}
    try:
        for trial in range(args.repeat):
            order = list(timings) if trial % 2 == 0 else list(reversed(timings))
            for mode in order:
                def estimate(*a, **kw):
                    kw['_project_calendar'] = mode == 'redundant_calendar_baseline'
                    return estimator(*a, **kw)
                stages.estimate_completion = estimate
                clock, usage = time.perf_counter(), resource.getrusage(resource.RUSAGE_SELF)
                output = stages.ordered_estimate(snapshot['workload'], snapshot['telemetry'], now,
                                                 day_is_protected=protected,
                                                 secondary_admission=snapshot['secondary_admission'])
                timings[mode].append(time.perf_counter() - clock)
                finished = resource.getrusage(resource.RUSAGE_SELF)
                cpu[mode].append(finished.ru_utime + finished.ru_stime - usage.ru_utime - usage.ru_stime)
                digest = hashlib.sha256(json.dumps(output, sort_keys=True).encode()).hexdigest()
                if mode in outputs:
                    assert outputs[mode] == digest, 'non_deterministic_fixed_input_projection'
                outputs[mode] = digest
        assert len(set(outputs.values())) == 1, 'calendar_optimization_changed_estimate'
    finally:
        stages.estimate_completion = estimator
    report = {'schema_version': 1, 'passed': True, 'observed_at_utc': datetime.now(UTC).isoformat(),
              'fixed_estimate_cutoff_utc': now.isoformat(), 'extra_provider_calls': 0,
              'production_queue_writes': 0, 'calendar': calendar_report,
              'estimate': {'mode': 'same_v7_model_redundant_calendar_vs_single_ordered_projection',
                           'repeat': args.repeat, 'wall_seconds': timings, 'cpu_seconds': cpu,
                           'median_wall_seconds': {key: statistics.median(values) for key, values in timings.items()},
                           'output_sha256': outputs, 'peak_process_rss_kib': resource.getrusage(resource.RUSAGE_SELF).ru_maxrss},
              'not_claimed': ['full_history_complete', 'measured_provider_throughput_gain',
                              'candidate_difference_equals_actual_saved_requests']}
    atomic_write_json(args.output, report)
    print(json.dumps({'passed': True, 'candidate_difference': sum(before_plan.values()) - sum(after_plan.values()),
                      'wall_medians': report['estimate']['median_wall_seconds'], 'output': str(args.output)}))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())

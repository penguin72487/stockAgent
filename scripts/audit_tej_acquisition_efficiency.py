"""Compare the same TEJ source scope against explicit local capacity bounds.

Read-only: no desktop, provider call, queue write, raw observation or deletion.
Counts are query geometry, not an observed download-speed improvement.
"""
from __future__ import annotations

import argparse
from contextlib import closing
from datetime import UTC, datetime
import json
import math
from pathlib import Path
import sqlite3
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from downloader.artifact_io import atomic_write_json
from downloader.tej_eta import query_geometry
from downloader.tej_history import SOURCE_SCOPE_CONTRACT, expand_request
from downloader.tej_key_layout import key_count
from downloader.tej_planning import TILING_CONTRACT, build_plan, next_request
from stockagent.live.tej_dashboard import build_tej_public_status


def audit(repo: Path, output: Path, *, max_cells: int, max_companies: int) -> dict:
    if output.exists():
        raise FileExistsError('Retain original efficiency evidence')
    if not 100 <= max_cells <= 400000 or not 1 <= max_companies <= 1024:
        raise ValueError('Outside reviewed native Preview bounds')
    repo = repo.resolve()
    root = repo / 'data_tej'
    status = build_tej_public_status(repo)
    rows = []
    with closing(sqlite3.connect(f'file:{root / "queue.sqlite3"}?mode=ro', uri=True)) as con:
        con.row_factory = sqlite3.Row
        con.execute('BEGIN')
        config = json.loads(con.execute("SELECT value FROM meta WHERE key='config'").fetchone()[0])
        proposed = {**config, 'max_cells_per_export':max_cells,
                    'max_companies_per_export':max_companies, 'query_tiling_contract':TILING_CONTRACT}
        for table in status['tables']:
            row = con.execute('SELECT * FROM download_plans WHERE table_id=?', (table['table_id'],)).fetchone()
            before = table['forecast']['remaining_queries']['middle']
            after = lower = None
            basis = 'unverified_axis_scenario_not_a_plan'
            if row:
                plan = json.loads(row['plan_json'])
                if plan['total_queries']:
                    next_request(plan, 0)  # Exact immutable scope fingerprint.
                definition = dict(con.execute('SELECT * FROM tables WHERE table_id=?', (table['table_id'],)).fetchone())
                finished = [dict(r) for r in con.execute("SELECT * FROM tasks WHERE table_id=? "
                    "AND kind='download' AND state='complete' AND scope_contract=?",
                    (table['table_id'], SOURCE_SCOPE_CONTRACT))]
                done = [expand_request(json.loads(r['request_json']), definition, table['table_id']) for r in finished]
                request = {**plan['request'], 'max_cells':max_cells}
                try:
                    candidate = build_plan(request, plan['companies'], plan['dates'], proposed, done)
                    resolved = sum(r['work_expected_rows'] if r['work_expected_rows'] is not None else r['expected_rows'] for r in finished)
                    if (candidate['total_work_rows'] != plan['total_work_rows']
                            or candidate['completed_work_rows'] != resolved):
                        raise ValueError('Coverage changed')
                    after = min(before, candidate['total_queries'])
                    lower = math.ceil(candidate['remaining_work_rows'] / request['max_rows'])
                    basis = 'exact_same_axes_completed_coverage_and_30_columns'
                except ValueError:
                    # A different key era / snapshot must be reconciled by
                    # its source contract, not dropped to make a faster plan.
                    basis = 'coverage_reconciliation_required_not_optimized'
            elif table.get('universe_count') and table.get('grid_dates'):
                after, _ = query_geometry(table['universe_count'], table['grid_dates'], table['fields'],
                                          table.get('source_key_mode') or 2, proposed)
            rows.append({'table_id':table['table_id'], 'table':table['name'],
                         'fields':table['fields'], 'axis_verified':row is not None,
                         'before_queries':before, 'after_queries':after,
                         'capacity_volume_lower_bound':lower, 'basis':basis})
        con.rollback()
    measured = [r for r in rows if r['axis_verified'] and r['after_queries'] is not None]
    before = sum(r['before_queries'] for r in measured)
    after = sum(r['after_queries'] for r in measured)
    report = {'contract':'tej_same_scope_capacity_comparison_v1', 'observed_at_utc':datetime.now(UTC).isoformat(),
              'before_config':{k:config[k] for k in ('max_rows_per_export','max_cells_per_export','max_companies_per_export')},
              'proposed_config':{k:proposed[k] for k in ('max_rows_per_export','max_cells_per_export','max_companies_per_export')},
              'preview_max_columns':30, 'tables':rows, 'comparable_tables':len(measured),
              'before_remaining_queries':before, 'after_remaining_queries':after,
              'queries_saved':before-after, 'reduction_ratio':1-after/before if before else None,
              'provider_queries_sent':0, 'queue_modified':False, 'source_values_read':False,
              'public_input_sha256':status['eta']['forecast']['input_sha256'],
              'interpretation':'Exact query-count comparison, not a deployment, measured speedup, official quota or global packing optimum'}
    atomic_write_json(output, report)
    return report


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--repo', type=Path, default=ROOT)
    p.add_argument('--output', type=Path, required=True)
    p.add_argument('--max-cells', type=int, default=400000)
    p.add_argument('--max-companies', type=int, default=128)
    a = p.parse_args(argv)
    result = audit(a.repo, a.output, max_cells=a.max_cells, max_companies=a.max_companies)
    print(json.dumps({k:v for k,v in result.items() if k not in ('tables',)}, ensure_ascii=False))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())

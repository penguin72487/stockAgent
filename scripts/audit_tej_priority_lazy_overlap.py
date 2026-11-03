#!/usr/bin/env python3
"""Read-only independent cell audit for one future TEJ lazy-plan rectangle."""
from __future__ import annotations

import argparse
from collections import Counter
from contextlib import closing
from datetime import UTC, datetime
import hashlib
import json
from pathlib import Path
import sqlite3
import sys

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))
from downloader.artifact_io import atomic_write_json, sha256_file
from downloader.tej_history import SOURCE_SCOPE_CONTRACT, expand_request
from downloader.tej_planning import next_request
from downloader.tej_priority_coverage import _retained_receipt, exclude_verified_scopes


def oracle_cells(request: dict) -> set[tuple[str, str, str]]:
    # Independent string/clock parsing for the cell-level comparison. The
    # original source labels remain untouched in every submitted rectangle.
    def source_code(label: str) -> str:
        code, sep, _ = label.partition('=>')
        if not sep:
            raise ValueError('Unrecognized native company label')
        return code.removesuffix('=')
    def day(value: str) -> str:
        digits = value.replace('/', '').replace('-', '')
        return datetime.strptime(digits, '%Y%m%d').date().isoformat()
    return {(source_code(company), day(period), field)
            for company in request['company_labels'] for period in request['date_labels']
            for field in request['fields']}


def audit(root: Path, table_id: str, ordinal: int) -> dict:
    root = root.resolve(strict=True)
    db = (root / 'queue.sqlite3').resolve(strict=True)
    with closing(sqlite3.connect(db.as_uri() + '?mode=ro', uri=True)) as con:
        con.row_factory = sqlite3.Row
        con.execute('BEGIN')
        definition_row = con.execute('SELECT * FROM tables WHERE table_id=?', (table_id,)).fetchone()
        plan_row = con.execute('SELECT * FROM download_plans WHERE table_id=?', (table_id,)).fetchone()
        if definition_row is None or plan_row is None or ordinal < plan_row['next_query']:
            raise ValueError('A still-unmaterialized exact table/ordinal is required')
        definition = dict(definition_row)
        plan_text = plan_row['plan_json']
        plan = json.loads(plan_text)
        original = next_request(plan, ordinal)
        original_cells = oracle_cells(original)
        task_ids = set()
        for record in con.execute("SELECT value FROM meta WHERE key LIKE 'finite_priority_plan:%'"):
            for item in json.loads(record['value']).get('tasks', []):
                if item.get('table_id') == table_id:
                    task_ids.add(item['task_id'])
        witnesses = []
        receipts = []
        oracle_covered: set[tuple[str, str, str]] = set()
        for identifier in sorted(task_ids):
            row = con.execute('SELECT * FROM tasks WHERE task_id=?', (identifier,)).fetchone()
            if row is None or row['table_id'] != table_id or row['scope_contract'] != SOURCE_SCOPE_CONTRACT:
                raise ValueError('Retained priority identity differs')
            if row['state'] != 'complete':
                continue
            task = dict(row)
            request = expand_request(json.loads(task['request_json']), definition, table_id)
            if request.get('frequency') != 'daily' or request.get('source_key_mode', 2) != 2:
                continue
            overlap = oracle_cells(request).intersection(original_cells)
            if not overlap:
                continue
            _retained_receipt(root, task)
            receipt_path = root / task['receipt_path']
            receipt = json.loads(receipt_path.read_text(encoding='utf-8-sig'))
            witnesses.append({'task_id': identifier, 'request': request})
            oracle_covered.update(overlap)
            receipts.append({'task_id': identifier, 'receipt_path': str(receipt_path),
                             'receipt_sha256': sha256_file(receipt_path),
                             'raw_sha256': receipt['raw_sha256'], 'parquet_sha256': receipt['parquet_sha256']})
        children, exclusion = exclude_verified_scopes(table_id, ordinal, original, witnesses)
        observed = Counter(cell for child in children for cell in oracle_cells(child))
        if set(observed) != original_cells - oracle_covered or any(count != 1 for count in observed.values()):
            raise ValueError('Independent native source-cell oracle rejected the candidate split')
        if not witnesses or not exclusion['excluded_work_rows']:
            raise ValueError('The exact requested example has no completed priority overlap')
        return {'contract': 'tej_priority_lazy_overlap_readonly_audit_v1',
                'observed_at_utc': datetime.now(UTC).isoformat(), 'table_id': table_id,
                'table_name': definition['name'], 'plan_ordinal': ordinal,
                'current_next_query': plan_row['next_query'],
                'source_plan_json_sha256': hashlib.sha256(plan_text.encode()).hexdigest(),
                'plan_fingerprint': plan['fingerprint'],
                'audit_script_sha256': sha256_file(Path(__file__)),
                'coverage_implementation_sha256': sha256_file(REPO / 'downloader/tej_priority_coverage.py'),
                'planner_implementation_sha256': sha256_file(REPO / 'downloader/tej_planning.py'),
                'native_source_witnesses': receipts, 'exclusion': exclusion,
                'original_field_cells': len(original_cells), 'covered_field_cells': len(oracle_covered),
                'remaining_field_cells': len(observed), 'remaining_rectangles': len(children),
                'independent_cell_oracle_passed': True, 'canonical_queue_changed': False,
                'provider_queries_sent': 0, 'live_owner_adoption_verified': False,
                'training_ready': False}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=REPO / 'data_tej')
    parser.add_argument('--table-id', required=True)
    parser.add_argument('--ordinal', type=int, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        raise ValueError('Do not overwrite retained audit evidence')
    result = audit(args.root, args.table_id, args.ordinal)
    atomic_write_json(args.output, result)
    if json.loads(args.output.read_text()) != result:
        raise ValueError('Audit readback differs')
    print(json.dumps({k: result[k] for k in ('table_id', 'plan_ordinal', 'current_next_query',
        'original_field_cells', 'covered_field_cells', 'remaining_field_cells',
        'remaining_rectangles', 'independent_cell_oracle_passed', 'provider_queries_sent')}))


if __name__ == '__main__':
    main()

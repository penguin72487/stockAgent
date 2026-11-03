#!/usr/bin/env python3
"""Build finite TEJ tasks from an independently verified native menu witness.

The plan remains a local request. It does not query TEJ, accept market data,
or install the partial menu as the complete historical discovery.
"""
from __future__ import annotations

import argparse
from contextlib import closing
import json
from pathlib import Path
import sys

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

from downloader.artifact_io import atomic_write_json, sha256_file
from downloader.tej_history import SOURCE_SCOPE_CONTRACT, connect, task_request
from downloader.tej_planning import build_plan, next_request
from downloader.tej_priority import CONTRACT, finite_axis_dependency_pins
from scripts.audit_tej_finite_option_axis_capture import audit


def build(root: Path, observation: Path) -> dict:
    root = root.resolve(strict=True)
    observation = observation.resolve(strict=True)
    proof = audit(root, observation)
    if (proof.get('source_key_mode') != 2 or proof.get('date_menu_contains_day') is not True
            or proof.get('market_observations_obtained') is not False
            or proof.get('full_axis_admitted_to_plan') is not False
            or proof.get('training_input_admitted') is not False):
        raise ValueError('Exact daily Key=2 finite menu witness is required')
    with closing(connect(root)) as con:
        task = con.execute('SELECT * FROM tasks WHERE task_id=?', (proof['task_id'],)).fetchone()
        if (task is None or task['kind'] != 'discover' or task['scope_contract'] != SOURCE_SCOPE_CONTRACT):
            raise ValueError('Finite witness is not tied to the native discovery task')
        definition = con.execute('SELECT * FROM tables WHERE table_id=?', (task['table_id'],)).fetchone()
        if (definition is None or definition['discovery_path'] is not None
                or any(proof.get(k) != definition[v] for k, v in
                       (('type', 'query_type'), ('smart_id', 'smart_id'), ('table', 'name')))):
            raise ValueError('Use complete discovery when available; source binding must match')
        source_request = task_request(root, dict(task), connection=con)
        config = json.loads(con.execute("SELECT value FROM meta WHERE key='config'").fetchone()[0])
    native = json.loads(observation.read_text(encoding='utf-8-sig'))
    plan = build_plan(source_request, native['company_labels'], native['date_labels'], config, [])
    if not 1 <= plan['total_queries'] <= 1000:
        raise ValueError('Finite source plan is empty or exceeds the priority batch bound')
    reference = {'path': str(observation), 'sha256': sha256_file(observation)}
    pins = finite_axis_dependency_pins(root, observation, proof)
    requests = []
    for ordinal in range(plan['total_queries']):
        request = next_request(plan, ordinal)
        requests.append({'table_id': definition['table_id'], 'request': request,
                         'priority': -20, 'discovery_sha256': reference['sha256'],
                         'finite_axis_observation': reference,
                         'reason': 'Official positive option contract/date cross-check with a verified finite TEJ menu'})
    return {'contract': CONTRACT,
            'sources': [{'path': path, 'sha256': digest} for path, digest in sorted(pins.items())],
            'requests': requests, 'finite_axis_only': True, 'source_observation': reference,
            'provider_queries_by_planning': 0, 'full_axis_admitted_to_plan': False,
            'training_input_admitted': False}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=REPO / 'data_tej')
    parser.add_argument('--observation', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        raise FileExistsError('Existing finite plan must be preserved')
    plan = build(args.root, args.observation)
    atomic_write_json(args.output, plan, durable=True)
    print(json.dumps({'plan': str(args.output), 'tasks': len(plan['requests']),
                      'provider_queries': 0, 'source_data_admitted': False}))


if __name__ == '__main__':
    main()

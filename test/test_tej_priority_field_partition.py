from collections import Counter
from contextlib import closing
from copy import deepcopy
import json

import pytest

from downloader.artifact_io import atomic_write_json, sha256_file
from downloader.tej_history import (company_code, configure_preview_planning, connect,
                                    normalize_period, run_one)
from downloader.tej_planning import next_request
from downloader.tej_priority import CONTRACT, register_priority_requests
from test_tej_planning import DynamicBridge, axes, registry as registry


def priority_plan(registry, tmp_path):
    root, config = registry
    configure_preview_planning(root, config)
    bridge = DynamicBridge()
    assert run_one(root, bridge) == 'completed_task'
    with closing(connect(root)) as con:
        definition = dict(con.execute('SELECT * FROM tables').fetchone())
        plan = json.loads(con.execute('SELECT plan_json FROM download_plans').fetchone()[0])
    discovery = root / definition['discovery_path']
    source = tmp_path / 'known-source.txt'
    source.write_text('verified external scope')
    items = []
    for ordinal in range(3):
        native = next_request(plan, ordinal)
        request = {**native, 'company_labels': native['company_labels'][:1],
                   'date_labels': native['date_labels'][:1]}
        items.append({'table_id': definition['table_id'], 'discovery_sha256': sha256_file(discovery),
                      'request': request, 'priority': -20, 'reason': 'verified finite field scope'})
    query = {'contract': CONTRACT, 'sources': [{'path': str(source), 'sha256': sha256_file(source)}],
             'requests': items}
    return root, bridge, query, tmp_path / 'priority.json', tmp_path / 'receipt.json'


def submit(batch):
    root, _, plan, path, receipt = batch
    atomic_write_json(path, plan)
    return register_priority_requests(root, path, receipt)


def test_three_source_bound_field_batches_preserve_all_fields_and_lazy_history(registry, tmp_path):
    batch = priority_plan(registry, tmp_path)
    root, bridge, _, _, _ = batch
    receipt = submit(batch)
    assert len(receipt['tasks']) == 3 and receipt['desktop_actions'] == 0
    assert receipt['global_plan_changed'] is False
    for _ in range(15):
        if run_one(root, bridge) == 'idle':
            break
    else:
        pytest.fail('Owner did not finish the field-partition fixture')
    seen = Counter()
    for req in bridge.requests:
        if req['action'] != 'download':
            continue
        assert len(req['fields']) + 2 <= 30
        for field in req['fields']:
            for company in req['company_labels']:
                for period in req['date_labels']:
                    seen[(field, company_code(company), normalize_period(period))] += 1
    companies, dates = axes()
    assert len(seen) == 59 * len(companies) * len(dates)
    assert set(seen.values()) == {1}
    with closing(connect(root)) as con:
        assert con.execute("SELECT sum(work_expected_rows) FROM tasks WHERE kind='download' AND state='complete'").fetchone()[0] == 30
        assert con.execute('SELECT work_grid_rows FROM tables').fetchone()[0] == 30


@pytest.mark.parametrize('fault', ['different_fingerprint', 'unaligned_range', 'changed_catalog',
                                   'changed_fields', 'unbound_daily_mode', 'overlap'])
def test_unverified_field_partition_never_changes_queue(registry, tmp_path, fault):
    batch = priority_plan(registry, tmp_path)
    root, _, plan, _, receipt = batch
    req = plan['requests'][0]['request']
    if fault == 'different_fingerprint':
        req['field_partition']['plan_fingerprint'] = 'f' * 64
    elif fault == 'unaligned_range':
        req['field_partition']['field_range'] = [1, 28]
        req['fields'] = req['catalog_fields'][1:28]
    elif fault == 'changed_catalog':
        req['catalog_fields'] = ['not the registered catalog']
    elif fault == 'changed_fields':
        req['fields'] = ['not the registered field']
    elif fault == 'unbound_daily_mode':
        req['frequency'] = 'monthly'
    else:
        plan['requests'].append(deepcopy(plan['requests'][0]))
    with pytest.raises(ValueError):
        submit(batch)
    assert not receipt.exists()
    with closing(connect(root)) as con:
        assert con.execute("SELECT count(*) FROM tasks WHERE kind='download'").fetchone()[0] == 0
        assert con.execute("SELECT count(*) FROM meta WHERE key LIKE 'finite_priority_plan:%'").fetchone()[0] == 0

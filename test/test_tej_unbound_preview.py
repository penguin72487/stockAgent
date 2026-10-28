"""Unknown native values must survive before a bounded authorized fresh query."""
from contextlib import closing
from copy import deepcopy
from datetime import UTC, datetime, timedelta
import json
from pathlib import Path
import uuid

import pytest

from downloader.artifact_io import atomic_write_json
from downloader.tej_desktop_attempts import authorized_replay_policy, retry_unknown_download
from downloader.tej_history import configure_runtime_policy, connect
from downloader.tej_unbound_preview import CONFIG_HEADERS, CONTRACT
from test_tej_authorized_replay import FreshReplayBridge, age_original, grant_config
from test_tej_history import registry
from test_tej_startup import abandoned, retire_expired_worker, surviving_desktop


@pytest.fixture
def retained(abandoned):
    root, task, attempt, prepared, now = abandoned
    retire_expired_worker(root, now=now)
    surviving_desktop(root)
    age_original(root, attempt)
    config = grant_config()
    configure_runtime_policy(root, config)
    return root, task, attempt, prepared, now, config


class RetainedPreview(FreshReplayBridge):
    def __init__(self, now, fault=None):
        super().__init__()
        self.birth = (now - timedelta(days=1)).isoformat()
        self.fault, self.inspections = fault, 0
        self.grids = None

    def execute(self, root, task):
        req = json.loads(task['request_json'])
        fields = req['fields']
        if self.grids is None:
            self.grids = [dict(handle=11, native_shape=[4, len(fields) + 3],
                               source_headers=['Top Left Header Cell', 'Future Code', 'Date'] + fields),
                          dict(handle=12, native_shape=[2, 10], source_headers=CONFIG_HEADERS)]
            if self.fault == 'unknown_grid':
                self.grids.append(dict(handle=13, native_shape=[2, 3], source_headers=['unknown']))
        if req['action'] == 'preserve_unbound_preview':
            self.calls.append(req['action'])
            assert req['source_key_mode'] == 2  # Legacy requests can omit the default Key=2.
            assert not any(req.get(k) for k in ('query_attempt_id', 'original_query_scope_proof', 'original_query_stage'))
            cells = [['Future Code', 'Date'] + fields, ['AA', '2014/01/02'] + ['NULL'] * len(fields),
                     ['BB', '2014/01/03'] + ['unattributed original value'] * len(fields)]
            payload = dict(contract_version=4, provider='tej_smart_wizard', action=req['action'],
                preservation_contract=CONTRACT, observed_at_utc=datetime.now(UTC).isoformat(),
                task_id=task['task_id'], **{k: req[k] for k in ('type', 'smart_id', 'table', 'fields')},
                **{k: False for k in ('market_data_query_submitted', 'date_text_input_sent',
                    'query_button_invoked', 'source_rows_adopted', 'credentials_read',
                    'original_query_result_claimed', 'fresh_preview_transition_verified')},
                query_attempt_id=None, source_scope_proof=None,
                date_axis='unattributed_current_preview_not_original_observation',
                vendor_numeric_scale_readback_basis='current_ui_only_original_query_setting_unverified',
                capture_method='native_msaa_preview_full',
                source_value_representation='vendor_display_strings_not_underlying_excel_values',
                selected_field_order_readback_verified=True, grid=deepcopy(self.grids[0]),
                owned_desktop_process_start_utc=self.birth,
                preview_signature_before='a' * 64, preview_signature_after='a' * 64,
                cells=cells, source_grid_columns=len(fields) + 2, source_grid_rows=2, source_key_mode=2)
            payload['preview'] = [4, [[i + 1, deepcopy(row)] for i, row in enumerate(cells)]
                                  + [[4, [''] * (len(fields) + 2)]]]
            if self.fault == 'ragged':
                payload['cells'][-1].pop()
            elif self.fault == 'truncated':
                payload['cells'].pop(); payload['source_grid_rows'] -= 1
            elif self.fault == 'changed_values':
                payload['preview_signature_after'] = 'b' * 64
            elif self.fault in ('query_attempt_id', 'source_scope_proof'):
                payload[self.fault] = 'unproved'
            elif self.fault == 'scope_claim':
                payload['original_query_result_claimed'] = True
            elif self.fault == 'visible_scrollbar':
                payload['grid']['native_shape'][0] += 1
            elif self.fault == 'different_grid':
                payload['grid']['handle'] += 1
            elif self.fault == 'unbounded_child_change':
                payload['grid']['native_shape'][0] += 3
            output = root / 'raw' / (task['task_id'] + '-' + uuid.uuid4().hex + '.json')
            atomic_write_json(output, payload)
            if self.fault == 'artifact_mismatch':
                atomic_write_json(output, {**payload, 'cells': []})
            return payload, output, 1.0
        if req['action'] == 'download':
            # Capture must be durably present before the first new query.
            audits = list((root / 'operator_replays').glob('*.json'))
            assert len(audits) == 1
            proof = json.loads(audits[0].read_text())['retained_unbound_preview_preservation']
            captured = json.loads((root / proof['capture']['path']).read_text())
            assert captured['cells'][-1][-1] == 'unattributed original value'
        payload, output, seconds = super().execute(root, task)
        if req['action'] == 'inspect_query_runtime':
            self.inspections += 1
            payload['current_preview_grids'] = deepcopy(self.grids)
            payload['owned_desktop_process_start_utc'] = self.birth
            if self.inspections == 2 and self.fault == 'changed_desktop':
                payload['owned_desktop_process_start_utc'] = datetime.now(UTC).isoformat()
            if self.inspections == 2 and self.fault == 'changed_shape':
                payload['current_preview_grids'][0]['native_shape'][0] += 1
            atomic_write_json(output, payload)
        return payload, output, seconds


def replay(retained, bridge):
    root, task, _, prepared, _, config = retained
    return retry_unknown_download(root, task['task_id'], bridge, prepared,
        allow_orphaned_worker_loss=True, allow_retained_preview_preservation=True,
        standing_authorization=authorized_replay_policy(config))


@pytest.mark.parametrize('visibility', [None, 'visible_scrollbar'])
def test_full_preservation_precedes_new_attempt_and_never_adopts_unbound_rows(retained, visibility):
    root, task, attempt, prepared, now, _ = retained
    original = prepared.read_bytes()
    result = replay(retained, RetainedPreview(now, visibility))
    assert result['state'] == 'completed_task'
    assert prepared.read_bytes() == original
    assert not (root / 'raw' / (attempt + '.json')).exists()
    with closing(connect(root)) as con:
        assert con.execute('SELECT state FROM desktop_attempts WHERE attempt_id=?', (attempt,)).fetchone()[0] == 'unknown_outcome'
        assert con.execute('SELECT actual_rows FROM tasks WHERE task_id=?', (task['task_id'],)).fetchone()[0] == 4
    audit = json.loads(next((root / 'operator_replays').glob('*.json')).read_text())
    proof = audit['retained_unbound_preview_preservation']
    assert proof['source_rows'] == 2 and proof['source_rows_adopted'] is False
    assert proof['original_outcome'] == 'unknown_retained_not_claimed_unsent'
    assert proof['capture']['sha256'] and proof['fresh_readback']['sha256']


def test_standing_scheduler_preserves_values_before_its_bounded_replay(retained):
    from downloader.tej_scheduler import replay_authorized_unknown
    root, task, attempt, prepared, now, config = retained
    original = prepared.read_bytes()
    bridge = RetainedPreview(now)
    result = replay_authorized_unknown(root, bridge, config)
    assert result['performed'] and result['state'] == 'completed_task'
    assert bridge.calls == ['inspect_query_runtime', 'preserve_unbound_preview',
        'inspect_query_runtime', 'download']
    assert prepared.read_bytes() == original
    with closing(connect(root)) as con:
        assert con.execute('SELECT state FROM desktop_attempts WHERE attempt_id=?',
            (attempt,)).fetchone()[0] == 'unknown_outcome'
        assert con.execute('SELECT actual_rows FROM tasks WHERE task_id=?',
            (task['task_id'],)).fetchone()[0] == 4


@pytest.mark.parametrize('fault', ['unknown_grid', 'ragged', 'truncated', 'changed_values',
    'query_attempt_id', 'source_scope_proof', 'scope_claim', 'artifact_mismatch',
    'changed_desktop', 'changed_shape', 'different_grid', 'unbounded_child_change'])
def test_unverified_or_incomplete_preservation_never_submits_query(retained, fault):
    root, task, _, prepared, now, _ = retained
    original = prepared.read_bytes()
    bridge = RetainedPreview(now, fault)
    with pytest.raises(ValueError):
        replay(retained, bridge)
    assert 'download' not in bridge.calls and prepared.read_bytes() == original
    with closing(connect(root)) as con:
        assert con.execute('SELECT state FROM tasks WHERE task_id=?', (task['task_id'],)).fetchone()[0] == 'blocked'
        assert not con.execute('SELECT 1 FROM desktop_replays').fetchone()

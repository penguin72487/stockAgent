"""Preserve an unstaged owned Preview without attributing or ingesting it.

The canonical replay owner retains its original unknown attempt and authority.
This read-only path only removes the risk of overwriting surviving source values;
it never proves the old query unsent, free, complete or historically available.
"""
from datetime import UTC, datetime
import hashlib
import json
from pathlib import Path
import re

from downloader.tej_operation_clock import source_operation_clock

CONTRACT = 'surviving_unstaged_preview_preservation_v1'
CONFIG_HEADERS = ['Top Left Header Cell', 'Table Field', 'Copy',
                  'Derived Field Name', 'Formula', 'Sort', 'Filter Operator',
                  'Filter Value', 'Ranking', 'Ranking Value']
FALSE_FLAGS = ('market_data_query_submitted', 'date_text_input_sent',
               'query_button_invoked', 'source_rows_adopted', 'credentials_read')


def _grids(payload: dict) -> list[dict]:
    grids = payload.get('current_preview_grids')
    if not isinstance(grids, list) or len(grids) > 16:
        raise ValueError('Complete bounded owned grid inventory required')
    handles = set()
    for grid in grids:
        if (not isinstance(grid, dict) or type(grid.get('handle')) is not int
                or grid['handle'] <= 0 or grid['handle'] in handles
                or not isinstance(grid.get('native_shape'), list)
                or len(grid['native_shape']) != 2
                or any(type(n) is not int or n < 0 for n in grid['native_shape'])
                or not isinstance(grid.get('source_headers'), list)
                or any(not isinstance(h, str) for h in grid['source_headers'])):
            raise ValueError('Exact owned grid handles, dimensions and headers required')
        handles.add(grid['handle'])
    return sorted(grids, key=lambda grid: grid['handle'])


def _readback(root: Path, task_id: str, path: Path, payload: dict, limit: int) -> dict:
    if path.is_symlink():
        raise ValueError('Private preservation artifact cannot redirect ownership')
    path = path.resolve()
    if (path.parent != (root / 'raw').resolve() or path.is_symlink()
            or not re.fullmatch(re.escape(task_id) + r'-[0-9a-f]{32}\.json', path.name)
            or not path.is_file() or not 0 < path.stat().st_size <= limit):
        raise ValueError('Exact bounded private preservation artifact required')
    raw = path.read_bytes()
    if json.loads(raw.decode('utf-8-sig')) != payload:
        raise ValueError('Preservation artifact differs from independent readback')
    return {'path': str(path.relative_to(root.resolve())),
            'sha256': hashlib.sha256(raw).hexdigest(), 'bytes': len(raw)}


def preserve_surviving_preview(root: Path, task: dict, request: dict, bridge,
                               before: dict) -> dict:
    """Called only by the locked, explicitly authorized worker-loss replay."""
    grids = _grids(before)
    fields = before.get('current_field_lists', [])
    if (len(fields) != 2 or fields[-1].get('items') != request['fields']
            or before.get('source_key_mode') != request.get('source_key_mode', 2)):
        raise ValueError('Unbound preservation requires exact current field schema')
    # The second DataGridView is the vendor's field-configuration metadata.
    # Any other populated grid must be preserved, never silently dismissed.
    metadata = [g for g in grids if g['source_headers'] == CONFIG_HEADERS]
    data = [g for g in grids if g['native_shape'][0] > 1 and g not in metadata]
    if len(metadata) > 1 or len(data) != 1:
        raise ValueError('One exact populated data grid required for full preservation')
    wire = {k: v for k, v in request.items()
            if k not in ('query_attempt_id', 'original_query_stage', 'original_query_scope_proof')}
    wire['action'] = 'preserve_unbound_preview'
    wire.setdefault('source_key_mode', before['source_key_mode'])
    started = datetime.now(UTC)
    saved, output, _ = bridge.execute(root, {**task, 'request_json': json.dumps(wire)})
    source_operation_clock(saved.get('observed_at_utc'), started, datetime.now(UTC))
    capture = _readback(root, task['task_id'], output, saved, 128 * 1024 ** 2)
    captured_grid = _grids({'current_preview_grids': [saved.get('grid')]})[0]
    if (saved.get('contract_version') != 4 or saved.get('provider') != 'tej_smart_wizard'
            or saved.get('action') != wire['action'] or saved.get('preservation_contract') != CONTRACT
            or saved.get('task_id') != task['task_id']
            or any(saved.get(k) != request.get(k) for k in ('type', 'smart_id', 'table', 'fields'))
            or any(saved.get(k) is not False for k in FALSE_FLAGS + ('original_query_result_claimed',
                                                                              'fresh_preview_transition_verified'))
            or saved.get('query_attempt_id') is not None or saved.get('source_scope_proof') is not None
            or saved.get('date_axis') != 'unattributed_current_preview_not_original_observation'
            or saved.get('vendor_numeric_scale_readback_basis') != 'current_ui_only_original_query_setting_unverified'
            or saved.get('capture_method') != 'native_msaa_preview_full'
            or saved.get('source_value_representation') != 'vendor_display_strings_not_underlying_excel_values'
            or saved.get('selected_field_order_readback_verified') is not True
            or any(captured_grid[k] != data[0][k] for k in ('handle', 'source_headers'))
            or captured_grid['native_shape'][1] != data[0]['native_shape'][1]
            # Showing Preview can add/remove its two ROLE_SYSTEM_SCROLLBAR
            # children. Full native row enumeration and independent values
            # below remain mandatory; child count alone is not a row count.
            or abs(captured_grid['native_shape'][0] - data[0]['native_shape'][0]) > 2
            or saved.get('owned_desktop_process_start_utc') != before.get('owned_desktop_process_start_utc')
            or not re.fullmatch(r'[0-9a-f]{64}', str(saved.get('preview_signature_before')))
            or saved.get('preview_signature_before') != saved.get('preview_signature_after')):
        raise ValueError('Complete unattributed native preservation proof required')
    cells = saved.get('cells')
    keys = request.get('source_key_mode', 2)
    columns = len(request['fields']) + keys
    if (not isinstance(cells, list) or not 2 <= len(cells) <= request['max_rows'] + 1
            or len(cells) * columns > request['max_cells']
            or any(not isinstance(row, list) or len(row) != columns
                   or any(value is not None and not isinstance(value, str) for value in row) for row in cells)
            or saved.get('source_grid_columns') != columns or saved.get('source_grid_rows') != len(cells) - 1
            or saved.get('source_key_mode') != keys
            or not len(cells) <= captured_grid['native_shape'][0] <= len(cells) + 3
            or captured_grid['native_shape'][1] != columns + 1):
        raise ValueError('Full rectangular native values and source dimensions required')
    from downloader.tej_header_mapping import validate_headers
    validate_headers(request, saved, cells[0][:keys])
    # Reuse the native independent object-path sample, but compare literal
    # row positions: an unbound capture cannot claim historical query keys.
    preview = saved.get('preview')
    if (not isinstance(preview, list) or len(preview) != 2
            or type(preview[0]) is not int or preview[0] not in (len(cells), len(cells) + 1)
            or not isinstance(preview[1], list)):
        raise ValueError('Complete independent native Preview samples required')
    seen = set()
    for sample in preview[1]:
        if (not isinstance(sample, list) or len(sample) != 2 or type(sample[0]) is not int
                or not 1 <= sample[0] <= preview[0] or sample[0] in seen
                or not isinstance(sample[1], list)):
            raise ValueError('Invalid independent native Preview sample')
        index, values = sample
        seen.add(index)
        if len(values) == columns + 1 and values[0] in (None, '', 'Top Left Header Cell'):
            values = values[1:]
        if len(values) != columns:
            raise ValueError('Independent native sample width differs')
        if index == len(cells) + 1 and all(v in (None, '') for v in values):
            continue
        if index > len(cells) or any(a != b and not (a in (None, '') and b in (None, ''))
                                     for a, b in zip(cells[index - 1], values, strict=True)):
            raise ValueError('Preserved values differ from independent native sample')
    if seen != {i for i in range(1, preview[0] + 1) if i <= 4 or i >= preview[0] - 2}:
        raise ValueError('Incomplete independent native sample coverage')
    started = datetime.now(UTC)
    after, readback_path, _ = bridge.execute(root, {
        **task, 'request_json': json.dumps({**wire, 'action': 'inspect_query_runtime'})})
    source_operation_clock(after.get('observed_at_utc'), started, datetime.now(UTC))
    readback = _readback(root, task['task_id'], readback_path, after, 2 * 1024 ** 2)
    stable_keys = ('provider', 'action', 'task_id', 'type', 'smart_id', 'table', 'source_key_mode',
                   'owned_desktop_process_start_utc', 'current_field_lists', 'current_query_axes',
                   'date_input_controls', 'preview_button', 'vendor_notices_absent',
                   'source_binding_stable', 'source_selectors_enabled', 'company_group_enabled',
                   'source_binding_unchanged')
    if (any(after.get(k) != before.get(k) for k in stable_keys)
            or any(after.get(k) is not False for k in FALSE_FLAGS) or _grids(after) != grids):
        raise ValueError('Desktop or grid changed after full preservation; no replay')
    return {'contract': CONTRACT, 'capture': capture, 'fresh_readback': readback,
            'inspected_data_grid': data[0], 'preserved_data_grid': captured_grid,
            'configuration_grids': metadata,
            'source_rows': len(cells) - 1, 'original_outcome': 'unknown_retained_not_claimed_unsent',
            'source_rows_adopted': False}

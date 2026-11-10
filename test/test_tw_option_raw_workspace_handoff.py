from contextlib import ExitStack, contextmanager
from datetime import datetime
import fcntl
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

from scripts import prepare_handoff_index_archive as archive
from scripts import run_workspace_source_staging as staging
from stockagent.data_sync import tw_option_raw_workspace_guard as guard
from stockagent.data_sync import workspace_source_handoff as handoff
from stockagent.data_sync.desync_snapshots import SnapshotError


def test_only_exact_option_raw_subtree_is_enrolled():
    items = staging.load_sources()
    item, = [row for row in items if row['name'] == handoff.OPTIONS_RAW_NAME]
    assert staging.stream_scope(item) == staging.OPTIONS_RAW_STREAM_SCOPE
    assert item['catalog_source'] == 'data_tw_index_options_daily'
    assert item['copy_mode'] == 'source_stream_no_readback'
    assert archive.ENROLLED[handoff.OPTIONS_RAW_NAME][0] == 'tw-index-options-raw-workspace'
    assert 'tw-index-options-raw-workspace' in archive.PRIVATE_ONLY_DATASETS


def test_raw_stream_scope_cannot_expand_to_other_sources(tmp_path):
    import json

    name, source, marker = staging.OPTIONS_RAW_STREAM_SCOPE
    item = {'name': name, 'source': source, 'catalog_source': 'data_tw_index_options_daily',
            'marker': marker, 'budget_bytes': 4096, 'seed_hardlinks': False,
            'copy_mode': 'source_stream_no_readback'}
    path = tmp_path/'queue.json'
    for change in ({'name': 'other'}, {'source': 'data_tw_index_options_daily/other'},
                   {'marker': 'annual/other.zip'}, {'catalog_source': 'data_other'},
                   {'seed_hardlinks': True}):
        path.write_text(json.dumps({'schema_version': 1, 'contract': staging.CONTRACT,
                                    'sources': [{**item, **change}]}))
        with pytest.raises(ValueError, match='scope'):
            staging.load_sources(path)


def test_option_raw_writers_cannot_use_unactivated_stage(tmp_path, monkeypatch):
    source, destination = tmp_path/'source', tmp_path/'destination'
    source.mkdir(); destination.mkdir()
    monkeypatch.setattr(guard, 'SOURCE', source)
    monkeypatch.setattr(guard, 'DESTINATION', destination)
    monkeypatch.setattr(handoff, 'registered_names', lambda: ())
    with guard.writer_guard(source/'annual/file'):
        pass
    with pytest.raises(SnapshotError, match='not an active writer workspace'):
        with guard.writer_guard(destination/'annual/file'):
            pass

    monkeypatch.setattr(handoff, 'registered_names', lambda: (handoff.OPTIONS_RAW_NAME,))
    seen = []
    @contextmanager
    def lock(root, timeout):
        seen.append((root, timeout))
        yield
    monkeypatch.setattr(guard, '_writer_guard', lock)
    monkeypatch.setattr(handoff, 'read_journal', lambda: {'state': 'active', 'destination': str(destination)})
    monkeypatch.setattr(handoff.provider_workspace, 'exact_mount', lambda *_args: True)
    with guard.writer_guard(destination/'annual/file'):
        assert seen == [(source, 30.0)]
    monkeypatch.setattr(handoff.provider_workspace, 'exact_mount', lambda *_args: False)
    with pytest.raises(SnapshotError, match='not an active writer workspace'):
        with guard.writer_guard(destination/'annual/file'):
            pass


def test_option_raw_handoff_fences_existing_auxiliary_owner(tmp_path, monkeypatch):
    source, destination = tmp_path/'source', tmp_path/'destination'
    source.mkdir(); destination.mkdir()
    native = tmp_path/'data_tw_index_options_daily/state/registered_auxiliary_daily.lock'
    native.parent.mkdir(parents=True); native.touch()
    monkeypatch.setattr(handoff, 'ROOT', tmp_path)
    monkeypatch.setattr(handoff, '_scope', lambda: SimpleNamespace(name=handoff.OPTIONS_RAW_NAME))
    with native.open('r+') as owner:
        fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with ExitStack() as locks, pytest.raises(BlockingIOError):
            handoff._lock_writer_roots(locks, source, destination)
    with ExitStack() as locks:
        handoff._lock_writer_roots(locks, source, destination)


def test_option_raw_uses_the_original_dataset_publication_lock(tmp_path, monkeypatch):
    from stockagent.data_sync import cold_primary, packed_snapshots

    seen = []
    @contextmanager
    def publication_lock(path):
        seen.append(path)
        yield
    monkeypatch.setattr(handoff, 'COLD_PRIMARY', tmp_path)
    monkeypatch.setattr(cold_primary, '_check_d_primary_mount', lambda _: None)
    monkeypatch.setattr(packed_snapshots, '_try_publication_lock', publication_lock)
    with handoff._market_publication_owner('tw-index-options-daily'):
        assert seen == [tmp_path/'.local-state/locks/publish-tw-index-options-daily.lock']
    with pytest.raises(SnapshotError, match='not enrolled'):
        with handoff._market_publication_owner('another-dataset'):
            pass


@pytest.mark.parametrize('at,allowed', [
    ((2026, 11, 8, 19, 0), True),    # Sunday; next capture is Monday morning.
    ((2026, 11, 9, 8, 25, 1), False),
    ((2026, 11, 9, 9, 0), False),
    ((2026, 11, 9, 14, 0), True),
    ((2026, 11, 9, 14, 46), False),
    ((2026, 11, 10, 2, 0), False),
])
def test_option_raw_maintenance_yields_to_capture_bootstrap(at, allowed):
    now = datetime(*at, tzinfo=ZoneInfo('Asia/Taipei'))
    assert handoff.options_raw_handoff_window_allowed(for_start=True, observed=now) is allowed


def test_option_raw_auto_enrollment_waits_for_fixed_stage(monkeypatch):
    monkeypatch.setattr(handoff, 'registered_names', lambda: ())
    seen = []
    def enroll(name, *, apply):
        seen.append((name, apply))
        return {'state': 'enrolled_for_existing_owner'}
    assert staging.auto_enroll_options_raw([], start_allowed=lambda: True, enroll=enroll)['state'] == 'waiting'
    rows = [{'name': handoff.OPTIONS_RAW_NAME, 'state': 'source_stream_staged'}]
    assert staging.auto_enroll_options_raw(rows, start_allowed=lambda: False, enroll=enroll)['state'] == 'waiting'
    assert staging.auto_enroll_options_raw(rows, start_allowed=lambda: True, enroll=enroll)['state'] == 'enrolled'
    assert seen == [(handoff.OPTIONS_RAW_NAME, True)]

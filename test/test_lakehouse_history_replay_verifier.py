import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

pytest.importorskip('temporalio', reason='use the locked lakehouse control role')
from scripts import verify_lakehouse_workflow_history as verifier


@pytest.mark.parametrize('events', [[], [2], [1, 3], [1, 2, 2], [2, 1]])
def test_history_replay_refuses_dropped_duplicated_or_reordered_events(events):
    with pytest.raises(ValueError, match='every event'):
        verifier.validate_events(SimpleNamespace(events=[SimpleNamespace(event_id=value) for value in events]))


def test_complete_history_order_is_preserved():
    verifier.validate_events(SimpleNamespace(events=[SimpleNamespace(event_id=value) for value in range(1, 12)]))


def test_replay_fetches_pinned_histories_and_never_starts_a_worker(tmp_path, monkeypatch):
    from temporalio.client import Client, WorkflowHistory
    from temporalio.worker import Replayer
    from stockagent.control import storage_workflow as sw
    seen = []
    history = SimpleNamespace(events=[SimpleNamespace(event_id=1)], to_json=lambda: '{"events":[1]}')
    description = SimpleNamespace(status=SimpleNamespace(name='RUNNING'), run_id='fixed-run',
        raw_description=SimpleNamespace(workflow_execution_info=SimpleNamespace(first_run_id='chain')))

    def handle(name, **options):
        seen.append((name, options))
        return SimpleNamespace(describe=AsyncMock(return_value=description), fetch_history=AsyncMock(return_value=history))

    monkeypatch.setattr(Client, 'connect', AsyncMock(return_value=SimpleNamespace(get_workflow_handle=handle)))
    monkeypatch.setattr(WorkflowHistory, 'from_json', lambda name, raw: history)

    class FakeReplayer:
        def __init__(self, **options):
            assert options['workflows'] == [sw.StorageLifecycle, sw.SourceReplicationLifecycle]
            assert options['workflow_task_executor']._max_workers == 2

        async def replay_workflow(self, restored, **options):
            assert restored is history and options == {'raise_on_replay_failure': True}
            with pytest.raises(AssertionError, match='activity'):
                sw.configuration()
            return SimpleNamespace(replay_failure=None)

    monkeypatch.setattr(Replayer, '__new__', lambda *args, **kwargs: FakeReplayer(**kwargs))
    result = asyncio.run(verifier.replay({'temporal_endpoint': 'local', 'temporal_namespace': 'private'}, tmp_path))
    assert len(result) == 2 and all(row['events'] == 1 for row in result)
    assert seen == [(name, options) for name in verifier.IDS for options in ({}, {'run_id': 'fixed-run'})]
    assert all((tmp_path / (name + '.history.json')).read_text() == '{"events":[1]}' for name in verifier.IDS)

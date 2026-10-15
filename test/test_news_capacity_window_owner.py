"""Persisted capacity tuning does not make old throughput this owner's ETA."""
from test_news_capacity import config, controller_fixture, observe


def test_capacity_window_and_counter_snapshot_share_the_canonical_owner(config, tmp_path):
    http, controller, clock = controller_fixture(config, tmp_path)
    http.metrics.owner_id = 'a' * 32
    controller.tick()
    state = observe(http, controller, clock)
    assert state['last_window']['owner_id'] == 'a' * 32
    assert http.metrics.snapshot()['owner_id'] == 'a' * 32
    assert controller.snapshot()['completion_window_owner_contract_version'] == 1
    old_window = state['last_window'].copy()
    http.metrics.owner_id = 'b' * 32
    state = observe(http, controller, clock)
    assert state['last_window']['owner_id'] == 'b' * 32
    assert old_window['owner_id'] == 'a' * 32
    http.close()

"""Paused lanes yield dispatch only, never their quota or pending work."""
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

from downloader import paused_provider_owners as owners
from downloader.finmind_product_query_verification import yield_explicitly_paused_owners
from scripts import download_finlab_market_intraday as finlab


NOW = datetime(2026, 11, 3, 12, tzinfo=UTC)


def _unit(name, *, active='inactive', enabled='disabled'):
    return f'Id={name}\nLoadState=loaded\nActiveState={active}\nUnitFileState={enabled}'


def test_systemd_observation_requires_every_unit_and_no_general_process(monkeypatch):
    names = owners.UNITS['finlab']
    text = '\n\n'.join(_unit(name) for name in names)
    monkeypatch.setattr(owners.subprocess, 'run', lambda *a, **k: SimpleNamespace(stdout=text))
    monkeypatch.setattr(owners, '_normal_owner_running', lambda _: False)
    assert owners.explicitly_paused({'finlab'}) == {'finlab'}
    monkeypatch.setattr(owners, '_normal_owner_running', lambda _: True)
    assert not owners.explicitly_paused({'finlab'})
    monkeypatch.setattr(owners, '_normal_owner_running', lambda _: False)
    for value in (_unit(names[0]), text.replace('ActiveState=inactive', 'ActiveState=active', 1),
                  text.replace('UnitFileState=disabled', 'UnitFileState=enabled', 1), text+'\n\n'+_unit(names[0])):
        monkeypatch.setattr(owners.subprocess, 'run', lambda *a, **k: SimpleNamespace(stdout=value))
        assert not owners.explicitly_paused({'finlab'})


def test_manual_general_owner_is_not_mistaken_for_a_paused_lane(tmp_path):
    proc = tmp_path / '7'
    proc.mkdir()
    cmdline = proc / 'cmdline'
    cmdline.write_bytes(b'python\0-m\0downloader.download_finmind_complement\0--loop\0')
    assert owners._normal_owner_running('complement', tmp_path)
    cmdline.write_bytes(b'python\0-m\0downloader.download_finmind_complement\0--task-key\0a\0')
    assert not owners._normal_owner_running('complement', tmp_path)


def test_finite_finlab_requires_valid_pending_work_and_verified_paused_owner(monkeypatch):
    valid = dict(state='general_work_pending', minute_residual_allowed=False, actionable_pending=9)
    monkeypatch.setattr(owners, 'explicitly_paused', lambda _: frozenset({'finlab'}))
    assert finlab._paused_general_futures_priority(valid)['allowed'] is True
    for change in (dict(state='discovery_unverified'), dict(minute_residual_allowed=True),
                   dict(actionable_pending=0), dict(actionable_pending=True)):
        assert not finlab._paused_general_futures_priority(dict(valid, **change))['allowed']
    monkeypatch.setattr(owners, 'explicitly_paused', lambda _: frozenset())
    assert not finlab._paused_general_futures_priority(valid)['allowed']


def test_finmind_yields_only_proven_paused_due_work_and_keeps_complete_reservation():
    due = (NOW - timedelta(minutes=1)).isoformat()
    later = (NOW + timedelta(minutes=1)).isoformat()
    events = [dict(owner='free', due_at_utc=due, requests=3),
              dict(owner='sponsor', due_at_utc=due, requests=2),
              dict(owner='free', due_at_utc=later, requests=4)]
    plan = dict(observed_at_utc=NOW.isoformat(), ready_requests=5,
                reserve_requests=9, events=events, queue_errors=[])
    changed = yield_explicitly_paused_owners(plan, NOW, frozenset({'free'}))
    assert changed['ready_requests'] == 2
    assert changed['reserve_requests'] == 9
    assert changed['events'] == events and plan['ready_requests'] == 5
    assert changed['paused_owner_ready_yielded'] == 3
    assert changed['quota_reservation_for_paused_owners_preserved'] is True
    for broken in (dict(plan, ready_requests=4), dict(plan, queue_errors=['free']),
                   dict(plan, observed_at_utc=later), dict(plan, reserve_requests=4),
                   dict(plan, events=[dict(owner='free', due_at_utc='bad', requests=3)])):
        assert yield_explicitly_paused_owners(broken, NOW, frozenset({'free'})) is broken
    assert yield_explicitly_paused_owners(plan, NOW, frozenset()) is plan

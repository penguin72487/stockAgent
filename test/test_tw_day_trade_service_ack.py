"""Exact revision observations must not alias periodic owner heartbeats."""
from pathlib import Path

import pytest

from stockagent.live import tw_day_trade_service_sync as sync

ENGINE, BOT = Path("engine"), Path("bot")


def clock(monkeypatch):
    now = [0.]
    monkeypatch.setattr(sync.time, "monotonic", lambda: now[0])
    monkeypatch.setattr(sync.time, "sleep", lambda delay: now.__setitem__(0, now[0] + delay))
    return now


def test_short_observation_breaks_whole_second_alias_without_relaxing_ack(monkeypatch):
    now = clock(monkeypatch)
    def read(path):
        revision = 10 + int(now[0])
        if path == ENGINE:
            return {"engine_run_id": "native", "state_revision": revision}
        # Bot acknowledges each new revision 200 ms after the engine.
        lag = int(now[0] % 1 < .2)
        return {"engine_run_id": "native", "engine_state_revision": revision - lag}
    engine, bot, proof = sync.read_service_ack_pair(ENGINE, BOT, reader=read)
    assert proof["matched"] and proof["stable_engine_pair"]
    assert proof["revision_lag"] == 0 and proof["attempts"] > 1
    assert .2 <= now[0] < 1 and proof["elapsed_ms"] >= 200
    assert engine["state_revision"] == bot["engine_state_revision"] == 10


def test_torn_pair_retries_instead_of_acknowledging_an_obsolete_engine(monkeypatch):
    clock(monkeypatch)
    rows = iter([
        {"engine_run_id": "native", "state_revision": 10},
        {"engine_run_id": "native", "engine_state_revision": 10},
        {"engine_run_id": "native", "state_revision": 11},
        {"engine_run_id": "native", "state_revision": 11},
        {"engine_run_id": "native", "engine_state_revision": 11},
        {"engine_run_id": "native", "state_revision": 11},
    ])
    engine, bot, proof = sync.read_service_ack_pair(ENGINE, BOT, reader=lambda _: next(rows))
    assert proof["matched"] and proof["attempts"] == 2
    assert engine["state_revision"] == bot["engine_state_revision"] == 11


@pytest.mark.parametrize("failure", ["behind", "wrong_run", "missing", "bad_revision", "bool_revision", "io_error"])
def test_true_lag_or_unknown_owner_stays_unmatched(monkeypatch, failure):
    now = clock(monkeypatch)
    def read(path):
        if failure == "io_error":
            raise OSError("receipt not readable")
        if failure == "missing":
            return {}
        if path == ENGINE:
            return {"engine_run_id": "native", "state_revision": 10}
        revision = 9 if failure == "behind" else "10" if failure == "bad_revision" else True if failure == "bool_revision" else 10
        return {"engine_run_id": "other" if failure == "wrong_run" else "native",
                "engine_state_revision": revision}
    _, _, proof = sync.read_service_ack_pair(ENGINE, BOT, reader=read, timeout_seconds=.2)
    assert not proof["matched"] and now[0] == .2
    assert proof["revision_lag"] == (1 if failure == "behind" else 0 if failure == "wrong_run" else None)


@pytest.mark.parametrize("timeout,poll", [(0, .05), (11, .05), (1, 0), (1, 2)])
def test_observation_has_finite_non_busy_bounds(timeout, poll):
    with pytest.raises(ValueError):
        sync.read_service_ack_pair(ENGINE, BOT, timeout_seconds=timeout, poll_seconds=poll)

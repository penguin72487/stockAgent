from argparse import Namespace
from datetime import datetime

import pytest

from scripts import check_tw_day_trade_preopen_readiness as gate


@pytest.mark.parametrize("crosses_session", [False, True])
def test_readiness_clock_is_sampled_after_runtime_receipts(tmp_path, monkeypatch, crosses_session):
    tz = gate.TAIPEI
    times = iter([
        datetime(2026, 10, 28, 9, 0, 0, tzinfo=tz),
        datetime(2026, 10, 28, 9, 0, 7, tzinfo=tz),
        datetime(2026, 10, 29 if crosses_session else 28, 9, 0, 8, tzinfo=tz),
    ])

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return next(times)

    receipt_time = datetime(2026, 10, 28, 9, 0, 6, tzinfo=tz)
    args = Namespace(strict_after="08:57:00", output=tmp_path / "result.json",
                     public_receipt=tmp_path / "public.json", model_receipt=tmp_path / "model.json",
                     simulation_receipt=tmp_path / "simulation.json", event_receipt=tmp_path / "event.json",
                     engine_status=tmp_path / "engine.json", engine_sync=tmp_path / "sync.json",
                     discord_status=tmp_path / "discord.json", execution_latency=None)
    monkeypatch.setattr(gate, "datetime", Clock)
    monkeypatch.setattr(gate, "parse_args", lambda: args)
    monkeypatch.setattr(gate, "_session_contract", lambda now: ("open", "verified", ("market",)))
    monkeypatch.setattr(gate, "_read_json", lambda path: {})
    monkeypatch.setattr(gate, "_service_states", lambda: {})
    monkeypatch.setattr(gate, "_read_synchronized_runtime_receipts", lambda *a: (
        {"published_at": receipt_time.isoformat()}, {"updated_at": receipt_time.isoformat()}))
    results = []

    def evaluate(**kwargs):
        results.append(kwargs)
        return {"ready": True, "strict": True}

    monkeypatch.setattr(gate, "evaluate_readiness", evaluate)
    written = []
    monkeypatch.setattr(gate, "_atomic_json", lambda path, payload: written.append(payload))
    assert gate.main() == (1 if crosses_session else 0)
    if crosses_session:
        assert not results
        assert written[0]["failures"] == ["session date changed while sampling readiness receipts"]
    else:
        assert results[0]["observed"] == datetime(2026, 10, 28, 9, 0, 8, tzinfo=tz)
        assert results[0]["observed"] > receipt_time

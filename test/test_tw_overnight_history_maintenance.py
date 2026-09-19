from __future__ import annotations

from scripts.rebuild_tw_overnight_history import _history_lineage


def _market(config: str, checkpoint: str = "c" * 64) -> dict[str, str]:
    return {
        "market": "tw_overnight_fixture",
        "checkpoint_sha256": checkpoint,
        "market_config_sha256": config,
    }


def test_history_lineage_is_stable_for_new_sessions_but_changes_with_model_contract() -> None:
    first = _history_lineage("2026-02-25", [_market("a" * 64)])
    same = _history_lineage("2026-02-25", [_market("a" * 64)])
    changed = _history_lineage("2026-02-25", [_market("b" * 64)])

    assert first == same
    assert first["fingerprint_sha256"] != changed["fingerprint_sha256"]
    assert "end_date" not in first


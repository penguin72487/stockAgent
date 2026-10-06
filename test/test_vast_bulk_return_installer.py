"""A retry timer repair must preserve every other installed owner setting."""
from pathlib import Path

import pytest

from scripts.install_vast_bulk_return import upgrade_body


TIMER = (Path(__file__).resolve().parents[1] / "deploy/systemd/stockagent-vast-bulk-return.timer.in").read_bytes()


@pytest.mark.parametrize("final_blank", [b"", b"\n", b"\n\n"])
def test_known_predecessor_only_gets_restart_anchor(final_blank):
    previous = TIMER.replace(b"OnActiveSec=30s\n", b"").rstrip(b"\n") + b"\n" + final_blank
    result, upgraded = upgrade_body("timer", previous, TIMER, upgrade=True)
    assert upgraded is True
    assert result.replace(b"OnActiveSec=30s\n", b"") == previous


@pytest.mark.parametrize("kind,enabled,mutation", [
    ("service", True, b""),
    ("timer", False, b""),
    ("timer", True, b"\n# locally maintained override\n"),
])
def test_unknown_or_unauthorized_unit_is_preserved(kind, enabled, mutation):
    previous = TIMER.replace(b"OnActiveSec=30s\n", b"") + mutation
    with pytest.raises(ValueError, match="preserve"):
        upgrade_body(kind, previous, TIMER, upgrade=enabled)


def test_existing_restart_anchor_is_idempotent():
    assert upgrade_body("timer", TIMER, TIMER, upgrade=True) == (TIMER, False)
    assert upgrade_body("timer", TIMER + b"\n", TIMER, upgrade=True) == (TIMER + b"\n", False)

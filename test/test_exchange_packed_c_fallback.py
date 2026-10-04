from __future__ import annotations

from pathlib import Path

import pytest

import scripts.exchange_packed_c_fallback as exchange
from stockagent.data_sync.desync_snapshots import SnapshotError


def _fixture(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> tuple[Path, Path, Path]:
    canonical = tmp_path / "canonical"
    prepared = tmp_path / "prepared"
    preserved = tmp_path / "preserved"
    (canonical / ".local-state").mkdir(parents=True)
    (canonical / ".local-state/node-id").write_text("penguin\n")
    (canonical / "unique-object").write_bytes(b"keep")
    prepared.mkdir()
    (prepared / exchange.SENTINEL).write_text("D required\n")
    monkeypatch.setattr(exchange, "CANONICAL", canonical)
    monkeypatch.setattr(exchange, "PREPARED", prepared)
    monkeypatch.setattr(exchange, "PRESERVED", preserved)
    monkeypatch.setattr(exchange, "RECEIPT", tmp_path / "receipt.json")
    return canonical, prepared, preserved


def test_atomic_exchange_preserves_c_and_blocks_unmounted_fallback(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    canonical, prepared, preserved = _fixture(tmp_path, monkeypatch)
    result = exchange.run()
    assert result["state"] == "c_preserved_fallback_ready"
    assert exchange._only_sentinel(canonical)
    assert (preserved / "unique-object").read_bytes() == b"keep"
    assert not prepared.exists()


def test_exchange_rejects_nonempty_prepared_fallback(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    canonical, prepared, preserved = _fixture(tmp_path, monkeypatch)
    (prepared / "unexpected").write_text("unsafe")
    with pytest.raises(SnapshotError, match="only the mount-required sentinel"):
        exchange.run()
    assert (canonical / "unique-object").exists()
    assert not preserved.exists()

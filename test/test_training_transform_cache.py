from __future__ import annotations

import json
from types import SimpleNamespace

import numpy as np
import pytest

from stockagent.training.trainer import (
    _load_training_transform_cache,
    _store_training_transform_cache,
    _training_transform_cache_key,
    _training_transform_cache_path,
)


def _fixture_panel():
    features = np.arange(24, dtype=np.float32).reshape(3, 4, 2)
    alive = np.ones((3, 4), dtype=np.bool_)
    panel = SimpleNamespace(
        features=features,
        alive_mask=alive,
        content_fingerprints=None,
    )
    dataset = SimpleNamespace(valid_indices=np.asarray([1, 2], dtype=np.int64))
    return panel, dataset


def test_training_transform_cache_is_atomic_verified_and_content_addressed(
    tmp_path, monkeypatch
):
    monkeypatch.setenv(
        "STOCKAGENT_TRAINING_TRANSFORM_CACHE_DIR", str(tmp_path / "cache")
    )
    panel, dataset = _fixture_panel()
    output = tmp_path / "artifacts" / "markets" / "run"
    output.mkdir(parents=True)
    key = _training_transform_cache_key(
        kind="unit",
        panel=panel,
        train_ds=dataset,
        contract={"version": 1},
        include_alive_mask=True,
    )
    payload = {"values": [1.0, 2.0], "metadata": {"accepted": True}}

    assert _load_training_transform_cache(
        output, kind="unit", cache_key=key
    ) is None
    _store_training_transform_cache(
        output, kind="unit", cache_key=key, payload=payload
    )
    assert _load_training_transform_cache(
        output, kind="unit", cache_key=key
    ) == payload

    changed_key = dict(key)
    changed_key["contract"] = {"version": 2}
    assert _load_training_transform_cache(
        output, kind="unit", cache_key=changed_key
    ) is None

    path = _training_transform_cache_path(output, kind="unit", cache_key=key)
    envelope = json.loads(path.read_text(encoding="utf-8"))
    envelope["payload"]["values"][0] = 99.0
    path.write_text(json.dumps(envelope), encoding="utf-8")
    with pytest.warns(UserWarning, match="mismatched training transform cache"):
        assert _load_training_transform_cache(
            output, kind="unit", cache_key=key
        ) is None
    assert list(path.parent.glob("*.tmp")) == []


def test_training_transform_cache_key_invalidates_feature_and_alive_changes():
    panel, dataset = _fixture_panel()
    original = _training_transform_cache_key(
        kind="unit",
        panel=panel,
        train_ds=dataset,
        contract={"version": 1},
        include_alive_mask=True,
    )
    panel.features = panel.features.copy()
    panel.features[0, 0, 0] += 1.0
    feature_changed = _training_transform_cache_key(
        kind="unit",
        panel=panel,
        train_ds=dataset,
        contract={"version": 1},
        include_alive_mask=True,
    )
    assert feature_changed != original

    panel.features[0, 0, 0] -= 1.0
    panel.alive_mask = panel.alive_mask.copy()
    panel.alive_mask[0, 0] = False
    alive_changed = _training_transform_cache_key(
        kind="unit",
        panel=panel,
        train_ds=dataset,
        contract={"version": 1},
        include_alive_mask=True,
    )
    assert alive_changed != original

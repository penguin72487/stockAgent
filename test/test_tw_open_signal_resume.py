import json

import pytest

from scripts.backfill_tw_day_trade_open_signals import _resume_summary, _sha256, _snapshot_sources


def test_resume_requires_exact_source_recipe_and_output_bytes(tmp_path):
    path = tmp_path / "summary.json"
    identity, provenance = {"checkpoint_sha256": "checkpoint"}, {"input_sha256": "csv"}
    assert _resume_summary(path, identity=identity, signal_id="id", provenance=provenance) is None
    weights = tmp_path / "target_weights.parquet"
    weights.write_bytes(b"complete-output")
    summary = {"signal_id": "id", "simulation_only": True, "production_order_possible": False,
               "counterfactual_signal_regeneration": True, "generated_at": "actual-clock",
               "counterfactual_open_provenance": provenance,
               "counterfactual_artifact_proofs": {"recipe_identity": identity,
                                                  "weights_sha256": _sha256(weights)}}
    path.write_text(json.dumps(summary))
    assert _resume_summary(path, identity=identity, signal_id="id", provenance=provenance) == summary
    for changed in ({"checkpoint_sha256": "different"}, {"checkpoint_sha256": "checkpoint", "extra": "different"}):
        with pytest.raises(ValueError, match="exact artifact proof"):
            _resume_summary(path, identity=changed, signal_id="id", provenance=provenance)
    weights.write_bytes(b"truncated")
    with pytest.raises(ValueError, match="exact artifact proof"):
        _resume_summary(path, identity=identity, signal_id="id", provenance=provenance)


def test_replay_source_copy_is_byte_identical_and_not_a_moving_producer(tmp_path):
    source = tmp_path / "live.parquet"
    source.write_bytes(b"accepted-generation")
    retained = _snapshot_sources(tmp_path / "snapshot", {"features": source})["features"]
    assert retained.read_bytes() == source.read_bytes()
    source.write_bytes(b"later-generation")
    assert retained.read_bytes() == b"accepted-generation"
    receipt = json.loads((tmp_path / "snapshot/source_snapshot_receipt.json").read_text())
    assert receipt["sources"]["features"]["sha256"] == _sha256(retained)
    assert receipt["source_publication_claim"] is False
    retained.write_bytes(b"corruption")
    source.write_bytes(b"accepted-generation")
    with pytest.raises(ValueError, match="retained replay source differs"):
        _snapshot_sources(tmp_path / "snapshot", {"features": source})


@pytest.mark.parametrize("field,value", [("simulation_only", False), ("production_order_possible", True),
                                        ("counterfactual_signal_regeneration", False), ("signal_id", "other")])
def test_resume_rejects_changed_execution_claims(tmp_path, field, value):
    weights, path = tmp_path / "target_weights.parquet", tmp_path / "summary.json"
    weights.write_bytes(b"weights")
    identity, provenance = {"checkpoint_sha256": "a"}, {"input_sha256": "b"}
    summary = {"signal_id": "id", "simulation_only": True, "production_order_possible": False,
               "counterfactual_signal_regeneration": True, "counterfactual_open_provenance": provenance,
               "counterfactual_artifact_proofs": {"recipe_identity": identity, "weights_sha256": _sha256(weights)}}
    summary[field] = value
    path.write_text(json.dumps(summary))
    with pytest.raises(ValueError, match="exact artifact proof"):
        _resume_summary(path, identity=identity, signal_id="id", provenance=provenance)

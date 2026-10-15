from copy import deepcopy

import pytest

from pathlib import Path
import json

from scripts.benchmark_factorized_live_owner import (
    ROOT, canonical_cache_path, canonical_output_contract, output_contract, shared_identity,
)
from scripts.prepare_tw_day_trade_factorized_live import _native_context_identity
from scripts.prepare_tw_day_trade_factorized_panel import annual_preparation_identity
from stockagent.data.factorized_panel import file_sha256


def manifest():
    return {"dates": ["2026-10-13"], "symbols": ["2330"],
            "build_wall_seconds": 480., "feature_lag": 1,
            "individual_channels": ["cash"], "historical_point_in_time": False,
            "research_only": True, "blocks": [{"sha256": "frozen-original"}],
            "paper_inference": {"built_at_utc": "current-generation", "completed_session": "2026-10-13",
                                "decision_session": "2026-10-14", "delta_observations": 3,
                                "historical_pit_proven": False,
                                "source_identity": {"sweep_sha": "original-code", "builder_sha": "unchanged-owner",
                                                    "public_sha": "original-public", "finlab": {"cash": "original-cash"}}}}


def test_whole_contract_ignores_only_declared_timing_and_candidate_code():
    original = manifest()
    before = deepcopy(original)
    candidate = deepcopy(original)
    candidate["build_wall_seconds"] = 200.
    candidate["paper_inference"]["built_at_utc"] = "candidate-time"
    candidate["paper_inference"]["source_identity"]["sweep_sha"] = "candidate-code"
    assert output_contract(candidate) == output_contract(original)
    assert original == before


@pytest.mark.parametrize("changed", ["clock", "date", "universe", "value_bits", "feature", "lag", "pit",
                                   "source", "normalization", "builder", "permission", "finlab"])
def test_whole_source_and_output_changes_cannot_be_hidden(changed):
    original = manifest()
    candidate = deepcopy(original)
    if changed == "clock":
        candidate["paper_inference"]["decision_session"] = "2026-10-15"
    elif changed == "date":
        candidate["dates"] = ["2026-10-12"]
    elif changed == "universe":
        candidate["symbols"] = ["2317"]
    elif changed == "value_bits":
        candidate["blocks"][0]["sha256"] = "quantized"
    elif changed == "feature":
        candidate["individual_channels"] = ["ratio"]
    elif changed == "lag":
        candidate["feature_lag"] = 0
    elif changed == "pit":
        candidate["historical_point_in_time"] = True
    elif changed == "source":
        candidate["paper_inference"]["source_identity"]["public_sha"] = "different-data"
    elif changed == "normalization":
        candidate["paper_inference"]["delta_observations"] = 2
    elif changed == "builder":
        candidate["paper_inference"]["source_identity"]["builder_sha"] = "other-owner"
    elif changed == "permission":
        candidate["research_only"] = False
    else:
        candidate["paper_inference"]["source_identity"]["finlab"]["cash"] = "other-source"
    assert output_contract(candidate) != output_contract(original)


def test_missing_paper_contract_is_not_accepted():
    with pytest.raises(KeyError):
        output_contract({})


def test_shared_dependency_set_is_real_and_includes_full_base_and_clocks():
    codes = shared_identity()
    assert any(name.endswith("stockagent/data/panel.py") for name in codes)
    assert any(name.endswith("scripts/build_tw_release_schedule_dataset.py") for name in codes)
    assert any(name.endswith("stockagent/live/market_status.py") for name in codes)
    assert len(set(codes.values())) == len(codes)
    assert all(len(digest) == 64 for digest in codes.values())


def canonical_manifest(tmp_path):
    (tmp_path / "original_owner.py").write_text("independent original owner")
    (tmp_path / "config.json").write_text("{}")
    native = tmp_path / "native_source"
    native.mkdir()
    (native / "source_manifest.json").write_text(json.dumps({}))
    original = manifest()
    original["paper_inference"]["source_update_owner"] = str(tmp_path / "original_owner.py")
    original["paper_inference"]["source_identity"]["builder_sha"] = file_sha256(tmp_path / "original_owner.py")
    candidate = deepcopy(original)
    identity = candidate["paper_inference"]["source_identity"]
    candidate["paper_inference"]["source_update_owner"] = str(ROOT / "scripts/prepare_tw_day_trade_factorized_live.py")
    identity.update({
        "builder_sha": file_sha256(ROOT / "scripts/prepare_tw_day_trade_factorized_live.py"),
        "sweep_sha": file_sha256(ROOT / "scripts/prepare_tw_day_trade_factorized_panel.py"),
        "annual_preparation": annual_preparation_identity(),
        "writer_sha": file_sha256(ROOT / "stockagent/data/factorized_panel.py"),
        "config_sha": file_sha256(tmp_path / "config.json"),
        "native_context": _native_context_identity(native, {}),
    })
    return original, candidate


def test_canonical_metadata_is_verified_before_normalizing(tmp_path):
    original, candidate = canonical_manifest(tmp_path)
    before = deepcopy(candidate)
    assert canonical_output_contract(candidate, tmp_path) == output_contract(original)
    assert candidate == before


@pytest.mark.parametrize("key", ["builder_sha", "sweep_sha", "writer_sha", "annual_preparation",
                                 "config_sha", "native_context", "source_update_owner"])
def test_canonical_normalization_cannot_hide_unverified_fingerprints(tmp_path, key):
    _original, candidate = canonical_manifest(tmp_path)
    if key == "source_update_owner":
        candidate["paper_inference"][key] = "unrelated-owner"
    else:
        candidate["paper_inference"]["source_identity"][key] = "wrong-fingerprint"
    with pytest.raises(ValueError, match="fingerprints differ"):
        canonical_output_contract(candidate, tmp_path)


def test_canonical_oracle_still_rejects_data_and_clock_changes(tmp_path):
    original, candidate = canonical_manifest(tmp_path)
    candidate["paper_inference"]["source_identity"]["public_sha"] = "different-source"
    assert canonical_output_contract(candidate, tmp_path) != output_contract(original)
    candidate["paper_inference"]["source_identity"]["public_sha"] = "original-public"
    candidate["blocks"][0]["sha256"] = "different-value-bits"
    assert canonical_output_contract(candidate, tmp_path) != output_contract(original)


def test_only_exact_declared_canonical_cache_destination_is_allowed():
    assert canonical_cache_path(ROOT / "artifacts/cache/tw_factorized_live/annual") == ROOT / "artifacts/cache/tw_factorized_live/annual"
    with pytest.raises(ValueError, match="derived-cache"):
        canonical_cache_path(Path("/tmp/not-the-canonical-cache"))

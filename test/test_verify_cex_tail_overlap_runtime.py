from __future__ import annotations

from types import SimpleNamespace

import pytest

from downloader.artifact_io import atomic_write_json
from scripts import verify_cex_tail_overlap_runtime as verifier


def profile_body(*, correct=True):
    return {"shared": {"frames": [{"name": "tail_newer_summary",
                                  "file": str(verifier.ROOT / "downloader/ohlcv_hot_tail.py") if correct else "/other/file.py"}]},
            "profiles": [{"samples": [[0], [], [0]]}]}


def test_native_frame_count_requires_exact_helper_and_source_file():
    assert verifier.helper_samples(profile_body()) == 2
    assert verifier.helper_samples(profile_body(correct=False)) == 0
    assert verifier.helper_samples({}) == 0


@pytest.mark.parametrize("provider", ["bybit", "okx", "binance"])
def test_static_projection_check_does_not_accept_full_or_different_column_reads(provider):
    source = "def f(path):\n    return tail_newer_summary(_read_overlap_dates(tail_path), base_latest_ms=1, normalize=f)\n"
    verifier.overlap_ast(source, provider=provider, prototype_input=True)
    wrong = source.replace("_read_overlap_dates(tail_path)", "read_logical_parquet(tail_path)")
    with pytest.raises(ValueError, match="date projection"):
        verifier.overlap_ast(wrong, provider=provider, prototype_input=True)


def fixture(tmp_path, monkeypatch, *, owners):
    monkeypatch.setattr(verifier, "ROOT", tmp_path)
    code = tmp_path / "code.py"
    code.write_text("test fixture")
    output = tmp_path / "artifacts/operations/receipt.json"
    admission = {"code_sha256": {str(code): "test-only"}, "live_matches_complete_prototype": True}
    monkeypatch.setattr(verifier, "validate_code", lambda _: admission)
    unit = {"ActiveState": "activating", "MainPID": "10", "ControlGroup": "/test", "InvocationID": "a"}
    monkeypatch.setattr(verifier, "unit_identity", lambda _: unit.copy())
    monkeypatch.setattr(verifier, "native_owners", lambda *args: owners.copy())
    return output


def test_absent_live_owners_cannot_promote_code_or_receipt_to_native_acceptance(tmp_path, monkeypatch):
    output = fixture(tmp_path, monkeypatch, owners={})
    monkeypatch.setattr(verifier.subprocess, "run", lambda *args, **kwargs: pytest.fail("no owner must not be sampled"))
    result = verifier.verify(tmp_path / "unused-comparison", output, wait_seconds=0)
    assert result["state"] == "awaiting_native_helper_execution"
    assert not result["native_lifecycle_complete_claim"]
    assert not result["all_history_complete_claim"]
    assert result["provider_calls"] == result["source_writes"] == 0


@pytest.mark.parametrize("helper_observed", [True, False])
def test_three_stable_real_frames_are_required_not_a_code_label(tmp_path, monkeypatch, helper_observed):
    owners = {provider: {"pid": 10 + i, "start_ticks": str(i)} for i, provider in enumerate(verifier.MODULES)}
    output = fixture(tmp_path, monkeypatch, owners=owners)

    def sample(command, **kwargs):
        atomic_write_json(command[-1], profile_body(correct=helper_observed), durable=True)
        return SimpleNamespace(returncode=0, stdout="sampled", stderr="")

    monkeypatch.setattr(verifier.subprocess, "run", sample)
    result = verifier.verify(tmp_path / "unused-comparison", output, wait_seconds=0)
    assert result["state"] == ("accepted_native_helper_execution" if helper_observed else "awaiting_native_helper_execution")
    assert result["same_native_owners"]
    assert not result["native_lifecycle_complete_claim"]
    assert not result["services_restarted"]


def test_owner_generation_change_is_inconclusive_not_success(tmp_path, monkeypatch):
    owners = {provider: {"pid": 10 + i, "start_ticks": str(i)} for i, provider in enumerate(verifier.MODULES)}
    output = fixture(tmp_path, monkeypatch, owners=owners)
    calls = 0

    def current(*args):
        nonlocal calls
        calls += 1
        return owners if calls == 1 else {}

    monkeypatch.setattr(verifier, "native_owners", current)

    def sample(command, **kwargs):
        atomic_write_json(command[-1], profile_body(), durable=True)
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(verifier.subprocess, "run", sample)
    result = verifier.verify(tmp_path / "unused", output, wait_seconds=0)
    assert result["state"] == "awaiting_native_helper_execution"
    assert result["same_native_owners"] is False


def test_fresh_output_guard_prevents_overwriting_evidence(tmp_path, monkeypatch):
    output = fixture(tmp_path, monkeypatch, owners={})
    output.parent.mkdir(parents=True)
    output.write_text("original evidence")
    with pytest.raises(ValueError, match="fresh private"):
        verifier.verify(tmp_path / "unused", output, wait_seconds=0)
    assert output.read_text() == "original evidence"


def test_canonical_normalization_admission_samples_only_changed_providers(tmp_path, monkeypatch):
    owners = {provider: {"pid": 10 + i, "start_ticks": str(i)} for i, provider in enumerate(verifier.MODULES)}
    output = fixture(tmp_path, monkeypatch, owners=owners)
    monkeypatch.setattr(verifier, "validate_normalization_code", verifier.validate_code)
    calls = 0

    def current(*args):
        nonlocal calls
        calls += 1
        return owners if calls == 1 else {p: owners[p] for p in ("bybit", "okx")}

    monkeypatch.setattr(verifier, "native_owners", current)

    def sample(command, **kwargs):
        body = profile_body()
        body["shared"]["frames"][0]["name"] = "canonical_date_frame"
        atomic_write_json(command[-1], body, durable=True)
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(verifier.subprocess, "run", sample)
    result = verifier.verify(tmp_path / "unused", output, wait_seconds=0, normalization=True)
    assert result["state"] == "accepted_native_helper_execution"
    assert result["sampled_helper"] == "canonical_date_frame"
    assert result["required_providers"] == ["bybit", "okx"]
    assert {row["provider"] for row in result["rows"]} == {"bybit", "okx"}
    assert not result["services_restarted"]


def test_previous_overlap_frames_do_not_prove_new_normalizer_execution(tmp_path, monkeypatch):
    owners = {p: {"pid": 10 + i, "start_ticks": str(i)} for i, p in enumerate(("bybit", "okx"))}
    output = fixture(tmp_path, monkeypatch, owners=owners)
    monkeypatch.setattr(verifier, "validate_normalization_code", verifier.validate_code)

    def sample(command, **kwargs):
        atomic_write_json(command[-1], profile_body(), durable=True)
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(verifier.subprocess, "run", sample)
    result = verifier.verify(tmp_path / "unused", output, wait_seconds=0, normalization=True)
    assert result["state"] == "awaiting_native_helper_execution"
    assert all(row["helper_samples"] == 0 for row in result["rows"])


def test_normalizer_admission_rejects_wrong_or_incomplete_benchmark(tmp_path):
    receipt = tmp_path / "not-complete.json"
    atomic_write_json(receipt, {"contract": "complete_cex_stored_tail_merge_normalization_v3",
                                "state": "accepted_output_parity", "workflow_speedup": 10})
    with pytest.raises(ValueError, match="complete accepted"):
        verifier.validate_normalization_code(receipt)

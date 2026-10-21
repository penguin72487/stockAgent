"""Immutable producer reuse retains the original full Float32 column codec."""
import os
import json
from pathlib import Path
import subprocess
import sys
import uuid

import numpy as np
import pytest

from downloader.artifact_io import atomic_write_json
from stockagent.data.factorized_panel import VALUE_ONLY_CONTRACT, _BlockCache, write_array_block
from stockagent.data_sync import factorized_projection_reuse as reuse_module
from stockagent.data_sync.factorized_projection_reuse import FactorizedProjectionReuse, seal_new_file


def source_values():
    values = np.asarray([[[0., -0., 11.], [2., -3., 19.]],
                         [[1., 0., -17.], [4., -5., 0.]]], dtype=np.float32)
    return values, np.asarray([0, 3, 6], dtype=np.uint32), (2, 2, 8)


def seed(tmp_path, *, sealed=True):
    root = tmp_path / "generations" / "seed"
    root.mkdir(parents=True)
    producer = FactorizedProjectionReuse(root)
    values, columns, shape = source_values()
    proof = producer.block("part.zst", values, logical_columns=columns, logical_shape=shape)
    common = producer.common(np.asarray([[1., -0.], [np.nan, -2.]], dtype=np.float32))
    manifest = {"contract": VALUE_ONLY_CONTRACT, "paper_inference": {"status": "verified"},
                "storage_build": producer.stats(), "common": common,
                "blocks": [{"symbol_blocks": [{**proof, "symbol_start": 0}]}]}
    path = root / "factorized_manifest.json"
    atomic_write_json(path, manifest)
    if sealed:
        seal_new_file(path)
    return root, path, manifest


def new_producer(tmp_path, donor):
    path = tmp_path / "generations" / "next"
    path.mkdir()
    return FactorizedProjectionReuse(path, donor)


def test_same_values_skip_codec_and_share_only_sealed_members(tmp_path, monkeypatch):
    root, donor, manifest = seed(tmp_path)
    producer = new_producer(tmp_path, donor)
    values, columns, shape = source_values()
    # The new producer already captured the real codec identity. A hit must
    # not call it; negative zero must remain a different raw input bit.
    with monkeypatch.context() as patch:
        patch.setattr(reuse_module, "write_array_block", lambda *a, **k: pytest.fail("unchanged payload recompressed"))
        proof = producer.block("same.zst", values, logical_columns=columns, logical_shape=shape)
    assert (root / "part.zst").stat().st_ino == (producer.staging / "same.zst").stat().st_ino
    assert proof["sha256"] == manifest["blocks"][0]["symbol_blocks"][0]["sha256"]
    assert "symbol_start" not in proof
    result = _BlockCache(producer.staging, [proof], 1024**2).get(0)
    assert result.columns.tolist() == columns.tolist()
    assert result.values.tobytes() == values.tobytes()
    assert producer.stats()["reused_payloads"] == 1
    assert producer.stats()["written_payload_bytes"] == 0


@pytest.mark.parametrize("change", ["value", "negative_zero", "coordinates", "shape"])
def test_any_input_bit_or_coordinate_change_writes_a_new_original_payload(tmp_path, change):
    root, donor, _ = seed(tmp_path)
    original = (root / "part.zst").read_bytes()
    producer = new_producer(tmp_path, donor)
    values, columns, shape = source_values()
    if change == "value":
        values[0, 1, 0] = -17.
    elif change == "negative_zero":
        values[0, 0, 1] = 0.
    elif change == "coordinates":
        columns = np.asarray([1, 3, 6], dtype=np.uint32)
    else:
        shape = (2, 2, 9)
    proof = producer.block("changed.zst", values, logical_columns=columns, logical_shape=shape)
    control = write_array_block(tmp_path / "control.zst", values, omit_zero_columns=True,
                                logical_columns=columns, logical_shape=shape)
    assert proof["sha256"] == control["sha256"]
    assert (producer.staging / proof["path"]).read_bytes() == (tmp_path / "control.zst").read_bytes()
    assert producer.stats()["written_payloads"] == 1 and producer.stats()["reused_payloads"] == 0
    assert (root / "part.zst").read_bytes() == original


@pytest.mark.parametrize("what", ["manifest", "payload", "corrupt_payload"])
def test_writable_or_corrupt_donors_are_not_shared_or_modified(tmp_path, what):
    root, donor, _ = seed(tmp_path, sealed=what != "manifest")
    path = root / "part.zst"
    if what in {"payload", "corrupt_payload"}:
        path.chmod(0o644)
        if what == "corrupt_payload":
            path.write_bytes(b"damaged derived file")
            path.chmod(0o444)
    before = (path.read_bytes(), path.stat().st_mode)
    producer = new_producer(tmp_path, donor)
    values, columns, shape = source_values()
    producer.block("new.zst", values, logical_columns=columns, logical_shape=shape)
    assert producer.stats()["written_payloads"] == 1
    assert path.stat().st_ino != (producer.staging / "new.zst").stat().st_ino
    assert (path.read_bytes(), path.stat().st_mode) == before


def test_common_is_reused_and_mutable_dictionary_is_copied_once(tmp_path):
    root, donor, _ = seed(tmp_path)
    producer = new_producer(tmp_path, donor)
    producer.common(np.asarray([[1., -0.], [np.nan, -2.]], dtype=np.float32))
    assert (root / "common.npy").stat().st_ino == (producer.staging / "common.npy").stat().st_ino
    source = tmp_path / "dictionary.json"
    source.write_text('{"original":"dictionary"}')
    mode = source.stat().st_mode
    producer.dictionary(source, reuse_module.file_sha256(source))
    target = producer.staging / "feature_dictionary.json"
    assert target.read_bytes() == source.read_bytes()
    assert target.stat().st_ino != source.stat().st_ino
    assert not target.stat().st_mode & 0o222 and source.stat().st_mode == mode


def test_new_members_within_same_generation_also_reuse(tmp_path):
    stage = tmp_path / "stage"
    stage.mkdir()
    producer = FactorizedProjectionReuse(stage)
    values, columns, shape = source_values()
    producer.block("one.zst", values, logical_columns=columns, logical_shape=shape)
    producer.block("two.zst", values, logical_columns=columns, logical_shape=shape)
    assert (stage / "one.zst").stat().st_ino == (stage / "two.zst").stat().st_ino
    assert producer.stats()["written_payloads"] == producer.stats()["reused_payloads"] == 1


def test_sealing_refuses_old_shared_inodes_and_redirected_donors(tmp_path):
    root, donor, _ = seed(tmp_path)
    os.link(root / "part.zst", root / "alias.zst")
    with pytest.raises(ValueError, match="single-link"):
        seal_new_file(root / "alias.zst")
    elsewhere = tmp_path / "other"
    elsewhere.mkdir()
    with pytest.raises(ValueError, match="generation owner"):
        FactorizedProjectionReuse(elsewhere, donor)


def test_bad_coordinates_never_bypass_original_codec_admission(tmp_path):
    _, donor, _ = seed(tmp_path)
    producer = new_producer(tmp_path, donor)
    values, _, shape = source_values()
    with pytest.raises(ValueError, match="coordinates"):
        producer.block("bad.zst", values, logical_columns=np.asarray([0, 0, 6]), logical_shape=shape)
    assert not (producer.staging / "bad.zst").exists()


def test_changed_storage_code_cannot_be_published_as_same_implementation(tmp_path, monkeypatch):
    stage = tmp_path / "stage"
    stage.mkdir()
    producer = FactorizedProjectionReuse(stage)
    monkeypatch.setattr(reuse_module, "implementation_identity", lambda: {"different": "code"})
    with pytest.raises(ValueError, match="implementation changed"):
        producer.stats()


@pytest.mark.parametrize("value", [np.nan, np.inf, -np.inf])
def test_storage_reuse_does_not_bypass_finite_model_input_admission(tmp_path, value):
    root, donor, _ = seed(tmp_path)
    producer = new_producer(tmp_path, donor)
    values, columns, shape = source_values()
    values[0, 0, 2] = value
    proof = producer.block("nonfinite.zst", values, logical_columns=columns, logical_shape=shape)
    assert proof["sha256"] != reuse_module.file_sha256(root / "part.zst")
    with pytest.raises(ValueError, match="must be finite"):
        _BlockCache(producer.staging, [proof], 1024**2).get(0)


def test_writable_original_dictionary_does_not_get_copied_on_every_generation(tmp_path):
    root, donor, manifest = seed(tmp_path, sealed=False)
    original = tmp_path / "original.json"
    original.write_text('{"unchanged":"trained dictionary"}')
    digest = reuse_module.file_sha256(original)
    first = FactorizedProjectionReuse(root)
    first.dictionary(original, digest)
    manifest["feature_dictionary_sha256"] = digest
    atomic_write_json(donor, manifest)
    seal_new_file(donor)
    before = reuse_module.payload_info(original)
    second = new_producer(tmp_path, donor)
    second.dictionary(original, digest)
    assert reuse_module.payload_info(original) == before
    assert (root / "feature_dictionary.json").stat().st_ino == (second.staging / "feature_dictionary.json").stat().st_ino
    assert second.stats()["written_dictionary_bytes"] == 0
    assert second.stats()["reused_dictionary_bytes"] == original.stat().st_size


@pytest.mark.parametrize("damage", ["bad_json", "bad_key", "missing_blocks", "escaped_path"])
def test_malformed_optional_donor_misses_without_poisoning_original_producer(tmp_path, damage):
    root, donor, manifest = seed(tmp_path, sealed=False)
    if damage == "bad_key":
        manifest["common"]["producer_input_sha256"] = "not a fingerprint"
    elif damage == "missing_blocks":
        del manifest["blocks"]
    elif damage == "escaped_path":
        manifest["common"]["path"] = "../outside.npy"
    donor.write_text("invalid JSON" if damage == "bad_json" else json.dumps(manifest))
    seal_new_file(donor)
    producer = new_producer(tmp_path, donor)
    values, columns, shape = source_values()
    proof = producer.block("rebuilt.zst", values, logical_columns=columns, logical_shape=shape)
    assert producer.stats()["reused_payloads"] == 0
    assert producer.stats()["rejected_donors"] == 1
    assert proof["sha256"] == reuse_module.file_sha256(root / "part.zst")


def test_bounded_benchmark_compares_initial_and_changed_update_with_full_readback(tmp_path):
    from scripts.benchmark_factorized_projection_reuse import persist
    fixture, _, manifest = seed(tmp_path)
    values, columns, shape = source_values()
    values[1, 1, 2] = -61.
    changed = write_array_block(fixture / "changed.zst", values, omit_zero_columns=True,
                                logical_columns=columns, logical_shape=shape)
    original = manifest["blocks"][0]["symbol_blocks"][0]
    old = {"common": manifest["common"], "blocks": [original, original]}
    new = {**old, "blocks": [original, changed]}
    baseline = tmp_path / "baseline/generations"
    candidate = tmp_path / "candidate/generations"
    first_a = persist(fixture, baseline / "initial", old, "baseline")
    first_b = persist(fixture, candidate / "initial", old, "reuse")
    final_a = persist(fixture, baseline / "update", new, "baseline")
    final_b = persist(fixture, candidate / "update", new, "reuse", candidate / "initial/bounded_manifest.json")
    assert first_a["complete_arrays"] == first_b["complete_arrays"]
    assert final_a["complete_arrays"] == final_b["complete_arrays"]
    assert final_b["storage_build"]["reused_payloads"] == 2  # One block plus common.npy.
    assert final_b["storage_build"]["written_payload_bytes"] < final_a["storage_build"]["written_payload_bytes"]


def test_readonly_original_dictionary_can_be_shared_without_an_extra_copy(tmp_path):
    stage = tmp_path / "stage"
    stage.mkdir()
    original = tmp_path / "dictionary.json"
    original.write_text('{"trained":"readonly"}')
    seal_new_file(original)
    producer = FactorizedProjectionReuse(stage)
    producer.dictionary(original, reuse_module.file_sha256(original))
    assert original.stat().st_ino == (stage / original.name.replace("dictionary", "feature_dictionary")).stat().st_ino
    assert producer.stats()["written_dictionary_bytes"] == 0
    assert producer.stats()["reused_dictionary_bytes"] == original.stat().st_size
    assert not original.stat().st_mode & 0o222


@pytest.mark.parametrize("changed", ["donor", "original"])
def test_dictionary_donor_never_hides_damage_or_a_changed_original(tmp_path, changed):
    root, donor, manifest = seed(tmp_path, sealed=False)
    original = tmp_path / "original.json"
    original.write_text('{"trained":"original"}')
    digest = reuse_module.file_sha256(original)
    FactorizedProjectionReuse(root).dictionary(original, digest)
    manifest["feature_dictionary_sha256"] = digest
    atomic_write_json(donor, manifest)
    seal_new_file(donor)
    damaged = root / "feature_dictionary.json" if changed == "donor" else original
    damaged.chmod(0o644)
    damaged.write_bytes(b"changed dictionary")
    if changed == "donor":
        seal_new_file(damaged)
    producer = new_producer(tmp_path, donor)
    if changed == "original":
        with pytest.raises(ValueError, match="original dictionary changed"):
            producer.dictionary(original, digest)
        assert not (producer.staging / "feature_dictionary.json").exists()
    else:
        producer.dictionary(original, digest)
        assert (producer.staging / "feature_dictionary.json").read_bytes() == original.read_bytes()
        assert producer.stats()["written_dictionary_bytes"] == original.stat().st_size
    assert damaged.read_bytes() == b"changed dictionary"


def attempt(tmp_path, generation="a" * 64):
    stage = tmp_path / "generations" / f".{generation}.attempt.{uuid.uuid4().hex}"
    stage.mkdir(parents=True, mode=0o700)
    return FactorizedProjectionReuse(stage, resume_generation=generation)


def test_failed_attempt_resumes_complete_members_without_a_ready_manifest(tmp_path, monkeypatch):
    values, columns, shape = source_values()
    first = attempt(tmp_path)
    proof = first.block("part.zst", values, logical_columns=columns, logical_shape=shape)
    common = first.common(np.asarray([[1., -0.], [np.nan, -2.]], dtype=np.float32))
    original = tmp_path / "dictionary.json"
    original.write_bytes(b'{"original":"dictionary"}')
    first.dictionary(original, reuse_module.file_sha256(original))
    assert not (first.staging / "factorized_manifest.json").exists()
    progress = first.staging / reuse_module.PROGRESS_NAME
    assert progress.stat().st_mode & 0o777 == 0o444
    assert len(json.loads(progress.read_bytes())["members"]) == 2

    retry = attempt(tmp_path)
    with monkeypatch.context() as patch:
        patch.setattr(reuse_module, "write_array_block", lambda *a, **k: pytest.fail("finished block recompressed"))
        actual = retry.block("part.zst", values, logical_columns=columns, logical_shape=shape)
        shared = retry.common(np.asarray([[1., -0.], [np.nan, -2.]], dtype=np.float32))
        retry.dictionary(original, reuse_module.file_sha256(original))
    assert actual == proof and shared == common
    for name in ("part.zst", "common.npy", "feature_dictionary.json"):
        assert (first.staging / name).stat().st_ino == (retry.staging / name).stat().st_ino
    assert _BlockCache(retry.staging, [actual], 1024**2).get(0).values.tobytes() == values.tobytes()
    assert retry.stats()["resumed_payloads"] == 2
    assert retry.stats()["written_payload_bytes"] == retry.stats()["written_dictionary_bytes"] == 0
    assert retry.stats()["resumed_payload_bytes"] == proof["bytes"] + common["bytes"]
    # A second interruption propagates the accepted prefix through one receipt,
    # rather than creating a sidecar for every payload or changing current.
    third = attempt(tmp_path)
    third.block("part.zst", values, logical_columns=columns, logical_shape=shape)
    assert third.stats()["resumed_payloads"] == 1
    assert not (tmp_path / "current").exists()


def test_resume_still_hashes_input_bits_and_only_rewrites_the_changed_member(tmp_path):
    values, columns, shape = source_values()
    first = attempt(tmp_path)
    first.block("one.zst", values, logical_columns=columns, logical_shape=shape)
    retry = attempt(tmp_path)
    unchanged = retry.block("one.zst", values, logical_columns=columns, logical_shape=shape)
    values[0, 0, 1] = 0.  # Signed zero is a real input identity change.
    changed = retry.block("two.zst", values, logical_columns=columns, logical_shape=shape)
    assert changed["producer_input_sha256"] != unchanged["producer_input_sha256"]
    assert retry.stats()["resumed_payloads"] == retry.stats()["written_payloads"] == 1
    assert (retry.staging / "two.zst").stat().st_ino != (first.staging / "one.zst").stat().st_ino
    assert _BlockCache(retry.staging, [changed], 1024**2).get(0).values.tobytes() == values.tobytes()


@pytest.mark.parametrize("damage", ["writable_progress", "bad_json", "other_identity",
                                     "escaped_member", "duplicate_member", "writable_payload",
                                     "corrupt_payload", "redirected_progress", "uncheckpointed"])
def test_bad_or_uncommitted_attempt_data_is_preserved_but_never_reused(tmp_path, damage):
    values, columns, shape = source_values()
    first = attempt(tmp_path)
    first.block("part.zst", values, logical_columns=columns, logical_shape=shape)
    progress = first.staging / reuse_module.PROGRESS_NAME
    payload = first.staging / "part.zst"
    if damage in {"writable_progress", "bad_json", "other_identity", "escaped_member", "duplicate_member"}:
        progress.chmod(0o644)
        metadata = json.loads(progress.read_bytes())
        if damage == "other_identity":
            metadata["implementation"] = {"other": "implementation"}
        elif damage == "escaped_member":
            metadata["members"][0]["path"] = "../outside.zst"
        elif damage == "duplicate_member":
            metadata["members"].append(metadata["members"][0])
        progress.write_text("damaged JSON" if damage == "bad_json" else json.dumps(metadata))
        if damage != "writable_progress":
            progress.chmod(0o444)
    elif damage in {"writable_payload", "corrupt_payload"}:
        payload.chmod(0o644)
        if damage == "corrupt_payload":
            payload.write_bytes(b"damaged payload")
            payload.chmod(0o444)
    else:
        progress.unlink()
        if damage == "redirected_progress":
            outside = tmp_path / "outside.json"
            outside.write_text("untrusted")
            progress.symlink_to(outside)
    before = (payload.read_bytes(), payload.stat().st_mode)
    retry = attempt(tmp_path)
    retry.block("part.zst", values, logical_columns=columns, logical_shape=shape)
    assert retry.stats()["written_payloads"] == 1 and retry.stats()["resumed_payloads"] == 0
    assert before == (payload.read_bytes(), payload.stat().st_mode)


def test_other_generation_and_unknown_files_are_not_resume_donors(tmp_path):
    values, columns, shape = source_values()
    first = attempt(tmp_path)
    first.block("part.zst", values, logical_columns=columns, logical_shape=shape)
    unknown = first.staging.parent / "unknown-source.bin"
    unknown.write_bytes(b"never touch unknown data")
    retry = attempt(tmp_path, "b" * 64)
    retry.block("part.zst", values, logical_columns=columns, logical_shape=shape)
    assert retry.stats()["resume_attempts_loaded"] == retry.stats()["resumed_payloads"] == 0
    assert unknown.read_bytes() == b"never touch unknown data"


def test_progress_commit_failure_never_claims_a_complete_or_reusable_generation(tmp_path, monkeypatch):
    values, columns, shape = source_values()
    first = attempt(tmp_path)
    with monkeypatch.context() as patch:
        def fail(*args, **kwargs):
            raise OSError("injected durable receipt failure")
        patch.setattr(reuse_module, "atomic_write_bytes", fail)
        with pytest.raises(OSError, match="durable receipt"):
            first.block("part.zst", values, logical_columns=columns, logical_shape=shape)
    assert not (first.staging / reuse_module.PROGRESS_NAME).exists()
    assert not (first.staging / "factorized_manifest.json").exists()
    retry = attempt(tmp_path)
    retry.block("part.zst", values, logical_columns=columns, logical_shape=shape)
    assert retry.stats()["written_payloads"] == 1 and retry.stats()["resumed_payloads"] == 0


def test_own_progress_replacement_is_not_silently_overwritten(tmp_path):
    values, columns, shape = source_values()
    first = attempt(tmp_path)
    first.block("part.zst", values, logical_columns=columns, logical_shape=shape)
    progress = first.staging / reuse_module.PROGRESS_NAME
    progress.chmod(0o644)
    progress.write_text("changed outside owner")
    with pytest.raises(ValueError, match="own attempt progress changed"):
        first.common(np.ones((2, 2), dtype=np.float32))
    assert progress.read_text() == "changed outside owner"


def test_progress_batches_repeated_receipts_and_flushes_at_acceptance(tmp_path, monkeypatch):
    monkeypatch.setattr(reuse_module.time, "monotonic", lambda: 1.0)
    values, columns, shape = source_values()
    first = attempt(tmp_path)
    for index in range(130):
        first.block(f"part-{index}.zst", values, logical_columns=columns, logical_shape=shape)
    assert first.counters["progress_commits"] == 3
    assert len(json.loads((first.staging / reuse_module.PROGRESS_NAME).read_bytes())["members"]) == 129
    assert first.stats()["progress_commits"] == 4
    retry = attempt(tmp_path)
    retry.block("part-129.zst", values, logical_columns=columns, logical_shape=shape)
    assert retry.stats()["resumed_payloads"] == 1


def test_progress_elapsed_time_flushes_even_a_small_batch(tmp_path, monkeypatch):
    now = [1.0]
    monkeypatch.setattr(reuse_module.time, "monotonic", lambda: now[0])
    values, columns, shape = source_values()
    first = attempt(tmp_path)
    first.block("a.zst", values, logical_columns=columns, logical_shape=shape)
    now[0] += reuse_module.PROGRESS_INTERVAL_SECONDS
    first.block("b.zst", values, logical_columns=columns, logical_shape=shape)
    assert first.counters["progress_commits"] == 2


def test_interrupted_uncheckpointed_batch_is_not_a_donor(tmp_path, monkeypatch):
    monkeypatch.setattr(reuse_module.time, "monotonic", lambda: 1.0)
    values, columns, shape = source_values()
    first = attempt(tmp_path)
    first.block("a.zst", values, logical_columns=columns, logical_shape=shape)
    first.block("b.zst", values + 1, logical_columns=columns, logical_shape=shape)
    retry = attempt(tmp_path)
    retry.block("a.zst", values, logical_columns=columns, logical_shape=shape)
    retry.block("b.zst", values + 1, logical_columns=columns, logical_shape=shape)
    assert retry.stats()["resumed_payloads"] == 1
    assert retry.stats()["written_payloads"] == 1


def test_real_process_exit_retains_durable_progress_for_a_fresh_owner(tmp_path):
    script = """
import json, os, sys, uuid
from pathlib import Path
import numpy as np
from stockagent.data_sync import factorized_projection_reuse as module
generation = 'c' * 64
stage = Path(sys.argv[1]) / 'generations' / f'.{generation}.attempt.{uuid.uuid4().hex}'
stage.mkdir(parents=True, mode=0o700)
producer = module.FactorizedProjectionReuse(stage, resume_generation=generation)
if sys.argv[2] == 'retry':
    module.write_array_block = lambda *a, **k: (_ for _ in ()).throw(AssertionError('recompressed after crash'))
proof = producer.block('part.zst', np.asarray([[[1., -0.]]], dtype=np.float32),
                       logical_columns=np.asarray([0, 2], dtype=np.uint32), logical_shape=(1, 1, 3))
print(json.dumps({'path': str(stage / 'part.zst'), 'inode': (stage / 'part.zst').stat().st_ino,
                  'sha256': proof['sha256'], 'resumed': producer.counters['resumed_payloads']}), flush=True)
if sys.argv[2] == 'crash':
    os._exit(71)  # No context manager/finalizer may rescue this receipt.
"""
    crashed = subprocess.run([sys.executable, "-c", script, str(tmp_path), "crash"],
                             capture_output=True, text=True, timeout=30)
    assert crashed.returncode == 71, crashed.stderr
    resumed = subprocess.run([sys.executable, "-c", script, str(tmp_path), "retry"],
                             capture_output=True, text=True, timeout=30)
    assert resumed.returncode == 0, resumed.stderr
    first, retry = (json.loads(result.stdout.splitlines()[-1]) for result in (crashed, resumed))
    assert first["inode"] == retry["inode"] and first["sha256"] == retry["sha256"]
    assert first["resumed"] == 0 and retry["resumed"] == 1
    assert Path(first["path"]).exists() and Path(retry["path"]).exists()


def test_optional_resume_scan_is_bounded_without_deleting_any_data(tmp_path, monkeypatch):
    first = attempt(tmp_path)
    values, columns, shape = source_values()
    first.block("part.zst", values, logical_columns=columns, logical_shape=shape)
    monkeypatch.setattr(reuse_module, "MAX_GENERATION_NAMES", 1)
    retry = attempt(tmp_path)
    retry.block("part.zst", values, logical_columns=columns, logical_shape=shape)
    assert retry.stats()["resumed_payloads"] == 0 and retry.stats()["rejected_donors"] == 1
    assert (first.staging / "part.zst").exists()

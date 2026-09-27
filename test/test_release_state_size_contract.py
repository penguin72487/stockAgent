"""Local-only restart, corruption and persistence tests for release receipts."""

import hashlib
import io
import json
from pathlib import Path
import sys

import pytest

from downloader import release_archive_io as receipts
from downloader import download_tw_cbc_fx_release_archive as fx
from downloader import download_tw_cbc_money_release_archive as money


DATASETS = (money.OUTPUT_NAME, fx.OUTPUT_NAME)


def completed(dataset):
    return {
        "dataset": dataset, "status": "complete", "complete": True,
        "parquet_sha256": "a" * 64, "listing_receipts": [{"page": 1}],
        "saved_releases": 1, "registered_releases": 1, "failures": [],
        "generated_at_utc": "2026-01-01T00:00:00+00:00",
        "scan_scope": "full_index", "padding": "",
    }


def encode(value):
    return (json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2) + "\n").encode()


def state_path(root, dataset):
    return root / "state" / f"{dataset}.json"


def load_state(root, dataset):
    return json.loads(state_path(root, dataset).read_bytes())


def near_limit(root, dataset, *, limit):
    value = completed(dataset)
    value["padding"] = "中🙂" + "x" * (limit - len(encode(value)) - 7)
    assert len(encode(value)) == limit
    receipts.write_release_state(root, dataset, value)
    return value


def fail(root, dataset, **extra):
    receipts.write_release_state(root, dataset, {
        "dataset": dataset, "status": "degraded", "error": "upstream failure", **extra,
    })


def reference_path(root, dataset):
    ref = load_state(root, dataset)["resume_checkpoint"]["completed_state_ref"]
    return root / "state" / "release_resume_evidence" / dataset / f'{ref["sha256"]}.json'


@pytest.mark.parametrize("dataset", DATASETS)
def test_actual_four_mib_boundary_and_utf8_survive_restart(tmp_path, dataset):
    prior = near_limit(tmp_path, dataset, limit=4 * 1024 * 1024)
    original = state_path(tmp_path, dataset).read_bytes()
    fail(tmp_path, dataset)
    state = load_state(tmp_path, dataset)
    assert state["status"] == "degraded" and state["complete"] is False
    assert state["resume_checkpoint"]["schema_version"] == 2
    assert state["oversized_diagnostic"]["saved"] is True
    assert state_path(tmp_path, dataset).stat().st_size < 2048
    assert reference_path(tmp_path, dataset).read_bytes() == original
    assert receipts.read_release_resume_state(tmp_path, dataset) == prior


@pytest.mark.parametrize("dataset", DATASETS)
@pytest.mark.parametrize("legacy_wrapped", [False, True])
def test_repeated_status_uses_verified_reference_without_inflating_old_proof(
    tmp_path, monkeypatch, dataset, legacy_wrapped,
):
    monkeypatch.setattr(receipts, "MAX_RELEASE_STATE_BYTES", 4096)
    prior = near_limit(tmp_path, dataset, limit=4096)
    if legacy_wrapped:
        prior["padding"] = ""
        envelope = {"dataset": dataset, "status": "degraded", "complete": False,
                    "resume_checkpoint": {"schema_version": 1, "completed_state": prior}}
        prior["padding"] = "x" * (4096 - len(encode(envelope)))
        state_path(tmp_path, dataset).write_bytes(encode(envelope))
        assert state_path(tmp_path, dataset).stat().st_size == 4096
    fail(tmp_path, dataset, error="x" * 4096)
    first = load_state(tmp_path, dataset)
    evidence = reference_path(tmp_path, dataset)
    evidence_stat = evidence.stat()
    diagnostic = tmp_path / first["oversized_diagnostic"]["path"]
    diagnostic_bytes = diagnostic.read_bytes()
    for status in ("running", "degraded", "running"):
        fail(tmp_path, dataset, status=status)
        state = load_state(tmp_path, dataset)
        assert state["status"] == status and state["complete"] is False
        assert state["resume_checkpoint"] == first["resume_checkpoint"]
        assert receipts.read_release_resume_state(tmp_path, dataset) == prior
    assert evidence.stat() == evidence_stat
    assert diagnostic.read_bytes() == diagnostic_bytes
    assert len(list(evidence.parent.iterdir())) == 1
    # A later valid online success retires the reference from latest, not history.
    receipts.write_release_state(tmp_path, dataset, completed(dataset))
    assert "resume_checkpoint" not in load_state(tmp_path, dataset)
    assert evidence.exists()


@pytest.mark.parametrize("module", [money, fx])
@pytest.mark.parametrize("prior_exists", [False, True])
def test_oversized_success_cli_raises_and_never_prints_green(
    tmp_path, monkeypatch, capsys, module, prior_exists,
):
    monkeypatch.setattr(receipts, "MAX_RELEASE_STATE_BYTES", 4096)
    dataset = module.OUTPUT_NAME
    prior = near_limit(tmp_path, dataset, limit=4096) if prior_exists else None
    proposed = completed(dataset)
    proposed["padding"] = "新" * 4096

    def collect(root, **kwargs):
        module._write_state(root, proposed)
        return proposed

    monkeypatch.setattr(module, "collect", collect)
    monkeypatch.setattr(sys, "argv", ["collector", "--output-dir", str(tmp_path)])
    with pytest.raises(receipts.ReleaseStateTooLarge):
        module.main()
    state = load_state(tmp_path, dataset)
    assert state["status"] == "degraded" and state["complete"] is False
    assert "ReleaseStateTooLarge" in state["error"]
    assert capsys.readouterr().out == ""
    assert receipts.read_release_resume_state(tmp_path, dataset) == prior
    diagnostic = tmp_path / "state" / "release_state_diagnostics" / f"{dataset}.oversized.json"
    assert json.loads(diagnostic.read_bytes()) == proposed


@pytest.mark.parametrize("module", [money, fx])
def test_large_failed_cli_keeps_original_upstream_exception(tmp_path, monkeypatch, module):
    monkeypatch.setattr(receipts, "MAX_RELEASE_STATE_BYTES", 4096)
    dataset = module.OUTPUT_NAME
    prior = near_limit(tmp_path, dataset, limit=4096)
    error = module.SourceAccessBlocked("upstream:" + "x" * 8192)

    def collect(*args, **kwargs):
        raise error

    monkeypatch.setattr(module, "collect", collect)
    monkeypatch.setattr(sys, "argv", ["collector", "--output-dir", str(tmp_path)])
    with pytest.raises(module.SourceAccessBlocked) as caught:
        module.main()
    assert caught.value is error
    assert load_state(tmp_path, dataset)["complete"] is False
    assert receipts.read_release_resume_state(tmp_path, dataset) == prior


@pytest.mark.parametrize("change", [
    "missing", "bytes", "size", "bool_size", "huge_size", "bad_hash", "extra_path",
    "bool_version", "inline_plus_ref", "dataset", "recursive", "symlink", "parent_symlink",
])
def test_corrupt_references_fail_closed(tmp_path, monkeypatch, change):
    monkeypatch.setattr(receipts, "MAX_RELEASE_STATE_BYTES", 4096)
    dataset = DATASETS[0]
    near_limit(tmp_path, dataset, limit=4096)
    fail(tmp_path, dataset)
    path = reference_path(tmp_path, dataset)
    state = load_state(tmp_path, dataset)
    checkpoint = state["resume_checkpoint"]
    ref = checkpoint["completed_state_ref"]
    if change == "missing":
        path.unlink()
    elif change == "bytes":
        path.write_bytes(b"x" * 4096)
    elif change in {"size", "bool_size", "huge_size"}:
        ref["bytes"] = {"size": 4095, "bool_size": True, "huge_size": 4097}[change]
    elif change == "bad_hash":
        ref["sha256"] = "../escape"
    elif change == "extra_path":
        ref["path"] = "/tmp/untrusted"
    elif change == "bool_version":
        checkpoint["schema_version"] = True
    elif change == "inline_plus_ref":
        checkpoint["completed_state"] = completed(dataset)
    elif change in {"dataset", "recursive"}:
        raw = encode(completed(DATASETS[1]) if change == "dataset" else state)
        ref.update(sha256=hashlib.sha256(raw).hexdigest(), bytes=len(raw))
        (path.parent / f'{ref["sha256"]}.json').write_bytes(raw)
    elif change == "symlink":
        alternate = tmp_path / "proof.json"
        path.rename(alternate)
        path.symlink_to(alternate)
    elif change == "parent_symlink":
        alternate = tmp_path / "proofs"
        parent = path.parent
        parent.rename(alternate)
        parent.symlink_to(alternate, target_is_directory=True)
    state_path(tmp_path, dataset).write_bytes(encode(state))
    assert receipts.read_release_resume_state(tmp_path, dataset) is None


@pytest.mark.parametrize("where", ["diagnostic", "evidence", "latest"])
def test_io_failure_preserves_evidence_and_reports_no_new_success(tmp_path, monkeypatch, where):
    monkeypatch.setattr(receipts, "MAX_RELEASE_STATE_BYTES", 4096)
    dataset = DATASETS[0]
    prior = near_limit(tmp_path, dataset, limit=4096)
    before = state_path(tmp_path, dataset).read_bytes()
    original = receipts.atomic_write_bytes

    def fault(path, raw, **kwargs):
        match = {"diagnostic": "release_state_diagnostics" in path.parts,
                 "evidence": "release_resume_evidence" in path.parts,
                 "latest": path == state_path(tmp_path, dataset)}[where]
        if match:
            raise OSError("injected persistence failure")
        return original(path, raw, **kwargs)

    monkeypatch.setattr(receipts, "atomic_write_bytes", fault)
    if where == "diagnostic":
        fail(tmp_path, dataset)
        state = load_state(tmp_path, dataset)
        assert state["complete"] is False
        assert state["oversized_diagnostic"]["saved"] is False
        assert state["oversized_diagnostic"]["error_type"] == "OSError"
    else:
        with pytest.raises(OSError, match="injected"):
            fail(tmp_path, dataset)
        # Cannot atomically persist a new status AND preserve unique evidence
        # when the evidence or current-state storage is unavailable.
        assert state_path(tmp_path, dataset).read_bytes() == before
    assert receipts.read_release_resume_state(tmp_path, dataset) == prior


def test_normal_path_encodes_once_and_keeps_durable_write(tmp_path, monkeypatch):
    original_encode = receipts._state_json_bytes
    original_write = receipts.atomic_write_bytes
    encoded, durable = [], []

    def encode_once(payload):
        encoded.append(payload)
        return original_encode(payload)

    def write(path, raw, **kwargs):
        durable.append(kwargs.get("durable"))
        return original_write(path, raw, **kwargs)

    monkeypatch.setattr(receipts, "_state_json_bytes", encode_once)
    monkeypatch.setattr(receipts, "atomic_write_bytes", write)
    receipts.write_release_state(tmp_path, DATASETS[0], completed(DATASETS[0]))
    assert len(encoded) == 1 and durable == [True]


def test_reference_reader_reads_exactly_two_bounded_files(tmp_path, monkeypatch):
    monkeypatch.setattr(receipts, "MAX_RELEASE_STATE_BYTES", 4096)
    dataset = DATASETS[0]
    near_limit(tmp_path, dataset, limit=4096)
    fail(tmp_path, dataset)
    paths = (state_path(tmp_path, dataset), reference_path(tmp_path, dataset))
    contents = {path: path.read_bytes() for path in paths}
    reads = []

    class Bounded(io.BytesIO):
        def read(self, size=-1):
            reads.append(size)
            return super().read(size)

    monkeypatch.setattr(Path, "open", lambda path, *args, **kwargs: Bounded(contents[path]))
    assert receipts.read_release_resume_state(tmp_path, dataset) is not None
    assert reads == [4097, 4097]


@pytest.mark.parametrize("fault", ["permission", "enospc", "symlink"])
@pytest.mark.parametrize("module", [money, fx])
def test_evidence_storage_failure_keeps_only_proof_and_cli_fails(
    tmp_path, monkeypatch, capsys, module, fault,
):
    monkeypatch.setattr(receipts, "MAX_RELEASE_STATE_BYTES", 4096)
    dataset = module.OUTPUT_NAME
    prior = near_limit(tmp_path, dataset, limit=4096)
    before = state_path(tmp_path, dataset).read_bytes()
    original = receipts.atomic_write_bytes

    def write(path, raw, **kwargs):
        if "release_resume_evidence" in path.parts:
            raise PermissionError("denied") if fault == "permission" else OSError(28, "full")
        return original(path, raw, **kwargs)

    if fault == "symlink":
        (tmp_path / "state" / "release_resume_evidence").symlink_to(tmp_path)
    else:
        monkeypatch.setattr(receipts, "atomic_write_bytes", write)

    def collect(root, **kwargs):
        fail(root, dataset)

    monkeypatch.setattr(module, "collect", collect)
    monkeypatch.setattr(sys, "argv", ["collector", "--output-dir", str(tmp_path)])
    with pytest.raises((OSError, ValueError)) as caught:
        module.main()
    assert "new_state_not_persisted" in " ".join(caught.value.__notes__)
    assert state_path(tmp_path, dataset).read_bytes() == before
    assert receipts.read_release_resume_state(tmp_path, dataset) == prior
    assert capsys.readouterr().out == ""

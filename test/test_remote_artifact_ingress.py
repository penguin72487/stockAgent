from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from scripts.ingest_remote_cold_artifacts import (
    RemoteCandidate,
    _decode_discovery,
    _resolved_release,
    _spec,
    _staging_artifact_root,
    _validate_remote_absolute,
    _validate_ssh_target,
    eligible_candidates,
)
from stockagent.data_sync.desync_snapshots import SnapshotError


def _candidate(relative_root: str, *, references: tuple[str, ...] = ()) -> RemoteCandidate:
    return RemoteCandidate(
        relative_root=relative_root,
        files=501,
        logical_bytes=1_250_237_113,
        portable_fingerprint_sha256="a" * 64,
        newest_mtime_ns=1_000,
        process_references=references,
    )


def test_discovery_decoder_ignores_ssh_banner_and_validates_rows() -> None:
    payload = {
        "schema_version": 1,
        "candidates": [
            {
                "relative_root": "ablations/suite/layernorm",
                "files": 2,
                "logical_bytes": 7,
                "portable_fingerprint_sha256": "b" * 64,
                "newest_mtime_ns": 123,
                "process_references": [],
            }
        ],
    }
    rows = _decode_discovery("Welcome to the peer\n" + json.dumps(payload) + "\n")
    assert rows == [
        RemoteCandidate(
            relative_root="ablations/suite/layernorm",
            files=2,
            logical_bytes=7,
            portable_fingerprint_sha256="b" * 64,
            newest_mtime_ns=123,
            process_references=(),
        )
    ]


def test_eligible_candidates_are_exactly_allowlisted_and_unused() -> None:
    wanted = _candidate("ablations/suite/layernorm")
    busy = _candidate("ablations/suite/busy", references=("pid=7:fd:checkpoint",))
    unrelated = _candidate("ablations/other/large")

    rows = eligible_candidates(
        [unrelated, busy, wanted],
        include_roots=[wanted.relative_root, busy.relative_root],
        stable_hours=0,
        now_ns=2_000,
    )

    assert rows == [wanted]


def test_remote_and_staging_paths_fail_closed(tmp_path: Path) -> None:
    with pytest.raises(SnapshotError, match="unsafe SSH target"):
        _validate_ssh_target("peer; touch /tmp/pwned")
    with pytest.raises(SnapshotError, match="unsafe remote repo"):
        _validate_remote_absolute("/root/../etc", "remote repo")

    candidate = _candidate("ablations/suite/layernorm")
    artifact_root = _staging_artifact_root(tmp_path, "vastai1T", _spec(candidate, 0))
    assert artifact_root.is_relative_to(tmp_path.resolve())
    assert "artifact-auto-layernorm-" in str(artifact_root)


def test_existing_release_must_match_remote_immutable_identity(
    tmp_path: Path, monkeypatch
) -> None:
    candidate = _candidate("ablations/suite/layernorm")
    spec = _spec(candidate, 0)
    (tmp_path / "heads" / spec.dataset).mkdir(parents=True)
    resolved = SimpleNamespace(
        manifest={
            "metadata": {"artifact_relative_root": spec.relative_root},
            "source": {
                "files": candidate.files,
                "logical_bytes": candidate.logical_bytes,
                "portable_fingerprint_sha256": "c" * 64,
            },
        }
    )
    monkeypatch.setattr(
        "scripts.ingest_remote_cold_artifacts.resolve_latest_packed",
        lambda *_args: resolved,
    )
    monkeypatch.setattr(
        "scripts.ingest_remote_cold_artifacts.verify_packed_snapshot",
        lambda *_args: {},
    )

    with pytest.raises(SnapshotError, match="changed after its immutable release"):
        _resolved_release(tmp_path, spec, candidate)

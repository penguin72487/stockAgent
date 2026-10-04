from concurrent.futures import ThreadPoolExecutor
import hashlib
import os
from pathlib import Path

import pytest

from stockagent.data_sync.artifact_dedup import link_immutable_source


def test_evidence_aliases_share_bytes_but_not_mutable_producer(tmp_path: Path):
    source = tmp_path / "raw.json"
    payload = b"official evidence"
    source.write_bytes(payload)
    digest = hashlib.sha256(payload).hexdigest()
    cache = tmp_path / ".rule-source-objects"
    first = tmp_path / "v1/sources" / digest
    second = tmp_path / "v2/sources" / digest
    link_immutable_source(source, first, cache, digest)
    link_immutable_source(source, second, cache, digest)
    assert os.path.samestat(first.stat(), second.stat())
    assert not os.path.samestat(first.stat(), source.stat())
    assert first.stat().st_mode & 0o222 == 0
    source.write_bytes(b"new producer version")
    assert first.read_bytes() == second.read_bytes() == payload
    link_immutable_source(source, first, cache, digest)
    assert first.read_bytes() == payload


def test_new_object_rejects_wrong_digest_without_alias(tmp_path: Path):
    source = tmp_path / "raw"
    source.write_bytes(b"wrong bytes")
    destination = tmp_path / "v1/sources/evidence"
    cache = tmp_path / "objects"
    with pytest.raises(ValueError, match="changed during copy"):
        link_immutable_source(source, destination, cache, "0" * 64)
    assert not destination.exists()
    assert not list(cache.rglob("*.partial"))


def test_corrupt_alias_is_not_overwritten(tmp_path: Path):
    source = tmp_path / "raw"
    source.write_bytes(b"good")
    digest = hashlib.sha256(b"good").hexdigest()
    destination = tmp_path / "v1/sources/evidence"
    destination.parent.mkdir(parents=True)
    destination.write_bytes(b"bad")
    with pytest.raises(ValueError, match="hash/type mismatch"):
        link_immutable_source(source, destination, tmp_path / "objects", digest)
    assert destination.read_bytes() == b"bad"


def test_corrupt_object_is_not_overwritten_or_linked(tmp_path: Path):
    source = tmp_path / "raw"
    source.write_bytes(b"good")
    digest = hashlib.sha256(b"good").hexdigest()
    cache = tmp_path / "objects"
    object_path = cache / digest[:2] / digest
    object_path.parent.mkdir(parents=True)
    object_path.write_bytes(b"bad")
    object_path.chmod(0o444)
    destination = tmp_path / "v1/sources/evidence"
    with pytest.raises(ValueError, match="hash/type mismatch"):
        link_immutable_source(source, destination, cache, digest)
    assert object_path.read_bytes() == b"bad"
    assert not destination.exists()


def test_symlink_object_is_rejected(tmp_path: Path):
    source = tmp_path / "raw"
    source.write_bytes(b"good")
    digest = hashlib.sha256(b"good").hexdigest()
    cache = tmp_path / "objects"
    object_path = cache / digest[:2] / digest
    object_path.parent.mkdir(parents=True)
    object_path.symlink_to(source)
    with pytest.raises(ValueError, match="hash/type mismatch"):
        link_immutable_source(source, tmp_path / "v1/evidence", cache, digest)
    assert source.read_bytes() == b"good"


def test_redirected_cache_is_rejected(tmp_path: Path):
    source = tmp_path / "raw"
    source.write_bytes(b"good")
    outside = tmp_path / "outside"
    outside.mkdir()
    cache = tmp_path / "objects"
    cache.symlink_to(outside, target_is_directory=True)
    with pytest.raises(ValueError, match="redirected"):
        link_immutable_source(source, tmp_path / "v1/source", cache,
                              hashlib.sha256(b"good").hexdigest())
    assert not (tmp_path / "v1/source").exists()


def test_concurrent_evidence_builds_do_not_copy_per_version(tmp_path: Path):
    source = tmp_path / "raw"
    payload = b"immutable evidence" * 1000
    source.write_bytes(payload)
    digest = hashlib.sha256(payload).hexdigest()
    cache = tmp_path / "objects"
    destinations = [tmp_path / f"v{i}/sources/evidence" for i in range(12)]
    with ThreadPoolExecutor(max_workers=4) as pool:
        list(pool.map(lambda target: link_immutable_source(source, target, cache, digest),
                      destinations))
    assert len({p.stat().st_ino for p in destinations}) == 1
    assert len(list(cache.glob("*/*"))) == 1
    assert all(p.read_bytes() == payload for p in destinations)


def test_rule_archive_keeps_manifest_paths_and_hashes(tmp_path: Path):
    from stockagent.data.tw_futures_margin_preparation import RuleArchive

    source = tmp_path / "raw.json"
    source.write_bytes(b'{"text":"original"}')
    digest = hashlib.sha256(source.read_bytes()).hexdigest()
    archives = []
    for name in ("v1", "v2"):
        archive = RuleArchive.__new__(RuleArchive)
        archive.bundle, archive.sources = tmp_path / name, {}
        relative = archive.copy(source, digest, url="official", kind="parsed_v2")
        assert relative == f"sources/{digest}.json"
        assert archive.sources[relative] == dict(path=relative, sha256=digest,
                                                url="official", kind="parsed_v2")
        archives.append(archive.bundle / relative)
    assert os.path.samestat(archives[0].stat(), archives[1].stat())

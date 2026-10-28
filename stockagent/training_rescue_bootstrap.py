"""Versioned bootstrap bindings for an unchanged checkpoint-only enrollment.

The rescue owner publishes these controls. This module validates their closed
bindings and streams existing objects; it neither selects hosts nor owns data.
Legacy bootstrap manifests retain their original exact-file-list contract.
"""
from __future__ import annotations

import gzip
import hashlib
import json
from pathlib import PurePosixPath
import posixpath
import tarfile

from stockagent.runtime_identity import validate_runtime_lock
from stockagent.training_rescue_worker import CONTRACT, HASH, digest, relative

BOOTSTRAP_BINDING_V2 = "stockagent.training_bootstrap_binding.v2"
BOOTSTRAP_REQUEST_V1 = "stockagent.training_bootstrap_attachment.v1"
MAX_BOOTSTRAP_CONTROL_BYTES = 64 * 1024**2


def _path(value):
    if (not isinstance(value, str) or not value or "\x00" in value or "\\" in value
            or not value.startswith("/") or str(PurePosixPath(value)) != value
            or ".." in PurePosixPath(value).parts):
        raise ValueError("bootstrap path must be an absolute canonical POSIX path")
    return value


def object_binding(row, *, paths=False):
    if (not isinstance(row, dict) or not isinstance(row.get("sha256"), str)
            or not HASH.fullmatch(row["sha256"]) or type(row.get("bytes")) is not int
            or not 0 <= row["bytes"] < 8 * 1024**3):
        raise ValueError("invalid bounded bootstrap object identity")
    if paths:
        _path(row.get("source_path"))
        _path(row.get("target_path"))
        if not isinstance(row.get("role"), str) or not row["role"]:
            raise ValueError("bootstrap file has no role")
    return row


def attachment_request(spec, request):
    fields = {"contract", "run_id", "spec_sha256", "source_identity", "checkpoint_commit_id",
              "files", "runtime_archive", "runtime_archive_proof"}
    if (not isinstance(request, dict) or set(request) != fields
            or request["contract"] != BOOTSTRAP_REQUEST_V1
            or request["run_id"] != spec["run_id"] or request["spec_sha256"] != digest(spec)
            or not isinstance(request["checkpoint_commit_id"], str)
            or not HASH.fullmatch(request["checkpoint_commit_id"])):
        raise ValueError("bootstrap attachment differs from the original enrollment")
    identity = request["source_identity"]
    if (not isinstance(identity, dict) or set(identity) != {"boot_id", "instance_id"}
            or not isinstance(identity["boot_id"], str) or not identity["boot_id"]):
        raise ValueError("bootstrap attachment needs the enrolled source identity")
    if spec.get("source_releases"):
        raise ValueError("bootstrap attachment currently requires one original frozen source release")
    rows = request["files"]
    if not isinstance(rows, list) or not 0 < len(rows) <= 64:
        raise ValueError("bootstrap attachment needs a bounded file list")
    targets = set()
    for row in rows:
        object_binding(row, paths=True)
        if row["target_path"] in targets:
            raise ValueError("bootstrap target paths overlap")
        targets.add(row["target_path"])
    if any(a != b and PurePosixPath(a) in PurePosixPath(b).parents for a in targets for b in targets):
        raise ValueError("bootstrap target files overlap a directory")
    for key in ("runtime_archive", "runtime_archive_proof"):
        object_binding(request[key])
        _path(request[key].get("source_path"))
    if request["runtime_archive_proof"]["source_path"] != request["runtime_archive"]["source_path"] + ".json":
        raise ValueError("runtime capsule proof belongs to another archive path")
    if (spec.get("runtime_capsule_path") is not None
            and spec["runtime_capsule_path"] != request["runtime_archive"]["source_path"]):
        raise ValueError("bootstrap attachment cannot replace an enrolled runtime capsule")
    # Complete only the missing size. All declared paths, roles, hashes and
    # optional original metadata must survive byte-for-byte as JSON values.
    original = spec.get("bootstrap_files")
    if not isinstance(original, list) or not original:
        raise ValueError("bootstrap attachment needs the original training config binding")
    for old in original:
        if (not isinstance(old, dict) or not old
                or sum(all(row.get(k) == v for k, v in old.items()) for row in rows) != 1):
            raise ValueError("bootstrap attachment changed an original file binding")
    return request


def bootstrap_objects(bootstrap):
    rows = [*bootstrap["files"], bootstrap["runtime_archive"], bootstrap["runtime_archive_proof"]]
    if bootstrap.get("bootstrap_contract") == BOOTSTRAP_BINDING_V2:
        rows.append(bootstrap["runtime_provenance_file"])
    return rows


def validate_bootstrap_binding(spec, bootstrap, *, read_object=None, source_identity=None):
    """Validate closed metadata; v2 also requires exact small original objects.

    The caller hashes every object it reads. Full archive decoding, destination
    CUDA and optimizer resume remain separate evidence, never inferred here.
    """
    if bootstrap.get("contract") != CONTRACT or bootstrap.get("spec_sha256") != digest(spec):
        raise ValueError("runtime bootstrap differs from original enrollment")
    version = bootstrap.get("bootstrap_contract")
    if version is None:
        if bootstrap["files"] != spec.get("bootstrap_files", []):
            raise ValueError("legacy runtime bootstrap file list differs from original enrollment")
        return bootstrap
    if version != BOOTSTRAP_BINDING_V2 or read_object is None:
        raise ValueError("unsupported or unreadable versioned runtime bootstrap")
    request = attachment_request(spec, bootstrap["attachment"])
    if request["source_identity"] != source_identity:
        raise ValueError("bootstrap attachment differs from the enrolled source identity")
    if (bootstrap.get("run_id") != spec["run_id"] or bootstrap["files"] != request["files"]
            or bootstrap["runtime_archive"] != request["runtime_archive"]
            or bootstrap["runtime_archive_proof"] != request["runtime_archive_proof"]
            or bootstrap.get("attachment_sha256") != digest(request)):
        raise ValueError("versioned bootstrap differs from its closed attachment request")
    commit = bootstrap["provenance_commit"]
    if (digest(commit) != request["checkpoint_commit_id"] or commit.get("run_id") != spec["run_id"]
            or commit.get("spec_sha256") != digest(spec)):
        raise ValueError("bootstrap runtime provenance has another checkpoint enrollment")
    inventory = commit["inventory"]
    if inventory.get("spec_sha256") != digest(spec) or inventory.get("context_sha256") != digest(inventory["files"]):
        raise ValueError("bootstrap runtime provenance context differs")
    row = bootstrap["runtime_provenance_file"]
    object_binding(row)
    matches = [r for r in inventory["files"] if r.get("relative") == "run_manifest.json"]
    if matches != [row]:
        raise ValueError("bootstrap runtime provenance is not the original committed manifest")
    provenance = json.loads(read_object(row))["runtime_provenance"]
    lock = provenance["runtime"]
    if (bootstrap.get("runtime_lock") != lock or validate_runtime_lock(lock, lock)
            or (spec.get("runtime_lock") is not None and spec["runtime_lock"] != lock)):
        raise ValueError("bootstrap runtime lock differs from original training provenance")
    release = spec.get("source_release", {})
    if not release or provenance.get("source_release") != release:
        raise ValueError("bootstrap source release differs from original training provenance")
    files = bootstrap["files"]
    config = [r for r in files if r["target_path"] == spec["config_file"] and r["role"] == "training_config"]
    if len(config) != 1 or not any(r.get("role") == "training_config" for r in spec["bootstrap_files"]):
        raise ValueError("bootstrap attachment lacks the original training config")
    receipts = [r for r in files if r["role"] == "source_receipt" and r["target_path"] == spec["source_receipt"]]
    if len(receipts) != 1 or receipts[0]["sha256"] != release.get("receipt_sha256"):
        raise ValueError("bootstrap frozen source receipt differs from original release")
    receipt = json.loads(read_object(receipts[0]))
    code, bundle = receipt["code"], receipt["source_bundle"]
    if (receipt.get("schema_version") != 1 or receipt.get("state") != "built"
            or digest(code["files"]) != code["source_sha256"]
            or code["source_sha256"] != release.get("source_sha256")
            or bundle["sha256"] != release.get("source_bundle_sha256")
            or not isinstance(bundle.get("files"), dict) or not bundle["files"]
            or any(bundle["files"].get(n) != sha for n, sha in code["files"].items())):
        raise ValueError("bootstrap source inventory differs from its original frozen release")
    admitted = [*spec["bootstrap_files"], receipts[0]]
    for key, role in (("source_bundle", "source_zip"), ("wheel", "source_wheel")):
        artifact = receipt[key]
        name = relative(artifact["file"])
        path = str(PurePosixPath(spec["source_receipt"]).parent / name)
        rows = [r for r in files if r["role"] == role and r["target_path"] == path]
        if (len(rows) != 1 or rows[0]["source_path"] != path
                or rows[0]["sha256"] != artifact["sha256"] or rows[0]["bytes"] != artifact["bytes"]):
            raise ValueError("bootstrap source artifact differs from its pinned receipt")
        admitted.append(rows[0])
    if (receipts[0]["source_path"] != receipts[0]["target_path"] or len(files) != len(admitted)
            or any(not any(all(row.get(k) == v for k, v in old.items()) for old in admitted) for row in files)):
        raise ValueError("bootstrap attachment contains an unbound supplemental file")
    proof = json.loads(read_object(bootstrap["runtime_archive_proof"]))
    runtime = bootstrap["runtime_archive"]
    if (proof.get("kind") != "fixed_prefix_runtime" or proof["prefix"] != spec["runtime_prefix"]
            or proof["archive_sha256"] != runtime["sha256"] or proof["archive_bytes"] != runtime["bytes"]
            or bootstrap["runtime_platform"] != {k: proof[k] for k in ("system", "machine", "libc", "uncompressed_bytes")}
            or bootstrap["source_runtime_stability"].get("state") != "source_runtime_still_matches_closed_capsule"
            or bootstrap.get("D_original_readback_verified") is not True):
        raise ValueError("bootstrap runtime capsule preservation binding differs")
    return bootstrap


def bootstrap_runtime_lock(spec, bootstrap):
    lock = spec.get("runtime_lock", bootstrap.get("runtime_lock"))
    if not isinstance(lock, dict) or validate_runtime_lock(lock, lock):
        raise ValueError("bootstrap lacks the original valid training runtime lock")
    return lock


def decode_runtime_capsule(stream, proof):
    """Read every runtime member, link, compressed byte and gzip CRC without C IO."""
    expected = {}
    rows = proof.get("files")
    if not isinstance(rows, list) or not 0 < len(rows) <= 250000:
        raise ValueError("runtime capsule needs a bounded complete inventory")
    prefix = PurePosixPath(_path(proof["prefix"]))
    for row in rows:
        name = relative(row["relative"])
        if name in expected or row.get("kind") not in {"file", "symlink"}:
            raise ValueError("invalid or duplicate runtime capsule member")
        if row["kind"] == "file":
            object_binding(row)
        expected[name] = row
    for name in expected:
        if any(p.as_posix() in expected for p in PurePosixPath(name).parents):
            raise ValueError("runtime member path overlaps another member")
    if (type(proof.get("uncompressed_bytes")) is not int or not 0 < proof["uncompressed_bytes"] <= 512 * 1024**3
            or sum(r.get("bytes", 0) for r in rows) != proof["uncompressed_bytes"]):
        raise ValueError("runtime original size differs from its inventory")
    seen, total, links = {}, 0, 0

    class Encoded:
        def __init__(self):
            self.sha, self.count = hashlib.sha256(), 0

        def read(self, size=-1):
            data = stream.read(size)
            self.count += len(data)
            if self.count > proof["archive_bytes"]:
                raise ValueError("runtime capsule exceeds its bound encoded size")
            self.sha.update(data)
            return data

    encoded = Encoded()
    with gzip.GzipFile(fileobj=encoded, mode="rb") as decoded:
        with tarfile.open(fileobj=decoded, mode="r|") as archive:
            for member in archive:
                name = relative(member.name)
                if name not in expected or name in seen or member.mode & 0o6000:
                    raise ValueError("runtime archive has an unknown, duplicate or unsafe member")
                row = expected[name]
                if member.isfile():
                    if row["kind"] != "file" or member.size != row["bytes"]:
                        raise ValueError("runtime archive member size/kind differs")
                    h = hashlib.sha256()
                    with archive.extractfile(member) as source:
                        for block in iter(lambda: source.read(4 * 1024**2), b""):
                            h.update(block)
                    if h.hexdigest() != row["sha256"]:
                        raise ValueError("runtime archive original SHA differs")
                    seen[name] = (row["sha256"], row["bytes"])
                    total += row["bytes"]
                elif member.islnk():
                    target = relative(member.linkname)
                    if row["kind"] != "file" or seen.get(target) != (row["sha256"], row["bytes"]):
                        raise ValueError("runtime archive hardlink differs")
                    seen[name] = seen[target]
                    total += row["bytes"]
                    links += 1
                elif member.issym():
                    encoding = proof.get("link_target_encoding", "path_normalized_v1")
                    if encoding not in {"raw_readlink_v2", "path_normalized_v1"}:
                        raise ValueError("unknown runtime symlink encoding")
                    text = member.linkname if encoding == "raw_readlink_v2" else str(PurePosixPath(member.linkname))
                    target = PurePosixPath(posixpath.normpath(member.linkname if member.linkname.startswith("/")
                        else str(prefix / PurePosixPath(name).parent / member.linkname)))
                    if (row["kind"] != "symlink" or text != row["target"] or not target.is_relative_to(prefix)
                            or "\x00" in member.linkname):
                        raise ValueError("runtime archive symlink differs or escapes its prefix")
                    seen[name] = None
                    links += 1
                else:
                    raise ValueError("unsupported runtime archive member type")
            if set(seen) != set(expected):
                raise ValueError("runtime archive is missing original members")
            for block in iter(lambda: archive.fileobj.read(4 * 1024**2), b""):
                if any(block):
                    raise ValueError("runtime archive has hidden data after its tar end")
    for _ in iter(lambda: encoded.read(4 * 1024**2), b""):
        pass
    if (encoded.count != proof["archive_bytes"] or encoded.sha.hexdigest() != proof["archive_sha256"]
            or total != proof["uncompressed_bytes"]):
        raise ValueError("runtime archive full SHA/size differs")
    return {"files": len(seen), "original_bytes": total, "links": links,
            "encoded_bytes": encoded.count, "archive_sha256": encoded.sha.hexdigest(),
            "gzip_CRC_and_complete_original_SHA_verified": True, "C_payload_bytes_written": 0}

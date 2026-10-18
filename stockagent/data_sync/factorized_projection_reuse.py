"""Reuse canonical factorized payloads inside their original generation owner.

The caller holds prepare.lock. A sealed preceding generation, a checkpointed
failed attempt of the SAME generation, or a newly sealed member supplies donors.
There is no global CAS, retirement policy, alternate codec, feature selection
or relaxed source fence. An attempt checkpoint is never a complete manifest.
"""
from __future__ import annotations

from dataclasses import dataclass, field
import hashlib
import json
import os
from pathlib import Path
import re
import shutil

import numpy as np

from downloader.artifact_io import atomic_write_bytes
from stockagent.data.factorized_panel import (
    VALUE_ONLY_CONTRACT, file_sha256, write_array_block,
)
from stockagent.data.panel_cache import array_content_fingerprint
from stockagent.data_sync.immutable_cache_files import (
    link_immutable_payload, payload_info, stable_payload_hash,
)
from stockagent.data_sync.physical_cache_reuse import _metadata as read_cache_metadata

CONTRACT = "factorized_generation_input_reuse_v1"
PROGRESS_CONTRACT = "factorized_attempt_progress_v1"
PROGRESS_NAME = "storage_progress.json"
MAX_MEMBERS = 8192
MAX_RESUME_ATTEMPTS = 8
MAX_GENERATION_NAMES = 4096


def implementation_identity() -> dict[str, str]:
    """Bind producer reuse and its existing integrity/codec dependencies."""
    paths = {Path(__file__), *(Path(function.__code__.co_filename) for function in (
        write_array_block, array_content_fingerprint, link_immutable_payload, read_cache_metadata,
        atomic_write_bytes))}
    return {str(path.resolve()): file_sha256(path) for path in sorted(paths)}


def seal_new_file(path: Path) -> None:
    """Only private new files may change mode; never seal an old/source inode."""
    before = payload_info(path)
    if before["nlink"] != 1 or before["uid"] != os.geteuid():
        raise ValueError("only a new owned single-link file may be sealed")
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
    try:
        opened = os.fstat(descriptor)
        if (opened.st_dev, opened.st_ino) != (before["device"], before["inode"]):
            raise ValueError("new member changed while sealing")
        os.fchmod(descriptor, 0o444)
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


@dataclass
class FactorizedProjectionReuse:
    staging: Path
    donor_manifest: Path | None = None
    resume_generation: str | None = None
    donors: dict = field(init=False, default_factory=dict)
    dictionary_donors: list = field(init=False, default_factory=list)
    progress_members: dict = field(init=False, default_factory=dict)
    progress_dictionary: dict | None = field(init=False, default=None)
    progress_metadata: dict | None = field(init=False, default=None)
    resume_roots: set = field(init=False, default_factory=set)
    counters: dict = field(init=False, default_factory=lambda: dict.fromkeys((
        "written_payloads", "reused_payloads", "written_payload_bytes", "avoided_payload_bytes",
        "unsealed_donor_generations", "rejected_donors",
        "written_dictionary_bytes", "reused_dictionary_bytes",
        "resume_attempts_loaded", "resumed_payloads", "resumed_payload_bytes", "progress_commits",
    ), 0))

    def __post_init__(self):
        self.staging = self.staging.absolute()
        if self.staging.resolve(strict=True) != self.staging or not self.staging.is_dir():
            raise ValueError("projection staging is redirected or missing")
        self.code = implementation_identity()
        if self.resume_generation is not None:
            info = self.staging.stat()
            if (not isinstance(self.resume_generation, str)
                    or not re.fullmatch(r"[0-9a-f]{64}", self.resume_generation)
                    or not re.fullmatch(rf"\.{self.resume_generation}\.attempt\.[0-9a-f]{{32}}",
                                        self.staging.name)
                    or os.path.lexists(self.staging / PROGRESS_NAME)
                    or info.st_uid != os.geteuid() or info.st_mode & 0o022):
                raise ValueError("resume requires a fresh attempt of one exact generation")
            self._load_attempt_donors()
        if self.donor_manifest is None:
            return
        path = self.donor_manifest.absolute()
        if (path.resolve(strict=True) != path or path.parent.parent != self.staging.parent
                or path.parent == self.staging):
            raise ValueError("projection donor escaped its original generation owner")
        info = payload_info(path)
        if info["mode"] & 0o222 or info["uid"] != os.geteuid():
            self.counters["unsealed_donor_generations"] += 1
            return  # Old writable generations stay unchanged, never hardlinked.
        try:
            manifest, _ = read_cache_metadata(path)
            if (manifest.get("contract") != VALUE_ONLY_CONTRACT
                    or manifest.get("paper_inference", {}).get("status") != "verified"
                    or manifest.get("storage_build", {}).get("contract") != CONTRACT):
                return
            members = [manifest["common"]]
            for block in manifest["blocks"]:
                members.extend(block["symbol_blocks"])
                if len(members) > MAX_MEMBERS:
                    raise ValueError("projection donor exceeds bounded member inventory")
            # Validate the whole small inventory before admitting any index.
            # A broken optional donor is a miss, not a permanently blocked build.
            self._validate_members(members)
        except (AttributeError, KeyError, TypeError, ValueError, FileNotFoundError):
            self.counters["rejected_donors"] += 1
            return
        for proof in members:
            self.donors.setdefault(proof["producer_input_sha256"], []).append((path.parent / proof["path"], proof))
        dictionary_sha = manifest.get("feature_dictionary_sha256")
        if isinstance(dictionary_sha, str) and re.fullmatch(r"[0-9a-f]{64}", dictionary_sha):
            self.dictionary_donors.append((path.parent / "feature_dictionary.json", dictionary_sha))

    @staticmethod
    def _validate_members(members):
        if not isinstance(members, list) or len(members) > MAX_MEMBERS:
            raise ValueError("projection donor exceeds bounded member inventory")
        names = set()
        for proof in members:
            relative = proof.get("path")
            if (not isinstance(relative, str) or Path(relative).name != relative
                    or relative in {"", ".", "..", PROGRESS_NAME, "factorized_manifest.json",
                                    "projection_receipt.json", "feature_dictionary.json"}
                    or relative in names
                    or any(not isinstance(proof.get(key), str)
                           or not re.fullmatch(r"[0-9a-f]{64}", proof[key])
                           for key in ("producer_input_sha256", "sha256"))
                    or type(proof.get("bytes")) is not int or proof["bytes"] < 0
                    or not isinstance(proof.get("shape"), list)
                    or any(type(size) is not int or size < 0 for size in proof["shape"])):
                raise ValueError("malformed projection reuse member")
            names.add(relative)

    def _load_attempt_donors(self):
        """Only this generation's bounded, owned attempts; never adopt unknown files."""
        pattern = re.compile(rf"\.{self.resume_generation}\.attempt\.[0-9a-f]{{32}}\Z")
        candidates = []
        with os.scandir(self.staging.parent) as entries:
            for count, entry in enumerate(entries, 1):
                if count > MAX_GENERATION_NAMES:
                    self.counters["rejected_donors"] += 1
                    return  # Optional reuse cannot turn a large namespace into a failed build.
                if entry.name != self.staging.name and pattern.fullmatch(entry.name):
                    if entry.is_dir(follow_symlinks=False):
                        candidates.append((entry.stat(follow_symlinks=False).st_mtime_ns, entry.name))
        for _, name in sorted(candidates, reverse=True)[:MAX_RESUME_ATTEMPTS]:
            root = self.staging.parent / name
            path = root / PROGRESS_NAME
            try:
                info = root.lstat()
                if (root.resolve(strict=True) != root or info.st_uid != os.geteuid()
                        or info.st_mode & 0o022 or info.st_dev != self.staging.stat().st_dev):
                    raise ValueError("untrusted attempt directory")
                proof = payload_info(path)
                if (proof["mode"] != 0o444 or proof["uid"] != os.geteuid()
                        or proof["nlink"] != 1 or proof["xattrs"]):
                    raise ValueError("unsealed attempt progress")
                progress, _ = read_cache_metadata(path)
                if (progress.get("contract") != PROGRESS_CONTRACT
                        or progress.get("generation") != self.resume_generation
                        or progress.get("implementation") != self.code):
                    raise ValueError("attempt progress input identity mismatch")
                members = progress["members"]
                self._validate_members(members)
                dictionary = progress.get("dictionary")
                if dictionary is not None and (
                        not isinstance(dictionary, dict)
                        or not isinstance(dictionary.get("sha256"), str)
                        or not re.fullmatch(r"[0-9a-f]{64}", dictionary["sha256"])
                        or type(dictionary.get("bytes")) is not int or dictionary["bytes"] < 0):
                    raise ValueError("malformed attempt dictionary")
                if payload_info(path) != proof:
                    raise ValueError("attempt progress changed while reading")
            except (AttributeError, KeyError, TypeError, ValueError, FileNotFoundError):
                self.counters["rejected_donors"] += 1
                continue
            for member in members:
                self.donors.setdefault(member["producer_input_sha256"], []).append((root / member["path"], member))
            if dictionary is not None:
                self.dictionary_donors.append((root / "feature_dictionary.json", dictionary["sha256"]))
            self.resume_roots.add(root)
            self.counters["resume_attempts_loaded"] += 1

    def _checkpoint(self, proof=None, dictionary=None):
        """One replace-in-place small receipt, never one sidecar per payload."""
        if self.resume_generation is None:
            return
        if proof is not None:
            self.progress_members[proof["path"]] = dict(proof)
        if dictionary is not None:
            self.progress_dictionary = dict(dictionary)
        members = list(self.progress_members.values())
        self._validate_members(members)
        path = self.staging / PROGRESS_NAME
        if (self.progress_metadata is not None and payload_info(path) != self.progress_metadata):
            raise ValueError("own attempt progress changed before publication")
        value = {"contract": PROGRESS_CONTRACT, "generation": self.resume_generation,
                 "implementation": self.code, "members": members,
                 "dictionary": self.progress_dictionary}
        atomic_write_bytes(path, json.dumps(value, sort_keys=True, separators=(",", ":"),
                                           allow_nan=False).encode(), durable=True, mode=0o444)
        self.progress_metadata = payload_info(path)
        self.counters["progress_commits"] += 1

    def _key(self, kind, values, columns=None, logical_shape=None):
        value = {"contract": CONTRACT, "kind": kind, "implementation": self.code,
                 "numpy_version": np.__version__, "values": array_content_fingerprint(values),
                 "columns": array_content_fingerprint(columns),
                 "logical_shape": list(logical_shape) if logical_shape is not None else None,
                 "fortran_npy": bool(kind == "common_npy" and values.flags.f_contiguous
                                      and not values.flags.c_contiguous)}
        return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()

    def _reuse(self, name, key, shape):
        target = self.staging / name
        if Path(name).name != name or name in {"", ".", ".."} or os.path.lexists(target):
            raise ValueError("projection member must be a new immediate file")
        for source, proof in self.donors.get(key, ()):
            if proof.get("shape") != list(shape):
                self.counters["rejected_donors"] += 1
                continue
            if link_immutable_payload(source, target, sha256=proof["sha256"], size=proof["bytes"]):
                self.counters["reused_payloads"] += 1
                self.counters["avoided_payload_bytes"] += proof["bytes"]
                if source.parent in self.resume_roots:
                    self.counters["resumed_payloads"] += 1
                    self.counters["resumed_payload_bytes"] += proof["bytes"]
                result = {**{k: v for k, v in proof.items() if k != "symbol_start"}, "path": name}
                self._checkpoint(result)
                return result
            self.counters["rejected_donors"] += 1
        return None

    def _record_written(self, proof, key):
        path = self.staging / proof["path"]
        seal_new_file(path)
        proof = {**proof, "producer_input_sha256": key}
        self.donors.setdefault(key, []).append((path, proof))
        self.counters["written_payloads"] += 1
        self.counters["written_payload_bytes"] += proof["bytes"]
        self._checkpoint(proof)
        return proof

    def block(self, name, values, *, logical_columns, logical_shape):
        values = np.ascontiguousarray(values, dtype=np.float32)
        columns = np.asarray(logical_columns)
        key = self._key("canonical_float32_columns", values, columns, logical_shape)
        reused = self._reuse(name, key, logical_shape)
        if reused is not None:
            return reused
        proof = write_array_block(self.staging / name, values, omit_zero_columns=True,
                                  logical_columns=columns, logical_shape=logical_shape)
        return self._record_written(proof, key)

    def common(self, values):
        values = np.asarray(values)
        key = self._key("common_npy", values)
        reused = self._reuse("common.npy", key, values.shape)
        if reused is not None:
            return reused
        path = self.staging / "common.npy"
        with path.open("xb") as stream:
            np.save(stream, values, allow_pickle=False)
        proof = {"path": path.name, "shape": list(values.shape),
                 "sha256": file_sha256(path), "bytes": path.stat().st_size}
        return self._record_written(proof, key)

    def dictionary(self, source: Path, expected_sha256: str):
        """Writable originals need one sealed copy, not another copy per build."""
        target = self.staging / "feature_dictionary.json"
        if os.path.lexists(target):
            raise ValueError("projection dictionary already exists")
        if link_immutable_payload(source, target, sha256=expected_sha256, size=source.stat().st_size):
            self.counters["reused_dictionary_bytes"] += target.stat().st_size
            self._checkpoint(dictionary={"sha256": expected_sha256, "bytes": target.stat().st_size})
            return
        before = payload_info(source)
        if stable_payload_hash(source, before) != expected_sha256:
            raise ValueError("original dictionary changed before copy")
        for donor, digest in self.dictionary_donors:
            if digest == expected_sha256 and link_immutable_payload(
                    donor, target, sha256=expected_sha256, size=before["size"]):
                if payload_info(source) != before:
                    raise ValueError("original dictionary changed during reuse")
                self.counters["reused_dictionary_bytes"] += before["size"]
                self._checkpoint(dictionary={"sha256": expected_sha256, "bytes": before["size"]})
                return
        with target.open("xb") as writer, source.open("rb") as reader:
            shutil.copyfileobj(reader, writer, 1024**2)
        seal_new_file(target)
        if payload_info(source) != before or file_sha256(target) != expected_sha256:
            raise ValueError("original dictionary changed during copy")
        self.counters["written_dictionary_bytes"] += before["size"]
        self._checkpoint(dictionary={"sha256": expected_sha256, "bytes": before["size"]})

    def stats(self):
        if implementation_identity() != self.code:
            raise ValueError("projection storage implementation changed during preparation")
        return {"contract": CONTRACT, **self.counters}

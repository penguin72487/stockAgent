"""Immutable, lossless paper-journal byte views with explicit append lineage.

Reader-first representation only: there is deliberately no environment switch
or automatic production enrollment. The existing executor owns transactions,
the original JSONL projection, financial semantics and post-fsync commit/ACK.
Unknown mutations require a complete new epoch, never an inferred append.
"""
from __future__ import annotations

from bisect import bisect_right
from contextlib import contextmanager
import hashlib
import io
import json
import os
from pathlib import Path
import stat
import uuid

from downloader.artifact_io import atomic_write_json
from stockagent.live.paper_mode_snapshot import _put_object, _read_object, _generation


CONTRACT = "paper_immutable_journal_lineage_v1"
FILENAMES = ("signals.jsonl", "orders.jsonl", "fills.jsonl", "events.jsonl", "marks.jsonl", "benchmark_marks.jsonl")
DIRECTORY = "journal_objects"
HEADS = "journal_heads"
CHUNK_BYTES = 4 * 1024**2
MAX_HEAD_BYTES = 2 * 1024**2
MAX_ENCODED_CHUNK_BYTES = CHUNK_BYTES + 128 * 1024
MAX_SOURCE_BYTES = 1024**4
MAX_NODES = 100_000
MAX_CHUNKS = 1_000_000
APPEND_INTENT_CONTRACT = "paper_journal_expected_append_v1"
MAX_APPEND_BYTES = 256 * 1024**2


def _head_path(path: Path) -> Path:
    if path.name not in FILENAMES:
        raise ValueError("unsupported paper journal")
    return path.parent / HEADS / (path.name + ".json")


def managed(path: Path) -> bool:
    path = Path(path)
    if path.name not in FILENAMES:
        return False
    head = _head_path(path)
    for control in (head, head.with_suffix(".enrolled"), _intent_path(path)):
        try:
            control.lstat()
        except FileNotFoundError:
            continue
        return True
    return False


def _intent_path(path: Path) -> Path:
    return _head_path(path).with_suffix(".append-intent.json")


def layout_generations(root: Path, *, names=FILENAMES) -> dict:
    result = {}
    for name in names:
        if name not in FILENAMES:
            raise ValueError("unsupported paper journal")
        path = _head_path(Path(root) / name)
        for member in (path, path.with_suffix(".enrolled")):
            try:
                value = member.lstat()
            except FileNotFoundError:
                continue
            if not stat.S_ISREG(value.st_mode):
                raise ValueError("journal layout requires regular control files")
            result[str(member.relative_to(root))] = list(_generation(value))
        source = Path(root) / name
        if managed(source):
            # A financial commit may not acknowledge a raw projection that
            # is ahead of its immutable head after an interrupted write.
            pointer = _pointer(source)
            if tuple(pointer["source_generation"]) != _generation(source.lstat()):
                raise OSError("paper journal projection is ahead of its accepted head")
            if list(_generation(path.lstat())) != result[str(path.relative_to(root))]:
                raise OSError("paper journal head changed during commit capture")
    return result


@contextmanager
def _objects(root: Path):
    descriptor = os.open(root / DIRECTORY, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        yield descriptor
    finally:
        os.close(descriptor)


def _reference(value, *, maximum=MAX_HEAD_BYTES):
    return (isinstance(value, dict) and set(value) == {"sha256", "size_bytes"}
            and isinstance(value["sha256"], str) and len(value["sha256"]) == 64
            and all(c in "0123456789abcdef" for c in value["sha256"])
            and type(value["size_bytes"]) is int and 0 < value["size_bytes"] <= maximum)


def _pointer(path):
    pointer = _head_path(path)
    descriptor = os.open(pointer, os.O_RDONLY | os.O_NONBLOCK | os.O_NOFOLLOW)
    with os.fdopen(descriptor, "rb") as handle:
        before = os.fstat(handle.fileno())
        if not stat.S_ISREG(before.st_mode) or before.st_size > MAX_HEAD_BYTES:
            raise ValueError("invalid journal head pointer")
        payload = json.loads(handle.read(MAX_HEAD_BYTES + 1))
        if _generation(before) != _generation(os.fstat(handle.fileno())) or _generation(before) != _generation(pointer.lstat()):
            raise OSError("journal head changed during read")
    if (not isinstance(payload, dict) or set(payload) != {"contract", "journal", "head", "source_generation"}
            or payload["contract"] != CONTRACT or payload["journal"] != path.name
            or not _reference(payload["head"]) or not isinstance(payload["source_generation"], list)
            or len(payload["source_generation"]) != 5 or any(type(x) is not int for x in payload["source_generation"])):
        raise ValueError("invalid journal head contract")
    return payload


def _read_node(directory, reference):
    if not _reference(reference):
        raise ValueError("invalid journal node reference")
    node = json.loads(_read_object(directory, reference, cache=True))
    if (not isinstance(node, dict) or set(node) != {"contract", "operation", "epoch", "parent", "base_size", "size_bytes", "chunks"}
            or node["contract"] != CONTRACT or node["operation"] not in {"seed", "append", "replace"}
            or not isinstance(node["epoch"], str) or len(node["epoch"]) != 32
            or any(c not in "0123456789abcdef" for c in node["epoch"])
            or type(node["base_size"]) is not int or type(node["size_bytes"]) is not int
            or not 0 <= node["base_size"] <= node["size_bytes"] <= MAX_SOURCE_BYTES
            or not isinstance(node["chunks"], list) or len(node["chunks"]) > MAX_CHUNKS
            or (node["parent"] is not None and not _reference(node["parent"]))):
        raise ValueError("invalid immutable journal node")
    size = node["base_size"]
    for item in node["chunks"]:
        if (not isinstance(item, dict) or set(item) != {"sha256", "size_bytes", "raw_sha256", "raw_size_bytes"}
                or not _reference({k: item[k] for k in ("sha256", "size_bytes")}, maximum=MAX_ENCODED_CHUNK_BYTES)
                or type(item["raw_size_bytes"]) is not int or not 0 < item["raw_size_bytes"] <= CHUNK_BYTES
                or not isinstance(item["raw_sha256"], str) or len(item["raw_sha256"]) != 64
                or any(c not in "0123456789abcdef" for c in item["raw_sha256"])):
            raise ValueError("invalid immutable journal chunk")
        size += item["raw_size_bytes"]
    if size != node["size_bytes"] or (node["operation"] != "append" and node["base_size"] != 0):
        raise ValueError("journal node byte coverage mismatch")
    return node


class JournalSnapshot:
    """One content-bound byte partition; replaces never extend an old epoch."""
    def __init__(self, path: Path):
        self.path = Path(path)
        self.pointer = _pointer(self.path)
        self.head = self.pointer["head"]
        self.generation = tuple(self.pointer["source_generation"])
        self.chunks, self.ancestors, self.starts = [], {}, []
        with _objects(self.path.parent) as directory:
            reference, chain, seen = self.head, [], set()
            for _ in range(MAX_NODES):
                if reference["sha256"] in seen:
                    raise ValueError("journal lineage cycle")
                seen.add(reference["sha256"])
                node = _read_node(directory, reference)
                self.ancestors[reference["sha256"]] = node["size_bytes"]
                chain.append(node)
                if node["operation"] != "append":
                    break
                if node["parent"] is None:
                    raise ValueError("append journal has no accepted parent")
                reference = node["parent"]
            else:
                raise ValueError("journal lineage exceeds bounded depth")
            self.epoch = chain[0]["epoch"]
            size = 0
            for node in reversed(chain):
                if node["epoch"] != self.epoch or node["base_size"] != size:
                    raise ValueError("journal append parent/epoch mismatch")
                self.chunks.extend(node["chunks"])
                size = node["size_bytes"]
                if len(self.chunks) > MAX_CHUNKS:
                    raise ValueError("journal chunk inventory exceeds bounds")
        self.size = size
        cursor = 0
        for chunk in self.chunks:
            self.starts.append(cursor)
            cursor += chunk["raw_size_bytes"]
        if self.size != self.generation[2]:
            raise ValueError("journal projection size does not match immutable view")

    def extends(self, head: str | None, size: int) -> bool:
        return head is not None and self.ancestors.get(head) == size

    def read_chunk(self, index):
        item = self.chunks[index]
        with _objects(self.path.parent) as directory:
            return _decode_chunk(directory, item)


def _decode_chunk(directory, item):
    import pyarrow as pa
    reference = {key: item[key] for key in ("sha256", "size_bytes")}
    encoded = _read_object(directory, reference, cache=True, suffix=".zst")
    # The validated node bounds the native decoder's destination to <= 4 MiB.
    raw = pa.decompress(encoded, decompressed_size=item["raw_size_bytes"],
                        codec="zstd", asbytes=True)
    if len(raw) != item["raw_size_bytes"] or hashlib.sha256(raw).hexdigest() != item["raw_sha256"]:
        raise ValueError("journal raw chunk hash mismatch")
    return raw


class _JournalReader(io.RawIOBase):
    def __init__(self, snapshot):
        self.snapshot, self.cursor = snapshot, 0
        self.descriptor = os.open(snapshot.path, os.O_RDONLY | os.O_NONBLOCK | os.O_NOFOLLOW)
        try:
            self._check()
        except BaseException:
            os.close(self.descriptor)
            self.descriptor = -1
            raise
        self.cached_chunk = (-1, b"")

    def _check(self):
        if (not stat.S_ISREG(os.fstat(self.descriptor).st_mode)
                or _generation(os.fstat(self.descriptor)) != self.snapshot.generation
                or _generation(self.snapshot.path.lstat()) != self.snapshot.generation):
            raise OSError("paper journal projection changed outside accepted lineage")

    def fileno(self):
        # Metadata only unless verified_journal_projection has independently
        # proved every retained raw byte against the immutable representation.
        return self.descriptor

    def readable(self): return True
    def seekable(self): return True
    def tell(self): return self.cursor

    def seek(self, offset, whence=io.SEEK_SET):
        self._checkClosed()
        if whence not in (io.SEEK_SET, io.SEEK_CUR, io.SEEK_END):
            raise ValueError("invalid journal seek origin")
        target = offset + (self.cursor if whence == io.SEEK_CUR else self.snapshot.size if whence == io.SEEK_END else 0)
        if target < 0:
            raise ValueError("negative journal seek")
        self.cursor = target
        return target

    def readinto(self, target):
        self._checkClosed()
        count = min(len(target), max(0, self.snapshot.size - self.cursor))
        written = 0
        while written < count:
            index = bisect_right(self.snapshot.starts, self.cursor) - 1
            if self.cached_chunk[0] != index:
                # Immutable chunks, plus open/close fences, make per-8-KiB
                # projection stat polling redundant. Keep a boundary check
                # before each new chunk, not millions of extra syscalls.
                self._check()
                self.cached_chunk = (index, self.snapshot.read_chunk(index))
            chunk = self.cached_chunk[1]
            offset = self.cursor - self.snapshot.starts[index]
            length = min(count - written, len(chunk) - offset)
            target[written:written + length] = chunk[offset:offset + length]
            self.cursor += length
            written += length
        return written

    def close(self):
        if not self.closed:
            try:
                if self.descriptor >= 0:
                    self._check()
            finally:
                if self.descriptor >= 0:
                    os.close(self.descriptor)
                    self.descriptor = -1
                super().close()


def open_paper_journal(path: Path, mode="rb", *, encoding=None, **options):
    path = Path(path)
    if not managed(path):
        return path.open(mode, encoding=encoding, **options)
    if mode not in {"r", "rb"}:
        raise ValueError("immutable journal writers must use canonical append/replacement owner")
    if set(options) - {"errors", "newline"} or (mode == "rb" and options):
        raise ValueError("unknown immutable journal reader options")
    if mode == "rb" and encoding is not None:
        raise ValueError("binary journal reader does not take an encoding")
    stream = io.BufferedReader(_JournalReader(JournalSnapshot(path)))
    return stream if mode == "rb" else io.TextIOWrapper(stream, encoding=encoding or "utf-8", **options)


def snapshot_of(stream):
    if isinstance(stream, io.TextIOWrapper):
        stream = stream.buffer
    raw = getattr(stream, "raw", None)
    return raw.snapshot if isinstance(raw, _JournalReader) else None


@contextmanager
def verified_journal_projection(path: Path):
    """Bounded compatibility bridge for file-only lazy/columnar readers.

    Materialize/collect inside this context. Enrolled inputs first prove EVERY
    retained projection byte against decoded immutable chunks, then expose the
    already-open inode through procfs. Open/close and head-generation fences
    reject concurrent changes. This deliberately costs a full validation pass;
    it is an operator/export bridge, not an opening-path latency shortcut or a
    RAM/disk copy of a multi-gigabyte journal. Legacy readers retain their path.
    """
    path = Path(path)
    if not managed(path):
        yield path
        return
    snapshot = JournalSnapshot(path)
    with _JournalReader(snapshot) as reader:
        offset = 0
        for index in range(len(snapshot.chunks)):
            block = snapshot.read_chunk(index)
            if os.pread(reader.fileno(), len(block), offset) != block:
                raise ValueError("journal projection differs from immutable bytes")
            offset += len(block)
        reader._check()
        descriptor_path = Path(f"/proc/self/fd/{reader.fileno()}")
        if not descriptor_path.exists():
            raise OSError("file-only journal readers require a live procfs descriptor")
        if _pointer(path) != snapshot.pointer:
            raise OSError("journal head changed during projection validation")
        yield descriptor_path
        reader._check()
        if _pointer(path) != snapshot.pointer:
            raise OSError("journal head changed during projection collection")


def rebind_cloned_journals(source_root: Path, target_root: Path) -> dict:
    """Verify an explicit private clone before binding its new raw inodes.

    A directory copy changes raw file generations. Copying the old head or
    simply accepting matching sizes is NOT an append proof. Validate every
    retained target byte against the copied immutable chunk hashes first.
    Callers retain their source/maintenance lease and may only promote after
    their ordinary complete-ledger checks and a new committed-view receipt.
    There is no enrollment, financial write, live repair or deletion here.
    """
    source_root, target_root = Path(source_root), Path(target_root)
    if source_root.resolve() == target_root.resolve():
        raise ValueError("journal clone requires a distinct private target")
    plans = []
    for name in FILENAMES:
        source, target = source_root / name, target_root / name
        if not managed(source):
            if managed(target):
                raise ValueError("clone contains a journal layout absent from its source")
            continue
        original, cloned = JournalSnapshot(source), JournalSnapshot(target)
        if original.pointer != cloned.pointer:
            raise ValueError("clone does not retain the exact immutable journal head")
        # The original projection must still describe the accepted source;
        # reading a stale/corrupt head can never authorize a clone rebind.
        with _JournalReader(original):
            pass
        fd = os.open(target, os.O_RDONLY | os.O_NONBLOCK | os.O_NOFOLLOW)
        with os.fdopen(fd, "rb") as stream:
            before = os.fstat(stream.fileno())
            generation = _generation(before)
            if not stat.S_ISREG(before.st_mode) or before.st_size != cloned.size:
                raise ValueError("clone projection is not the complete regular journal")
            for index in range(len(cloned.chunks)):
                block = cloned.read_chunk(index)
                if stream.read(len(block)) != block:
                    raise ValueError("cloned projection bytes differ from immutable history")
            if (stream.read(1) or _generation(os.fstat(stream.fileno())) != generation
                    or _generation(target.lstat()) != generation):
                raise OSError("clone projection changed during complete readback")
        if _generation(source.lstat()) != original.generation or _pointer(source) != original.pointer:
            raise OSError("original journal changed during clone validation")
        plans.append((source, target, original, generation))
    result = {}
    # Do not change any pointer until all journals passed independent full
    # readback. A partial/failed clone is never a promotable financial commit.
    for source, target, original, generation in plans:
        if (_generation(source.lstat()) != original.generation or _pointer(source) != original.pointer
                or _generation(target.lstat()) != generation or _pointer(target) != original.pointer):
            raise OSError("journal clone changed before metadata rebind")
        pointer = {**original.pointer, "source_generation": list(generation)}
        atomic_write_json(_head_path(target), pointer, durable=True)
        result[target.name] = {"head_sha256": original.head["sha256"], "verified_bytes": original.size,
                               "source_generation": list(original.generation), "target_generation": list(generation)}
    return result


def publish_snapshot(path: Path, *, operation: str, before_generation=None, expected_parent=None) -> dict:
    """Seal an authorized completed mutation; never modify a financial byte.

    Caller must hold the canonical journal/maintenance write lease. Append
    admission requires the exact BEFORE-write generation supplied by that
    owner. Unknown/same-inode rewrites cannot establish this parent proof.
    Initial enrollment/replacement scans every source byte in bounded chunks.
    No writer switch, GC, cleanup, broker call or financial repair occurs here.
    """
    import pyarrow as pa
    path = Path(path)
    if operation not in {"seed", "append", "replace"}:
        raise ValueError("unknown journal operation")
    pointer_path = _head_path(path)
    previous = _pointer(path) if managed(path) else None
    if operation == "seed" and previous is not None:
        raise ValueError("journal already enrolled")
    if operation != "seed" and previous is None:
        raise ValueError("journal is not enrolled")
    parent = previous["head"] if previous else None
    if expected_parent is not None and (parent or {}).get("sha256") != expected_parent:
        raise OSError("journal parent compare-and-set rejected")
    offset, epoch = 0, uuid.uuid4().hex
    if operation == "append":
        if before_generation is None or tuple(previous["source_generation"]) != tuple(before_generation):
            raise OSError("append lacks exact prior journal generation")
        with _objects(path.parent) as directory:
            old_node = _read_node(directory, parent)
        offset, epoch = old_node["size_bytes"], old_node["epoch"]
    folder = path.parent / DIRECTORY
    folder.mkdir(mode=0o700, parents=True, exist_ok=True)
    (path.parent / HEADS).mkdir(mode=0o700, exist_ok=True)
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(descriptor, "rb") as source, _objects(path.parent) as directory:
        before = os.fstat(source.fileno())
        if not stat.S_ISREG(before.st_mode) or not offset <= before.st_size <= MAX_SOURCE_BYTES:
            raise ValueError("journal source is not a complete regular projection")
        if operation == "append" and tuple(_generation(before)[:2]) != tuple(before_generation[:2]):
            raise OSError("append replaced its journal projection")
        source.seek(offset)
        chunks, compressor = [], pa.Codec("zstd", compression_level=1)
        while raw := source.read(CHUNK_BYTES):
            if len(chunks) >= MAX_CHUNKS:
                raise ValueError("journal chunk inventory exceeds bounds")
            encoded = compressor.compress(raw, asbytes=True)
            reference, _created = _put_object(directory, encoded, suffix=".zst")
            chunks.append({**reference, "raw_sha256": hashlib.sha256(raw).hexdigest(), "raw_size_bytes": len(raw)})
        generation = list(_generation(before))
        if tuple(generation) != _generation(os.fstat(source.fileno())) or tuple(generation) != _generation(path.lstat()):
            raise OSError("journal source changed during immutable publication")
        node = {"contract": CONTRACT, "operation": operation, "epoch": epoch, "parent": parent,
                "base_size": offset, "size_bytes": before.st_size, "chunks": chunks}
        body = json.dumps(node, separators=(",", ":"), allow_nan=False).encode()
        if len(body) > MAX_HEAD_BYTES:
            raise ValueError("journal node exceeds metadata budget")
        reference, _created = _put_object(directory, body)
        os.fsync(directory)
        # Persist newly created child directory names before a head can name
        # their objects. The small pointer has its own directory barrier too.
        root_fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            os.fsync(root_fd)
        finally:
            os.close(root_fd)
    # CAS again immediately before the pointer. External source/maintenance
    # ownership is mandatory; atomics alone cannot serialize competing writers.
    if previous is not None and _pointer(path) != previous:
        raise OSError("journal head changed before pointer publication")
    if tuple(generation) != _generation(path.lstat()):
        raise OSError("journal source changed before pointer publication")
    pointer = {"contract": CONTRACT, "journal": path.name, "head": reference, "source_generation": generation}
    atomic_write_json(pointer_path, pointer, durable=True)
    marker = pointer_path.with_suffix(".enrolled")
    if not marker.exists():
        atomic_write_json(marker, {"contract": CONTRACT, "journal": path.name}, durable=True)
    return pointer


@contextmanager
def _write_lock(path: Path):
    import fcntl
    lock_fd = os.open(path.parent / (".journal-write-" + path.name + ".lock"),
                      os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600)
    try:
        if not stat.S_ISREG(os.fstat(lock_fd).st_mode):
            raise ValueError("journal mutation lock is not a regular file")
        fcntl.flock(lock_fd, fcntl.LOCK_EX)
        yield
    finally:
        os.close(lock_fd)


def _read_append_intent(path: Path):
    target = _intent_path(path)
    try:
        fd = os.open(target, os.O_RDONLY | os.O_NONBLOCK | os.O_NOFOLLOW)
    except FileNotFoundError:
        # A dangling symlink must not downgrade to an absent intent.
        if target.is_symlink():
            raise ValueError("invalid journal append intent symlink")
        return None
    with os.fdopen(fd, "rb") as handle:
        before = os.fstat(handle.fileno())
        if not stat.S_ISREG(before.st_mode) or not 0 < before.st_size <= MAX_HEAD_BYTES:
            raise ValueError("invalid journal append intent size/type")
        intent = json.loads(handle.read(MAX_HEAD_BYTES + 1))
        if (_generation(before) != _generation(os.fstat(handle.fileno()))
                or _generation(before) != _generation(target.lstat())):
            raise OSError("journal append intent changed during read")
    if (not isinstance(intent, dict)
            or set(intent) != {"contract", "journal", "parent", "head", "append_sha256", "append_size_bytes"}
            or intent["contract"] != APPEND_INTENT_CONTRACT or intent["journal"] != path.name
            or not _reference(intent["head"])
            or not _reference({"sha256": intent["append_sha256"], "size_bytes": intent["append_size_bytes"]},
                              maximum=MAX_APPEND_BYTES)):
        raise ValueError("invalid journal expected append contract")
    parent = intent["parent"]
    if (not isinstance(parent, dict) or set(parent) != {"contract", "journal", "head", "source_generation"}
            or parent["contract"] != CONTRACT or parent["journal"] != path.name
            or not _reference(parent["head"]) or not isinstance(parent["source_generation"], list)
            or len(parent["source_generation"]) != 5
            or any(type(x) is not int for x in parent["source_generation"])):
        raise ValueError("invalid journal append parent")
    with _objects(path.parent) as directory:
        old = _read_node(directory, parent["head"])
        new = _read_node(directory, intent["head"])
        if (new["operation"] != "append" or new["parent"] != parent["head"]
                or new["epoch"] != old["epoch"] or new["base_size"] != old["size_bytes"]
                or new["base_size"] != parent["source_generation"][2]
                or new["size_bytes"] - new["base_size"] != intent["append_size_bytes"]):
            raise ValueError("journal expected append lineage mismatch")
        digest = hashlib.sha256()
        for chunk in new["chunks"]:
            digest.update(_decode_chunk(directory, chunk))
        if digest.hexdigest() != intent["append_sha256"]:
            raise ValueError("journal expected append digest mismatch")
    return intent, new


def _stage_append_intent(path: Path, prior: JournalSnapshot, body: bytes):
    import pyarrow as pa
    with _objects(path.parent) as directory:
        chunks, codec = [], pa.Codec("zstd", compression_level=1)
        for offset in range(0, len(body), CHUNK_BYTES):
            raw = body[offset:offset + CHUNK_BYTES]
            reference, _created = _put_object(directory, codec.compress(raw, asbytes=True), suffix=".zst")
            chunks.append({**reference, "raw_sha256": hashlib.sha256(raw).hexdigest(), "raw_size_bytes": len(raw)})
        node = {"contract": CONTRACT, "operation": "append", "epoch": prior.epoch,
                "parent": prior.head, "base_size": prior.size, "size_bytes": prior.size + len(body), "chunks": chunks}
        encoded = json.dumps(node, separators=(",", ":"), allow_nan=False).encode()
        if len(encoded) > MAX_HEAD_BYTES or node["size_bytes"] > MAX_SOURCE_BYTES:
            raise ValueError("journal expected append exceeds bounds")
        reference, _created = _put_object(directory, encoded)
        # Object names precede the durable intent which names them. Enrolled
        # directories already exist; no parent directory is created here.
        os.fsync(directory)
    intent = {"contract": APPEND_INTENT_CONTRACT, "journal": path.name, "parent": prior.pointer,
              "head": reference, "append_sha256": hashlib.sha256(body).hexdigest(), "append_size_bytes": len(body)}
    atomic_write_json(_intent_path(path), intent, durable=True)
    return intent


def _append_bytes(path: Path, body: bytes, *, expected_generation=None):
    fd = os.open(path, os.O_APPEND | os.O_CREAT | os.O_WRONLY | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600)
    try:
        before = os.fstat(fd)
        if not stat.S_ISREG(before.st_mode):
            raise ValueError("journal append requires a regular file")
        if (expected_generation is not None
                and (_generation(before) != expected_generation or _generation(path.lstat()) != expected_generation)):
            raise OSError("journal projection changed before expected append")
        view = memoryview(body)
        while view:
            written = os.write(fd, view)
            if written <= 0:
                raise OSError("short paper journal append")
            view = view[written:]
        os.fsync(fd)
        generation = _generation(os.fstat(fd))
        if _generation(path.lstat()) != generation or generation[2] != before.st_size + len(body):
            raise OSError("journal projection changed during expected append")
        return generation
    finally:
        os.close(fd)


def append_paper_journal(path: Path, body: bytes):
    """Canonical exact-byte append, with recoverable intent for enrolled views.

    Legacy appends retain their format and single raw-file fsync. Managed
    appends persist immutable expected bytes BEFORE touching the projection.
    A crash never causes an automatic retry/duplicate fill: explicit recovery
    may only seal bytes already present, not write missing execution data.
    Financial state/commit/ACK remain the executor's separate responsibility.
    """
    path = Path(path)
    if type(body) is not bytes:
        raise TypeError("paper append requires immutable encoded bytes")
    if not body:
        return
    if not managed(path):
        _append_bytes(path, body)
        return
    if len(body) > MAX_APPEND_BYTES:
        raise ValueError("managed journal append exceeds bounded intent")
    with _write_lock(path):
        # Validate any retained intent before replacing it, and require the
        # exact accepted projection. An interrupted append cannot be laundered
        # into a new parent by another ordinary append attempt.
        _read_append_intent(path)
        prior = JournalSnapshot(path)
        with _JournalReader(prior):
            pass
        intent = _stage_append_intent(path, prior, body)
        if _pointer(path) != prior.pointer:
            raise OSError("journal parent changed before expected append")
        generation = _append_bytes(path, body, expected_generation=prior.generation)
        if _pointer(path) != prior.pointer:
            raise OSError("journal parent changed before append head")
        atomic_write_json(_head_path(path), {"contract": CONTRACT, "journal": path.name,
                          "head": intent["head"], "source_generation": list(generation)}, durable=True)


def recover_paper_journal_append(path: Path):
    """Idempotently seal an exact, fully written intent; never create fills.

    Hold the ordinary financial/maintenance lease as well. This independent
    cold readback compares EVERY retained byte and delta to immutable objects.
    Partial/foreign rewrites fail closed and remain untouched. Sealing a head
    is not financial recovery, publication, or permission to ACK a revision.
    """
    path = Path(path)
    if not managed(path):
        raise ValueError("append recovery requires an enrolled journal")
    with _write_lock(path):
        pending = _read_append_intent(path)
        if pending is None:
            raise ValueError("journal has no durable expected append")
        intent, new = pending
        pointer = _pointer(path)
        parent = intent["parent"]
        already_sealed = pointer["head"] == intent["head"]
        if pointer != parent and not already_sealed:
            raise OSError("journal recovery parent compare-and-set rejected")
        fd = os.open(path, os.O_RDWR | os.O_NONBLOCK | os.O_NOFOLLOW)
        with os.fdopen(fd, "rb") as source, _objects(path.parent) as directory:
            before = os.fstat(source.fileno())
            generation = _generation(before)
            if (not stat.S_ISREG(before.st_mode) or generation[:2] != tuple(parent["source_generation"][:2])
                    or before.st_size != new["size_bytes"]):
                raise OSError("incomplete or foreign journal projection; recovery writes no bytes")
            if already_sealed and pointer["source_generation"] != list(generation):
                raise OSError("journal changed after append seal")
            # Snapshot construction validates the entire parent chain; it does
            # not accept raw-file metadata as a substitute for byte integrity.
            prior = JournalSnapshot(path)
            chunks = prior.chunks if already_sealed else [*prior.chunks, *new["chunks"]]
            for chunk in chunks:
                raw = _decode_chunk(directory, chunk)
                if source.read(len(raw)) != raw:
                    raise ValueError("journal recovery bytes differ from durable intent")
            if (source.read(1) or generation != _generation(os.fstat(source.fileno()))
                    or generation != _generation(path.lstat()) or _pointer(path) != pointer):
                raise OSError("journal projection changed during recovery readback")
            # A complete matching write may have crashed before its fsync.
            # Re-establish that barrier before publishing its expected head.
            os.fsync(source.fileno())
        if (generation != _generation(path.lstat()) or _pointer(path) != pointer
                or _read_append_intent(path) != pending):
            raise OSError("journal recovery changed before head publication")
        if not already_sealed:
            atomic_write_json(_head_path(path), {"contract": CONTRACT, "journal": path.name,
                              "head": intent["head"], "source_generation": list(generation)}, durable=True)
        else:
            # A previous process may have exited after rename but before the
            # namespace barrier. A visible matching head is not that barrier.
            directory = os.open(path.parent / HEADS, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
        return {"contract": APPEND_INTENT_CONTRACT, "journal": path.name, "already_sealed": already_sealed,
                "verified_bytes": new["size_bytes"], "append_bytes": intent["append_size_bytes"],
                "head_sha256": intent["head"]["sha256"], "financial_commit_acknowledged": False}


def recover_pending_journal_heads(root: Path):
    """Startup adapter, called only under the native engine's existing lease.

    Unchanged heads cost only metadata reads. A damaged/partial intent still
    aborts restore; no enrollment or financial commit is attempted. The engine
    must subsequently run its ordinary state/ledger divergence and order-ID
    recovery checks, even if every byte-view head was successfully sealed.
    """
    root, recovered, enrolled = Path(root), [], False
    for name in FILENAMES:
        path = root / name
        if not managed(path):
            continue
        enrolled = True
        pointer = _pointer(path)
        if tuple(pointer["source_generation"]) != _generation(path.lstat()):
            recovered.append(recover_paper_journal_append(path))
    if enrolled:
        # Even an already-matching pointer may be a rename whose producer
        # died before syncing this child directory. The later financial root
        # commit fsync is not a substitute for this separate directory.
        directory = os.open(root / HEADS, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    return recovered


@contextmanager
def journal_write_transaction(path: Path, *, operation: str):
    """Reader-first adapter for existing canonical mutation owners.

    There is no automatic enrollment. Legacy journals retain their existing
    path. Enrolled journals serialize pointer publication and refuse unknown
    mutations before a new write. A crash between projection fsync and head
    publication remains a fail-closed recovery condition, not an empty book.
    Existing financial/maintenance leases and commit/ACK remain mandatory.
    """
    path = Path(path)
    if operation not in {"append", "replace"}:
        raise ValueError("unknown journal mutation operation")
    if not managed(path):
        yield
        return
    with _write_lock(path):
        prior = JournalSnapshot(path)
        with _JournalReader(prior):
            pass
        yield
        if _generation(path.lstat()) != prior.generation:
            publish_snapshot(path, operation=operation, before_generation=prior.generation,
                             expected_parent=prior.head["sha256"])

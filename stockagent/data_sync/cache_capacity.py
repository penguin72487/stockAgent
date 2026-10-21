"""Bound cache growth at the existing writers; never evict or touch sources.

Reuse the completed-forward kernel-lease admission primitive. One per-user,
per-filesystem control namespace coordinates cooperating writers' free-space
reservations; a separate per-cache namespace counts actual unique allocations
and limits that family's growth. Neither control directory stores payloads.
Existing data and cache hits remain readable when admission is deferred.
"""
from __future__ import annotations

from contextlib import ExitStack, contextmanager
import hashlib
import io
import json
import os
from pathlib import Path
import re
import socket
import stat
import subprocess
import time
from typing import Callable

from stockagent.data_sync.completed_forward_capacity import CapacityDeferred, reserve_capacity
from stockagent.data_sync.desync_snapshots import SnapshotError

REPO_ROOT = Path(__file__).resolve().parents[2]
POLICY_PATH = REPO_ROOT / "configs/data_sync/cache_capacity.json"
PREPARED_POLICY_PATH = REPO_ROOT / "configs/data_sync/prepared_capacity.json"
MATERIALIZED_ROOT = Path("/srv/stockagent-packed-materialized")
ARTIFACT_CACHE_ROOT = REPO_ROOT / "artifacts/cache"
PANEL_COMPATIBILITY_ROOT = REPO_ROOT / "data_tw_public"
PREPARED_ROOT = REPO_ROOT / "artifacts/data_preparation"
# Not /tmp or /var/tmp: PrivateTmp services must see the SAME reservations.
CONTROL_ROOT = Path("/var/lib") if os.geteuid() == 0 else Path.home() / ".cache"
CONTRACT = "bounded_cache_writes_v1"


def _mountinfo() -> str:
    with Path("/proc/self/mountinfo").open("rb") as source:
        raw = source.read(1024**2 + 1)
    if len(raw) > 1024**2:
        raise CapacityDeferred("cache_mount_inventory_bound")
    return raw.decode("utf-8", errors="surrogateescape")


def load_capacity_policy(path: Path = POLICY_PATH) -> dict:
    def unique(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("duplicate cache capacity policy field")
            result[key] = value
        return result
    with path.open("rb") as source:
        raw = source.read(4097)
    if len(raw) > 4096:
        raise ValueError("cache capacity policy is not bounded")
    value = json.loads(raw, object_pairs_hook=unique)
    integer_fields = {"minimum_free_bytes", "materialized_capacity_bytes",
                      "panel_family_capacity_bytes", "maximum_inventory_entries",
                      "maximum_inventory_seconds"}
    if (not isinstance(value, dict) or set(value) != {"schema_version", "contract", "nodes", *integer_fields}
            or type(value["schema_version"]) is not int or value["schema_version"] != 1
            or value["contract"] != CONTRACT
            or not isinstance(value["nodes"], list) or not 0 < len(value["nodes"]) <= 16
            or any(not isinstance(name, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,63}", name)
                   for name in value["nodes"])
            or len(set(value["nodes"])) != len(value["nodes"])
            or any(type(value[name]) is not int or not 0 < value[name] < 2**63 for name in integer_fields)
            or value["maximum_inventory_entries"] > 1_000_000
            or value["maximum_inventory_seconds"] > 30):
        raise ValueError("invalid cache capacity policy")
    return value


def load_prepared_capacity_policy(path: Path = PREPARED_POLICY_PATH) -> dict:
    """Separate schema: do not break still-running strict v1 cache readers."""
    with path.open("rb") as source:
        raw = source.read(4097)
    if len(raw) > 4096:
        raise ValueError("prepared capacity policy is not bounded")
    pairs = json.loads(raw, object_pairs_hook=lambda pairs: pairs)
    if not isinstance(pairs, list) or any(not isinstance(item, tuple) or len(item) != 2 for item in pairs):
        raise ValueError("invalid prepared capacity policy")
    value = dict(pairs)
    if (len(value) != len(pairs) or set(value) != {"schema_version", "contract", "family_capacity_bytes", "maximum_generation_bytes"}
            or type(value["schema_version"]) is not int or value["schema_version"] != 1
            or value["contract"] != "prepared_generation_admission_v1"
            or any(type(value[name]) is not int or not 0 < value[name] < 2**63
                   for name in ("family_capacity_bytes", "maximum_generation_bytes"))
            or value["maximum_generation_bytes"] > value["family_capacity_bytes"]):
        raise ValueError("invalid prepared capacity policy")
    return value


def allocated_tree_bytes(root: Path, *, maximum_entries: int,
                         maximum_seconds: float) -> int:
    """Physical census without decoding payloads; unknown objects count or defer.

    Count hardlinked inodes once, including directory blocks. A symlink, mount,
    unreadable entry is not permission to ignore its bytes.
    Traversal is bounded, never a full-repository scan on a service hot path.
    """
    root = root.absolute()
    if not root.exists():
        return 0
    started = time.monotonic()
    pending, seen, total, entries = [root], set(), 0, 0
    root_info = root.lstat()
    device = root_info.st_dev
    mounts = _mountinfo()
    for line in mounts.splitlines():
        columns = line.split()
        if len(columns) < 10 or "-" not in columns:
            raise CapacityDeferred("unmeasured_cache_mounts")
        mount = Path(re.sub(r"\\([0-7]{3})", lambda match: chr(int(match[1], 8)), columns[4]))
        if mount != root and mount.is_relative_to(root):
            # st_dev alone misses a same-filesystem bind mount. Read the
            # bounded mount table once, not realpath/stat each payload file.
            raise CapacityDeferred("nested_cache_mount", root=str(root))
    while pending:
        path = pending.pop()
        info = path.lstat()
        entries += 1
        if entries > maximum_entries or time.monotonic() - started > maximum_seconds:
            raise CapacityDeferred("cache_inventory_bound", root=str(root), observed_entries=entries)
        if info.st_dev != device or not (stat.S_ISDIR(info.st_mode) or stat.S_ISREG(info.st_mode)):
            # The managed current/ directory contains only link metadata. Its
            # targets already belong to the enrolled root and are counted there.
            if stat.S_ISLNK(info.st_mode):
                total += info.st_blocks * 512
                continue
            raise CapacityDeferred("unmeasured_cache_entry", root=str(root))
        key = (info.st_dev, info.st_ino)
        if key not in seen:
            total += info.st_blocks * 512
            seen.add(key)
        if stat.S_ISDIR(info.st_mode):
            with os.scandir(path) as children:
                for child in children:
                    pending.append(Path(child.path))
                    if len(pending) + entries > maximum_entries:
                        raise CapacityDeferred("cache_inventory_bound", root=str(root))
    after = root.lstat()
    if (after.st_dev, after.st_ino) != (root_info.st_dev, root_info.st_ino) or _mountinfo() != mounts:
        raise CapacityDeferred("cache_namespace_changed")
    return total


def _control_directory(name: str) -> Path:
    parent = CONTROL_ROOT / f"stockagent-capacity-{os.geteuid()}"
    parent.mkdir(mode=0o700, exist_ok=True)
    def validate(directory):
        info = directory.lstat()
        if (directory.resolve() != directory or not stat.S_ISDIR(info.st_mode)
                or info.st_uid != os.geteuid() or stat.S_IMODE(info.st_mode) != 0o700):
            raise CapacityDeferred("untrusted_cache_capacity_controls")
    validate(parent)
    path = parent / name
    path.mkdir(mode=0o700, exist_ok=True)
    validate(path)
    return path


def _admit_backing_volume(existing: Path, required_bytes: int) -> dict:
    if "microsoft" not in Path("/proc/sys/kernel/osrelease").read_text().lower():
        return {"state": "native-filesystem"}
    # A virtual ext4 free count is not proof that its Windows VHDX backing
    # drive can grow. Use the same actual-distro registry/physical-volume
    # measurement as canonical return/recovery, never assume it is C:.
    from stockagent.data_sync.windows_cold_io import admit_workspace
    try:
        distribution = os.environ.get("WSL_DISTRO_NAME", "")
        if not distribution:
            # Native systemd services do not inherit a login WSL environment.
            # Reuse this authority's EXPLICIT existing rescue configuration,
            # not a guessed default distro and not another private env file.
            enrollment = REPO_ROOT / "configs/training_rescue.json"
            info = enrollment.lstat()
            if (enrollment.resolve() != enrollment or not stat.S_ISREG(info.st_mode)
                    or info.st_uid != os.geteuid()):
                raise ValueError("authority runtime identity ownership/path differs")
            with enrollment.open("rb") as source:
                raw = source.read(4097)
            if len(raw) > 4096:
                raise ValueError("unbounded authority runtime identity")
            identity = json.loads(raw)
            if (not isinstance(identity, dict) or identity.get("contract") != "stockagent.training_rescue.v1"
                    or type(identity.get("schema_version")) is not int or identity["schema_version"] != 1
                    or identity.get("cold_guard") != "penguin-d-primary"
                    or not isinstance(identity.get("windows_distribution"), str)
                    or not re.fullmatch(r"[A-Za-z0-9_.-]{1,80}", identity["windows_distribution"])):
                raise ValueError("authority runtime identity differs")
            distribution = identity["windows_distribution"]
        return admit_workspace(existing, required_bytes, windows_distribution=distribution)
    except (OSError, ValueError, SnapshotError, subprocess.SubprocessError) as error:
        raise CapacityDeferred("physical_backing_headroom_unconfirmed",
                               required_bytes_including_reserve=required_bytes,
                               error_type=type(error).__name__) from error


def _enrolled_capacity(root: Path, family: str, policy: dict) -> int | None:
    if socket.gethostname() not in policy["nodes"]:
        return None
    if family == "materialized" and root == MATERIALIZED_ROOT:
        return policy["materialized_capacity_bytes"]
    if family == "panel" and root.is_relative_to(ARTIFACT_CACHE_ROOT) and root != ARTIFACT_CACHE_ROOT:
        return policy["panel_family_capacity_bytes"]
    # The native service reader still supports panels next to the public
    # Parquets when no explicit cache root was selected. Do not leave that
    # compatibility writer as an unbounded bypass or move accepted data.
    if (family == "panel" and root.name == "panel_cache_v2"
            and root.is_relative_to(PANEL_COMPATIBILITY_ROOT.resolve())):
        return policy["panel_family_capacity_bytes"]
    if family == "prepared" and root.is_relative_to(PREPARED_ROOT) and root != PREPARED_ROOT:
        return load_prepared_capacity_policy()["family_capacity_bytes"]
    return None


def cache_capacity_status(root: Path, *, family: str = "materialized") -> dict:
    """Bounded observation of enrolled cache allocations; no payload mutation."""
    root = root.absolute()
    policy = load_capacity_policy()
    limit = _enrolled_capacity(root, family, policy)
    if limit is None:
        raise ValueError("capacity observation requires an enrolled cache namespace")
    if root.resolve() != root:
        raise CapacityDeferred("redirected_cache_namespace")
    existing = root
    while not existing.exists():
        existing = existing.parent
    filesystem = os.statvfs(existing)
    result = {"contract": CONTRACT, "root": str(root), "family": family,
              "capacity_bytes": limit, "minimum_free_bytes": policy["minimum_free_bytes"],
              "filesystem_available_bytes": filesystem.f_bavail * filesystem.f_frsize,
              "data_paths_removed": 0, "automatic_early_eviction": False}
    try:
        used = allocated_tree_bytes(root, maximum_entries=policy["maximum_inventory_entries"],
                                    maximum_seconds=policy["maximum_inventory_seconds"])
    except (OSError, CapacityDeferred) as error:
        result.update(inventory_complete=False, deferred_reason=str(error))
        if isinstance(error, CapacityDeferred):
            result["deferred_details"] = error.details
    else:
        result.update(inventory_complete=True, allocated_bytes=used,
                      capacity_remaining_bytes=max(0, limit - used))
    return result


@contextmanager
def reserve_cache_write(root: Path, *, key: str, maximum_growth_bytes: int | Callable[[], int],
                        family: str, enabled: bool = True, policy: dict | None = None):
    """Admit only enrolled local caches; existing tests/custom roots stay opt-in.

    Supplying a policy explicitly enrolls an isolated test/operator namespace.
    No receipt, head, pin, lease, payload or old reservation is treated as a
    deletion candidate here. Kernel-held reservation records alone are reapable.
    """
    if not enabled:
        yield {"state": "existing-hit", "data_paths_removed": 0}
        return
    if family not in {"materialized", "panel", "prepared"}:
        raise ValueError("unknown enrolled cache family")
    root = root.absolute()
    configured = load_capacity_policy() if policy is None else policy
    capacity = (_enrolled_capacity(root, family, configured) if policy is None else
                load_prepared_capacity_policy()["family_capacity_bytes"] if family == "prepared" else
                configured[{"materialized": "materialized_capacity_bytes", "panel": "panel_family_capacity_bytes"}[family]])
    if capacity is None:
        yield {"state": "not-enrolled", "data_paths_removed": 0}
        return
    if callable(maximum_growth_bytes):
        maximum_growth_bytes = maximum_growth_bytes()
    if root.resolve() != root:
        raise CapacityDeferred("redirected_cache_namespace")
    existing = root
    while not existing.exists():
        existing = existing.parent
    device = existing.stat().st_dev
    filesystem_controls = _control_directory(f"filesystem-{device}")
    if filesystem_controls.stat().st_dev != device:
        raise CapacityDeferred("cache_controls_on_other_filesystem")
    token = hashlib.sha256((str(root) + "\0" + key).encode()).hexdigest()
    controls = _control_directory("cache-" + hashlib.sha256(str(root).encode()).hexdigest())
    with ExitStack() as leases:
        filesystem = leases.enter_context(reserve_capacity(
            filesystem_controls, key=token, maximum_growth_bytes=maximum_growth_bytes,
            capacity_bytes=2**63 - 1, minimum_free_bytes=configured["minimum_free_bytes"],
        ))
        backing = _admit_backing_volume(existing, maximum_growth_bytes
            + filesystem["other_reserved_growth_bytes"] + filesystem["control_bytes"]
            + configured["minimum_free_bytes"])
        cache = leases.enter_context(reserve_capacity(
            controls, key=token, maximum_growth_bytes=maximum_growth_bytes,
            capacity_bytes=capacity,
            external_usage_bytes=lambda: allocated_tree_bytes(root,
                maximum_entries=configured["maximum_inventory_entries"],
                maximum_seconds=configured["maximum_inventory_seconds"]),
        ))
        yield {"contract": CONTRACT, "filesystem": filesystem, "backing_volume": backing, "cache": cache,
               "data_paths_removed": 0}


class GenerationWriteBudget:
    """Enforce one reserved generation allowance before *every* payload write.

    Charge cumulative writes (even replaced checkpoints), rounded allocations
    and directory/temporary overhead. This deliberately overcounts rather than
    reclaiming credit from partially written or failed files. Hardlinks charge
    namespace overhead, never the donor's immutable bytes again. The caller
    holds reserve_cache_write for the whole attempt and must route all writes
    through open/charge/charge_json. No automatic eviction is involved.
    """

    def __init__(self, maximum_bytes):
        if type(maximum_bytes) is not int or not 0 < maximum_bytes < 2**63:
            raise ValueError("positive generation byte allowance required")
        self.maximum_bytes = maximum_bytes
        self.charged_bytes = self.write_calls = 0
        self.deferred = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        # Arrow/Rust I/O adapters can wrap a Python callback error in IOError.
        # Preserve the typed capacity reason at the canonical owner boundary.
        if self.deferred is not None:
            raise self.deferred from exc
        return False

    def charge(self, size):
        if type(size) is not int or size < 0:
            raise ValueError("nonnegative write length required")
        # Round to 64 KiB, with another 64 KiB for inode/directory/atomic temp.
        allocation = ((size + 65535) // 65536 + 1) * 65536
        if self.deferred is not None:
            raise self.deferred
        if self.charged_bytes + allocation > self.maximum_bytes:
            self.deferred = CapacityDeferred("prepared_generation_byte_budget_exhausted",
                maximum_generation_bytes=self.maximum_bytes, charged_bytes=self.charged_bytes,
                requested_charge_bytes=allocation, last_good_generation_retained=True)
            raise self.deferred
        self.charged_bytes += allocation
        self.write_calls += 1

    def charge_json(self, value):
        self.charge(len(json.dumps(value, ensure_ascii=True, indent=2, allow_nan=False).encode()) * 2)

    def open(self, path):
        self.charge(0)
        return _BudgetedWriter(Path(path).open("xb"), self)

    def stats(self):
        return {"contract": "prepared_generation_cumulative_write_budget_v1",
                "maximum_bytes": self.maximum_bytes, "charged_bytes": self.charged_bytes,
                "write_calls": self.write_calls, "early_eviction": False}


class _BudgetedWriter(io.RawIOBase):
    """No fileno escape hatch: native serializers must use bounded write()."""

    def __init__(self, stream, budget):
        super().__init__()
        self.stream, self.budget = stream, budget

    def writable(self):
        return True

    def seekable(self):
        return True

    def tell(self):
        return self.stream.tell()

    def seek(self, offset, whence=0):
        return self.stream.seek(offset, whence)

    def write(self, value):
        self.budget.charge(memoryview(value).nbytes)
        return self.stream.write(value)

    def flush(self):
        if not self.stream.closed:
            self.stream.flush()

    def close(self):
        if not self.closed:
            try:
                super().close()
            finally:
                self.stream.close()

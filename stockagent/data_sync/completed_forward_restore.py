"""Cold-first client for the existing flat completed-forward writer.

No new store, registry, publisher, model or cleanup owner. A fixed standard pin
owns restoration; a missing binding for an already published key is an error,
not permission to compute. The caller retains its original key lease throughout.
"""
from __future__ import annotations

from dataclasses import dataclass
from contextlib import contextmanager
import fcntl
import os
from pathlib import Path
import stat
from typing import Any, Callable

from stockagent.data_sync.completed_forward_publication import (
    FAMILY, _generation, _guard_pin_parents, _pin_parents, _root_generation,
    fixed_pin_path, select_entry, validate_key,
)
from stockagent.data_sync.desync_snapshots import SnapshotError, _fsync_directory
from stockagent.data_sync.packed_snapshots import (
    _check_packed_read_authority, _load_inventory, _validate_inventory,
    resolve_packed_pin, stage_packed_root_files,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
CANONICAL_CACHE = Path("artifacts/cache/completed_live_inference")
MAX_RECEIPT_BYTES = 1024**2
# Match the existing packed reconstruction's SSD reserve. This is a physical
# free-space admission guard, NOT an installed namespace quota or eviction.
MINIMUM_FREE_BYTES = 32 * 1024**3


@contextmanager
def forward_miss_admission(root: Path, key: str, maximum_payload_bytes: int):
    from stockagent.data_sync.completed_forward_capacity import reserve_capacity

    # Bound a complete output, serialization/restore temporary, both receipts
    # and control/directory growth, not merely the final logical tensor size.
    with reserve_capacity(root, key=key,
                          maximum_growth_bytes=2 * maximum_payload_bytes + 8 * MAX_RECEIPT_BYTES,
                          capacity_bytes=2**63 - 1, minimum_free_bytes=MINIMUM_FREE_BYTES) as reservation:
        reservation["policy"] = "physical_free_admission_not_namespace_quota"
        yield reservation


def _configuration(path: Path) -> tuple[int, ...]:
    generation = _generation(path)
    if path.resolve(strict=True) != path or generation[2] > 1024**2 or generation[5] & 0o022:
        raise SnapshotError("forward storage configuration must be bounded, real and private")
    return generation


@dataclass(frozen=True)
class ForwardColdBinding:
    entry: dict[str, Any]
    sync_root: Path
    retention_config: Path
    repo_root: Path
    configuration_generations: tuple[tuple[Path, tuple[int, ...]], ...] = ()

    def guard(self) -> None:
        if any(_configuration(path) != generation
               for path, generation in self.configuration_generations):
            raise SnapshotError("completed-forward storage configuration changed")


def production_binding(root: Path) -> ForwardColdBinding | None:
    """Enroll only the original canonical root, never an offline override.

    Keep the publication catalog and retention config authoritative. A valid
    hot hit never calls this loader or needs D/config availability.
    """
    if root.absolute() != REPO_ROOT / CANONICAL_CACHE:
        return None
    from scripts.publish_data_releases import _load_catalog
    from stockagent.data_sync.packed_retention import RetentionConfig

    catalog = REPO_ROOT / "configs/data_sync/packed_datasets.json"
    retention = REPO_ROOT / "configs/data_sync/packed_retention.json"
    generations = tuple((path, _configuration(path)) for path in (catalog, retention))
    entries = [entry for entry in _load_catalog(catalog, include_families=True)
               if entry.get("publication_family") == FAMILY]
    if len(entries) != 1 or entries[0].get("publish") is not True:
        raise SnapshotError("canonical completed-forward catalog family is missing or disabled")
    entry = entries[0]
    source = Path(entry["source"])
    if (source if source.is_absolute() else REPO_ROOT / source) != root.absolute():
        raise SnapshotError("completed-forward catalog source differs from the canonical writer")
    config = RetentionConfig.load(retention, repo_root=REPO_ROOT)
    result = ForwardColdBinding(entry, config.sync_root, retention, REPO_ROOT, generations)
    result.guard()
    return result


def _key_guard(root: Path, key: str, descriptor: int) -> Callable[[], None]:
    if type(descriptor) is not int or descriptor < 0:
        raise SnapshotError("cold restore requires the original key lease descriptor")
    owner = root / f"{key}.lock"
    before = _generation(owner)
    if before[6] != 1 or before[5] & 0o077:
        raise SnapshotError("cold restore requires the private original key owner")

    def guard():
        held = os.fstat(descriptor)
        if (_generation(owner) != before or not stat.S_ISREG(held.st_mode)
                or (held.st_dev, held.st_ino) != before[:2]):
            raise SnapshotError("cold restore key owner changed")
        # A wrong open description cannot borrow another worker's lease.
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise SnapshotError("cold restore does not own the key lease") from error

    guard()
    return guard


def restore_completed_forward(
    root: Path, key: str, identity: dict[str, Any], *, binding: ForwardColdBinding,
    key_descriptor: int, read: Callable, validate_before_commit: Callable[[], None] | None,
    maximum_payload_bytes: int,
) -> tuple[Any, dict[str, Any]] | None:
    """Restore a complete fixed output; return None ONLY for a never-enrolled key.

    Caller must reserve the complete payload/receipt/temporary growth before
    entering. Failed private stages remain counted and retained for diagnosis.
    No overwrite of an unrelated key, managed tree or READY is possible.
    """
    from stockagent.live.completed_inference_cache import _CACHE_MISS, identity_digest

    validate_key(key)
    if identity_digest(identity) != key or type(maximum_payload_bytes) is not int or maximum_payload_bytes <= 0:
        raise SnapshotError("cold restore identity or payload bound is invalid")
    root = root.absolute()
    root_generation = _root_generation(root)
    key_guard = _key_guard(root, key, key_descriptor)
    entry = select_entry(binding.entry, key)
    if entry.get("publish") is not True:
        raise SnapshotError("cold restore requires an enabled original catalog family")
    source = Path(entry["source"])
    if (source if source.is_absolute() else binding.repo_root / source) != root:
        raise SnapshotError("cold restore catalog source does not match the writer")
    binding.guard()
    _check_packed_read_authority(binding.sync_root)
    pin = fixed_pin_path(entry, sync_root=binding.sync_root,
                         retention_config=binding.retention_config, repo_root=binding.repo_root)
    parents = _pin_parents(pin)

    def guard():
        binding.guard()
        key_guard()
        if _root_generation(root) != root_generation:
            raise SnapshotError("cold restore destination owner changed")
        _guard_pin_parents(parents)
        if validate_before_commit is not None:
            validate_before_commit()

    guard()
    if not pin.exists() and not pin.is_symlink():
        # The dataset name contains the entire key. Even an interrupted
        # enrollment must not become a fresh-model miss when its pin is lost.
        if any((binding.sync_root / group / entry["dataset"]).exists()
               or (binding.sync_root / group / entry["dataset"]).is_symlink()
               for group in ("heads", "manifests")):
            raise SnapshotError("published completed-forward key is missing its fixed pin")
        guard()
        _check_packed_read_authority(binding.sync_root)
        return None
    pin_generation = _generation(pin)
    resolved = resolve_packed_pin(binding.sync_root, pin, expected_dataset=entry["dataset"])
    names = entry["included_root_files"]
    manifest = resolved.manifest
    metadata = manifest.get("metadata", {})
    if (metadata.get("completed_forward_contract") != FAMILY
            or metadata.get("completed_forward_key") != key
            or manifest.get("source", {}).get("selection", {}).get("included_root_files") != names):
        raise SnapshotError("fixed forward release is bound to a different output")
    # Full fixed-release reconstruction uses the original publication/retention
    # owner and copier, including original mode, full SHA, bounded size and fsync.
    stage = stage_packed_root_files(
        binding.sync_root, root, resolved, expected_names=names,
        maximum_bytes=maximum_payload_bytes + MAX_RECEIPT_BYTES,
    )
    staged = stage["files"]
    payload, receipt = (staged[name] for name in names)
    generations = tuple((path, _generation(path)) for path in (payload, receipt))
    if (generations[0][1][5] & 0o222 or generations[1][1][6] != 1
            or not 0 < generations[0][1][2] <= maximum_payload_bytes
            or generations[1][1][2] > MAX_RECEIPT_BYTES):
        raise SnapshotError("fixed forward restored writable or oversized files")
    commit: dict[str, Any] = {}
    value = read(payload, key, identity, commit=commit)
    if value is _CACHE_MISS:
        raise SnapshotError("fixed forward restore did not produce a complete CPU output")
    expected = {
        names[0]: (commit["payload_sha256"], commit["payload_bytes"]),
        names[1]: (commit["receipt_sha256"], generations[1][1][2]),
    }
    inventory = _load_inventory(binding.sync_root, manifest)
    _validate_inventory(manifest, inventory)
    if (len(inventory) != 2 or any(row.get("kind") != "file" or row.get("path") not in expected
            or (row.get("sha256"), row.get("size")) != expected[row["path"]] for row in inventory)
            or metadata.get("completed_forward_payload_sha256") != commit["payload_sha256"]
            or metadata.get("completed_forward_payload_bytes") != str(commit["payload_bytes"])
            or metadata.get("completed_forward_receipt_sha256") != commit["receipt_sha256"]):
        raise SnapshotError("fixed forward original file binding differs from decoded CPU output")

    def commit_guard():
        guard()
        if _generation(pin) != pin_generation or any(_generation(path) != proof for path, proof in generations):
            raise SnapshotError("fixed forward pin or private stages changed before commit")

    commit_guard()
    # Stages are already durable. Two renames with directory barriers preserve
    # payload-before-receipt commit without writing a second complete payload.
    os.replace(payload, root / names[0])
    _fsync_directory(root)
    guard()
    if _generation(pin) != pin_generation or _generation(receipt) != generations[1][1]:
        raise SnapshotError("fixed forward changed during durable commit")
    os.replace(receipt, root / names[1])
    _fsync_directory(root)
    guard()
    return value, {"contract": "completed_forward_fixed_cold_restore_v1", "key": key,
                   "snapshot_id": resolved.manifest["snapshot_id"],
                   "manifest_sha256": resolved.manifest_sha256,
                   "payload_bytes": commit["payload_bytes"], "commit": commit,
                   "gpu_forward_calls": 0, "managed_materialization_created": False,
                   "payload_reencoded": False, "consumer_commit": True}
